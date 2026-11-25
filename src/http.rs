//! The HTTP layer: routes, authentication, the multipart upload and the
//! response formats. All input rules live in `normalize`.

use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{HeaderMap, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{oneshot, Semaphore};

use crate::config::Config;
use crate::engine::Run;
use crate::normalize;
use crate::sched::{Job, Msg, Query};

type Resp = Response<Full<Bytes>>;

pub struct State {
    cfg: Arc<Config>,
    tx: UnboundedSender<Msg>,
    next_id: AtomicU64,
    ffmpeg: Semaphore,
}

impl State {
    pub fn new(cfg: Arc<Config>, tx: UnboundedSender<Msg>) -> State {
        let ffmpeg = Semaphore::new(cfg.server.ffmpeg_parallel.max(1));
        State { cfg, tx, next_id: AtomicU64::new(1), ffmpeg }
    }
}

/// Serves one TCP connection.
pub async fn serve(state: Arc<State>, stream: TcpStream) {
    let service = service_fn(move |req| route(state.clone(), req));
    let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
}

// ------------------------------------------------------------------ replies

fn reply(status: u16, content_type: &'static str, body: impl Into<Bytes>) -> Resp {
    let mut resp = Response::new(Full::new(body.into()));
    *resp.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    resp.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    resp
}

fn json_reply(status: u16, value: &Value) -> Resp {
    reply(status, "application/json", value.to_string())
}

/// An error body with the OpenAI form (`error`) and a `detail` string.
fn error(status: u16, kind: &str, code: &str, message: impl Into<String>) -> Resp {
    let message = message.into();
    json_reply(status, &json!({"error": {"message": message, "type": kind, "param": null, "code": code}, "detail": message}))
}

fn bad(code: &str, message: impl Into<String>) -> Resp {
    error(400, "invalid_request_error", code, message)
}

// --------------------------------------------------------------------- auth

struct Who {
    name: String,
    class: u8,
    allow_fallback: bool,
}

fn same(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn authenticate(cfg: &Config, headers: &HeaderMap) -> Result<Who, Resp> {
    let text = |name: &str| headers.get(name).and_then(|v: &HeaderValue| v.to_str().ok());
    let token = text("authorization")
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .or_else(|| text("x-api-key"))
        .map(str::trim);
    if let Some(token) = token {
        if let Some(k) = cfg.keys.iter().find(|k| same(&k.secret, token)) {
            return Ok(Who { name: k.name.clone(), class: k.class, allow_fallback: k.allow_fallback });
        }
    }
    if cfg.server.open {
        return Ok(Who { name: "open".into(), class: cfg.server.open_class, allow_fallback: false });
    }
    Err(error(401, "invalid_request_error", "invalid_api_key", "Missing or wrong API key. Send `Authorization: Bearer <key>`."))
}

// ------------------------------------------------------------------- routes

async fn route(state: Arc<State>, req: Request<Incoming>) -> Result<Resp, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let resp = match (&method, path.as_str()) {
        (&Method::GET, "/health") => {
            match tokio::time::timeout(Duration::from_secs(1), ask(&state, Query::Health)).await {
                Ok(Some(body)) => reply(200, "application/json", body),
                _ => error(503, "server_error", "scheduler_stuck", "The scheduler does not answer."),
            }
        }
        (&Method::GET, "/v1/models" | "/running" | "/metrics") => match authenticate(&state.cfg, req.headers()) {
            Err(resp) => resp,
            Ok(_) => {
                let (query, kind) = match path.as_str() {
                    "/v1/models" => (Query::Models, "application/json"),
                    "/running" => (Query::Running, "application/json"),
                    _ => (Query::Metrics, "text/plain; version=0.0.4"),
                };
                match ask(&state, query).await {
                    Some(body) => reply(200, kind, body),
                    None => error(503, "server_error", "shutdown", "The hub stops."),
                }
            }
        },
        (&Method::POST, "/v1/audio/transcriptions") => match authenticate(&state.cfg, req.headers()) {
            Err(resp) => resp,
            Ok(who) => {
                let (resp, model, class) = transcribe(&state, req, &who).await;
                let code = resp.status().as_u16();
                let _ = state.tx.send(Msg::Record { model, key: who.name, class, code });
                resp
            }
        },
        _ => error(404, "invalid_request_error", "not_found", format!("No route for {method} {path}.")),
    };
    Ok(resp)
}

async fn ask(state: &State, query: Query) -> Option<String> {
    let (tx, rx) = oneshot::channel();
    state.tx.send(Msg::Query(query, tx)).ok()?;
    rx.await.ok()
}

// ------------------------------------------------------------ transcription

/// Deletes a temporary file when it goes out of scope. This also runs when
/// the client disconnects and hyper drops the handler.
struct Temp(PathBuf);

impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Tells the scheduler that the client is gone, unless the job finished.
struct CancelOnDrop<'a> {
    tx: &'a UnboundedSender<Msg>,
    id: u64,
    armed: bool,
}

impl Drop for CancelOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.tx.send(Msg::Cancel(self.id));
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Json,
    Text,
    Verbose,
}

struct Upload {
    fields: HashMap<String, String>,
    file: Option<Temp>,
}

/// Reads the multipart body. The file part goes to a temporary file in
/// pieces; the body never sits in memory. The error is a ready response.
async fn read_upload(state: &State, req: Request<Incoming>, id: u64) -> Result<Upload, Resp> {
    let cfg = &state.cfg;
    let max_bytes = cfg.server.max_upload_mb * 1024 * 1024;
    let too_large = || error(413, "invalid_request_error", "file_too_large", format!("The upload is larger than {} MB.", cfg.server.max_upload_mb));
    let header = |name| req.headers().get(name).and_then(|v: &HeaderValue| v.to_str().ok()).map(str::to_string);
    if header(CONTENT_LENGTH.as_str()).and_then(|v| v.parse::<u64>().ok()).is_some_and(|n| n > max_bytes) {
        return Err(too_large());
    }
    let boundary = header(CONTENT_TYPE.as_str())
        .and_then(|v| multer::parse_boundary(&v).ok())
        .ok_or_else(|| bad("invalid_content_type", "Send multipart/form-data with a `file` part."))?;
    let mut multipart = multer::Multipart::new(req.into_body().into_data_stream(), boundary);
    let mut up = Upload { fields: HashMap::new(), file: None };
    let mut total = 0u64;
    let broken = |e: multer::Error| bad("invalid_multipart", format!("The upload is not valid multipart data: {e}"));
    while let Some(mut field) = multipart.next_field().await.map_err(broken)? {
        let name = field.name().unwrap_or("").to_string();
        if name == "file" {
            let path = cfg.tmp_dir().join(format!("ssw-{id}.in"));
            let mut sink = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&path)
                .map_err(|e| error(500, "server_error", "tmp_failed", format!("Temporary file failed: {e}")))?;
            up.file = Some(Temp(path));
            while let Some(chunk) = field.chunk().await.map_err(broken)? {
                total += chunk.len() as u64;
                if total > max_bytes {
                    return Err(too_large());
                }
                sink.write_all(&chunk).map_err(|e| error(500, "server_error", "tmp_failed", format!("Write failed: {e}")))?;
            }
        } else {
            let mut value = Vec::new();
            while let Some(chunk) = field.chunk().await.map_err(broken)? {
                total += chunk.len() as u64;
                value.extend_from_slice(&chunk);
                if value.len() > cfg.server.field_max_kb * 1024 {
                    return Err(bad("field_too_large", format!("The field `{name}` is larger than {} KB.", cfg.server.field_max_kb)));
                }
            }
            up.fields.insert(name, String::from_utf8_lossy(&value).into_owned());
        }
    }
    Ok(up)
}

fn model_not_found(cfg: &Config, model: &str) -> Resp {
    let ids = cfg.model_ids();
    json_reply(
        404,
        &json!({
            "error": {
                "message": format!("The model `{model}` does not exist. Available models: {}.", ids.join(", ")),
                "type": "invalid_request_error", "param": "model", "code": "model_not_found",
            },
            "detail": format!("The model `{model}` does not exist. Available models: {}.", ids.join(", ")),
            "available_models": ids,
        }),
    )
}

/// Returns the response, the model id and the class, for the request counter.
async fn transcribe(state: &State, req: Request<Incoming>, who: &Who) -> (Resp, String, u8) {
    let cfg = &state.cfg;
    let id = state.next_id.fetch_add(1, Ordering::Relaxed);
    let priority = req.headers().get("x-priority").and_then(|v| v.to_str().ok()).map(|v| v.trim().to_lowercase());
    let mut class = who.class;
    let mut model_id = "-".to_string();
    macro_rules! out {
        ($resp:expr) => {
            return ($resp, model_id, class)
        };
    }

    let upload = match read_upload(state, req, id).await {
        Ok(u) => u,
        Err(resp) => out!(resp),
    };
    let Some(input) = upload.file else { out!(bad("missing_file", "The `file` part is necessary.")) };
    let fields = upload.fields;

    // Model: an unknown id is 404 with the list. No `model` field gives the default.
    let idx = match fields.get("model").map(|m| m.trim()).filter(|m| !m.is_empty()) {
        Some(model) => match cfg.engine_index(model) {
            Some(idx) => idx,
            None => out!(model_not_found(cfg, model)),
        },
        None => cfg.server.default_model.as_deref().and_then(|m| cfg.engine_index(m)).unwrap_or(0),
    };
    let engine = &cfg.engines[idx];
    model_id = engine.id.clone();

    let format = match fields.get("response_format").map(|f| f.trim().to_lowercase()).as_deref() {
        None | Some("") | Some("json") => Format::Json,
        Some("text") => Format::Text,
        Some("verbose_json") => Format::Verbose,
        Some(f @ ("srt" | "vtt")) => out!(bad("unsupported_response_format", format!("response_format `{f}` needs timestamps (planned). Use json, text or verbose_json."))),
        Some("diarized_json") => out!(bad("unsupported_response_format", "response_format `diarized_json` is not supported.")),
        Some(other) => {
            log!("job {id}: unknown response_format `{other}`; the hub uses json");
            Format::Json
        }
    };
    let stream = fields.get("stream").is_some_and(|s| s.trim().eq_ignore_ascii_case("true"));

    // Language: one form for all engines. An engine with a language list gets `auto` for the rest.
    let language = normalize::language(fields.get("language").map(String::as_str), &cfg.languages.extra)
        .filter(|code| engine.languages.is_empty() || engine.languages.iter().any(|l| l == code));
    let prompt = fields.get("prompt").map(|p| p.trim().to_string()).filter(|p| !p.is_empty());

    // Audio: any format to 16 kHz mono WAV.
    let wav = Temp(cfg.tmp_dir().join(format!("ssw-{id}.wav")));
    let decoded = {
        let _permit = state.ffmpeg.acquire().await;
        normalize::transcode(&cfg.server.ffmpeg, &input.0, &wav.0, Duration::from_secs(cfg.server.ffmpeg_timeout_s)).await
    };
    drop(input);
    if let Err(why) = decoded {
        if why.starts_with("ffmpeg did not start") || why.starts_with("ffmpeg failed") {
            log!("job {id}: {why}");
            out!(error(500, "server_error", "ffmpeg_failed", why));
        }
        out!(bad("invalid_audio", format!("The audio cannot be decoded: {why}")));
    }
    let data = match normalize::wav_data(&wav.0) {
        Ok(d) if d.len > 0 => d,
        _ => out!(bad("invalid_audio", "The audio has no samples.")),
    };
    let seconds = data.seconds();

    // Class: a client can lower its class with X-Priority. Long audio is batch.
    match priority.as_deref() {
        Some("batch") => class = class.max(2),
        Some("interactive") => class = class.max(1),
        _ => {}
    }
    if seconds > cfg.scheduler.batch_if_audio_longer_s {
        class = 2;
    }

    let (reply_tx, reply_rx) = oneshot::channel();
    let run = Run {
        job: id,
        wav: wav.0.clone(),
        data_offset: data.offset,
        data_len: data.len,
        language: language.clone(),
        prompt,
    };
    let job = Job { id, engine: idx, class, key: who.name.clone(), allow_fallback: who.allow_fallback, run, reply: reply_tx };
    if state.tx.send(Msg::Submit(job)).is_err() {
        out!(error(503, "server_error", "shutdown", "The hub stops."));
    }
    let mut guard = CancelOnDrop { tx: &state.tx, id, armed: true };
    let result = reply_rx.await;
    guard.armed = false;
    let done = match result {
        Ok(Ok(done)) => done,
        Ok(Err(reject)) => {
            let kind = if reject.status >= 500 { "server_error" } else { "invalid_request_error" };
            let mut resp = error(reject.status, kind, reject.code, reject.message);
            if let Some(s) = reject.retry_after {
                resp.headers_mut().insert(RETRY_AFTER, HeaderValue::from(s));
            }
            out!(resp)
        }
        Err(_) => out!(error(500, "server_error", "scheduler_gone", "The scheduler dropped the job.")),
    };
    model_id = done.model.clone();
    let heard = language.or(done.output.language.clone());
    let text = done.output.text.clone();
    let seconds = (seconds * 1000.0).round() / 1000.0;
    let mut resp = if stream {
        let delta = json!({"type": "transcript.text.delta", "delta": text});
        let end = json!({"type": "transcript.text.done", "text": text});
        reply(200, "text/event-stream", format!("data: {delta}\n\ndata: {end}\n\ndata: [DONE]\n\n"))
    } else {
        match format {
            Format::Text => reply(200, "text/plain; charset=utf-8", text),
            Format::Json => {
                let mut body = json!({"text": text, "duration": seconds, "usage": {"type": "duration", "seconds": seconds}});
                if let Some(code) = &heard {
                    body["language"] = json!(code);
                }
                json_reply(200, &body)
            }
            Format::Verbose => {
                let mut body = json!({
                    "task": "transcribe", "duration": seconds, "text": text,
                    "usage": {"type": "duration", "seconds": seconds},
                });
                if let Some(code) = &heard {
                    body["language"] = json!(code);
                }
                if !done.output.segments.is_empty() {
                    body["segments"] = done.output.segments.iter().enumerate().map(|(i, s)| json!({
                        "id": i, "seek": 0, "start": s.start, "end": s.end, "text": s.text,
                        "tokens": [], "temperature": 0, "avg_logprob": 0, "compression_ratio": 0, "no_speech_prob": 0,
                    })).collect();
                }
                json_reply(200, &body)
            }
        }
    };
    let headers = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(&done.model) {
        headers.insert("x-speech-swap-model", v);
    }
    headers.insert("x-speech-swap-wait-ms", HeaderValue::from(done.wait_ms));
    headers.insert("x-speech-swap-run-ms", HeaderValue::from(done.run_ms));
    headers.insert("x-speech-swap-preempted", HeaderValue::from(done.preempted));
    (resp, model_id, class)
}
