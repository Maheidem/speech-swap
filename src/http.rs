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
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{HeaderMap, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{oneshot, Semaphore};

use crate::config::{Adapter, Config, Route};
use crate::engine::{Output, Run};
use crate::normalize::{self, WavData};
use crate::pipeline::{self, Cue, Word, WindowResult};
use crate::sched::{Done, Job, Msg, Query, Reject};

type Resp = Response<Full<Bytes>>;

pub struct State {
    pub cfg: Arc<Config>,
    pub tx: UnboundedSender<Msg>,
    next_id: AtomicU64,
    ffmpeg: Semaphore,
}

impl State {
    pub fn new(cfg: Arc<Config>, tx: UnboundedSender<Msg>) -> State {
        let ffmpeg = Semaphore::new(cfg.server.ffmpeg_parallel.max(1));
        State { cfg, tx, next_id: AtomicU64::new(1), ffmpeg }
    }

    /// A new id for a job, a request or a temporary file name.
    pub fn new_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
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

pub struct Who {
    pub name: String,
    pub class: u8,
    pub allow_fallback: bool,
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
        // Legacy aligner API: its clients read `uptime_s` here as a health check.
        (&Method::GET, "/status") => match authenticate(&state.cfg, req.headers()) {
            Err(resp) => legacy_error(resp.status().as_u16(), "unauthorized"),
            Ok(_) => match ask(&state, Query::AlignStatus).await {
                Some(body) => reply(200, "application/json", body),
                None => legacy_error(503, "scheduler_stuck"),
            },
        },
        (&Method::POST, "/v1/audio/alignments" | "/align") => {
            let legacy = path == "/align";
            match authenticate(&state.cfg, req.headers()) {
                Err(resp) if legacy => legacy_error(resp.status().as_u16(), "unauthorized"),
                Err(resp) => resp,
                Ok(who) => {
                    let (resp, model, class) = align(&state, req, &who, legacy).await;
                    let resp = if legacy { legacy_shape(resp).await } else { resp };
                    let code = resp.status().as_u16();
                    let _ = state.tx.send(Msg::Record { model, key: who.name, class, code });
                    resp
                }
            }
        }
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
pub struct Temp(pub PathBuf);

impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Tells the scheduler that the client is gone: it cancels all jobs of the
/// request. A job that is finished is not found, and the cancel does nothing.
struct Cancels<'a> {
    tx: &'a UnboundedSender<Msg>,
    ids: Vec<u64>,
    armed: bool,
}

impl Drop for Cancels<'_> {
    fn drop(&mut self) {
        if self.armed {
            for id in &self.ids {
                let _ = self.tx.send(Msg::Cancel(*id));
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Json,
    Text,
    Verbose,
    Srt,
    Vtt,
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
            let value = String::from_utf8_lossy(&value).into_owned();
            // A field like `timestamp_granularities[]` can repeat: keep all values.
            match up.fields.get_mut(&name) {
                Some(old) if name.ends_with("[]") => {
                    old.push(',');
                    old.push_str(&value);
                }
                _ => {
                    up.fields.insert(name, value);
                }
            }
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

/// The error body of the legacy `/align` route: `{"error": "text"}`.
fn legacy_error(status: u16, message: &str) -> Resp {
    json_reply(status, &json!({"error": message, "detail": message}))
}

/// Changes an error body of the hub into the legacy form `{"error": "text"}`.
async fn legacy_shape(resp: Resp) -> Resp {
    if resp.status().is_success() {
        return resp;
    }
    let (parts, body) = resp.into_parts();
    let bytes = body.collect().await.map(|b| b.to_bytes()).unwrap_or_default();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    if v["error"].is_string() {
        return Response::from_parts(parts, Full::new(bytes));
    }
    let message = v["detail"].as_str().unwrap_or("request failed").to_string();
    let mut out = legacy_error(parts.status.as_u16(), &message);
    if let Some(retry) = parts.headers.get(RETRY_AFTER) {
        out.headers_mut().insert(RETRY_AFTER, retry.clone());
    }
    out
}

/// Converts any audio file to the 16 kHz mono WAV of the hub.
pub async fn to_wav(state: &State, input: Temp, id: u64) -> Result<(Temp, WavData), Resp> {
    let cfg = &state.cfg;
    let wav = Temp(cfg.tmp_dir().join(format!("ssw-{id}.wav")));
    let decoded = {
        let _permit = state.ffmpeg.acquire().await;
        normalize::transcode(&cfg.server.ffmpeg, &input.0, &wav.0, Duration::from_secs(cfg.server.ffmpeg_timeout_s)).await
    };
    drop(input);
    if let Err(why) = decoded {
        if why.starts_with("ffmpeg did not start") || why.starts_with("ffmpeg failed") {
            log!("job {id}: {why}");
            return Err(error(500, "server_error", "ffmpeg_failed", why));
        }
        return Err(bad("invalid_audio", format!("The audio cannot be decoded: {why}")));
    }
    match normalize::wav_data(&wav.0) {
        Ok(d) if d.len > 0 => Ok((wav, d)),
        _ => Err(bad("invalid_audio", "The audio has no samples.")),
    }
}

/// Puts one job in the scheduler queue. The receiver gets the answer.
#[allow(clippy::too_many_arguments)]
pub fn submit(
    state: &State,
    who: &Who,
    engine: usize,
    class: u8,
    aging: bool,
    run: Run,
) -> Result<oneshot::Receiver<Result<Done, Reject>>, Resp> {
    let (reply, rx) = oneshot::channel();
    let job = Job { id: run.job, engine, class, key: who.name.clone(), allow_fallback: who.allow_fallback, aging, run, reply };
    state.tx.send(Msg::Submit(job)).map_err(|_| error(503, "server_error", "shutdown", "The hub stops."))?;
    Ok(rx)
}

fn reject_resp(reject: Reject) -> Resp {
    let kind = if reject.status >= 500 { "server_error" } else { "invalid_request_error" };
    let mut resp = error(reject.status, kind, reject.code, reject.message);
    if let Some(s) = reject.retry_after {
        resp.headers_mut().insert(RETRY_AFTER, HeaderValue::from(s));
    }
    resp
}

/// Waits for the answer of one job.
pub async fn wait(rx: oneshot::Receiver<Result<Done, Reject>>) -> Result<Done, Resp> {
    match rx.await {
        Ok(Ok(done)) => Ok(done),
        Ok(Err(reject)) => Err(reject_resp(reject)),
        Err(_) => Err(error(500, "server_error", "scheduler_gone", "The scheduler dropped the job.")),
    }
}

/// The result of a transcription, from one engine or from a pipeline.
struct Transcript {
    text: String,
    language: Option<String>,
    segments: Vec<Cue>,
    words: Vec<Word>,
    model: String,
    wait_ms: u64,
    run_ms: u64,
    preempted: u32,
    windows: Option<usize>,
}

/// Returns the response, the model id and the class, for the request counter.
async fn transcribe(state: &State, req: Request<Incoming>, who: &Who) -> (Resp, String, u8) {
    let cfg = &state.cfg;
    let id = state.new_id();
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
    let route = match fields.get("model").map(|m| m.trim()).filter(|m| !m.is_empty()) {
        Some(model) => match cfg.resolve(model) {
            Some(route) => route,
            None => out!(model_not_found(cfg, model)),
        },
        None => cfg.server.default_model.as_deref().and_then(|m| cfg.resolve(m)).unwrap_or(Route::Engine(0)),
    };

    let format = match fields.get("response_format").map(|f| f.trim().to_lowercase()).as_deref() {
        None | Some("") | Some("json") => Format::Json,
        Some("text") => Format::Text,
        Some("verbose_json") => Format::Verbose,
        Some("srt") => Format::Srt,
        Some("vtt") => Format::Vtt,
        Some("diarized_json") => out!(bad("unsupported_response_format", "response_format `diarized_json` is not supported.")),
        Some(other) => {
            log!("job {id}: unknown response_format `{other}`; the hub uses json");
            Format::Json
        }
    };
    let stream = fields.get("stream").is_some_and(|s| s.trim().eq_ignore_ascii_case("true"));
    let words_wanted = fields
        .get("timestamp_granularities[]")
        .or_else(|| fields.get("timestamp_granularities"))
        .is_some_and(|g| g.split(',').any(|v| v.trim().eq_ignore_ascii_case("word")));

    // A pipeline model, or an engine with an aligner when words or subtitles are wanted.
    let (idx, align) = match route {
        Route::Pipeline { stt, align, id: pid } => {
            model_id = pid;
            (stt, Some(align))
        }
        Route::Engine(i) => {
            model_id = cfg.engines[i].id.clone();
            if cfg.engines[i].adapter == Adapter::HttpAlign {
                out!(bad("wrong_model_kind", format!("`{}` is an aligner. Use /v1/audio/alignments.", cfg.engines[i].id)));
            }
            match cfg.implicit_pipeline(i) {
                Some((stt, align)) if words_wanted || matches!(format, Format::Srt | Format::Vtt) => (stt, Some(align)),
                _ => (i, None),
            }
        }
    };
    let engine = &cfg.engines[idx];

    // Language: one form for all engines. An engine with a language list gets `auto` for the rest.
    let language = normalize::language(fields.get("language").map(String::as_str), &cfg.languages.extra)
        .filter(|code| engine.languages.is_empty() || engine.languages.iter().any(|l| l == code));
    let prompt = fields.get("prompt").map(|p| p.trim().to_string()).filter(|p| !p.is_empty());

    // Audio: any format to 16 kHz mono WAV.
    let (wav, data) = match to_wav(state, input, id).await {
        Ok(v) => v,
        Err(resp) => out!(resp),
    };
    let seconds = data.seconds();

    // Class: a client can lower its class with X-Priority. Long audio is batch.
    match priority.as_deref() {
        Some("batch") => class = class.max(2),
        Some("interactive") => class = class.max(1),
        _ => {}
    }
    if seconds > cfg.scheduler.batch_if_audio_longer_s || align.is_some() {
        class = 2;
    }

    let result = match align {
        Some(align) => run_pipeline(state, who, id, &wav, data, (idx, align), language.clone(), prompt, class).await,
        None => {
            let run = Run { job: id, wav: wav.0.clone(), data_offset: data.offset, data_len: data.len, language: language.clone(), prompt, text: None };
            let rx = match submit(state, who, idx, class, true, run) {
                Ok(rx) => rx,
                Err(resp) => out!(resp),
            };
            let mut guard = Cancels { tx: &state.tx, ids: vec![id], armed: true };
            let result = wait(rx).await;
            guard.armed = false;
            result.map(|done| plain_transcript(done, seconds))
        }
    };
    let t = match result {
        Ok(t) => t,
        Err(resp) => out!(resp),
    };
    model_id = t.model.clone();
    let heard = language.or(t.language.clone());
    let seconds = (seconds * 1000.0).round() / 1000.0;
    let mut resp = if stream {
        let delta = json!({"type": "transcript.text.delta", "delta": t.text});
        let end = json!({"type": "transcript.text.done", "text": t.text});
        reply(200, "text/event-stream", format!("data: {delta}\n\ndata: {end}\n\ndata: [DONE]\n\n"))
    } else {
        match format {
            Format::Text => reply(200, "text/plain; charset=utf-8", t.text.clone()),
            Format::Srt => reply(200, "text/plain; charset=utf-8", pipeline::srt(&cues_of(&t, seconds))),
            Format::Vtt => reply(200, "text/vtt; charset=utf-8", pipeline::vtt(&cues_of(&t, seconds))),
            Format::Json => {
                let mut body = json!({"text": t.text, "duration": seconds, "usage": {"type": "duration", "seconds": seconds}});
                if let Some(code) = &heard {
                    body["language"] = json!(code);
                }
                json_reply(200, &body)
            }
            Format::Verbose => {
                let mut body = json!({
                    "task": "transcribe", "duration": seconds, "text": t.text,
                    "usage": {"type": "duration", "seconds": seconds},
                });
                if let Some(code) = &heard {
                    body["language"] = json!(code);
                }
                if !t.segments.is_empty() {
                    body["segments"] = t.segments.iter().enumerate().map(|(i, s)| json!({
                        "id": i, "seek": 0, "start": s.start, "end": s.end, "text": s.text,
                        "tokens": [], "temperature": 0, "avg_logprob": 0, "compression_ratio": 0, "no_speech_prob": 0,
                    })).collect();
                }
                if !t.words.is_empty() {
                    body["words"] = t.words.iter().map(|w| json!({"word": w.text, "start": w.start, "end": w.end})).collect();
                }
                json_reply(200, &body)
            }
        }
    };
    let headers = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(&t.model) {
        headers.insert("x-speech-swap-model", v);
    }
    headers.insert("x-speech-swap-wait-ms", HeaderValue::from(t.wait_ms));
    headers.insert("x-speech-swap-run-ms", HeaderValue::from(t.run_ms));
    headers.insert("x-speech-swap-preempted", HeaderValue::from(t.preempted));
    if let Some(n) = t.windows {
        headers.insert("x-speech-swap-windows", HeaderValue::from(n as u64));
    }
    (resp, model_id, class)
}

fn plain_transcript(done: Done, _seconds: f64) -> Transcript {
    let Output { text, segments, language, .. } = done.output;
    Transcript {
        text,
        language,
        segments: segments.into_iter().map(|s| Cue { start: s.start, end: s.end, text: s.text }).collect(),
        words: Vec::new(),
        model: done.model,
        wait_ms: done.wait_ms,
        run_ms: done.run_ms,
        preempted: done.preempted,
        windows: None,
    }
}

/// The cues of an SRT or VTT file. An engine without segments gives one
/// cue for the whole audio.
fn cues_of(t: &Transcript, seconds: f64) -> Vec<Cue> {
    if !t.segments.is_empty() {
        return t.segments.clone();
    }
    if t.text.trim().is_empty() {
        return Vec::new();
    }
    vec![Cue { start: 0.0, end: seconds, text: t.text.clone() }]
}

// ----------------------------------------------------------------- pipeline

/// The pipeline STT to align (design 5.6). Phase 1: all windows go to the
/// STT engine. Phase 2: all windows with text go to the aligner. The
/// scheduler swaps the engines. The jobs of one phase are all in the queue
/// at the same time, so the engine of the phase stays busy and the scheduler
/// does not swap it away in the middle of the phase.
#[allow(clippy::too_many_arguments)]
async fn run_pipeline(
    state: &State,
    who: &Who,
    id: u64,
    wav: &Temp,
    data: WavData,
    (stt, align): (usize, usize),
    language: Option<String>,
    prompt: Option<String>,
    class: u8,
) -> Result<Transcript, Resp> {
    let cfg = &state.cfg;
    let frame = pipeline::RATE * cfg.chunking.frame_ms as usize / 1000;
    let total = (data.len / 2) as usize;
    let energy = pipeline::frame_energies(&wav.0, data.offset, data.len, frame)
        .map_err(|e| error(500, "server_error", "tmp_failed", format!("Cannot read the audio: {e}")))?;
    // Sample range of each window.
    let ranges: Vec<(usize, usize)> = pipeline::cut_windows(&energy, &cfg.chunking)
        .into_iter()
        .map(|(a, b)| (a * frame, if b >= energy.len() { total } else { b * frame }))
        .collect();
    let slice = |k: usize, text: Option<String>, language: Option<String>, prompt: Option<String>| {
        let (a, b) = ranges[k];
        Run {
            job: state.new_id(),
            wav: wav.0.clone(),
            data_offset: data.offset + a as u64 * 2,
            data_len: (b - a) as u64 * 2,
            language,
            prompt,
            text,
        }
    };
    let mut guard = Cancels { tx: &state.tx, ids: Vec::new(), armed: true };
    let (mut wait_ms, mut run_ms, mut preempted) = (0u64, 0u64, 0u32);

    // Phase 1: transcribe all windows.
    let mut pending = Vec::new();
    for k in 0..ranges.len() {
        let run = slice(k, None, language.clone(), prompt.clone());
        guard.ids.push(run.job);
        pending.push(submit(state, who, stt, class, false, run)?);
    }
    let mut results: Vec<WindowResult> = Vec::new();
    for (k, rx) in pending.into_iter().enumerate() {
        let done = wait(rx).await?;
        wait_ms += done.wait_ms;
        run_ms += done.run_ms;
        preempted += done.preempted;
        results.push(WindowResult {
            start_s: ranges[k].0 as f64 / pipeline::RATE as f64,
            text: done.output.text,
            language: done.output.language,
            words: Vec::new(),
        });
    }
    log!("job {id}: pipeline phase 1 done ({} windows)", results.len());

    // Phase 2: align each window that has text.
    let texts: Vec<&str> = results.iter().map(|r| r.text.as_str()).collect();
    let steps = pipeline::align_steps(&texts);
    let mut pending = Vec::new();
    for &k in &steps {
        let lang = language
            .clone()
            .or_else(|| normalize::language(results[k].language.as_deref(), &cfg.languages.extra));
        if lang.is_none() {
            return Err(bad("language_unknown", "The engine did not tell the language. Send the `language` field."));
        }
        let run = slice(k, Some(results[k].text.clone()), lang, None);
        guard.ids.push(run.job);
        pending.push((k, submit(state, who, align, class, false, run)?));
    }
    for (k, rx) in pending {
        let done = wait(rx).await?;
        wait_ms += done.wait_ms;
        run_ms += done.run_ms;
        preempted += done.preempted;
        results[k].words = done.output.words;
    }
    guard.armed = false;
    log!("job {id}: pipeline phase 2 done ({} windows aligned)", steps.len());

    // Merge: one time line, words and cues.
    let (text, words) = pipeline::merge(&results);
    let mut segments = Vec::new();
    for r in &results {
        if r.text.trim().is_empty() {
            continue;
        }
        let (_, shifted) = pipeline::merge(std::slice::from_ref(r));
        if shifted.is_empty() {
            let end = r.start_s + 0.0;
            segments.push(Cue { start: r.start_s, end, text: r.text.trim().to_string() });
            continue;
        }
        let display = pipeline::tokens_with_marks(&r.text, &shifted).unwrap_or_else(|| shifted.iter().map(|w| w.text.clone()).collect());
        segments.extend(pipeline::cues(&shifted, &display));
    }
    let detected = results.iter().find_map(|r| r.language.clone());
    Ok(Transcript {
        text,
        language: language.or(detected),
        segments,
        words,
        model: cfg.engines[stt].id.clone(),
        wait_ms,
        run_ms,
        preempted,
        windows: Some(ranges.len()),
    })
}

// ---------------------------------------------------------------- alignment

/// `POST /v1/audio/alignments` (multipart) and the legacy `POST /align` (JSON).
async fn align(state: &State, req: Request<Incoming>, who: &Who, legacy: bool) -> (Resp, String, u8) {
    let cfg = &state.cfg;
    let id = state.new_id();
    let priority = req.headers().get("x-priority").and_then(|v| v.to_str().ok()).map(|v| v.trim().to_lowercase());
    let mut class = who.class;
    let mut model_id = "-".to_string();
    macro_rules! out {
        ($resp:expr) => {
            return ($resp, model_id, class)
        };
    }
    let (input, fields) = if legacy {
        match read_legacy(state, req, id).await {
            Ok(v) => v,
            Err(resp) => out!(resp),
        }
    } else {
        match read_upload(state, req, id).await {
            Ok(up) => match up.file {
                Some(file) => (file, up.fields),
                None => out!(bad("missing_file", "The `file` part is necessary.")),
            },
            Err(resp) => out!(resp),
        }
    };

    // The aligner: the `model` field, or the first aligner engine.
    let idx = match fields.get("model").map(|m| m.trim()).filter(|m| !m.is_empty()) {
        Some(model) => match cfg.engine_index(model) {
            Some(i) if cfg.engines[i].adapter == Adapter::HttpAlign => i,
            Some(_) => out!(bad("wrong_model_kind", format!("`{model}` is not an aligner."))),
            None => out!(model_not_found(cfg, model)),
        },
        None => match cfg.engines.iter().position(|e| e.adapter == Adapter::HttpAlign) {
            Some(i) => i,
            None => out!(error(404, "invalid_request_error", "no_aligner", "The hub has no aligner engine.")),
        },
    };
    model_id = cfg.engines[idx].id.clone();
    let text = fields.get("text").map(|t| t.trim().to_string()).unwrap_or_default();
    if text.is_empty() {
        out!(bad("missing_text", "The `text` field is necessary."));
    }
    let Some(language) = normalize::language(fields.get("language").map(String::as_str), &cfg.languages.extra) else {
        out!(bad("missing_language", "The `language` field is necessary (for example `pt`)."))
    };

    let (wav, data) = match to_wav(state, input, id).await {
        Ok(v) => v,
        Err(resp) => out!(resp),
    };
    let seconds = data.seconds();
    // The aligner takes at most 31 s. The hub cannot split the text of a longer file.
    if seconds > 31.0 {
        out!(bad("audio_too_long", format!("The audio is {seconds:.1} s. The aligner takes at most 31 s.")));
    }
    match priority.as_deref() {
        Some("batch") => class = class.max(2),
        Some("interactive") => class = class.max(1),
        _ => {}
    }
    let run = Run { job: id, wav: wav.0.clone(), data_offset: data.offset, data_len: data.len, language: Some(language.clone()), prompt: None, text: Some(text.clone()) };
    let rx = match submit(state, who, idx, class, true, run) {
        Ok(rx) => rx,
        Err(resp) => out!(resp),
    };
    let mut guard = Cancels { tx: &state.tx, ids: vec![id], armed: true };
    let result = wait(rx).await;
    guard.armed = false;
    let done = match result {
        Ok(d) => d,
        Err(resp) => out!(resp),
    };
    let words = &done.output.words;
    let mut resp = if legacy {
        let list: Vec<Value> = words.iter().map(|w| json!({"text": w.text, "start": w.start, "end": w.end})).collect();
        json_reply(200, &json!({"words": list, "model": done.model, "ms": done.run_ms}))
    } else {
        let list: Vec<Value> = words
            .iter()
            .map(|w| json!({
                "word": w.text, "start": w.start, "end": w.end,
                "start_sample": (w.start * 16000.0).round() as u64, "end_sample": (w.end * 16000.0).round() as u64,
                "confidence": null,
            }))
            .collect();
        let rtf = if seconds > 0.0 { done.run_ms as f64 / 1000.0 / seconds } else { 0.0 };
        json_reply(200, &json!({
            "text": text, "language": language, "words": list,
            "timing": {"wall_ms": done.run_ms, "audio_duration_ms": (seconds * 1000.0).round() as u64, "rtf": (rtf * 1000.0).round() / 1000.0},
        }))
    };
    let headers = resp.headers_mut();
    headers.insert("x-speech-swap-wait-ms", HeaderValue::from(done.wait_ms));
    headers.insert("x-speech-swap-run-ms", HeaderValue::from(done.run_ms));
    (resp, model_id, class)
}

/// Reads the JSON body of the legacy `/align` route. The audio goes to a
/// temporary file. The fields are `text`, `language` and `model`.
async fn read_legacy(state: &State, req: Request<Incoming>, id: u64) -> Result<(Temp, HashMap<String, String>), Resp> {
    let cfg = &state.cfg;
    let max_bytes = (cfg.server.max_upload_mb * 1024 * 1024) as usize;
    let too_large = || legacy_error(413, &format!("The body is larger than {} MB.", cfg.server.max_upload_mb));
    let body = Limited::new(req.into_body(), max_bytes).collect().await.map_err(|_| too_large())?.to_bytes();
    let v: Value = serde_json::from_slice(&body).map_err(|_| legacy_error(400, "body is not JSON"))?;
    let audio = v["audio_b64"].as_str().and_then(normalize::base64_decode).filter(|a| !a.is_empty());
    let Some(audio) = audio else { return Err(legacy_error(400, "audio_b64 is required and must be valid base64")) };
    let path = cfg.tmp_dir().join(format!("ssw-{id}.in"));
    let mut sink = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&path)
        .map_err(|e| legacy_error(500, &format!("Temporary file failed: {e}")))?;
    let temp = Temp(path);
    sink.write_all(&audio).map_err(|e| legacy_error(500, &format!("Write failed: {e}")))?;
    let mut fields = HashMap::new();
    for key in ["text", "language", "model"] {
        if let Some(s) = v[key].as_str() {
            fields.insert(key.to_string(), s.to_string());
        }
    }
    Ok((temp, fields))
}
