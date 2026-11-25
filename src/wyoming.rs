//! A Wyoming STT server (the protocol of Home Assistant voice).
//!
//! Framing: one JSON header line that ends with `\n`. If the header has
//! `data_length`, that many bytes of JSON follow, and the hub merges them
//! into `data`. If it has `payload_length`, that many bytes of audio follow.
//! Each port of the config serves one model. Every job has class 0.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::config::{Adapter, Wyoming};
use crate::engine::{wav_header, Run};
use crate::http::{self, State, Temp, Who};
use crate::normalize::{self, WavData};
use crate::sched::Msg;

/// The longest header line, the longest data block and the longest audio chunk.
const MAX_HEADER: usize = 64 * 1024;
const MAX_DATA: usize = 1024 * 1024;
const MAX_PAYLOAD: usize = 8 * 1024 * 1024;
/// A connection that sends nothing for this time is closed.
const IDLE: Duration = Duration::from_secs(300);

#[derive(Debug, PartialEq)]
pub struct Event {
    pub kind: String,
    pub data: Value,
    pub payload: Vec<u8>,
}

/// Reads one event. `Ok(None)` means the peer closed the connection.
pub async fn read_event<R: AsyncBufRead + Unpin>(reader: &mut R) -> std::io::Result<Option<Event>> {
    let bad = |why: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, why.to_string());
    let mut line = Vec::new();
    loop {
        line.clear();
        let n = (&mut *reader).take(MAX_HEADER as u64 + 1).read_until(b'\n', &mut line).await?;
        if n == 0 {
            return Ok(None);
        }
        if line.len() > MAX_HEADER {
            return Err(bad("the header line is too long"));
        }
        if !line.iter().all(u8::is_ascii_whitespace) {
            break;
        }
    }
    let header: Value = serde_json::from_slice(&line).map_err(|_| bad("the header is not JSON"))?;
    let kind = header["type"].as_str().ok_or_else(|| bad("the header has no type"))?.to_string();
    let mut data = header.get("data").filter(|d| d.is_object()).cloned().unwrap_or_else(|| json!({}));
    let data_length = header["data_length"].as_u64().unwrap_or(0) as usize;
    if data_length > MAX_DATA {
        return Err(bad("the data block is too long"));
    }
    if data_length > 0 {
        let mut buf = vec![0u8; data_length];
        reader.read_exact(&mut buf).await?;
        let extra: Value = serde_json::from_slice(&buf).map_err(|_| bad("the data block is not JSON"))?;
        if let (Some(base), Some(more)) = (data.as_object_mut(), extra.as_object()) {
            base.extend(more.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
    }
    let payload_length = header["payload_length"].as_u64().unwrap_or(0) as usize;
    if payload_length > MAX_PAYLOAD {
        return Err(bad("the audio payload is too long"));
    }
    let mut payload = vec![0u8; payload_length];
    reader.read_exact(&mut payload).await?;
    Ok(Some(Event { kind, data, payload }))
}

/// The bytes of one event.
pub fn encode_event(kind: &str, data: &Value, payload: &[u8]) -> Vec<u8> {
    let data_bytes = if data.as_object().is_some_and(|o| !o.is_empty()) { data.to_string().into_bytes() } else { Vec::new() };
    let mut header = json!({"type": kind});
    if !data_bytes.is_empty() {
        header["data_length"] = json!(data_bytes.len());
    }
    if !payload.is_empty() {
        header["payload_length"] = json!(payload.len());
    }
    let mut out = header.to_string().into_bytes();
    out.push(b'\n');
    out.extend_from_slice(&data_bytes);
    out.extend_from_slice(payload);
    out
}

async fn write_event<W: AsyncWrite + Unpin>(w: &mut W, kind: &str, data: &Value) -> std::io::Result<()> {
    w.write_all(&encode_event(kind, data, &[])).await?;
    w.flush().await
}

/// The `info` event: the models of the hub and the languages of the port.
fn info(state: &State, port: &Wyoming) -> Value {
    let attribution = json!({"name": "speech-swap", "url": "https://github.com/Maheidem/speech-swap"});
    let version = env!("CARGO_PKG_VERSION");
    let models: Vec<Value> = state
        .cfg
        .engines
        .iter()
        .filter(|e| e.adapter != Adapter::HttpAlign)
        .map(|e| json!({
            "name": e.id, "description": e.id, "attribution": attribution, "installed": true,
            "languages": port.languages, "version": version,
        }))
        .collect();
    json!({
        "asr": [{
            "name": format!("speech-swap {}", port.model), "description": "speech-swap hub",
            "attribution": attribution, "installed": true, "version": version,
            "supports_transcript_streaming": false, "models": models,
        }],
        "tts": [], "handle": [], "intent": [], "wake": [], "mic": [], "snd": [],
    })
}

/// The audio of one request.
#[derive(Default)]
struct Session {
    name: Option<String>,
    language: Option<String>,
    rate: u32,
    width: u32,
    channels: u32,
    pcm: Vec<u8>,
}

/// Listens on one port until the process ends.
pub async fn serve(state: Arc<State>, listener: TcpListener, port: Wyoming) {
    let port = Arc::new(port);
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(connection(state.clone(), stream, port.clone()));
            }
            Err(e) => {
                log!("wyoming {}: accept failed: {e}", port.listen);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn connection(state: Arc<State>, stream: TcpStream, port: Arc<Wyoming>) {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut session = Session { rate: 16000, width: 2, channels: 1, ..Session::default() };
    let max_bytes = state.cfg.server.max_upload_mb as usize * 1024 * 1024;
    loop {
        let event = match tokio::time::timeout(IDLE, read_event(&mut reader)).await {
            Ok(Ok(Some(event))) => event,
            Ok(Ok(None)) | Err(_) => return,
            Ok(Err(e)) => {
                log!("wyoming {}: {e}", port.listen);
                return;
            }
        };
        let d = &event.data;
        let sent = match event.kind.as_str() {
            "describe" => write_event(&mut write, "info", &info(&state, &port)).await,
            "transcribe" => {
                session.name = d["name"].as_str().map(str::to_string);
                session.language = d["language"].as_str().map(str::to_string);
                Ok(())
            }
            "audio-start" | "audio-chunk" => {
                if event.kind == "audio-start" {
                    session.pcm.clear();
                }
                for (field, slot) in [("rate", &mut session.rate), ("width", &mut session.width), ("channels", &mut session.channels)] {
                    if let Some(v) = d[field].as_u64().filter(|v| *v > 0) {
                        *slot = v as u32;
                    }
                }
                if session.pcm.len() + event.payload.len() <= max_bytes {
                    session.pcm.extend_from_slice(&event.payload);
                }
                Ok(())
            }
            "audio-stop" => {
                let text = transcribe(&state, &port, &session).await;
                session.pcm.clear();
                write_event(&mut write, "transcript", &json!({"text": text})).await
            }
            _ => Ok(()), // The hub ignores events that it does not know.
        };
        if sent.is_err() {
            return;
        }
    }
}

/// Runs one transcription. A failure gives an empty text, as the old bridge did.
async fn transcribe(state: &State, port: &Wyoming, s: &Session) -> String {
    let cfg = &state.cfg;
    let model = s
        .name
        .as_deref()
        .and_then(|n| cfg.engine_index(n))
        .filter(|i| cfg.engines[*i].adapter != Adapter::HttpAlign)
        .or_else(|| cfg.engine_index(&port.model))
        .expect("checked at start");
    let engine = &cfg.engines[model];
    let who = Who { name: format!("wyoming:{}", port.listen), class: 0, allow_fallback: false };
    let record = |code: u16| {
        let _ = state.tx.send(Msg::Record { model: engine.id.clone(), key: who.name.clone(), class: 0, code });
    };
    let id = state.new_id();
    let (wav, data) = match to_wav(state, id, s).await {
        Ok(v) => v,
        Err(why) => {
            log!("wyoming job {id}: {why}");
            record(400);
            return String::new();
        }
    };
    if data.len == 0 {
        record(200);
        return String::new();
    }
    let language = normalize::language(s.language.as_deref(), &cfg.languages.extra)
        .filter(|code| engine.languages.is_empty() || engine.languages.iter().any(|l| l == code));
    let run = Run { job: id, wav: wav.0.clone(), data_offset: data.offset, data_len: data.len, language, prompt: None, text: None };
    let result = match http::submit(state, &who, model, 0, true, run) {
        Ok(rx) => {
            let mut guard = CancelGuard { state, id, armed: true };
            let r = http::wait(rx).await;
            guard.armed = false;
            r
        }
        Err(resp) => Err(resp),
    };
    match result {
        Ok(done) => {
            record(200);
            log!("wyoming job {id}: {} s of audio, wait {} ms, run {} ms", data.seconds() as u64, done.wait_ms, done.run_ms);
            done.output.text
        }
        Err(resp) => {
            log!("wyoming job {id}: failed with status {}", resp.status());
            record(resp.status().as_u16());
            String::new()
        }
    }
}

/// Cancels the job if the task is dropped before the answer comes.
struct CancelGuard<'a> {
    state: &'a State,
    id: u64,
    armed: bool,
}

impl Drop for CancelGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.state.tx.send(Msg::Cancel(self.id));
        }
    }
}

/// Makes the 16 kHz mono WAV of the PCM. A stream in this form needs no ffmpeg.
async fn to_wav(state: &State, id: u64, s: &Session) -> Result<(Temp, WavData), String> {
    let cfg = &state.cfg;
    let wav = Temp(cfg.tmp_dir().join(format!("ssw-{id}.wav")));
    if (s.rate, s.width, s.channels) == (16000, 2, 1) {
        let len = s.pcm.len() & !1;
        let mut bytes = wav_header(len as u32).to_vec();
        bytes.extend_from_slice(&s.pcm[..len]);
        std::fs::write(&wav.0, bytes).map_err(|e| format!("temporary file: {e}"))?;
        return Ok((wav, WavData { offset: 44, len: len as u64 }));
    }
    let raw = Temp(cfg.tmp_dir().join(format!("ssw-{id}.raw")));
    std::fs::write(&raw.0, &s.pcm).map_err(|e| format!("temporary file: {e}"))?;
    let timeout = Duration::from_secs(cfg.server.ffmpeg_timeout_s);
    normalize::transcode_raw(&cfg.server.ffmpeg, Path::new(&raw.0), &wav.0, s.rate, s.width, s.channels, timeout).await?;
    let data = normalize::wav_data(&wav.0).map_err(|e| e.to_string())?;
    Ok((wav, data))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read(bytes: &[u8]) -> std::io::Result<Option<Event>> {
        read_event(&mut BufReader::new(bytes)).await
    }

    #[tokio::test]
    async fn an_event_with_data_and_payload_round_trips() {
        let bytes = encode_event("audio-chunk", &json!({"rate": 16000, "width": 2, "channels": 1}), &[1, 2, 3, 4]);
        let e = read(&bytes).await.unwrap().unwrap();
        assert_eq!(e.kind, "audio-chunk");
        assert_eq!(e.data, json!({"rate": 16000, "width": 2, "channels": 1}));
        assert_eq!(e.payload, [1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn an_event_without_data_is_one_line() {
        let bytes = encode_event("describe", &json!({}), &[]);
        assert_eq!(bytes, b"{\"type\":\"describe\"}\n");
        let e = read(&bytes).await.unwrap().unwrap();
        assert_eq!((e.kind.as_str(), e.data, e.payload.len()), ("describe", json!({}), 0));
    }

    #[tokio::test]
    async fn inline_data_and_a_data_block_are_merged() {
        let block = br#"{"language":"pt-BR"}"#;
        let mut bytes = format!("{{\"type\":\"transcribe\",\"data\":{{\"name\":\"q\"}},\"data_length\":{}}}\n", block.len()).into_bytes();
        bytes.extend_from_slice(block);
        let e = read(&bytes).await.unwrap().unwrap();
        assert_eq!(e.data, json!({"name": "q", "language": "pt-BR"}));
    }

    #[tokio::test]
    async fn several_events_come_in_order_and_the_end_is_none() {
        let mut bytes = encode_event("audio-start", &json!({"rate": 16000}), &[]);
        bytes.extend(encode_event("audio-stop", &json!({}), &[]));
        let mut r = BufReader::new(&bytes[..]);
        assert_eq!(read_event(&mut r).await.unwrap().unwrap().kind, "audio-start");
        assert_eq!(read_event(&mut r).await.unwrap().unwrap().kind, "audio-stop");
        assert_eq!(read_event(&mut r).await.unwrap(), None);
    }

    #[tokio::test]
    async fn bad_input_is_an_error_not_a_panic() {
        assert!(read(b"not json\n").await.is_err());
        assert!(read(b"{\"nope\":1}\n").await.is_err());
        assert!(read(b"{\"type\":\"x\",\"payload_length\":99}\nshort").await.is_err());
        let huge = format!("{{\"type\":\"x\",\"payload_length\":{}}}\n", MAX_PAYLOAD + 1);
        assert!(read(huge.as_bytes()).await.is_err());
    }

    #[test]
    fn the_info_lists_the_stt_models_and_the_languages() {
        let text = "[server]\nlisten='127.0.0.1:1'\ntmp_dir='/tmp/x'\nopen=true\n\
            [[engines]]\nid='a'\nadapter='jsonl'\ncmd=['x']\ncost_mb=1\n\
            [[engines]]\nid='al'\nadapter='http-align'\nurl='http://127.0.0.1:2'\ncmd=['x']\ncost_mb=1\n";
        let cfg = Arc::new(crate::config::Config::parse(text).unwrap());
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let state = State::new(cfg, tx);
        let port = Wyoming { listen: "127.0.0.1:3".into(), model: "a".into(), languages: vec!["pt".into(), "pt-BR".into()] };
        let v = info(&state, &port);
        let models = v["asr"][0]["models"].as_array().unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0]["name"], "a");
        assert_eq!(models[0]["languages"], json!(["pt", "pt-BR"]));
    }
}
