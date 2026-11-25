//! Engines are child processes. One task supervises one child: it starts
//! the child, sends one request at a time, reads the answers and stops it.
//!
//! Three adapters exist. Two use stdin and stdout:
//!
//! - `speech-worker`: speech.cpp `speech worker`, JSON Lines protocol 4.
//! - `jsonl`: one line `{"file","language"?,"prompt"?}` for each request and
//!   one line `{"file","text","segments"?}` or `{"file","error"}` for each
//!   answer. The first line of the child is `{"ready":true}`.
//!
//! The third adapter, `http-align`, starts a child with an HTTP server on a
//! private port and sends it `POST /align` requests (see `supervise_http`).

use std::future::Future;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::config::{Adapter, Engine};
use crate::guard;
use crate::pipeline::Word;

/// The protocol version of `speech worker` that this adapter speaks.
const WORKER_PROTOCOL: u64 = 4;
/// One `chunk` line holds 1 s of 16 kHz mono 16-bit audio.
const CHUNK_BYTES: usize = 32000;
/// The time between two health requests to a worker.
const PING_EVERY: Duration = Duration::from_secs(15);
/// The time that a child gets to exit after its stdin closes.
const EXIT_GRACE: Duration = Duration::from_secs(5);

/// One request for an engine. The audio is a 16 kHz mono 16-bit WAV file.
#[derive(Debug, Clone)]
pub struct Run {
    pub job: u64,
    pub wav: PathBuf,
    pub data_offset: u64,
    pub data_len: u64,
    pub language: Option<String>,
    pub prompt: Option<String>,
    /// The text to align (aligner engines only).
    pub text: Option<String>,
}

pub enum Cmd {
    Run(Run),
    /// Stop the request that runs now.
    Cancel,
    /// Stop the child.
    Stop,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Segment {
    pub start: f64,
    pub end: f64,
    pub text: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Output {
    pub text: String,
    pub segments: Vec<Segment>,
    /// The language that the engine heard, when the engine tells it.
    pub language: Option<String>,
    /// Word times in seconds from the start of the audio (aligner engines).
    pub words: Vec<Word>,
}

#[derive(Debug, PartialEq)]
pub enum Fail {
    Cancelled,
    Error(String),
}

pub enum Event {
    Ready { load_ms: u64 },
    Done { job: u64, result: Result<Output, Fail> },
    /// The child is gone. This is always the last event of a task.
    Exited { reason: String },
}

/// Starts the child in its own process group. A signal to the group also
/// stops the children of the child. The guard stops the group if the hub dies.
pub fn spawn(cfg: &Engine) -> std::io::Result<Child> {
    let mut cmd = Command::new(&cfg.cmd[0]);
    let http = cfg.adapter == Adapter::HttpAlign;
    cmd.args(&cfg.cmd[1..])
        .envs(&cfg.env)
        .stdin(if http { Stdio::null() } else { Stdio::piped() })
        .stdout(if http { Stdio::null() } else { Stdio::piped() })
        .stderr(Stdio::inherit())
        .process_group(0);
    if let Some(cwd) = &cfg.cwd {
        cmd.current_dir(cwd);
    }
    let child = cmd.spawn()?;
    if let Some(pid) = child.id() {
        guard::add(pid);
    }
    Ok(child)
}

/// Sends a signal to the process group of a child.
pub fn kill_group(pid: u32, signal: i32) {
    // SAFETY: killpg has no memory effects. A group that is gone gives ESRCH.
    unsafe {
        libc::killpg(pid as i32, signal);
    }
}

fn base64(input: &[u8], out: &mut String) {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for chunk in input.chunks(3) {
        let n = (chunk[0] as u32) << 16 | (*chunk.get(1).unwrap_or(&0) as u32) << 8 | *chunk.get(2).unwrap_or(&0) as u32;
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { T[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { T[n as usize & 63] as char } else { '=' });
    }
}

/// What the answer line of an engine means for the request that runs.
#[derive(Debug, PartialEq)]
enum Parsed {
    /// The line ends the request.
    Terminal(Result<Output, Fail>),
    /// The line answers a health request.
    Pong,
    /// The line is for another request or it is a progress message.
    Other,
}

fn segments(value: &Value) -> Vec<Segment> {
    let list = value.get("segments").and_then(Value::as_array);
    list.into_iter()
        .flatten()
        .filter_map(|s| {
            Some(Segment {
                start: s.get("start")?.as_f64()?,
                end: s.get("end")?.as_f64()?,
                text: s.get("text")?.as_str()?.trim().to_string(),
            })
        })
        .collect()
}

fn text_of(value: &Value) -> String {
    value.get("text").and_then(Value::as_str).unwrap_or("").trim().to_string()
}

/// Reads one line of `speech worker`. `rid` is the id of the request that runs.
fn parse_worker(line: &str, rid: Option<&str>, ping: Option<&str>) -> Result<Parsed, String> {
    let v: Value = serde_json::from_str(line).map_err(|_| "the worker wrote a line that is not JSON".to_string())?;
    let id = v.get("id").and_then(Value::as_str);
    let kind = v.get("type").and_then(Value::as_str).unwrap_or("");
    if kind == "fatal" {
        return Err(format!("worker fatal: {}", v["error"]["message"].as_str().unwrap_or("?")));
    }
    if id.is_some() && id == ping && kind != "progress" {
        return Ok(Parsed::Pong);
    }
    if id.is_none() || id != rid {
        return Ok(Parsed::Other);
    }
    Ok(match kind {
        "end" => Parsed::Terminal(Ok(Output {
            text: text_of(&v),
            segments: segments(&v),
            language: v["languages"].get(0).and_then(Value::as_str).map(str::to_string),
            words: Vec::new(),
        })),
        "cancelled" => Parsed::Terminal(Err(Fail::Cancelled)),
        "error" => Parsed::Terminal(Err(Fail::Error(format!(
            "engine error {}: {}",
            v["error"]["code"].as_str().unwrap_or("?"),
            v["error"]["message"].as_str().unwrap_or("?")
        )))),
        _ => Parsed::Other,
    })
}

/// Reads one answer line of a `jsonl` runner. `file` names the request that runs.
fn parse_jsonl(line: &str, file: Option<&str>) -> Result<Parsed, String> {
    let v: Value = serde_json::from_str(line).map_err(|_| "the runner wrote a line that is not JSON".to_string())?;
    if file.is_none() || v.get("file").and_then(Value::as_str) != file {
        return Ok(Parsed::Other);
    }
    if let Some(err) = v.get("error") {
        return Ok(Parsed::Terminal(Err(Fail::Error(format!("engine error: {}", err.as_str().unwrap_or("?"))))));
    }
    Ok(Parsed::Terminal(Ok(Output { text: text_of(&v), segments: segments(&v), ..Output::default() })))
}

/// The request that the child works on now.
struct Current {
    job: u64,
    /// The worker request id, or the file path for a `jsonl` runner.
    tag: String,
    cancelled: bool,
}

struct Task {
    cfg: Engine,
    stdin: ChildStdin,
    current: Option<Current>,
    next_id: u64,
    ping: Option<String>,
}

impl Task {
    async fn send(&mut self, value: &Value) -> std::io::Result<()> {
        let mut line = value.to_string();
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).await
    }

    fn takes(&self, field: &str) -> bool {
        self.cfg.fields.iter().any(|f| f == field)
    }

    /// Sends one request to the child. Only the fields in `fields` go to the engine.
    async fn start(&mut self, run: Run) -> std::io::Result<()> {
        self.next_id += 1;
        let language = run.language.filter(|_| self.takes("language"));
        let prompt = run.prompt.filter(|p| !p.is_empty() && self.takes("prompt"));
        let tag = match self.cfg.adapter {
            Adapter::SpeechWorker => {
                let rid = format!("r{}", self.next_id);
                // The file stays readable through this handle if the hub deletes it.
                let mut file = std::fs::File::open(&run.wav)?;
                file.seek(SeekFrom::Start(run.data_offset))?;
                let mut left = run.data_len as usize;
                let mut pcm = vec![0u8; CHUNK_BYTES];
                let mut line = String::with_capacity(CHUNK_BYTES * 4 / 3 + 128);
                let mut seq = 0u64;
                while left > 0 {
                    let n = left.min(CHUNK_BYTES);
                    file.read_exact(&mut pcm[..n])?;
                    left -= n;
                    line.clear();
                    line.push_str(&format!(r#"{{"type":"chunk","id":"{rid}","seq":{seq},"pcm":""#));
                    base64(&pcm[..n], &mut line);
                    line.push_str("\"}\n");
                    self.stdin.write_all(line.as_bytes()).await?;
                    seq += 1;
                }
                let mut msg = json!({"type": "transcribe", "id": rid, "sample_rate": 16000});
                if let Some(language) = language {
                    msg["language"] = json!(language);
                }
                if let Some(prompt) = prompt {
                    msg["prompt"] = json!(prompt);
                }
                self.send(&msg).await?;
                rid
            }
            Adapter::HttpAlign => unreachable!("http-align has its own supervisor"),
            Adapter::Jsonl => {
                let file = run.wav.to_string_lossy().to_string();
                self.send(&json!({"file": file, "language": language, "prompt": prompt, "stream": false})).await?;
                file
            }
        };
        self.current = Some(Current { job: run.job, tag, cancelled: false });
        Ok(())
    }

    /// Asks the child to stop the request that runs. A `jsonl` runner has no
    /// cancel: the task waits for its answer and then drops it.
    async fn cancel(&mut self) -> std::io::Result<()> {
        let Some(current) = self.current.as_mut() else { return Ok(()) };
        if current.cancelled {
            return Ok(());
        }
        current.cancelled = true;
        if self.cfg.adapter == Adapter::SpeechWorker {
            let msg = json!({"type": "cancel", "id": current.tag});
            self.send(&msg).await?;
        }
        Ok(())
    }

    fn parse(&self, line: &str) -> Result<Parsed, String> {
        let tag = self.current.as_ref().map(|c| c.tag.as_str());
        match self.cfg.adapter {
            Adapter::SpeechWorker => parse_worker(line, tag, self.ping.as_deref()),
            Adapter::Jsonl => parse_jsonl(line, tag),
            Adapter::HttpAlign => unreachable!("http-align has its own supervisor"),
        }
    }
}

/// Waits for the first line of the child and checks it.
async fn wait_ready(cfg: &Engine, lines: &mut Lines<BufReader<ChildStdout>>) -> Result<(), String> {
    let wait = Duration::from_secs(cfg.ready_timeout_s);
    let line = match tokio::time::timeout(wait, lines.next_line()).await {
        Err(_) => return Err(format!("not ready after {} s", wait.as_secs())),
        Ok(Ok(Some(line))) => line,
        Ok(_) => return Err("the child closed stdout before it was ready".into()),
    };
    let v: Value = serde_json::from_str(&line).map_err(|_| "the first line is not JSON".to_string())?;
    match cfg.adapter {
        Adapter::SpeechWorker => {
            if v["type"] == "fatal" {
                return Err(format!("worker fatal: {}", v["error"]["message"].as_str().unwrap_or("?")));
            }
            if v["type"] != "ready" || v["protocol"].as_u64() != Some(WORKER_PROTOCOL) {
                return Err(format!("the worker protocol is {}; this hub speaks protocol {WORKER_PROTOCOL}", v["protocol"]));
            }
        }
        Adapter::Jsonl => {
            if v["ready"] != true {
                return Err(format!("runner load error: {}", v["error"].as_str().unwrap_or("?")));
            }
        }
        Adapter::HttpAlign => unreachable!("http-align has its own supervisor"),
    }
    Ok(())
}

/// Supervises one child until it exits. `emit` sends the events to the scheduler.
pub async fn supervise(cfg: Engine, mut child: Child, mut rx: UnboundedReceiver<Cmd>, emit: impl Fn(Event)) {
    if cfg.adapter == Adapter::HttpAlign {
        return supervise_http(cfg, child, rx, emit).await;
    }
    let pid = child.id().unwrap_or(0);
    let started = Instant::now();
    let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
        emit(Event::Exited { reason: "the child has no pipes".into() });
        return;
    };
    let mut lines = BufReader::new(stdout).lines();
    let mut task = Task { cfg, stdin, current: None, next_id: 0, ping: None };

    let mut reason = match wait_ready(&task.cfg, &mut lines).await {
        Ok(()) => {
            emit(Event::Ready { load_ms: started.elapsed().as_millis() as u64 });
            String::new()
        }
        Err(why) => why,
    };

    let mut ping_timer = tokio::time::interval(PING_EVERY);
    ping_timer.reset();
    while reason.is_empty() {
        tokio::select! {
            cmd = rx.recv() => {
                let sent = match cmd {
                    None | Some(Cmd::Stop) => break,
                    Some(Cmd::Run(run)) => task.start(run).await,
                    Some(Cmd::Cancel) => task.cancel().await,
                };
                if let Err(e) = sent {
                    reason = format!("write to the child failed: {e}");
                }
            }
            line = lines.next_line() => {
                let Ok(Some(line)) = line else {
                    reason = "the child closed stdout".into();
                    break;
                };
                match task.parse(&line) {
                    Ok(Parsed::Terminal(result)) => {
                        if let Some(current) = task.current.take() {
                            emit(Event::Done { job: current.job, result });
                        }
                    }
                    Ok(Parsed::Pong) => task.ping = None,
                    Ok(Parsed::Other) => {}
                    Err(why) => reason = why,
                }
            }
            // Health check: the worker answers `info` at once, also while a
            // request runs. No answer before the next tick means a dead worker.
            _ = ping_timer.tick(), if task.cfg.adapter == Adapter::SpeechWorker => {
                if task.ping.is_some() {
                    reason = "the worker did not answer the health request".into();
                } else {
                    task.next_id += 1;
                    let id = format!("h{}", task.next_id);
                    if let Err(e) = task.send(&json!({"type": "info", "id": id})).await {
                        reason = format!("write to the child failed: {e}");
                    }
                    task.ping = Some(id);
                }
            }
        }
    }

    // Stop: a running worker request does not stop when stdin closes, so
    // send `cancel` first. Then close stdin and wait for the exit.
    let _ = task.cancel().await;
    let Task { stdin, current, .. } = task;
    drop(stdin);
    let status = match tokio::time::timeout(EXIT_GRACE, child.wait()).await {
        Ok(status) => status,
        Err(_) => {
            kill_group(pid, libc::SIGKILL);
            child.wait().await
        }
    };
    // The child is gone. Stop the processes that it left in its group.
    kill_group(pid, libc::SIGKILL);
    guard::remove(pid);
    if let Some(current) = current {
        let why = if reason.is_empty() { "the engine stopped".to_string() } else { reason.clone() };
        emit(Event::Done { job: current.job, result: Err(Fail::Error(why)) });
    }
    if reason.is_empty() {
        reason = match status {
            Ok(s) => format!("stopped ({s})"),
            Err(e) => format!("stopped ({e})"),
        };
    }
    emit(Event::Exited { reason });
}

// ------------------------------------------------------------ http-align

/// The longest answer that the hub reads from an HTTP engine.
const HTTP_MAX_ANSWER: u64 = 16 * 1024 * 1024;
/// The aligner answers 503 while it works on another request (for example
/// one that the hub cancelled). The hub tries again for this long.
const BUSY_RETRY: Duration = Duration::from_secs(20);

/// One HTTP/1.1 request with `Connection: close`. It returns the status and
/// the body. The hub talks only to its own child on the loopback address.
async fn http_call(addr: &str, method: &str, path: &str, bearer: Option<&str>, body: &[u8]) -> Result<(u16, Vec<u8>), String> {
    use tokio::io::AsyncReadExt;
    let mut stream = tokio::net::TcpStream::connect(addr).await.map_err(|e| format!("connect {addr}: {e}"))?;
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n", body.len());
    if let Some(token) = bearer.filter(|t| !t.is_empty()) {
        head.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    head.push_str("\r\n");
    let io = |e: std::io::Error| format!("http {addr}: {e}");
    stream.write_all(head.as_bytes()).await.map_err(io)?;
    stream.write_all(body).await.map_err(io)?;
    let mut answer = Vec::new();
    (&mut stream).take(HTTP_MAX_ANSWER).read_to_end(&mut answer).await.map_err(io)?;
    parse_http_answer(&answer)
}

fn parse_http_answer(answer: &[u8]) -> Result<(u16, Vec<u8>), String> {
    let split = answer.windows(4).position(|w| w == b"\r\n\r\n").ok_or("the engine answer has no header end")?;
    let head = String::from_utf8_lossy(&answer[..split]);
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or("the engine answer has no status")?;
    Ok((status, answer[split + 4..].to_vec()))
}

/// A 44-byte header for 16 kHz mono 16-bit PCM of `len` bytes.
pub fn wav_header(len: u32) -> [u8; 44] {
    let mut h = [0u8; 44];
    h[0..4].copy_from_slice(b"RIFF");
    h[4..8].copy_from_slice(&(36 + len).to_le_bytes());
    h[8..16].copy_from_slice(b"WAVEfmt ");
    h[16..20].copy_from_slice(&16u32.to_le_bytes());
    h[20..22].copy_from_slice(&1u16.to_le_bytes());
    h[22..24].copy_from_slice(&1u16.to_le_bytes());
    h[24..28].copy_from_slice(&16000u32.to_le_bytes());
    h[28..32].copy_from_slice(&32000u32.to_le_bytes());
    h[32..34].copy_from_slice(&2u16.to_le_bytes());
    h[34..36].copy_from_slice(&16u16.to_le_bytes());
    h[36..40].copy_from_slice(b"data");
    h[40..44].copy_from_slice(&len.to_le_bytes());
    h
}

/// Builds the body of `POST /align`: the slice of the file as a WAV in base64.
fn align_body(run: &Run) -> std::io::Result<Vec<u8>> {
    let mut file = std::fs::File::open(&run.wav)?;
    file.seek(SeekFrom::Start(run.data_offset))?;
    let mut pcm = vec![0u8; run.data_len as usize];
    file.read_exact(&mut pcm)?;
    let mut wav = wav_header(pcm.len() as u32).to_vec();
    wav.extend_from_slice(&pcm);
    let mut b64 = String::with_capacity(wav.len() * 4 / 3 + 4);
    base64(&wav, &mut b64);
    let text = serde_json::to_string(run.text.as_deref().unwrap_or("")).unwrap_or_default();
    let language = serde_json::to_string(run.language.as_deref().unwrap_or("")).unwrap_or_default();
    Ok(format!(r#"{{"audio_b64":"{b64}","text":{text},"language":{language}}}"#).into_bytes())
}

/// Reads the answer of `POST /align`.
fn parse_align(status: u16, body: &[u8]) -> Result<Output, Fail> {
    let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    if status != 200 {
        let why = v["error"].as_str().unwrap_or("no reason");
        return Err(Fail::Error(format!("aligner status {status}: {why}")));
    }
    let list = v["words"].as_array().ok_or_else(|| Fail::Error("the aligner answer has no words".into()))?;
    let words = list
        .iter()
        .filter_map(|w| Some(Word { text: w["text"].as_str()?.to_string(), start: w["start"].as_f64()?, end: w["end"].as_f64()? }))
        .collect();
    Ok(Output { words, ..Output::default() })
}

/// Sends one align request. A 503 means "busy": the hub tries again.
async fn align_call(cfg: Engine, run: Run) -> Result<Output, Fail> {
    let addr = http_addr(&cfg);
    let body = align_body(&run).map_err(|e| Fail::Error(format!("cannot read the audio: {e}")))?;
    let token = (!cfg.token.is_empty()).then_some(cfg.token.as_str());
    let started = Instant::now();
    loop {
        let (status, answer) = http_call(&addr, "POST", "/align", token, &body).await.map_err(Fail::Error)?;
        if status == 503 && started.elapsed() < BUSY_RETRY {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }
        return parse_align(status, &answer);
    }
}

/// `host:port` of an `http-align` engine.
fn http_addr(cfg: &Engine) -> String {
    cfg.url.as_deref().unwrap_or("").trim_start_matches("http://").trim_end_matches('/').to_string()
}

type Inflight = Option<(u64, Pin<Box<dyn Future<Output = Result<Output, Fail>> + Send>>)>;

/// Supervises an `http-align` child: waits for the health path, sends the
/// requests one at a time and stops the process group at the end.
///
/// A cancel drops the HTTP request. The aligner finishes its work and then
/// answers 503 for a short time, so the next request waits and tries again.
async fn supervise_http(cfg: Engine, mut child: Child, mut rx: UnboundedReceiver<Cmd>, emit: impl Fn(Event)) {
    let pid = child.id().unwrap_or(0);
    let started = Instant::now();
    let addr = http_addr(&cfg);
    let mut reason = String::new();
    let deadline = started + Duration::from_secs(cfg.ready_timeout_s);
    loop {
        tokio::select! {
            status = child.wait() => {
                reason = format!("the child exited before it was ready ({})", status.map(|s| s.to_string()).unwrap_or_default());
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let health = tokio::time::timeout(Duration::from_secs(2), http_call(&addr, "GET", &cfg.health, None, b"")).await;
                if matches!(health, Ok(Ok((200, _)))) {
                    break;
                }
                if Instant::now() > deadline {
                    reason = format!("not ready after {} s", cfg.ready_timeout_s);
                    break;
                }
            }
            // A Stop during the start.
            cmd = rx.recv() => if matches!(cmd, None | Some(Cmd::Stop)) { break },
        }
    }
    if reason.is_empty() {
        emit(Event::Ready { load_ms: started.elapsed().as_millis() as u64 });
    }
    let mut inflight: Inflight = None;
    while reason.is_empty() {
        tokio::select! {
            cmd = rx.recv() => match cmd {
                None | Some(Cmd::Stop) => break,
                Some(Cmd::Run(run)) => {
                    inflight = Some((run.job, Box::pin(align_call(cfg.clone(), run))));
                }
                Some(Cmd::Cancel) => {
                    if let Some((job, _)) = inflight.take() {
                        emit(Event::Done { job, result: Err(Fail::Cancelled) });
                    }
                }
            },
            result = async { inflight.as_mut().expect("guarded").1.as_mut().await }, if inflight.is_some() => {
                let (job, _) = inflight.take().expect("guarded");
                emit(Event::Done { job, result });
            }
            status = child.wait() => {
                reason = format!("the child exited ({})", status.map(|s| s.to_string()).unwrap_or_default());
            }
        }
    }
    if let Some((job, _)) = inflight.take() {
        let why = if reason.is_empty() { "the engine stopped".to_string() } else { reason.clone() };
        emit(Event::Done { job, result: Err(Fail::Error(why)) });
    }
    // SIGTERM to the group, then SIGKILL after the grace time.
    kill_group(pid, libc::SIGTERM);
    let status = match tokio::time::timeout(EXIT_GRACE, child.wait()).await {
        Ok(status) => status,
        Err(_) => {
            kill_group(pid, libc::SIGKILL);
            child.wait().await
        }
    };
    kill_group(pid, libc::SIGKILL);
    guard::remove(pid);
    if reason.is_empty() {
        reason = match status {
            Ok(s) => format!("stopped ({s})"),
            Err(e) => format!("stopped ({e})"),
        };
    }
    emit(Event::Exited { reason });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_known_values() {
        for (input, expect) in [("", ""), ("f", "Zg=="), ("fo", "Zm8="), ("foo", "Zm9v"), ("foobar", "Zm9vYmFy")] {
            let mut out = String::new();
            base64(input.as_bytes(), &mut out);
            assert_eq!(out, expect);
        }
    }

    #[test]
    fn the_worker_end_line_gives_the_text() {
        let line = r#"{"type":"end","id":"r1","text":" Liga a luz. ","stop":"complete","languages":["pt"]}"#;
        let expect = Output { text: "Liga a luz.".into(), segments: vec![], language: Some("pt".into()), words: vec![] };
        assert_eq!(parse_worker(line, Some("r1"), None), Ok(Parsed::Terminal(Ok(expect))));
    }

    #[test]
    fn worker_lines_for_other_ids_and_progress_are_skipped() {
        let end = r#"{"type":"end","id":"r1","text":"old"}"#;
        assert_eq!(parse_worker(end, Some("r2"), None), Ok(Parsed::Other));
        assert_eq!(parse_worker(end, None, None), Ok(Parsed::Other));
        let progress = r#"{"type":"progress","id":"r2","done":0.4}"#;
        assert_eq!(parse_worker(progress, Some("r2"), None), Ok(Parsed::Other));
    }

    #[test]
    fn the_worker_cancel_and_error_lines_end_the_request() {
        let cancelled = r#"{"type":"cancelled","id":"r3"}"#;
        assert_eq!(parse_worker(cancelled, Some("r3"), None), Ok(Parsed::Terminal(Err(Fail::Cancelled))));
        let error = r#"{"type":"error","id":"r3","error":{"code":"out_of_range","option":"language","message":"bad"}}"#;
        let expect = Fail::Error("engine error out_of_range: bad".into());
        assert_eq!(parse_worker(error, Some("r3"), None), Ok(Parsed::Terminal(Err(expect))));
    }

    #[test]
    fn the_worker_info_answer_is_a_pong() {
        let line = r#"{"type":"end","id":"h7","model":{}}"#;
        assert_eq!(parse_worker(line, None, Some("h7")), Ok(Parsed::Pong));
    }

    #[test]
    fn a_worker_line_that_is_not_json_is_a_defect() {
        assert!(parse_worker("hello", Some("r1"), None).is_err());
        assert!(parse_worker(r#"{"type":"fatal","error":{"message":"no model"}}"#, None, None).is_err());
    }

    #[test]
    fn the_jsonl_answer_gives_text_and_segments() {
        let line = r#"{"file":"/t/a.wav","text":"Hello.","segments":[{"start":0.0,"end":2.5,"text":"Hello."}],"engine_ms":80}"#;
        let seg = Segment { start: 0.0, end: 2.5, text: "Hello.".into() };
        let expect = Output { text: "Hello.".into(), segments: vec![seg], language: None, words: vec![] };
        assert_eq!(parse_jsonl(line, Some("/t/a.wav")), Ok(Parsed::Terminal(Ok(expect))));
        assert_eq!(parse_jsonl(line, Some("/t/b.wav")), Ok(Parsed::Other));
    }

    #[test]
    fn the_jsonl_error_line_ends_the_request() {
        let line = r#"{"file":"/t/a.wav","error":"bad audio"}"#;
        let expect = Fail::Error("engine error: bad audio".into());
        assert_eq!(parse_jsonl(line, Some("/t/a.wav")), Ok(Parsed::Terminal(Err(expect))));
    }

    #[test]
    fn the_http_answer_is_split_into_status_and_body() {
        let answer = b"HTTP/1.0 503 Service Unavailable\r\nContent-Type: application/json\r\n\r\n{\"error\": \"busy\"}";
        assert_eq!(parse_http_answer(answer), Ok((503, br#"{"error": "busy"}"#.to_vec())));
        assert!(parse_http_answer(b"garbage").is_err());
    }

    #[test]
    fn the_align_answer_gives_words() {
        let body = br#"{"words":[{"text":"Cancela","start":0.48,"end":1.44},{"text":"o","start":1.44,"end":1.44}],"model":"m","ms":10}"#;
        let out = parse_align(200, body).unwrap();
        assert_eq!(out.words, [Word { text: "Cancela".into(), start: 0.48, end: 1.44 }, Word { text: "o".into(), start: 1.44, end: 1.44 }]);
        assert_eq!(parse_align(400, br#"{"error": "audio is 40.0s; max 31s"}"#), Err(Fail::Error("aligner status 400: audio is 40.0s; max 31s".into())));
        assert!(parse_align(200, b"{}").is_err());
    }

    #[test]
    fn the_wav_header_is_44_bytes_and_names_the_length() {
        let h = wav_header(64000);
        assert_eq!(&h[0..4], b"RIFF");
        assert_eq!(u32::from_le_bytes([h[4], h[5], h[6], h[7]]), 64036);
        assert_eq!(u32::from_le_bytes([h[40], h[41], h[42], h[43]]), 64000);
        assert_eq!(u32::from_le_bytes([h[24], h[25], h[26], h[27]]), 16000);
    }
}
