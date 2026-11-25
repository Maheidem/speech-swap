//! Footprint probe for phase M0.
//!
//! The probe measures the memory cost of the HTTP layer that the hub will use.
//! It does not run speech engines. It has three routes:
//!
//! - `GET /health` returns `{"status":"ok"}`.
//! - `GET /v1/models` returns a static model list.
//! - `POST /v1/audio/transcriptions` streams the multipart upload to a
//!   temporary file, reports the size, deletes the file and returns
//!   `{"text":""}`.
//!
//! Usage: `probe [ADDR]`. The default address is `127.0.0.1:8780`.
//!
//! Design rules that the probe tests: one thread (tokio `current_thread`),
//! hyper without a web framework, the system allocator, and no upload held
//! in memory.

use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::{BodyStream, Full};
use hyper::body::Incoming;
use hyper::header::{HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

/// Upload limit. A larger upload gets status 413.
const MAX_UPLOAD_BYTES: u64 = 100 * 1024 * 1024;
/// Limit for each text field. A larger field gets status 400.
const MAX_FIELD_BYTES: usize = 16 * 1024;

const MODELS_JSON: &str = r#"{"object":"list","data":[{"id":"probe","object":"model","created":0,"owned_by":"speech-swap"}]}"#;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

type Resp = Response<Full<Bytes>>;

/// Deletes the temporary file when the request ends, also when the client
/// disconnects in the middle of an upload.
struct TempFile(PathBuf);

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn json(status: StatusCode, body: impl Into<Bytes>) -> Resp {
    let mut resp = Response::new(Full::new(body.into()));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

fn error(status: StatusCode, message: &str) -> Resp {
    json(status, format!(r#"{{"error":{{"message":"{message}"}}}}"#))
}

async fn route(req: Request<Incoming>) -> Result<Resp, Infallible> {
    let resp = match (req.method(), req.uri().path()) {
        (&Method::GET, "/health") => json(StatusCode::OK, r#"{"status":"ok"}"#),
        (&Method::GET, "/v1/models") => json(StatusCode::OK, MODELS_JSON),
        (&Method::POST, "/v1/audio/transcriptions") => transcriptions(req).await,
        _ => error(StatusCode::NOT_FOUND, "not found"),
    };
    Ok(resp)
}

async fn transcriptions(req: Request<Incoming>) -> Resp {
    let header = |name| {
        req.headers()
            .get(name)
            .and_then(|value: &HeaderValue| value.to_str().ok())
    };
    // Refuse a declared size above the limit before the probe reads the body.
    if header(CONTENT_LENGTH)
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > MAX_UPLOAD_BYTES)
    {
        return error(StatusCode::PAYLOAD_TOO_LARGE, "upload too large");
    }
    let Some(boundary) = header(CONTENT_TYPE).and_then(|value| multer::parse_boundary(value).ok())
    else {
        return error(StatusCode::BAD_REQUEST, "multipart/form-data is necessary");
    };

    // Give the body to multer as a stream of data frames.
    let frames = BodyStream::new(req.into_body()).filter_map(|frame| async move {
        match frame {
            Ok(frame) => frame.into_data().ok().map(Ok),
            Err(err) => Some(Err(err)),
        }
    });
    let mut multipart = multer::Multipart::new(frames, boundary);

    let mut file_bytes: Option<u64> = None;
    let mut total: u64 = 0;
    loop {
        let mut field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(_) => return error(StatusCode::BAD_REQUEST, "bad multipart body"),
        };
        let is_file = field.name() == Some("file");
        let mut sink = None;
        if is_file {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("ssw-probe-{}-{id}.in", std::process::id()));
            match tokio::fs::File::create(&path).await {
                Ok(file) => sink = Some((file, TempFile(path))),
                Err(_) => {
                    return error(StatusCode::INTERNAL_SERVER_ERROR, "temporary file failed")
                }
            }
        }
        let mut size: u64 = 0;
        loop {
            let chunk = match field.chunk().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(_) => return error(StatusCode::BAD_REQUEST, "bad multipart body"),
            };
            size += chunk.len() as u64;
            total += chunk.len() as u64;
            if total > MAX_UPLOAD_BYTES {
                return error(StatusCode::PAYLOAD_TOO_LARGE, "upload too large");
            }
            match sink.as_mut() {
                // The file part goes to the disk chunk by chunk.
                Some((file, _)) => {
                    if file.write_all(&chunk).await.is_err() {
                        return error(StatusCode::INTERNAL_SERVER_ERROR, "write failed");
                    }
                }
                // The probe counts a text field and then drops it.
                None if size as usize > MAX_FIELD_BYTES => {
                    return error(StatusCode::BAD_REQUEST, "text field too large")
                }
                None => {}
            }
        }
        if let Some((mut file, _guard)) = sink {
            if file.flush().await.is_err() {
                return error(StatusCode::INTERNAL_SERVER_ERROR, "write failed");
            }
            file_bytes = Some(size);
            // `_guard` goes out of scope here and deletes the file.
        }
    }

    let Some(bytes) = file_bytes else {
        return error(StatusCode::BAD_REQUEST, "the file part is necessary");
    };
    eprintln!("upload: {bytes} bytes");
    let mut resp = json(StatusCode::OK, r#"{"text":""}"#);
    // The size header lets a test check that the full upload arrived.
    resp.headers_mut()
        .insert("x-probe-bytes", HeaderValue::from(bytes));
    resp
}

fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8780".to_string());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?;
    runtime.block_on(async move {
        let listener = TcpListener::bind(&addr).await?;
        eprintln!("probe: listening on {addr}, pid {}", std::process::id());
        loop {
            let (stream, _) = listener.accept().await?;
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service_fn(route))
                    .await;
            });
        }
    })
}
