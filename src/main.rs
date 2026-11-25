//! speech-swap: a small hub that puts local speech engines behind one
//! OpenAI-style API. Usage: `speech-swap CONFIG.toml`.

/// Writes one log line to stderr with a Unix time stamp.
macro_rules! log {
    ($($arg:tt)*) => {{
        let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        eprintln!("[{}.{:03}] {}", t.as_secs(), t.subsec_millis(), format_args!($($arg)*));
    }};
}

mod config;
mod engine;
mod guard;
mod http;
mod macos;
mod normalize;
mod pipeline;
mod planner;
mod sched;
mod wyoming;

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};

use crate::config::Config;

/// Makes the temporary folder (mode 0700) and deletes the files of an earlier run.
fn prepare_tmp(cfg: &Config) -> std::io::Result<()> {
    let dir = cfg.tmp_dir();
    std::fs::create_dir_all(&dir)?;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    let mut removed = 0;
    for entry in std::fs::read_dir(&dir)?.flatten() {
        if entry.file_name().to_string_lossy().starts_with("ssw-") && std::fs::remove_file(entry.path()).is_ok() {
            removed += 1;
        }
    }
    if removed > 0 {
        log!("removed {removed} temporary files of an earlier run");
    }
    Ok(())
}

fn main() {
    // The guard process: it stops the engines if the hub dies (see guard.rs).
    if std::env::args().nth(1).as_deref() == Some("--guard") {
        std::process::exit(guard::run());
    }
    let Some(path) = std::env::args().nth(1).filter(|a| !a.starts_with('-')) else {
        eprintln!("usage: speech-swap CONFIG.toml");
        std::process::exit(2);
    };
    let cfg = match Config::load(&path) {
        Ok(cfg) => Arc::new(cfg),
        Err(e) => {
            eprintln!("config error: {e}");
            std::process::exit(2);
        }
    };
    if let Err(e) = prepare_tmp(&cfg) {
        eprintln!("temporary folder {}: {e}", cfg.server.tmp_dir);
        std::process::exit(1);
    }
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
    let code = runtime.block_on(run(cfg));
    std::process::exit(code);
}

async fn run(cfg: Arc<Config>) -> i32 {
    let listener = match TcpListener::bind(&cfg.server.listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("cannot listen on {}: {e}", cfg.server.listen);
            return 1;
        }
    };
    let (mut term, mut int) = match (signal(SignalKind::terminate()), signal(SignalKind::interrupt())) {
        (Ok(a), Ok(b)) => (a, b),
        _ => return 1,
    };
    guard::start();
    let tx = sched::start(cfg.clone());
    let state = Arc::new(http::State::new(cfg.clone(), tx.clone()));
    for w in &cfg.wyoming {
        match TcpListener::bind(&w.listen).await {
            Ok(l) => {
                log!("wyoming: model {} listens on {}", w.model, w.listen);
                tokio::spawn(wyoming::serve(state.clone(), l, w.clone()));
            }
            Err(e) => {
                eprintln!("cannot listen on {} (wyoming): {e}", w.listen);
                return 1;
            }
        }
    }
    log!(
        "speech-swap {} listens on {} (pid {}, {} engines, auth {})",
        env!("CARGO_PKG_VERSION"),
        cfg.server.listen,
        std::process::id(),
        cfg.engines.len(),
        if cfg.server.open { "open" } else { "keys" }
    );
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let state = state.clone();
                    tokio::spawn(http::serve(state, stream));
                }
                Err(e) => log!("accept failed: {e}"),
            },
            _ = term.recv() => break,
            _ = int.recv() => break,
        }
    }
    log!("stop signal: the hub stops the engines");
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    if tx.send(sched::Msg::Shutdown(done_tx)).is_ok() {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(15), done_rx).await;
    }
    let _ = prepare_tmp(&cfg);
    log!("stopped");
    0
}
