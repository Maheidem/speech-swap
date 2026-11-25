//! The guard keeps engines from outliving the hub.
//!
//! macOS has no "kill the child when the parent dies" option. The hub starts
//! one small helper process (itself, with `--guard`) and holds the write end
//! of a pipe to it. The hub writes `add <pgid>` and `del <pgid>` lines for the
//! process group of each engine. When the hub dies for any reason, also from
//! SIGKILL, the kernel closes the pipe. The guard reads end-of-file and stops
//! all registered groups: SIGTERM first, SIGKILL after 3 s.
//!
//! The engines do not get the pipe: Rust opens it with close-on-exec.

use std::io::{BufRead, Write};
use std::process::{ChildStdin, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use std::os::unix::process::CommandExt;

static PIPE: Mutex<Option<ChildStdin>> = Mutex::new(None);

/// Starts the guard process. Call it once, before the first engine starts.
pub fn start() {
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(e) => {
            log!("guard: no path of the executable ({e}); engines can outlive a killed hub");
            return;
        }
    };
    // Its own process group: a signal to the group of the hub does not reach it.
    match Command::new(exe).arg("--guard").stdin(Stdio::piped()).stdout(Stdio::null()).process_group(0).spawn() {
        Ok(mut child) => {
            *PIPE.lock().unwrap() = child.stdin.take();
            log!("guard: started (pid {})", child.id());
        }
        Err(e) => log!("guard: did not start ({e}); engines can outlive a killed hub"),
    }
}

fn send(line: &str) {
    let mut pipe = PIPE.lock().unwrap();
    if let Some(stdin) = pipe.as_mut() {
        if stdin.write_all(line.as_bytes()).and_then(|_| stdin.flush()).is_err() {
            log!("guard: the pipe is broken; engines can outlive a killed hub");
            *pipe = None;
        }
    }
}

/// Tells the guard to watch the process group `pgid`.
pub fn add(pgid: u32) {
    send(&format!("add {pgid}\n"));
}

/// Tells the guard that the group is gone.
pub fn remove(pgid: u32) {
    send(&format!("del {pgid}\n"));
}

/// The state of the guard: the groups that it watches.
#[derive(Default)]
struct Groups(Vec<i32>);

impl Groups {
    /// Applies one line of the protocol.
    fn apply(&mut self, line: &str) {
        let mut parts = line.split_whitespace();
        let (Some(verb), Some(Ok(pgid))) = (parts.next(), parts.next().map(str::parse::<i32>)) else { return };
        if pgid <= 1 {
            return;
        }
        match verb {
            "add" if !self.0.contains(&pgid) => self.0.push(pgid),
            "del" => self.0.retain(|g| *g != pgid),
            _ => {}
        }
    }
}

fn group_alive(pgid: i32) -> bool {
    // SAFETY: signal 0 only checks that the group exists.
    unsafe { libc::killpg(pgid, 0) == 0 }
}

/// The guard process. It returns the exit code.
pub fn run() -> i32 {
    let mut groups = Groups::default();
    for line in std::io::stdin().lock().lines() {
        match line {
            Ok(line) => groups.apply(&line),
            Err(_) => break,
        }
    }
    // The pipe is closed: the hub is gone.
    for g in &groups.0 {
        // SAFETY: killpg has no memory effects.
        unsafe { libc::killpg(*g, libc::SIGTERM) };
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline && groups.0.iter().any(|g| group_alive(*g)) {
        std::thread::sleep(Duration::from_millis(100));
    }
    for g in &groups.0 {
        // SAFETY: as above.
        unsafe { libc::killpg(*g, libc::SIGKILL) };
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_del_change_the_list() {
        let mut g = Groups::default();
        g.apply("add 100");
        g.apply("add 200");
        g.apply("add 100");
        assert_eq!(g.0, [100, 200]);
        g.apply("del 100");
        assert_eq!(g.0, [200]);
    }

    #[test]
    fn bad_lines_and_dangerous_ids_are_ignored() {
        let mut g = Groups::default();
        for line in ["", "add", "add x", "add 0", "add 1", "add -5", "kill 77", "add 12 extra"] {
            g.apply(line);
        }
        // "add 12 extra" is valid: the guard reads the first two words.
        assert_eq!(g.0, [12]);
    }
}
