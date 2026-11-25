//! The scheduler. One task owns all state: the queues, the engine table,
//! the memory data and the counters. Other tasks talk to it with messages.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

use crate::config::Config;
use crate::engine::{self, Cmd, Event, Fail, Output, Run};
use crate::macos;
use crate::planner::{self, EngineView, Mem, Plan, Res};

pub const CLASS_NAMES: [&str; 3] = ["voice", "interactive", "batch"];
/// Upper limits of the histogram buckets, in seconds.
const BUCKETS: [f64; 10] = [0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0];

/// A transcription job from one HTTP request.
pub struct Job {
    pub id: u64,
    /// Index of the engine in the config.
    pub engine: usize,
    /// 0 = voice, 1 = interactive, 2 = batch.
    pub class: u8,
    pub key: String,
    pub allow_fallback: bool,
    pub run: Run,
    pub reply: oneshot::Sender<Result<Done, Reject>>,
}

pub struct Done {
    pub output: Output,
    /// The id of the model that did the work. A fallback changes it.
    pub model: String,
    pub wait_ms: u64,
    pub run_ms: u64,
    /// How many times a job of a better class stopped this job.
    pub preempted: u32,
}

#[derive(Debug)]
pub struct Reject {
    pub status: u16,
    pub code: &'static str,
    pub message: String,
    pub retry_after: Option<u32>,
}

pub enum Query {
    Running,
    Metrics,
    Health,
    Models,
}

pub enum Msg {
    Submit(Job),
    /// The client of this job is gone.
    Cancel(u64),
    Engine { idx: usize, gen: u64, event: Event },
    /// One finished HTTP request, for the request counter.
    Record { model: String, key: String, class: u8, code: u16 },
    Query(Query, oneshot::Sender<String>),
    Shutdown(oneshot::Sender<()>),
}

struct Queued {
    job: Job,
    arrival: Instant,
    preempted: u32,
}

struct Running {
    q: Queued,
    started: Instant,
    /// The scheduler sent `cancel` for a job of a better class.
    preempting: bool,
    /// The client is gone. The answer of the engine has no reader.
    abandoned: bool,
}

struct Slot {
    res: Res,
    gen: u64,
    cmd: Option<UnboundedSender<Cmd>>,
    pid: Option<u32>,
    queue: Vec<Queued>,
    running: Option<Running>,
    loaded_at: Instant,
    last_used: Instant,
    footprint: u64,
    peak_mb: u64,
    loads: u64,
    last_exit: String,
}

#[derive(Default)]
struct Histogram {
    counts: [u64; BUCKETS.len()],
    sum: f64,
    n: u64,
}

impl Histogram {
    fn observe(&mut self, seconds: f64) {
        for (i, limit) in BUCKETS.iter().enumerate() {
            if seconds <= *limit {
                self.counts[i] += 1;
            }
        }
        self.sum += seconds;
        self.n += 1;
    }
}

#[derive(Default)]
struct Counters {
    requests: BTreeMap<(String, String, u8, u16), u64>,
    swaps: BTreeMap<(String, String), u64>,
    fallbacks: BTreeMap<(String, String), u64>,
    preemptions: u64,
    wait: [Histogram; 3],
    run: [Histogram; 3],
}

/// The class that the queue uses for a job. A batch job that waits for a
/// long time rises to class 1, so a flow of better jobs cannot starve it.
pub fn effective_class(class: u8, waited: Duration, aging_after_s: u64) -> u8 {
    if class == 2 && aging_after_s > 0 && waited.as_secs() >= aging_after_s {
        1
    } else {
        class
    }
}

pub struct Scheduler {
    cfg: Arc<Config>,
    tx: UnboundedSender<Msg>,
    slots: Vec<Slot>,
    counters: Counters,
    available_mb: Option<u64>,
    pressure: u32,
    swap_used_mb: u64,
    started: Instant,
}

/// Starts the scheduler task and returns its message channel.
pub fn start(cfg: Arc<Config>) -> UnboundedSender<Msg> {
    let (tx, rx) = unbounded_channel();
    let now = Instant::now();
    let slots = cfg
        .engines
        .iter()
        .map(|_| Slot {
            res: Res::Unloaded,
            gen: 0,
            cmd: None,
            pid: None,
            queue: Vec::new(),
            running: None,
            loaded_at: now,
            last_used: now,
            footprint: 0,
            peak_mb: 0,
            loads: 0,
            last_exit: String::new(),
        })
        .collect();
    let mut s = Scheduler {
        cfg,
        tx: tx.clone(),
        slots,
        counters: Counters::default(),
        available_mb: None,
        pressure: 1,
        swap_used_mb: 0,
        started: now,
    };
    s.sample_memory();
    tokio::spawn(s.run(rx));
    tx
}

impl Scheduler {
    async fn run(mut self, mut rx: UnboundedReceiver<Msg>) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                msg = rx.recv() => match msg {
                    None => return,
                    Some(Msg::Shutdown(done)) => {
                        self.shutdown(&mut rx).await;
                        let _ = done.send(());
                        return;
                    }
                    Some(msg) => self.handle(msg),
                },
                _ = tick.tick() => self.tick(),
            }
            self.pump();
        }
    }

    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Submit(job) => self.submit(job),
            Msg::Cancel(id) => self.cancel(id),
            Msg::Engine { idx, gen, event } => {
                if self.slots[idx].gen == gen {
                    self.engine_event(idx, event);
                }
            }
            Msg::Record { model, key, class, code } => {
                *self.counters.requests.entry((model, key, class, code)).or_default() += 1;
            }
            Msg::Query(query, reply) => {
                let _ = reply.send(match query {
                    Query::Running => self.running_json(),
                    Query::Metrics => self.metrics_text(),
                    Query::Health => self.health_json(),
                    Query::Models => self.models_json(),
                });
            }
            Msg::Shutdown(_) => {}
        }
    }

    fn id(&self, idx: usize) -> &str {
        &self.cfg.engines[idx].id
    }

    /// The cost of an engine for the planner: the configured value or the
    /// highest footprint seen, the larger of the two.
    fn cost_mb(&self, idx: usize) -> u64 {
        self.cfg.engines[idx].cost_mb.max(self.slots[idx].peak_mb)
    }

    fn can_preempt(&self, running: &Running, now: Instant) -> bool {
        let aging = self.cfg.scheduler.aging_after_s;
        self.cfg.scheduler.preempt
            && !running.abandoned
            && effective_class(running.q.job.class, now - running.q.arrival, aging) == 2
    }

    // ---------------------------------------------------------------- jobs

    fn submit(&mut self, mut job: Job) {
        let now = Instant::now();
        // Fallback: a voice job does not wait for an engine that cannot start it at once.
        if let (true, 0, Some(fallback)) = (job.allow_fallback, job.class, &self.cfg.engines[job.engine].fallback) {
            let slot = &self.slots[job.engine];
            let free_now = slot.queue.is_empty()
                && match (&slot.res, &slot.running) {
                    (Res::Idle, _) => true,
                    (Res::Busy, Some(running)) => self.can_preempt(running, now),
                    _ => false,
                };
            if !free_now {
                let to = self.cfg.engine_index(fallback).expect("checked at start");
                *self.counters.fallbacks.entry((self.id(job.engine).to_string(), self.id(to).to_string())).or_default() += 1;
                log!("job {} falls back from {} to {}", job.id, self.id(job.engine), self.id(to));
                job.engine = to;
            }
        }
        let class = job.class as usize;
        let slot = &mut self.slots[job.engine];
        let waiting = slot.queue.iter().filter(|q| q.job.class == job.class).count();
        if waiting >= self.cfg.scheduler.class_max_queue[class] {
            let _ = job.reply.send(Err(Reject {
                status: 503,
                code: "queue_full",
                message: format!("the {} queue of this model is full", CLASS_NAMES[class]),
                retry_after: Some(self.cfg.scheduler.retry_after_s),
            }));
            return;
        }
        slot.queue.push(Queued { job, arrival: now, preempted: 0 });
    }

    fn cancel(&mut self, id: u64) {
        for slot in &mut self.slots {
            if let Some(pos) = slot.queue.iter().position(|q| q.job.id == id) {
                slot.queue.remove(pos);
                log!("job {id}: the client is gone; removed from the queue");
                return;
            }
            if let Some(running) = slot.running.as_mut().filter(|r| r.q.job.id == id) {
                running.abandoned = true;
                if let Some(cmd) = &slot.cmd {
                    let _ = cmd.send(Cmd::Cancel);
                }
                log!("job {id}: the client is gone; cancel sent to the engine");
                return;
            }
        }
    }

    fn reject_queue(&mut self, idx: usize, code: &'static str, message: &str) {
        let retry_after = Some(self.cfg.scheduler.retry_after_s);
        for q in self.slots[idx].queue.drain(..) {
            let _ = q.job.reply.send(Err(Reject { status: 503, code, message: message.to_string(), retry_after }));
        }
    }

    // ---------------------------------------------------------------- engines

    fn engine_event(&mut self, idx: usize, event: Event) {
        let now = Instant::now();
        match event {
            Event::Ready { load_ms } => {
                let slot = &mut self.slots[idx];
                if slot.res == Res::Loading {
                    slot.res = Res::Idle;
                }
                slot.loaded_at = now;
                slot.last_used = now;
                log!("engine {} is ready after {load_ms} ms (pid {})", self.id(idx), self.slots[idx].pid.unwrap_or(0));
            }
            Event::Done { job, result } => {
                let slot = &mut self.slots[idx];
                let Some(running) = slot.running.take().filter(|r| r.q.job.id == job) else { return };
                if slot.res == Res::Busy {
                    slot.res = Res::Idle;
                }
                slot.last_used = now;
                self.job_done(idx, running, result, now);
            }
            Event::Exited { reason } => {
                let was = self.slots[idx].res;
                log!("engine {} exited: {reason}", self.id(idx));
                let slot = &mut self.slots[idx];
                slot.res = Res::Unloaded;
                slot.cmd = None;
                slot.pid = None;
                slot.footprint = 0;
                slot.last_exit = reason.clone();
                if let Some(running) = slot.running.take() {
                    self.job_done(idx, running, Err(Fail::Error(reason.clone())), now);
                }
                if was == Res::Loading {
                    // The engine did not start. Do not start it again for the same jobs.
                    self.reject_queue(idx, "engine_unavailable", &format!("the engine did not start: {reason}"));
                }
            }
        }
    }

    fn job_done(&mut self, idx: usize, running: Running, result: Result<Output, Fail>, now: Instant) {
        let Running { q, started, preempting, abandoned } = running;
        let class = q.job.class as usize;
        let run_s = (now - started).as_secs_f64();
        if abandoned {
            return;
        }
        match result {
            Ok(output) => {
                let wait = started - q.arrival;
                self.counters.wait[class].observe(wait.as_secs_f64());
                self.counters.run[class].observe(run_s);
                let _ = q.job.reply.send(Ok(Done {
                    output,
                    model: self.id(idx).to_string(),
                    wait_ms: wait.as_millis() as u64,
                    run_ms: (run_s * 1000.0) as u64,
                    preempted: q.preempted,
                }));
            }
            // A job of a better class stopped this job. It goes back to the
            // queue with its first arrival time, so it is the first of its class.
            Err(Fail::Cancelled) if preempting => {
                self.counters.preemptions += 1;
                log!("job {} was preempted after {run_s:.2} s; it goes back to the queue", q.job.id);
                self.slots[idx].queue.push(Queued { preempted: q.preempted + 1, ..q });
            }
            Err(fail) => {
                let message = match fail {
                    Fail::Cancelled => "the engine cancelled the request".to_string(),
                    Fail::Error(message) => message,
                };
                log!("job {} failed on {}: {message}", q.job.id, self.id(idx));
                let _ = q.job.reply.send(Err(Reject { status: 500, code: "engine_error", message, retry_after: None }));
            }
        }
    }

    fn stop_engine(&mut self, idx: usize) {
        let slot = &mut self.slots[idx];
        if let Some(cmd) = &slot.cmd {
            let _ = cmd.send(Cmd::Stop);
            slot.res = Res::Stopping;
        }
    }

    fn load_engine(&mut self, idx: usize) {
        let cfg = self.cfg.engines[idx].clone();
        let child = match engine::spawn(&cfg) {
            Ok(child) => child,
            Err(e) => {
                log!("engine {}: start failed: {e}", cfg.id);
                self.slots[idx].last_exit = format!("start failed: {e}");
                self.reject_queue(idx, "engine_unavailable", &format!("the engine did not start: {e}"));
                return;
            }
        };
        let (cmd_tx, cmd_rx) = unbounded_channel();
        let slot = &mut self.slots[idx];
        slot.gen += 1;
        slot.res = Res::Loading;
        slot.pid = child.id();
        slot.cmd = Some(cmd_tx);
        slot.loads += 1;
        slot.loaded_at = Instant::now();
        let (tx, gen) = (self.tx.clone(), slot.gen);
        log!("engine {}: start (pid {})", cfg.id, slot.pid.unwrap_or(0));
        tokio::spawn(engine::supervise(cfg, child, cmd_rx, move |event| {
            let _ = tx.send(Msg::Engine { idx, gen, event });
        }));
    }

    fn views(&self, now: Instant) -> Vec<EngineView> {
        let min_resident = Duration::from_secs(self.cfg.memory.min_resident_s);
        (0..self.slots.len())
            .map(|i| {
                let (slot, cfg) = (&self.slots[i], &self.cfg.engines[i]);
                EngineView {
                    cost_mb: self.cost_mb(i),
                    res: slot.res,
                    evictable: slot.res == Res::Idle
                        && !cfg.persistent
                        && slot.queue.is_empty()
                        && now - slot.loaded_at >= min_resident,
                    evict_cost: cfg.evict_cost,
                    last_used: (slot.last_used - self.started).as_millis() as u64,
                }
            })
            .collect()
    }

    fn mem(&self) -> Mem {
        Mem {
            budget_mb: self.cfg.memory.engine_budget_mb,
            headroom_mb: self.cfg.memory.headroom_mb,
            available_mb: self.available_mb,
            pressure: self.pressure,
            refuse_at: self.cfg.memory.refuse_loads_on,
        }
    }

    /// Gives work to the engines. The scheduler calls it after each event.
    fn pump(&mut self) {
        let now = Instant::now();
        let aging = self.cfg.scheduler.aging_after_s;
        // Serve the engine with the best waiting job first.
        let mut order: Vec<(u8, u64, usize)> = Vec::new();
        for (idx, slot) in self.slots.iter().enumerate() {
            let keys = slot.queue.iter().map(|q| (effective_class(q.job.class, now - q.arrival, aging), q.job.id));
            if let Some(best) = keys.min() {
                order.push((best.0, best.1, idx));
            }
        }
        order.sort();
        for (head_class, head_id, idx) in order {
            match self.slots[idx].res {
                Res::Idle => {
                    let slot = &mut self.slots[idx];
                    let pos = slot.queue.iter().position(|q| q.job.id == head_id).expect("the head is in the queue");
                    let q = slot.queue.remove(pos);
                    let sent = slot.cmd.as_ref().is_some_and(|cmd| cmd.send(Cmd::Run(q.job.run.clone())).is_ok());
                    if sent {
                        slot.res = Res::Busy;
                        slot.running = Some(Running { q, started: now, preempting: false, abandoned: false });
                    } else {
                        slot.queue.push(q); // The task is gone. Its exit event comes next.
                    }
                }
                Res::Busy => {
                    // Preemption: a voice or interactive job stops a running batch job.
                    let preempt = self.slots[idx].running.as_ref().is_some_and(|r| !r.preempting && self.can_preempt(r, now));
                    if preempt && head_class <= 1 {
                        let slot = &mut self.slots[idx];
                        let running = slot.running.as_mut().expect("checked");
                        running.preempting = true;
                        log!("job {head_id} (class {head_class}) preempts batch job {}", running.q.job.id);
                        if let Some(cmd) = &slot.cmd {
                            let _ = cmd.send(Cmd::Cancel);
                        }
                    }
                }
                Res::Loading | Res::Stopping => {}
                Res::Unloaded => match planner::plan(idx, &self.views(now), &self.mem()) {
                    Plan::Load => self.load_engine(idx),
                    Plan::Evict(victims) => {
                        for victim in victims {
                            log!("engine {} stops to make room for {}", self.id(victim), self.id(idx));
                            *self.counters.swaps.entry((self.id(victim).to_string(), self.id(idx).to_string())).or_default() += 1;
                            self.stop_engine(victim);
                        }
                    }
                    Plan::Wait(_) => {}
                    Plan::Never(why) => self.reject_queue(idx, "engine_unavailable", why),
                },
            }
        }
    }

    fn sample_memory(&mut self) {
        self.available_mb = macos::available_mb();
        self.pressure = macos::pressure_level().unwrap_or(1);
        self.swap_used_mb = macos::swap_used_mb().unwrap_or(0);
        for slot in &mut self.slots {
            if let Some(bytes) = slot.pid.and_then(|pid| macos::phys_footprint(pid as i32)) {
                slot.footprint = bytes;
                slot.peak_mb = slot.peak_mb.max(bytes / (1024 * 1024));
            }
        }
    }

    /// Runs one time each second: memory data, queue timeouts and idle stops.
    fn tick(&mut self) {
        let now = Instant::now();
        self.sample_memory();
        let retry_after = Some(self.cfg.scheduler.retry_after_s);
        let timeouts = self.cfg.scheduler.class_timeout_s;
        for slot in &mut self.slots {
            let mut i = 0;
            while i < slot.queue.len() {
                let limit = timeouts[slot.queue[i].job.class as usize];
                if (now - slot.queue[i].arrival).as_secs() >= limit {
                    let q = slot.queue.remove(i);
                    log!("job {}: no engine slot after {limit} s", q.job.id);
                    let _ = q.job.reply.send(Err(Reject {
                        status: 503,
                        code: "queue_timeout",
                        message: format!("the job waited {limit} s in the queue"),
                        retry_after,
                    }));
                } else {
                    i += 1;
                }
            }
        }
        // Idle stop (TTL).
        let idle = |slot: &Slot| slot.res == Res::Idle && slot.queue.is_empty();
        for idx in 0..self.slots.len() {
            let (slot, cfg) = (&self.slots[idx], &self.cfg.engines[idx]);
            if idle(slot) && !cfg.persistent && cfg.ttl_s > 0 && (now - slot.last_used).as_secs() >= cfg.ttl_s {
                log!("engine {} was idle for {} s; it stops", cfg.id, cfg.ttl_s);
                self.stop_engine(idx);
            }
        }
        // Memory pressure: stop the idle engine with the largest footprint.
        if self.pressure >= self.cfg.memory.evict_on_pressure {
            let victim = (0..self.slots.len())
                .filter(|i| idle(&self.slots[*i]) && !self.cfg.engines[*i].persistent)
                .max_by_key(|i| self.slots[*i].footprint);
            if let Some(idx) = victim {
                log!("memory pressure level {}: engine {} stops", self.pressure, self.id(idx));
                self.stop_engine(idx);
            }
        }
    }

    /// Stops all engines and waits for their exit.
    async fn shutdown(&mut self, rx: &mut UnboundedReceiver<Msg>) {
        for idx in 0..self.slots.len() {
            self.reject_queue(idx, "shutdown", "the hub stops");
            self.stop_engine(idx);
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while self.slots.iter().any(|s| s.res != Res::Unloaded) {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(Msg::Engine { idx, gen, event })) if self.slots[idx].gen == gen => self.engine_event(idx, event),
                Ok(Some(_)) => {}
                _ => break,
            }
        }
        // Last resort for a child that did not exit.
        for slot in &self.slots {
            if let Some(pid) = slot.pid {
                engine::kill_group(pid, libc::SIGKILL);
            }
        }
    }

    // ---------------------------------------------------------------- reports

    fn res_name(res: Res) -> &'static str {
        match res {
            Res::Unloaded => "unloaded",
            Res::Loading => "loading",
            Res::Idle => "idle",
            Res::Busy => "busy",
            Res::Stopping => "stopping",
        }
    }

    fn running_json(&self) -> String {
        let now = Instant::now();
        let engines: Vec<_> = (0..self.slots.len())
            .map(|i| {
                let slot = &self.slots[i];
                let mut queue = [0usize; 3];
                for q in &slot.queue {
                    queue[q.job.class as usize] += 1;
                }
                json!({
                    "model": self.id(i),
                    "state": Self::res_name(slot.res),
                    "pid": slot.pid,
                    "cost_mb": self.cost_mb(i),
                    "footprint_mb": slot.footprint / (1024 * 1024),
                    "peak_footprint_mb": slot.peak_mb,
                    "loads": slot.loads,
                    "idle_s": (slot.res == Res::Idle).then(|| (now - slot.last_used).as_secs()),
                    "queue": {"voice": queue[0], "interactive": queue[1], "batch": queue[2]},
                    "job": slot.running.as_ref().map(|r| json!({
                        "id": r.q.job.id,
                        "class": CLASS_NAMES[r.q.job.class as usize],
                        "key": r.q.job.key,
                        "run_s": (now - r.started).as_secs_f64(),
                        "preempted": r.q.preempted,
                    })),
                    "last_exit": (!slot.last_exit.is_empty()).then_some(&slot.last_exit),
                })
            })
            .collect();
        let resident: u64 = (0..self.slots.len()).filter(|i| self.slots[*i].res != Res::Unloaded).map(|i| self.cost_mb(i)).sum();
        json!({
            "engines": engines,
            "memory": {
                "engine_budget_mb": self.cfg.memory.engine_budget_mb,
                "resident_cost_mb": resident,
                "available_mb": self.available_mb,
                "pressure_level": self.pressure,
                "swap_used_mb": self.swap_used_mb,
            },
        })
        .to_string()
    }

    fn health_json(&self) -> String {
        let engines: serde_json::Map<String, serde_json::Value> =
            (0..self.slots.len()).map(|i| (self.id(i).to_string(), json!(Self::res_name(self.slots[i].res)))).collect();
        json!({"status": "ok", "engines": engines}).to_string()
    }

    fn models_json(&self) -> String {
        let data: Vec<_> = (0..self.slots.len())
            .map(|i| {
                json!({
                    "id": self.id(i),
                    "object": "model",
                    "created": 0,
                    "owned_by": "speech-swap",
                    "speech_swap": {
                        "kind": "stt",
                        "aliases": self.cfg.engines[i].aliases,
                        "resident": self.slots[i].res != Res::Unloaded,
                    },
                })
            })
            .collect();
        json!({"object": "list", "data": data}).to_string()
    }

    fn metrics_text(&self) -> String {
        let mut out = String::new();
        let family = |out: &mut String, name: &str, kind: &str, help: &str| {
            let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
        };
        family(&mut out, "ssw_requests_total", "counter", "Finished HTTP transcription requests.");
        for ((model, key, class, code), n) in &self.counters.requests {
            let class = CLASS_NAMES[*class as usize];
            let _ = writeln!(out, r#"ssw_requests_total{{model="{model}",key="{key}",class="{class}",code="{code}"}} {n}"#);
        }
        family(&mut out, "ssw_queue_depth", "gauge", "Jobs that wait for an engine.");
        for (i, slot) in self.slots.iter().enumerate() {
            for (class, name) in CLASS_NAMES.iter().enumerate() {
                let n = slot.queue.iter().filter(|q| q.job.class as usize == class).count();
                let _ = writeln!(out, r#"ssw_queue_depth{{model="{}",class="{name}"}} {n}"#, self.id(i));
            }
        }
        for (name, help, set) in [
            ("ssw_wait_seconds", "Time of a job in the queue.", &self.counters.wait),
            ("ssw_run_seconds", "Time of a job in the engine.", &self.counters.run),
        ] {
            family(&mut out, name, "histogram", help);
            for (class, h) in set.iter().enumerate() {
                let class = CLASS_NAMES[class];
                for (limit, count) in BUCKETS.iter().zip(h.counts) {
                    let _ = writeln!(out, r#"{name}_bucket{{class="{class}",le="{limit}"}} {count}"#);
                }
                let _ = writeln!(out, r#"{name}_bucket{{class="{class}",le="+Inf"}} {}"#, h.n);
                let _ = writeln!(out, r#"{name}_sum{{class="{class}"}} {}"#, h.sum);
                let _ = writeln!(out, r#"{name}_count{{class="{class}"}} {}"#, h.n);
            }
        }
        family(&mut out, "ssw_engine_loaded", "gauge", "1 when the engine process is resident.");
        for (i, slot) in self.slots.iter().enumerate() {
            let _ = writeln!(out, r#"ssw_engine_loaded{{model="{}"}} {}"#, self.id(i), (slot.res != Res::Unloaded) as u8);
        }
        family(&mut out, "ssw_engine_footprint_bytes", "gauge", "phys_footprint of the engine process.");
        for (i, slot) in self.slots.iter().enumerate() {
            let _ = writeln!(out, r#"ssw_engine_footprint_bytes{{model="{}"}} {}"#, self.id(i), slot.footprint);
        }
        family(&mut out, "ssw_engine_cost_mb", "gauge", "Memory cost that the planner uses.");
        for i in 0..self.slots.len() {
            let _ = writeln!(out, r#"ssw_engine_cost_mb{{model="{}"}} {}"#, self.id(i), self.cost_mb(i));
        }
        family(&mut out, "ssw_engine_loads_total", "counter", "Starts of the engine process.");
        for (i, slot) in self.slots.iter().enumerate() {
            let _ = writeln!(out, r#"ssw_engine_loads_total{{model="{}"}} {}"#, self.id(i), slot.loads);
        }
        family(&mut out, "ssw_swaps_total", "counter", "Engines stopped to make room for another engine.");
        for ((from, to), n) in &self.counters.swaps {
            let _ = writeln!(out, r#"ssw_swaps_total{{from="{from}",to="{to}"}} {n}"#);
        }
        family(&mut out, "ssw_fallback_total", "counter", "Voice jobs that went to the fallback model.");
        for ((from, to), n) in &self.counters.fallbacks {
            let _ = writeln!(out, r#"ssw_fallback_total{{from="{from}",to="{to}"}} {n}"#);
        }
        family(&mut out, "ssw_preemptions_total", "counter", "Batch jobs stopped for a job of a better class.");
        let _ = writeln!(out, "ssw_preemptions_total {}", self.counters.preemptions);
        family(&mut out, "ssw_pressure_level", "gauge", "macOS memory pressure level: 1, 2 or 4.");
        let _ = writeln!(out, "ssw_pressure_level {}", self.pressure);
        family(&mut out, "ssw_available_mb", "gauge", "Free, inactive and purgeable memory.");
        let _ = writeln!(out, "ssw_available_mb {}", self.available_mb.unwrap_or(0));
        family(&mut out, "ssw_swap_used_mb", "gauge", "Used swap space.");
        let _ = writeln!(out, "ssw_swap_used_mb {}", self.swap_used_mb);
        family(&mut out, "ssw_hub_footprint_bytes", "gauge", "phys_footprint of the hub process.");
        let _ = writeln!(out, "ssw_hub_footprint_bytes {}", macos::phys_footprint(std::process::id() as i32).unwrap_or(0));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_job_rises_one_class_after_the_aging_time() {
        let s = Duration::from_secs;
        assert_eq!(effective_class(2, s(119), 120), 2);
        assert_eq!(effective_class(2, s(120), 120), 1);
        assert_eq!(effective_class(2, s(9999), 120), 1);
        assert_eq!(effective_class(1, s(9999), 120), 1);
        assert_eq!(effective_class(0, s(0), 120), 0);
        assert_eq!(effective_class(2, s(9999), 0), 2);
    }

    #[test]
    fn an_old_batch_job_goes_before_a_new_interactive_job() {
        // The batch job 1 waited 130 s, so it is class 1 with the older id.
        // The queue picks the smallest (class, id) pair.
        let aged = effective_class(2, Duration::from_secs(130), 120);
        assert_eq!([(1u8, 8u64), (aged, 1)].into_iter().min(), Some((1, 1)));
        // A voice job still goes first, and a fresh batch job goes last.
        assert_eq!([(2u8, 3u64), (1, 5), (0, 7)].into_iter().min(), Some((0, 7)));
    }

    #[test]
    fn the_histogram_counts_each_bucket_at_or_above_the_value() {
        let mut h = Histogram::default();
        h.observe(0.3);
        h.observe(40.0);
        assert_eq!(h.counts, [0, 0, 1, 1, 1, 1, 1, 1, 2, 2]);
        assert_eq!(h.n, 2);
    }
}
