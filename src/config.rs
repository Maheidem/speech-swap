//! The TOML configuration of the hub.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: Server,
    #[serde(default)]
    pub keys: Vec<Key>,
    #[serde(default)]
    pub scheduler: Scheduler,
    #[serde(default)]
    pub memory: Memory,
    #[serde(default)]
    pub languages: Languages,
    #[serde(default)]
    pub chunking: Chunking,
    pub engines: Vec<Engine>,
    /// Virtual models: a chain of an STT engine and an aligner.
    #[serde(default)]
    pub pipelines: Vec<Pipeline>,
    /// Wyoming STT listeners, one model for each port.
    #[serde(default)]
    pub wyoming: Vec<Wyoming>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Server {
    pub listen: String,
    #[serde(default = "d_100")]
    pub max_upload_mb: u64,
    #[serde(default = "d_16")]
    pub field_max_kb: usize,
    pub tmp_dir: String,
    #[serde(default = "d_ffmpeg")]
    pub ffmpeg: String,
    #[serde(default = "d_60")]
    pub ffmpeg_timeout_s: u64,
    /// The largest number of ffmpeg children at one time.
    #[serde(default = "d_2")]
    pub ffmpeg_parallel: usize,
    /// `true` turns authentication off. Use it for tests only.
    #[serde(default)]
    pub open: bool,
    /// The priority class of each request in open mode.
    #[serde(default = "d_1u8")]
    pub open_class: u8,
    /// The model for a request that has no `model` field.
    pub default_model: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Key {
    pub name: String,
    /// The key text. Prefer `key_env`, which keeps the key out of the file.
    pub key: Option<String>,
    /// The name of the environment variable that holds the key.
    pub key_env: Option<String>,
    /// 0 = voice, 1 = interactive, 2 = batch.
    pub class: u8,
    /// A class-0 job of this key can go to the fallback engine of its model.
    #[serde(default)]
    pub allow_fallback: bool,
    /// The resolved key text. `load` fills it.
    #[serde(skip)]
    pub secret: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields, default)]
pub struct Scheduler {
    /// The longest wait in the queue for each class. After it, the job gets 503.
    pub class_timeout_s: [u64; 3],
    /// The largest number of waiting jobs for each class, for each model.
    pub class_max_queue: [usize; 3],
    /// Audio longer than this value makes the job a batch job.
    pub batch_if_audio_longer_s: f64,
    /// A batch job that waits longer than this value rises to class 1.
    pub aging_after_s: u64,
    /// A class-0 or class-1 job cancels a running batch job on the same engine.
    pub preempt: bool,
    pub retry_after_s: u32,
    /// The longest run time of one job in the engine, for each class. 0 = no limit.
    /// After it, the hub cancels the job. If the engine does not stop, the hub stops it.
    pub run_timeout_s: [u64; 3],
}

impl Default for Scheduler {
    fn default() -> Self {
        Scheduler {
            class_timeout_s: [15, 120, 1800],
            class_max_queue: [8, 32, 256],
            batch_if_audio_longer_s: 60.0,
            aging_after_s: 120,
            preempt: true,
            retry_after_s: 5,
            run_timeout_s: [30, 300, 3600],
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields, default)]
pub struct Memory {
    /// The sum of the costs of all resident engines stays at or below this value.
    pub engine_budget_mb: u64,
    /// The system must have the cost of the engine plus this value available before a load.
    pub headroom_mb: u64,
    /// An engine stays resident for this time after a load before it can be evicted.
    pub min_resident_s: u64,
    /// At this pressure level the hub stops idle engines (macOS: 2 = warning).
    pub evict_on_pressure: u32,
    /// At this pressure level the hub does not load engines (macOS: 4 = critical).
    pub refuse_loads_on: u32,
}

impl Default for Memory {
    fn default() -> Self {
        Memory {
            engine_budget_mb: 5200,
            headroom_mb: 400,
            min_resident_s: 20,
            evict_on_pressure: 2,
            refuse_loads_on: 4,
        }
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields, default)]
pub struct Languages {
    /// More names for languages, for example `"brazilian portuguese" = "pt"`.
    pub extra: HashMap<String, String>,
}

/// How the hub cuts long audio for the pipelines (the RMS window cutter).
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields, default)]
pub struct Chunking {
    /// A window is at most this long. The aligner takes at most 31 s.
    pub window_max_s: f64,
    /// The cut falls at or after this time in the window.
    pub window_min_s: f64,
    /// The cut falls in the quietest span of this length.
    pub cut_window_ms: u64,
    /// The length of one frame of the energy curve.
    pub frame_ms: u64,
}

impl Default for Chunking {
    fn default() -> Self {
        Chunking { window_max_s: 30.0, window_min_s: 20.0, cut_window_ms: 200, frame_ms: 20 }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Pipeline {
    pub id: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    /// The STT engine of phase 1.
    pub stt: String,
    /// The aligner engine of phase 2.
    pub align: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Wyoming {
    pub listen: String,
    /// The id of the STT engine for this port.
    pub model: String,
    #[serde(default = "d_wy_languages")]
    pub languages: Vec<String>,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum Adapter {
    /// speech.cpp `speech worker`, JSON Lines protocol 4.
    #[serde(rename = "speech-worker")]
    SpeechWorker,
    /// One JSON line for each request and each answer (see `runners/`).
    #[serde(rename = "jsonl")]
    Jsonl,
    /// A child process with an HTTP server on a private port: the subtitle-align
    /// forced aligner (`POST /align`).
    #[serde(rename = "http-align")]
    HttpAlign,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Engine {
    pub id: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    pub adapter: Adapter,
    pub cmd: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub cwd: Option<String>,
    #[serde(default = "d_60")]
    pub ready_timeout_s: u64,
    /// The memory cost in MB that the planner uses.
    pub cost_mb: u64,
    /// The planner evicts engines with a low value first.
    #[serde(default = "d_1u32")]
    pub evict_cost: u32,
    /// The hub stops the engine after this idle time. 0 = no idle stop.
    #[serde(default)]
    pub ttl_s: u64,
    /// The hub never stops a persistent engine to make room.
    #[serde(default)]
    pub persistent: bool,
    /// The request fields that the engine takes: "language", "prompt".
    #[serde(default)]
    pub fields: Vec<String>,
    /// The language codes that the engine takes. Empty = all codes.
    #[serde(default)]
    pub languages: Vec<String>,
    /// The model that takes a class-0 job when this engine cannot start it at once.
    pub fallback: Option<String>,
    /// The aligner that gives word times for this engine (the pipeline model).
    pub word_timestamps_via: Option<String>,
    /// `http-align`: the base URL of the child, for example `http://127.0.0.1:18131`.
    pub url: Option<String>,
    /// `http-align`: the path that answers 200 when the child is ready.
    #[serde(default = "d_health")]
    pub health: String,
    /// `http-align`: a file with the Bearer token of the child. The hub never logs it.
    pub token_file: Option<String>,
    #[serde(skip)]
    pub token: String,
}

fn d_100() -> u64 {
    100
}
fn d_60() -> u64 {
    60
}
fn d_16() -> usize {
    16
}
fn d_2() -> usize {
    2
}
fn d_1u8() -> u8 {
    1
}
fn d_1u32() -> u32 {
    1
}
fn d_health() -> String {
    "/health".into()
}
fn d_wy_languages() -> Vec<String> {
    vec!["pt".into(), "pt-BR".into(), "en".into()]
}
fn d_ffmpeg() -> String {
    "ffmpeg".into()
}

/// What a model name means.
#[derive(Debug, Clone, PartialEq)]
pub enum Route {
    Engine(usize),
    Pipeline { stt: usize, align: usize, id: String },
}

/// Changes a leading `~/` into the home folder of the user.
pub fn expand_home(s: &str) -> String {
    match (s.strip_prefix("~/"), std::env::var("HOME")) {
        (Some(rest), Ok(home)) => format!("{home}/{rest}"),
        _ => s.to_string(),
    }
}

impl Config {
    pub fn parse(text: &str) -> Result<Config, String> {
        let mut cfg: Config = toml::from_str(text).map_err(|e| e.to_string())?;
        cfg.server.tmp_dir = expand_home(&cfg.server.tmp_dir);
        cfg.server.ffmpeg = expand_home(&cfg.server.ffmpeg);
        if cfg.server.open_class > 2 {
            return Err("server.open_class must be 0, 1 or 2".into());
        }
        if cfg.engines.is_empty() {
            return Err("the config has no [[engines]]".into());
        }
        for e in &mut cfg.engines {
            if e.cmd.is_empty() {
                return Err(format!("engine {}: cmd is empty", e.id));
            }
            for part in e.cmd.iter_mut() {
                *part = expand_home(part);
            }
            for value in e.env.values_mut() {
                *value = expand_home(value);
            }
            e.cwd = e.cwd.as_deref().map(expand_home);
        }
        for e in &mut cfg.engines {
            if e.adapter == Adapter::HttpAlign {
                let Some(url) = &e.url else { return Err(format!("engine {}: http-align needs `url`", e.id)) };
                let Some(rest) = url.strip_prefix("http://") else {
                    return Err(format!("engine {}: `url` must start with http://", e.id));
                };
                if rest.trim_end_matches('/').contains('/') {
                    return Err(format!("engine {}: `url` has no path; use `health` for a path", e.id));
                }
            }
            if let Some(file) = &e.token_file {
                let path = expand_home(file);
                let text = std::fs::read_to_string(&path).map_err(|err| format!("engine {}: token_file {path}: {err}", e.id))?;
                e.token = text.trim().to_string();
            }
        }
        for (i, e) in cfg.engines.iter().enumerate() {
            let mut names = vec![&e.id];
            names.extend(e.aliases.iter());
            for name in names {
                if cfg.engines.iter().enumerate().any(|(j, o)| {
                    j != i && (o.id.eq_ignore_ascii_case(name) || o.aliases.iter().any(|a| a.eq_ignore_ascii_case(name)))
                }) {
                    return Err(format!("the model name {name} is in two engines"));
                }
            }
            if let Some(fb) = &e.fallback {
                if cfg.engine_index(fb).is_none() {
                    return Err(format!("engine {}: fallback {fb} is not a model", e.id));
                }
            }
        }
        for (i, p) in cfg.pipelines.iter().enumerate() {
            let clash = |name: &str| {
                cfg.engine_index(name).is_some()
                    || cfg.pipelines.iter().enumerate().any(|(j, o)| j != i && (o.id.eq_ignore_ascii_case(name) || o.aliases.iter().any(|a| a.eq_ignore_ascii_case(name))))
            };
            for name in std::iter::once(&p.id).chain(p.aliases.iter()) {
                if clash(name) {
                    return Err(format!("the model name {name} is in two models"));
                }
            }
            match cfg.engine_index(&p.stt) {
                Some(i) if cfg.engines[i].adapter != Adapter::HttpAlign => {}
                _ => return Err(format!("pipeline {}: stt {} is not an STT engine", p.id, p.stt)),
            }
            match cfg.engine_index(&p.align) {
                Some(i) if cfg.engines[i].adapter == Adapter::HttpAlign => {}
                _ => return Err(format!("pipeline {}: align {} is not an aligner engine", p.id, p.align)),
            }
        }
        for e in &cfg.engines {
            if let Some(via) = &e.word_timestamps_via {
                match cfg.engine_index(via) {
                    Some(i) if cfg.engines[i].adapter == Adapter::HttpAlign && e.adapter != Adapter::HttpAlign => {}
                    _ => return Err(format!("engine {}: word_timestamps_via {via} is not an aligner engine", e.id)),
                }
            }
        }
        for w in &cfg.wyoming {
            match cfg.engine_index(&w.model) {
                Some(i) if cfg.engines[i].adapter != Adapter::HttpAlign => {}
                _ => return Err(format!("wyoming {}: model {} is not an STT engine", w.listen, w.model)),
            }
        }
        if let Some(model) = &cfg.server.default_model {
            if cfg.resolve(model).is_none() {
                return Err(format!("server.default_model {model} is not a model"));
            }
        }
        let c = &cfg.chunking;
        if !(c.window_min_s > 1.0 && c.window_min_s < c.window_max_s && c.window_max_s <= 31.0 && c.frame_ms > 0 && c.cut_window_ms >= c.frame_ms) {
            return Err("chunking: need 1 < window_min_s < window_max_s <= 31 and cut_window_ms >= frame_ms > 0".into());
        }
        for k in &mut cfg.keys {
            if k.class > 2 {
                return Err(format!("key {}: class must be 0, 1 or 2", k.name));
            }
            k.secret = match (&k.key, &k.key_env) {
                (Some(text), None) => text.clone(),
                (None, Some(var)) => std::env::var(var)
                    .map_err(|_| format!("key {}: the variable {var} is not set", k.name))?,
                _ => return Err(format!("key {}: give `key` or `key_env`, not both", k.name)),
            };
            if k.secret.is_empty() {
                return Err(format!("key {}: the key is empty", k.name));
            }
        }
        if cfg.keys.is_empty() && !cfg.server.open {
            return Err("the config has no [[keys]]; add keys or set server.open = true".into());
        }
        Ok(cfg)
    }

    pub fn load(path: &str) -> Result<Config, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
        Config::parse(&text).map_err(|e| format!("{path}: {e}"))
    }

    /// Finds an engine by its id or by one of its aliases. Case does not matter.
    pub fn engine_index(&self, model: &str) -> Option<usize> {
        self.engines.iter().position(|e| {
            e.id.eq_ignore_ascii_case(model) || e.aliases.iter().any(|a| a.eq_ignore_ascii_case(model))
        })
    }

    pub fn pipeline_index(&self, model: &str) -> Option<usize> {
        self.pipelines.iter().position(|p| {
            p.id.eq_ignore_ascii_case(model) || p.aliases.iter().any(|a| a.eq_ignore_ascii_case(model))
        })
    }

    /// Finds a model: an engine or a pipeline.
    pub fn resolve(&self, model: &str) -> Option<Route> {
        if let Some(i) = self.engine_index(model) {
            return Some(Route::Engine(i));
        }
        self.pipeline_index(model).map(|i| Route::Pipeline {
            stt: self.engine_index(&self.pipelines[i].stt).expect("checked at start"),
            align: self.engine_index(&self.pipelines[i].align).expect("checked at start"),
            id: self.pipelines[i].id.clone(),
        })
    }

    /// The pipeline of an engine that has `word_timestamps_via`: STT engine and aligner.
    pub fn implicit_pipeline(&self, engine: usize) -> Option<(usize, usize)> {
        let via = self.engines[engine].word_timestamps_via.as_deref()?;
        Some((engine, self.engine_index(via)?))
    }

    pub fn model_ids(&self) -> Vec<&str> {
        self.engines.iter().map(|e| e.id.as_str()).chain(self.pipelines.iter().map(|p| p.id.as_str())).collect()
    }

    pub fn tmp_dir(&self) -> PathBuf {
        PathBuf::from(&self.server.tmp_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_example_config_parses() {
        let text = include_str!("../examples/m1.toml");
        std::env::set_var("SSW_KEY_VOICE", "k0");
        std::env::set_var("SSW_KEY_APP", "k1");
        std::env::set_var("SSW_KEY_BATCH", "k2");
        // The token file of the example does not exist on a test machine.
        let text: String = text.lines().filter(|l| !l.starts_with("token_file")).collect::<Vec<_>>().join("\n");
        let cfg = Config::parse(&text).unwrap();
        assert_eq!(cfg.model_ids(), ["Qwen3-ASR-1.7B", "parakeet-tdt-0.6b-v3", "Qwen3-ForcedAligner-0.6B", "qwen3-asr+align"]);
        assert_eq!(cfg.resolve("qwen3-asr+align"), Some(Route::Pipeline { stt: 0, align: 2, id: "qwen3-asr+align".into() }));
        assert_eq!(cfg.implicit_pipeline(0), Some((0, 2)));
        assert_eq!(cfg.wyoming.len(), 2);
        assert_eq!(cfg.engines[2].cost_mb, 3400);
        assert_eq!(cfg.engine_index("whisper-1"), Some(0));
        assert_eq!(cfg.engine_index("PARAKEET"), Some(1));
        assert_eq!(cfg.engines[0].cost_mb, 2500);
        assert!(!cfg.engines[0].cmd[0].starts_with('~'));
    }

    #[test]
    fn a_config_without_keys_needs_open_mode() {
        let base = "[server]\nlisten='127.0.0.1:1'\ntmp_dir='/tmp/x'\n[[engines]]\nid='a'\nadapter='jsonl'\ncmd=['x']\ncost_mb=1\n";
        assert!(Config::parse(base).is_err());
        assert!(Config::parse(&base.replace("tmp_dir", "open=true\ntmp_dir")).is_ok());
    }

    #[test]
    fn an_unknown_config_key_is_an_error() {
        let text = "[server]\nlisten='x'\ntmp_dir='/tmp/x'\nopen=true\nwrong=1\n[[engines]]\nid='a'\nadapter='jsonl'\ncmd=['x']\ncost_mb=1\n";
        assert!(Config::parse(text).is_err());
    }
}
