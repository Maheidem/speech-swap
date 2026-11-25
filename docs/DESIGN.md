# speech-swap: design

Status: design. Phase M0 (measurement) is in progress. The hub is not built yet.

The first target host is an 8 GB M1 MacBook Air with macOS 26 ("the M1 host").
Each hub number in this document is an **estimate**, unless the text says "measured".
Each engine number comes from a bench on that 8 GB M1 Air, or from the cited documentation.

---

## 1. Decision: build a new hub, do not extend llama-swap

**Decision:** build a new, small Rust binary. Do not fork llama-swap. Copy four design ideas from it (MIT license).

**Reasons:**

1. **Footprint.** The hub must stay at a few MB. llama-swap is a Go program with many linked libraries. Its darwin_arm64 release archive is about 23 MB compressed (release v275). Its memory use was **not measured**.
2. **It is not a library.** All its code is in `internal/`, so another Go module cannot import it. It has no plugin API.
3. **Many functions are missing for speech.** These gaps are in its code:
   - Field filters do not apply to multipart forms (`internal/server/filters.go`).
   - It does not transcode audio.
   - Above the limit, it answers 429 immediately. A request cannot wait in a queue (`internal/router/scheduler/fifo.go`).
   - Priority is per model, not per request.
   - It has no stdin/stdout engine, no chaining and no Wyoming protocol.
4. **Maintenance cost.** A fork needs about 1,500-2,500 new lines (estimate) in a code base that changes quickly.

**What the hub copies from llama-swap as design (MIT):**

- one scheduler loop without locks, with the Effects pattern (`internal/router/design.md`);
- the `capacity` planner with pools and `evict_cost` (`internal/router/capacity.go`);
- `EnsureReady`, which prevents a race between simultaneous loads;
- the TTL loop and the health check loop (`internal/process/process_command.go`).

**Fallback, only if time is short:** run llama-swap with the `capacity` router behind a small proxy. The proxy does the transcode, the normalization and the queue.

---

## 2. Repository layout

One crate, about 2,500-3,500 lines of Rust (**estimate**). Only the parts that M0 needs exist today.

```
speech-swap/
  Cargo.toml              # release: opt-level="z", lto="fat", codegen-units=1, panic="abort", strip=true
  README.md  LICENSE
  examples/m1.toml
  contrib/launchd/com.example.speech-swap.plist
  src/
    main.rs               # current_thread runtime, config, listeners, signals, child cleanup
    config.rs             # serde + toml
    auth.rs               # Bearer / x-api-key, key -> class
    http/
      router.rs
      transcriptions.rs   # POST /v1/audio/transcriptions
      alignments.rs       # POST /v1/audio/alignments
      legacy_align.rs     # POST /align (legacy JSON contract)
      models.rs           # GET /v1/models
      admin.rs            # /health /running /admin/* /logs
      errors.rs           # double body: error{} + detail
    normalize/
      audio.rs            # ffmpeg child, duration, RMS energy cutter
      lang.rs             # ISO / BCP-47 / full name, in the two directions
      fields.rs           # whitelist per engine
    sched/
      actor.rs            # single loop, Effects enum
      queue.rs            # heap per model, (class, seq), aging
      planner.rs          # pure function: load / evict / wait / fallback (with tests)
      memory.rs           # cost ledger, max of peaks, live probe
      lifecycle.rs        # EnsureReady, TTL, health, process group kill
    engines/
      speech_worker.rs    # speech.cpp JSONL protocol 4
      jsonl.rs            # hub-jsonl v1 protocol (FluidAudio runner)
      http_align.rs       # aligner service (local HTTP)
      http_openai.rs      # reserve: an OpenAI-style HTTP engine
    pipeline.rs           # stt -> align, in phases
    format.rs             # json / text / verbose_json / srt / vtt / SSE
    wyoming.rs            # codec + describe/info/transcribe/transcript
    metrics.rs            # Prometheus text, written by hand
    logring.rs            # 256 KB ring buffer
    macos.rs              # sysctlbyname, host_statistics64, proc_pid_rusage
  runners/fluidaudio/     # Swift runner with id + pcm_path + tokenTimings
  tests/
    planner_tests.rs      # example: "the two GPU engines are never resident together"
    e2e/                  # scripts with real clients against the real hub
```

---

## 3. Architecture

### 3.1 Components and processes

- The hub is **one process** (`speech-swap`) with tokio `current_thread` (one thread). On macOS it runs as a launchd job of a normal user.
- The hub **does not run models**. Each engine is a child process. The hub starts and stops that process.
- The **scheduler** is one task that owns all the state: queues, the engine table and the memory ledger. Handlers send a `Job` through an mpsc channel and wait for the answer on a oneshot channel.
- Each engine has a **supervisor task**. It owns the pid, the pipes and the stderr reader, which feeds the log ring. Each child runs in its own process group, so one kill also stops its children.

### 3.2 Diagram

```
 clients                           speech-swap (1 process, 1 thread)                  children (on demand)
 -------                           ---------------------------------                  --------------------
 dictation app   --HTTP--+
 messaging gateway -HTTP-+--> [auth] -> [multer: file -> $TMPDIR/ssw-<id>.in]
 subtitle tool   --HTTP--+          -> [normalize: fields, language, format]
                                     -> [ffmpeg child -> ssw-<id>.pcm s16le 16k mono]
 Home Assistant --Wyoming TCP--> [wyoming codec] -> PCM (resample if rate != 16000)
                                          |
                                          v
                               [scheduler actor]
                                - heap per model (class, seq) + aging
                                - memory ledger (MB) + live probe (macos.rs)
                                - planner: load / evict / wait / fallback
                                - pipeline: STT phase, swap, align phase
                                          |  Effects: Spawn, Kill, Dispatch, Cancel
                                          v
                               [supervisors]
                                - speech_worker --stdin/stdout JSONL p4--> speech.cpp worker, Qwen3-ASR-1.7B Q8_0 (Metal GPU)
                                - jsonl         --stdin/stdout v1------> FluidAudio runner, Parakeet v3 (ANE)
                                - http_align    --HTTP on loopback-----> aligner service (MLX, GPU)
                               [metrics] [log ring 256 KB] [phys_footprint sampler per pid]
```

### 3.3 Flow of one transcription

1. `auth` reads `Authorization: Bearer` or `x-api-key` and finds the key and its class.
2. `multer` writes the `file` part to `$TMPDIR/ssw-<id>.in` in chunks. Text fields stay in memory, with a maximum of 16 KB each. The upload limit is 100 MiB. A larger upload gets status 413.
3. The hub normalizes the fields: language, whitelist and model alias (section 4.3).
4. The hub runs `ffmpeg -nostdin -i in -ac 1 -ar 16000 -f s16le -c:a pcm_s16le out.pcm`. Measured on an 8 GB M1 Air: 0.03 s and 3.18 MB peak footprint for a short voice command. The duration is `bytes / 32000`. If the decode fails, the answer is 400.
5. The hub gives the job a class (section 5.1) and sends the job to the scheduler.
6. The scheduler makes the engine ready (`EnsureReady`) and dispatches the job. Long batch audio goes in chunks (section 5.5).
7. The formatter makes the response. A Drop guard deletes the temporary files.
8. If the client disconnects, the Drop of the handler sends `Cancel(id)`.

The hub never holds a full audio file in RAM, except for Wyoming. There, the PCM stays in a buffer, at 32 KB per second.

---

## 4. External API

### 4.1 Routes

| Route | Function | Contract |
|---|---|---|
| `POST /v1/audio/transcriptions` | OpenAI-style STT | OpenAI spec, tolerant of extra and wrong fields |
| `GET /v1/models`, `GET /v1/models/{id}` | List of engines and pipelines | `{"object":"list","data":[{id, object:"model", created, owned_by:"speech-swap", "speech_swap":{kind, resident}}]}` |
| `POST /v1/audio/alignments` | Forced alignment | multipart `file`, `model`, `text`, `language` |
| `POST /align` | Legacy alignment contract | JSON `{audio_b64, text, language}` → `{"words":[{"text","start","end"}],"model","ms"}` |
| `GET /health` | Liveness | 200 if the actor answers in 1 s, plus the state of each engine |
| `GET /running` | Resident engines | JSON |
| `GET /metrics` | Prometheus | see 4.5 |
| `POST /admin/load/{model}`, `POST /admin/unload/{model}` | Manual control | Needs an admin key |
| `GET /logs?engine=&tail=N` | End of the log ring | Needs an admin key |
| TCP Wyoming ports | Wyoming ASR | No auth, by design of the Wyoming protocol |

### 4.2 Fields of `/v1/audio/transcriptions`

| Field | Hub rule |
|---|---|
| `file` | Each format that ffmpeg reads: flac, mp3, mp4, mpeg, mpga, m4a, ogg/opus, wav, webm, mkv, mov. The hub finds the format from the content, not from the extension. Raw OGG/Opus voice notes work. |
| `model` | The hub resolves the id or an alias. An unknown id gets 404 with the model list. |
| `language` | See section 4.3. If the field is absent, the value is `auto`. |
| `prompt` | Goes to the engines that accept a prompt. For other engines, the hub drops the field and writes a debug log. It never causes an error. |
| `response_format` | `json` (default), `text`, `srt`, `vtt`, `verbose_json`. `diarized_json` gets 400. An unknown value becomes `json` and the hub writes a warning. `text`, `srt` and `vtt` return a plain text body. |
| `temperature` | The hub accepts and **drops** it. A non-numeric value is also ignored. It never goes to an engine that refuses it. |
| `stream` | `true` gives SSE: `transcript.text.delta` and then `transcript.text.done`. An engine that cannot stream sends one delta. |
| `timestamp_granularities[]` | `segment`: segments from the hub chunks. `word`: chains to the aligner, if the model config sets that. |
| `max_tokens` | Goes to the engine only if the engine accepts the field. |
| Any other field | The hub accepts and ignores it. It never gives 400. |

**`json` response (superset of the OpenAI shape):** `{text, language, duration, segments?, usage:{type:"duration", seconds}}`.

**`verbose_json`:** `{task:"transcribe", language, duration, text, segments[{id, seek, start, end, text, tokens:[], temperature:0, avg_logprob:0, compression_ratio:0, no_speech_prob:0}], words?[{word,start,end}], usage}`. If the engine gives no segments, the hub makes one segment from 0 to the duration, or one segment per hub chunk.

### 4.3 Compatibility rules (normalization in the hub)

1. **Audio:** each upload goes through ffmpeg and becomes 16 kHz mono s16le, for all extensions.
2. **Language**, in this order:
   1. The hub removes spaces. An empty value or `auto` means auto.
   2. A BCP-47 tag keeps only the primary subtag (`pt-BR` → `pt`).
   3. A full name, in any letter case, becomes an ISO code (`English` → `en`, `Português` → `pt`). The table covers the 30 languages of Qwen3-ASR, plus the `extra` entries of the config.
   4. The hub converts the value to the form of the engine, from the `lang_form` field: `code`, `name` or `none`.
   5. A value that is not in the table becomes `auto`, and the hub writes a warning. It is not an error.
3. **An absent language means `auto`.** The hub always sends the language field to the engine. Thus the default language of an engine never applies silently.
4. **Whitelist per engine:** the hub sends only the fields in the `fields` list of the engine and drops the others.
5. **Parakeet:** it has no language input. The hub ignores `language` for it and returns the requested language in the response.
6. **Auth:** Bearer or `x-api-key`, with many keys. If no key is configured, the hub accepts only a loopback bind.
7. **Errors:** the body has the two common shapes, `{"error":{message,type,param,code},"detail":"..."}`, because clients read different shapes. A client fault gives 4xx, never 500: 400 (decode), 401, 404 (model), 413 (size). A full queue gives 503 with `Retry-After`.

### 4.4 Alignment

- `POST /v1/audio/alignments`: multipart `file`, `model`, `text`, `language` (necessary).
  Response: `{text, language, words:[{word, start, end, start_sample, end_sample, confidence}], timing:{wall_ms, audio_duration_ms, rtf}}`. If the engine gives no `confidence`, the hub sends `null`.
- `POST /align`: legacy JSON. The hub accepts `audio_b64` in any format, sends it through ffmpeg and answers in the legacy shape. The client changes only the URL. The token of the aligner service stays inside the hub.
- **31 s limit:** above 31 s, the hub cuts the audio into windows (section 5.6) and adds the start of each window to the times.

### 4.5 Wyoming and metrics

**Wyoming:**

- Framing: one JSON header line with `\n`, then `data_length` bytes of JSON, then `payload_length` bytes of PCM.
- The hub answers `describe` at any time with `info`. The `info` lists the ASR models with ISO and BCP-47 languages.
- `transcribe.name` selects the model. Without a name, the `default_model` of the port applies.
- Home Assistant sends 16000 Hz, width 2, 1 channel. If `rate != 16000`, the hub resamples through ffmpeg.
- The hub ignores events and keys that it does not know.
- Each Wyoming job is in class 0.

**Prometheus metrics:**

- `ssw_requests_total{model,key,class,code}`
- `ssw_queue_depth{model,class}`
- `ssw_wait_seconds` and `ssw_run_seconds` (histograms)
- `ssw_engine_loaded{model}`, `ssw_engine_footprint_bytes{model}`, `ssw_engine_cost_mb{model}`
- `ssw_swaps_total{from,to}`, `ssw_fallback_total{from,to,key}`
- `ssw_pressure_level`, `ssw_available_mb`, `ssw_swap_used_mb`
- `ssw_hub_footprint_bytes`

---

## 5. Scheduler

### 5.1 Priority classes

| Class | Who | How the hub assigns it | Queue limit | Timeout |
|---|---|---|---|---|
| 0 `voice` | Voice assistant (Wyoming and HTTP) | Wyoming listener, or a key with `class=0` | 8 | 15 s |
| 1 `interactive` | Dictation, voice notes | Class of the key, with duration ≤ 60 s | 32 | 120 s |
| 2 `batch` | Subtitles, long files, pipelines | Class of the key, or **any** job with duration > 60 s | 256 | 1800 s |

- A client can only **lower** its own class, with the header `X-Priority: batch`. It can never raise the class.
- Order: the lowest class first, and FIFO inside a class.
- **Aging:** a batch job that waits more than 120 s goes up one class, so batch work does not starve.
- A full queue gives 503 with `Retry-After`.
- If the client disconnects, the job leaves the queue. If the job runs in the worker, the hub sends `cancel`.

### 5.2 Parallelism per engine

| Engine | `parallel` | Reason |
|---|---|---|
| speech.cpp worker (Qwen) | 1 | The worker runs one job at a time. The hub sends the `transcribe` of the next job **only after** the `end` of the current job. Thus the FIFO of the worker never overrides the priority of the hub. |
| FluidAudio runner | 1 | Default value. It runs on the ANE and does not compete with the Qwen engine for the GPU. |
| aligner service | 1 | It answers 503 when it is busy. |
| Global | no limit | The two GPU engines are never resident together, so they do not compete. |

### 5.3 Memory budget (numbers from an 8 GB M1 Air)

The hub decides from `phys_footprint`, never from RSS. On the M1 Air, `ps` showed 832 KB of RSS for a speech server, and `footprint` showed 2181 MB for the same process.

| Engine | Configured cost (`cost_mb`) | Source |
|---|---|---|
| Qwen3-ASR-1.7B Q8_0 (speech.cpp) | 2400 | 2181 MB phys_footprint, measured on an 8 GB M1 Air |
| Parakeet v3 (FluidAudio) | 300 | 101 MB RSS measured, plus a reserve for the ANE, which RSS does not show. **A guess.** |
| Qwen3-ForcedAligner (MLX) | 3300 | 1.35-3.3 GB per call in a load test, measured on an 8 GB M1 Air |

- `engine_budget_mb = 5200`. **Hypothesis:** 8192 MB minus about 3000 MB for macOS, the hub and ffmpeg. Phase M0 calibrates it.
- Possible sets:
  - {Parakeet, Qwen} = 2700 MB: fits.
  - {Parakeet, aligner} = 3600 MB: fits.
  - {Qwen, aligner} = 5700 MB > 5200 MB: **never resident together**. This pair caused 1% free RAM and 876 MB of swap in a test.
- **Learned cost:** `cost = max(cost_mb, highest phys_footprint peak seen)`. The hub samples each child every 1 s during a job (`proc_pid_rusage`, `ri_phys_footprint`). The hub uses the maximum and **not** a moving average. A moving average makes the aligner peaks look smaller than they are.

**Live gate before each load** (all three conditions must pass):

1. `resident_cost + cost(new) <= engine_budget_mb` (after the planned evictions).
2. `available_mb >= cost(new) + headroom_mb` (headroom 400). `available_mb = (free + inactive + purgeable) × page size`, from `host_statistics64(HOST_VM_INFO64)`.
3. `kern.memorystatus_vm_pressure_level == 1` (normal).

The hub polls `kern.memorystatus_vm_pressure_level` and `vm.swapusage` every 2 s.

- Level 2 (warning): the hub stops idle engines that are not persistent, the largest footprint first.
- Level 4 (critical): the hub also refuses new loads and gives 503 to class 2.

### 5.4 Engine swap (the planner is a pure function)

When the head of the queue of model M has no ready engine:

1. If M is loading, the job waits (`EnsureReady`).
2. If the gate passes, the hub starts M.
3. If the gate fails, the hub builds the eviction set from the resident engines that are not persistent, **idle**, and have no queued job of an equal or better class. The order is `evict_cost` ascending, then `last_used` ascending. The hub takes the smallest prefix that frees sufficient MB and checks the gate again.
4. If the only candidates are busy, the job waits. A batch job never evicts an engine that has class 0 or class 1 work in its queue.
5. **Anti-thrash:** `min_resident_s = 20` after a load, except in critical pressure. The hub empties the full aligner queue before it goes back to the Qwen engine.
6. **TTL:** Qwen `ttl_s = 0` (resident by default), aligner 120 s, Parakeet `persistent`.
7. **Unload:**
   - worker and runner: the hub closes stdin, and the worker exits with 0;
   - HTTP child: SIGTERM to the process group, then SIGKILL after 5 s.
8. **Cold start cost:**
   - Qwen: about 2.0 s to health for the server form, measured on an 8 GB M1 Air. The worker form was **not measured**.
   - Parakeet: 170-484 ms, measured on an 8 GB M1 Air.
   - aligner: **unknown**.

**Voice fallback (opt-in per key):** if a class 0 job for the Qwen engine has an estimated wait above `fallback_after_ms` (1500), and the key has `allow_fallback = true`, the job goes to Parakeet on the ANE. The hub counts each case in `ssw_fallback_total` and puts the real `"model"` in the response. Risk: Parakeet has no language setting and writes English for some non-English commands. In the bench on an 8 GB M1 Air, its WER was 0.213 against 0.168 for Qwen (Brazilian Portuguese voice commands).

### 5.5 Batch chunks (preemption at chunk level)

- The hub cuts audio longer than 35 s into chunks of 25-35 s. The cut is at the 200 ms window with the lowest RMS energy (20 ms frames) in that range.
- Each chunk is a separate dispatch. A voice command then waits for **one chunk at most**. Estimate: about 5 s. This was **not measured**.
- The hub joins the texts and adds the start of each chunk to the times.
- The cut is per model (`batch_chunk = true`). The effect on WER was **not measured**. Phase M0 measures it.
- Reason: the worker form does not accept a VAD model, and Qwen3-ASR gives no timestamps. Thus the hub must cut the audio.

### 5.6 Chain STT → align

Pipelines are virtual models. They show in `/v1/models`.

Pipeline `qwen3-asr+align` (class 2, in phases, with a maximum of 2 swaps per request):

1. **STT phase:** the hub cuts the audio into windows of ≤ 30 s with the RMS cutter and transcribes all windows with Qwen.
2. **Swap:** the planner evicts Qwen (rule 5700 > 5200) and starts the aligner. The hub releases Qwen only if no class 0 or class 1 job waits for it. If one waits, the align phase waits.
3. **Align phase:** for each window, the hub sends the WAV slice, the window text and the language code.
4. **Merge:** the hub adds the window start to the times and builds `words[]`, `segments[]`, srt or vtt.

Triggers:

- `model = "qwen3-asr+align"`;
- or an STT model with `timestamp_granularities[]=word` and `word_timestamps_via` set in the config.

Alternative without a swap: `parakeet+words` uses the token timings of FluidAudio. Its quality for subtitles was **not measured**.

---

## 6. Integration of each engine

### 6.1 Qwen3-ASR-1.7B (speech.cpp 0.11.2): `speech worker`

- **Choice:** use the `speech_worker` adapter (JSONL protocol 4). Gains:
  - cancel in less than one second;
  - priority order through the `transcribe` lines;
  - no WAV-only rule and no 400 for `temperature`.
- **Condition:** phase M0 must measure the worker on the M1 host (load time, footprint, WER equal to the server form). If the gate fails, the hub uses `http_openai` against the server form, and the RMS cutter stays in the hub.
- Protocol:
  1. The hub waits for `{"type":"ready","protocol":4,...}`. If `protocol != 4`, the hub marks the engine as failed and writes a clear error.
  2. The hub sends `{"type":"chunk","id","seq","pcm":"<b64 s16le>"}` lines, with 1 s chunks (32 KB, about 43 KB in base64).
  3. The hub sends `{"type":"transcribe","id","sample_rate":16000,"language":"pt"|"auto","prompt"?}`.
  4. The hub reads `progress` and then `end`, `error` or `cancelled`.
  5. Health: the `info` request answers immediately, also during a job.
- Whitelist: `language`, `prompt`. No other field.
- **Change in the engine:** none.

### 6.2 Parakeet TDT 0.6B v3 (FluidAudio 0.18.4, ANE)

- A small Swift runner (in `runners/fluidaudio/`) with this protocol:
  - input: `{"id","pcm_path":"/…/ssw-<id>.pcm","sample_rate":16000}`, without base64;
  - output: `{"id","text","segments","words"?,"engine_ms"}` or `{"id","error"}`;
  - the runner emits the token timings of FluidAudio.
- The ready line is `{"ready":true,"load_ms":…}`.
- `persistent = true`, `fields = []`, `lang_form = "none"`.

### 6.3 Qwen3-ForcedAligner-0.6B (aligner service, MLX)

- The aligner is a small local HTTP server around an MLX forced aligner. It runs in a Python venv.
- **Rule:** the aligner must bind to loopback, on a port that the hub selects with `HOST` and `PORT` in the environment. If it listens on all interfaces, clients can go around the hub queue.
- Token: the hub reads a token file (mode 0600) and sends the Bearer token to the child.
- Health: `GET /health` → `{"ok": true}`.
- Input: 16 kHz mono WAV ≤ 31 s, language as a code.
- The TTL of the hub (120 s) decides the unload.
- **Possible later change, config only:** another aligner runtime with a smaller footprint (a GGUF Q8 build or a CoreML INT8 build). These were **not measured** on the M1 host. If one passes, `cost_mb` goes below 1000, and Qwen and the aligner fit together.

### 6.4 ffmpeg

- The hub uses ffmpeg **only as a child process**. The hub never links libav. Thus the license of the hub stays MIT. This is a reading of the aggregation rule, not legal advice.

---

## 7. Example TOML config (8 GB M1 Air)

```toml
[server]
listen        = "0.0.0.0:80"
max_upload_mb = 100
field_max_kb  = 16
tmp_dir       = "/Users/you/.cache/speech-swap/tmp"
ffmpeg        = "/opt/homebrew/bin/ffmpeg"
ffmpeg_timeout_s = 60
log_ring_kb   = 256

# Keys come from the environment. No key is in the file.
[[keys]]
name = "dictation"
key_env = "SSW_KEY_DICTATION"
class = 1
[[keys]]
name = "subtitles"
key_env = "SSW_KEY_SUBTITLES"
class = 2
[[keys]]
name = "voice-assistant"
key_env = "SSW_KEY_VOICE"
class = 0
allow_fallback = true             # opt-in: Qwen -> Parakeet when the wait is long
[[keys]]
name = "admin"
key_env = "SSW_KEY_ADMIN"
class = 1
admin = true

[scheduler]
class_timeout_s         = [15, 120, 1800]
class_max_queue         = [8, 32, 256]
batch_if_audio_longer_s = 60
aging_after_s           = 120
priority_header         = "X-Priority"   # can only lower the class

[chunking]
chunk_min_s = 25
chunk_max_s = 35
cut_window_ms = 200
frame_ms = 20

[memory]
engine_budget_mb   = 5200          # hypothesis, calibrate in M0
headroom_mb        = 400           # hypothesis, calibrate in M0
poll_s             = 2
sample_footprint_s = 1
min_resident_s     = 20
evict_on_pressure  = 2             # warning
refuse_loads_on    = 4             # critical

[languages]
default = "auto"
extra = { "portugues" = "pt", "português" = "pt", "brazilian portuguese" = "pt" }

[[engines]]
id       = "Qwen3-ASR-1.7B"
aliases  = ["qwen3-asr", "whisper-1"]
kind     = "stt"
adapter  = "speech-worker"
cmd      = ["/path/to/speech", "worker", "/path/to/models/Qwen3-ASR-1.7B-Q8_0.gguf"]
expect_protocol   = 4
ready_timeout_s   = 60
cost_mb           = 2400
evict_cost        = 5
ttl_s             = 0              # resident by default
parallel          = 1
fields            = ["language", "prompt"]
lang_form         = "code"
batch_chunk       = true
fallback          = "parakeet-tdt-0.6b-v3"
fallback_after_ms = 1500           # class 0 only, and only a key with allow_fallback
word_timestamps_via = "Qwen3-ForcedAligner-0.6B"

[[engines]]
id       = "parakeet-tdt-0.6b-v3"
aliases  = ["parakeet"]
kind     = "stt"
adapter  = "jsonl"
cmd      = ["/path/to/fluidaudio-runner"]
ready_timeout_s = 30
cost_mb    = 300                   # 101 MB RSS + ANE reserve (a guess)
persistent = true
parallel   = 1
fields     = []
lang_form  = "none"
batch_chunk = true

[[engines]]
id       = "Qwen3-ForcedAligner-0.6B"
kind     = "aligner"
adapter  = "http-align"
cmd      = ["/path/to/aligner/.venv/bin/python", "/path/to/aligner/launch.py"]
cwd      = "/path/to/aligner"
env      = { HOST = "127.0.0.1", PORT = "18131" }
url      = "http://127.0.0.1:18131"
health   = "/health"
token_file = "/path/to/aligner/token"
ready_timeout_s = 120
cost_mb    = 3300
evict_cost = 1
ttl_s      = 120
parallel   = 1
max_audio_s = 31
lang_form  = "code"

[[pipelines]]
id    = "qwen3-asr+align"
class = 2
steps = [
  { step = "stt",   model = "Qwen3-ASR-1.7B", window_s = 30 },
  { step = "align", model = "Qwen3-ForcedAligner-0.6B" },
]

[[pipelines]]
id    = "parakeet+words"
class = 1
steps = [ { step = "stt", model = "parakeet-tdt-0.6b-v3", words = "token_timings" } ]

[[wyoming]]
listen        = "0.0.0.0:10300"
default_model = "parakeet-tdt-0.6b-v3"
models        = ["parakeet-tdt-0.6b-v3", "Qwen3-ASR-1.7B"]
languages     = ["pt", "pt-BR", "en"]
```

---

## 8. Hub footprint: estimate and method

**Estimates made before M0:**

| State | Estimate | Basis |
|---|---|---|
| Idle hub | 3-6 MB phys_footprint | a public hyper + tokio memory bench on Linux, system allocator |
| Per HTTP upload | + tens of KB | multer buffer, fields ≤ 16 KB, file on disk |
| Per active Qwen job | + about 75 KB | 1 s PCM chunk (32 KB) + base64 (43 KB) |
| Per Wyoming request | + 32 KB per second of audio | a 10 s command ≈ 320 KB |
| Per align window | + about 2.3 MB | WAV slice of ≤ 30 s (960 KB) + base64 (1.3 MB) |
| ffmpeg (child) | 3.18 MB peak, about 0.03 s | **measured on an 8 GB M1 Air**, short voice command |
| Binary | unknown, "a few MB" | profile z + fat LTO |

Choices that keep the footprint low: the system allocator, hyper without a web framework, no tracing subscriber, Prometheus and Wyoming written by hand, one thread.

**Method on the M1 host** (proposed M0 gate: idle ≤ 6 MB, load ≤ 15 MB):

1. `footprint <pid>`: the primary metric.
2. `ps -o rss=,vsz= -p <pid>`: reference only, because RSS is not reliable on macOS.
3. `vmmap --summary <pid> | tail -20`: the split by region.
4. `/usr/bin/time -l ./probe`: the maximum resident set size and the peak memory footprint at exit.
5. Measure three states: idle, during a 30 MB upload, and during 20 parallel uploads.

The measured results of the probe are in the README, section "Measurements (M0)".

---

## 9. Plan in phases

General rule: each phase ends with real commands and their real output.

### M0: measurement gates

1. Install the Rust toolchain on the build machine.
2. Build a hyper + multer probe and run it on the M1 host. Measure the footprint as in section 8.
3. Measure the `speech worker` form on the M1 host: cold and warm load time, phys_footprint, and one WER run.
4. Measure the aligner: cold load time and peak footprint per call.
5. Measure the WER effect of the RMS cutter on long audio against a VAD-based cutter.
6. Capture a real Wyoming `transcribe` event from Home Assistant, to see the form of `language`.
7. Test a bind on port 80 by a process without root on macOS 26.
8. Optional: measure the memory of llama-swap with `/usr/bin/time -l`.

**Done when:** the numbers are written down and the worker form or the server form is selected from the result of step 3.

### M1: MVP on a test port

Scope:

- `POST /v1/audio/transcriptions`, `GET /v1/models`, auth with classes;
- ffmpeg, language, whitelist, json/text/verbose_json/srt/vtt, SSE with one delta;
- the `speech-worker` adapter (or `http-openai`, from M0) and the `jsonl` adapter;
- queues with priority and aging, the batch cutter, the planner with tests, the memory gate;
- the opt-in fallback, `/health`, `/metrics`, `/logs`.

**Done when** all these cases pass, with the command and the output read back:

1. **Contract, one curl per client type:** WAV + Bearer + `prompt`; raw OGG/Opus + `language=pt`; `temperature=0`; `language=English`; no `language` (the answer must not be forced to one language); formats `text` and `srt`; unknown model gives 404 with the list; a 101 MiB upload gives 413.
2. **WER:** the bench harness against the hub gives the same WER as the direct engine, with a tolerance of ±0.005 (proposal).
3. **Load:** a voice command every 10 s during a long batch job. Voice commands stay inside the 15 s timeout. Class 0 wait p95 ≤ 6 s (proposal). No new swap in `vm.swapusage`.
4. **Footprint:** the hub stays inside the M0 gate.

### M2: aligner, chaining and Wyoming

Scope: the `http-align` adapter, `POST /v1/audio/alignments`, the legacy `POST /align`, the `qwen3-asr+align` pipeline in phases, the Wyoming listener.

**Done when:**

1. A 30 s chunk through the hub gives the same words and times as the direct path.
2. A Wyoming client receives a transcript.
3. During a pipeline, `vm.swapusage` does not grow more than 50 MB (proposal), and `ssw_swaps_total` shows a maximum of 2 swaps per request.

### M3: cutover and robustness

1. Stop the old separate services.
2. Start the hub on its final ports.
3. Change the URL of each client.

**Done when:**

1. A real voice command, during a 300 s batch job, gets its answer in 15 s. The proof is the screen of the voice assistant and `ssw_wait_seconds{class="0"}`.
2. Each client does one real operation through its own screen or flow.

### M4: open source release

- README with the example config, e2e scripts, a launchd plist, a release build for aarch64-apple-darwin.
- Optional: test a smaller aligner runtime through config only.

**Done when:** a clean clone builds, and the e2e test passes on the M1 host from the README.

---

## 10. Risks

- The 3-6 MB idle estimate came from a Linux bench. M0 measures it on macOS.
- ANE memory does not show in RSS or in phys_footprint. The 300 MB cost of Parakeet is a guess. Only the live gate protects the host.
- The 3300 MB peak of the MLX aligner is the worst value **seen**, not a limit. The learned cost reacts only after the event.
- `engine_budget_mb = 5200` and `headroom_mb = 400` are hypotheses. A wrong value causes swap or too many evictions.
- The speech.cpp worker uses protocol 4 of a young project. An upgrade can break the adapter. The hub refuses a different protocol, but then the service stops.
- The RMS cutter can make WER worse on long audio. M0 measures this.
- The server form of speech.cpp has no HTTP cancel. If M0 selects the server form, a voice job waits for one full chunk.
- The error shape (`error{}` or `detail`) that each client reads is unknown. The hub sends the two shapes.
- The licenses of the engines for redistribution of configs or examples are not verified.

---

## 11. Known gaps

This design does **not** cover or verify these items:

1. **Worker form of speech.cpp:** load time, footprint and WER are not measured. The MVP depends on M0 to select the adapter.
2. **RMS cutter:** there is no proof that it keeps the WER of a VAD-based cutter. The cut can also be in the middle of a word in continuous speech.
3. **Alignment of hub windows:** it is unknown if the text of a window aligns well when the cut is near a word.
4. **Real clients:** error formats, extra fields and timeouts of each client are unknown.
5. **Wyoming:** streaming of transcripts (`transcript-chunk`) is not in the design.
6. **Security:**
   - Wyoming has no auth. The design trusts the local network.
   - There is no TLS and no rate limit per key.
   - The temporary file stays on disk. Proposal: mode 0600, and clean `tmp_dir` at start.
7. **Crash and retry:** the design does not define what occurs with a pipeline that stops in the middle, or a restart limit for an engine that fails in a loop.
8. **Config reload:** it does not exist. Each change needs a restart, which stops the jobs in progress.
9. **Disk:** 100 MiB uploads × a queue of 256 batch jobs can be about 25 GB in `tmp_dir`. The design has no disk limit.
10. **`diarized_json`, real streaming from worker `progress` events, and `/v1/realtime`:** out of scope.
11. **VAD:** the worker path has no VAD. Audio with much silence or noise can cause hallucinated text. This was not tested.
12. **Observability:** there is no per-request tracing between the hub and the engines, and no structured log.
13. **Other hosts:** the design covers only the 8 GB M1 Air. Another Mac needs only another config, but this was not examined in detail.
14. **CPU and energy** of the hub and ffmpeg per request: not measured.
