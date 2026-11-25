# speech-swap

speech-swap is a small hub for local speech engines.
It puts different engines behind one OpenAI-style API.
It queues requests, loads and stops engines by available memory, and makes each client request look the same to each engine.

The idea comes from [llama-swap](https://github.com/mostlygeek/llama-swap), which does this for LLM servers.
speech-swap is a separate program. It does not share code with llama-swap.
The design is in [docs/DESIGN.md](docs/DESIGN.md).

## Status

**M1 (MVP) works on an 8 GB M1 MacBook Air.**
It runs as a normal user process. It is not a launchd service yet, and only the speech-to-text routes exist.

Implemented:

- `POST /v1/audio/transcriptions`, `GET /v1/models`, `GET /health`, `GET /running`, `GET /metrics` (Prometheus text).
- Bearer keys from the config. Each key has a priority class: 0 voice, 1 interactive, 2 batch. `open = true` turns authentication off for tests.
- Input normalization, so clients need no change:
  - Any audio format (WAV, OGG/Opus, MP3, M4A, FLAC, WebM) goes through an ffmpeg child process and becomes 16 kHz mono.
  - The language can be an ISO code, a BCP-47 tag (`pt-BR`) or a full name (`English`). No language means `auto`.
  - Each engine gets only the fields that it takes (`fields` in the config). A documented OpenAI field never gives a 400.
  - An unknown model gives 404 with the list of model ids. An upload above the limit gives 413.
  - `response_format`: `json`, `text`, `verbose_json`. `stream=true` gives one SSE delta and one done event.
- Two engine adapters. Each engine is a child process in its own process group.
  - `speech-worker`: speech.cpp `speech worker` (JSON Lines protocol 4). Tested with Qwen3-ASR-1.7B Q8_0.
  - `jsonl`: one JSON line for each request and each answer. The FluidAudio runner (Parakeet TDT 0.6B v3) is in `runners/fluidaudio`.
- A scheduler. One task owns all state. It has a queue for each model, ordered by (class, arrival). A batch job that waits long moves up one class. Each engine runs one request at a time. A full queue gives 503 with `Retry-After`. A job leaves the queue when its client disconnects.
- Preemption: a voice or interactive job cancels a running batch job on the same engine. The batch job goes back to the front of its class and starts again from the beginning. The hub never cuts audio.
- A memory planner (a pure function with unit tests) and a live gate. The gate reads `host_statistics64` (free + inactive + purgeable), `kern.memorystatus_vm_pressure_level` and the `phys_footprint` of each child (`proc_pid_rusage`). Idle engines stop after `ttl_s`.

Not built yet:

- `srt` and `vtt` output. The hub answers 400 with "needs timestamps (planned)".
- The forced aligner engine, word timestamps and the chained `qwen3-asr+align` model (phase M2).
- A Wyoming listener for Home Assistant, `/v1/audio/alignments`, `/align`, admin routes and a log route.
- The fallback to another model is coded (`fallback`, `allow_fallback`). It was not tested live.

## Build

Rust 1.100 builds it. Older versions are not tested.
The release profile optimizes for size.

```sh
cargo build --release
cargo test
```

The binary is `target/release/speech-swap` (about 870 KB). It links only system libraries.
To build for another Mac, use `--target aarch64-apple-darwin` and copy the binary.

## Run

```sh
export SSW_KEY_VOICE=... SSW_KEY_APP=... SSW_KEY_BATCH=...   # one long random value for each key
./speech-swap examples/m1.toml
```

Each `[[keys]]` entry names an environment variable (`key_env`) or holds the key (`key`). Prefer `key_env`.
The temporary folder (`tmp_dir`) has mode 0700. The hub removes old `ssw-*` files in it at start and at stop.
SIGTERM or SIGINT stops the engines and then the hub.
If the hub dies from SIGKILL, its engines keep running (they have their own process groups). Stop them by hand.
A launchd template is in `contrib/launchd/`.

Test it:

```sh
curl -H "Authorization: Bearer $SSW_KEY_APP" \
  -F file=@clip.ogg -F model=Qwen3-ASR-1.7B -F language=pt \
  http://127.0.0.1:8780/v1/audio/transcriptions

HUB=http://127.0.0.1:8780 KEY=$SSW_KEY_APP PT_WAV=pt.wav EN_WAV=en.wav tests/e2e/contract.sh
```

`tests/e2e/load.py` runs the mixed-load test and the preemption test.

## Config

See [examples/m1.toml](examples/m1.toml). The file is TOML. An unknown key is an error.

| Section | What it sets |
|---|---|
| `[server]` | `listen`, `max_upload_mb`, `tmp_dir`, `ffmpeg` (path), `ffmpeg_timeout_s`, `default_model`, `open` |
| `[[keys]]` | `name`, `key_env` or `key`, `class` (0, 1, 2), `allow_fallback` |
| `[scheduler]` | `class_timeout_s`, `class_max_queue`, `batch_if_audio_longer_s`, `aging_after_s`, `preempt` |
| `[memory]` | `engine_budget_mb`, `headroom_mb`, `min_resident_s`, `evict_on_pressure`, `refuse_loads_on` |
| `[languages]` | `extra`: more language names |
| `[[engines]]` | `id`, `aliases`, `adapter`, `cmd`, `env`, `cwd`, `cost_mb`, `evict_cost`, `ttl_s`, `persistent`, `fields`, `languages`, `fallback` |

Rules:

- A client can lower its class with the header `X-Priority: batch`. It cannot raise it. Audio longer than `batch_if_audio_longer_s` is always batch.
- A batch job that waits longer than `aging_after_s` moves up to class 1.
- The planner uses `max(cost_mb, highest footprint seen)` for each engine. The sum of the costs of the resident engines stays at or below `engine_budget_mb`.
- The hub never stops a `persistent` engine to make room.
- At memory pressure level 2 (warning), the hub stops idle engines that are not persistent. At level 4 (critical), it does not load engines.

## Measured on the M1 (2026-11-25)

Host: M1 MacBook Air, 8 GB, macOS 26.5.1. The hub listened on port 8780.
Engines: Qwen3-ASR-1.7B Q8_0 (`speech worker`, speech.cpp 0.11.2) and Parakeet TDT 0.6B v3 (FluidAudio 0.18.4).

- **WER through the hub.** 138 Portuguese Home Assistant clips, language `pt`. Qwen 0.1684. Parakeet 0.2127. Both match the direct runs. Qwen: 138 of 138 texts equal. Parakeet: 137 of 138 equal.
- **Request time through the hub.** Qwen median 0.50 s, p90 0.90 s. Parakeet median 0.10 s, p90 0.13 s.
- **Mixed load, 5 minutes.** A 300 s file runs on Qwen (6 jobs, 51 to 56 s each, 764 words each). A clip of 2 to 8 s goes to Parakeet every 10 s (30 clips). Clip time: p50 0.17 s, p90 0.49 s, max 0.63 s. No error.
- **Preemption.** A short interactive clip arrived 15 s into a 300 s batch job on Qwen. It got its answer in 0.57 s. The batch job started again and finished with the full text (764 words), 71.8 s in total.
- **Hub memory** (`footprint`, phys_footprint). 1.2 MB at start with no engine. 1.8 MB with both engines loaded. 4.4 MB at the peak during the load (a 9.6 MB upload). 2.1 MB after the load. Swap did not change (168 MB before and after). The hub writes the upload with synchronous writes, not `tokio::fs`. The M0 test with 20 parallel uploads (12 MB idle afterwards) was not repeated.
- **Memory planner, live.** With a budget of 2600 MB, Qwen (2500 MB) and Parakeet (300 MB) never ran together. A request for the other model stopped the idle one first. The swap took 5.5 s and 7.1 s, including the 5 s minimum resident time.

The M0 probe numbers (the HTTP layer alone) follow.

## The M0 probe

The probe is a minimal HTTP server. It tests the memory cost of the HTTP layer of the hub.
It uses tokio (`current_thread`), hyper 1.x and multer.

Routes:

- `GET /health`
- `GET /v1/models` (a static list)
- `POST /v1/audio/transcriptions`: streams the upload to a temporary file, deletes the file, and returns `{"text":""}`. The header `x-probe-bytes` gives the size of the file part.

Build and run:

```sh
cargo build --release
./target/release/probe 127.0.0.1:8780
curl -F model=probe -F file=@audio.wav http://127.0.0.1:8780/v1/audio/transcriptions
```

### Measurements (M0)

Measured on 2026-11-25. Host: an 8 GB M1 MacBook Air, macOS 26.5.1.
Build: rustc 1.100.0, target `aarch64-apple-darwin`, the release profile in `Cargo.toml`.
A second Mac sent the uploads through the local network with `curl --limit-rate`.
The test file was a 30,080,078 byte WAV. Each upload returned status 200 and the correct `x-probe-bytes`.
A script read the values every 0.5 s.

**Binary size:** 537,696 bytes (525 KB), stripped. It links only `libSystem` and `libiconv`.

| State | `footprint` | phys_footprint | `ps` RSS | `vmmap` dirty total | Threads |
|---|---|---|---|---|---|
| Idle, after start | 993 KB | 1009 KB | 1712 KB | 1025 KB | 1 |
| During 1 upload of 30 MB (3 MB/s, 9.6 s) | 1681 KB | 1697 KB | 2416 KB | 1713 KB | 2 |
| End of that upload | 2673 KB | 2689 KB | 3408 KB | not read | 2 |
| During 20 parallel uploads of 30 MB (2 MB/s each, 14.4 s) | 12 MB | not read | 12752 KB | not read | 11 |
| About 10 s after the 20 uploads | 12 MB | 12 MB | 12752 KB | 11.6 MB | 11 |
| Idle, about 45 s after the 20 uploads | 12,157,888 B | 12,174,272 B | 12592 KB | not read | 1 |

`/usr/bin/time -l` for the full run (idle, 1 upload, 20 parallel uploads; 108 s):

- maximum resident set size: 13,058,048 bytes
- peak memory footprint: 12,338,112 bytes
- CPU time: 0.70 s user, 2.47 s system, for 21 uploads (632 MB in total)

`ps` VSZ was about 435,300,000 KB in all states. That value is address space, not memory in use.

**Proposed gate:** idle ≤ 6 MB, load ≤ 15 MB of phys_footprint.

- Idle after start: 1.0 MB. The gate passes.
- Peak with 20 parallel uploads: 12.3 MB. The gate passes.
- Idle after the load: 12.2 MB. This is above the 6 MB idle value. The process did not give the memory back in 45 s.

**Findings:**

- No upload was held in memory. The 20 temporary files were on disk during the load, and 0 files stayed after it.
- The cost per parallel upload was about 0.55 MB (11 MB for 20 uploads). The design estimated "tens of KB".
- After the load, `vmmap` showed 48 KB allocated and 10.8 MB of free space in the malloc zone. The memory is free for the process, but the system still counts it.
- The cause of the 0.55 MB per upload is not verified. A possible cause is the write buffer of `tokio::fs::File`.
- Asynchronous file writes use extra threads from the tokio blocking pool (11 threads at the peak). The threads stopped after the load.

**Port 80 without root:** a process of a normal user (uid 501) can bind `0.0.0.0:80` on macOS 26.5.1.
The probe answered `GET /health` with status 200 from a second host.
The test used a process from an SSH shell. A launchd job was not tested.

## License

MIT. See [LICENSE](LICENSE).
