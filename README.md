# speech-swap

speech-swap is a plan for a tiny Rust hub for local speech engines.
The hub puts different engines behind one OpenAI-style API.
It queues requests, swaps engines by available memory, and chains engines.

The idea comes from [llama-swap](https://github.com/mostlygeek/llama-swap), which does this for LLM servers.
speech-swap is a separate program. It does not share code with llama-swap.

## Status

**M0: measurement.**

The hub does not exist yet. This repository contains:

- the design: [docs/DESIGN.md](docs/DESIGN.md);
- a footprint probe: [src/bin/probe.rs](src/bin/probe.rs);
- the measured results of the probe (see "Measurements (M0)").

Do not use this repository in production.

## Planned features

These features are a plan. They are not built.

- One API for speech-to-text: `POST /v1/audio/transcriptions` and `GET /v1/models`.
- Forced alignment: `POST /v1/audio/alignments`.
- A Wyoming listener for Home Assistant.
- Input normalization: each audio format goes through ffmpeg; each language form becomes one form.
- A queue with three priority classes: voice, interactive and batch.
- Engine swap by memory: the hub loads and stops engines to stay inside a memory budget.
- Chaining: speech-to-text, then word alignment, as one virtual model.
- A small footprint: one thread, the system allocator, no web framework.

The first target is an 8 GB Apple Silicon Mac. The engines are child processes; the hub does not run models.

## The probe

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

## Measurements (M0)

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
| During 20 parallel uploads of 30 MB (2 MB/s each, 14.4 s) | 12 MB | 12 MB | 12752 KB | 11.6 MB | 11 |
| Idle, 45 s after the 20 uploads | 12,157,888 B | 12,174,272 B | 12592 KB | not read | 1 |

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
