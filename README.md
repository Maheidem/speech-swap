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

## License

MIT. See [LICENSE](LICENSE).
