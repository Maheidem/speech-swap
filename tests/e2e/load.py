#!/usr/bin/env python3
"""Load tests for a running hub (stdlib only, Python 3.9 or newer).

mix      A long batch job runs on one model while a short voice clip goes to
         another model every 10 s. It prints the latency of the voice clips.
preempt  A long batch job runs on a model. After a delay a short interactive
         clip goes to the same model. The hub must cancel the batch job,
         answer the short clip fast, and run the batch job again to the end.

Example (on the host of the hub):
  load.py mix --url http://127.0.0.1:8780 --key-voice K0 --key-batch K2 \
      --long long-300s.wav --clips-dir clips --minutes 5
  load.py preempt --url http://127.0.0.1:8780 --key-app K1 --key-batch K2 \
      --long long-300s.wav --clip clip.wav
"""
import argparse, json, os, sys, threading, time, urllib.error, urllib.request, uuid


def post(url, key, wav, model, language="pt", timeout=1800):
    """POST one file. Returns a dict with status, wall seconds, text and hub headers."""
    b = uuid.uuid4().hex
    fields = {"model": model, "response_format": "json"}
    if language:
        fields["language"] = language
    parts = [f'--{b}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode() for k, v in fields.items()]
    with open(wav, "rb") as f:
        data = f.read()
    parts.append(f'--{b}\r\nContent-Disposition: form-data; name="file"; filename="a.wav"\r\n'
                 f'Content-Type: audio/wav\r\n\r\n'.encode() + data + b"\r\n")
    parts.append(f"--{b}--\r\n".encode())
    req = urllib.request.Request(url + "/v1/audio/transcriptions", data=b"".join(parts), method="POST")
    req.add_header("Content-Type", f"multipart/form-data; boundary={b}")
    req.add_header("Authorization", "Bearer " + key)
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body, status, headers = r.read(), r.status, r.headers
    except urllib.error.HTTPError as e:
        body, status, headers = e.read(), e.code, e.headers
    except Exception as e:  # network error
        return {"status": 0, "wall": time.time() - t0, "text": "", "error": str(e), "start": t0}
    wall = time.time() - t0
    try:
        text = json.loads(body).get("text", "")
    except ValueError:
        text = ""
    return {"status": status, "wall": round(wall, 3), "text": text, "start": t0,
            "wait_ms": headers.get("x-speech-swap-wait-ms"), "run_ms": headers.get("x-speech-swap-run-ms"),
            "preempted": headers.get("x-speech-swap-preempted"), "model": headers.get("x-speech-swap-model")}


def pct(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))] if values else float("nan")


def mix(a):
    clips = sorted(os.path.join(a.clips_dir, f) for f in os.listdir(a.clips_dir) if f.endswith(".wav"))
    stop = time.time() + a.minutes * 60
    batch, voice = [], []

    def batch_loop():
        while time.time() < stop:
            r = post(a.url, a.key_batch, a.long, a.batch_model)
            r["words"] = len(r["text"].split())
            r.pop("text")
            batch.append(r)
            print(f"batch job: status {r['status']} wall {r['wall']} s words {r['words']} preempted {r['preempted']}", flush=True)

    def one_voice(i, path):
        r = post(a.url, a.key_voice, path, a.voice_model)
        r["clip"] = os.path.basename(path)
        voice.append(r)
        print(f"voice {i}: status {r['status']} wall {r['wall']} s text {r['text']!r}", flush=True)

    tb = threading.Thread(target=batch_loop)
    tb.start()
    threads, i = [], 0
    while time.time() < stop:
        t = threading.Thread(target=one_voice, args=(i, clips[i % len(clips)]))
        t.start()
        threads.append(t)
        i += 1
        time.sleep(10)
    tb.join()
    for t in threads:
        t.join()
    walls = [r["wall"] for r in voice if r["status"] == 200]
    summary = {"voice_n": len(voice), "voice_ok": len(walls), "voice_p50_s": pct(walls, 50),
               "voice_p90_s": pct(walls, 90), "voice_max_s": max(walls) if walls else None,
               "batch_jobs": len(batch), "batch": batch}
    print(json.dumps(summary, indent=1))
    if a.out:
        json.dump({"summary": summary, "voice": voice}, open(a.out, "w"), indent=1)
    return 0 if len(walls) == len(voice) and all(b["status"] == 200 for b in batch) else 1


def preempt(a):
    result = {}

    def run_batch():
        r = post(a.url, a.key_batch, a.long, a.model)
        r["words"] = len(r["text"].split())
        result["batch"] = r

    tb = threading.Thread(target=run_batch)
    t0 = time.time()
    tb.start()
    time.sleep(a.delay)
    short = post(a.url, a.key_app, a.clip, a.model)
    short_at = time.time() - t0
    tb.join()
    result["short"] = short
    result["short_sent_after_s"] = round(short_at, 1)
    result["batch_total_s"] = round(time.time() - t0, 1)
    print(json.dumps(result, indent=1, ensure_ascii=False))
    if a.out:
        json.dump(result, open(a.out, "w"), indent=1, ensure_ascii=False)
    b = result["batch"]
    ok = short["status"] == 200 and b["status"] == 200 and int(b.get("preempted") or 0) >= 1
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="mode", required=True)
    m = sub.add_parser("mix")
    m.add_argument("--url", required=True)
    m.add_argument("--key-voice", required=True)
    m.add_argument("--key-batch", required=True)
    m.add_argument("--long", required=True)
    m.add_argument("--clips-dir", required=True)
    m.add_argument("--minutes", type=float, default=5)
    m.add_argument("--batch-model", default="Qwen3-ASR-1.7B")
    m.add_argument("--voice-model", default="parakeet-tdt-0.6b-v3")
    m.add_argument("--out")
    p = sub.add_parser("preempt")
    p.add_argument("--url", required=True)
    p.add_argument("--key-app", required=True)
    p.add_argument("--key-batch", required=True)
    p.add_argument("--long", required=True)
    p.add_argument("--clip", required=True)
    p.add_argument("--model", default="Qwen3-ASR-1.7B")
    p.add_argument("--delay", type=float, default=15)
    p.add_argument("--out")
    a = ap.parse_args()
    sys.exit(mix(a) if a.mode == "mix" else preempt(a))


if __name__ == "__main__":
    main()
