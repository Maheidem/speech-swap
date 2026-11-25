"""A small Wyoming STT client (Python standard library only).

  python3 wyoming_client.py --port 10300 --audio DIR --limit 20 --out out.csv [--language pt-BR] [--name MODEL]
  python3 wyoming_client.py --port 10300 --describe

It sends what a Home Assistant satellite sends: `transcribe`, `audio-start`,
`audio-chunk` frames of PCM and `audio-stop`. It waits for the `transcript`.
The CSV has the columns file, audio_s, wall_s (the whole request), stop_s
(from audio-stop to the transcript) and text. The last lines show p50 and p90.
"""
import argparse
import csv
import glob
import json
import os
import socket
import sys
import time
import wave


def send(sock, kind, data=None, payload=b""):
    body = json.dumps(data).encode() if data else b""
    head = {"type": kind}
    if body:
        head["data_length"] = len(body)
    if payload:
        head["payload_length"] = len(payload)
    sock.sendall(json.dumps(head).encode() + b"\n" + body + payload)


def recv(f):
    """Reads one event from a file object of the socket. Returns (type, data, payload)."""
    line = f.readline()
    if not line:
        raise EOFError("the server closed the connection")
    head = json.loads(line)
    data = dict(head.get("data") or {})
    if head.get("data_length"):
        data.update(json.loads(f.read(head["data_length"])))
    payload = f.read(head["payload_length"]) if head.get("payload_length") else b""
    return head["type"], data, payload


def transcribe(host, port, path, language, name, chunk_ms=100, timeout=120):
    with wave.open(path) as w:
        rate, width, channels = w.getframerate(), w.getsampwidth(), w.getnchannels()
        pcm = w.readframes(w.getnframes())
    t0 = time.perf_counter()
    with socket.create_connection((host, port), timeout=timeout) as s:
        f = s.makefile("rb")
        t = {"transcribe": {"language": language}}
        if name:
            t["transcribe"]["name"] = name
        send(s, "transcribe", t["transcribe"])
        fmt = {"rate": rate, "width": width, "channels": channels}
        send(s, "audio-start", fmt)
        step = rate * width * channels * chunk_ms // 1000
        for i in range(0, len(pcm), step):
            send(s, "audio-chunk", fmt, pcm[i:i + step])
        send(s, "audio-stop", {})
        t1 = time.perf_counter()
        kind, data, _ = recv(f)
        t2 = time.perf_counter()
    if kind != "transcript":
        raise RuntimeError(f"expected a transcript, got {kind}: {data}")
    return data.get("text", ""), len(pcm) / (rate * width * channels), t2 - t0, t2 - t1


def percentile(values, p):
    v = sorted(values)
    k = (len(v) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--describe", action="store_true")
    ap.add_argument("--audio")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--language", default="pt-BR")
    ap.add_argument("--name")
    ap.add_argument("--out")
    ap.add_argument("--every", type=float, default=0.0, help="seconds between two clips")
    a = ap.parse_args()
    if a.describe:
        with socket.create_connection((a.host, a.port), timeout=10) as s:
            send(s, "describe")
            print(json.dumps(recv(s.makefile("rb"))[1], indent=1))
        return
    files = sorted(glob.glob(os.path.join(a.audio, "*.wav")))[:a.limit]
    rows, walls, stops = [], [], []
    for p in files:
        text, dur, wall, stop = transcribe(a.host, a.port, p, a.language, a.name)
        rows.append({"file": os.path.basename(p), "audio_s": f"{dur:.2f}", "wall_s": f"{wall:.3f}", "stop_s": f"{stop:.3f}", "text": text})
        walls.append(wall)
        stops.append(stop)
        print(f"{os.path.basename(p)} {wall:.3f}s {text!r}", flush=True)
        if a.every:
            time.sleep(a.every)
    if a.out:
        with open(a.out, "w", newline="") as f:
            w = csv.DictWriter(f, ["file", "audio_s", "wall_s", "stop_s", "text"])
            w.writeheader()
            w.writerows(rows)
    print(f"clips {len(rows)}  wall p50 {percentile(walls, .5):.3f} p90 {percentile(walls, .9):.3f} max {max(walls):.3f}  "
          f"audio-stop to transcript p50 {percentile(stops, .5):.3f} p90 {percentile(stops, .9):.3f}", file=sys.stderr)


if __name__ == "__main__":
    main()
