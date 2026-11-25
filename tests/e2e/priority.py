"""Priority under the pipeline (M2 test I).

A pipeline request runs on a long file (class 2). While it runs:
  - a Wyoming client sends one Parakeet clip every 10 s (class 0). They must stay fast.
  - when the aligner has work, one class-1 request goes to Qwen. The script records how long it waits.
The script prints a timeline of the engine states, the Parakeet clip times and the class-1 time.

  python3 priority.py --hub URL --key KEY --file long.wav --clips DIR --wyoming-port 10380 --out result.json
"""
import argparse
import glob
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # PYTHONSAFEPATH=1 drops the script folder
import lib
import wyoming_client


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hub", required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--file", required=True)
    ap.add_argument("--clips", required=True)
    ap.add_argument("--wyoming-port", type=int, required=True)
    ap.add_argument("--aligner", default="Qwen3-ForcedAligner-0.6B")
    ap.add_argument("--out")
    a = ap.parse_args()
    t0 = time.time()
    now = lambda: round(time.time() - t0, 1)
    res = {"timeline": [], "parakeet": [], "class1": None, "pipeline": None}
    done = threading.Event()
    clips = sorted(glob.glob(os.path.join(a.clips, "*.wav")))

    def pipeline():
        status, hdr, body = lib.post_audio(a.hub, a.key, "/v1/audio/transcriptions",
                                           [("model", "qwen3-asr+align"), ("response_format", "verbose_json"),
                                            ("timestamp_granularities[]", "word")], a.file)
        res["pipeline"] = {"status": status, "end_s": now(), "words": len(json.loads(body).get("words", [])) if status == 200 else None}
        done.set()

    def parakeet():
        i = 0
        while not done.is_set():
            text, dur, wall, stop = wyoming_client.transcribe("127.0.0.1", a.wyoming_port, clips[i % len(clips)], "pt-BR", None)
            res["parakeet"].append({"at_s": now(), "wall_s": round(wall, 3), "text": text})
            i += 1
            done.wait(10)

    def class1():
        t = time.time()
        status, hdr, body = lib.post_audio(a.hub, a.key, "/v1/audio/transcriptions",
                                           [("model", "Qwen3-ASR-1.7B"), ("language", "pt")], clips[0])
        res["class1"] = {"sent_at_s": round(t - t0, 1), "answer_s": round(time.time() - t, 2), "status": status,
                         "wait_ms": hdr.get("x-speech-swap-wait-ms"), "run_ms": hdr.get("x-speech-swap-run-ms")}

    threading.Thread(target=pipeline, daemon=True).start()
    threading.Thread(target=parakeet, daemon=True).start()
    sent = False
    last = None
    while not done.is_set() or (sent and res["class1"] is None):
        _, _, raw = lib.request("GET", a.hub + "/running", a.key, timeout=10)
        v = json.loads(raw)
        states = {e["model"]: (e["state"], sum(e["queue"].values())) for e in v["engines"]}
        line = {m: f"{s}/q{q}" for m, (s, q) in states.items()}
        if line != last:
            res["timeline"].append({"at_s": now(), **line})
            last = line
        al = states.get(a.aligner, ("unloaded", 0))
        if not sent and al[0] in ("busy", "idle") and al[1] > 0 or (not sent and al[0] == "busy"):
            sent = True
            threading.Thread(target=class1, daemon=True).start()
        time.sleep(0.5)
        if now() > 600:
            break
    walls = [p["wall_s"] for p in res["parakeet"]]
    res["parakeet_summary"] = {"n": len(walls), "p50": lib.percentile(walls, .5), "p90": lib.percentile(walls, .9), "max": max(walls) if walls else None}
    if a.out:
        json.dump(res, open(a.out, "w"), indent=1, ensure_ascii=False)
    for row in res["timeline"]:
        print(row)
    print("pipeline", res["pipeline"])
    print("parakeet clips (class 0)", res["parakeet_summary"])
    print("class-1 Qwen request", res["class1"])


if __name__ == "__main__":
    main()
