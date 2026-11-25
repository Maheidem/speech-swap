"""Runs the STT to align pipeline through the hub and prints the numbers.

  python3 pipeline.py --hub http://127.0.0.1:8780 --key KEY --file audio.wav \
      [--model qwen3-asr+align] [--language pt] [--out result.json]

Two requests: `verbose_json` (words, segments, time, swaps) and `srt`.
It reads `vm.swapusage` before and after and the lowest free memory
percentage during the run.
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # PYTHONSAFEPATH=1 drops the script folder
import lib


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hub", required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--file", required=True)
    ap.add_argument("--model", default="qwen3-asr+align")
    ap.add_argument("--language")
    ap.add_argument("--out")
    ap.add_argument("--no-srt", action="store_true")
    a = ap.parse_args()
    fields = [("model", a.model), ("response_format", "verbose_json"), ("timestamp_granularities[]", "word"),
              ("timestamp_granularities[]", "segment")]
    if a.language:
        fields.append(("language", a.language))
    result = {"file": a.file, "model": a.model}
    swap_before = lib.swap_used_mb()
    with lib.MemWatch() as mem:
        swaps0 = lib.metric_sum(a.hub, a.key, "ssw_swaps_total")
        t0 = time.time()
        status, hdr, body = lib.post_audio(a.hub, a.key, "/v1/audio/transcriptions", fields, a.file)
        result["seconds"] = round(time.time() - t0, 2)
        result["status"] = status
        result["swaps"] = lib.metric_sum(a.hub, a.key, "ssw_swaps_total") - swaps0
        if status == 200:
            v = json.loads(body)
            words, segs = v.get("words", []), v.get("segments", [])
            result.update(text=v["text"], language=v.get("language"), duration=v["duration"], words=words, segments=segs,
                          windows=hdr.get("x-speech-swap-windows"), headers={k: x for k, x in hdr.items() if k.lower().startswith("x-speech-swap")})
        else:
            result["error"] = body.decode()[:500]
        if status == 200 and not a.no_srt:
            f2 = [(k, v) for k, v in fields if k not in ("response_format", "timestamp_granularities[]")] + [("response_format", "srt")]
            swaps1 = lib.metric_sum(a.hub, a.key, "ssw_swaps_total")
            t1 = time.time()
            s2, _, srt = lib.post_audio(a.hub, a.key, "/v1/audio/transcriptions", f2, a.file)
            result["srt_status"] = s2
            result["srt_seconds"] = round(time.time() - t1, 2)
            result["srt_swaps"] = lib.metric_sum(a.hub, a.key, "ssw_swaps_total") - swaps1
            result["srt"] = srt.decode()
    result["swap_mb_before"], result["swap_mb_after"] = swap_before, lib.swap_used_mb()
    result["min_free_pct"] = mem.min
    if a.out:
        json.dump(result, open(a.out, "w"), ensure_ascii=False, indent=1)
    w = result.get("words", [])
    print(f"status {result['status']}  total {result['seconds']} s  windows {result.get('windows')}  swaps {result['swaps']}")
    if w:
        print(f"words {len(w)}  first {w[0]['word']!r} {w[0]['start']}  last {w[-1]['word']!r} {w[-1]['end']}  segments {len(result['segments'])}")
    if result.get("srt"):
        cues = result["srt"].strip().split("\n\n")
        print(f"srt status {result['srt_status']}  {result['srt_seconds']} s  swaps {result['srt_swaps']}  cues {len(cues)}; first 3:")
        print("\n\n".join(cues[:3]))
    print(f"swapusage MB before {result['swap_mb_before']} after {result['swap_mb_after']}  min free % {result['min_free_pct']}")
    if result["status"] != 200:
        print(result.get("error"))
        sys.exit(1)


if __name__ == "__main__":
    main()
