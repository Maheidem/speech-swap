"""Alignment equality: the hub against the direct aligner (run IN SEQUENCE).

The two aligners do not fit in RAM together, so the steps run one after the other:

  1. python3 align_equal.py prepare CASES.json --hub URL --key KEY --ha-refs ha.json --ha-dir DIR --long LONG.wav
       Builds 3 cases: 2 HA clips with their reference text and a 25 s cut of a long file with the Qwen text.
  2. python3 align_equal.py hub CASES.json OUT_HUB.json --hub URL --key KEY --token-key @TOKENFILE
       Calls the hub: POST /align (the legacy route, with the aligner token as the key) and POST /v1/audio/alignments.
  3. Stop the hub. Start the old aligner service on 127.0.0.1:8131.
  4. python3 align_equal.py direct CASES.json OUT_DIRECT.json --url http://127.0.0.1:8131 --token @TOKENFILE
  5. python3 align_equal.py compare OUT_HUB.json OUT_DIRECT.json

A token argument is a value or `@file`. The script never prints the token.
"""
import argparse
import base64
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # PYTHONSAFEPATH=1 drops the script folder
import lib


def secret(value):
    return open(value[1:]).read().strip() if value.startswith("@") else value


def prepare(a):
    refs = json.load(open(a.ha_refs))
    cases = []
    for name in sorted(refs):
        path = os.path.join(a.ha_dir, name)
        if os.path.exists(path) and len(refs[name].split()) >= 4:
            cases.append({"name": name, "file": path, "text": refs[name], "language": "pt"})
        if len(cases) == 2:
            break
    cut = os.path.join(os.path.dirname(os.path.abspath(a.cases)), "cut25.wav")
    subprocess.run([a.ffmpeg, "-nostdin", "-loglevel", "error", "-y", "-i", a.long, "-t", "25", "-ac", "1", "-ar", "16000",
                    "-c:a", "pcm_s16le", cut], check=True)
    status, _, body = lib.post_audio(a.hub, a.key, "/v1/audio/transcriptions",
                                     [("model", "Qwen3-ASR-1.7B"), ("language", "pt")], cut)
    assert status == 200, body
    cases.append({"name": "long-060s-a.0-25s", "file": cut, "text": json.loads(body)["text"], "language": "pt"})
    json.dump(cases, open(a.cases, "w"), ensure_ascii=False, indent=1)
    for c in cases:
        print(c["name"], len(c["text"].split()), "words")


def b64(path):
    return base64.b64encode(open(path, "rb").read()).decode()


def run_hub(a):
    out = []
    for c in json.load(open(a.cases)):
        legacy_body = json.dumps({"audio_b64": b64(c["file"]), "text": c["text"], "language": c["language"]}).encode()
        s1, _, b1 = lib.request("POST", a.hub + "/align", secret(a.token_key), legacy_body, {"Content-Type": "application/json"})
        s2, _, b2 = lib.post_audio(a.hub, a.key, "/v1/audio/alignments",
                                   [("text", c["text"]), ("language", c["language"])], c["file"])
        assert s1 == 200 and s2 == 200, (s1, s2, b1[:200], b2[:200])
        out.append({"name": c["name"], "legacy": json.loads(b1)["words"],
                    "alignments": [{"text": w["word"], "start": w["start"], "end": w["end"]} for w in json.loads(b2)["words"]]})
        print(c["name"], "hub /align", len(out[-1]["legacy"]), "words;", "/v1/audio/alignments", len(out[-1]["alignments"]), "words")
    json.dump(out, open(a.out, "w"), indent=1)


def run_direct(a):
    out = []
    for c in json.load(open(a.cases)):
        body = json.dumps({"audio_b64": b64(c["file"]), "text": c["text"], "language": c["language"]}).encode()
        s, _, b = lib.request("POST", a.url + "/align", secret(a.token), body, {"Content-Type": "application/json"})
        assert s == 200, (s, b[:200])
        out.append({"name": c["name"], "direct": json.loads(b)["words"]})
        print(c["name"], "direct", len(out[-1]["direct"]), "words")
    json.dump(out, open(a.out, "w"), indent=1)


def compare(a):
    hub, direct = json.load(open(a.hub_file)), json.load(open(a.direct_file))
    ok = True
    print(f"{'case':22} {'route':22} {'words':>5} {'same words':>10} {'max start diff ms':>18} {'max end diff ms':>16}")
    for h, d in zip(hub, direct):
        for route in ("legacy", "alignments"):
            hw, dw = h[route], d["direct"]
            same = len(hw) == len(dw) and all(x["text"] == y["text"] for x, y in zip(hw, dw))
            ds = max((abs(x["start"] - y["start"]) for x, y in zip(hw, dw)), default=0) * 1000
            de = max((abs(x["end"] - y["end"]) for x, y in zip(hw, dw)), default=0) * 1000
            ok &= same and ds == 0 and de == 0
            name = "/align" if route == "legacy" else "/v1/audio/alignments"
            print(f"{h['name']:22} {name:22} {len(hw):>5} {str(same):>10} {ds:>18.1f} {de:>16.1f}")
    print("RESULT", "EQUAL" if ok else "DIFFERENT")
    sys.exit(0 if ok else 1)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("cases")
    p.add_argument("--hub", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--ha-refs", required=True)
    p.add_argument("--ha-dir", required=True)
    p.add_argument("--long", required=True)
    p.add_argument("--ffmpeg", default="ffmpeg")
    p = sub.add_parser("hub")
    p.add_argument("cases")
    p.add_argument("out")
    p.add_argument("--hub", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--token-key", required=True, help="the aligner token, used as a hub key for /align (value or @file)")
    p = sub.add_parser("direct")
    p.add_argument("cases")
    p.add_argument("out")
    p.add_argument("--url", default="http://127.0.0.1:8131")
    p.add_argument("--token", required=True, help="value or @file")
    p = sub.add_parser("compare")
    p.add_argument("hub_file")
    p.add_argument("direct_file")
    a = ap.parse_args()
    {"prepare": prepare, "hub": run_hub, "direct": run_direct, "compare": compare}[a.cmd](a)


if __name__ == "__main__":
    main()
