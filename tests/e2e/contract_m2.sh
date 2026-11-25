#!/usr/bin/env bash
# Contract test of the M2 routes. It prints the status and the start of the body of each request.
#
#   HUB=http://host:8780 KEY=<class-1 key> OLD_TOKEN=<a key for /align> PT_WAV=clip.wav FFMPEG=ffmpeg \
#   tests/e2e/contract_m2.sh
#
# It exits with 1 when a status differs from the expected one.
set -u
HUB=${HUB:?set HUB}
KEY=${KEY:?set KEY}
OLD_TOKEN=${OLD_TOKEN:?set OLD_TOKEN}
PT_WAV=${PT_WAV:?set PT_WAV}
FFMPEG=${FFMPEG:-ffmpeg}
W=$(mktemp -d)
trap 'rm -rf "$W"' EXIT
fail=0
T="$HUB/v1/audio/transcriptions"

# post EXPECTED_STATUS LABEL CURL_ARGS...  (the Bearer key is added)
post() {
  local want=$1 label=$2; shift 2
  local code
  code=$(timeout 300 curl -s -m 280 -o "$W/body" -w '%{http_code}' -H "Authorization: Bearer $KEY" "$@")
  printf '\n--- %s (expect %s)\nstatus %s\n%s\n' "$label" "$want" "$code" "$(head -c 420 "$W/body")"
  [ "$code" = "$want" ] || { echo "MISMATCH"; fail=1; }
}

# A file of 40 s: the clip in a loop.
"$FFMPEG" -nostdin -loglevel error -y -stream_loop 20 -i "$PT_WAV" -t 40 -ac 1 -ar 16000 "$W/long40.wav"
python3 - "$PT_WAV" "$W" <<'PY'
import base64, json, sys
body = {"audio_b64": base64.b64encode(open(sys.argv[1], "rb").read()).decode(), "text": "Cancela o timer, atual.", "language": "pt"}
json.dump(body, open(sys.argv[2] + "/legacy.json", "w"))
PY

post 200 "models list has the pipeline" -X GET "$HUB/v1/models"
grep -q qwen3-asr+align "$W/body" || { echo "MISMATCH: no pipeline in /v1/models"; fail=1; }
post 200 "Qwen + timestamp_granularities[]=word (the pipeline starts)" -F file=@"$PT_WAV" -F model=Qwen3-ASR-1.7B -F language=pt \
  -F response_format=verbose_json -F 'timestamp_granularities[]=word' "$T"
post 200 "virtual model, srt" -F file=@"$PT_WAV" -F model=qwen3-asr+align -F language=pt -F response_format=srt "$T"
post 200 "virtual model, vtt" -F file=@"$PT_WAV" -F model=qwen3-asr+align -F language=pt -F response_format=vtt "$T"
post 200 "Parakeet, vtt (one cue from the engine segment)" -F file=@"$PT_WAV" -F model=parakeet -F response_format=vtt "$T"
post 400 "transcription with the aligner model" -F file=@"$PT_WAV" -F model=Qwen3-ForcedAligner-0.6B "$T"
post 200 "/v1/audio/alignments" -F file=@"$PT_WAV" -F text="Cancela o timer, atual." -F language=pt-BR "$HUB/v1/audio/alignments"
post 400 "/v1/audio/alignments without language" -F file=@"$PT_WAV" -F text="Cancela o timer" "$HUB/v1/audio/alignments"
post 400 "/v1/audio/alignments without text" -F file=@"$PT_WAV" -F language=pt "$HUB/v1/audio/alignments"
post 400 "/v1/audio/alignments with a 40 s file" -F file=@"$W/long40.wav" -F text="um dois tres" -F language=pt "$HUB/v1/audio/alignments"

# /align (legacy JSON). `post` adds the hub key as Bearer, so use curl directly here.
legacy() {
  local want=$1 label=$2 key=$3 data=$4
  local code
  code=$(timeout 120 curl -s -m 100 -o "$W/body" -w '%{http_code}' -H "Authorization: Bearer $key" -H 'Content-Type: application/json' --data "$data" "$HUB/align")
  printf '\n--- %s (expect %s)\nstatus %s\n%s\n' "$label" "$want" "$code" "$(head -c 300 "$W/body")"
  [ "$code" = "$want" ] || { echo "MISMATCH"; fail=1; }
}
legacy 200 "/align with the old client token" "$OLD_TOKEN" @"$W/legacy.json"
legacy 200 "/align with a hub key" "$KEY" @"$W/legacy.json"
legacy 401 "/align with a wrong key" "nope" @"$W/legacy.json"
legacy 400 "/align with a body that is not JSON" "$KEY" 'hello'
legacy 400 "/align without text" "$KEY" '{"audio_b64":"AAAA","language":"pt"}'
exit $fail
