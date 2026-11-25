#!/usr/bin/env bash
# Contract test: one request for each client shape. It prints the status and
# the body of each request. Run it against a running hub.
#
#   HUB=http://host:8780 KEY=<any key> \
#   PT_WAV=pt.wav EN_WAV=en.wav FFMPEG=ffmpeg tests/e2e/contract.sh
#
# PT_WAV is a short Portuguese clip. EN_WAV is a short English clip.
# The script makes the OGG/Opus, MP3 and M4A files from PT_WAV with ffmpeg.
# It exits with 1 when a status differs from the expected one.
set -u
HUB=${HUB:?set HUB}
KEY=${KEY:?set KEY}
PT_WAV=${PT_WAV:?set PT_WAV}
EN_WAV=${EN_WAV:?set EN_WAV}
FFMPEG=${FFMPEG:-ffmpeg}
U="$HUB/v1/audio/transcriptions"
W=$(mktemp -d)
trap 'rm -rf "$W"' EXIT
fail=0

# post EXPECTED_STATUS LABEL CURL_ARGS...  (the Bearer key is added)
post() {
  local want=$1 label=$2; shift 2
  local out code
  out=$(timeout 120 curl -s -m 100 -o "$W/body" -w '%{http_code}' -H "Authorization: Bearer $KEY" "$@")
  code=$out
  printf '\n--- %s (expect %s)\nstatus %s\n%s\n' "$label" "$want" "$code" "$(head -c 700 "$W/body")"
  [ "$code" = "$want" ] || { echo "MISMATCH"; fail=1; }
}

"$FFMPEG" -nostdin -loglevel error -y -i "$PT_WAV" -c:a libopus -b:a 24k "$W/note.ogg"
"$FFMPEG" -nostdin -loglevel error -y -i "$PT_WAV" -c:a libmp3lame "$W/clip.mp3"
"$FFMPEG" -nostdin -loglevel error -y -i "$PT_WAV" -c:a aac "$W/clip.m4a"
head -c 105000000 /dev/zero > "$W/big.bin"

post 200 "1 wav + bearer + prompt + language=pt" -F "file=@$PT_WAV" -F model=Qwen3-ASR-1.7B -F language=pt -F "prompt=comandos de casa" "$U"
post 200 "2 raw ogg/opus + language=pt + prompt" -F "file=@$W/note.ogg;type=audio/ogg" -F model=Qwen3-ASR-1.7B -F language=pt -F "prompt=comandos de casa" "$U"
post 200 "3 wav + temperature=0" -F "file=@$PT_WAV" -F model=Qwen3-ASR-1.7B -F temperature=0 "$U"
post 200 "4 language=English on the English clip" -F "file=@$EN_WAV" -F model=Qwen3-ASR-1.7B -F language=English "$U"
post 200 "5 no language on the English clip" -F "file=@$EN_WAV" -F model=Qwen3-ASR-1.7B "$U"
post 200 "6a mp3" -F "file=@$W/clip.mp3" -F model=Qwen3-ASR-1.7B -F language=pt-BR "$U"
post 200 "6b m4a" -F "file=@$W/clip.m4a" -F model=Qwen3-ASR-1.7B -F language=pt-BR "$U"
post 200 "7 response_format=text" -F "file=@$PT_WAV" -F model=Qwen3-ASR-1.7B -F response_format=text "$U"
post 404 "8 unknown model" -F "file=@$PT_WAV" -F model=does-not-exist "$U"
post 413 "9 oversize (105 MB)" -F "file=@$W/big.bin" -F model=Qwen3-ASR-1.7B "$U"
post 200 "extra: parakeet + verbose_json + timestamp_granularities" -F "file=@$PT_WAV" -F model=parakeet-tdt-0.6b-v3 -F response_format=verbose_json -F "timestamp_granularities[]=word" -F language=pt "$U"
post 400 "extra: srt" -F "file=@$PT_WAV" -F model=Qwen3-ASR-1.7B -F response_format=srt "$U"
echo "this is not audio" > "$W/x.txt"
post 400 "extra: not audio" -F "file=@$W/x.txt;filename=x.mp3" -F model=Qwen3-ASR-1.7B "$U"

# wrong key: no Authorization header from post(), so call curl here
code=$(timeout 60 curl -s -m 50 -o "$W/body" -w '%{http_code}' -H "Authorization: Bearer wrong-key" -F "file=@$PT_WAV" -F model=Qwen3-ASR-1.7B "$U")
printf '\n--- 10 wrong key (expect 401)\nstatus %s\n%s\n' "$code" "$(head -c 400 "$W/body")"
[ "$code" = 401 ] || { echo "MISMATCH"; fail=1; }

printf '\n--- 11 GET /v1/models (expect 200 and both ids)\n'
code=$(timeout 30 curl -s -m 20 -o "$W/body" -w '%{http_code}' -H "Authorization: Bearer $KEY" "$HUB/v1/models")
printf 'status %s\n%s\n' "$code" "$(head -c 900 "$W/body")"
[ "$code" = 200 ] || { echo "MISMATCH"; fail=1; }

printf '\n--- GET /health, /running\n'
timeout 20 curl -s -m 10 "$HUB/health"; echo
timeout 20 curl -s -m 10 -H "Authorization: Bearer $KEY" "$HUB/running"; echo
exit $fail
