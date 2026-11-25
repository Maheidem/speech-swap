#!/usr/bin/env bash
# kill -9 test: no engine may outlive the hub, and the next start removes the temporary files.
#
#   HUB=http://127.0.0.1:8780 KEY=<key> START="./start.sh" TMPDIR_HUB=~/.cache/speech-swap/tmp \
#   PT_WAV=clip.wav LONG_WAV=long.wav ALIGN_JSON=align-body.json tests/e2e/killtest.sh
#
# START is a command that starts the hub in the background (it must also stop an old hub).
# ALIGN_JSON is a legacy /align body (`audio_b64`, `text`, `language`); ALIGN_KEY is the key for /align.
# Round A loads the worker and the runner. Round B loads the aligner and the runner.
# In each round the script sends `kill -9` to the hub while a long job runs.
# It prints the engine processes before the kill and every 0.5 s after it, until none is left.
set -u
HUB=${HUB:?set HUB}
KEY=${KEY:?set KEY}
START=${START:?set START}
TMPDIR_HUB=${TMPDIR_HUB:?set TMPDIR_HUB}
PT_WAV=${PT_WAV:?set PT_WAV}
LONG_WAV=${LONG_WAV:?set LONG_WAV}
ALIGN_JSON=${ALIGN_JSON:?set ALIGN_JSON}
ALIGN_KEY=${ALIGN_KEY:-$KEY}
PAT='speech worker|fluidaudio-runner|launch.py|speech-swap --guard'
fail=0

hub_pid() { pgrep -x speech-swap | head -1; }
engines() { pgrep -fl "$PAT" | grep -v pgrep; }

start_hub() { $START >/dev/null; for i in $(seq 1 20); do curl -s -m 2 -o /dev/null "$HUB/health" && return 0; sleep 0.5; done; echo "the hub did not start"; exit 1; }

round() {
  local name=$1 load=$2 bg=$3
  echo; echo "=== round $name"
  start_hub
  curl -s -m 60 -o /dev/null -H "Authorization: Bearer $KEY" -F file=@"$PT_WAV" -F model=parakeet "$HUB/v1/audio/transcriptions"
  eval "$load"
  echo "--- processes before the kill:"; engines
  # A long job in the background, so that temporary files exist at the kill.
  eval "$bg" &
  sleep 3
  echo "--- temporary files at the kill:"; ls "$TMPDIR_HUB" | tr '\n' ' '; echo
  local pid; pid=$(hub_pid); echo "kill -9 hub pid $pid"
  local t0; t0=$(python3 -c 'import time;print(time.time())')
  kill -9 "$pid"
  for i in $(seq 1 60); do
    n=$(engines | wc -l | tr -d ' ')
    [ "$n" = 0 ] && break
    sleep 0.5
  done
  local t1; t1=$(python3 -c 'import time;print(time.time())')
  echo "processes left after the kill: $n  (time to none: $(python3 -c "print(round($t1-$t0,1))") s)"
  engines
  [ "$n" = 0 ] || fail=1
  wait 2>/dev/null
  echo "--- temporary files after the kill:"; ls "$TMPDIR_HUB" | tr '\n' ' '; echo
}

QWEN_LONG="curl -s -m 120 -o /dev/null -H 'Authorization: Bearer $KEY' -F file=@$LONG_WAV -F model=Qwen3-ASR-1.7B $HUB/v1/audio/transcriptions"
ALIGN_CALL="curl -s -m 90 -o /dev/null -H 'Authorization: Bearer $ALIGN_KEY' -H 'Content-Type: application/json' --data @$ALIGN_JSON $HUB/align"
round A "curl -s -m 60 -o /dev/null -H 'Authorization: Bearer $KEY' -F file=@$PT_WAV -F model=Qwen3-ASR-1.7B $HUB/v1/audio/transcriptions" "$QWEN_LONG"
echo; echo "=== restart: the hub removes the files of the dead run"
start_hub
grep -h 'temporary files' "${HUB_LOG:-/dev/null}" 2>/dev/null | tail -1
echo "temporary files after the restart: $(ls "$TMPDIR_HUB" | wc -l | tr -d ' ')"
[ "$(ls "$TMPDIR_HUB" | wc -l | tr -d ' ')" = 0 ] || fail=1

round B "$ALIGN_CALL" "$ALIGN_CALL"
pkill -TERM -x speech-swap; sleep 2
echo; [ $fail = 0 ] && echo "RESULT PASS" || echo "RESULT FAIL"
exit $fail
