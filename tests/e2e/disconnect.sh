#!/usr/bin/env bash
# Client disconnect with the real worker.
# A long job runs on the engine. The client closes the connection after 5 s.
# The hub must send `cancel`, the engine must become idle within a few seconds,
# and the next short request must work.
#
#   HUB=http://127.0.0.1:8780 KEY=<key> LONG_WAV=long.wav PT_WAV=clip.wav HUB_LOG=hub.log tests/e2e/disconnect.sh
set -u
HUB=${HUB:?set HUB}
KEY=${KEY:?set KEY}
LONG_WAV=${LONG_WAV:?set LONG_WAV}
PT_WAV=${PT_WAV:?set PT_WAV}
HUB_LOG=${HUB_LOG:?set HUB_LOG}
MODEL=${MODEL:-Qwen3-ASR-1.7B}
fail=0

state() { curl -s -m 5 -H "Authorization: Bearer $KEY" "$HUB/running" | python3 -c "
import json,sys
v=json.load(sys.stdin)
e=[x for x in v['engines'] if x['model']=='$MODEL'][0]
print(e['state'], 'job' if e['job'] else '-')"; }

echo "state before: $(state)"
lines0=$(wc -l < "$HUB_LOG")
# The client gives up after 5 s (curl -m 5 closes the connection).
curl -s -m 5 -o /dev/null -H "Authorization: Bearer $KEY" -F file=@"$LONG_WAV" -F model=$MODEL "$HUB/v1/audio/transcriptions"
echo "client closed the connection (curl exit $?)"
t0=$(python3 -c 'import time;print(time.time())')
for i in $(seq 1 40); do
  s=$(state)
  case "$s" in idle*) break ;; esac
  sleep 0.25
done
t1=$(python3 -c 'import time;print(time.time())')
echo "engine state: $s  (time from the disconnect to idle: $(python3 -c "print(round($t1-$t0,2))") s)"
case "$s" in "idle -") ;; *) fail=1 ;; esac
echo "--- hub log:"; tail -n +$((lines0 + 1)) "$HUB_LOG" | grep -E "client is gone|cancel|exited|start"
t2=$(python3 -c 'import time;print(time.time())')
out=$(curl -s -m 60 -w ' [%{http_code}]' -H "Authorization: Bearer $KEY" -F file=@"$PT_WAV" -F model=$MODEL -F language=pt "$HUB/v1/audio/transcriptions")
t3=$(python3 -c 'import time;print(time.time())')
echo "next request: $out  ($(python3 -c "print(round($t3-$t2,2))") s)"
case "$out" in *"[200]") ;; *) fail=1 ;; esac
echo "state after: $(state)"
[ $fail = 0 ] && echo "RESULT PASS" || echo "RESULT FAIL"
exit $fail
