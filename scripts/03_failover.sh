#!/bin/bash
source "$(dirname "$0")/common.sh"

FENCING=${FENCING:-0}
PAUSE_AFTER=${PAUSE_AFTER:-8}
HOLD=${HOLD:-40}
DELAY=${DELAY:-1}
if [ "$FENCING" = 1 ]; then
  name=04_fencing
  tag=fence
else
  name=03_failover
  tag=nofence
fi

coord() {
  printf '\n$ docker run -d --name coord-%s -e FENCING=%s -e ZK_TIMEOUT=10 ... python -m lab.coordinator --name %s --run %s --task-delay %s\n' \
    "$1" "$FENCING" "$1" "$tag" "$DELAY"
  docker run -d --name coord-$1 --network "$NET" --add-host host.docker.internal:host-gateway \
    -e ZK_HOSTS="$ZK_HOSTS" -e ZK_TIMEOUT=10 -e FENCING="$FENCING" \
    -e TZ="$TZ_NAME" -v "$LAB/lab:/app/lab:ro" -v "$LAB/logs:/app/logs" -w /app "$PYIMG" \
    python -m lab.coordinator --name "$1" --run "$tag" --task-delay "$DELAY" >/dev/null || return 1
  docker logs -f coord-$1 2>&1 &
}

seen() {
  docker logs coord-$1 2>&1 | grep -c "$2"
}

wait_for() {
  local n=0
  until [ "$(seen "$1" "$2")" -ge "${3:-1}" ]; do
    n=$((n + 1))
    state=$(docker inspect -f '{{.State.Status}}' coord-$1 2>/dev/null)
    if [ $n -gt 1500 ] || [ -z "$state" ] || [ "$state" = exited ]; then
      note "coord-$1: не дождался строки «$2»"
      return 1
    fi
    sleep 0.1
  done
}

logtod() {
  docker logs coord-$1 2>&1 | grep -m1 "$2" | perl -ne 'printf "%.3f\n", $1 * 3600 + $2 * 60 + $3 if /^(\d\d):(\d\d):(\d\d\.\d+)/'
}

timeline() {
  {
    grep -h -E "стал лидером|беру из s3|записал результат|отклонена|LOST" logs/$tag-a.log logs/$tag-b.log
    echo "$pclock [скрипт] docker pause coord-a"
    echo "$uclock [скрипт] docker unpause coord-a"
  } | sort | perl -CSD -Mutf8 -ne 'BEGIN { $p = shift @ARGV } next unless /^(\d\d):(\d\d):(\d\d\.\d+) \[(.+?)\] (.*)$/;
    ($h, $m, $s, $w, $e) = ($1, $2, $3, $4, $5); $d = $h * 3600 + $m * 60 + $s - $p;
    $e =~ s/, s3:.*//; $e =~ s/, их результаты беру из s3//;
    printf "%s:%s:%s & %+.1f & %s & %s \\\\\n", $h, $m, $s, $d, $w, $e' "$p0" | sed 's/_/\\_/g'
}

experiment() {
  title "две копии координатора, fencing=$FENCING, тайм-аут сессии 10 с"
  docker rm -f coord-a coord-b >/dev/null 2>&1
  rm -f logs/$tag-a.log logs/$tag-b.log
  runpy -m lab.coordinator reset --run $tag || return 1
  coord a || return 1
  wait_for a "стал лидером" || return 1
  coord b || return 1
  wait_for b "жду" || return 1
  wait_for a "чекпоинт сохранён" "$PAUSE_AFTER" || return 1

  printf '\n$ docker pause coord-a\n'
  docker pause coord-a >/dev/null || return 1
  t0=$(now)
  pclock=$(clock)
  p0=$(echo "$pclock" | perl -ne 'printf "%.3f", $1 * 3600 + $2 * 60 + $3 if /^(\d\d):(\d\d):(\d\d\.\d+)/')
  note "$pclock coord-a приостановлен"
  wait_for b "стал лидером" || return 1
  switch=$(perl -e 'printf "%.1f", $ARGV[1] - $ARGV[0]' "$p0" "$(logtod b "стал лидером")")
  note "лидер сменился через $switch с после паузы"
  rc_b=$(docker wait coord-b)
  until [ "$(perl -e 'print $ARGV[1] - $ARGV[0] >= $ARGV[2] ? 1 : 0' "$t0" "$(now)" "$HOLD")" = 1 ]; do
    sleep 0.2
  done
  printf '\n$ docker unpause coord-a\n'
  docker unpause coord-a >/dev/null || return 1
  paused=$(since "$t0")
  uclock=$(clock)
  note "$uclock coord-a снова работает, пауза длилась $paused с"
  rc_a=$(docker wait coord-a)
  wait
  note "coord-b завершился с кодом $rc_b, coord-a с кодом $rc_a"

  title "итог"
  run grep -h -E "стал лидером|беру из s3|контроль|совпадает|записал результат|отклонена" logs/$tag-a.log logs/$tag-b.log
  zkcli 1 get -s /hh/result
  zkcli 1 get /hh/epoch
  zkcli 1 ls /hh/checkpoint
  ea=$(grep -m1 -o 'стал лидером, эпоха [0-9]*' logs/$tag-a.log | grep -o '[0-9]*$')
  eb=$(grep -m1 -o 'стал лидером, эпоха [0-9]*' logs/$tag-b.log | grep -o '[0-9]*$')
  printf '\\renewcommand{\\%s}{%s}\n' ${tag}Switch "${switch/./,}" ${tag}Paused "${paused/./,}" \
    ${tag}EpochA "$ea" ${tag}EpochB "$eb" > "$OUT/vars-$tag.tex"
  timeline > "$OUT/timeline-$tag.tex"
  verify
}

verify() {
  local want_rc=0 want_owner=a want_line="записал результат"
  if [ "$FENCING" = 1 ]; then
    want_rc=1
    want_owner=b
    want_line="отклонена: BadVersionError"
  fi
  [ "$rc_b" = 0 ] && [ "$rc_a" = "$want_rc" ] && grep -q "$want_line" logs/$tag-a.log &&
    limit 20 zk/server1/bin/zkCli.sh -server 127.0.0.1:2181 get /hh/result 2>/dev/null | grep -q "\"owner\": \"$want_owner\"" && return 0
  note "итог не тот: коды b=$rc_b a=$rc_a, ждали a=$want_rc, строка «$want_line» в журнале a, владелец указателя $want_owner"
  return 1
}

cleanup() {
  kill $(jobs -p) 2>/dev/null
  wait 2>/dev/null
  docker unpause coord-a >/dev/null 2>&1
  docker rm -f coord-a coord-b >/dev/null 2>&1
}

body() {
  trap 'cleanup; exit 130' INT TERM
  experiment
  local rc=$?
  cleanup
  return $rc
}

storage
clear 2>/dev/null
capture $name body &
pid=$!
trap 'pkill -TERM -P $pid 2>/dev/null; sleep 1; kill -TERM $pid 2>/dev/null; cleanup; exit 143' INT TERM
wait $pid
