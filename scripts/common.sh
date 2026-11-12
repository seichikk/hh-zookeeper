#!/bin/bash

LAB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$LAB" || exit 1

ZK_VERSION=3.9.6
ZK_TGZ=zk/apache-zookeeper-$ZK_VERSION-bin.tar.gz
ZK_URL=https://archive.apache.org/dist/zookeeper/zookeeper-$ZK_VERSION/apache-zookeeper-$ZK_VERSION-bin.tar.gz
ZK_SHA512=e999626df06de30dc8bb53bb51da9bb786b1658406adafc6b92f104d72ca25e869f70a57c83a726f8b9558cc0fb519fc20f201aa5d6da09884bc4be5fe6dd3b0
JDK_DIR="$HOME/Library/Java/JavaVirtualMachines/temurin-17.jdk"
if [ "$(uname -m)" = arm64 ]; then
  JDK_URL=https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.21%2B9/OpenJDK17U-jdk_aarch64_mac_hotspot_17.0.21_9.tar.gz
  JDK_SHA256=78b08c7f5a0598242d5719c022e19479cb4a3aa45c37c19726a757a469ed1c15
else
  JDK_URL=https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.21%2B9/OpenJDK17U-jdk_x64_mac_hotspot_17.0.21_9.tar.gz
  JDK_SHA256=868f5c672b3389fdbe48e44f94ae170f01668b1401360944fc7159d7cc9772cc
fi
if [ -x "$JDK_DIR/Contents/Home/bin/java" ]; then
  export JAVA_HOME="$JDK_DIR/Contents/Home"
fi

PYIMG=hh-zk-py:1
NET=hhnet
ZK_HOSTS=host.docker.internal:2181,host.docker.internal:2182,host.docker.internal:2183
TZ_NAME="$(readlink /etc/localtime | sed 's|.*/zoneinfo/||')"
OUT="$LAB/report/out"
mkdir -p "$OUT" "$LAB/logs"

port() {
  echo $((2180 + $1))
}

title() {
  printf '\n==== %s ====\n' "$*"
}

note() {
  printf '%s\n' "$*"
}

shown() {
  local a out=""
  for a in "$@"; do
    case "$a" in
      *" "* | *"|"* | *"*"*) out="$out \"$a\"" ;;
      *) out="$out $a" ;;
    esac
  done
  printf '%s' "${out# }"
}

run() {
  printf '\n$ %s\n' "$(shown "$@")"
  "$@"
}

clock() {
  perl -MTime::HiRes=time -MPOSIX=strftime -e '$t = time; printf "%s.%03d\n", strftime("%H:%M:%S", localtime $t), ($t - int $t) * 1000'
}

now() {
  perl -MTime::HiRes=time -e 'printf "%.3f\n", time'
}

since() {
  perl -e 'printf "%.1f\n", $ARGV[1] - $ARGV[0]' "$1" "${2:-$(now)}"
}

limit() {
  perl -e '$t = shift; $pid = fork; if (!$pid) { setpgrp(0, 0); exec @ARGV; exit 127 }
    $SIG{ALRM} = sub { $hit = 1; kill "TERM", -$pid }; alarm $t; waitpid $pid, 0;
    exit 124 if $hit; exit($? & 127 ? 128 + ($? & 127) : $? >> 8)' "$@"
}

zkcli() {
  local p
  p=$(port "$1")
  shift
  printf '\n$ zkCli.sh -server 127.0.0.1:%s %s\n' "$p" "$(shown "$@")"
  limit 20 zk/server1/bin/zkCli.sh -server "127.0.0.1:$p" "$@" 2>&1 \
    | grep -vE '^(Connecting to |$|WATCHER::|WatchedEvent state)|^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:,]+ \[myid'
  return "${PIPESTATUS[0]}"
}

srvr() {
  (printf srvr; sleep 0.5) | nc 127.0.0.1 "$(port "$1")" 2>/dev/null
}

mode() {
  srvr "$1" | sed -n 's/^Mode: //p'
}

roles() {
  local i out
  printf '\n$ echo srvr | nc 127.0.0.1 2181..2183\n'
  for i in 1 2 3; do
    out=$(srvr "$i")
    if [ -z "$out" ]; then
      out="соединение отклонено"
    elif echo "$out" | grep -q '^Mode:'; then
      out=$(echo "$out" | grep -E '^(Mode|Zxid|Node count):' | tr '\n' ' ')
    fi
    printf 'server%s (порт %s): %s\n' "$i" "$(port "$i")" "$out"
  done
}

py() {
  docker run --rm --network "$NET" --add-host host.docker.internal:host-gateway -e ZK_HOSTS="$ZK_HOSTS" -e TZ="$TZ_NAME" \
    -v "$LAB/lab:/app/lab:ro" -v "$LAB/logs:/app/logs" -w /app "$PYIMG" python "$@"
}

runpy() {
  printf '\n$ python %s\n' "$*"
  py "$@"
}

clean_out() {
  [ -f "$1" ] || return 0
  perl -CSD -i -pe 's/\e\[[0-9;?]*[A-Za-z]//g; s/\r//g; s/[^\x{09}\x{0A}\x{20}-\x{7E}\x{401}\x{410}-\x{44F}\x{451}\x{AB}\x{BB}\x{2013}\x{2014}\x{2116}]/?/g' "$1" 2>/dev/null
  expand "$1" > "$1.tmp" && mv "$1.tmp" "$1"
}

capture() {
  local name="$1"
  shift
  "$@" 2>&1 | tee "$OUT/$name.txt"
  local rc=${PIPESTATUS[0]}
  clean_out "$OUT/$name.txt"
  return $rc
}

storage() {
  docker start rustfs >/dev/null || exit 1
  for _ in $(seq 1 60); do
    docker run --rm --network "$NET" "$PYIMG" python -c "import urllib.request; urllib.request.urlopen('http://rustfs:9000/health/live', timeout=2)" >/dev/null 2>&1 && return 0
    sleep 1
  done
  exit 1
}

serving() {
  local i ok
  for _ in $(seq 1 120); do
    ok=1
    for i in "$@"; do
      [ -n "$(mode "$i")" ] || ok=0
    done
    [ $ok = 1 ] && return 0
    sleep 0.5
  done
  return 1
}

leader() {
  local i
  for i in 1 2 3; do
    [ "$(mode "$i")" = leader ] && echo "$i" && return 0
  done
  return 1
}
