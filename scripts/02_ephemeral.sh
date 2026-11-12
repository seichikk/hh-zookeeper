#!/bin/bash
source "$(dirname "$0")/common.sh"

window() {
  fifo=$(mktemp -u "${TMPDIR:-/tmp}/zkcli.XXXXXX")
  mkfifo "$fifo"
  zk/server1/bin/zkCli.sh -server "127.0.0.1:$(port "$1")" < "$fifo" > "$OUT/$2.txt" 2>&1 &
  wpid=$!
  disown
  exec 3> "$fifo"
  rm -f "$fifo"
}

closed() {
  while kill -0 "$wpid" 2>/dev/null; do
    sleep 0.1
  done
  grep -vE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:,]+ \[myid' "$OUT/$1.txt" > "$OUT/$1.tmp" && mv "$OUT/$1.tmp" "$OUT/$1.txt"
  clean_out "$OUT/$1.txt"
}

gone() {
  limit 20 zk/server1/bin/zkCli.sh -server 127.0.0.1:2181 stat "$1" 2>&1 | grep -q 'does not exist'
}

created() {
  local n=0
  until grep -q "Created $1" "$OUT/$2.txt"; do
    n=$((n + 1))
    [ $n -gt 150 ] && return 1
    sleep 0.2
  done
}

vanish() {
  local n=0
  until gone "$1"; do
    n=$((n + 1))
    [ $n -gt 90 ] && return 1
    sleep 1
  done
}

body() {
  title "второе окно: zkCli на server2 создаёт эфемерный узел"
  window 2 02_window2
  echo 'create -e /hh-eph "узел из второго окна"' >&3
  created /hh-eph 02_window2 || return 1
  zkcli 1 ls /
  zkcli 1 get -s /hh-eph

  title "закрытие второго окна (SIGHUP)"
  note "$(clock) kill -HUP zkCli"
  t0=$(now)
  pkill -HUP -P "$wpid"
  kill -HUP "$wpid" 2>/dev/null
  exec 3>&-
  closed 02_window2
  zkcli 3 ls /
  vanish /hh-eph || return 1
  hup=$(since "$t0")
  note "$(clock) узел /hh-eph пропал через $hup с после закрытия окна"
  zkcli 3 ls /

  title "третье окно: выход командой quit"
  window 3 02_window3
  echo 'create -e /hh-eph2 "узел из третьего окна"' >&3
  created /hh-eph2 02_window3 || return 1
  zkcli 1 ls /
  note "$(clock) quit"
  t0=$(now)
  echo quit >&3
  exec 3>&-
  closed 02_window3
  vanish /hh-eph2 || return 1
  quit=$(since "$t0")
  note "$(clock) узел /hh-eph2 пропал через $quit с после quit"
  zkcli 1 ls /
  printf '\\renewcommand{\\%s}{%s}\n' ephHup "${hup/./,}" ephQuit "${quit/./,}" > "$OUT/vars-ephemeral.tex"
}

clear 2>/dev/null
capture 02_ephemeral body
