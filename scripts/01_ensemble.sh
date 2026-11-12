#!/bin/bash
source "$(dirname "$0")/common.sh"

body() {
  title "конфигурации трёх серверов"
  run cat zk/server1/conf/zoo.cfg
  run diff zk/server1/conf/zoo.cfg zk/server2/conf/zoo.cfg
  run diff zk/server1/conf/zoo.cfg zk/server3/conf/zoo.cfg
  run grep -H . zk/server1/data/myid zk/server2/data/myid zk/server3/data/myid

  for i in 1 2 3; do
    zk/server$i/bin/zkServer.sh stop >/dev/null 2>&1
  done
  title "запуск ансамбля"
  for i in 1 2 3; do
    run zk/server$i/bin/zkServer.sh start || return 1
  done
  serving 1 2 3 || return 1
  run zk/server1/bin/zkServer.sh status
  printf '\n$ echo srvr | nc 127.0.0.1 2181\n'
  srvr 1
  roles
  zkcli 1 create /hh-test "три сервера"

  first=$(leader)
  rest=()
  for i in 1 2 3; do
    [ "$i" = "$first" ] || rest+=("$i")
  done
  title "остановка одного сервера: лидер server$first"
  run zk/server$first/bin/zkServer.sh stop
  t0=$(now)
  serving "${rest[@]}" || return 1
  reelect=$(since "$t0")
  note "новый лидер выбран за $reelect с"
  roles
  zkcli "${rest[0]}" get /hh-test
  zkcli "${rest[1]}" set /hh-test "два сервера из трёх"
  zkcli "${rest[0]}" get /hh-test

  second=${rest[0]}
  last=${rest[1]}
  title "остановка второго сервера: server$second"
  run zk/server$second/bin/zkServer.sh stop
  sleep 2
  roles
  zkcli "$last" get /hh-test

  title "запуск остановленных серверов"
  run zk/server$first/bin/zkServer.sh start
  run zk/server$second/bin/zkServer.sh start
  t0=$(now)
  serving 1 2 3 || return 1
  quorum=$(since "$t0")
  note "кворум собран за $quorum с"
  roles
  zkcli "$first" get -s /hh-test
  printf '\\renewcommand{\\%s}{%s}\n' ensFirst "$first" ensSecond "$second" ensLast "$last" \
    ensReelect "${reelect/./,}" ensQuorum "${quorum/./,}" > "$OUT/vars-ensemble.tex"
}

clear 2>/dev/null
capture 01_ensemble body
