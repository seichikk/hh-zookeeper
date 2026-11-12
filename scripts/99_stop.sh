#!/bin/bash
source "$(dirname "$0")/common.sh"

docker rm -f coord-a coord-b >/dev/null 2>&1
for i in 1 2 3; do
  zk/server$i/bin/zkServer.sh stop
done
