#!/bin/bash
source "$(dirname "$0")/common.sh"

body() {
  title "java 17"
  if [ ! -x "$JDK_DIR/Contents/Home/bin/java" ]; then
    [ -f zk/jdk17.tar.gz ] || curl -fsSL -o zk/jdk17.tar.gz "$JDK_URL" || return 1
    echo "$JDK_SHA256  zk/jdk17.tar.gz" | shasum -a 256 -c || return 1
    mkdir -p "$JDK_DIR"
    tar -xzf zk/jdk17.tar.gz -C "$JDK_DIR" --strip-components 1 || return 1
  fi
  export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
  run /usr/libexec/java_home -V
  run echo "JAVA_HOME=$JAVA_HOME"
  run "$JAVA_HOME/bin/java" -version

  title "zookeeper $ZK_VERSION, три копии"
  [ -f "$ZK_TGZ" ] || curl -fsSL -o "$ZK_TGZ" "$ZK_URL" || return 1
  run shasum -a 512 "$ZK_TGZ"
  echo "$ZK_SHA512  $ZK_TGZ" | shasum -a 512 -c || return 1
  for i in 1 2 3; do
    tar -xzf "$ZK_TGZ" -C zk/server$i --strip-components 1 || return 1
    perl -pi -e "s|^dataDir=.*|dataDir=$LAB/zk/server$i/data|" zk/server$i/conf/zoo.cfg
  done
  run ls zk/server1 zk/server1/conf zk/server1/data
  run zk/server1/bin/zkServer.sh version

  title "образ с python и kazoo для координатора"
  docker build -q -t "$PYIMG" docker/ >/dev/null || return 1
  run docker run --rm "$PYIMG" python -c "import sys, importlib.metadata as m; print('python', sys.version.split()[0], *[p + ' ' + m.version(p) for p in ('kazoo', 'pyarrow', 'boto3')])"
}

clear 2>/dev/null
capture 00_setup body
