#!/bin/bash -e
function print_usage {
  echo "Usage: $0 [build | release | bump_version version ]"
}

function check_env {
  if [ -z $LEVELDB_HOME ] ; then
    echo "Expect LEVELDB_HOME to be SET"
    exit 1
  fi
  if [ -z $JAVA_HOME ] ; then
    export JAVA_HOME=/usr/local/jdk-6u14-64/
    echo "JAVA_HOME not set. Assuming JAVA_HOME=$JAVA_HOME"
  fi
  if [ -z $LEVELDBJNI_HOME ] ; then
    export LEVELDBJNI_HOME=$LEVELDB_HOME/java/leveldbjni/
    echo "LEVELDBJNI_HOME not set. Assuming LEVELDBJNI_HOME=$LEVELDBJNI_HOME"
  fi
  if [ -z $SNAPPY_HOME ] ; then
    export SNAPPY_HOME="/home/dhruba/snappy-1.0.5"
    echo "SNAPPY_HOME not set. Assuming SNAPPY_HOME=$SNAPPY_HOME"
  fi
  if [ -z $LEVELDB_PATCH ] ; then 
    LEVELDB_PATCH=$LEVELDB_HOME/java/leveldbjni/db.h.patch
    echo "LEVELDB_PATCH not set. Assuming LEVELDB_PATCH=$LEVELDB_PATCH"
  fi
}

function build {
  cd $LEVELDB_HOME
  git apply $LEVELDB_PATCH
  make clean libleveldb.a
  cd $LEVELDB_HOME/java/leveldb/leveldb-api
  mvn clean package
  cd $LEVELDBJNI_HOME
  mvn clean install -P linux64
  cd $LEVELDB_HOME
  git checkout $LEVELDB_HOME/include/leveldb/db.h
}

function release {
  cd $LEVELDB_HOME
  git apply $LEVELDB_PATCH
  make clean libleveldb.a
  cd $LEVELDB_HOME/java/leveldb/leveldb-api
  mvn clean package
  cd $LEVELDBJNI_HOME
  mvn deploy -P linux64 -DskipTests
  cd $LEVELDB_HOME
  git checkout $LEVELDB_HOME/include/leveldb/db.h
}

CMD=$1
if [ -z $CMD ]; then
  print_usage
  exit 1
fi

case "$CMD" in 
  build) 
    check_env
    build
    ;;
  bump_version)
    if [ -z $2 ] ; then
      echo "bump_version requires a version.no parameter at the end"
      exit 1
    fi 
    check_env
    VERSION=$2
    pushd $LEVELDBJNI_HOME
    mvn versions:set -DnewVersion="$VERSION"
    popd
    echo "VERSION SET TO $VERSION"
    ;;
  release)
    check_env
    pushd $LEVELDBJNI_HOME
    release
    popd
    ;;
  *)
    print_usage
    exit 1
    ;;
esac
