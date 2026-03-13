#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

set -euox pipefail

: "${J:=1}"

mkdir -p /rocksdb-local-build

rm -rf /rocksdb-local-build/*
cp -r /rocksdb-host/* /rocksdb-local-build
cd /rocksdb-local-build

GETDEPS_SCRATCH_PATH="${GETDEPS_SCRATCH_PATH:-/getdeps-scratch}"
PY="${PYTHON:-python3}"

TOOLSET=""
if hash scl 2>/dev/null; then
  for ts in devtoolset-12 devtoolset-11 devtoolset-8 devtoolset-7 devtoolset-2; do
    if scl --list 2>/dev/null | grep -q "$ts"; then
      TOOLSET="$ts"
      break
    fi
  done
fi

run() {
  local cmd="GETDEPS_SCRATCH_PATH='${GETDEPS_SCRATCH_PATH}' PYTHON='${PY}' $*"

  if [[ -n "${TOOLSET}" ]]; then
    scl enable "${TOOLSET}" "${cmd}"
  else
    bash -lc "${cmd}"
  fi
}

get_folly_path() {
  (
    cd third-party/folly
    "${PY}" build/fbcode_builder/getdeps.py show-inst-dir \
      --scratch-path "${GETDEPS_SCRATCH_PATH}"
  )
}

have_folly() {
  [[ -d "$1/include/folly" && -f "$1/lib/libfolly.a" ]]
}

run "make clean-not-downloaded"
run "make checkout_folly"

FOLLY_PATH="$(get_folly_path)"
echo "Using FOLLY_PATH=$FOLLY_PATH"

BUILD_VARS=(
  "GETDEPS_SCRATCH_PATH=$GETDEPS_SCRATCH_PATH"
  "FOLLY_PATH=$FOLLY_PATH"
  "USE_COROUTINES=1"
  "LIB_MODE=static"
  "PORTABLE=1"
  "J=$J"
)

if have_folly "$FOLLY_PATH"; then
  echo "Reusing prebuilt folly at $FOLLY_PATH"
else
  run "GETDEPS_SCRATCH_PATH=$GETDEPS_SCRATCH_PATH make build_folly"

  have_folly "$FOLLY_PATH" || { echo "Bad FOLLY_PATH: $FOLLY_PATH"; exit 1; }
fi

run "${BUILD_VARS[*]} make rocksdbjavastatic"

cp java/target/librocksdbjni-linux*.so \
   java/target/rocksdbjni-*-linux*.jar \
   java/target/rocksdbjni-*-linux*.jar.sha1 \
   /rocksdb-java-target
