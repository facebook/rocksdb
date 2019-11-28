#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

set -e
#set -x

# just in-case this is run outside Docker
mkdir -p /rocksdb-local-build

rm -rf /rocksdb-local-build/*
cp -r /rocksdb-host/* /rocksdb-local-build
cd /rocksdb-local-build

make clean-not-downloaded
PORTABLE=1 make rocksdbjavastatic

cp java/target/librocksdbjni-linux*.so java/target/rocksdbjni-*-linux*.jar /rocksdb-java-target

