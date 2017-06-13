#!/usr/bin/env bash

set -e

rm -rf /rocksdb-local
cp -r /rocksdb-host /rocksdb-local
cd /rocksdb-local
scl enable devtoolset-2 'make jclean clean'
scl enable devtoolset-2 'PORTABLE=1 make rocksdbjavastatic'
cp java/target/librocksdbjni-linux*.so java/target/rocksdbjni-*-linux*.jar /rocksdb-host/java/target

