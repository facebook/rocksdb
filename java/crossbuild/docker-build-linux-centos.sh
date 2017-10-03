#!/usr/bin/env bash

set -e

rm -rf /rocksdb-local
cp -r /rocksdb-host /rocksdb-local
cd /rocksdb-local

# Use scl devtoolset if available (i.e. CentOS <7)
if hash scl 2>/dev/null; then
	scl enable devtoolset-2 'make jclean clean'
	scl enable devtoolset-2 'PORTABLE=1 make rocksdbjavastatic'
else
	make jclean clean
        PORTABLE=1 make rocksdbjavastatic
fi

cp java/target/librocksdbjni-linux*.so java/target/rocksdbjni-*-linux*.jar /rocksdb-host/java/target

