#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
#
VER=8.1.0
PLATFORM=osx
make clean jclean
make -j12
make -j12 rocksdbjava
(cd java; mvn install:install-file -Dfile=./target/rocksdbjni-${VER}-${PLATFORM}.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=${VER} -Dpackaging=jar)
(cd java/jmh; mvn clean package)
echo "Very quick run of a single benchmark test.."
(cd java/jmh; java --enable-preview --enable-native-access=ALL-UNNAMED -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=100000 -p keySize=128 -p valueSize=65536 -p columnFamilyTestType="no_column_family" -rf csv org.rocksdb.jmh.GetBenchmarks.ffiGetPinnableSlice -wi 1 -to 1m -i 1)
echo "Run all get benchmarks.."
(cd java/jmh; java --enable-preview --enable-native-access=ALL-UNNAMED -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=100000 -p keySize=128 -p valueSize=65536 -p columnFamilyTestType="no_column_family" -rf csv org.rocksdb.jmh.GetBenchmarks)
