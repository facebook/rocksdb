#!/bin/bash -e
cd $LEVELDB_HOME
git apply $LEVELDB_HOME/java/leveldbjni/db.h.patch
make clean libleveldb.a
cd $LEVELDB_HOME
cd java/leveldb/leveldb-api
mvn clean package
export JAVA_HOME=/usr/local/jdk-6u14-64/
export LEVELDBJNI_HOME=$LEVELDB_HOME/java/leveldbjni/
export SNAPPY_HOME=/home/dhruba/snappy-1.0.5
cd $LEVELDBJNI_HOME
mvn clean install -P linux64
cd $LEVELDB_HOME
git checkout $LEVELDB_HOME/include/leveldb/db.h
