#!/usr/bin/env bash

set -e

cat >entrypoint.sh <<ENTRYPOINT
#!/bin/bash -e
export PATH=\${PATH}:\${JAVA_HOME}/bin
cd /rocksdb
make jclean clean
PORTABLE=1 make rocksdbjavastatic
cp /rocksdb/java/target/librocksdbjni-* /rocksdb-build
cp /rocksdb/java/target/rocksdbjni-* /rocksdb-build
ENTRYPOINT
chmod +x entrypoint.sh

cat >Dockerfile <<DOCKERFILE
FROM alpine:3.6

# 'jemalloc jemalloc-dev' cause loader errors
RUN apk add --update bash git build-base coreutils file linux-headers wget tar curl perl openssl zlib zlib-dev lz4 lz4-dev bzip2 bzip2-dev snappy snappy-dev zstd zstd-dev openjdk7
RUN cd /tmp && \
    git clone -b v2.0 --single-branch https://github.com/gflags/gflags.git && \
    cd gflags && \
    ./configure && make && make install && \
    rm -rf /tmp/*
ADD entrypoint.sh /
RUN chmod +x /entrypoint.sh
ENV JAVA_HOME=/usr/lib/jvm/java-1.7-openjdk
CMD /entrypoint.sh
DOCKERFILE

docker build -t rocksdbbuild:latest . &&\
docker run -v /rocksdb:/rocksdb -v /rocksdb-build:/rocksdb-build -i --rm rocksdbbuild:latest

