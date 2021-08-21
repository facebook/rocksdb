#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

set -e

# update Alpine with latest versions
echo '@edge http://nl.alpinelinux.org/alpine/edge/main' >> /etc/apk/repositories
echo '@community http://nl.alpinelinux.org/alpine/edge/community' >> /etc/apk/repositories
apk update
apk upgrade

# install CA certificates
apk add ca-certificates

# install build tools
apk add \
  build-base \
  coreutils \
  file \
  git \
  perl \
  automake \
  autoconf \
  cmake

# install tool dependencies for building RocksDB static library
apk add \
  curl \
  bash \
  wget \
  tar \
  openssl

# install RocksDB dependencies
apk add \
  snappy snappy-dev \
  zlib zlib-dev \
  bzip2 bzip2-dev \
  lz4 lz4-dev \
  zstd zstd-dev \
  linux-headers \
  jemalloc jemalloc-dev

# install OpenJDK7
apk add openjdk7 \
  && apk add java-cacerts \
  && rm /usr/lib/jvm/java-1.7-openjdk/jre/lib/security/cacerts \
  && ln -s /etc/ssl/certs/java/cacerts /usr/lib/jvm/java-1.7-openjdk/jre/lib/security/cacerts

# cleanup
rm -rf /var/cache/apk/*

# puts javac in the PATH
export JAVA_HOME=/usr/lib/jvm/java-1.7-openjdk
export PATH=/usr/lib/jvm/java-1.7-openjdk/bin:$PATH

# gflags from source
cd /tmp &&\
  git clone -b v2.0 --single-branch https://github.com/gflags/gflags.git &&\
  cd gflags &&\
  ./configure --prefix=/usr && make && make install &&\
  rm -rf /tmp/*


# build rocksdb
cd /rocksdb
make jclean clean
PORTABLE=1 make -j8 rocksdbjavastatic
cp /rocksdb/java/target/librocksdbjni-* /rocksdb-build
cp /rocksdb/java/target/rocksdbjni-* /rocksdb-build
