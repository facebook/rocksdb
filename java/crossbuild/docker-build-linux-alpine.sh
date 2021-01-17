#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

# Build Script for building RocksJava Static
# binaries on Alpine Linux within a Docker container

set -e
#set -x

HOST_SRC_DIR="${HOST_SRC_DIR:-/rocksdb-host}"
CONTAINER_SRC_DIR="${CONTAINER_SRC_DIR:-/rocksdb-local-build}"
CONTAINER_TARGET_DIR="${CONTAINER_TARGET_DIR:-/rocksdb-java-target}"
J="${J:-2}"

# Just in-case this is run outside the Docker container
mkdir -p $CONTAINER_SRC_DIR
mkdir -p $CONTAINER_TARGET_DIR

# Copy RocksDB from the Docker host into the container
if [ -z "$SKIP_COPY" ]; then
	rm -rf $CONTAINER_SRC_DIR/*
	cp -r $HOST_SRC_DIR/* $CONTAINER_SRC_DIR
	cd $CONTAINER_SRC_DIR
fi

make clean-not-downloaded
PORTABLE=1 make -j$J rocksdbjavastatic

cp java/target/librocksdbjni-linux*.so java/target/rocksdbjni-*-linux*.jar java/target/rocksdbjni-*-linux*.jar.sha1 $CONTAINER_TARGET_DIR
