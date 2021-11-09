#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

# Build Script for building RocksJava Static
# binaries on CentOS within a Docker container

set -e
#set -x

HOST_SRC_DIR="${HOST_SRC_DIR:-/rocksdb-host}"
CONTAINER_SRC_DIR="${CONTAINER_SRC_DIR:-/rocksdb-local-build}"
CONTAINER_TARGET_DIR="${CONTAINER_TARGET_DIR:-/rocksdb-java-target}"
J="${J:-2}"
ROCKSDB_JAVA_TARGET="rocksdbjavastatic"
if [ -z "$SHARED_LIB" ]; then
	ROCKSDB_JAVA_TARGET="rocksdbjavastatic"
else
	ROCKSDB_JAVA_TARGET="rocksdbjava"
fi

# Just in-case this is run outside the Docker container
mkdir -p $CONTAINER_SRC_DIR
mkdir -p $CONTAINER_TARGET_DIR

# Copy RocksDB from the Docker host into the container
if [ -z "$SKIP_COPY" ]; then
	rm -rf $CONTAINER_SRC_DIR/*
	cp -r $HOST_SRC_DIR/* $CONTAINER_SRC_DIR
	cd $CONTAINER_SRC_DIR
fi

# Use scl devtoolset if available
if hash scl 2>/dev/null; then
	if scl --list | grep -q 'devtoolset-8'; then
		# CentOS 6+
                scl enable devtoolset-8 'make clean-not-downloaded'
		echo "Using scl devtoolset-8"
                scl enable devtoolset-8 "PORTABLE=1 make -j$J $ROCKSDB_JAVA_TARGET"
	elif scl --list | grep -q 'devtoolset-7'; then
		# CentOS 6+
		scl enable devtoolset-7 'make clean-not-downloaded'
		echo "Using scl devtoolset-7"
		scl enable devtoolset-7 "PORTABLE=1 make -j$J $ROCKSDB_JAVA_TARGET"
	elif scl --list | grep -q 'devtoolset-2'; then
		# CentOS 5 or 6
		scl enable devtoolset-2 'make clean-not-downloaded'
		echo "Using scl devtoolset-2"
		scl enable devtoolset-2 "PORTABLE=1 make -j$J $ROCKSDB_JAVA_TARGET"
	else
		echo "Could not find devtoolset"
		exit 1;
	fi
else
	echo "No suitable scl devtoolset found"
	make clean-not-downloaded
	PORTABLE=1 make -j$J $ROCKSDB_JAVA_TARGET
fi

cp java/target/librocksdbjni-linux*.so java/target/rocksdbjni-*-linux*.jar java/target/rocksdbjni-*-linux*.jar.sha1 $CONTAINER_TARGET_DIR
