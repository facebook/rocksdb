#!/bin/bash
# Create a tmp directory for the test to use
TEST_DIR=$(mktemp -d $1/fbcode_rocksdb_XXXXXXX)
TEST_TMPDIR="$TEST_DIR" $2 && rm -rf "$TEST_DIR"
