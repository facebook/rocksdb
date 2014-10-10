#!/bin/bash
# REQUIRE: benchmark.sh exists in the current directory
# After execution of this script, log files are generated in $output_dir.
# report.txt provides a high level statistics

# Size constants
K=1024
M=$((1024 * K))
G=$((1024 * M))

n=$((1 * G))
wps=$((80 * K))
duration=$((12 * 60 * 60))
num_read_threads=24

# Update these parameters before execution !!!
db_dir="/tmp/rocksdb/"
wal_dir="/tmp/rocksdb/"
output_dir="/tmp/output"

# Test 1: bulk load
OUTPUT_DIR=$output_dir NUM_KEYS=$n DB_DIR=$db_dir WAL_DIR=$wal_dir \
  ./benchmark.sh bulkload

# Test 2: sequential fill
OUTPUT_DIR=$output_dir NUM_KEYS=$n DB_DIR=$db_dir WAL_DIR=$wal_dir \
  ./benchmark.sh fillseq

# Test 3: overwrite
OUTPUT_DIR=$output_dir NUM_KEYS=$n DB_DIR=$db_dir WAL_DIR=$wal_dir \
  ./benchmark.sh overwrite

# Prepare: populate DB with random data
OUTPUT_DIR=$output_dir NUM_KEYS=$n DB_DIR=$db_dir WAL_DIR=$wal_dir \
  ./benchmark.sh filluniquerandom

# Test 4: random read
OUTPUT_DIR=$output_dir NUM_KEYS=$n DB_DIR=$db_dir WAL_DIR=$wal_dir \
  DURATION=$duration NUM_READ_THREADS=$num_read_threads \
  ./benchmark.sh readrandom

# Test 5: random read while writing
OUTPUT_DIR=$output_dir NUM_KEYS=$n DB_DIR=$db_dir WAL_DIR=$wal_dir \
  DURATION=$duration NUM_READ_THREADS=$num_read_threads WRITES_PER_SECOND=$wps \
  ./benchmark.sh readwhilewriting
