#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
# A shell script to load some pre generated data file to a DB using ldb tool
# ./ldb needs to be avaible to be executed.
#
# Usage: <SCRIPT> <input_data_path> <DB Path>

if [ "$#" -lt 2 ]; then
  echo "usage: $BASH_SOURCE <input_data_path> <DB Path>"
  exit 1
fi

input_data_dir=$1
db_dir=$2
rm -rf $db_dir

second_gen_compression_support=
mixed_compression_support=
# Support for `ldb --version` is a crude under-approximation for versions
# supporting dictionary compression and algorithms including zstd and lz4
if ./ldb --version 2>/dev/null >/dev/null; then
  second_gen_compression_support=1

  if ./ldb load --db=$db_dir --compression_type=mixed --create_if_missing \
      < /dev/null 2>/dev/null >/dev/null; then
    mixed_compression_support=1
  fi
  rm -rf $db_dir
fi

echo == Loading data from $input_data_dir to $db_dir

declare -a compression_opts=("no" "snappy" "zlib" "bzip2")
allow_dict=0

if [ "$second_gen_compression_support" == 1 ]; then
  if [ "$mixed_compression_support" == 1 ]; then
    compression_opts=("zstd" "no" "snappy" "zlib" "bzip2" "lz4" "lz4hc" "mixed")
  else
    compression_opts=("zstd" "no" "snappy" "zlib" "bzip2" "lz4" "lz4hc")
  fi
fi

set -e

n=$RANDOM
c_count=${#compression_opts[@]}

for f in `ls -1 $input_data_dir`
do
  # NOTE: This will typically accumulate the loaded data into a .log file which
  # will only be flushed to an SST file on recovery in the next iteration, so
  # compression settings of this iteration might only apply to data from the
  # previous iteration (if there was one). This has the advantage of leaving a
  # WAL file for testing its format compatibility (in addition to SST files
  # etc.)
  c=${compression_opts[n % c_count]}
  d=$((n / c_count % 2 * 12345))
  echo == Loading $f with compression $c dict bytes $d
  if [ "$second_gen_compression_support" == 1 ]; then
    d_arg=--compression_max_dict_bytes=$d
  else
    d_arg=""
  fi
  ./ldb load --db=$db_dir --compression_type=$c $d_arg --bloom_bits=10 \
    --auto_compaction=false --create_if_missing < $input_data_dir/$f
  let "n = n + 1"
done
