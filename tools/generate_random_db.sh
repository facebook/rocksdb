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

# Check if deleterange command is supported by grepping ldb --help
deleterange_support=
if ./ldb --help 2>&1 | grep -q deleterange; then
  deleterange_support=1
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

  # Use md5sum of file to deterministically decide whether to add a range
  # tombstone (approximately 1/4 of files) and which key to delete
  file_path=$input_data_dir/$f
  hash=$(md5sum "$file_path" | cut -c1-8)
  hash_int=$((16#$hash))

  if [ $((hash_int % 4)) -eq 0 ]; then
    # Pick a key from this file based on the hash
    line_count=$(wc -l < "$file_path")
    if [ "$line_count" -gt 0 ]; then
      line_num=$((hash_int % line_count + 1))
      key=$(sed -n "${line_num}p" "$file_path" | cut -d' ' -f1)
      if [ -n "$key" ]; then
        # Create end key by appending a character to make a small range
        end_key="${key}0"
        if [ "$deleterange_support" == "1" ]; then
          echo "== Deleting range [$key, $end_key) from $f"
          ./ldb deleterange --db=$db_dir "$key" "$end_key"
        else
          # Fall back to point delete for equivalent logical contents
          echo "== Deleting key $key from $f"
          ./ldb delete --db=$db_dir "$key"
        fi
      fi
    fi
  fi

  let "n = n + 1"
done
