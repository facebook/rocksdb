#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# REQUIRE: benchmark.sh exists in the current directory
# After execution of this script, log files are generated in $output_dir.
# report.txt provides a high level statistics

# This should be run from the parent of the tools directory. The command line is:
#   [$env_vars] tools/run_blob_bench.sh
#
# This runs the following sequence of tests for various value sizes, with and
# without blob files enabled:
#   step 1) load - bulkload, compact, overwrite
#   step 2) read-write - readwhilewriting, seekwhilewriting
#   step 3) read-only - readrandom, seekrandom
#

# Exit Codes
EXIT_INVALID_ARGS=1

# Size constants
K=1024
M=$((1024 * K))
G=$((1024 * M))
T=$((1024 * G))

if [ -z $DB_DIR ]; then
  echo "DB_DIR is not defined"
  exit $EXIT_INVALID_ARGS
fi

if [ -z $WAL_DIR ]; then
  echo "WAL_DIR is not defined"
  exit $EXIT_INVALID_ARGS
fi

db_dir=$DB_DIR
wal_dir=$WAL_DIR
output_dir=${OUTPUT_DIR:-"/tmp/blob_bench_output"}

num_threads=${NUM_THREADS:-16}

compression_type=${COMPRESSION_TYPE:-lz4}

db_size=${DB_SIZE:-$((1 * T))}
value_size=${VALUE_SIZE:-$((1 * K))}
num_keys=${NUM_KEYS:-$(($db_size / $value_size))}

duration=${DURATION:-1800}

enable_blob_files=${ENABLE_BLOB_FILES:-1}
min_blob_size=${MIN_BLOB_SIZE:-0}
blob_file_size=${BLOB_FILE_SIZE:-$((1 * G))}
blob_compression_type=${BLOB_COMPRESSION_TYPE:-lz4}
enable_blob_garbage_collection=${ENABLE_BLOB_GC:-1}
blob_garbage_collection_age_cutoff=${BLOB_GC_AGE_CUTOFF:-0.25}

write_buffer_size=${WRITE_BUFFER_SIZE:-$blob_file_size}

target_file_size_base=$blob_file_size
if [ "$enable_blob_files" == "1" ]; then
  target_file_size_base=$(($blob_file_size / $value_size * 32))
fi

max_bytes_for_level_base=$((8 * $target_file_size_base))

echo "======================== Value size: $value_size, blob files: $enable_blob_files, GC cutoff: $blob_garbage_collection_age_cutoff ========================"

rm -rf $db_dir
rm -rf $wal_dir
rm -rf $output_dir
mkdir -p $output_dir

ENV_VARS="\
  DB_DIR=$db_dir \
  WAL_DIR=$wal_dir \
  OUTPUT_DIR=$output_dir \
  NUM_THREADS=$num_threads \
  COMPRESSION_TYPE=$compression_type \
  VALUE_SIZE=$value_size \
  NUM_KEYS=$num_keys"

ENV_VARS_D="$ENV_VARS DURATION=$duration"

PARAMS="\
  --enable_blob_files=$enable_blob_files \
  --min_blob_size=$min_blob_size \
  --blob_file_size=$blob_file_size \
  --blob_compression_type=$blob_compression_type \
  --write_buffer_size=$write_buffer_size \
  --target_file_size_base=$target_file_size_base \
  --max_bytes_for_level_base=$max_bytes_for_level_base"

PARAMS_GC="$PARAMS \
  --enable_blob_garbage_collection=$enable_blob_garbage_collection \
  --blob_garbage_collection_age_cutoff=$blob_garbage_collection_age_cutoff"

echo -e "ops/sec\tmb/sec\tSize-GB\tL0_GB\tSum_GB\tW-Amp\tW-MB/s\tusec/op\tp50\tp75\tp99\tp99.9\tp99.99\tUptime\tStall-time\tStall%\tTest" \
  > $output_dir/report.txt

# bulk load (using fillrandom) + compact
env $ENV_VARS ./tools/benchmark.sh bulkload $PARAMS
echo -n "Disk usage after bulkload: " >> $output_dir/report.txt
du $db_dir >> $output_dir/report.txt

# overwrite + waitforcompaction
env $ENV_VARS ./tools/benchmark.sh overwrite $PARAMS_GC
echo -n "Disk usage after overwrite: " >> $output_dir/report.txt
du $db_dir >> $output_dir/report.txt

# readwhilewriting
env $ENV_VARS_D ./tools/benchmark.sh readwhilewriting $PARAMS_GC
echo -n "Disk usage after readwhilewriting: " >> $output_dir/report.txt
du $db_dir >> $output_dir/report.txt

# fwdrangewhilewriting
env $ENV_VARS_D ./tools/benchmark.sh fwdrangewhilewriting $PARAMS_GC
echo -n "Disk usage after fwdrangewhilewriting: " >> $output_dir/report.txt
du $db_dir >> $output_dir/report.txt

# readrandom
env $ENV_VARS_D ./tools/benchmark.sh readrandom $PARAMS_GC
echo -n "Disk usage after readrandom: " >> $output_dir/report.txt
du $db_dir >> $output_dir/report.txt

# fwdrange
env $ENV_VARS_D ./tools/benchmark.sh fwdrange $PARAMS_GC
echo -n "Disk usage after fwdrange: " >> $output_dir/report.txt
du $db_dir >> $output_dir/report.txt

cp $db_dir/LOG* $output_dir/

echo bulkload > $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep bulkload $output_dir/report.txt >> $output_dir/report2.txt

echo overwrite sync=0 >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep overwrite $output_dir/report.txt | grep \.s0  >> $output_dir/report2.txt

echo readwhilewriting >> $output_dir/report2.txt >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep readwhilewriting $output_dir/report.txt >> $output_dir/report2.txt

echo fwdrangewhilewriting >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep fwdrangewhilewriting $output_dir/report.txt >> $output_dir/report2.txt

echo readrandom >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep readrandom $output_dir/report.txt  >> $output_dir/report2.txt

echo fwdrange >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep fwdrange\.t $output_dir/report.txt >> $output_dir/report2.txt

cat $output_dir/report2.txt
