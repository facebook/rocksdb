#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
# BlobDB benchmark script
#
# REQUIRES: benchmark.sh is in the tools subdirectory
#
# After the execution of this script, log files are available in $output_dir.
# report.tsv provides high level statistics.
#
# Should be run from the parent of the tools directory. The command line is:
#   [$env_vars] tools/run_blob_bench.sh
#
# This runs the following sequence of BlobDB performance tests:
#   phase 1) write-only - bulkload+compact, overwrite+waitforcompaction
#   phase 2) read-write - readwhilewriting, fwdrangewhilewriting
#   phase 3) read-only - readrandom, fwdrange
#

# Exit Codes
EXIT_INVALID_ARGS=1

# Size constants
K=1024
M=$((1024 * K))
G=$((1024 * M))
T=$((1024 * G))

function display_usage() {
  echo "usage: run_blob_bench.sh [--help]"
  echo ""
  echo "Runs the following sequence of BlobDB benchmark tests using tools/benchmark.sh:"
  echo -e "\tPhase 1: write-only tests: bulkload+compact, overwrite+waitforcompaction"
  echo -e "\tPhase 2: read-write tests: readwhilewriting, fwdrangewhilewriting"
  echo -e "\tPhase 3: read-only tests: readrandom, fwdrange"
  echo ""
  echo "Environment Variables:"
  echo -e "\tJOB_ID\t\t\t\tIdentifier for the benchmark job, will appear in the results (default: empty)"
  echo -e "\tDB_DIR\t\t\t\tPath for the RocksDB data directory (mandatory)"
  echo -e "\tWAL_DIR\t\t\t\tPath for the RocksDB WAL directory (mandatory)"
  echo -e "\tOUTPUT_DIR\t\t\tPath for the benchmark results (mandatory)"
  echo -e "\tNUM_THREADS\t\t\tNumber of threads (default: 16)"
  echo -e "\tCOMPRESSION_TYPE\t\tCompression type for the SST files (default: lz4)"
  echo -e "\tDB_SIZE\t\t\t\tRaw (uncompressed) database size (default: 1 TB)"
  echo -e "\tVALUE_SIZE\t\t\tValue size (default: 1 KB)"
  echo -e "\tNUM_KEYS\t\t\tNumber of keys (default: raw database size divided by value size)"
  echo -e "\tDURATION\t\t\tIndividual duration for read-write/read-only tests in seconds (default: 1800)"
  echo -e "\tWRITE_BUFFER_SIZE\t\tWrite buffer (memtable) size (default: 1 GB)"
  echo -e "\tENABLE_BLOB_FILES\t\tEnable blob files (default: 1)"
  echo -e "\tMIN_BLOB_SIZE\t\t\tSize threshold for storing values in blob files (default: 0)"
  echo -e "\tBLOB_FILE_SIZE\t\t\tBlob file size (default: same as write buffer size)"
  echo -e "\tBLOB_COMPRESSION_TYPE\t\tCompression type for the blob files (default: lz4)"
  echo -e "\tENABLE_BLOB_GC\t\t\tEnable blob garbage collection (default: 1)"
  echo -e "\tBLOB_GC_AGE_CUTOFF\t\tBlob garbage collection age cutoff (default: 0.25)"
  echo -e "\tBLOB_GC_FORCE_THRESHOLD\t\tThreshold for forcing garbage collection of the oldest blob files (default: 1.0)"
  echo -e "\tBLOB_COMPACTION_READAHEAD_SIZE\tBlob compaction readahead size (default: 0)"
  echo -e "\tTARGET_FILE_SIZE_BASE\t\tTarget SST file size for compactions (default: write buffer size, scaled down if blob files are enabled)"
  echo -e "\tMAX_BYTES_FOR_LEVEL_BASE\tMaximum size for the base level (default: 8 * target SST file size)"
}

if [ $# -ge 1 ]; then
  display_usage

  if [ "$1" == "--help" ]; then
    exit
  else
    exit $EXIT_INVALID_ARGS
  fi
fi

# shellcheck disable=SC2153
if [ -z "$DB_DIR" ]; then
  echo "DB_DIR is not defined"
  exit $EXIT_INVALID_ARGS
fi

# shellcheck disable=SC2153
if [ -z "$WAL_DIR" ]; then
  echo "WAL_DIR is not defined"
  exit $EXIT_INVALID_ARGS
fi

# shellcheck disable=SC2153
if [ -z "$OUTPUT_DIR" ]; then
  echo "OUTPUT_DIR is not defined"
  exit $EXIT_INVALID_ARGS
fi

# shellcheck disable=SC2153
job_id=$JOB_ID

db_dir=$DB_DIR
wal_dir=$WAL_DIR
output_dir=$OUTPUT_DIR

num_threads=${NUM_THREADS:-16}

compression_type=${COMPRESSION_TYPE:-lz4}

db_size=${DB_SIZE:-$((1 * T))}
value_size=${VALUE_SIZE:-$((1 * K))}
num_keys=${NUM_KEYS:-$((db_size / value_size))}

duration=${DURATION:-1800}

write_buffer_size=${WRITE_BUFFER_SIZE:-$((1 * G))}

enable_blob_files=${ENABLE_BLOB_FILES:-1}
min_blob_size=${MIN_BLOB_SIZE:-0}
blob_file_size=${BLOB_FILE_SIZE:-$write_buffer_size}
blob_compression_type=${BLOB_COMPRESSION_TYPE:-lz4}
enable_blob_garbage_collection=${ENABLE_BLOB_GC:-1}
blob_garbage_collection_age_cutoff=${BLOB_GC_AGE_CUTOFF:-0.25}
blob_garbage_collection_force_threshold=${BLOB_GC_FORCE_THRESHOLD:-1.0}
blob_compaction_readahead_size=${BLOB_COMPACTION_READAHEAD_SIZE:-0}

if [ "$enable_blob_files" == "1" ]; then
  target_file_size_base=${TARGET_FILE_SIZE_BASE:-$((32 * write_buffer_size / value_size))}
else
  target_file_size_base=${TARGET_FILE_SIZE_BASE:-$write_buffer_size}
fi

max_bytes_for_level_base=${MAX_BYTES_FOR_LEVEL_BASE:-$((8 * target_file_size_base))}

echo "======================== Benchmark setup ========================"
echo -e "Job ID:\t\t\t\t\t$job_id"
echo -e "Data directory:\t\t\t\t$db_dir"
echo -e "WAL directory:\t\t\t\t$wal_dir"
echo -e "Output directory:\t\t\t$output_dir"
echo -e "Number of threads:\t\t\t$num_threads"
echo -e "Compression type for SST files:\t\t$compression_type"
echo -e "Raw database size:\t\t\t$db_size"
echo -e "Value size:\t\t\t\t$value_size"
echo -e "Number of keys:\t\t\t\t$num_keys"
echo -e "Duration of read-write/read-only tests:\t$duration"
echo -e "Write buffer size:\t\t\t$write_buffer_size"
echo -e "Blob files enabled:\t\t\t$enable_blob_files"
echo -e "Blob size threshold:\t\t\t$min_blob_size"
echo -e "Blob file size:\t\t\t\t$blob_file_size"
echo -e "Compression type for blob files:\t$blob_compression_type"
echo -e "Blob GC enabled:\t\t\t$enable_blob_garbage_collection"
echo -e "Blob GC age cutoff:\t\t\t$blob_garbage_collection_age_cutoff"
echo -e "Blob GC force threshold:\t\t$blob_garbage_collection_force_threshold"
echo -e "Blob compaction readahead size:\t\t$blob_compaction_readahead_size"
echo -e "Target SST file size:\t\t\t$target_file_size_base"
echo -e "Maximum size of base level:\t\t$max_bytes_for_level_base"
echo "================================================================="

rm -rf "$db_dir"
rm -rf "$wal_dir"
rm -rf "$output_dir"

ENV_VARS="\
  JOB_ID=$job_id \
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
  --blob_garbage_collection_age_cutoff=$blob_garbage_collection_age_cutoff \
  --blob_garbage_collection_force_threshold=$blob_garbage_collection_force_threshold \
  --blob_compaction_readahead_size=$blob_compaction_readahead_size"

# bulk load (using fillrandom) + compact
env -u DURATION -S "$ENV_VARS" ./tools/benchmark.sh bulkload "$PARAMS"

# overwrite + waitforcompaction
env -u DURATION -S "$ENV_VARS" ./tools/benchmark.sh overwrite "$PARAMS_GC"

# readwhilewriting
env -S "$ENV_VARS_D" ./tools/benchmark.sh readwhilewriting "$PARAMS_GC"

# fwdrangewhilewriting
env -S "$ENV_VARS_D" ./tools/benchmark.sh fwdrangewhilewriting "$PARAMS_GC"

# readrandom
env -S "$ENV_VARS_D" ./tools/benchmark.sh readrandom "$PARAMS_GC"

# fwdrange
env -S "$ENV_VARS_D" ./tools/benchmark.sh fwdrange "$PARAMS_GC"

# save logs to output directory
cp "$db_dir"/LOG* "$output_dir/"
