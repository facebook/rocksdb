#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# REQUIRE: db_bench binary exists in the current directory

# Exit Codes
EXIT_INVALID_ARGS=1
EXIT_NOT_COMPACTION_TEST=2
EXIT_UNKNOWN_JOB=3

# Size Constants
K=1024
M=$((1024 * K))
G=$((1024 * M))
T=$((1024 * G))

function display_usage() {
  echo "useage: benchmark.sh [--help] <test>"
  echo ""
  echo "These are the available benchmark tests:"
  echo -e "\tbulkload"
  echo -e "\tfillseq_disable_wal\t\tSequentially fill the database with no WAL"
  echo -e "\tfillseq_enable_wal\t\tSequentially fill the database with WAL"
  echo -e "\toverwrite"
  echo -e "\tupdaterandom"
  echo -e "\treadrandom"
  echo -e "\tmergerandom"
  echo -e "\tfilluniquerandom"
  echo -e "\tmultireadrandom"
  echo -e "\tfwdrange"
  echo -e "\trevrange"
  echo -e "\treadwhilewriting"
  echo -e "\treadwhilemerging"
  echo -e "\tfwdrangewhilewriting"
  echo -e "\trevrangewhilewriting"
  echo -e "\tfwdrangewhilemerging"
  echo -e "\trevrangewhilemerging"
  echo -e "\trandomtransaction"
  echo -e "\tuniversal_compaction"
  echo -e "\tdebug"
  echo ""
  echo "Enviroment Variables:"
  echo -e "\tJOB_ID\t\tAn identifier for the benchmark job, will appear in the results"
  echo -e "\tDB_DIR\t\t\t\tPath to write the database data directory"
  echo -e "\tWAL_DIR\t\t\t\tPath to write the database WAL directory"
  echo -e "\tOUTPUT_DIR\t\t\tPath to write the benchmark results to (default: /tmp)"
  echo -e "\tNUM_KEYS\t\t\tThe number of keys to use in the benchmark"
  echo -e "\tKEY_SIZE\t\t\tThe size of the keys to use in the benchmark (default: 20 bytes)"
  echo -e "\tVALUE_SIZE\t\t\tThe size of the values to use in the benchmark (default: 400 bytes)"
  echo -e "\tBLOCK_SIZE\t\t\tThe size of the database blocks in the benchmark (default: 8 KB)"
  echo -e "\tDB_BENCH_NO_SYNC\t\tDisable fsync on the WAL"
  echo -e "\tNUM_THREADS\t\t\tThe number of threads to use (default: 64)"
  echo -e "\tMB_WRITE_PER_SEC"
  echo -e "\tNUM_NEXTS_PER_SEEK\t\t(default: 10)"
  echo -e "\tCACHE_SIZE\t\t\t(default: 16GB)"
  echo -e "\tCOMPRESSION_MAX_DICT_BYTES"
  echo -e "\tCOMPRESSION_TYPE\t\t(default: zstd)"
  echo -e "\tDURATION"
}

if [ $# -lt 1 ]; then
  display_usage
  exit $EXIT_INVALID_ARGS
fi
bench_cmd=$1
shift
bench_args=$*

if [[ "$bench_cmd" == "--help" ]]; then
  display_usage
  exit
fi

job_id=${JOB_ID}

# Make it easier to run only the compaction test. Getting valid data requires
# a number of iterations and having an ability to run the test separately from
# rest of the benchmarks helps.
if [ "$COMPACTION_TEST" == "1" -a "$bench_cmd" != "universal_compaction" ]; then
  echo "Skipping $1 because it's not a compaction test."
  exit $EXIT_NOT_COMPACTION_TEST
fi

if [ -z $DB_DIR ]; then
  echo "DB_DIR is not defined"
  exit $EXIT_INVALID_ARGS
fi

if [ -z $WAL_DIR ]; then
  echo "WAL_DIR is not defined"
  exit $EXIT_INVALID_ARGS
fi

output_dir=${OUTPUT_DIR:-/tmp}
if [ ! -d $output_dir ]; then
  mkdir -p $output_dir
fi

report="$output_dir/report.tsv"
schedule="$output_dir/schedule.txt"

# all multithreaded tests run with sync=1 unless
# $DB_BENCH_NO_SYNC is defined
syncval="1"
if [ ! -z $DB_BENCH_NO_SYNC ]; then
  echo "Turning sync off for all multithreaded tests"
  syncval="0";
fi

num_threads=${NUM_THREADS:-64}
mb_written_per_sec=${MB_WRITE_PER_SEC:-0}
# Only for tests that do range scans
num_nexts_per_seek=${NUM_NEXTS_PER_SEEK:-10}
cache_size=${CACHE_SIZE:-$((17179869184))}
compression_max_dict_bytes=${COMPRESSION_MAX_DICT_BYTES:-0}
compression_type=${COMPRESSION_TYPE:-zstd}
duration=${DURATION:-0}

num_keys=${NUM_KEYS:-8000000000}
key_size=${KEY_SIZE:-20}
value_size=${VALUE_SIZE:-400}
block_size=${BLOCK_SIZE:-8192}

const_params="
  --db=$DB_DIR \
  --wal_dir=$WAL_DIR \
  \
  --num=$num_keys \
  --num_levels=6 \
  --key_size=$key_size \
  --value_size=$value_size \
  --block_size=$block_size \
  --cache_size=$cache_size \
  --cache_numshardbits=6 \
  --compression_max_dict_bytes=$compression_max_dict_bytes \
  --compression_ratio=0.5 \
  --compression_type=$compression_type \
  --level_compaction_dynamic_level_bytes=true \
  --bytes_per_sync=$((8 * M)) \
  --cache_index_and_filter_blocks=0 \
  --pin_l0_filter_and_index_blocks_in_cache=1 \
  --benchmark_write_rate_limit=$(( 1024 * 1024 * $mb_written_per_sec )) \
  \
  --write_buffer_size=$((128 * M)) \
  --target_file_size_base=$((128 * M)) \
  --max_bytes_for_level_base=$((1 * G)) \
  \
  --verify_checksum=1 \
  --delete_obsolete_files_period_micros=$((60 * M)) \
  --max_bytes_for_level_multiplier=8 \
  \
  --statistics=0 \
  --stats_per_interval=1 \
  --stats_interval_seconds=60 \
  --histogram=1 \
  \
  --memtablerep=skip_list \
  --bloom_bits=10 \
  --open_files=-1 \
  \
  $bench_args"

l0_config="
  --level0_file_num_compaction_trigger=4 \
  --level0_stop_writes_trigger=20"

if [ $duration -gt 0 ]; then
  const_params="$const_params --duration=$duration"
fi

params_w="$l0_config \
          --max_background_compactions=16 \
          --max_write_buffer_number=8 \
          --max_background_flushes=7 \
          $const_params"

params_bulkload="--max_background_compactions=16 \
                 --max_write_buffer_number=8 \
                 --allow_concurrent_memtable_write=false \
                 --max_background_flushes=7 \
                 --level0_file_num_compaction_trigger=$((10 * M)) \
                 --level0_slowdown_writes_trigger=$((10 * M)) \
                 --level0_stop_writes_trigger=$((10 * M)) \
                 $const_params "

params_fillseq="--allow_concurrent_memtable_write=false \
                $params_w "

#
# Tune values for level and universal compaction.
# For universal compaction, these level0_* options mean total sorted of runs in
# LSM. In level-based compaction, it means number of L0 files.
#
params_level_compact="$const_params \
                --max_background_flushes=4 \
                --max_write_buffer_number=4 \
                --level0_file_num_compaction_trigger=4 \
                --level0_slowdown_writes_trigger=16 \
                --level0_stop_writes_trigger=20"

params_univ_compact="$const_params \
                --max_background_flushes=4 \
                --max_write_buffer_number=4 \
                --level0_file_num_compaction_trigger=8 \
                --level0_slowdown_writes_trigger=16 \
                --level0_stop_writes_trigger=20"

function month_to_num() {
    local date_str=$1
    date_str="${date_str/Jan/01}"
    date_str="${date_str/Feb/02}"
    date_str="${date_str/Mar/03}"
    date_str="${date_str/Apr/04}"
    date_str="${date_str/May/05}"
    date_str="${date_str/Jun/06}"
    date_str="${date_str/Jul/07}"
    date_str="${date_str/Aug/08}"
    date_str="${date_str/Sep/09}"
    date_str="${date_str/Oct/10}"
    date_str="${date_str/Nov/11}"
    date_str="${date_str/Dec/12}"
    echo $date_str
}

function summarize_result {
  test_out=$1
  test_name=$2
  bench_name=$3

  # Note that this function assumes that the benchmark executes long enough so
  # that "Compaction Stats" is written to stdout at least once. If it won't
  # happen then empty output from grep when searching for "Sum" will cause
  # syntax errors.
  version=$( grep ^RocksDB: $test_out | awk '{ print $3 }' )
  date=$( grep ^Date: $test_out | awk '{ print $6 "-" $3 "-" $4 "T" $5 ".000" }' )
  iso_date=$( month_to_num $date )
  tz=$( date "+%z" )
  iso_tz="${tz:0:3}:${tz:3:2}"
  iso_date="$iso_date$iso_tz"
  uptime=$( grep ^Uptime\(secs $test_out | tail -1 | awk '{ printf "%.0f", $2 }' )
  stall_time=$( grep "^Cumulative stall" $test_out | tail -1  | awk '{  print $3 }' )
  stall_pct=$( grep "^Cumulative stall" $test_out| tail -1  | awk '{  print $5 }' )
  ops_sec=$( grep ^${bench_name} $test_out | awk '{ print $5 }' )
  mb_sec=$( grep ^${bench_name} $test_out | awk '{ print $7 }' )
  l0_wgb=$( grep "^  L0" $test_out | tail -1 | awk '{ print $9 }' )
  sum_wgb=$( grep "^ Sum" $test_out | tail -1 | awk '{ print $9 }' )
  sum_size=$( grep "^ Sum" $test_out | tail -1 | awk '{ printf "%.1f", $3 / 1024.0 }' )
  wamp=$( grep "^ Sum" $test_out | tail -1 | awk '{ printf "%.1f", $12 }' )
  if [[ "$sum_wgb" == "" ]]; then
      wmb_ps=""
  else
      wmb_ps=$( echo "scale=1; ( $sum_wgb * 1024.0 ) / $uptime" | bc )
  fi
  usecs_op=$( grep ^${bench_name} $test_out | awk '{ printf "%.1f", $3 }' )
  p50=$( grep "^Percentiles:" $test_out | tail -1 | awk '{ printf "%.1f", $3 }' )
  p75=$( grep "^Percentiles:" $test_out | tail -1 | awk '{ printf "%.1f", $5 }' )
  p99=$( grep "^Percentiles:" $test_out | tail -1 | awk '{ printf "%.0f", $7 }' )
  p999=$( grep "^Percentiles:" $test_out | tail -1 | awk '{ printf "%.0f", $9 }' )
  p9999=$( grep "^Percentiles:" $test_out | tail -1 | awk '{ printf "%.0f", $11 }' )

  # if the report TSV (Tab Separate Values) file does not yet exist, create it and write the header row to it
  if [ ! -f "$report" ]; then
    echo -e "ops_sec\tmb_sec\ttotal_size_gb\tlevel0_size_gb\tsum_gb\twrite_amplification\twrite_mbps\tusec_op\tpercentile_50\tpercentile_75\tpercentile_99\tpercentile_99.9\tpercentile_99.99\tuptime\tstall_time\tstall_percent\ttest_name\ttest_date\trocksdb_version\tjob_id" \
      >> $report
  fi

  echo -e "$ops_sec\t$mb_sec\t$sum_size\t$l0_wgb\t$sum_wgb\t$wamp\t$wmb_ps\t$usecs_op\t$p50\t$p75\t$p99\t$p999\t$p9999\t$uptime\t$stall_time\t$stall_pct\t$test_name\t$iso_date\t$version\t$job_id" \
    >> $report
}

function run_bulkload {
  # This runs with a vector memtable and the WAL disabled to load faster. It is still crash safe and the
  # client can discover where to restart a load after a crash. I think this is a good way to load.
  echo "Bulk loading $num_keys random keys"
  log_file_name=$output_dir/benchmark_bulkload_fillrandom.log
  cmd="./db_bench --benchmarks=fillrandom \
       --use_existing_db=0 \
       --disable_auto_compactions=1 \
       --sync=0 \
       $params_bulkload \
       --threads=1 \
       --memtablerep=vector \
       --allow_concurrent_memtable_write=false \
       --disable_wal=1 \
       --seed=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd
  summarize_result $log_file_name bulkload fillrandom

  echo "Compacting..."
  log_file_name=$output_dir/benchmark_bulkload_compact.log
  cmd="./db_bench --benchmarks=compact \
       --use_existing_db=1 \
       --disable_auto_compactions=1 \
       --sync=0 \
       $params_w \
       --threads=1 \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd
}

#
# Parameter description:
#
# $1 - 1 if I/O statistics should be collected.
# $2 - compaction type to use (level=0, universal=1).
# $3 - number of subcompactions.
# $4 - number of maximum background compactions.
#
function run_manual_compaction_worker {
  # This runs with a vector memtable and the WAL disabled to load faster.
  # It is still crash safe and the client can discover where to restart a
  # load after a crash. I think this is a good way to load.
  echo "Bulk loading $num_keys random keys for manual compaction."

  log_file_name=$output_dir/benchmark_man_compact_fillrandom_$3.log

  if [ "$2" == "1" ]; then
    extra_params=$params_univ_compact
  else
    extra_params=$params_level_compact
  fi

  # Make sure that fillrandom uses the same compaction options as compact.
  cmd="./db_bench --benchmarks=fillrandom \
       --use_existing_db=0 \
       --disable_auto_compactions=0 \
       --sync=0 \
       $extra_params \
       --threads=$num_threads \
       --compaction_measure_io_stats=$1 \
       --compaction_style=$2 \
       --subcompactions=$3 \
       --memtablerep=vector \
       --allow_concurrent_memtable_write=false \
       --disable_wal=1 \
       --max_background_compactions=$4 \
       --seed=$( date +%s ) \
       2>&1 | tee -a $log_file_name"

  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd

  summarize_result $log_file_namefillrandom_output_file man_compact_fillrandom_$3 fillrandom

  echo "Compacting with $3 subcompactions specified ..."

  log_file_name=$output_dir/benchmark_man_compact_$3.log

  # This is the part we're really interested in. Given that compact benchmark
  # doesn't output regular statistics then we'll just use the time command to
  # measure how long this step takes.
  cmd="{ \
       time ./db_bench --benchmarks=compact \
       --use_existing_db=1 \
       --disable_auto_compactions=0 \
       --sync=0 \
       $extra_params \
       --threads=$num_threads \
       --compaction_measure_io_stats=$1 \
       --compaction_style=$2 \
       --subcompactions=$3 \
       --max_background_compactions=$4 \
       ;}
       2>&1 | tee -a $log_file_name"

  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd

  # Can't use summarize_result here. One way to analyze the results is to run
  # "grep real" on the resulting log files.
}

function run_univ_compaction {
  # Always ask for I/O statistics to be measured.
  io_stats=1

  # Values: kCompactionStyleLevel = 0x0, kCompactionStyleUniversal = 0x1.
  compaction_style=1

  # Define a set of benchmarks.
  subcompactions=(1 2 4 8 16)
  max_background_compactions=(16 16 8 4 2)

  i=0
  total=${#subcompactions[@]}

  # Execute a set of benchmarks to cover variety of scenarios.
  while [ "$i" -lt "$total" ]
  do
    run_manual_compaction_worker $io_stats $compaction_style ${subcompactions[$i]} \
      ${max_background_compactions[$i]}
    ((i++))
  done
}

function run_fillseq {
  # This runs with a vector memtable. WAL can be either disabled or enabled
  # depending on the input parameter (1 for disabled, 0 for enabled). The main
  # benefit behind disabling WAL is to make loading faster. It is still crash
  # safe and the client can discover where to restart a load after a crash. I
  # think this is a good way to load.

  # Make sure that we'll have unique names for all the files so that data won't
  # be overwritten.
  if [ $1 == 1 ]; then
    log_file_name="${output_dir}/benchmark_fillseq.wal_disabled.v${value_size}.log"
    test_name=fillseq.wal_disabled.v${value_size}
  else
    log_file_name="${output_dir}/benchmark_fillseq.wal_enabled.v${value_size}.log"
    test_name=fillseq.wal_enabled.v${value_size}
  fi

  echo "Loading $num_keys keys sequentially"
  cmd="./db_bench --benchmarks=fillseq \
       --use_existing_db=0 \
       --sync=0 \
       $params_fillseq \
       --min_level_to_compress=0 \
       --threads=1 \
       --memtablerep=vector \
       --allow_concurrent_memtable_write=false \
       --disable_wal=$1 \
       --seed=$( date +%s ) \
       2>&1 | tee -a $log_file_name"

  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd

  # The constant "fillseq" which we pass to db_bench is the benchmark name.
  summarize_result $log_file_name $test_name fillseq
}

function run_change {
  operation=$1
  echo "Do $num_keys random $operation"
  log_file_name="$output_dir/benchmark_${operation}.t${num_threads}.s${syncval}.log"
  cmd="./db_bench --benchmarks=$operation \
       --use_existing_db=1 \
       --sync=$syncval \
       $params_w \
       --threads=$num_threads \
       --merge_operator=\"put\" \
       --seed=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd
  summarize_result $log_file_name ${operation}.t${num_threads}.s${syncval} $operation
}

function run_filluniquerandom {
  echo "Loading $num_keys unique keys randomly"
  log_file_name=$output_dir/benchmark_filluniquerandom.log
  cmd="./db_bench --benchmarks=filluniquerandom \
       --use_existing_db=0 \
       --sync=0 \
       $params_w \
       --threads=1 \
       --seed=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd
  summarize_result $log_file_name filluniquerandom filluniquerandom
}

function run_readrandom {
  echo "Reading $num_keys random keys"
  log_file_name="${output_dir}/benchmark_readrandom.t${num_threads}.log"
  cmd="./db_bench --benchmarks=readrandom \
       --use_existing_db=1 \
       $params_w \
       --threads=$num_threads \
       --seed=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd
  summarize_result $log_file_name readrandom.t${num_threads} readrandom
}

function run_multireadrandom {
  echo "Multi-Reading $num_keys random keys"
  log_file_name="${output_dir}/benchmark_multireadrandom.t${num_threads}.log"
  cmd="./db_bench --benchmarks=multireadrandom \
       --use_existing_db=1 \
       --threads=$num_threads \
       --batch_size=10 \
       $params_w \
       --seed=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd
  summarize_result $log_file_name multireadrandom.t${num_threads} multireadrandom
}

function run_readwhile {
  operation=$1
  echo "Reading $num_keys random keys while $operation"
  log_file_name="${output_dir}/benchmark_readwhile${operation}.t${num_threads}.log"
  cmd="./db_bench --benchmarks=readwhile${operation} \
       --use_existing_db=1 \
       --sync=$syncval \
       $params_w \
       --threads=$num_threads \
       --merge_operator=\"put\" \
       --seed=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd
  summarize_result $log_file_name readwhile${operation}.t${num_threads} readwhile${operation}
}

function run_rangewhile {
  operation=$1
  full_name=$2
  reverse_arg=$3
  log_file_name="${output_dir}/benchmark_${full_name}.t${num_threads}.log"
  echo "Range scan $num_keys random keys while ${operation} for reverse_iter=${reverse_arg}"
  cmd="./db_bench --benchmarks=seekrandomwhile${operation} \
       --use_existing_db=1 \
       --sync=$syncval \
       $params_w \
       --threads=$num_threads \
       --merge_operator=\"put\" \
       --seek_nexts=$num_nexts_per_seek \
       --reverse_iterator=$reverse_arg \
       --seed=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  echo $cmd | tee $log_file_name
  eval $cmd
  summarize_result $log_file_name ${full_name}.t${num_threads} seekrandomwhile${operation}
}

function run_range {
  full_name=$1
  reverse_arg=$2
  log_file_name="${output_dir}/benchmark_${full_name}.t${num_threads}.log"
  echo "Range scan $num_keys random keys for reverse_iter=${reverse_arg}"
  cmd="./db_bench --benchmarks=seekrandom \
       --use_existing_db=1 \
       $params_w \
       --threads=$num_threads \
       --seek_nexts=$num_nexts_per_seek \
       --reverse_iterator=$reverse_arg \
       --seed=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd
  summarize_result $log_file_name ${full_name}.t${num_threads} seekrandom
}

function run_randomtransaction {
  echo "..."
  log_file_name=$output_dir/benchmark_randomtransaction.log
  cmd="./db_bench $params_r --benchmarks=randomtransaction \
       --num=$num_keys \
       --transaction_db \
       --threads=5 \
       --transaction_sets=5 \
       2>&1 | tee $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  eval $cmd
}

function now() {
  echo `date +"%s"`
}


echo "===== Benchmark ====="

# Run!!!
IFS=',' read -a jobs <<< $bench_cmd
# shellcheck disable=SC2068
for job in ${jobs[@]}; do

  if [ $job != debug ]; then
    echo "Starting $job (ID: $job_id) at `date`" | tee -a $schedule
  fi

  start=$(now)
  if [ $job = bulkload ]; then
    run_bulkload
  elif [ $job = fillseq_disable_wal ]; then
    run_fillseq 1
  elif [ $job = fillseq_enable_wal ]; then
    run_fillseq 0
  elif [ $job = overwrite ]; then
    syncval="0"
    params_w="$params_w \
        --writes=125000000 \
        --subcompactions=4 \
        --soft_pending_compaction_bytes_limit=$((1 * T)) \
        --hard_pending_compaction_bytes_limit=$((4 * T)) "
    run_change overwrite
  elif [ $job = updaterandom ]; then
    run_change updaterandom
  elif [ $job = mergerandom ]; then
    run_change mergerandom
  elif [ $job = filluniquerandom ]; then
    run_filluniquerandom
  elif [ $job = readrandom ]; then
    run_readrandom
  elif [ $job = multireadrandom ]; then
    run_multireadrandom
  elif [ $job = fwdrange ]; then
    run_range $job false
  elif [ $job = revrange ]; then
    run_range $job true
  elif [ $job = readwhilewriting ]; then
    run_readwhile writing
  elif [ $job = readwhilemerging ]; then
    run_readwhile merging
  elif [ $job = fwdrangewhilewriting ]; then
    run_rangewhile writing $job false
  elif [ $job = revrangewhilewriting ]; then
    run_rangewhile writing $job true
  elif [ $job = fwdrangewhilemerging ]; then
    run_rangewhile merging $job false
  elif [ $job = revrangewhilemerging ]; then
    run_rangewhile merging $job true
  elif [ $job = randomtransaction ]; then
    run_randomtransaction
  elif [ $job = universal_compaction ]; then
    run_univ_compaction
  elif [ $job = debug ]; then
    num_keys=1000; # debug
    echo "Setting num_keys to $num_keys"
  else
    echo "unknown job $job"
    exit $EXIT_UNKNOWN_JOB
  fi
  end=$(now)

  if [ $job != debug ]; then
    echo "Completed $job (ID: $job_id) in $((end-start)) seconds" | tee -a $schedule
  fi

  echo -e "ops/sec\tmb/sec\tSize-GB\tL0_GB\tSum_GB\tW-Amp\tW-MB/s\tusec/op\tp50\tp75\tp99\tp99.9\tp99.99\tUptime\tStall-time\tStall%\tTest\tDate\tVersion\tJob-ID"
  tail -1 $report

done
