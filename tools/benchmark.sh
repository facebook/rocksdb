#!/bin/bash
# REQUIRE: db_bench binary exists in the current directory

if [ $# -ne 1 ]; then
  echo "./benchmark.sh [bulkload/fillseq/overwrite/filluniquerandom/readrandom/readwhilewriting]"
  exit 0
fi

# size constants
K=1024
M=$((1024 * K))
G=$((1024 * M))

if [ -z $DB_DIR ]; then
  echo "DB_DIR is not defined"
  exit 0
fi

if [ -z $WAL_DIR ]; then
  echo "WAL_DIR is not defined"
  exit 0
fi

output_dir=${OUTPUT_DIR:-/tmp/}
if [ ! -d $output_dir ]; then
  mkdir -p $output_dir
fi

num_read_threads=${NUM_READ_THREADS:-16}
writes_per_second=${WRITES_PER_SEC:-$((80 * K))}  # (only for readwhilewriting)
num_nexts_per_seek=${NUM_NEXTS_PER_SEEK:-10}      # (only for rangescanwhilewriting)
cache_size=$((1 * G))
duration=${DURATION:-0}

num_keys=${NUM_KEYS:-$((1 * G))}
key_size=20
value_size=800

const_params="
  --db=$DB_DIR \
  --wal_dir=$WAL_DIR \
  \
  --num_levels=6 \
  --key_size=$key_size \
  --value_size=$value_size \
  --block_size=4096 \
  --cache_size=$cache_size \
  --cache_numshardbits=6 \
  --compression_type=zlib \
  --min_level_to_compress=2 \
  --compression_ratio=0.5 \
  \
  --hard_rate_limit=2 \
  --rate_limit_delay_max_milliseconds=1000000 \
  --write_buffer_size=$((128 * M)) \
  --max_write_buffer_number=3 \
  --target_file_size_base=$((128 * M)) \
  --max_bytes_for_level_base=$((1 * G)) \
  \
  --verify_checksum=1 \
  --delete_obsolete_files_period_micros=$((60 * M)) \
  --max_grandparent_overlap_factor=10 \
  \
  --statistics=1 \
  --stats_per_interval=1 \
  --stats_interval=$((1 * M)) \
  --histogram=1 \
  \
  --memtablerep=skip_list \
  --bloom_bits=10 \
  --open_files=$((20 * K))"

l0_config="
  --level0_file_num_compaction_trigger=4 \
  --level0_slowdown_writes_trigger=8 \
  --level0_stop_writes_trigger=12"

if [ $duration -gt 0 ]; then
  const_params="$const_params --duration=$duration"
fi

params_r="$const_params $l0_config --max_background_compactions=4 --max_background_flushes=1"
params_w="$const_params $l0_config --max_background_compactions=16 --max_background_flushes=16"
params_bulkload="$const_params --max_background_compactions=16 --max_background_flushes=16 \
                 --level0_file_num_compaction_trigger=$((10 * M)) \
                 --level0_slowdown_writes_trigger=$((10 * M)) \
                 --level0_stop_writes_trigger=$((10 * M))"

function run_bulkload {
  echo "Bulk loading $num_keys random keys into database..."
  cmd="./db_bench $params_bulkload --benchmarks=fillrandom \
       --use_existing_db=0 \
       --num=$num_keys \
       --disable_auto_compactions=1 \
       --sync=0 \
       --disable_data_sync=0 \
       --threads=1 2>&1 | tee $output_dir/benchmark_bulkload_fillrandom.log"
  echo $cmd | tee $output_dir/benchmark_bulkload_fillrandom.log
  eval $cmd
  echo "Compacting..."
  cmd="./db_bench $params_w --benchmarks=compact \
       --use_existing_db=1 \
       --num=$num_keys \
       --disable_auto_compactions=1 \
       --sync=0 \
       --disable_data_sync=0 \
       --threads=1 2>&1 | tee $output_dir/benchmark_bulkload_compact.log"
  echo $cmd | tee $output_dir/benchmark_bulkload_compact.log
  eval $cmd
}

function run_fillseq {
  echo "Loading $num_keys keys sequentially into database..."
  cmd="./db_bench $params_w --benchmarks=fillseq \
       --use_existing_db=0 \
       --num=$num_keys \
       --sync=1 \
       --disable_data_sync=0 \
       --threads=1 2>&1 | tee $output_dir/benchmark_fillseq.log"
  echo $cmd | tee $output_dir/benchmark_fillseq.log
  eval $cmd
}

function run_overwrite {
  echo "Loading $num_keys keys sequentially into database..."
  cmd="./db_bench $params_w --benchmarks=overwrite \
       --use_existing_db=1 \
       --num=$num_keys \
       --sync=1 \
       --disable_data_sync=0 \
       --threads=1 2>&1 | tee $output_dir/benchmark_overwrite.log"
  echo $cmd | tee $output_dir/benchmark_overwrite.log
  eval $cmd
}

function run_filluniquerandom {
  echo "Loading $num_keys unique keys randomly into database..."
  cmd="./db_bench $params_w --benchmarks=filluniquerandom \
       --use_existing_db=0 \
       --num=$num_keys \
       --sync=1 \
       --disable_data_sync=0 \
       --threads=1 2>&1 | tee $output_dir/benchmark_filluniquerandom.log"
  echo $cmd | tee $output_dir/benchmark_filluniquerandom.log
  eval $cmd
}

function run_readrandom {
  echo "Reading $num_keys random keys from database..."
  cmd="./db_bench $params_r --benchmarks=readrandom \
       --use_existing_db=1 \
       --num=$num_keys \
       --threads=$num_read_threads \
       --disable_auto_compactions=1 \
       2>&1 | tee $output_dir/benchmark_readrandom.log"
  echo $cmd | tee $output_dir/benchmark_readrandom.log
  eval $cmd
}

function run_readwhilewriting {
  echo "Reading $num_keys random keys from database whiling writing.."
  cmd="./db_bench $params_r --benchmarks=readwhilewriting \
       --use_existing_db=1 \
       --num=$num_keys \
       --sync=1 \
       --disable_data_sync=0 \
       --threads=$num_read_threads \
       --writes_per_second=$writes_per_second \
       2>&1 | tee $output_dir/benchmark_readwhilewriting.log"
  echo $cmd | tee $output_dir/benchmark_readwhilewriting.log
  eval $cmd
}

function run_rangescanwhilewriting {
  echo "Range scan $num_keys random keys from database whiling writing.."
  cmd="./db_bench $params_r --benchmarks=seekrandomwhilewriting \
       --use_existing_db=1 \
       --num=$num_keys \
       --sync=1 \
       --disable_data_sync=0 \
       --threads=$num_read_threads \
       --writes_per_second=$writes_per_second \
       --seek_nexts=$num_nexts_per_seek \
       2>&1 | tee $output_dir/benchmark_rangescanwhilewriting.log"
  echo $cmd | tee $output_dir/benchmark_rangescanwhilewriting.log
  eval $cmd
}

function now() {
  echo `date +"%s"`
}

report="$output_dir/report.txt"

echo "===== Benchmark ====="

# Run!!!
IFS=',' read -a jobs <<< $1
for job in ${jobs[@]}; do

  if [ $job != debug ]; then
    echo "Start $job at `date`" | tee -a $report
  fi

  start=$(now)
  if [ $job = bulkload ]; then
    run_bulkload
  elif [ $job = fillseq ]; then
    run_fillseq
  elif [ $job = overwrite ]; then
    run_overwrite
  elif [ $job = filluniquerandom ]; then
    run_filluniquerandom
  elif [ $job = readrandom ]; then
    run_readrandom
  elif [ $job = readwhilewriting ]; then
    run_readwhilewriting
  elif [ $job = rangescanwhilewriting ]; then
    run_rangescanwhilewriting
  elif [ $job = debug ]; then
    num_keys=10000; # debug
    echo "Setting num_keys to $num_keys"
  else
    echo "unknown job $job"
    exit
  fi
  end=$(now)

  if [ $job != debug ]; then
    echo "Complete $job in $((end-start)) seconds" | tee -a $report
  fi

  if [[ $job = readrandom || $job = readwhilewriting || $job == rangescanwhilewriting ]]; then
    lat=$(grep "micros\/op" "$output_dir/benchmark_$job.log" | grep "ops\/sec" | awk '{print $3}')
    qps=$(grep "micros\/op" "$output_dir/benchmark_$job.log" | grep "ops\/sec" | awk '{print $5}')
    line=$(grep "rocksdb.db.get.micros" "$output_dir/benchmark_$job.log")
    p50=$(echo $line | awk '{print $7}')
    p99=$(echo $line | awk '{print $13}')
    print_percentile=$(echo "$p50 != 0 || $p99 != 0" | bc);
    if [ $print_percentile == "1" ]; then
      echo "Read latency p50 = $p50 us, p99 = $p99 us" | tee -a $report
    fi
    echo "QPS = $qps ops/sec" | tee -a $report
    echo "Avg Latency = $lat micros/op " | tee -a $report
  fi
done
