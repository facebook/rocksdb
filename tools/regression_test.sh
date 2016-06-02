#!/bin/bash
# REQUIRE: db_bench binary exists in the current directory

function main {
  commit=${1:-"origin/master"}
  test_root_dir=${TEST_PATH:-"/tmp/rocksdb/regression_test"}

  init_arguments $test_root_dir

  checkout_rocksdb $commit
  build_db_bench

  setup_test_directory

  # an additional dot indicates we share same env variables
  run_db_bench "fillseq" 0
  run_db_bench "overwrite"
  run_db_bench "readrandom"
  run_db_bench "readwhilewriting"
  run_db_bench "deleterandom"
  run_db_bench "seekrandom"
  run_db_bench "seekrandomwhilewriting"

  echo ""
  echo "Benchmark completed!  Results are available in $RESULT_PATH"
}

############################################################################
function init_arguments {
  K=1024
  M=$((1024 * K))
  G=$((1024 * M))

  current_time=$(date +"%F-%H:%M:%S")
  RESULT_PATH=${2:-"$1/results/$current_time"}
  COMMIT_ID=`git log | head -n1 | cut -c 8-`
  SUMMARY_FILE="$RESULT_PATH/SUMMARY.csv"

  DB_PATH=${3:-"$1/db/"}
  WAL_PATH=${4:-"$1/wal/"}
  if [ -z "$REMOTE_HOST_USER" ]; then
    DB_BENCH_DIR=${5:-"."}
  else
    DB_BENCH_DIR=${5:-"$1/db_bench"}
  fi

  NUM_THREADS=${NUM_THREADS:-16}
  NUM_KEYS=${NUM_KEYS:-$((1 * G))}
  KEY_SIZE=${KEY_SIZE:-100}
  VALUE_SIZE=${VALUE_SIZE:-900}
  CACHE_SIZE=${CACHE_SIZE:-$((1 * G))}
  STATISTICS=${STATISTICS:-0}
  COMPRESSION_RATIO=${COMPRESSION_RATIO:-0.5}
  HISTOGRAM=${HISTOGRAM:-1}
  STATS_PER_INTERVAL=${STATS_PER_INTERVAL:-1}
  STATS_INTERVAL_SECONDS=${STATS_INTERVAL_SECONDS:-60}
  MAX_BACKGROUND_FLUSHES=${MAX_BACKGROUND_FLUSHES:-4}
  MAX_BACKGROUND_COMPACTIONS=${MAX_BACKGROUND_COMPACTIONS:-16} 
  SEEK_NEXTS=${SEEK_NEXTS:-10}
  SEED=${SEED:-$( date +%s )}
}

# $1 --- benchmark name
# $2 --- use_existing_db (optional)
function run_db_bench {
  # this will terminate all currently-running db_bench
  find_db_bench_cmd="ps aux | grep db_bench | grep -v grep | grep -v aux | awk '{print \$2}'"

  USE_EXISTING_DB=${2:-1}
  echo ""
  echo "======================================================================="
  echo "Benchmark $1"
  echo "======================================================================="
  echo ""
  db_bench_error=0
  db_bench_cmd="$DB_BENCH_DIR/db_bench \
      --benchmarks=$1 --db=$DB_PATH --wal_dir=$WAL_PATH \
      --use_existing_db=$USE_EXISTING_DB \
      --threads=$NUM_THREADS \
      --num=$NUM_KEYS \
      --key_size=$KEY_SIZE \
      --value_size=$VALUE_SIZE \
      --cache_size=$CACHE_SIZE \
      --statistics=$STATISTICS \
      --compression_ratio=$COMPRESSION_RATIO \
      --histogram=$HISTOGRAM \
      --seek_nexts=$SEEK_NEXTS \
      --stats_per_interval=$STATS_PER_INTERVAL \
      --stats_interval_seconds=$STATS_INTERVAL_SECONDS \
      --max_background_flushes=$MAX_BACKGROUND_FLUSHES \
      --max_background_compactions=$MAX_BACKGROUND_COMPACTIONS \
      --seed=$SEED 2>&1"
  kill_db_bench_cmd="pkill db_bench"
  ps_cmd="ps aux"
  if ! [ -z "$REMOTE_HOST_USER" ]; then
    kill_db_bench_cmd="$SSH $REMOTE_HOST_USER $kill_db_bench_cmd"
    db_bench_cmd="$SSH $REMOTE_HOST_USER $db_bench_cmd"
    ps_cmd="$SSH $REMOTE_HOST_USER $ps_cmd"
  fi

  ## kill existing db_bench processes
  eval "$kill_db_bench_cmd"
  if [ $? -eq 0 ]; then
    echo "Killed all currently running db_bench"
  fi

  ## make sure no db_bench is running
  # The following statement is necessary make sure "eval $ps_cmd" will success.
  # Otherwise, if we simply check whether "$(eval $ps_cmd | grep db_bench)" is
  # successful or not, then it will always be false since grep will return
  # non-zero status when there's no matching output.
  ps_output="$(eval $ps_cmd)"
  exit_on_error $? "$ps_cmd"

  # perform the actual command to check whether db_bench is running
  grep_output="$(eval $ps_cmd | grep db_bench)"
  if [ "$grep_output" != "" ]; then
    echo "Stopped regression_test.sh as there're still db_bench processes running:"
    echo $grep_output
    exit 1
  fi

  ## run the db_bench
  cmd="($db_bench_cmd || db_bench_error=1) | tee -a $RESULT_PATH/$1"
  exit_on_error $?
  echo $cmd
  eval $cmd
  exit_on_error $db_bench_error

  update_report "$1" "$RESULT_PATH/$1"
}

# $1 --- name of the benchmark
# $2 --- the filename of the output log of db_bench
function update_report {
  main_result=`cat $2 | grep $1`
  exit_on_error $?
  perc_statement=`cat $2 | grep Percentile`
  exit_on_error $?

  # Obtain micros / op
  main_pattern="$1"'[[:blank:]]+:[[:blank:]]+([0-9\.]+)[[:blank:]]+micros/op'
  [[ $main_result =~ $main_pattern ]]
  micros_op=${BASH_REMATCH[1]}
  
  # Obtain percentile information
  perc_pattern='Percentiles: P50: ([0-9\.]+) P75: ([0-9\.]+) P99: ([0-9\.]+) P99.9: ([0-9\.]+) P99.99: ([0-9\.]+)'
  [[ $perc_statement =~ $perc_pattern ]]

  perc[0]=${BASH_REMATCH[1]}  # p50
  perc[1]=${BASH_REMATCH[2]}  # p75
  perc[2]=${BASH_REMATCH[3]}  # p99
  perc[3]=${BASH_REMATCH[4]}  # p99.9
  perc[4]=${BASH_REMATCH[5]}  # p99.99

  printf "$COMMIT_ID, %30s, %10.2f, %10.2f, %10.2f, %10.2f, %10.2f, %10.2f\n" \
      $1 $micros_op ${perc[0]} ${perc[1]} ${perc[2]} ${perc[3]} ${perc[4]} \
      >> $SUMMARY_FILE 
  exit_on_error $?
}

function exit_on_error {
  if [ $1 -ne 0 ]; then
    echo ""
    echo "ERROR: Benchmark did not complete successfully."
    if ! [ -z "$2" ]; then
      echo "Failure command: $2"
    fi
    echo "Partial results are output to $RESULT_PATH"
    echo "ERROR" >> $SUMMARY_FILE
    exit $1
  fi
}

function checkout_rocksdb {
  echo "Checking out commit $1 ..."

  git fetch --all
  exit_on_error $?

  git checkout $1
  exit_on_error $?
}

function build_db_bench {
  echo "Building db_bench ..."

  make clean
  exit_on_error $?

  DEBUG_LEVEL=0 make db_bench -j32
  exit_on_error $?
}

function run_remote {
  if ! [ -z "$REMOTE_HOST_USER" ]; then
    cmd="$SSH $REMOTE_HOST_USER $1"
  else
    cmd="$1"
  fi
  
  result=0
  eval "($cmd) || result=1"
  exit_on_error $result "$cmd"
}

function run_local {
  result=0
  eval "($1 || result=1)"
  exit_on_error $result
}

function setup_test_directory {
  echo "Deleting old regression test directories and creating new ones"

  run_remote "rm -rf $DB_PATH"
  run_remote "rm -rf $WAL_PATH"
  if ! [ -z "$REMOTE_HOST_USER" ]; then
    run_remote "rm -rf $DB_BENCH_DIR"
  fi
  run_remote "mkdir -p $DB_PATH"
  run_remote "mkdir -p $WAL_PATH"
  if ! [ -z "$REMOTE_HOST_USER" ]; then
    run_remote "mkdir -p $DB_BENCH_DIR"
    run_local "$SCP ./db_bench $REMOTE_HOST_USER:$DB_BENCH_DIR/db_bench"
  fi
  
  run_local "rm -rf $RESULT_PATH"
  run_local "mkdir -p $RESULT_PATH"

  printf "%40s, %30s, %10s, %10s, %10s, %10s, %10s, %10s\n" \
      "commit id" "benchmark" "ms-per-op" "p50" "p75" "p99" "p99.9" "p99.99" \
       $micros_op ${perc[0]} ${perc[1]} ${perc[2]} ${perc[3]} ${perc[4]} \
      >> $SUMMARY_FILE 
}

############################################################################

main $@
