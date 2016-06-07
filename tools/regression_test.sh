#!/bin/bash
# The RocksDB regression test script.
# REQUIREMENT: must be able to run make db_bench in the current directory
#
# This script will do the following things in order:
#
# 1. check out the specified rocksdb commit.
# 2. build db_bench using the specified commit
# 3. setup test directory $TEST_PATH.  If not specified, then the test directory
#    will be "/tmp/rocksdb/regression_test"
# 4. run set of benchmarks on the specified host
#    (can be either locally or remotely)
# 5. generate report in the $RESULT_PATH.  If RESULT_PATH is not specified,
#    RESULT_PATH will be set to $TEST_PATH/current_time
#
# = Examples =
# * Run the regression test using rocksdb commit abcdef that outputs results
#   and temp files in "/my/output/dir" 
# 
#   TEST_PATH=/my/output/dir COMMIT_ID=abcdef ./tools/regression_test.sh
#
# * Run the regression test on a remost host under "/my/output/dir" directory
#   and stores the result locally in "/my/benchmark/results" using commit
#   abcdef and with the rocksdb options specified in /my/path/to/OPTIONS-012345
#   with 1000000000 keys in each benchmark in the regression test where each
#   key and value are 100 and 900 bytes respectively:
#
#   REMOTE_USER_AT_HOST=yhchiang@my.remote.host \
#       TEST_PATH=/my/output/dir \
#       RESULT_PATH=/my/benchmark/results \
#       COMMIT_ID=abcdef \
#       OPTIONS_FILE=/my/path/to/OPTIONS-012345 \
#       NUM_KEYS=1000000000 \
#       KEY_SIZE=100 \
#       VALUE_SIZE=900 \
#       ./tools/regression_test.sh
#
# = Regression test environmental parameters =
#   TEST_PATH: the root directory of the regression test.
#       Default: "/tmp/rocksdb/regression_test"
#   RESULT_PATH: the directory where the regression results will be generated.
#       Default: "$TEST_PATH/current_time"
#   REMOTE_USER_AT_HOST: If set, then test will run on the specified host under
#       TEST_PATH directory and outputs test results locally in RESULT_PATH
#       The REMOTE_USER_AT_HOST should follow the format user-id@host.name
#   DB_PATH: the path where the rocksdb database will be created during the
#       regression test.  Default:  $TEST_PATH/db
#   WAL_PATH: the path where the rocksdb WAL will be outputed.
#       Default:  $TEST_PATH/wal
#   OPTIONS_FILE:  If specified, then the regression test will use the specified
#       file to initialize the RocksDB options in its benchmarks.  Note that
#       this feature only work for commits after 88acd93 or rocksdb version
#       later than 4.9.
#
# = db_bench parameters =
#   NUM_THREADS:  The number of concurrent foreground threads that will issue
#       database operations in the benchmark.  Default: 16.
#   NUM_KEYS:  The number of keys issued by each thread in the benchmark.
#       Default: 1G.
#   KEY_SIZE:  The size of each key in bytes in db_bench.  Default: 100.
#   VALUE_SIZE:  The size of each value in bytes in db_bench.  Default: 900.
#   CACHE_SIZE:  The size of RocksDB block cache used in db_bench.  Default: 1G
#   STATISTICS:  If 1, then statistics is on in db_bench.  Default: 0.
#   COMPRESSION_RATIO:  The compression ratio of the key generated in db_bench.
#       Default: 0.5.
#   HISTOGRAM:  If 1, then the histogram feature on performance feature is on.
#   STATS_PER_INTERVAL:  If 1, then the statistics will be reported for every
#       STATS_INTERVAL_SECONDS seconds.  Default 1.
#   STATS_INTERVAL_SECONDS:  If STATS_PER_INTERVAL is set to 1, then statistics
#       will be reported for every STATS_INTERVAL_SECONDS.  Default 60.
#   MAX_BACKGROUND_FLUSHES:  The maxinum number of concurrent flushes in
#       db_bench.  Default: 4.
#   MAX_BACKGROUND_COMPACTIONS:  The maximum number of concurrent compactions
#       in db_bench.  Default: 16.
#   SEEK_NEXTS:  Controls how many Next() will be called after seek.
#       Default: 10.
#   SEED:  random seed that controls the randomness of the benchmark.
#       Default: $( date +%s )

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
  RESULT_PATH=${RESULT_PATH:-"$1/results/$current_time"}
  COMMIT_ID=`git log | head -n1 | cut -c 8-`
  SUMMARY_FILE="$RESULT_PATH/SUMMARY.csv"

  DB_PATH=${3:-"$1/db/"}
  WAL_PATH=${4:-"$1/wal/"}
  if [ -z "$REMOTE_USER_AT_HOST" ]; then
    DB_BENCH_DIR=${5:-"."}
  else
    DB_BENCH_DIR=${5:-"$1/db_bench"}
  fi

  SCP=${SCP:-"scp"}
  SSH=${SSH:-"ssh"}
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
  options_file_arg=$(setup_options_file)
  db_bench_cmd="$DB_BENCH_DIR/db_bench \
      --benchmarks=$1 --db=$DB_PATH --wal_dir=$WAL_PATH \
      --use_existing_db=$USE_EXISTING_DB \
      --threads=$NUM_THREADS \
      --num=$NUM_KEYS \
      --key_size=$KEY_SIZE \
      --value_size=$VALUE_SIZE \
      --cache_size=$CACHE_SIZE \
      --statistics=$STATISTICS \
      $options_file_arg \
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
  if ! [ -z "$REMOTE_USER_AT_HOST" ]; then
    echo "Running benchmark remotely on $REMOTE_USER_AT_HOST"
    kill_db_bench_cmd="$SSH $REMOTE_USER_AT_HOST $kill_db_bench_cmd"
    db_bench_cmd="$SSH $REMOTE_USER_AT_HOST $db_bench_cmd"
    ps_cmd="$SSH $REMOTE_USER_AT_HOST $ps_cmd"
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
  grep_output="$(eval $ps_cmd | grep db_bench | grep -v grep)"
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
  if ! [ -z "$REMOTE_USER_AT_HOST" ]; then
    cmd="$SSH $REMOTE_USER_AT_HOST $1"
  else
    cmd="$1"
  fi
  
  eval "$cmd"
  exit_on_error $? "$cmd"
}

function run_local {
  eval "$1"
  exit_on_error $?
}

function setup_options_file {
  if ! [ -z "$OPTIONS_FILE" ]; then
    if ! [ -z "$REMOTE_USER_AT_HOST" ]; then
      options_file="$DB_BENCH_DIR/OPTIONS_FILE"
      run_local "$SCP $OPTIONS_FILE $REMOTE_USER_AT_HOST:$options_file"
    else
      options_file="$OPTIONS_FILE"
    fi
    echo "--options_file=$options_file"
  fi
  echo ""
}

function setup_test_directory {
  echo "Deleting old regression test directories and creating new ones"

  run_remote "rm -rf $DB_PATH"
  run_remote "rm -rf $WAL_PATH"
  if ! [ -z "$REMOTE_USER_AT_HOST" ]; then
    run_remote "rm -rf $DB_BENCH_DIR"
  fi
  run_remote "mkdir -p $DB_PATH"
  run_remote "mkdir -p $WAL_PATH"
  if ! [ -z "$REMOTE_USER_AT_HOST" ]; then
    run_remote "mkdir -p $DB_BENCH_DIR"
    run_local "$SCP ./db_bench $REMOTE_USER_AT_HOST:$DB_BENCH_DIR/db_bench"
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
