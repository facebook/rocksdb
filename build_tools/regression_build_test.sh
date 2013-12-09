#!/bin/bash

set -e

NUM=10000000

if [ $# -eq 1 ];then
  DATA_DIR=$1
elif [ $# -eq 2 ];then
  DATA_DIR=$1
  STAT_FILE=$2
fi

# On the production build servers, set data and stat
# files/directories not in /tmp or else the tempdir cleaning
# scripts will make you very unhappy.
DATA_DIR=${DATA_DIR:-$(mktemp -t -d rocksdb_XXXX)}
STAT_FILE=${STAT_FILE:-$(mktemp -t -u rocksdb_test_stats_XXXX)}

function cleanup {
  rm -rf $DATA_DIR
  rm -f $STAT_FILE.fillseq
  rm -f $STAT_FILE.readrandom
  rm -f $STAT_FILE.overwrite
  rm -f $STAT_FILE.memtablefillreadrandom
}

trap cleanup EXIT
git_br=$(basename $GIT_BRANCH)
if [ $git_br == "master" ]; then
  git_br=""
else
  git_br="."$git_br
fi

make clean
OPT=-DNDEBUG make db_bench -j$(nproc)

# measure fillseq + fill up the DB for overwrite benchmark
./db_bench \
    --benchmarks=fillseq \
    --db=$DATA_DIR \
    --use_existing_db=0 \
    --bloom_bits=10 \
    --num=$NUM \
    --writes=$NUM \
    --cache_size=6442450944 \
    --cache_numshardbits=6 \
    --open_files=55000 \
    --statistics=1 \
    --histogram=1 \
    --disable_data_sync=1 \
    --disable_wal=1 \
    --sync=0  > ${STAT_FILE}.fillseq

# measure overwrite performance
./db_bench \
    --benchmarks=overwrite \
    --db=$DATA_DIR \
    --use_existing_db=1 \
    --bloom_bits=10 \
    --num=$NUM \
    --writes=$((NUM / 2)) \
    --cache_size=6442450944 \
    --cache_numshardbits=6  \
    --open_files=55000 \
    --statistics=1 \
    --histogram=1 \
    --disable_data_sync=1 \
    --disable_wal=1 \
    --sync=0 \
    --threads=8 > ${STAT_FILE}.overwrite

# fill up the db for readrandom benchmark (1GB total size)
./db_bench \
    --benchmarks=fillseq \
    --db=$DATA_DIR \
    --use_existing_db=0 \
    --bloom_bits=10 \
    --num=$NUM \
    --writes=$NUM \
    --cache_size=6442450944 \
    --cache_numshardbits=6 \
    --open_files=55000 \
    --statistics=1 \
    --histogram=1 \
    --disable_data_sync=1 \
    --disable_wal=1 \
    --sync=0 \
    --threads=1 > /dev/null

# measure readrandom with 6GB block cache
./db_bench \
    --benchmarks=readrandom \
    --db=$DATA_DIR \
    --use_existing_db=1 \
    --bloom_bits=10 \
    --num=$NUM \
    --reads=$NUM \
    --cache_size=6442450944 \
    --cache_numshardbits=8 \
    --open_files=55000 \
    --disable_seek_compaction=1 \
    --statistics=1 \
    --histogram=1 \
    --disable_data_sync=1 \
    --disable_wal=1 \
    --sync=0 \
    --threads=32 > ${STAT_FILE}.readrandom

# measure readrandom with 300MB block cache
./db_bench \
    --benchmarks=readrandom \
    --db=$DATA_DIR \
    --use_existing_db=1 \
    --bloom_bits=10 \
    --num=$NUM \
    --reads=$NUM \
    --cache_size=314572800 \
    --cache_numshardbits=8 \
    --open_files=55000 \
    --disable_seek_compaction=1 \
    --statistics=1 \
    --histogram=1 \
    --disable_data_sync=1 \
    --disable_wal=1 \
    --sync=0 \
    --threads=32 > ${STAT_FILE}.readrandomsmallblockcache

# measure memtable performance -- none of the data gets flushed to disk
./db_bench \
    --benchmarks=fillrandom,readrandom, \
    --db=$DATA_DIR \
    --use_existing_db=0 \
    --num=$((NUM / 10)) \
    --reads=$NUM \
    --cache_size=6442450944 \
    --cache_numshardbits=8 \
    --write_buffer_size=1000000000 \
    --open_files=55000 \
    --disable_seek_compaction=1 \
    --statistics=1 \
    --histogram=1 \
    --disable_data_sync=1 \
    --disable_wal=1 \
    --sync=0 \
    --value_size=10 \
    --threads=32 > ${STAT_FILE}.memtablefillreadrandom

# send data to ods
function send_to_ods {
  key="$1"
  value="$2"

  if [ -z "$value" ];then
    echo >&2 "ERROR: Key $key doesn't have a value."
    return
  fi
  curl -s "https://www.intern.facebook.com/intern/agent/ods_set.php?entity=rocksdb_build$git_br&key=$key&value=$value" \
    --connect-timeout 60
}

function send_benchmark_to_ods {
  bench="$1"
  bench_key="$2"
  file="$3"

  QPS=$(grep $bench $file | awk '{print $5}')
  P50_MICROS=$(grep $bench $file -A 4 | tail -n1 | awk '{print $3}' )
  P75_MICROS=$(grep $bench $file -A 4 | tail -n1 | awk '{print $5}' )
  P99_MICROS=$(grep $bench $file -A 4 | tail -n1 | awk '{print $7}' )

  send_to_ods rocksdb.build.$bench_key.qps $QPS
  send_to_ods rocksdb.build.$bench_key.p50_micros $P50_MICROS
  send_to_ods rocksdb.build.$bench_key.p75_micros $P75_MICROS
  send_to_ods rocksdb.build.$bench_key.p99_micros $P99_MICROS
}

send_benchmark_to_ods overwrite overwrite $STAT_FILE.overwrite
send_benchmark_to_ods fillseq fillseq $STAT_FILE.fillseq
send_benchmark_to_ods readrandom readrandom $STAT_FILE.readrandom
send_benchmark_to_ods readrandom readrandom_smallblockcache $STAT_FILE.readrandomsmallblockcache
send_benchmark_to_ods fillrandom memtablefillrandom $STAT_FILE.memtablefillreadrandom
send_benchmark_to_ods readrandom memtablereadrandom $STAT_FILE.memtablefillreadrandom
