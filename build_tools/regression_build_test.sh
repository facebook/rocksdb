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
}

trap cleanup EXIT

function send_to_ods {
  key="$1"
  value="$2"

  if [ -z "$value" ];then
    echo >&2 "ERROR: Key $key doesn't have a value."
    return
  fi
  curl -s "https://www.intern.facebook.com/intern/agent/ods_set.php?entity=rocksdb_build&key=$key&value=$value" \
    --connect-timeout 60
}

make clean
make db_bench -j$(nproc)

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

./db_bench \
    --benchmarks=readrandom \
    --db=$DATA_DIR \
    --use_existing_db=1 \
    --bloom_bits=10 \
    --num=$NUM \
    --reads=$((NUM / 100)) \
    --cache_size=6442450944 \
    --cache_numshardbits=6 \
    --open_files=55000 \
    --statistics=1 \
    --histogram=1 \
    --disable_data_sync=1 \
    --disable_wal=1 \
    --sync=0 \
    --threads=128 > ${STAT_FILE}.readrandom

OVERWRITE_OPS=$(awk '/overwrite/ {print $5}' $STAT_FILE.overwrite)
FILLSEQ_OPS=$(awk '/fillseq/ {print $5}' $STAT_FILE.fillseq)
READRANDOM_OPS=$(awk '/readrandom/ {print $5}' $STAT_FILE.readrandom)

send_to_ods rocksdb.build.overwrite.qps $OVERWRITE_OPS
send_to_ods rocksdb.build.fillseq.qps $FILLSEQ_OPS
send_to_ods rocksdb.build.readrandom.qps $READRANDOM_OPS
