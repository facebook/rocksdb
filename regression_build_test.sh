#!/bin/bash -e
make clean
make db_bench -j12

function send_to_ods {
  key="$1"
  value="$2"
  curl -s "https://www.intern.facebook.com/intern/agent/ods_set.php?entity=rocksdb_build&key=$key&value=$value"
}

NUM=100000000

DATA_DIR="$1"
if [ -z "$DATA_DIR" ]
then
  DATA_DIR="/tmp/test_ldb"
fi
STAT_FILE="/tmp/leveldb_test_stats"

./db_bench --benchmarks=fillseq --db="$DATA_DIR" --use_existing_db=0 --bloom_bits=10 --num=$NUM --writes=$NUM --cache_size=6442450944 --cache_numshardbits=6 --open_files=55000 --statistics=1 --histogram=1 --disable_data_sync=1 --disable_wal=1 --sync=0  > "$STAT_FILE.fillseq"

./db_bench --benchmarks=overwrite --db=$DATA_DIR --use_existing_db=1 --bloom_bits=10 --num=$NUM --writes=$((NUM / 2)) --cache_size=6442450944 --cache_numshardbits=6  --open_files=55000 --statistics=1 --histogram=1 --disable_data_sync=1 --disable_wal=1 --sync=0 --threads=8 > "$STAT_FILE.overwrite"

./db_bench --benchmarks=readrandom --db=$DATA_DIR --use_existing_db=1 --bloom_bits=10 --num=$NUM --reads=$((NUM / 100)) --cache_size=6442450944 --cache_numshardbits=6 --open_files=55000 --statistics=1 --histogram=1 --disable_data_sync=1 --disable_wal=1 --sync=0 --threads=128 > "$STAT_FILE.readrandom"

OVERWRITE_OPS=$(grep overwrite "$STAT_FILE.overwrite" |cut -d"/" -f2 |cut -d" " -f2)
FILLSEQ_OPS=$(grep fillseq a.out |cut -d"/" -f2 |cut -d" " -f2)
READRANDOM_OPS=$(grep readrandom a.out |cut -d"/" -f2 |cut -d" " -f2)
send_to_ods rocksdb.build.overwrite.qps $OVERWRITE_OPS
send_to_ods rocksdb.build.fillseq.qps $FILLSEQ_OPS
send_to_ods rocksdb.build.readrandom.qps $READRANDOM_OPS
