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

# Size constants
K=1024
M=$((1024 * K))
G=$((1024 * M))

# Update these parameters before execution !!!
db_dir=${DATA_DIR:-"/data/ltamasi-dbbench/"}
wal_dir=${LOG_DIR:-"/data/ltamasi-dbbench/"}

for value_size in $((4 * M)) $((1 * M)) $((256 * K)) $((64 * K)) $((16 * K)) $((4 * K)) $((1 * K)); do
for enable_blob_files in 0 1; do

echo "======================== Value size: $value_size, blob files: $enable_blob_files ========================"

rm -rf $db_dir
rm -rf $wal_dir

output_dir="/root/ltamasi/output/$value_size.$enable_blob_files"

ARGS="\
OUTPUT_DIR=$output_dir \
DB_DIR=$db_dir \
WAL_DIR=$wal_dir \
NUM_THREADS=16 \
VALUE_SIZE=$value_size \
COMPRESSION_TYPE=lz4 \
ENABLE_BLOB_FILES=$enable_blob_files \
BLOB_COMPRESSION_TYPE=lz4"

ARGS_GC="$ARGS ENABLE_BLOB_GC=$enable_blob_files"
ARGS_GC_D="$ARGS_GC DURATION=1800"

rm -rf $output_dir
mkdir -p $output_dir
echo -e "ops/sec\tmb/sec\tSize-GB\tL0_GB\tSum_GB\tW-Amp\tW-MB/s\tusec/op\tp50\tp75\tp99\tp99.9\tp99.99\tUptime\tStall-time\tStall%\tTest" \
  > $output_dir/report.txt

# bulk load (using fillrandom) + compact
env $ARGS ./tools/benchmark.sh bulkload
echo -n "Disk usage after bulkload: " >> $output_dir/report.txt
du $db_dir >> $output_dir/report.txt

# overwrite
env $ARGS_GC ./tools/benchmark.sh overwrite
echo -n "Disk usage after overwrite: " >> $output_dir/report.txt
du $db_dir >> $output_dir/report.txt

# readwhilewriting
env $ARGS_GC_D ./tools/benchmark.sh readwhilewriting
echo -n "Disk usage after readwhilewriting: " >> $output_dir/report.txt
du $db_dir >> $output_dir/report.txt

# fwdrangewhilewriting
env $ARGS_GC_D ./tools/benchmark.sh fwdrangewhilewriting
echo -n "Disk usage after fwdrangewhilewriting: " >> $output_dir/report.txt
du $db_dir >> $output_dir/report.txt

# readrandom
env $ARGS_GC_D ./tools/benchmark.sh readrandom
echo -n "Disk usage after readrandom: " >> $output_dir/report.txt
du $db_dir >> $output_dir/report.txt

# fwdrange
env $ARGS_GC_D ./tools/benchmark.sh fwdrange
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

done
done
