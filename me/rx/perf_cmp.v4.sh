#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# REQUIRE: db_bench binary exists in the current directory

dbdir=$1
odir=$2

# Size Constants
K=1024
M=$((1024 * K))
G=$((1024 * M))
T=$((1024 * G))

# Benchmark configuration
nsecs=${NSECS:-65}
nsecs_ro=${NSECS_RO:-65}
nkeys=${NKEYS:-1000000}
nthreads=${NTHREADS:-16}
key_bytes=${KEY_BYTES:-20}
value_bytes=${VALUE_BYTES:-400}
mb_wps=${MB_WPS:-2}

# RocksDB configuration
subcomp=${SUBCOMP:-1}
comp_type=${COMP_TYPE:-lz4}
write_buf_mb=${WRITE_BUF_MB:-32}
sst_mb=${SST_MB:-32}
l1_mb=${L1_MB:-128}
max_bg_jobs=${MAX_BG_JOBS:-8}
stats_seconds=${STATS_SECONDS:-20}
cache_meta=${CACHE_META:-1}
# DIRECT_IO doesn't need a default
cache_mb=${CACHE_MB:-128}
# Use defaults for hard/soft pending limits when PENDING_RATIO <= 0
pending_ratio=${PENDING_RATIO:-4}
pending_gb=${PENDING_GB:-40}
ml2_comp=${ML2_COMP:-"-1"}
pct_comp=${PCT_COMP:-"-1"}

comp_style=${COMP_STYLE:-leveled}
if [ $comp_style = "leveled" ]; then
  echo Use leveled compaction
elif [ $comp_style = "universal" ]; then
  echo Use universal compaction
elif [ $comp_style = "blob" ]; then
  echo Use blob compaction
else
  echo COMP_STYLE is :: $COMP_STYLE :: and must be one of leveled, universal, blob
  exit 1
fi

# Leveled compaction configuration
level_comp_start=${LEVEL_COMP_START:-4}
level_comp_slow=${LEVEL_COMP_SLOW:-20}
level_comp_stop=${LEVEL_COMP_STOP:-30}
per_level_fanout=${FANOUT:-8}

# Universal compaction configuration
univ_comp_start=${UNIV_COMP_START:-8}
univ_comp_slow=${UNIV_COMP_SLOW:-20}
univ_comp_stop=${UNIV_COMP_STOP:-30}
univ_min_merge_width=${UNIV_MIN_MERGE_WIDTH:-2}
univ_max_merge_width=${UNIV_MAX_MERGE_WIDTH:-20}
univ_size_ratio=${UNIV_SIZE_RATIO:-1}
univ_max_size_amp=${UNIV_MAX_SIZE_AMP:-200}

# Integrated BlobDB configuration

iblob_min_size=${IBLOB_MIN_SIZE:-0}
iblob_file_size=${IBLOB_FILE_SIZE:-$(( 256 * $M ))}
iblob_compression_type=${IBLOB_COMPRESSION_TYPE:-lz4}
iblob_gc_age_cutoff=${IBLOB_GC_AGE_CUTOFF:-"0.25"}
iblob_gc_force_threshold=${IBLOB_GC_FORCE_THRESHOLD:-1}

# Arguments used for all tests
base_args=( NUM_KEYS=$nkeys )
base_args+=( NUM_THREADS=$nthreads )
base_args+=( KEY_SIZE=$key_bytes )
base_args+=( VALUE_SIZE=$value_bytes )

base_args+=( SUBCOMPACTIONS=$subcomp )
base_args+=( COMPRESSION_TYPE=$comp_type )
base_args+=( WRITE_BUFFER_SIZE_MB=$write_buf_mb )
base_args+=( TARGET_FILE_SIZE_BASE_MB=$sst_mb )
base_args+=( MAX_BYTES_FOR_LEVEL_BASE_MB=$l1_mb )
base_args+=( MAX_BACKGROUND_JOBS=$max_bg_jobs )
base_args+=( STATS_INTERVAL_SECONDS=$stats_seconds )
base_args+=( CACHE_INDEX_AND_FILTER_BLOCKS=$cache_meta )
base_args+=( COMPACTION_STYLE=$comp_style )

if [ ! -z $DIRECT_IO ]; then
  base_args+=( USE_O_DIRECT=1 )
fi

if [ ! -z $NUMA ]; then
  base_args+=( NUMACTL=1 )
fi

if [ ! -z $CACHE_MB ]; then
  cacheb=$(( $CACHE_MB * 1024 * 1024 ))
  base_args+=( CACHE_SIZE=$cacheb )
fi

if [ $comp_style == "leveled" ]; then
  base_args+=( LEVEL0_FILE_NUM_COMPACTION_TRIGGER=$level_comp_start )
  base_args+=( LEVEL0_SLOWDOWN_WRITES_TRIGGER=$level_comp_slow )
  base_args+=( LEVEL0_STOP_WRITES_TRIGGER=$level_comp_stop )
elif [ $comp_style == "universal" ]; then
  base_args+=( LEVEL0_FILE_NUM_COMPACTION_TRIGGER=$univ_comp_start )
  base_args+=( LEVEL0_SLOWDOWN_WRITES_TRIGGER=$univ_comp_slow )
  base_args+=( LEVEL0_STOP_WRITES_TRIGGER=$univ_comp_stop )
  base_args+=( UNIVERSAL_MIN_MERGE_WIDTH=$univ_min_merge_width )
  base_args+=( UNIVERSAL_MAX_MERGE_WIDTH=$univ_max_merge_width )
  base_args+=( UNIVERSAL_SIZE_RATIO=$univ_size_ratio )
  base_args+=( UNIVERSAL_MAX_SIZE_AMP=$univ_max_size_amp )
  if [ ! -z $UNIV_ALLOW_TRIVIAL_MOVE ]; then
    base_args+=( UNIVERSAL_ALLOW_TRIVIAL_MOVE=1 )
  fi
else
  # Inherit settings for leveled because index uses leveled LSM
  base_args+=( LEVEL0_FILE_NUM_COMPACTION_TRIGGER=$level_comp_start )
  base_args+=( LEVEL0_SLOWDOWN_WRITES_TRIGGER=$level_comp_slow )
  base_args+=( LEVEL0_STOP_WRITES_TRIGGER=$level_comp_stop )
  # Then add BlobDB specific settings
  base_args+=( MIN_BLOB_SIZE=$iblob_min_size )
  base_args+=( BLOB_FILE_SIZE=$iblob_file_size )
  base_args+=( BLOB_COMPRESSION_TYPE=$iblob_compression_type )
  base_args+=( BLOB_GC_AGE_CUTOFF=$iblob_gc_age_cutoff )
  base_args+=( BLOB_GC_FORCE_THRESHOLD=$iblob_gc_force_threshold )
fi

function usage {
  echo "usage: perf_cmp.sh db_dir output_dir version+"
  echo -e "\tdb_dir - create RocksDB database in this directory"
  echo -e "\toutput_dir - write output from performance tests in this directory"
  echo -e "\tversion+ - space separated sequence of RocksDB versions to test."
  echo -e "\t\tThis expects that db_bench.\$version exists in \$PWD for each version in the sequence."
  echo -e "\t\tExample value for version+ is 6.23.0 6.24.0"
  echo ""
  echo -e "Environment variables for options"
  echo -e "\tNKEYS - number of keys to load"
  echo -e "\tKEY_BYTES - size of key"
  echo -e "\tVALUE_BYTES - size of value"
  echo -e "\tCACHE_MB - size of block cache in MB"
  echo -e "\tNSECS - number of seconds for which each test runs, except for read-only tests"
  echo -e "\tNSECS_RO - number of seconds for which each read-only test runs"
  echo -e "\tMB_WPS - rate limit for writer that runs concurrent with queries for some tests"
  echo -e "\tNTHREADS - number of user threads"
  echo -e "\tCOMP_TYPE - compression type (zstd, lz4, none, etc)"
  echo -e "\tML2_COMP - min_level_to_compress for leveled"
  echo -e "\tPCT_COMP - min_level_to_compress for universal"
  echo -e "\tWRITE_BUF_MB - size of write buffer in MB"
  echo -e "\tSST_MB - target_file_size_base in MB"
  echo -e "\tL1_MB - max_bytes_for_level_base in MB"
  echo -e "\tMAX_BG_JOBS - max_background_jobs"
  echo -e "\tCACHE_META - cache_index_and_filter_blocks"
  echo -e "\tDIRECT_IO\t\tUse O_DIRECT for user reads and compaction"
  echo -e "\tPENDING_RATIO - used to estimate write-stall limits"
  echo -e "\tSTATS_SECONDS\t\tValue for stats_interval_seconds"
  echo -e "\tSUBCOMP\t\tValue for subcompactions"
  echo -e "\tCOMP_STYLE\tCompaction style to use, one of: leveled, universal, blob"
  echo -e "\tOptions specific to leveled compaction:"
  echo -e "\t\tLEVEL_COMP_START\tValue for level0_file_num_compaction_trigger"
  echo -e "\t\tLEVEL_COMP_SLOW\tValue for level0_slowdown_writes_trigger"
  echo -e "\t\tLEVEL_COMP_STOP\tValue for level0_stop_writes_trigger"
  echo -e "\t\tFANOUT\tValue for max_bytes_for_level_multiplier"
  echo -e "\tOptions specific to universal compaction:"
  echo -e "\t\tUNIV_COMP_START\tValue for level0_file_num_compaction_trigger"
  echo -e "\t\tUNIV_COMP_SLOW\tValue for level0_slowdown_writes_trigger"
  echo -e "\t\tUNIV_COMP_STOP\tValue for level0_stop_writes_trigger"
  echo -e "\t\tUNIV_MIN_MERGE_WIDTH\tValue of min_merge_width option for universal"
  echo -e "\t\tUNIV_MAX_MERGE_WIDTH\tValue of min_merge_width option for universal"
  echo -e "\t\tUNIV_SIZE_RATIO\tValue of size_ratio option for universal"
  echo -e "\t\tUNIV_MAX_SIZE_AMP\tmax_size_amplification_percent for universal"
  echo -e "\t\tUNIV_ALLOW_TRIVIAL_MOVE\tSet allow_trivial_move to true for universal, default is false"
  echo -e "\tOptions for integrated BlobDB:"
  echo -e "\t\tIBLOB_MIN_SIZE\tValue for min_blob_size"
  echo -e "\t\tIBLOB_FILE_SIZE\tValue for blob_file_size"
  echo -e "\t\tIBLOB_COMPRESSION_TYPE\tValue for blob_compression_type"
  echo -e "\t\tIBLOB_GC_AGE_CUTOFF\tValue for blog_garbage_collection_age_cutoff"
  echo -e "\t\tIBLOB_GC_FORCE_THRESHOLD\tValue for blog_garbage_collection_force_threshold"
}

function dump_env {
  echo "Base args" > $odir/args
  echo "${base_args[@]}" | tr ' ' '\n' >> $odir/args

  echo -e "\nOther args" >> $odir/args
  echo -e "dbdir\t$dbdir" >> $odir/args
  echo -e "nsecs\t$nsecs" >> $odir/args
  echo -e "nsecs_ro\t$nsecs_ro" >> $odir/args
  echo -e "pending_ratio\t$pending_ratio" >> $odir/args
  echo -e "pending_gb\t$pending_gb" >> $odir/args
  echo -e "per_level_fanout\t$per_level_fanout" >> $odir/args

  echo -e "\nargs_load:" >> $odir/args
  echo "${args_load[@]}" | tr ' ' '\n' >> $odir/args
  echo -e "\nargs_nolim:" >> $odir/args
  echo "${args_nolim[@]}" | tr ' ' '\n' >> $odir/args
  echo -e "\nargs_lim:" >> $odir/args
  echo "${args_lim[@]}" | tr ' ' '\n' >> $odir/args
}

if [ $# -lt 3 ]; then
  usage
  echo
  echo "Need at least 3 arguments"
  exit 1
fi

shift 2

mkdir -p $odir

# The goal is to make the limits large enough so that:
# 1) there aren't write stalls after every L0->L1 compaction
#
# But small enough so that
# 2) space-amp doesn't get out of control
# 3) the LSM tree shape doesn't get out of control
#
# For 1) and L0->L1 compactions the limit should be larger than the max of:
#       a) L1a = sizeof(L0) + sizeof(L1)
#       b) L1b = sizeof(L0) * per-level-fanout
#
# For 1) and Ln->Ln+1 compactions the limit should be larger than the debt
# created by concurrent compactions ...
#       L1c = max-background-jobs * target-file-size-base * per-level-fanout
#       
# For 2) and 3) the assumption is that a reasonable value for 1) is the solution.
#
# Then use (pending_ratio * [ max(L1a, L1b) + L1c ]) + $pending_gb for soft_pending_compaction_bytes_limit,
# where pending_ratio is a fudge factor, and set the hard limit to be twice the soft limit.

if [ $pending_ratio -gt 0 ]; then
  l0_mb=$(( $level_comp_start * write_buf_mb ))
  val_L1a=$(( $l0_mb + $l1_mb ))
  val_L2a=$(( $l0_mb * $per_level_fanout ))
  val_max=$( echo $val_L1a $val_L2a | awk '{ if ($1 > $2) { print $1 } else { print $2 } }' )

  val_L1c=$(( $max_bg_jobs * $sst_mb * per_level_fanout ))
  val_sum=$(( (( $val_max + $val_L1c ) * $pending_ratio ) + ( $pending_gb * 1024 ) ))

  # The units are GB
  soft_gb=$( echo $val_sum | awk '{ printf "%.0f", $val_sum / 1024 }' )
  hard_gb=$(( $soft_gb * 2 ))
fi

echo Test versions: $@
echo Test versions: $@ >> $odir/args

for v in $@ ; do
  my_odir=$odir/$v

  if [ -d $my_odir ]; then
    echo "Exiting because the output directory ($my_odir) exists"
    exit 1
  fi

  args_common=("${base_args[@]}")

  args_common+=( OUTPUT_DIR=$my_odir DB_DIR=$dbdir WAL_DIR=$dbdir DB_BENCH_NO_SYNC=1 )
  if [ $pending_ratio -gt 0 ]; then
    args_common+=( SOFT_PENDING_COMPACTION_BYTES_LIMIT_IN_GB=$soft_gb HARD_PENDING_COMPACTION_BYTES_LIMIT_IN_GB=$hard_gb )
  fi

  if [ $comp_style == "leveled" ]; then
    args_common+=( MIN_LEVEL_TO_COMPRESS=$ml2_comp )
  elif [ $comp_style == "universal" ]; then
    args_common+=( UNIVERSAL=1 COMPRESSION_SIZE_PERCENT=$pct_comp )
  else
    args_common+=( MIN_LEVEL_TO_COMPRESS=$ml2_comp )
  fi

  args_load=("${args_common[@]}")

  args_nolim=("${args_common[@]}")
  args_nolim+=( PENDING_BYTES_RATIO=$pending_ratio )

  args_lim=("${args_nolim[@]}")
  args_lim+=( MB_WRITE_PER_SEC=$mb_wps )

  dump_env

  echo Run benchmark for $v at $( date ) with results at $my_odir
  rm -f db_bench
  echo ln -s db_bench.$v db_bench
  ln -s db_bench.$v db_bench

  # rm -rf $my_odir
  rm -rf $dbdir/*

  # Load in key order
  echo env "${args_load[@]}" bash b.v4.sh fillseq_disable_wal
  env "${args_load[@]}" bash b.v4.sh fillseq_disable_wal

  # Read-only tests. The LSM tree shape is in a deterministic state if trivial move
  # was used during the load.

  env "${args_nolim[@]}" DURATION=$nsecs_ro bash b.v4.sh readrandom
  env "${args_nolim[@]}" DURATION=$nsecs_ro bash b.v4.sh fwdrange
  env "${args_lim[@]}"   DURATION=$nsecs_ro bash b.v4.sh multireadrandom
  # Skipping --multiread_batched for now because it isn't supported on older 6.X releases
  # env "${args_lim[@]}" DURATION=$nsecs_ro bash b.v4.sh multireadrandom --multiread_batched

  # Write 10% of the keys. The goal is to randomize keys prior to Lmax
  p10=$( echo $nkeys $nthreads | awk '{ printf "%.0f", $1 / $2 / 10.0 }' )
  env "${args_nolim[@]}" WRITES=$p10        bash b.v4.sh overwritesome

  if [ $comp_style == "leveled" ]; then
    # These are not supported by older versions
    # Flush memtable & L0 to get LSM tree into deterministic state
    # env "${args_nolim[@]}"                  bash b.v4.sh flush_mt_l0
    echo flush and wait not supported
  elif [ $comp_style == "universal" ]; then
    # For universal don't compact L0 as can have too many sorted runs
    # waitforcompaction can hang, see https://github.com/facebook/rocksdb/issues/9275
    # While this is disabled the test that follows will have more variance from compaction debt.
    # env "${args_nolim[@]}"                    bash b.v4.sh waitforcompaction
    echo TODO enable when waitforcompaction hang is fixed
  else
    # These are not supported by older versions
    # Flush memtable & L0 to get LSM tree into deterministic state
    # env "${args_nolim[@]}"                  bash b.v4.sh flush_mt_l0
    echo flush and wait not supported
  fi

  # Read-mostly tests with a rate-limited writer
  env "${args_lim[@]}" DURATION=$nsecs    bash b.v4.sh revrangewhilewriting
  env "${args_lim[@]}" DURATION=$nsecs    bash b.v4.sh fwdrangewhilewriting
  env "${args_lim[@]}" DURATION=$nsecs    bash b.v4.sh readwhilewriting

  # Write-only tests

  # This creates much compaction debt which will be a problem for tests added after it.
  # Also, the compaction stats measured at test end can underestimate write-amp depending
  # on how much compaction debt is allowed.
  env "${args_nolim[@]}" DURATION=$(( nsecs * 1 )) bash b.v4.sh overwrite

  cp $dbdir/LOG* $my_odir
  gzip -9 $my_odir/LOG*

done

# Generate a file that groups lines from the same test for all versions
basev=$1
nlines=$( awk '/^ops_sec/,/END/' $odir/${basev}/report.tsv | grep -v ops_sec | wc -l )
hline=$( awk '/^ops_sec/ { print NR }' $odir/${basev}/report.tsv )
sline=$(( $hline + 1 ))
eline=$(( $sline + $nlines - 1 ))

sum_file=$odir/summary.tsv

for v in $*; do
  echo $odir/${v}/report.tsv
done >> $sum_file
echo >> $sum_file

for x in $( seq $sline $eline ); do
  awk '{ if (NR == lno) { print $0 } }' lno=$hline $odir/${basev}/report.tsv >> $sum_file
  for v in $*; do
    r=$odir/${v}/report.tsv
    awk '{ if (NR == lno) { print $0 } }' lno=$x $r >> $sum_file
  done
echo >> $sum_file
done
