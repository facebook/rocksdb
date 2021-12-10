dbdir=$1
odir=$2

# Benchmark configuration
nsecs=${NSECS:-65}
nsecs_ro=${NSECS_RO:-65}
nkeys=${NKEYS:-1000000}
nthreads=${NTHREADS:-16}
key_bytes=${KEY_BYTES:-20}
value_bytes=${VALUE_BYTES:-400}
mb_wps=${MB_WPS:-2}
write_amp_estimate=${WRITE_AMP_ESTIMATE:-20}

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
pending_ratio=${PENDING_RATIO:-"0.5"}
ml2_comp=${ML2_COMP:-"-1"}

# Leveled compaction configuration
level_comp_start=${LEVEL_COMP_START:-4}
level_comp_slow=${LEVEL_COMP_SLOW:-20}
level_comp_stop=${LEVEL_COMP_STOP:-30}

# Universal compaction configuration
univ_comp_start=${UNIV_COMP_START:-8}
univ_comp_slow=${UNIV_COMP_SLOW:-20}
univ_comp_stop=${UNIV_COMP_STOP:-30}
univ_min_merge_width=${UNIV_MIN_MERGE_WIDTH:-2}
univ_max_merge_width=${UNIV_MAX_MERGE_WIDTH:-20}
univ_size_ratio=${UNIV_SIZE_RATIO:-1}
univ_max_size_amp=${UNIV_MAX_SIZE_AMP:-200}

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

if [ ! -z $DIRECT_IO ]; then
  base_flags+=( USE_O_DIRECT=1 )
fi

if [ ! -z $CACHE_MB ]; then
  cacheb=$(( $CACHE_MB * 1024 * 1024 ))
  base_args+=( CACHE_SIZE=$cacheb )
fi

if [ -z $UNIV ]; then
  base_args+=( LEVEL0_FILE_NUM_COMPACTION_TRIGGER=$level_comp_start )
  base_args+=( LEVEL0_SLOWDOWN_WRITES_TRIGGER=$level_comp_slow )
  base_args+=( LEVEL0_STOP_WRITES_TRIGGER=$level_comp_stop )
else
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
fi

# This is separate because leveled compaction uses it after the load
# while universal uses it during and after the load.
comp_args=( MIN_LEVEL_TO_COMPRESS=$ml2_comp )

# Values for published results: 
# NUM_KEYS=900,000,000 CACHE_SIZE=6,442,450,944 DURATION=5400 MB_WRITE_PER_SEC=2 

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
  echo -e "\tML2_COMP - min_level_to_compress"
  echo -e "\tWRITE_BUF_MB - size of write buffer in MB"
  echo -e "\tSST_MB - target_file_size_base in MB"
  echo -e "\tL1_MB - max_bytes_for_level_base in MB"
  echo -e "\tMAX_BG_JOBS - max_background_jobs"
  echo -e "\tCACHE_META - cache_index_and_filter_blocks"
  echo -e "\tDIRECT_IO\t\tUse O_DIRECT for user reads and compaction"
  echo -e "\tPENDING_RATIO - used to estimate write-stall limits"
  echo -e "\tWRITE_AMP_ESTIMATE\t\tEstimate for the write-amp that will occur. Used to compute write-stall limits"
  echo -e "\tSTATS_SECONDS\t\tValue for stats_interval_seconds"
  echo -e "\tSUBCOMP\t\tValue for subcompactions"
  echo -e "\tOptions specific to leveled compaction:"
  echo -e "\t\tLEVEL_COMP_START\tValue for level0_file_num_compaction_trigger"
  echo -e "\t\tLEVEL_COMP_SLOW\tValue for level0_slowdown_writes_trigger"
  echo -e "\t\tLEVEL_COMP_STOP\tValue for level0_stop_writes_trigger"
  echo -e "\tOptions specific to universal compaction:"
  echo -e "\t\tUNIV_COMP_START\tValue for level0_file_num_compaction_trigger"
  echo -e "\t\tUNIV_COMP_SLOW\tValue for level0_slowdown_writes_trigger"
  echo -e "\t\tUNIV_COMP_STOP\tValue for level0_stop_writes_trigger"
  echo -e "\t\tUNIV\t\tUse universal compaction when set to anything, otherwise use leveled"
  echo -e "\t\tUNIV_MIN_MERGE_WIDTH\tValue of min_merge_width option for universal"
  echo -e "\t\tUNIV_MAX_MERGE_WIDTH\tValue of min_merge_width option for universal"
  echo -e "\t\tUNIV_SIZE_RATIO\tValue of size_ratio option for universal"
  echo -e "\t\tUNIV_MAX_SIZE_AMP\tmax_size_amplification_percent for universal"
  echo -e "\t\tUNIV_ALLOW_TRIVIAL_MOVE\tSet allow_trivial_move to true for universal, default is false"
}

function dump_env {
  echo "Base args" > $odir/args
  echo "${base_args[@]}" | tr ' ' '\n' >> $odir/args

  echo -e "\nCompression args" >> $odir/args
  echo "${comp_args[@]}" | tr ' ' '\n' >> $odir/args

  echo -e "\nOther args" >> $odir/args
  echo -e "dbdir\t$dbdir" >> $odir/args
  echo -e "nsecs\t$nsecs" >> $odir/args
  echo -e "nsecs_ro\t$nsecs_ro" >> $odir/args
  echo -e "pending_ratio\t$pending_ratio" >> $odir/args
  echo -e "write_amp_estimate\t$write_amp_estimate" >> $odir/args
  echo -e "univ\t$UNIV" >> $odir/args

  echo -e "\nbenchargs1:" >> $odir/args
  echo "${benchargs1[@]}" | tr ' ' '\n' >> $odir/args
  echo -e "\nbenchargs2:" >> $odir/args
  echo "${benchargs2[@]}" | tr ' ' '\n' >> $odir/args
  echo -e "\nbenchargs3:" >> $odir/args
  echo "${benchargs3[@]}" | tr ' ' '\n' >> $odir/args
}

if [ $# -lt 3 ]; then
  usage
  echo
  echo "Need at least 3 arguments"
  exit 1
fi

shift 2

if [ -d $odir ]; then
  echo "Exiting because the output directory ($odir) exists"
  exit 1
fi
mkdir $odir

# The goal is to make the limits large enough so that:
# 1) there aren't write stalls after every L0->L1 compaction
#
# But small enough so that
# 2) space-amp doesn't get out of control
# 3) the LSM tree shape doesn't get out of control
#
# For 1) the limit should be sufficiently larger than sizeof(L0) * write-amp
#     where write-amp is an estimate. Call this limit-1.
# For 2) and 3) the limit can be a function of the database size, f * sizeof(database)
#     where f is a value > 0, usually < 1, and the size is an estimate. Call
#     this limit-2,3. The value for f is passed by PENDING_BYTES_RATIO.
#
# Then use max(limit-1, limit-2,3) for soft_pending_compaction_bytes_limit
# and set the hard limit to be twice the soft limit.
#
# soft_pending_compaction_bytes_limit = estimated-db-size * pending_bytes_ratio
#     where estimated-db-size ignores compression
# hard_pending_compaction_bytes_limit = 2 * soft_pending_compaction_bytes_limit

pending_ratio=${PENDING_RATIO:-"0.5"}
write_amp_estimate=${WRITE_AMP_ESTIMATE:-20}

# This computes limit-2,3 in GB
soft_bytes23=$( echo $pending_ratio $nkeys $key_bytes $value_bytes | \
  awk '{ soft = (($3 + $4) * $2 * $1) / (1024*1024*1024); printf "%.1f", soft }' )

# This computes limit-1 in GB
# Multiplying by 2 below is a fudge factor
soft_bytes1=$( echo $compaction_trigger $write_buf_mb $write_amp_estimate | \
  awk '{ soft = ($1 * $2 * $3 * 2) / 1024; printf "%.1f", soft }' )
# Choose the max from soft_bytes1 and soft_bytes23
soft_bytes=$( echo $soft_bytes1 $soft_bytes23 | \
  awk '{ mx=$1; if ($2 > $1) { mx = $2 }; printf "%s", mx }' )
# To be safe make sure the soft limit is >= 10G
soft_bytes=$( echo $soft_bytes | \
  awk '{ mx=$1; if (10 > $1) { mx = 10 }; printf "%.0f", mx }' )
# Set the hard limit to be 2x the soft limit
hard_bytes=$( echo $soft_bytes | awk '{ printf "%.0f", $1 * 2 }' )

echo Test versions: $@
echo Test versions: $@ >> $odir/args

for v in $@ ; do
  my_odir=$odir/$v
  benchargs1=("${base_args[@]}")

  benchargs1+=( OUTPUT_DIR=$my_odir DB_DIR=$dbdir WAL_DIR=$dbdir DB_BENCH_NO_SYNC=1 )
  benchargs1+=( SOFT_PENDING_COMPACTION_BYTES_LIMIT_IN_GB=$soft_bytes HARD_PENDING_COMPACTION_BYTES_LIMIT_IN_GB=$hard_bytes )
  if [ ! -z $UNIV ]; then
    benchargs1+=( "${comp_args[@]}" UNIVERSAL=1 )
  fi

  benchargs2=("${benchargs1[@]}")
  benchargs2+=("${post_load_args[@]}")
  benchargs2+=( PENDING_BYTES_RATIO=$pending_ratio )
  if [ -z $UNIV ]; then
    benchargs2+=("${comp_args[@]}")
  fi

  benchargs3=("${benchargs2[@]}")
  benchargs3+=( MB_WRITE_PER_SEC=$mb_wps )

  dump_env

  echo Run benchmark for $v at $( date ) with results at $my_odir
  rm -f db_bench
  ln -s db_bench.$v db_bench

  rm -rf $my_odir
  rm -rf $dbdir/*

  # Load in key order
  echo env "${benchargs1[@]}" bash b.sh fillseq_disable_wal
  env "${benchargs1[@]}" bash b.sh fillseq_disable_wal

  if [ -z $UNIV ]; then
    # Read-only tests but only for leveled because the LSM tree is in a deterministic
    # state after fillseq. This is here rather than after flush_mt_l0 because the LSM
    # tree is friendlier to reads after fillseq -- SSTs are fully ordered and non-overlapping
    # thanks to trivial move.
    env "${benchargs2[@]}" DURATION=$nsecs_ro bash b.sh readrandom
  fi

  # Write 10% of the keys. The goal is to randomize keys prior to Lmax
  p10=$( echo $nkeys | awk '{ printf "%.0f", $1 / 10.0 }' )
  env "${benchargs2[@]}" WRITES=$p10        bash b.sh overwritesome

  if [ -z $UNIV ]; then
    # Flush memtable & L0 to get LSM tree into deterministic state
    # These are not supported by older versions
    env "${benchargs2[@]}"                    bash b.sh flush_mt_l0
  else
    # These are not supported by older versions
    # For universal don't compact L0 as can have too many sorted runs
    # Disabled for now because waitforcompaction can hang, see
    # https://github.com/facebook/rocksdb/issues/9275
    # While this is disabled the test that follows will have more variance from compaction debt.
    # env "${benchargs2[@]}"                    bash b.sh waitforcompaction
    echo TODO enable when waitforcompaction hang is fixed
  fi

  # While this runs for leveled and universal, the results will have more variance for
  # universal because nothing is done above to reduce compaction debt and get the LSM tree
  # into a deterministic state. But it still serves a purpose for universal, it lets compaction
  # get caught up prior to the read-mostly tests.
  # Skipping --multiread_batched for now because it isn't supported on older 6.X releases
  # env "${benchargs2[@]}" DURATION=$nsecs_ro bash b.sh multireadrandom --multiread_batched
  # TODO: implement multireadrandomwhilewriting
  env "${benchargs2[@]}" DURATION=$nsecs_ro bash b.sh multireadrandom

  # Read-mostly tests with a rate-limited writer
  env "${benchargs3[@]}" DURATION=$nsecs    bash b.sh revrangewhilewriting
  env "${benchargs3[@]}" DURATION=$nsecs    bash b.sh fwdrangewhilewriting
  env "${benchargs3[@]}" DURATION=$nsecs    bash b.sh readwhilewriting

  # Write-only tests

  # This creates much compaction debt which will be a problem for tests added after it.
  # Also, the compaction stats measured at test end can underestimate write-amp depending
  # on how much compaction debt is allowed.
  env "${benchargs2[@]}" DURATION=$nsecs    bash b.sh overwrite

  cp $dbdir/LOG* $my_odir
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
