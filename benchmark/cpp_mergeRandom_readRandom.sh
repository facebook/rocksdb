me=`basename $0`
if [ $# -eq 0 ]; then
  echo "ERROR: No db path provided."
  echo "Usage: ${me} <path-to-db>"
  exit 1
fi

############################################
####   General Benchmark                ####
############################################
# benchmarks  -- see each benchmark

num=$((1 * 1000 * 1000))
reads=$num
key_size=2
value_size=$((4 * 1024))

# Number of concurrent threads to run for the benchmark.
threads=2


############################################
####   Statistics                       ####
############################################
# Report database statistics (true/false)
statistics=1

# Print histogram of operation timings (true/false)
histogram=1

# a temp varibale that estimates the total size of an entry
entry_size=$(($key_size + $value_size))

# Stats are reported every N operations when this is greater than zero.
# When 0 the interval grows over time. The formula below comes out of an
# experiment where we had an 10M interval with 16B keys and 8B values.
# interval*entry_size=10M*(16+8), hence interval = 240M/entry_size
stats_interval=$((240000000 / $entry_size))

# Reports additional stats per interval when this is greater than 0.
stats_per_interval=1


############################################
####   DB                               ####
############################################
db=$1

if [ ! -d "$db" ]; then
  mkdir -p $db
fi

# use_existing_db  -- see each benchmark

############################################
####   Parallelism                      ####
############################################
# maximum number of concurrent background compactions
max_background_compactions=1

# maximum number of concurrent flush operations. 
max_background_flushes=1


############################################
####   General                          ####
############################################
# Default bits_per_key is 10, which yields ~1% false positive rate. Larger
# values will reduce false positive rate, but increase memory usage and
# space amplification.
# Bloom filter bits per key. Negative means use default settings
bloom_bits=10

# RocksDB keeps all file descriptors in a table cache. If number of file
# descriptors exceeds max_open_files, some files are evicted from table cache
# and their file descriptors closed. Every read must go through the table
# cache to lookup the file needed. Set max_open_files to -1 to always keep
# all files open, which avoids expensive table cache calls.
max_open_files=100000

# RocksDB packs user data in blocks. When reading a key-value pair from a
# table file, an entire block is loaded into memory. 
block_size=$(($entry_size * 16))

############################################
####   Flushing (memtables)             ####
############################################
# memtable size in bytes
write_buffer_size=$((1024 * 1024))

# If the active memtable fills up and the total number of memtables is
# larger than max_write_buffer_number we stall further writes
max_write_buffer_number=6

# minimum number of memtables to be merged before flushing to storage
min_write_buffer_number_to_merge=2


############################################
####   Level Style Compaction           ####
############################################
# Compation style:
# 0 for "level style", 1 for "universal", 2 for "FIFO", 3 for "NONE"
compaction_style=0

# Once level 0 reaches this number of files, L0->L1 compaction is triggered
level0_file_num_compaction_trigger=2

# Note: size of L0 = write_buffer_size * min_write_buffer_number_to_merge * level0_file_num_compaction_trigger
# total size of level 1. Recommended that this be around the size of level 0.
max_bytes_for_level_base=$(($write_buffer_size * $min_write_buffer_number_to_merge * $level0_file_num_compaction_trigger))

# each subsuquent level is this times larger than the previous one.
# Recommended value is 10
max_bytes_for_level_multiplier=2

# size of files in level 1. Each next level's file size will be
# target_file_size_multiplier times bigger than previous one.
target_file_size_base=$(($max_bytes_for_level_base / 2))

# Each next level's file size will be
# target_file_size_multiplier times bigger than previous one.
target_file_size_multiplier=1

# It is safe for num_levels to be bigger than expected number of levels in
# the database. Only change this option if you expect your number of levels
# will be greater than 7 (default).
num_levels=7

# Control maximum bytes of  overlaps in grandparent (i.e., level+2) before
# we stop building a single file in a level->level+1 compaction.
max_grandparent_overlap_factor=10


############################################
####   Universal Compaction             ####
############################################
# see compaction_style above

# max_size_amplification_percent -- Size amplification as defined by amount of additional storage needed (in percentage) to store a byte of data in the database.

# compression_size_percent -- Percentage of data in the database that is compressed. Older data is compressed, newer data is not compressed. If set to -1 (default), all data is compressed.



############################################
####   Write stalls                     ####
############################################
# When the number of level 0 files is greater than the slowdown limit,
# writes are stalled.
level0_slowdown_writes_trigger=6

# When the number is greater than stop limit, writes are fully stopped
# until compaction is done.
level0_stop_writes_trigger=8

# In level style compaction, each level has a compaction score. When a
# compaction score is greater than 1, compaction is triggered.
# If the score for any level exceeds the soft_rate_limit, writes are slowed down.
soft_rate_limit=1.1

# If a score exceeds hard_rate_limit, writes are stopped until compaction
# for that level reduces its score.
hard_rate_limit=1.2


############################################
####  Non-documented                    ####
############################################
# Not used, left here for backwards compatibility
disable_seek_compaction=1

# Allow reads to occur via mmap-ing files
mmap_read=0

# Number of bytes to use as a cache of uncompressed data. Negative means
# use default settings.
cache_size=$(($block_size * 16))

# Number of shards for the block cache is 2^cache_numshardbits. Negative
# means use default settings. This is applied only if FLAGS_cache_size is
# non-negative.
cache_numshardbits=6

# Verify checksum for every block read from storage
verify_checksum=0

# If true, do not write WAL for write
disable_wal=1

# Algorithm to use to compress the database, default snappy
compression_type=none

# Arrange to generate values that shrink  to this fraction of their original
# size after compression
compression_ratio=0.5

# If true, do not wait until data is synced to disk
disable_data_sync=1

# Ignored. Left here for backward compatibility
delete_obsolete_files_period_micros=3000000

# If non-negative, compression starts from this level. Levels with
# number < min_level_to_compress are not compressed. Otherwise, apply
# compression_type to all levels
min_level_to_compress=2

# Sync all writes to disk (true/false)
sync=0

# same as max_open_files, but the benchmark parser expects open_files, not max_open_files
open_files=$max_open_files


##########################################################
####  First set of benchmarks                        #####
##########################################################
benchmarks=mergerandom
merge_operator=bytesxor
use_existing_db=0
threads=1

STARTTIME=$(date +%s)
echo "\n**************************************"
echo "Executing benchmarks=$benchmarks....."
echo "**************************************\n"
parameters="--benchmarks=$benchmarks --num=$num --reads=$reads --key_size=$key_size --value_size=$value_size --threads=$threads --statistics=$statistics --histogram=$histogram --stats_interval=$stats_interval --stats_per_interval=$stats_per_interval --db=$db --use_existing_db=$use_existing_db --max_background_compactions=$max_background_compactions --max_background_flushes=$max_background_flushes --bloom_bits=$bloom_bits -block_size=$block_size --write_buffer_size=$write_buffer_size --max_write_buffer_number=$max_write_buffer_number --min_write_buffer_number_to_merge=$min_write_buffer_number_to_merge --level0_file_num_compaction_trigger=$level0_file_num_compaction_trigger --max_bytes_for_level_base=$max_bytes_for_level_base --max_bytes_for_level_multiplier=$max_bytes_for_level_multiplier --target_file_size_base=$target_file_size_base --target_file_size_multiplier=$target_file_size_multiplier --num_levels=$num_levels --max_grandparent_overlap_factor=$max_grandparent_overlap_factor --level0_slowdown_writes_trigger=$level0_slowdown_writes_trigger --level0_stop_writes_trigger=$level0_stop_writes_trigger --soft_rate_limit=$soft_rate_limit --hard_rate_limit=$hard_rate_limit --disable_seek_compaction=$disable_seek_compaction --mmap_read=$mmap_read --cache_size=$cache_size --cache_numshardbits=$cache_numshardbits --verify_checksum=$verify_checksum --disable_wal=$disable_wal --compression_type=$compression_type --compression_ratio=$compression_ratio --disable_data_sync=$disable_data_sync --delete_obsolete_files_period_micros=$delete_obsolete_files_period_micros --min_level_to_compress=$min_level_to_compress --sync=$sync --open_files=$open_files --merge_operator=$merge_operator"
echo "Parameters:  $parameters\n"
../db_bench $parameters
ENDTIME=$(date +%s)
echo "\n#########################"
echo "Took $(($ENDTIME - $STARTTIME)) seconds to complete benchmarks=$benchmarks..."
echo "DB size: $(du --block-size=1 $db)"
echo "#########################\n\n"



##########################################################
####  Second set of benchmark                        #####
##########################################################
benchmarks=readrandom
use_existing_db=1

STARTTIME=$(date +%s)
echo "\n**************************************"
echo "Executing benchmarks=$benchmarks....."
echo "**************************************"
parameters="--benchmarks=$benchmarks --num=$num --reads=$reads --key_size=$key_size --value_size=$value_size --threads=$threads --statistics=$statistics --histogram=$histogram --stats_interval=$stats_interval --stats_per_interval=$stats_per_interval --db=$db --use_existing_db=$use_existing_db --max_background_compactions=$max_background_compactions --max_background_flushes=$max_background_flushes --bloom_bits=$bloom_bits --block_size=$block_size --write_buffer_size=$write_buffer_size --max_write_buffer_number=$max_write_buffer_number --min_write_buffer_number_to_merge=$min_write_buffer_number_to_merge --level0_file_num_compaction_trigger=$level0_file_num_compaction_trigger --max_bytes_for_level_base=$max_bytes_for_level_base --max_bytes_for_level_multiplier=$max_bytes_for_level_multiplier --target_file_size_base=$target_file_size_base --target_file_size_multiplier=$target_file_size_multiplier --num_levels=$num_levels --max_grandparent_overlap_factor=$max_grandparent_overlap_factor --level0_slowdown_writes_trigger=$level0_slowdown_writes_trigger --level0_stop_writes_trigger=$level0_stop_writes_trigger --soft_rate_limit=$soft_rate_limit --hard_rate_limit=$hard_rate_limit --disable_seek_compaction=$disable_seek_compaction --mmap_read=$mmap_read --cache_size=$cache_size --cache_numshardbits=$cache_numshardbits --verify_checksum=$verify_checksum --disable_wal=$disable_wal --compression_type=$compression_type --compression_ratio=$compression_ratio --disable_data_sync=$disable_data_sync --delete_obsolete_files_period_micros=$delete_obsolete_files_period_micros --min_level_to_compress=$min_level_to_compress --sync=$sync --open_files=$open_files --merge_operator=$merge_operator"
echo "Parameters:  $parameters\n"
../db_bench $parameters
ENDTIME=$(date +%s)
echo "\n#########################"
echo "Took $(($ENDTIME - $STARTTIME)) seconds to complete benchmarks=$benchmarks..."
echo "DB size: $(du --block-size=1 $db)"
echo "#########################\n\n"

