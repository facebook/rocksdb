// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum TickerType {

    /**
     * total block cache misses
     *
     * REQUIRES: BLOCK_CACHE_MISS == BLOCK_CACHE_INDEX_MISS +
     *     BLOCK_CACHE_FILTER_MISS +
     *     BLOCK_CACHE_DATA_MISS;
     */
    BLOCK_CACHE_MISS((byte) 0x0),

    /**
     * total block cache hit
     *
     * REQUIRES: BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT +
     *     BLOCK_CACHE_FILTER_HIT +
     *     BLOCK_CACHE_DATA_HIT;
     */
    BLOCK_CACHE_HIT((byte) 0x1),

    BLOCK_CACHE_ADD((byte) 0x2),

    /**
     * # of failures when adding blocks to block cache.
     */
    BLOCK_CACHE_ADD_FAILURES((byte) 0x3),

    /**
     * # of times cache miss when accessing index block from block cache.
     */
    BLOCK_CACHE_INDEX_MISS((byte) 0x4),

    /**
     * # of times cache hit when accessing index block from block cache.
     */
    BLOCK_CACHE_INDEX_HIT((byte) 0x5),

    /**
     * # of index blocks added to block cache.
     */
    BLOCK_CACHE_INDEX_ADD((byte) 0x6),

    /**
     * # of bytes of index blocks inserted into cache
     */
    BLOCK_CACHE_INDEX_BYTES_INSERT((byte) 0x7),

    /**
     * # of bytes of index block erased from cache
     */
    BLOCK_CACHE_INDEX_BYTES_EVICT((byte) 0x8),

    /**
     * # of times cache miss when accessing filter block from block cache.
     */
    BLOCK_CACHE_FILTER_MISS((byte) 0x9),

    /**
     * # of times cache hit when accessing filter block from block cache.
     */
    BLOCK_CACHE_FILTER_HIT((byte) 0xA),

    /**
     * # of filter blocks added to block cache.
     */
    BLOCK_CACHE_FILTER_ADD((byte) 0xB),

    /**
     * # of bytes of bloom filter blocks inserted into cache
     */
    BLOCK_CACHE_FILTER_BYTES_INSERT((byte) 0xC),

    /**
     * # of bytes of bloom filter block erased from cache
     */
    BLOCK_CACHE_FILTER_BYTES_EVICT((byte) 0xD),

    /**
     * # of times cache miss when accessing data block from block cache.
     */
    BLOCK_CACHE_DATA_MISS((byte) 0xE),

    /**
     * # of times cache hit when accessing data block from block cache.
     */
    BLOCK_CACHE_DATA_HIT((byte) 0xF),

    /**
     * # of data blocks added to block cache.
     */
    BLOCK_CACHE_DATA_ADD((byte) 0x10),

    /**
     * # of bytes of data blocks inserted into cache
     */
    BLOCK_CACHE_DATA_BYTES_INSERT((byte) 0x11),

    /**
     * # of bytes read from cache.
     */
    BLOCK_CACHE_BYTES_READ((byte) 0x12),

    /**
     * # of bytes written into cache.
     */
    BLOCK_CACHE_BYTES_WRITE((byte) 0x13),

    /**
     * # of times bloom filter has avoided file reads.
     */
    BLOOM_FILTER_USEFUL((byte) 0x14),

    /**
     * # persistent cache hit
     */
    PERSISTENT_CACHE_HIT((byte) 0x15),

    /**
     * # persistent cache miss
     */
    PERSISTENT_CACHE_MISS((byte) 0x16),

    /**
     * # total simulation block cache hits
     */
    SIM_BLOCK_CACHE_HIT((byte) 0x17),

    /**
     * # total simulation block cache misses
     */
    SIM_BLOCK_CACHE_MISS((byte) 0x18),

    /**
     * # of memtable hits.
     */
    MEMTABLE_HIT((byte) 0x19),

    /**
     * # of memtable misses.
     */
    MEMTABLE_MISS((byte) 0x1A),

    /**
     * # of Get() queries served by L0
     */
    GET_HIT_L0((byte) 0x1B),

    /**
     * # of Get() queries served by L1
     */
    GET_HIT_L1((byte) 0x1C),

    /**
     * # of Get() queries served by L2 and up
     */
    GET_HIT_L2_AND_UP((byte) 0x1D),

    /**
     * COMPACTION_KEY_DROP_* count the reasons for key drop during compaction
     * There are 4 reasons currently.
     */

    /**
     * key was written with a newer value.
     */
    COMPACTION_KEY_DROP_NEWER_ENTRY((byte) 0x1E),

    /**
     * Also includes keys dropped for range del.
     * The key is obsolete.
     */
    COMPACTION_KEY_DROP_OBSOLETE((byte) 0x1F),

    /**
     * key was covered by a range tombstone.
     */
    COMPACTION_KEY_DROP_RANGE_DEL((byte) 0x20),

    /**
     * User compaction function has dropped the key.
     */
    COMPACTION_KEY_DROP_USER((byte) 0x21),

    /**
     * all keys in range were deleted.
     */
    COMPACTION_RANGE_DEL_DROP_OBSOLETE((byte) 0x22),

    /**
     * Number of keys written to the database via the Put and Write call's.
     */
    NUMBER_KEYS_WRITTEN((byte) 0x23),

    /**
     * Number of Keys read.
     */
    NUMBER_KEYS_READ((byte) 0x24),

    /**
     * Number keys updated, if inplace update is enabled
     */
    NUMBER_KEYS_UPDATED((byte) 0x25),

    /**
     * The number of uncompressed bytes issued by DB::Put(), DB::Delete(),\
     * DB::Merge(), and DB::Write().
     */
    BYTES_WRITTEN((byte) 0x26),

    /**
     * The number of uncompressed bytes read from DB::Get().  It could be
     * either from memtables, cache, or table files.
     *
     * For the number of logical bytes read from DB::MultiGet(),
     * please use {@link #NUMBER_MULTIGET_BYTES_READ}.
     */
    BYTES_READ((byte) 0x27),

    /**
     * The number of calls to seek.
     */
    NUMBER_DB_SEEK((byte) 0x28),

    /**
     * The number of calls to next.
     */
    NUMBER_DB_NEXT((byte) 0x29),

    /**
     * The number of calls to prev.
     */
    NUMBER_DB_PREV((byte) 0x2A),

    /**
     * The number of calls to seek that returned data.
     */
    NUMBER_DB_SEEK_FOUND((byte) 0x2B),

    /**
     * The number of calls to next that returned data.
     */
    NUMBER_DB_NEXT_FOUND((byte) 0x2C),

    /**
     * The number of calls to prev that returned data.
     */
    NUMBER_DB_PREV_FOUND((byte) 0x2D),

    /**
     * The number of uncompressed bytes read from an iterator.
     * Includes size of key and value.
     */
    ITER_BYTES_READ((byte) 0x2E),

    NO_FILE_CLOSES((byte) 0x2F),

    NO_FILE_OPENS((byte) 0x30),

    NO_FILE_ERRORS((byte) 0x31),

    /**
     * Time system had to wait to do LO-L1 compactions.
     *
     * @deprecated
     */
    @Deprecated
    STALL_L0_SLOWDOWN_MICROS((byte) 0x32),

    /**
     * Time system had to wait to move memtable to L1.
     *
     * @deprecated
     */
    @Deprecated
    STALL_MEMTABLE_COMPACTION_MICROS((byte) 0x33),

    /**
     * write throttle because of too many files in L0.
     *
     * @deprecated
     */
    @Deprecated
    STALL_L0_NUM_FILES_MICROS((byte) 0x34),

    /**
     * Writer has to wait for compaction or flush to finish.
     */
    STALL_MICROS((byte) 0x35),

    /**
     * The wait time for db mutex.
     *
     * Disabled by default. To enable it set stats level to {@link StatsLevel#ALL}
     */
    DB_MUTEX_WAIT_MICROS((byte) 0x36),

    RATE_LIMIT_DELAY_MILLIS((byte) 0x37),

    /**
     * Number of iterators currently open.
     */
    NO_ITERATORS((byte) 0x38),

    /**
     * Number of MultiGet calls.
     */
    NUMBER_MULTIGET_CALLS((byte) 0x39),

    /**
     * Number of MultiGet keys read.
     */
    NUMBER_MULTIGET_KEYS_READ((byte) 0x3A),

    /**
     * Number of MultiGet bytes read.
     */
    NUMBER_MULTIGET_BYTES_READ((byte) 0x3B),

    /**
     * Number of deletes records that were not required to be
     * written to storage because key does not exist.
     */
    NUMBER_FILTERED_DELETES((byte) 0x3C),
    NUMBER_MERGE_FAILURES((byte) 0x3D),

    /**
     * Number of times bloom was checked before creating iterator on a
     * file, and the number of times the check was useful in avoiding
     * iterator creation (and thus likely IOPs).
     */
    BLOOM_FILTER_PREFIX_CHECKED((byte) 0x3E),
    BLOOM_FILTER_PREFIX_USEFUL((byte) 0x3F),

    /**
     * Number of times we had to reseek inside an iteration to skip
     * over large number of keys with same userkey.
     */
    NUMBER_OF_RESEEKS_IN_ITERATION((byte) 0x40),

    /**
     * Record the number of calls to {@link RocksDB#getUpdatesSince(long)}. Useful to keep track of
     * transaction log iterator refreshes.
     */
    GET_UPDATES_SINCE_CALLS((byte) 0x41),

    /**
     * Miss in the compressed block cache.
     */
    BLOCK_CACHE_COMPRESSED_MISS((byte) 0x42),

    /**
     * Hit in the compressed block cache.
     */
    BLOCK_CACHE_COMPRESSED_HIT((byte) 0x43),

    /**
     * Number of blocks added to compressed block cache.
     */
    BLOCK_CACHE_COMPRESSED_ADD((byte) 0x44),

    /**
     * Number of failures when adding blocks to compressed block cache.
     */
    BLOCK_CACHE_COMPRESSED_ADD_FAILURES((byte) 0x45),

    /**
     * Number of times WAL sync is done.
     */
    WAL_FILE_SYNCED((byte) 0x46),

    /**
     * Number of bytes written to WAL.
     */
    WAL_FILE_BYTES((byte) 0x47),

    /**
     * Writes can be processed by requesting thread or by the thread at the
     * head of the writers queue.
     */
    WRITE_DONE_BY_SELF((byte) 0x48),

    /**
     * Equivalent to writes done for others.
     */
    WRITE_DONE_BY_OTHER((byte) 0x49),

    /**
     * Number of writes ending up with timed-out.
     */
    WRITE_TIMEDOUT((byte) 0x4A),

    /**
     * Number of Write calls that request WAL.
     */
    WRITE_WITH_WAL((byte) 0x4B),

    /**
     * Bytes read during compaction.
     */
    COMPACT_READ_BYTES((byte) 0x4C),

    /**
     * Bytes written during compaction.
     */
    COMPACT_WRITE_BYTES((byte) 0x4D),

    /**
     * Bytes written during flush.
     */
    FLUSH_WRITE_BYTES((byte) 0x4E),

    /**
     * Number of table's properties loaded directly from file, without creating
     * table reader object.
     */
    NUMBER_DIRECT_LOAD_TABLE_PROPERTIES((byte) 0x4F),
    NUMBER_SUPERVERSION_ACQUIRES((byte) 0x50),
    NUMBER_SUPERVERSION_RELEASES((byte) 0x51),
    NUMBER_SUPERVERSION_CLEANUPS((byte) 0x52),

    /**
     * # of compressions/decompressions executed
     */
    NUMBER_BLOCK_COMPRESSED((byte) 0x53),
    NUMBER_BLOCK_DECOMPRESSED((byte) 0x54),

    NUMBER_BLOCK_NOT_COMPRESSED((byte) 0x55),
    MERGE_OPERATION_TOTAL_TIME((byte) 0x56),
    FILTER_OPERATION_TOTAL_TIME((byte) 0x57),

    /**
     * Row cache.
     */
    ROW_CACHE_HIT((byte) 0x58),
    ROW_CACHE_MISS((byte) 0x59),

    /**
     * Read amplification statistics.
     *
     * Read amplification can be calculated using this formula
     * (READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES)
     *
     * REQUIRES: ReadOptions::read_amp_bytes_per_bit to be enabled
     */

    /**
     * Estimate of total bytes actually used.
     */
    READ_AMP_ESTIMATE_USEFUL_BYTES((byte) 0x5A),

    /**
     * Total size of loaded data blocks.
     */
    READ_AMP_TOTAL_READ_BYTES((byte) 0x5B),

    /**
     * Number of refill intervals where rate limiter's bytes are fully consumed.
     */
    NUMBER_RATE_LIMITER_DRAINS((byte) 0x5C),

    TICKER_ENUM_MAX((byte) 0x5D);


    private final byte value;

    TickerType(final byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }
}
