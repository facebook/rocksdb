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
    BLOCK_CACHE_MISS((char) 0x0),

    /**
     * total block cache hit
     *
     * REQUIRES: BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT +
     *     BLOCK_CACHE_FILTER_HIT +
     *     BLOCK_CACHE_DATA_HIT;
     */
    BLOCK_CACHE_HIT((char) 0x1),

    BLOCK_CACHE_ADD((char) 0x2),

    /**
     * # of failures when adding blocks to block cache.
     */
    BLOCK_CACHE_ADD_FAILURES((char) 0x3),

    /**
     * # of times cache miss when accessing index block from block cache.
     */
    BLOCK_CACHE_INDEX_MISS((char) 0x4),

    /**
     * # of times cache hit when accessing index block from block cache.
     */
    BLOCK_CACHE_INDEX_HIT((char) 0x5),

    /**
     * # of index blocks added to block cache.
     */
    BLOCK_CACHE_INDEX_ADD((char) 0x6),

    /**
     * # of bytes of index blocks inserted into cache
     */
    BLOCK_CACHE_INDEX_BYTES_INSERT((char) 0x7),

    /**
     * # of bytes of index block erased from cache
     */
    BLOCK_CACHE_INDEX_BYTES_EVICT((char) 0x8),

    /**
     * # of times cache miss when accessing filter block from block cache.
     */
    BLOCK_CACHE_FILTER_MISS((char) 0x9),

    /**
     * # of times cache hit when accessing filter block from block cache.
     */
    BLOCK_CACHE_FILTER_HIT((char) 0xA),

    /**
     * # of filter blocks added to block cache.
     */
    BLOCK_CACHE_FILTER_ADD((char) 0xB),

    /**
     * # of bytes of bloom filter blocks inserted into cache
     */
    BLOCK_CACHE_FILTER_BYTES_INSERT((char) 0xC),

    /**
     * # of bytes of bloom filter block erased from cache
     */
    BLOCK_CACHE_FILTER_BYTES_EVICT((char) 0xD),

    /**
     * # of times cache miss when accessing data block from block cache.
     */
    BLOCK_CACHE_DATA_MISS((char) 0xE),

    /**
     * # of times cache hit when accessing data block from block cache.
     */
    BLOCK_CACHE_DATA_HIT((char) 0xF),

    /**
     * # of data blocks added to block cache.
     */
    BLOCK_CACHE_DATA_ADD((char) 0x10),

    /**
     * # of bytes of data blocks inserted into cache
     */
    BLOCK_CACHE_DATA_BYTES_INSERT((char) 0x11),

    /**
     * # of bytes read from cache.
     */
    BLOCK_CACHE_BYTES_READ((char) 0x12),

    /**
     * # of bytes written into cache.
     */
    BLOCK_CACHE_BYTES_WRITE((char) 0x13),

    /**
     * # of times bloom filter has avoided file reads.
     */
    BLOOM_FILTER_USEFUL((char) 0x14),

    /**
     * # persistent cache hit
     */
    PERSISTENT_CACHE_HIT((char) 0x15),

    /**
     * # persistent cache miss
     */
    PERSISTENT_CACHE_MISS((char) 0x16),

    /**
     * # total simulation block cache hits
     */
    SIM_BLOCK_CACHE_HIT((char) 0x17),

    /**
     * # total simulation block cache misses
     */
    SIM_BLOCK_CACHE_MISS((char) 0x18),

    /**
     * # of memtable hits.
     */
    MEMTABLE_HIT((char) 0x19),

    /**
     * # of memtable misses.
     */
    MEMTABLE_MISS((char) 0x1A),

    /**
     * # of Get() queries served by L0
     */
    GET_HIT_L0((char) 0x1B),

    /**
     * # of Get() queries served by L1
     */
    GET_HIT_L1((char) 0x1C),

    /**
     * # of Get() queries served by L2 and up
     */
    GET_HIT_L2_AND_UP((char) 0x1D),

    /**
     * COMPACTION_KEY_DROP_* count the reasons for key drop during compaction
     * There are 4 reasons currently.
     */

    /**
     * key was written with a newer value.
     */
    COMPACTION_KEY_DROP_NEWER_ENTRY((char) 0x1E),

    /**
     * Also includes keys dropped for range del.
     * The key is obsolete.
     */
    COMPACTION_KEY_DROP_OBSOLETE((char) 0x1F),

    /**
     * key was covered by a range tombstone.
     */
    COMPACTION_KEY_DROP_RANGE_DEL((char) 0x20),

    /**
     * User compaction function has dropped the key.
     */
    COMPACTION_KEY_DROP_USER((char) 0x21),

    /**
     * all keys in range were deleted.
     */
    COMPACTION_RANGE_DEL_DROP_OBSOLETE((char) 0x22),

    /**
     * Number of keys written to the database via the Put and Write call's.
     */
    NUMBER_KEYS_WRITTEN((char) 0x23),

    /**
     * Number of Keys read.
     */
    NUMBER_KEYS_READ((char) 0x24),

    /**
     * Number keys updated, if inplace update is enabled
     */
    NUMBER_KEYS_UPDATED((char) 0x25),

    /**
     * The number of uncompressed bytes issued by DB::Put(), DB::Delete(),\
     * DB::Merge(), and DB::Write().
     */
    BYTES_WRITTEN((char) 0x26),

    /**
     * The number of uncompressed bytes read from DB::Get().  It could be
     * either from memtables, cache, or table files.
     *
     * For the number of logical bytes read from DB::MultiGet(),
     * please use {@link #NUMBER_MULTIGET_BYTES_READ}.
     */
    BYTES_READ((char) 0x27),

    /**
     * The number of calls to seek.
     */
    NUMBER_DB_SEEK((char) 0x28),

    /**
     * The number of calls to next.
     */
    NUMBER_DB_NEXT((char) 0x29),

    /**
     * The number of calls to prev.
     */
    NUMBER_DB_PREV((char) 0x2A),

    /**
     * The number of calls to seek that returned data.
     */
    NUMBER_DB_SEEK_FOUND((char) 0x2B),

    /**
     * The number of calls to next that returned data.
     */
    NUMBER_DB_NEXT_FOUND((char) 0x2C),

    /**
     * The number of calls to prev that returned data.
     */
    NUMBER_DB_PREV_FOUND((char) 0x2D),

    /**
     * The number of uncompressed bytes read from an iterator.
     * Includes size of key and value.
     */
    ITER_BYTES_READ((char) 0x2E),

    NO_FILE_CLOSES((char) 0x2F),

    NO_FILE_OPENS((char) 0x30),

    NO_FILE_ERRORS((char) 0x31),

    /**
     * Time system had to wait to do LO-L1 compactions.
     *
     * @deprecated
     */
    @Deprecated
    STALL_L0_SLOWDOWN_MICROS((char) 0x32),

    /**
     * Time system had to wait to move memtable to L1.
     *
     * @deprecated
     */
    @Deprecated
    STALL_MEMTABLE_COMPACTION_MICROS((char) 0x33),

    /**
     * write throttle because of too many files in L0.
     *
     * @deprecated
     */
    @Deprecated
    STALL_L0_NUM_FILES_MICROS((char) 0x34),

    /**
     * Writer has to wait for compaction or flush to finish.
     */
    STALL_MICROS((char) 0x35),

    /**
     * The wait time for db mutex.
     *
     * Disabled by default. To enable it set stats level to {@link StatsLevel#ALL}
     */
    DB_MUTEX_WAIT_MICROS((char) 0x36),

    RATE_LIMIT_DELAY_MILLIS((char) 0x37),

    /**
     * Number of iterators created.
     *
     */
    NO_ITERATORS((char) 0x38),

    /**
     * Number of MultiGet calls.
     */
    NUMBER_MULTIGET_CALLS((char) 0x39),

    /**
     * Number of MultiGet keys read.
     */
    NUMBER_MULTIGET_KEYS_READ((char) 0x3A),

    /**
     * Number of MultiGet bytes read.
     */
    NUMBER_MULTIGET_BYTES_READ((char) 0x3B),

    /**
     * Number of deletes records that were not required to be
     * written to storage because key does not exist.
     */
    NUMBER_FILTERED_DELETES((char) 0x3C),
    NUMBER_MERGE_FAILURES((char) 0x3D),

    /**
     * Number of times bloom was checked before creating iterator on a
     * file, and the number of times the check was useful in avoiding
     * iterator creation (and thus likely IOPs).
     */
    BLOOM_FILTER_PREFIX_CHECKED((char) 0x3E),
    BLOOM_FILTER_PREFIX_USEFUL((char) 0x3F),

    /**
     * Number of times we had to reseek inside an iteration to skip
     * over large number of keys with same userkey.
     */
    NUMBER_OF_RESEEKS_IN_ITERATION((char) 0x40),

    /**
     * Record the number of calls to {@link RocksDB#getUpdatesSince(long)}. Useful to keep track of
     * transaction log iterator refreshes.
     */
    GET_UPDATES_SINCE_CALLS((char) 0x41),

    /**
     * Miss in the compressed block cache.
     */
    BLOCK_CACHE_COMPRESSED_MISS((char) 0x42),

    /**
     * Hit in the compressed block cache.
     */
    BLOCK_CACHE_COMPRESSED_HIT((char) 0x43),

    /**
     * Number of blocks added to compressed block cache.
     */
    BLOCK_CACHE_COMPRESSED_ADD((char) 0x44),

    /**
     * Number of failures when adding blocks to compressed block cache.
     */
    BLOCK_CACHE_COMPRESSED_ADD_FAILURES((char) 0x45),

    /**
     * Number of times WAL sync is done.
     */
    WAL_FILE_SYNCED((char) 0x46),

    /**
     * Number of bytes written to WAL.
     */
    WAL_FILE_BYTES((char) 0x47),

    /**
     * Writes can be processed by requesting thread or by the thread at the
     * head of the writers queue.
     */
    WRITE_DONE_BY_SELF((char) 0x48),

    /**
     * Equivalent to writes done for others.
     */
    WRITE_DONE_BY_OTHER((char) 0x49),

    /**
     * Number of writes ending up with timed-out.
     */
    WRITE_TIMEDOUT((char) 0x4A),

    /**
     * Number of Write calls that request WAL.
     */
    WRITE_WITH_WAL((char) 0x4B),

    /**
     * Bytes read during compaction.
     */
    COMPACT_READ_BYTES((char) 0x4C),

    /**
     * Bytes written during compaction.
     */
    COMPACT_WRITE_BYTES((char) 0x4D),

    /**
     * Bytes written during flush.
     */
    FLUSH_WRITE_BYTES((char) 0x4E),

    /**
     * Number of table's properties loaded directly from file, without creating
     * table reader object.
     */
    NUMBER_DIRECT_LOAD_TABLE_PROPERTIES((char) 0x4F),
    NUMBER_SUPERVERSION_ACQUIRES((char) 0x50),
    NUMBER_SUPERVERSION_RELEASES((char) 0x51),
    NUMBER_SUPERVERSION_CLEANUPS((char) 0x52),

    /**
     * # of compressions/decompressions executed
     */
    NUMBER_BLOCK_COMPRESSED((char) 0x53),
    NUMBER_BLOCK_DECOMPRESSED((char) 0x54),

    NUMBER_BLOCK_NOT_COMPRESSED((char) 0x55),
    MERGE_OPERATION_TOTAL_TIME((char) 0x56),
    FILTER_OPERATION_TOTAL_TIME((char) 0x57),

    /**
     * Row cache.
     */
    ROW_CACHE_HIT((char) 0x58),
    ROW_CACHE_MISS((char) 0x59),

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
    READ_AMP_ESTIMATE_USEFUL_BYTES((char) 0x5A),

    /**
     * Total size of loaded data blocks.
     */
    READ_AMP_TOTAL_READ_BYTES((char) 0x5B),

    /**
     * Number of refill intervals where rate limiter's bytes are fully consumed.
     */
    NUMBER_RATE_LIMITER_DRAINS((char) 0x5C),

    /**
     * Number of internal skipped during iteration
     */
    NUMBER_ITER_SKIP((char) 0x5D),

    /**
     * Number of MultiGet keys found (vs number requested)
     */
    NUMBER_MULTIGET_KEYS_FOUND((char) 0x5E),

    /**
     * Number of iterators created.
     */
    NO_ITERATOR_CREATED((char) 0x5F),

    /**
     * Number of iterators deleted.
     */
    NO_ITERATOR_DELETED((char) 0x60),

    /**
     * Deletions obsoleted before bottom level due to file gap optimization.
     */
    COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE((char) 0x61),

    /**
     * If a compaction was cancelled in sfm to prevent ENOSPC
     */
    COMPACTION_CANCELLED((char) 0x62),

    /**
     * # of times bloom FullFilter has not avoided the reads.
     */
    BLOOM_FILTER_FULL_POSITIVE((char) 0x63),

    /**
     * # of times bloom FullFilter has not avoided the reads and data actually
     * exist.
     */
    BLOOM_FILTER_FULL_TRUE_POSITIVE((char) 0x64),

    /**
     * BlobDB specific stats
     * # of Put/PutTTL/PutUntil to BlobDB.
     */
    BLOB_DB_NUM_PUT((char) 0x65),

    /**
     * # of Write to BlobDB.
     */
    BLOB_DB_NUM_WRITE((char) 0x66),

    /**
     * # of Get to BlobDB.
     */
    BLOB_DB_NUM_GET((char) 0x67),

    /**
     * # of MultiGet to BlobDB.
     */
    BLOB_DB_NUM_MULTIGET((char) 0x68),

    /**
     * # of Seek/SeekToFirst/SeekToLast/SeekForPrev to BlobDB iterator.
     */
    BLOB_DB_NUM_SEEK((char) 0x69),

    /**
     * # of Next to BlobDB iterator.
     */
    BLOB_DB_NUM_NEXT((char) 0x6A),

    /**
     * # of Prev to BlobDB iterator.
     */
    BLOB_DB_NUM_PREV((char) 0x6B),

    /**
     * # of keys written to BlobDB.
     */
    BLOB_DB_NUM_KEYS_WRITTEN((char) 0x6C),

    /**
     * # of keys read from BlobDB.
     */
    BLOB_DB_NUM_KEYS_READ((char) 0x6D),

    /**
     * # of bytes (key + value) written to BlobDB.
     */
    BLOB_DB_BYTES_WRITTEN((char) 0x6E),

    /**
     * # of bytes (keys + value) read from BlobDB.
     */
    BLOB_DB_BYTES_READ((char) 0x6F),

    /**
     * # of keys written by BlobDB as non-TTL inlined value.
     */
    BLOB_DB_WRITE_INLINED((char) 0x70),

    /**
     * # of keys written by BlobDB as TTL inlined value.
     */
    BLOB_DB_WRITE_INLINED_TTL((char) 0x71),

    /**
     * # of keys written by BlobDB as non-TTL blob value.
     */
    BLOB_DB_WRITE_BLOB((char) 0x72),

    /**
     * # of keys written by BlobDB as TTL blob value.
     */
    BLOB_DB_WRITE_BLOB_TTL((char) 0x73),

    /**
     * # of bytes written to blob file.
     */
    BLOB_DB_BLOB_FILE_BYTES_WRITTEN((char) 0x74),

    /**
     * # of bytes read from blob file.
     */
    BLOB_DB_BLOB_FILE_BYTES_READ((char) 0x75),

    /**
     * # of times a blob files being synced.
     */
    BLOB_DB_BLOB_FILE_SYNCED((char) 0x76),

    /**
     * # of blob index evicted from base DB by BlobDB compaction filter because
     * of expiration.
     */
    BLOB_DB_BLOB_INDEX_EXPIRED_COUNT((char) 0x77),

    /**
     * Size of blob index evicted from base DB by BlobDB compaction filter
     * because of expiration.
     */
    BLOB_DB_BLOB_INDEX_EXPIRED_SIZE((char) 0x78),

    /**
     * # of blob index evicted from base DB by BlobDB compaction filter because
     * of corresponding file deleted.
     */
    BLOB_DB_BLOB_INDEX_EVICTED_COUNT((char) 0x79),

    /**
     * Size of blob index evicted from base DB by BlobDB compaction filter
     * because of corresponding file deleted.
     */
    BLOB_DB_BLOB_INDEX_EVICTED_SIZE((char) 0x7A),

    /**
     * # of blob files being garbage collected.
     */
    BLOB_DB_GC_NUM_FILES((char) 0x7B),

    /**
     * # of blob files generated by garbage collection.
     */
    BLOB_DB_GC_NUM_NEW_FILES((char) 0x7C),

    /**
     * # of BlobDB garbage collection failures.
     */
    BLOB_DB_GC_FAILURES((char) 0x7D),

    /**
     * # of keys drop by BlobDB garbage collection because they had been
     * overwritten.
     */
    BLOB_DB_GC_NUM_KEYS_OVERWRITTEN((char) 0x7E),

    /**
     * # of keys drop by BlobDB garbage collection because of expiration.
     */
    BLOB_DB_GC_NUM_KEYS_EXPIRED((char) 0x7F),

    /**
     * # of keys relocated to new blob file by garbage collection.
     */
    BLOB_DB_GC_NUM_KEYS_RELOCATED((char) 0x80),

    /**
     * # of bytes drop by BlobDB garbage collection because they had been
     * overwritten.
     */
    BLOB_DB_GC_BYTES_OVERWRITTEN((char) 0x81),

    /**
     * # of bytes drop by BlobDB garbage collection because of expiration.
     */
    BLOB_DB_GC_BYTES_EXPIRED((char) 0x82),

    /**
     * # of bytes relocated to new blob file by garbage collection.
     */
    BLOB_DB_GC_BYTES_RELOCATED((char) 0x83),

    /**
     * # of blob files evicted because of BlobDB is full.
     */
    BLOB_DB_FIFO_NUM_FILES_EVICTED((char) 0x84),

    /**
     * # of keys in the blob files evicted because of BlobDB is full.
     */
    BLOB_DB_FIFO_NUM_KEYS_EVICTED((char) 0x85),

    /**
     * # of bytes in the blob files evicted because of BlobDB is full.
     */
    BLOB_DB_FIFO_BYTES_EVICTED((char) 0x86),

    /**
     * These counters indicate a performance issue in WritePrepared transactions.
     * We should not seem them ticking them much.
     * # of times prepare_mutex_ is acquired in the fast path.
     */
    TXN_PREPARE_MUTEX_OVERHEAD((char) 0x87),

    /**
     * # of times old_commit_map_mutex_ is acquired in the fast path.
     */
    TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD((char) 0x88),

    /**
     * # of times we checked a batch for duplicate keys.
     */
    TXN_DUPLICATE_KEY_OVERHEAD((char) 0x89),

    /**
     * # of times snapshot_mutex_ is acquired in the fast path.
     */
    TXN_SNAPSHOT_MUTEX_OVERHEAD((char) 0x8A),

    TICKER_ENUM_MAX((char) 0x8B);

    private final char value;

    TickerType(final char value) {
        this.value = value;
    }

    public char getValue() {
        return value;
    }
}
