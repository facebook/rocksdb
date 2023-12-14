// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The logical mapping of tickers defined in rocksdb::Tickers.
 * <p>
 * Java byte value mappings don't align 1:1 to the c++ values. c++ rocksdb::Tickers enumeration type
 * is uint32_t and java org.rocksdb.TickerType is byte, this causes mapping issues when
 * rocksdb::Tickers value is greater then 127 (0x7F) for jbyte jni interface as range greater is not
 * available. Without breaking interface in minor versions, value mappings for
 * org.rocksdb.TickerType leverage full byte range [-128 (-0x80), (0x7F)]. Newer tickers added
 * should descend into negative values until TICKER_ENUM_MAX reaches -128 (-0x80).
 */
public enum TickerType {

    /**
     * total block cache misses
     * <p>
     * REQUIRES: BLOCK_CACHE_MISS == BLOCK_CACHE_INDEX_MISS +
     *     BLOCK_CACHE_FILTER_MISS +
     *     BLOCK_CACHE_DATA_MISS;
     */
    BLOCK_CACHE_MISS((byte) 0x0),

    /**
     * total block cache hit
     * <p>
     * REQUIRES: BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT +
     *     BLOCK_CACHE_FILTER_HIT +
     *     BLOCK_CACHE_DATA_HIT;
     */
    BLOCK_CACHE_HIT((byte) 0x1),

    BLOCK_CACHE_ADD((byte) 0x2),

    /**
     * Number of failures when adding blocks to block cache.
     */
    BLOCK_CACHE_ADD_FAILURES((byte) 0x3),

    /**
     * Number of times cache miss when accessing index block from block cache.
     */
    BLOCK_CACHE_INDEX_MISS((byte) 0x4),

    /**
     * Number of times cache hit when accessing index block from block cache.
     */
    BLOCK_CACHE_INDEX_HIT((byte) 0x5),

    /**
     * Number of index blocks added to block cache.
     */
    BLOCK_CACHE_INDEX_ADD((byte) 0x6),

    /**
     * Number of bytes of index blocks inserted into cache
     */
    BLOCK_CACHE_INDEX_BYTES_INSERT((byte) 0x7),

    /**
     * Number of times cache miss when accessing filter block from block cache.
     */
    BLOCK_CACHE_FILTER_MISS((byte) 0x8),

    /**
     * Number of times cache hit when accessing filter block from block cache.
     */
    BLOCK_CACHE_FILTER_HIT((byte) 0x9),

    /**
     * Number of filter blocks added to block cache.
     */
    BLOCK_CACHE_FILTER_ADD((byte) 0xA),

    /**
     * Number of bytes of bloom filter blocks inserted into cache
     */
    BLOCK_CACHE_FILTER_BYTES_INSERT((byte) 0xB),

    /**
     * Number of times cache miss when accessing data block from block cache.
     */
    BLOCK_CACHE_DATA_MISS((byte) 0xC),

    /**
     * Number of times cache hit when accessing data block from block cache.
     */
    BLOCK_CACHE_DATA_HIT((byte) 0xD),

    /**
     * Number of data blocks added to block cache.
     */
    BLOCK_CACHE_DATA_ADD((byte) 0xE),

    /**
     * Number of bytes of data blocks inserted into cache
     */
    BLOCK_CACHE_DATA_BYTES_INSERT((byte) 0xF),

    /**
     * Number of bytes read from cache.
     */
    BLOCK_CACHE_BYTES_READ((byte) 0x10),

    /**
     * Number of bytes written into cache.
     */
    BLOCK_CACHE_BYTES_WRITE((byte) 0x11),

    /**
     * Number of Block cache Compression dictionary misses.
     */
    BLOCK_CACHE_COMPRESSION_DICT_MISS((byte) 0x12),

    /**
     * Number of Block cache Compression dictionary hits.
     */
    BLOCK_CACHE_COMPRESSION_DICT_HIT((byte) 0x13),

    /**
     * Number of Block cache Compression dictionary additions.
     */
    BLOCK_CACHE_COMPRESSION_DICT_ADD((byte) 0x14),

    /**
     * Number of Block cache Compression dictionary bytes inserted.
     */
    BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT((byte) 0x15),

    /**
     * Redundant additions to block cache.
     */
    BLOCK_CACHE_ADD_REDUNDANT((byte) 0x16),

    /**
     * Redundant additions to block cache index.
     */
    BLOCK_CACHE_INDEX_ADD_REDUNDANT((byte) 0x17),

    /**
     * Redundant additions to block cache filter.
     */
    BLOCK_CACHE_FILTER_ADD_REDUNDANT((byte) 0x18),

    /**
     * Redundant additions to block cache data.
     */
    BLOCK_CACHE_DATA_ADD_REDUNDANT((byte) 0x19),

    /**
     * Redundant additions to block cache compression dictionary.
     */
    BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT((byte) 0x1A),

    /**
     * Number of secondary cache hits.
     */
    SECONDARY_CACHE_HITS((byte) 0x1B),

    /**
     * Number of secondary cache filter hits.
     */
    SECONDARY_CACHE_FILTER_HITS((byte) 0x1C),

    /**
     * Number of secondary cache index hits.
     */
    SECONDARY_CACHE_INDEX_HITS((byte) 0x1D),

    /**
     * Number of secondary cache data hits.
     */
    SECONDARY_CACHE_DATA_HITS((byte) 0x1E),

    /**
     * Number of compressed secondary cache dummy hits.
     */
    COMPRESSED_SECONDARY_CACHE_DUMMY_HITS((byte) 0x1F),

    /**
     * Number of compressed secondary cache hits.
     */
    COMPRESSED_SECONDARY_CACHE_HITS((byte) 0x20),

    /**
     * Number of compressed secondary cache promotions.
     */
    COMPRESSED_SECONDARY_CACHE_PROMOTIONS((byte) 0x21),

    /**
     * Number of compressed secondary cache promotion skips.
     */
    COMPRESSED_SECONDARY_CACHE_PROMOTION_SKIPS((byte) 0x22),

    /**
     * Number of times bloom filter has avoided file reads.
     */
    BLOOM_FILTER_USEFUL((byte) 0x23),

    /**
     * Number of times bloom FullFilter has not avoided the reads.
     */
    BLOOM_FILTER_FULL_POSITIVE((byte) 0x24),

    /**
     * Number of times bloom FullFilter has not avoided the reads and data actually
     * exist.
     */
    BLOOM_FILTER_FULL_TRUE_POSITIVE((byte) 0x25),

    /**
     * Number of times bloom was checked before creating iterator on a file.
     */
    BLOOM_FILTER_PREFIX_CHECKED((byte) 0x26),

    /**
     * Number of times it was useful (in avoiding iterator creation) that bloom was checked before creating iterator on a file.
     */
    BLOOM_FILTER_PREFIX_USEFUL((byte) 0x27),

    /**
     * Number of times bloom produced a true positive result.
     */
    BLOOM_FILTER_PREFIX_TRUE_POSITIVE((byte) 0x28),

    /**
     * Number of persistent cache hit
     */
    PERSISTENT_CACHE_HIT((byte) 0x29),

    /**
     * Number of persistent cache miss
     */
    PERSISTENT_CACHE_MISS((byte) 0x2A),

    /**
     * Number of total simulation block cache hits
     */
    SIM_BLOCK_CACHE_HIT((byte) 0x2B),

    /**
     * Number of total simulation block cache misses
     */
    SIM_BLOCK_CACHE_MISS((byte) 0x2C),

    /**
     * Number of memtable hits.
     */
    MEMTABLE_HIT((byte) 0x2D),

    /**
     * Number of of memtable misses.
     */
    MEMTABLE_MISS((byte) 0x2E),

    /**
     * Number of Get() queries served by L0
     */
    GET_HIT_L0((byte) 0x2F),

    /**
     * Number of Get() queries served by L1
     */
    GET_HIT_L1((byte) 0x30),

    /**
     * Number of Get() queries served by L2 and up
     */
    GET_HIT_L2_AND_UP((byte) 0x31),

    /**
     * COMPACTION_KEY_DROP_* count the reasons for key drop during compaction
     * There are 4 reasons currently.
     */

    /**
     * key was written with a newer value.
     */
    COMPACTION_KEY_DROP_NEWER_ENTRY((byte) 0x32),

    /**
     * Also includes keys dropped for range del.
     * The key is obsolete.
     */
    COMPACTION_KEY_DROP_OBSOLETE((byte) 0x33),

    /**
     * key was covered by a range tombstone.
     */
    COMPACTION_KEY_DROP_RANGE_DEL((byte) 0x34),

    /**
     * User compaction function has dropped the key.
     */
    COMPACTION_KEY_DROP_USER((byte) 0x35),

    /**
     * all keys in range were deleted.
     */
    COMPACTION_RANGE_DEL_DROP_OBSOLETE((byte) 0x36),

    /**
     * Deletions obsoleted before bottom level due to file gap optimization.
     */
    COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE((byte) 0x37),

    /**
     * Compactions cancelled to prevent ENOSPC
     */
    COMPACTION_CANCELLED((byte) 0x38),

    /**
     * Number of keys written to the database via the Put and Write call's.
     */
    NUMBER_KEYS_WRITTEN((byte) 0x39),

    /**
     * Number of Keys read.
     */
    NUMBER_KEYS_READ((byte) 0x3A),

    /**
     * Number keys updated, if inplace update is enabled
     */
    NUMBER_KEYS_UPDATED((byte) 0x3B),

    /**
     * The number of uncompressed bytes issued by DB::Put(), DB::Delete(),\
     * DB::Merge(), and DB::Write().
     */
    BYTES_WRITTEN((byte) 0x3C),

    /**
     * The number of uncompressed bytes read from DB::Get().  It could be
     * either from memtables, cache, or table files.
     *
     * For the number of logical bytes read from DB::MultiGet(),
     * please use {@link #NUMBER_MULTIGET_BYTES_READ}.
     */
    BYTES_READ((byte) 0x3D),

    /**
     * The number of calls to seek.
     */
    NUMBER_DB_SEEK((byte) 0x3E),

    /**
     * The number of calls to next.
     */
    NUMBER_DB_NEXT((byte) 0x3F),

    /**
     * The number of calls to prev.
     */
    NUMBER_DB_PREV((byte) 0x40),

    /**
     * The number of calls to seek that returned data.
     */
    NUMBER_DB_SEEK_FOUND((byte) 0x41),

    /**
     * The number of calls to next that returned data.
     */
    NUMBER_DB_NEXT_FOUND((byte) 0x42),

    /**
     * The number of calls to prev that returned data.
     */
    NUMBER_DB_PREV_FOUND((byte) 0x43),

    /**
     * The number of uncompressed bytes read from an iterator.
     * Includes size of key and value.
     */
    ITER_BYTES_READ((byte) 0x44),

    /**
     * Number of internal skipped during iteration
     */
    NUMBER_ITER_SKIP((byte) 0x45),

    /**
     * Number of times we had to reseek inside an iteration to skip
     * over large number of keys with same userkey.
     */
    NUMBER_OF_RESEEKS_IN_ITERATION((byte) 0x46),

    /**
     * Number of iterators created.
     */
    NO_ITERATOR_CREATED((byte) 0x47),

    /**
     * Number of iterators deleted.
     */
    NO_ITERATOR_DELETED((byte) 0x48),

    /**
     * Number of file opens.
     */
    NO_FILE_OPENS((byte) 0x49),

    /**
     * Number of file errors.
     */
    NO_FILE_ERRORS((byte) 0x4A),

    /**
     * Writer has to wait for compaction or flush to finish.
     */
    STALL_MICROS((byte) 0x4B),

    /**
     * The wait time for db mutex.
     *
     * Disabled by default. To enable it set stats level to {@link StatsLevel#ALL}
     */
    DB_MUTEX_WAIT_MICROS((byte) 0x4C),

    /**
     * Number of MultiGet calls.
     */
    NUMBER_MULTIGET_CALLS((byte) 0x4D),

    /**
     * Number of MultiGet keys read.
     */
    NUMBER_MULTIGET_KEYS_READ((byte) 0x4E),

    /**
     * Number of MultiGet bytes read.
     */
    NUMBER_MULTIGET_BYTES_READ((byte) 0x4F),

    /**
     * Number of MultiGet keys found (vs number requested)
     */
    NUMBER_MULTIGET_KEYS_FOUND((byte) 0x50),

    /**
     * Number of Merge failures.
     */
    NUMBER_MERGE_FAILURES((byte) 0x51),

    /**
     * Record the number of calls to {@link RocksDB#getUpdatesSince(long)}. Useful to keep track of
     * transaction log iterator refreshes.
     */
    GET_UPDATES_SINCE_CALLS((byte) 0x52),

    /**
     * Number of times WAL sync is done.
     */
    WAL_FILE_SYNCED((byte) 0x53),

    /**
     * Number of bytes written to WAL.
     */
    WAL_FILE_BYTES((byte) 0x54),

    /**
     * Writes can be processed by requesting thread or by the thread at the
     * head of the writers queue.
     */
    WRITE_DONE_BY_SELF((byte) 0x55),

    /**
     * Equivalent to writes done for others.
     */
    WRITE_DONE_BY_OTHER((byte) 0x56),

    /**
     * Number of Write calls that request WAL.
     */
    WRITE_WITH_WAL((byte) 0x57),

    /**
     * Bytes read during compaction.
     */
    COMPACT_READ_BYTES((byte) 0x58),

    /**
     * Bytes written during compaction.
     */
    COMPACT_WRITE_BYTES((byte) 0x59),

    /**
     * Bytes written during flush.
     */
    FLUSH_WRITE_BYTES((byte) 0x5A),

    /**
     * Compaction read bytes marked.
     */
    COMPACT_READ_BYTES_MARKED((byte) 0x5B),

    /**
     * Compaction read bytes periodically.
     */
    COMPACT_READ_BYTES_PERIODIC((byte) 0x5C),

    /**
     * Compaction read bytes TTL.
     */
    COMPACT_READ_BYTES_TTL((byte) 0x5D),

    /**
     * Compaction write bytes marked.
     */
    COMPACT_WRITE_BYTES_MARKED((byte) 0x5E),

    /**
     * Compaction write bytes periodically.
     */
    COMPACT_WRITE_BYTES_PERIODIC((byte) 0x5F),

    /**
     * Compaction write bytes TTL.
     */
    COMPACT_WRITE_BYTES_TTL((byte) 0x60),

    /**
     * Number of table's properties loaded directly from file, without creating table reader object.
     */
    NUMBER_DIRECT_LOAD_TABLE_PROPERTIES((byte) 0x61),

    /**
     * Number of supervision acquires.
     */
    NUMBER_SUPERVERSION_ACQUIRES((byte) 0x62),

    /**
     * Number of supervision releases.
     */
    NUMBER_SUPERVERSION_RELEASES((byte) 0x63),

    /**
     * Number of supervision cleanups.
     */
    NUMBER_SUPERVERSION_CLEANUPS((byte) 0x64),

    /**
     * Number of compressions executed.
     */
    NUMBER_BLOCK_COMPRESSED((byte) 0x65),

    /**
     * Number of decompressions executed.
     */
    NUMBER_BLOCK_DECOMPRESSED((byte) 0x66),

    /**
     * Number of input bytes (uncompressed) to compression for SST blocks that are stored compressed.
     */
    BYTES_COMPRESSED_FROM((byte) 0x67),

    /**
     * Number of output bytes (compressed) from compression for SST blocks that are stored compressed.
     */
    BYTES_COMPRESSED_TO((byte) 0x68),

    /**
     *  Number of uncompressed bytes for SST blocks that are stored uncompressed because compression type is kNoCompression, or some error case caused compression not to run or produce an output. Index blocks are only counted if enable_index_compression is true.
     */
    BYTES_COMPRESSION_BYPASSED((byte) 0x69),

    /**
     * Number of input bytes (uncompressed) to compression for SST blocks that are stored uncompressed because the compression result was rejected, either because the ratio was not acceptable (see CompressionOptions::max_compressed_bytes_per_kb) or found invalid by the `verify_compression` option.
     */
    BYTES_COMPRESSION_REJECTED((byte) 0x6A),

    /**
     * Like {@link #BYTES_COMPRESSION_BYPASSED} but counting number of blocks.
     */
    NUMBER_BLOCK_COMPRESSION_BYPASSED((byte) 0x6B),

    /**
     * Like {@link #BYTES_COMPRESSION_REJECTED} but counting number of blocks.
     */
    NUMBER_BLOCK_COMPRESSION_REJECTED((byte) 0x6C),

    /**
     * Number of input bytes (compressed) to decompression in reading compressed SST blocks from storage.
     */
    BYTES_DECOMPRESSED_FROM((byte) 0x6D),

    /**
     * Number of output bytes (uncompressed) from decompression in reading compressed SST blocks from storage.
     */
    BYTES_DECOMPRESSED_TO((byte) 0x6E),

    /**
     * Merge operations cumulative time.
     */
    MERGE_OPERATION_TOTAL_TIME((byte) 0x6F),


    /**
     * Filter operations cumulative time.
     */
    FILTER_OPERATION_TOTAL_TIME((byte) 0x70),

    /**
     * Compaction CPU cumulative time.
     */
    COMPACTION_CPU_TOTAL_TIME((byte) 0x71),

    /**
     * Row cache hits.
     */
    ROW_CACHE_HIT((byte) 0x72),

    /**
     * Row cache misses.
     */
    ROW_CACHE_MISS((byte) 0x73),

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
    READ_AMP_ESTIMATE_USEFUL_BYTES((byte) 0x74),

    /**
     * Total size of loaded data blocks.
     */
    READ_AMP_TOTAL_READ_BYTES((byte) 0x75),

    /**
     * Number of refill intervals where rate limiter's bytes are fully consumed.
     */
    NUMBER_RATE_LIMITER_DRAINS((byte) 0x76),

    /**
     * BlobDB specific stats
     * Number of Put/PutTTL/PutUntil to BlobDB.
     */
    BLOB_DB_NUM_PUT((byte) 0x77),

    /**
     * Number of Write to BlobDB.
     */
    BLOB_DB_NUM_WRITE((byte) 0x78),

    /**
     * Number of Get to BlobDB.
     */
    BLOB_DB_NUM_GET((byte) 0x79),

    /**
     * Number of MultiGet to BlobDB.
     */
    BLOB_DB_NUM_MULTIGET((byte) 0x7A),

    /**
     * Number of Seek/SeekToFirst/SeekToLast/SeekForPrev to BlobDB iterator.
     */
    BLOB_DB_NUM_SEEK((byte) 0x7B),

    /**
     * Number of Next to BlobDB iterator.
     */
    BLOB_DB_NUM_NEXT((byte) 0x7C),

    /**
     * Number of Prev to BlobDB iterator.
     */
    BLOB_DB_NUM_PREV((byte) 0x7D),

    /**
     * Number of keys written to BlobDB.
     */
    BLOB_DB_NUM_KEYS_WRITTEN((byte) 0x7E),

    /**
     * Number of keys read from BlobDB.
     */
    BLOB_DB_NUM_KEYS_READ((byte) 0x7F),

    /**
     * Number of bytes (key + value) written to BlobDB.
     */
    BLOB_DB_BYTES_WRITTEN((byte) -0x1),

    /**
     * Number of bytes (keys + value) read from BlobDB.
     */
    BLOB_DB_BYTES_READ((byte) -0x2),

    /**
     * Number of keys written by BlobDB as non-TTL inlined value.
     */
    BLOB_DB_WRITE_INLINED((byte) -0x3),

    /**
     * Number of keys written by BlobDB as TTL inlined value.
     */
    BLOB_DB_WRITE_INLINED_TTL((byte) -0x4),

    /**
     * Number of keys written by BlobDB as non-TTL blob value.
     */
    BLOB_DB_WRITE_BLOB((byte) -0x5),

    /**
     * Number of keys written by BlobDB as TTL blob value.
     */
    BLOB_DB_WRITE_BLOB_TTL((byte) -0x6),

    /**
     * Number of bytes written to blob file.
     */
    BLOB_DB_BLOB_FILE_BYTES_WRITTEN((byte) -0x7),

    /**
     * Number of bytes read from blob file.
     */
    BLOB_DB_BLOB_FILE_BYTES_READ((byte) -0x8),

    /**
     * Number of times a blob files being synced.
     */
    BLOB_DB_BLOB_FILE_SYNCED((byte) -0x9),

    /**
     * Number of blob index evicted from base DB by BlobDB compaction filter because
     * of expiration.
     */
    BLOB_DB_BLOB_INDEX_EXPIRED_COUNT((byte) -0xA),

    /**
     * Size of blob index evicted from base DB by BlobDB compaction filter
     * because of expiration.
     */
    BLOB_DB_BLOB_INDEX_EXPIRED_SIZE((byte) -0xB),

    /**
     * Number of blob index evicted from base DB by BlobDB compaction filter because
     * of corresponding file deleted.
     */
    BLOB_DB_BLOB_INDEX_EVICTED_COUNT((byte) -0xC),

    /**
     * Size of blob index evicted from base DB by BlobDB compaction filter
     * because of corresponding file deleted.
     */
    BLOB_DB_BLOB_INDEX_EVICTED_SIZE((byte) -0xD),

    /**
     * Number of blob files being garbage collected.
     */
    BLOB_DB_GC_NUM_FILES((byte) -0xE),

    /**
     * Number of blob files generated by garbage collection.
     */
    BLOB_DB_GC_NUM_NEW_FILES((byte) -0xF),

    /**
     * Number of BlobDB garbage collection failures.
     */
    BLOB_DB_GC_FAILURES((byte) -0x10),

    /**
     * Number of keys relocated to new blob file by garbage collection.
     */
    BLOB_DB_GC_NUM_KEYS_RELOCATED((byte) -0x11),

    /**
     * Number of bytes relocated to new blob file by garbage collection.
     */
    BLOB_DB_GC_BYTES_RELOCATED((byte) -0x12),

    /**
     * Number of blob files evicted because of BlobDB is full.
     */
    BLOB_DB_FIFO_NUM_FILES_EVICTED((byte) -0x13),

    /**
     * Number of keys in the blob files evicted because of BlobDB is full.
     */
    BLOB_DB_FIFO_NUM_KEYS_EVICTED((byte) -0x14),

    /**
     * Number of bytes in the blob files evicted because of BlobDB is full.
     */
    BLOB_DB_FIFO_BYTES_EVICTED((byte) -0x15),

    /**
     * Number of times cache miss when accessing blob from blob cache.
     */
    BLOB_DB_CACHE_MISS((byte) -0x16),

    /**
     * Number of times cache hit when accessing blob from blob cache.
     */
    BLOB_DB_CACHE_HIT((byte) -0x17),

    /**
     * Number of data blocks added to blob cache.
     */
    BLOB_DB_CACHE_ADD((byte) -0x18),

    /**
     * Number of failures when adding blobs to blob cache.
     */
    BLOB_DB_CACHE_ADD_FAILURES((byte) -0x19),

    /**
     * Number of bytes read from blob cache.
     */
    BLOB_DB_CACHE_BYTES_READ((byte) -0x1A),

    /**
     * Number of bytes written into blob cache.
     */
    BLOB_DB_CACHE_BYTES_WRITE((byte) -0x1B),

    /**
     * These counters indicate a performance issue in WritePrepared transactions.
     * We should not seem them ticking them much.
     * Number of times prepare_mutex_ is acquired in the fast path.
     */
    TXN_PREPARE_MUTEX_OVERHEAD((byte) -0x1C),

    /**
     * Number of times old_commit_map_mutex_ is acquired in the fast path.
     */
    TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD((byte) -0x1D),

    /**
     * Number of times we checked a batch for duplicate keys.
     */
    TXN_DUPLICATE_KEY_OVERHEAD((byte) -0x1E),

    /**
     * Number of times snapshot_mutex_ is acquired in the fast path.
     */
    TXN_SNAPSHOT_MUTEX_OVERHEAD((byte) -0x1F),

    /**
     * Number of times ::Get returned TryAgain due to expired snapshot seq
     */
    TXN_GET_TRY_AGAIN((byte) -0x20),

    /**
     * Number of files marked as trash by delete scheduler
     */
    FILES_MARKED_TRASH((byte) -0x21),

    /**
     * Number of trash files deleted by the background thread from the trash queue
     */
    FILES_DELETED_FROM_TRASH_QUEUE((byte) -0x22),

    /**
     * Number of files deleted immediately by delete scheduler
     */
    FILES_DELETED_IMMEDIATELY((byte) -0x23),

    /**
     * DB error handler statistics
     */
    ERROR_HANDLER_BG_ERROR_COUNT((byte) -0x24),

    /**
     * Number of background errors handled by the error handler.
     */
    ERROR_HANDLER_BG_IO_ERROR_COUNT((byte) -0x25),

    /**
     * Number of retryable background I/O errors handled by the error handler.
     * This is a subset of {@link #ERROR_HANDLER_BG_IO_ERROR_COUNT}.
     */
    ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT((byte) -0x26),

    /**
     * Number of auto resumes handled by the error handler.
     */
    ERROR_HANDLER_AUTORESUME_COUNT((byte) -0x27),

    /**
     * Total Number of auto resume retries handled by the error handler.
     */
    ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT((byte) -0x28),

    /**
     * Number of auto resumes that succeded that were handled by the error handler.
     */
    ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT((byte) -0x29),

    /**
     * Bytes of raw data (payload) found on memtable at flush time.
     * Contains the sum of garbage payload (bytes that are discarded
     * at flush time) and useful payload (bytes of data that will
     * eventually be written to SSTable).
     */
    MEMTABLE_PAYLOAD_BYTES_AT_FLUSH((byte) -0x2A),

    /**
     * Outdated bytes of data present on memtable at flush time.
     */
    MEMTABLE_GARBAGE_BYTES_AT_FLUSH((byte) -0x2B),

    /**
     * Bytes read by `VerifyChecksum()` and `VerifyFileChecksums()` APIs.
     */
    VERIFY_CHECKSUM_READ_BYTES((byte) -0x2C),

    /**
     * Bytes read whilst creating backups.
     */
    BACKUP_READ_BYTES((byte) -0x2D),

    /**
     * Bytes written whilst creating backups.
     */
    BACKUP_WRITE_BYTES((byte) -0x2E),

    /**
     * Remote compaction bytes read.
     */
    REMOTE_COMPACT_READ_BYTES((byte) -0x2F),

    /**
     * Remote compaction bytes written.
     */
    REMOTE_COMPACT_WRITE_BYTES((byte) -0x30),

    /**
     * Bytes read from hot files.
     */
    HOT_FILE_READ_BYTES((byte) -0x31),

    /**
     * Bytes read from warm files.
     */
    WARM_FILE_READ_BYTES((byte) -0x32),

    /**
     * Bytes read from cool files.
     */
    COOL_FILE_READ_BYTES((byte) -0x5B),

    /**
     * Bytes read from cold files.
     */
    COLD_FILE_READ_BYTES((byte) -0x33),

    /**
     * Bytes read from ice cold files.
     */
    ICE_FILE_READ_BYTES((byte) -0x59),

    /**
     * Numer of reads from hot files.
     */
    HOT_FILE_READ_COUNT((byte) -0x34),

    /**
     * Numer of reads from warm files.
     */
    WARM_FILE_READ_COUNT((byte) -0x35),

    /**
     * Numer of reads from cool files.
     */
    COOL_FILE_READ_COUNT((byte) -0x5C),

    /**
     * Numer of reads from cold files.
     */
    COLD_FILE_READ_COUNT((byte) -0x36),

    /**
     * Numer of reads from ice cold files.
     */
    ICE_FILE_READ_COUNT((byte) -0x5A),

    /**
     * Bytes read from the last level.
     */
    LAST_LEVEL_READ_BYTES((byte) -0x37),

    /**
     * Number of reads from the last level.
     */
    LAST_LEVEL_READ_COUNT((byte) -0x38),

    /**
     * Bytes read from the non-last level.
     */
    NON_LAST_LEVEL_READ_BYTES((byte) -0x39),

    /**
     * Number of reads from the non-last level.
     */
    NON_LAST_LEVEL_READ_COUNT((byte) -0x3A),

    /**
     * Statistics on iterator Seek() (and variants) for each sorted run.
     * i.e a  single user Seek() can result in many sorted run Seek()s.
     * The stats are split between last level and non-last level.
     * Filtered: a filter such as prefix Bloom filter indicate the Seek() would
     * not find anything relevant, so avoided a likely access to data+index
     * blocks.
     */

    LAST_LEVEL_SEEK_FILTERED((byte) -0x3B),
    /**
     * Filter match: a filter such as prefix Bloom filter was queried but did
     * not filter out the seek.
     */
    LAST_LEVEL_SEEK_FILTER_MATCH((byte) -0x3C),

    /**
     * At least one data block was accessed for a Seek() (or variant) on a
     * sorted run.
     */
    LAST_LEVEL_SEEK_DATA((byte) -0x3D),

    /**
     * At least one value() was accessed for the seek (suggesting it was useful),
     * and no filter such as prefix Bloom was queried.
     */
    LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER((byte) -0x3E),

    /**
     * At least one value() was accessed for the seek (suggesting it was useful),
     * after querying a filter such as prefix Bloom.
     */
    LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH((byte) -0x3F),

    /**
     * Similar to {@link #LAST_LEVEL_SEEK_FILTERED} but for the non-last level.
     */
    NON_LAST_LEVEL_SEEK_FILTERED((byte) -0x40),

    /**
     * Similar to {@link #LAST_LEVEL_SEEK_FILTER_MATCH} but for the non-last level.
     */
    NON_LAST_LEVEL_SEEK_FILTER_MATCH((byte) -0x41),

    /**
     * Similar to {@link #LAST_LEVEL_SEEK_DATA} but for the non-last level.
     */
    NON_LAST_LEVEL_SEEK_DATA((byte) -0x42),

    /**
     * Similar to {@link #LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER} but for the non-last level.
     */
    NON_LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER((byte) -0x43),

    /**
     * Similar to {@link #LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH} but for the non-last level.
     */
    NON_LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH((byte) -0x44),

    /**
     * Number of block checksum verifications.
     */
    BLOCK_CHECKSUM_COMPUTE_COUNT((byte) -0x45),

    /**
     * Number of times RocksDB detected a corruption while verifying a block
     * checksum. RocksDB does not remember corruptions that happened during user
     * reads so the same block corruption may be detected multiple times.
     */
    BLOCK_CHECKSUM_MISMATCH_COUNT((byte) -0x46),

    /**
     * Number of multiget co-rountines.
     */
    MULTIGET_COROUTINE_COUNT((byte) -0x47),

    /**
     * Time spent in the ReadAsync file system call.
     */
    READ_ASYNC_MICROS((byte) -0x48),

    /**
     * Number of errors returned to the async read callback.
     */
    ASYNC_READ_ERROR_COUNT((byte) -0x49),

    /**
     * Number of lookup into the prefetched tail (see
     * `TABLE_OPEN_PREFETCH_TAIL_READ_BYTES`)
     * that can't find its data for table open
     */
    TABLE_OPEN_PREFETCH_TAIL_MISS((byte) -0x4A),

    /**
     * Number of lookup into the prefetched tail (see
     * `TABLE_OPEN_PREFETCH_TAIL_READ_BYTES`)
     * that finds its data for table open
     */
    TABLE_OPEN_PREFETCH_TAIL_HIT((byte) -0x4B),

    /**
     * Number of times timestamps are checked on accessing the table
     */
    TIMESTAMP_FILTER_TABLE_CHECKED((byte) -0x4C),

    /**
     * Number of times timestamps can successfully help skip the table access
     */
    TIMESTAMP_FILTER_TABLE_FILTERED((byte) -0x4D),

    /**
     * Number of times readahead is trimmed during scans when ReadOptions.auto_readahead_size is set.
     */
    READAHEAD_TRIMMED((byte) -0x4E),

    /**
     * Maximum size of the FIFO compactions.
     */
    FIFO_MAX_SIZE_COMPACTIONS((byte) -0x4F),

    /**
     * TTL of the FIFO compactions.
     */
    FIFO_TTL_COMPACTIONS((byte) -0x50),

    /**
     * Change temperature of the FIFO compactions.
     */
    FIFO_CHANGE_TEMPERATURE_COMPACTIONS((byte) -0x58),

    /**
     * Number of bytes prefetched during user initiated scan.
     */
    PREFETCH_BYTES((byte) -0x51),

    /**
     * Number of prefetched bytes that were actually useful during user initiated scan.
     */
    PREFETCH_BYTES_USEFUL((byte) -0x52),

    /**
     * Number of FS reads avoided due to prefetching during user initiated scan.
     */
    PREFETCH_HITS((byte) -0x53),

    /**
     * Footer corruption detected when opening an SST file for reading.
     */
    SST_FOOTER_CORRUPTION_COUNT((byte) -0x55),

    /**
     * Counters for file read retries with the verify_and_reconstruct_read file system option after detecting a checksum mismatch.
     */
    FILE_READ_CORRUPTION_RETRY_COUNT((byte) -0x56),

    /**
     * Counters for file read retries with the verify_and_reconstruct_read file system option after detecting a checksum mismatch.
     */
    FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT((byte) -0x57),

    /**
     * Counter for the number of times a WBWI is ingested into the DB. This
     * happens when IngestWriteBatchWithIndex() is used and when large
     * transaction optimization is enabled through
     * TransactionOptions::large_txn_commit_optimize_threshold.
     */
    NUMBER_WBWI_INGEST((byte) -0x5D),

    /**
     * Failure to load the UDI during SST table open.
     */
    SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT((byte) -0x5E),

    /**
     * Bytes of output files successfully resumed during remote compaction.
     */
    REMOTE_COMPACT_RESUMED_BYTES((byte) -0x5F),

    /**
     * MultiScan statistics
     */

    /**
     * Number of calls to Iterator::Prepare() for multi-scan.
     */
    MULTISCAN_PREPARE_CALLS((byte) -0x60),

    /**
     * Number of errors during Iterator::Prepare() for multi-scan.
     */
    MULTISCAN_PREPARE_ERRORS((byte) -0x61),

    /**
     * Number of data blocks prefetched during multi-scan Prepare().
     */
    MULTISCAN_BLOCKS_PREFETCHED((byte) -0x62),

    /**
     * Number of data blocks found in cache during multi-scan Prepare().
     */
    MULTISCAN_BLOCKS_FROM_CACHE((byte) -0x63),

    /**
     * Total bytes prefetched during multi-scan Prepare().
     */
    MULTISCAN_PREFETCH_BYTES((byte) -0x64),

    /**
     * Number of prefetched blocks that were never accessed (wasted).
     */
    MULTISCAN_PREFETCH_BLOCKS_WASTED((byte) -0x65),

    /**
     * Number of I/O requests issued during multi-scan Prepare().
     */
    MULTISCAN_IO_REQUESTS((byte) -0x66),

    /**
     * Number of non-adjacent blocks coalesced into single I/O request.
     */
    MULTISCAN_IO_COALESCED_NONADJACENT((byte) -0x67),

    /**
     * Number of seek errors during multi-scan iteration.
     */
    MULTISCAN_SEEK_ERRORS((byte) -0x68),

    /**
     * Maximum number of ticker types.
     */
    TICKER_ENUM_MAX((byte) -0x54);

    private final byte value;

    TickerType(final byte value) {
        this.value = value;
    }

    /**
     * Returns the byte value of the enumerations value
     *
     * @return byte representation
     */
    public byte getValue() {
        return value;
    }

    /**
     * Get Ticker type by byte value.
     *
     * @param value byte representation of TickerType.
     *
     * @return {@link org.rocksdb.TickerType} instance.
     * @throws java.lang.IllegalArgumentException if an invalid
     *     value is provided.
     */
    public static TickerType getTickerType(final byte value) {
        for (final TickerType tickerType : TickerType.values()) {
            if (tickerType.getValue() == value) {
                return tickerType;
            }
        }
        throw new IllegalArgumentException(
            "Illegal value provided for TickerType.");
    }
}
