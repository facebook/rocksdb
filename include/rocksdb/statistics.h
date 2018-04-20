// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef STORAGE_ROCKSDB_INCLUDE_STATISTICS_H_
#define STORAGE_ROCKSDB_INCLUDE_STATISTICS_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <memory>
#include <vector>

#include "rocksdb/status.h"

namespace rocksdb {

/**
 * Keep adding ticker's here.
 *  1. Any ticker should be added before TICKER_ENUM_MAX.
 *  2. Add a readable string in TickersNameMap below for the newly added ticker.
 *  3. Add a corresponding enum value to TickerType.java in the java API
 */
enum Tickers : uint32_t {
  // total block cache misses
  // REQUIRES: BLOCK_CACHE_MISS == BLOCK_CACHE_INDEX_MISS +
  //                               BLOCK_CACHE_FILTER_MISS +
  //                               BLOCK_CACHE_DATA_MISS;
  BLOCK_CACHE_MISS = 0,
  // total block cache hit
  // REQUIRES: BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT +
  //                              BLOCK_CACHE_FILTER_HIT +
  //                              BLOCK_CACHE_DATA_HIT;
  BLOCK_CACHE_HIT,
  // # of blocks added to block cache.
  BLOCK_CACHE_ADD,
  // # of failures when adding blocks to block cache.
  BLOCK_CACHE_ADD_FAILURES,
  // # of times cache miss when accessing index block from block cache.
  BLOCK_CACHE_INDEX_MISS,
  // # of times cache hit when accessing index block from block cache.
  BLOCK_CACHE_INDEX_HIT,
  // # of index blocks added to block cache.
  BLOCK_CACHE_INDEX_ADD,
  // # of bytes of index blocks inserted into cache
  BLOCK_CACHE_INDEX_BYTES_INSERT,
  // # of bytes of index block erased from cache
  BLOCK_CACHE_INDEX_BYTES_EVICT,
  // # of times cache miss when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_MISS,
  // # of times cache hit when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_HIT,
  // # of filter blocks added to block cache.
  BLOCK_CACHE_FILTER_ADD,
  // # of bytes of bloom filter blocks inserted into cache
  BLOCK_CACHE_FILTER_BYTES_INSERT,
  // # of bytes of bloom filter block erased from cache
  BLOCK_CACHE_FILTER_BYTES_EVICT,
  // # of times cache miss when accessing data block from block cache.
  BLOCK_CACHE_DATA_MISS,
  // # of times cache hit when accessing data block from block cache.
  BLOCK_CACHE_DATA_HIT,
  // # of data blocks added to block cache.
  BLOCK_CACHE_DATA_ADD,
  // # of bytes of data blocks inserted into cache
  BLOCK_CACHE_DATA_BYTES_INSERT,
  // # of bytes read from cache.
  BLOCK_CACHE_BYTES_READ,
  // # of bytes written into cache.
  BLOCK_CACHE_BYTES_WRITE,

  // # of times bloom filter has avoided file reads, i.e., negatives.
  BLOOM_FILTER_USEFUL,
  // # of times bloom FullFilter has not avoided the reads.
  BLOOM_FILTER_FULL_POSITIVE,
  // # of times bloom FullFilter has not avoided the reads and data actually
  // exist.
  BLOOM_FILTER_FULL_TRUE_POSITIVE,

  // # persistent cache hit
  PERSISTENT_CACHE_HIT,
  // # persistent cache miss
  PERSISTENT_CACHE_MISS,

  // # total simulation block cache hits
  SIM_BLOCK_CACHE_HIT,
  // # total simulation block cache misses
  SIM_BLOCK_CACHE_MISS,

  // # of memtable hits.
  MEMTABLE_HIT,
  // # of memtable misses.
  MEMTABLE_MISS,

  // # of Get() queries served by L0
  GET_HIT_L0,
  // # of Get() queries served by L1
  GET_HIT_L1,
  // # of Get() queries served by L2 and up
  GET_HIT_L2_AND_UP,

  /**
   * COMPACTION_KEY_DROP_* count the reasons for key drop during compaction
   * There are 4 reasons currently.
   */
  COMPACTION_KEY_DROP_NEWER_ENTRY,  // key was written with a newer value.
                                    // Also includes keys dropped for range del.
  COMPACTION_KEY_DROP_OBSOLETE,     // The key is obsolete.
  COMPACTION_KEY_DROP_RANGE_DEL,    // key was covered by a range tombstone.
  COMPACTION_KEY_DROP_USER,  // user compaction function has dropped the key.
  COMPACTION_RANGE_DEL_DROP_OBSOLETE,  // all keys in range were deleted.
  // Deletions obsoleted before bottom level due to file gap optimization.
  COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,
  // If a compaction was cancelled in sfm to prevent ENOSPC
  COMPACTION_CANCELLED,

  // Number of keys written to the database via the Put and Write call's
  NUMBER_KEYS_WRITTEN,
  // Number of Keys read,
  NUMBER_KEYS_READ,
  // Number keys updated, if inplace update is enabled
  NUMBER_KEYS_UPDATED,
  // The number of uncompressed bytes issued by DB::Put(), DB::Delete(),
  // DB::Merge(), and DB::Write().
  BYTES_WRITTEN,
  // The number of uncompressed bytes read from DB::Get().  It could be
  // either from memtables, cache, or table files.
  // For the number of logical bytes read from DB::MultiGet(),
  // please use NUMBER_MULTIGET_BYTES_READ.
  BYTES_READ,
  // The number of calls to seek/next/prev
  NUMBER_DB_SEEK,
  NUMBER_DB_NEXT,
  NUMBER_DB_PREV,
  // The number of calls to seek/next/prev that returned data
  NUMBER_DB_SEEK_FOUND,
  NUMBER_DB_NEXT_FOUND,
  NUMBER_DB_PREV_FOUND,
  // The number of uncompressed bytes read from an iterator.
  // Includes size of key and value.
  ITER_BYTES_READ,
  NO_FILE_CLOSES,
  NO_FILE_OPENS,
  NO_FILE_ERRORS,
  // DEPRECATED Time system had to wait to do LO-L1 compactions
  STALL_L0_SLOWDOWN_MICROS,
  // DEPRECATED Time system had to wait to move memtable to L1.
  STALL_MEMTABLE_COMPACTION_MICROS,
  // DEPRECATED write throttle because of too many files in L0
  STALL_L0_NUM_FILES_MICROS,
  // Writer has to wait for compaction or flush to finish.
  STALL_MICROS,
  // The wait time for db mutex.
  // Disabled by default. To enable it set stats level to kAll
  DB_MUTEX_WAIT_MICROS,
  RATE_LIMIT_DELAY_MILLIS,
  NO_ITERATORS,  // number of iterators currently open

  // Number of MultiGet calls, keys read, and bytes read
  NUMBER_MULTIGET_CALLS,
  NUMBER_MULTIGET_KEYS_READ,
  NUMBER_MULTIGET_BYTES_READ,

  // Number of deletes records that were not required to be
  // written to storage because key does not exist
  NUMBER_FILTERED_DELETES,
  NUMBER_MERGE_FAILURES,

  // number of times bloom was checked before creating iterator on a
  // file, and the number of times the check was useful in avoiding
  // iterator creation (and thus likely IOPs).
  BLOOM_FILTER_PREFIX_CHECKED,
  BLOOM_FILTER_PREFIX_USEFUL,

  // Number of times we had to reseek inside an iteration to skip
  // over large number of keys with same userkey.
  NUMBER_OF_RESEEKS_IN_ITERATION,

  // Record the number of calls to GetUpadtesSince. Useful to keep track of
  // transaction log iterator refreshes
  GET_UPDATES_SINCE_CALLS,
  BLOCK_CACHE_COMPRESSED_MISS,  // miss in the compressed block cache
  BLOCK_CACHE_COMPRESSED_HIT,   // hit in the compressed block cache
  // Number of blocks added to compressed block cache
  BLOCK_CACHE_COMPRESSED_ADD,
  // Number of failures when adding blocks to compressed block cache
  BLOCK_CACHE_COMPRESSED_ADD_FAILURES,
  WAL_FILE_SYNCED,  // Number of times WAL sync is done
  WAL_FILE_BYTES,   // Number of bytes written to WAL

  // Writes can be processed by requesting thread or by the thread at the
  // head of the writers queue.
  WRITE_DONE_BY_SELF,
  WRITE_DONE_BY_OTHER,  // Equivalent to writes done for others
  WRITE_TIMEDOUT,       // Number of writes ending up with timed-out.
  WRITE_WITH_WAL,       // Number of Write calls that request WAL
  COMPACT_READ_BYTES,   // Bytes read during compaction
  COMPACT_WRITE_BYTES,  // Bytes written during compaction
  FLUSH_WRITE_BYTES,    // Bytes written during flush

  // Number of table's properties loaded directly from file, without creating
  // table reader object.
  NUMBER_DIRECT_LOAD_TABLE_PROPERTIES,
  NUMBER_SUPERVERSION_ACQUIRES,
  NUMBER_SUPERVERSION_RELEASES,
  NUMBER_SUPERVERSION_CLEANUPS,

  // # of compressions/decompressions executed
  NUMBER_BLOCK_COMPRESSED,
  NUMBER_BLOCK_DECOMPRESSED,

  NUMBER_BLOCK_NOT_COMPRESSED,
  MERGE_OPERATION_TOTAL_TIME,
  FILTER_OPERATION_TOTAL_TIME,

  // Row cache.
  ROW_CACHE_HIT,
  ROW_CACHE_MISS,

  // Read amplification statistics.
  // Read amplification can be calculated using this formula
  // (READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES)
  //
  // REQUIRES: ReadOptions::read_amp_bytes_per_bit to be enabled
  READ_AMP_ESTIMATE_USEFUL_BYTES,  // Estimate of total bytes actually used.
  READ_AMP_TOTAL_READ_BYTES,       // Total size of loaded data blocks.

  // Number of refill intervals where rate limiter's bytes are fully consumed.
  NUMBER_RATE_LIMITER_DRAINS,

  // Number of internal keys skipped by Iterator
  NUMBER_ITER_SKIP,

  // BlobDB specific stats
  // # of Put/PutTTL/PutUntil to BlobDB.
  BLOB_DB_NUM_PUT,
  // # of Write to BlobDB.
  BLOB_DB_NUM_WRITE,
  // # of Get to BlobDB.
  BLOB_DB_NUM_GET,
  // # of MultiGet to BlobDB.
  BLOB_DB_NUM_MULTIGET,
  // # of Seek/SeekToFirst/SeekToLast/SeekForPrev to BlobDB iterator.
  BLOB_DB_NUM_SEEK,
  // # of Next to BlobDB iterator.
  BLOB_DB_NUM_NEXT,
  // # of Prev to BlobDB iterator.
  BLOB_DB_NUM_PREV,
  // # of keys written to BlobDB.
  BLOB_DB_NUM_KEYS_WRITTEN,
  // # of keys read from BlobDB.
  BLOB_DB_NUM_KEYS_READ,
  // # of bytes (key + value) written to BlobDB.
  BLOB_DB_BYTES_WRITTEN,
  // # of bytes (keys + value) read from BlobDB.
  BLOB_DB_BYTES_READ,
  // # of keys written by BlobDB as non-TTL inlined value.
  BLOB_DB_WRITE_INLINED,
  // # of keys written by BlobDB as TTL inlined value.
  BLOB_DB_WRITE_INLINED_TTL,
  // # of keys written by BlobDB as non-TTL blob value.
  BLOB_DB_WRITE_BLOB,
  // # of keys written by BlobDB as TTL blob value.
  BLOB_DB_WRITE_BLOB_TTL,
  // # of bytes written to blob file.
  BLOB_DB_BLOB_FILE_BYTES_WRITTEN,
  // # of bytes read from blob file.
  BLOB_DB_BLOB_FILE_BYTES_READ,
  // # of times a blob files being synced.
  BLOB_DB_BLOB_FILE_SYNCED,
  // # of blob index evicted from base DB by BlobDB compaction filter because
  // of expiration.
  BLOB_DB_BLOB_INDEX_EXPIRED_COUNT,
  // size of blob index evicted from base DB by BlobDB compaction filter
  // because of expiration.
  BLOB_DB_BLOB_INDEX_EXPIRED_SIZE,
  // # of blob index evicted from base DB by BlobDB compaction filter because
  // of corresponding file deleted.
  BLOB_DB_BLOB_INDEX_EVICTED_COUNT,
  // size of blob index evicted from base DB by BlobDB compaction filter
  // because of corresponding file deleted.
  BLOB_DB_BLOB_INDEX_EVICTED_SIZE,
  // # of blob files being garbage collected.
  BLOB_DB_GC_NUM_FILES,
  // # of blob files generated by garbage collection.
  BLOB_DB_GC_NUM_NEW_FILES,
  // # of BlobDB garbage collection failures.
  BLOB_DB_GC_FAILURES,
  // # of keys drop by BlobDB garbage collection because they had been
  // overwritten.
  BLOB_DB_GC_NUM_KEYS_OVERWRITTEN,
  // # of keys drop by BlobDB garbage collection because of expiration.
  BLOB_DB_GC_NUM_KEYS_EXPIRED,
  // # of keys relocated to new blob file by garbage collection.
  BLOB_DB_GC_NUM_KEYS_RELOCATED,
  // # of bytes drop by BlobDB garbage collection because they had been
  // overwritten.
  BLOB_DB_GC_BYTES_OVERWRITTEN,
  // # of bytes drop by BlobDB garbage collection because of expiration.
  BLOB_DB_GC_BYTES_EXPIRED,
  // # of bytes relocated to new blob file by garbage collection.
  BLOB_DB_GC_BYTES_RELOCATED,
  // # of blob files evicted because of BlobDB is full.
  BLOB_DB_FIFO_NUM_FILES_EVICTED,
  // # of keys in the blob files evicted because of BlobDB is full.
  BLOB_DB_FIFO_NUM_KEYS_EVICTED,
  // # of bytes in the blob files evicted because of BlobDB is full.
  BLOB_DB_FIFO_BYTES_EVICTED,

  // These coutners indicate a performance issue in WritePrepared transactions.
  // We should not seem them ticking them much.
  // # of times prepare_mutex_ is acquired in the fast path.
  TXN_PREPARE_MUTEX_OVERHEAD,
  // # of times old_commit_map_mutex_ is acquired in the fast path.
  TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD,
  // # of times we checked a batch for duplicate keys.
  TXN_DUPLICATE_KEY_OVERHEAD,
  // # of times snapshot_mutex_ is acquired in the fast path.
  TXN_SNAPSHOT_MUTEX_OVERHEAD,

  // Number of keys actually found in MultiGet calls (vs number requested by caller)
  // NUMBER_MULTIGET_KEYS_READ gives the number requested by caller
  NUMBER_MULTIGET_KEYS_FOUND,
  TICKER_ENUM_MAX
};

// The order of items listed in  Tickers should be the same as
// the order listed in TickersNameMap
const std::vector<std::pair<Tickers, std::string>> TickersNameMap = {
    {BLOCK_CACHE_MISS, "rocksdb.block.cache.miss"},
    {BLOCK_CACHE_HIT, "rocksdb.block.cache.hit"},
    {BLOCK_CACHE_ADD, "rocksdb.block.cache.add"},
    {BLOCK_CACHE_ADD_FAILURES, "rocksdb.block.cache.add.failures"},
    {BLOCK_CACHE_INDEX_MISS, "rocksdb.block.cache.index.miss"},
    {BLOCK_CACHE_INDEX_HIT, "rocksdb.block.cache.index.hit"},
    {BLOCK_CACHE_INDEX_ADD, "rocksdb.block.cache.index.add"},
    {BLOCK_CACHE_INDEX_BYTES_INSERT, "rocksdb.block.cache.index.bytes.insert"},
    {BLOCK_CACHE_INDEX_BYTES_EVICT, "rocksdb.block.cache.index.bytes.evict"},
    {BLOCK_CACHE_FILTER_MISS, "rocksdb.block.cache.filter.miss"},
    {BLOCK_CACHE_FILTER_HIT, "rocksdb.block.cache.filter.hit"},
    {BLOCK_CACHE_FILTER_ADD, "rocksdb.block.cache.filter.add"},
    {BLOCK_CACHE_FILTER_BYTES_INSERT,
     "rocksdb.block.cache.filter.bytes.insert"},
    {BLOCK_CACHE_FILTER_BYTES_EVICT, "rocksdb.block.cache.filter.bytes.evict"},
    {BLOCK_CACHE_DATA_MISS, "rocksdb.block.cache.data.miss"},
    {BLOCK_CACHE_DATA_HIT, "rocksdb.block.cache.data.hit"},
    {BLOCK_CACHE_DATA_ADD, "rocksdb.block.cache.data.add"},
    {BLOCK_CACHE_DATA_BYTES_INSERT, "rocksdb.block.cache.data.bytes.insert"},
    {BLOCK_CACHE_BYTES_READ, "rocksdb.block.cache.bytes.read"},
    {BLOCK_CACHE_BYTES_WRITE, "rocksdb.block.cache.bytes.write"},
    {BLOOM_FILTER_USEFUL, "rocksdb.bloom.filter.useful"},
    {BLOOM_FILTER_FULL_POSITIVE, "rocksdb.bloom.filter.full.positive"},
    {BLOOM_FILTER_FULL_TRUE_POSITIVE,
     "rocksdb.bloom.filter.full.true.positive"},
    {PERSISTENT_CACHE_HIT, "rocksdb.persistent.cache.hit"},
    {PERSISTENT_CACHE_MISS, "rocksdb.persistent.cache.miss"},
    {SIM_BLOCK_CACHE_HIT, "rocksdb.sim.block.cache.hit"},
    {SIM_BLOCK_CACHE_MISS, "rocksdb.sim.block.cache.miss"},
    {MEMTABLE_HIT, "rocksdb.memtable.hit"},
    {MEMTABLE_MISS, "rocksdb.memtable.miss"},
    {GET_HIT_L0, "rocksdb.l0.hit"},
    {GET_HIT_L1, "rocksdb.l1.hit"},
    {GET_HIT_L2_AND_UP, "rocksdb.l2andup.hit"},
    {COMPACTION_KEY_DROP_NEWER_ENTRY, "rocksdb.compaction.key.drop.new"},
    {COMPACTION_KEY_DROP_OBSOLETE, "rocksdb.compaction.key.drop.obsolete"},
    {COMPACTION_KEY_DROP_RANGE_DEL, "rocksdb.compaction.key.drop.range_del"},
    {COMPACTION_KEY_DROP_USER, "rocksdb.compaction.key.drop.user"},
    {COMPACTION_RANGE_DEL_DROP_OBSOLETE,
     "rocksdb.compaction.range_del.drop.obsolete"},
    {COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,
     "rocksdb.compaction.optimized.del.drop.obsolete"},
    {COMPACTION_CANCELLED, "rocksdb.compaction.cancelled"},
    {NUMBER_KEYS_WRITTEN, "rocksdb.number.keys.written"},
    {NUMBER_KEYS_READ, "rocksdb.number.keys.read"},
    {NUMBER_KEYS_UPDATED, "rocksdb.number.keys.updated"},
    {BYTES_WRITTEN, "rocksdb.bytes.written"},
    {BYTES_READ, "rocksdb.bytes.read"},
    {NUMBER_DB_SEEK, "rocksdb.number.db.seek"},
    {NUMBER_DB_NEXT, "rocksdb.number.db.next"},
    {NUMBER_DB_PREV, "rocksdb.number.db.prev"},
    {NUMBER_DB_SEEK_FOUND, "rocksdb.number.db.seek.found"},
    {NUMBER_DB_NEXT_FOUND, "rocksdb.number.db.next.found"},
    {NUMBER_DB_PREV_FOUND, "rocksdb.number.db.prev.found"},
    {ITER_BYTES_READ, "rocksdb.db.iter.bytes.read"},
    {NO_FILE_CLOSES, "rocksdb.no.file.closes"},
    {NO_FILE_OPENS, "rocksdb.no.file.opens"},
    {NO_FILE_ERRORS, "rocksdb.no.file.errors"},
    {STALL_L0_SLOWDOWN_MICROS, "rocksdb.l0.slowdown.micros"},
    {STALL_MEMTABLE_COMPACTION_MICROS, "rocksdb.memtable.compaction.micros"},
    {STALL_L0_NUM_FILES_MICROS, "rocksdb.l0.num.files.stall.micros"},
    {STALL_MICROS, "rocksdb.stall.micros"},
    {DB_MUTEX_WAIT_MICROS, "rocksdb.db.mutex.wait.micros"},
    {RATE_LIMIT_DELAY_MILLIS, "rocksdb.rate.limit.delay.millis"},
    {NO_ITERATORS, "rocksdb.num.iterators"},
    {NUMBER_MULTIGET_CALLS, "rocksdb.number.multiget.get"},
    {NUMBER_MULTIGET_KEYS_READ, "rocksdb.number.multiget.keys.read"},
    {NUMBER_MULTIGET_BYTES_READ, "rocksdb.number.multiget.bytes.read"},
    {NUMBER_FILTERED_DELETES, "rocksdb.number.deletes.filtered"},
    {NUMBER_MERGE_FAILURES, "rocksdb.number.merge.failures"},
    {BLOOM_FILTER_PREFIX_CHECKED, "rocksdb.bloom.filter.prefix.checked"},
    {BLOOM_FILTER_PREFIX_USEFUL, "rocksdb.bloom.filter.prefix.useful"},
    {NUMBER_OF_RESEEKS_IN_ITERATION, "rocksdb.number.reseeks.iteration"},
    {GET_UPDATES_SINCE_CALLS, "rocksdb.getupdatessince.calls"},
    {BLOCK_CACHE_COMPRESSED_MISS, "rocksdb.block.cachecompressed.miss"},
    {BLOCK_CACHE_COMPRESSED_HIT, "rocksdb.block.cachecompressed.hit"},
    {BLOCK_CACHE_COMPRESSED_ADD, "rocksdb.block.cachecompressed.add"},
    {BLOCK_CACHE_COMPRESSED_ADD_FAILURES,
     "rocksdb.block.cachecompressed.add.failures"},
    {WAL_FILE_SYNCED, "rocksdb.wal.synced"},
    {WAL_FILE_BYTES, "rocksdb.wal.bytes"},
    {WRITE_DONE_BY_SELF, "rocksdb.write.self"},
    {WRITE_DONE_BY_OTHER, "rocksdb.write.other"},
    {WRITE_TIMEDOUT, "rocksdb.write.timeout"},
    {WRITE_WITH_WAL, "rocksdb.write.wal"},
    {COMPACT_READ_BYTES, "rocksdb.compact.read.bytes"},
    {COMPACT_WRITE_BYTES, "rocksdb.compact.write.bytes"},
    {FLUSH_WRITE_BYTES, "rocksdb.flush.write.bytes"},
    {NUMBER_DIRECT_LOAD_TABLE_PROPERTIES,
     "rocksdb.number.direct.load.table.properties"},
    {NUMBER_SUPERVERSION_ACQUIRES, "rocksdb.number.superversion_acquires"},
    {NUMBER_SUPERVERSION_RELEASES, "rocksdb.number.superversion_releases"},
    {NUMBER_SUPERVERSION_CLEANUPS, "rocksdb.number.superversion_cleanups"},
    {NUMBER_BLOCK_COMPRESSED, "rocksdb.number.block.compressed"},
    {NUMBER_BLOCK_DECOMPRESSED, "rocksdb.number.block.decompressed"},
    {NUMBER_BLOCK_NOT_COMPRESSED, "rocksdb.number.block.not_compressed"},
    {MERGE_OPERATION_TOTAL_TIME, "rocksdb.merge.operation.time.nanos"},
    {FILTER_OPERATION_TOTAL_TIME, "rocksdb.filter.operation.time.nanos"},
    {ROW_CACHE_HIT, "rocksdb.row.cache.hit"},
    {ROW_CACHE_MISS, "rocksdb.row.cache.miss"},
    {READ_AMP_ESTIMATE_USEFUL_BYTES, "rocksdb.read.amp.estimate.useful.bytes"},
    {READ_AMP_TOTAL_READ_BYTES, "rocksdb.read.amp.total.read.bytes"},
    {NUMBER_RATE_LIMITER_DRAINS, "rocksdb.number.rate_limiter.drains"},
    {NUMBER_ITER_SKIP, "rocksdb.number.iter.skip"},
    {BLOB_DB_NUM_PUT, "rocksdb.blobdb.num.put"},
    {BLOB_DB_NUM_WRITE, "rocksdb.blobdb.num.write"},
    {BLOB_DB_NUM_GET, "rocksdb.blobdb.num.get"},
    {BLOB_DB_NUM_MULTIGET, "rocksdb.blobdb.num.multiget"},
    {BLOB_DB_NUM_SEEK, "rocksdb.blobdb.num.seek"},
    {BLOB_DB_NUM_NEXT, "rocksdb.blobdb.num.next"},
    {BLOB_DB_NUM_PREV, "rocksdb.blobdb.num.prev"},
    {BLOB_DB_NUM_KEYS_WRITTEN, "rocksdb.blobdb.num.keys.written"},
    {BLOB_DB_NUM_KEYS_READ, "rocksdb.blobdb.num.keys.read"},
    {BLOB_DB_BYTES_WRITTEN, "rocksdb.blobdb.bytes.written"},
    {BLOB_DB_BYTES_READ, "rocksdb.blobdb.bytes.read"},
    {BLOB_DB_WRITE_INLINED, "rocksdb.blobdb.write.inlined"},
    {BLOB_DB_WRITE_INLINED_TTL, "rocksdb.blobdb.write.inlined.ttl"},
    {BLOB_DB_WRITE_BLOB, "rocksdb.blobdb.write.blob"},
    {BLOB_DB_WRITE_BLOB_TTL, "rocksdb.blobdb.write.blob.ttl"},
    {BLOB_DB_BLOB_FILE_BYTES_WRITTEN, "rocksdb.blobdb.blob.file.bytes.written"},
    {BLOB_DB_BLOB_FILE_BYTES_READ, "rocksdb.blobdb.blob.file.bytes.read"},
    {BLOB_DB_BLOB_FILE_SYNCED, "rocksdb.blobdb.blob.file.synced"},
    {BLOB_DB_BLOB_INDEX_EXPIRED_COUNT,
     "rocksdb.blobdb.blob.index.expired.count"},
    {BLOB_DB_BLOB_INDEX_EXPIRED_SIZE, "rocksdb.blobdb.blob.index.expired.size"},
    {BLOB_DB_BLOB_INDEX_EVICTED_COUNT,
     "rocksdb.blobdb.blob.index.evicted.count"},
    {BLOB_DB_BLOB_INDEX_EVICTED_SIZE, "rocksdb.blobdb.blob.index.evicted.size"},
    {BLOB_DB_GC_NUM_FILES, "rocksdb.blobdb.gc.num.files"},
    {BLOB_DB_GC_NUM_NEW_FILES, "rocksdb.blobdb.gc.num.new.files"},
    {BLOB_DB_GC_FAILURES, "rocksdb.blobdb.gc.failures"},
    {BLOB_DB_GC_NUM_KEYS_OVERWRITTEN, "rocksdb.blobdb.gc.num.keys.overwritten"},
    {BLOB_DB_GC_NUM_KEYS_EXPIRED, "rocksdb.blobdb.gc.num.keys.expired"},
    {BLOB_DB_GC_NUM_KEYS_RELOCATED, "rocksdb.blobdb.gc.num.keys.relocated"},
    {BLOB_DB_GC_BYTES_OVERWRITTEN, "rocksdb.blobdb.gc.bytes.overwritten"},
    {BLOB_DB_GC_BYTES_EXPIRED, "rocksdb.blobdb.gc.bytes.expired"},
    {BLOB_DB_GC_BYTES_RELOCATED, "rocksdb.blobdb.gc.bytes.relocated"},
    {BLOB_DB_FIFO_NUM_FILES_EVICTED, "rocksdb.blobdb.fifo.num.files.evicted"},
    {BLOB_DB_FIFO_NUM_KEYS_EVICTED, "rocksdb.blobdb.fifo.num.keys.evicted"},
    {BLOB_DB_FIFO_BYTES_EVICTED, "rocksdb.blobdb.fifo.bytes.evicted"},
    {TXN_PREPARE_MUTEX_OVERHEAD, "rocksdb.txn.overhead.mutex.prepare"},
    {TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD,
     "rocksdb.txn.overhead.mutex.old.commit.map"},
    {TXN_DUPLICATE_KEY_OVERHEAD, "rocksdb.txn.overhead.duplicate.key"},
    {TXN_SNAPSHOT_MUTEX_OVERHEAD, "rocksdb.txn.overhead.mutex.snapshot"},
    {NUMBER_MULTIGET_KEYS_FOUND, "rocksdb.number.multiget.keys.found"},
};

/**
 * Keep adding histogram's here.
 * Any histogram should have value less than HISTOGRAM_ENUM_MAX
 * Add a new Histogram by assigning it the current value of HISTOGRAM_ENUM_MAX
 * Add a string representation in HistogramsNameMap below
 * And increment HISTOGRAM_ENUM_MAX
 * Add a corresponding enum value to HistogramType.java in the java API
 */
enum Histograms : uint32_t {
  DB_GET = 0,
  DB_WRITE,
  COMPACTION_TIME,
  SUBCOMPACTION_SETUP_TIME,
  TABLE_SYNC_MICROS,
  COMPACTION_OUTFILE_SYNC_MICROS,
  WAL_FILE_SYNC_MICROS,
  MANIFEST_FILE_SYNC_MICROS,
  // TIME SPENT IN IO DURING TABLE OPEN
  TABLE_OPEN_IO_MICROS,
  DB_MULTIGET,
  READ_BLOCK_COMPACTION_MICROS,
  READ_BLOCK_GET_MICROS,
  WRITE_RAW_BLOCK_MICROS,
  STALL_L0_SLOWDOWN_COUNT,
  STALL_MEMTABLE_COMPACTION_COUNT,
  STALL_L0_NUM_FILES_COUNT,
  HARD_RATE_LIMIT_DELAY_COUNT,
  SOFT_RATE_LIMIT_DELAY_COUNT,
  NUM_FILES_IN_SINGLE_COMPACTION,
  DB_SEEK,
  WRITE_STALL,
  SST_READ_MICROS,
  // The number of subcompactions actually scheduled during a compaction
  NUM_SUBCOMPACTIONS_SCHEDULED,
  // Value size distribution in each operation
  BYTES_PER_READ,
  BYTES_PER_WRITE,
  BYTES_PER_MULTIGET,

  // number of bytes compressed/decompressed
  // number of bytes is when uncompressed; i.e. before/after respectively
  BYTES_COMPRESSED,
  BYTES_DECOMPRESSED,
  COMPRESSION_TIMES_NANOS,
  DECOMPRESSION_TIMES_NANOS,
  // Number of merge operands passed to the merge operator in user read
  // requests.
  READ_NUM_MERGE_OPERANDS,

  // BlobDB specific stats
  // Size of keys written to BlobDB.
  BLOB_DB_KEY_SIZE,
  // Size of values written to BlobDB.
  BLOB_DB_VALUE_SIZE,
  // BlobDB Put/PutWithTTL/PutUntil/Write latency.
  BLOB_DB_WRITE_MICROS,
  // BlobDB Get lagency.
  BLOB_DB_GET_MICROS,
  // BlobDB MultiGet latency.
  BLOB_DB_MULTIGET_MICROS,
  // BlobDB Seek/SeekToFirst/SeekToLast/SeekForPrev latency.
  BLOB_DB_SEEK_MICROS,
  // BlobDB Next latency.
  BLOB_DB_NEXT_MICROS,
  // BlobDB Prev latency.
  BLOB_DB_PREV_MICROS,
  // Blob file write latency.
  BLOB_DB_BLOB_FILE_WRITE_MICROS,
  // Blob file read latency.
  BLOB_DB_BLOB_FILE_READ_MICROS,
  // Blob file sync latency.
  BLOB_DB_BLOB_FILE_SYNC_MICROS,
  // BlobDB garbage collection time.
  BLOB_DB_GC_MICROS,
  // BlobDB compression time.
  BLOB_DB_COMPRESSION_MICROS,
  // BlobDB decompression time.
  BLOB_DB_DECOMPRESSION_MICROS,
  // Time spent flushing memtable to disk
  FLUSH_TIME,

  HISTOGRAM_ENUM_MAX,  // TODO(ldemailly): enforce HistogramsNameMap match
};

const std::vector<std::pair<Histograms, std::string>> HistogramsNameMap = {
    {DB_GET, "rocksdb.db.get.micros"},
    {DB_WRITE, "rocksdb.db.write.micros"},
    {COMPACTION_TIME, "rocksdb.compaction.times.micros"},
    {SUBCOMPACTION_SETUP_TIME, "rocksdb.subcompaction.setup.times.micros"},
    {TABLE_SYNC_MICROS, "rocksdb.table.sync.micros"},
    {COMPACTION_OUTFILE_SYNC_MICROS, "rocksdb.compaction.outfile.sync.micros"},
    {WAL_FILE_SYNC_MICROS, "rocksdb.wal.file.sync.micros"},
    {MANIFEST_FILE_SYNC_MICROS, "rocksdb.manifest.file.sync.micros"},
    {TABLE_OPEN_IO_MICROS, "rocksdb.table.open.io.micros"},
    {DB_MULTIGET, "rocksdb.db.multiget.micros"},
    {READ_BLOCK_COMPACTION_MICROS, "rocksdb.read.block.compaction.micros"},
    {READ_BLOCK_GET_MICROS, "rocksdb.read.block.get.micros"},
    {WRITE_RAW_BLOCK_MICROS, "rocksdb.write.raw.block.micros"},
    {STALL_L0_SLOWDOWN_COUNT, "rocksdb.l0.slowdown.count"},
    {STALL_MEMTABLE_COMPACTION_COUNT, "rocksdb.memtable.compaction.count"},
    {STALL_L0_NUM_FILES_COUNT, "rocksdb.num.files.stall.count"},
    {HARD_RATE_LIMIT_DELAY_COUNT, "rocksdb.hard.rate.limit.delay.count"},
    {SOFT_RATE_LIMIT_DELAY_COUNT, "rocksdb.soft.rate.limit.delay.count"},
    {NUM_FILES_IN_SINGLE_COMPACTION, "rocksdb.numfiles.in.singlecompaction"},
    {DB_SEEK, "rocksdb.db.seek.micros"},
    {WRITE_STALL, "rocksdb.db.write.stall"},
    {SST_READ_MICROS, "rocksdb.sst.read.micros"},
    {NUM_SUBCOMPACTIONS_SCHEDULED, "rocksdb.num.subcompactions.scheduled"},
    {BYTES_PER_READ, "rocksdb.bytes.per.read"},
    {BYTES_PER_WRITE, "rocksdb.bytes.per.write"},
    {BYTES_PER_MULTIGET, "rocksdb.bytes.per.multiget"},
    {BYTES_COMPRESSED, "rocksdb.bytes.compressed"},
    {BYTES_DECOMPRESSED, "rocksdb.bytes.decompressed"},
    {COMPRESSION_TIMES_NANOS, "rocksdb.compression.times.nanos"},
    {DECOMPRESSION_TIMES_NANOS, "rocksdb.decompression.times.nanos"},
    {READ_NUM_MERGE_OPERANDS, "rocksdb.read.num.merge_operands"},
    {BLOB_DB_KEY_SIZE, "rocksdb.blobdb.key.size"},
    {BLOB_DB_VALUE_SIZE, "rocksdb.blobdb.value.size"},
    {BLOB_DB_WRITE_MICROS, "rocksdb.blobdb.write.micros"},
    {BLOB_DB_GET_MICROS, "rocksdb.blobdb.get.micros"},
    {BLOB_DB_MULTIGET_MICROS, "rocksdb.blobdb.multiget.micros"},
    {BLOB_DB_SEEK_MICROS, "rocksdb.blobdb.seek.micros"},
    {BLOB_DB_NEXT_MICROS, "rocksdb.blobdb.next.micros"},
    {BLOB_DB_PREV_MICROS, "rocksdb.blobdb.prev.micros"},
    {BLOB_DB_BLOB_FILE_WRITE_MICROS, "rocksdb.blobdb.blob.file.write.micros"},
    {BLOB_DB_BLOB_FILE_READ_MICROS, "rocksdb.blobdb.blob.file.read.micros"},
    {BLOB_DB_BLOB_FILE_SYNC_MICROS, "rocksdb.blobdb.blob.file.sync.micros"},
    {BLOB_DB_GC_MICROS, "rocksdb.blobdb.gc.micros"},
    {BLOB_DB_COMPRESSION_MICROS, "rocksdb.blobdb.compression.micros"},
    {BLOB_DB_DECOMPRESSION_MICROS, "rocksdb.blobdb.decompression.micros"},
    {FLUSH_TIME, "rocksdb.db.flush.micros"},
};

struct HistogramData {
  double median;
  double percentile95;
  double percentile99;
  double average;
  double standard_deviation;
  // zero-initialize new members since old Statistics::histogramData()
  // implementations won't write them.
  double max = 0.0;
};

enum StatsLevel {
  // Collect all stats except time inside mutex lock AND time spent on
  // compression.
  kExceptDetailedTimers,
  // Collect all stats except the counters requiring to get time inside the
  // mutex lock.
  kExceptTimeForMutex,
  // Collect all stats, including measuring duration of mutex operations.
  // If getting time is expensive on the platform to run, it can
  // reduce scalability to more threads, especially for writes.
  kAll,
};

// Analyze the performance of a db
class Statistics {
 public:
  virtual ~Statistics() {}

  virtual uint64_t getTickerCount(uint32_t tickerType) const = 0;
  virtual void histogramData(uint32_t type,
                             HistogramData* const data) const = 0;
  virtual std::string getHistogramString(uint32_t /*type*/) const { return ""; }
  virtual void recordTick(uint32_t tickerType, uint64_t count = 0) = 0;
  virtual void setTickerCount(uint32_t tickerType, uint64_t count) = 0;
  virtual uint64_t getAndResetTickerCount(uint32_t tickerType) = 0;
  virtual void measureTime(uint32_t histogramType, uint64_t time) = 0;

  // Resets all ticker and histogram stats
  virtual Status Reset() {
    return Status::NotSupported("Not implemented");
  }

  // String representation of the statistic object.
  virtual std::string ToString() const {
    // Do nothing by default
    return std::string("ToString(): not implemented");
  }

  // Override this function to disable particular histogram collection
  virtual bool HistEnabledForType(uint32_t type) const {
    return type < HISTOGRAM_ENUM_MAX;
  }

  StatsLevel stats_level_ = kExceptDetailedTimers;
};

// Create a concrete DBStatistics object
std::shared_ptr<Statistics> CreateDBStatistics();

}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_INCLUDE_STATISTICS_H_
