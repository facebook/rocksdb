// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum TickerType {
  // total block cache misses
  // REQUIRES: BLOCK_CACHE_MISS == BLOCK_CACHE_INDEX_MISS +
  //                               BLOCK_CACHE_FILTER_MISS +
  //                               BLOCK_CACHE_DATA_MISS;
  BLOCK_CACHE_MISS(0),
  // total block cache hit
  // REQUIRES: BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT +
  //                              BLOCK_CACHE_FILTER_HIT +
  //                              BLOCK_CACHE_DATA_HIT;
  BLOCK_CACHE_HIT(1),
  // # of blocks added to block cache.
  BLOCK_CACHE_ADD(2),
  // # of failures when adding blocks to block cache.
  BLOCK_CACHE_ADD_FAILURES(3),
  // # of times cache miss when accessing index block from block cache.
  BLOCK_CACHE_INDEX_MISS(4),
  // # of times cache hit when accessing index block from block cache.
  BLOCK_CACHE_INDEX_HIT(5),
  // # of index blocks added to block cache.
  BLOCK_CACHE_INDEX_ADD(6),
  // # of bytes of index blocks inserted into cache
  BLOCK_CACHE_INDEX_BYTES_INSERT(7),
  // # of bytes of index block erased from cache
  BLOCK_CACHE_INDEX_BYTES_EVICT(8),
  // # of times cache miss when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_MISS(9),
  // # of times cache hit when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_HIT(10),
  // # of filter blocks added to block cache.
  BLOCK_CACHE_FILTER_ADD(11),
  // # of bytes of bloom filter blocks inserted into cache
  BLOCK_CACHE_FILTER_BYTES_INSERT(12),
  // # of bytes of bloom filter block erased from cache
  BLOCK_CACHE_FILTER_BYTES_EVICT(13),
  // # of times cache miss when accessing data block from block cache.
  BLOCK_CACHE_DATA_MISS(14),
  // # of times cache hit when accessing data block from block cache.
  BLOCK_CACHE_DATA_HIT(15),
  // # of data blocks added to block cache.
  BLOCK_CACHE_DATA_ADD(16),
  // # of bytes of data blocks inserted into cache
  BLOCK_CACHE_DATA_BYTES_INSERT(17),
  // # of bytes read from cache.
  BLOCK_CACHE_BYTES_READ(18),
  // # of bytes written into cache.
  BLOCK_CACHE_BYTES_WRITE(19),

  // # of times bloom filter has avoided file reads.
  BLOOM_FILTER_USEFUL(20),

  // # persistent cache hit
  PERSISTENT_CACHE_HIT(21),
  // # persistent cache miss
  PERSISTENT_CACHE_MISS(22),

  // # total simulation block cache hits
  SIM_BLOCK_CACHE_HIT(23),
  // # total simulation block cache misses
  SIM_BLOCK_CACHE_MISS(24),

  // # of memtable hits.
  MEMTABLE_HIT(25),
  // # of memtable misses.
  MEMTABLE_MISS(26),

  // # of Get() queries served by L0
  GET_HIT_L0(27),
  // # of Get() queries served by L1
  GET_HIT_L1(28),
  // # of Get() queries served by L2 and up
  GET_HIT_L2_AND_UP(29),

  /**
   * COMPACTION_KEY_DROP_* count the reasons for key drop during compaction
   * There are 4 reasons currently.
   */
  COMPACTION_KEY_DROP_NEWER_ENTRY(30),  // key was written with a newer value.
  // Also includes keys dropped for range del.
  COMPACTION_KEY_DROP_OBSOLETE(31),     // The key is obsolete.
  COMPACTION_KEY_DROP_RANGE_DEL(32),    // key was covered by a range tombstone.
  COMPACTION_KEY_DROP_USER(33),  // user compaction function has dropped the key.

  COMPACTION_RANGE_DEL_DROP_OBSOLETE(34),  // all keys in range were deleted.

  // Number of keys written to the database via the Put and Write call's
  NUMBER_KEYS_WRITTEN(35),
  // Number of Keys read,
  NUMBER_KEYS_READ(36),
  // Number keys updated, if inplace update is enabled
  NUMBER_KEYS_UPDATED(37),
  // The number of uncompressed bytes issued by DB::Put(), DB::Delete(),
  // DB::Merge(), and DB::Write().
  BYTES_WRITTEN(38),
  // The number of uncompressed bytes read from DB::Get().  It could be
  // either from memtables, cache, or table files.
  // For the number of logical bytes read from DB::MultiGet(),
  // please use NUMBER_MULTIGET_BYTES_READ.
  BYTES_READ(39),
  // The number of calls to seek/next/prev
  NUMBER_DB_SEEK(40),
  NUMBER_DB_NEXT(41),
  NUMBER_DB_PREV(42),
  // The number of calls to seek/next/prev that returned data
  NUMBER_DB_SEEK_FOUND(43),
  NUMBER_DB_NEXT_FOUND(44),
  NUMBER_DB_PREV_FOUND(45),
  // The number of uncompressed bytes read from an iterator.
  // Includes size of key and value.
  ITER_BYTES_READ(46),
  NO_FILE_CLOSES(47),
  NO_FILE_OPENS(48),
  NO_FILE_ERRORS(49),
  // DEPRECATED Time system had to wait to do LO-L1 compactions
  STALL_L0_SLOWDOWN_MICROS(50),
  // DEPRECATED Time system had to wait to move memtable to L1.
  STALL_MEMTABLE_COMPACTION_MICROS(51),
  // DEPRECATED write throttle because of too many files in L0
  STALL_L0_NUM_FILES_MICROS(52),
  // Writer has to wait for compaction or flush to finish.
  STALL_MICROS(53),
  // The wait time for db mutex.
  // Disabled by default. To enable it set stats level to kAll
  DB_MUTEX_WAIT_MICROS(54),
  RATE_LIMIT_DELAY_MILLIS(55),
  NO_ITERATORS(56),  // number of iterators currently open

  // Number of MultiGet calls, keys read, and bytes read
  NUMBER_MULTIGET_CALLS(57),
  NUMBER_MULTIGET_KEYS_READ(58),
  NUMBER_MULTIGET_BYTES_READ(59),

  // Number of deletes records that were not required to be
  // written to storage because key does not exist
  NUMBER_FILTERED_DELETES(60),
  NUMBER_MERGE_FAILURES(61),

  // number of times bloom was checked before creating iterator on a
  // file, and the number of times the check was useful in avoiding
  // iterator creation (and thus likely IOPs).
  BLOOM_FILTER_PREFIX_CHECKED(62),
  BLOOM_FILTER_PREFIX_USEFUL(63),

  // Number of times we had to reseek inside an iteration to skip
  // over large number of keys with same userkey.
  NUMBER_OF_RESEEKS_IN_ITERATION(64),

  // Record the number of calls to GetUpadtesSince. Useful to keep track of
  // transaction log iterator refreshes
  GET_UPDATES_SINCE_CALLS(65),
  BLOCK_CACHE_COMPRESSED_MISS(66),  // miss in the compressed block cache
  BLOCK_CACHE_COMPRESSED_HIT(67),   // hit in the compressed block cache
  // Number of blocks added to comopressed block cache
  BLOCK_CACHE_COMPRESSED_ADD(68),
  // Number of failures when adding blocks to compressed block cache
  BLOCK_CACHE_COMPRESSED_ADD_FAILURES(69),
  WAL_FILE_SYNCED(70),  // Number of times WAL sync is done
  WAL_FILE_BYTES(71),   // Number of bytes written to WAL

  // Writes can be processed by requesting thread or by the thread at the
  // head of the writers queue.
  WRITE_DONE_BY_SELF(72),
  WRITE_DONE_BY_OTHER(73),  // Equivalent to writes done for others
  WRITE_TIMEDOUT(74),       // Number of writes ending up with timed-out.
  WRITE_WITH_WAL(75),       // Number of Write calls that request WAL
  COMPACT_READ_BYTES(76),   // Bytes read during compaction
  COMPACT_WRITE_BYTES(77),  // Bytes written during compaction
  FLUSH_WRITE_BYTES(78),    // Bytes written during flush

  // Number of table's properties loaded directly from file, without creating
  // table reader object.
  NUMBER_DIRECT_LOAD_TABLE_PROPERTIES(79),
  NUMBER_SUPERVERSION_ACQUIRES(80),
  NUMBER_SUPERVERSION_RELEASES(81),
  NUMBER_SUPERVERSION_CLEANUPS(82),

  // # of compressions/decompressions executed
  NUMBER_BLOCK_COMPRESSED(83),
  NUMBER_BLOCK_DECOMPRESSED(84),

  NUMBER_BLOCK_NOT_COMPRESSED(85),
  MERGE_OPERATION_TOTAL_TIME(86),
  FILTER_OPERATION_TOTAL_TIME(87),

  // Row cache.
  ROW_CACHE_HIT(88),
  ROW_CACHE_MISS(89),

  // Read amplification statistics.
  // Read amplification can be calculated using this formula
  // (READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES)
  //
  // REQUIRES: ReadOptions::read_amp_bytes_per_bit to be enabled
  READ_AMP_ESTIMATE_USEFUL_BYTES(90),  // Estimate of total bytes actually used.
  READ_AMP_TOTAL_READ_BYTES(91),       // Total size of loaded data blocks.

  // Number of refill intervals where rate limiter's bytes are fully consumed.
  NUMBER_RATE_LIMITER_DRAINS(92);

  private final int value_;

  private TickerType(int value) {
    value_ = value;
  }

  public int getValue() {
    return value_;
  }
}
