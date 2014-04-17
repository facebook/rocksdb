// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

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
  // # of times cache miss when accessing index block from block cache.
  BLOCK_CACHE_INDEX_MISS(3),
  // # of times cache hit when accessing index block from block cache.
  BLOCK_CACHE_INDEX_HIT(4),
  // # of times cache miss when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_MISS(5),
  // # of times cache hit when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_HIT(6),
  // # of times cache miss when accessing data block from block cache.
  BLOCK_CACHE_DATA_MISS(7),
  // # of times cache hit when accessing data block from block cache.
  BLOCK_CACHE_DATA_HIT(8),
  // # of times bloom filter has avoided file reads.
  BLOOM_FILTER_USEFUL(9),

  // # of memtable hits.
  MEMTABLE_HIT(10),
  // # of memtable misses.
  MEMTABLE_MISS(11),

  /**
   * COMPACTION_KEY_DROP_* count the reasons for key drop during compaction
   * There are 3 reasons currently.
   */
  COMPACTION_KEY_DROP_NEWER_ENTRY(12),  // key was written with a newer value.
  COMPACTION_KEY_DROP_OBSOLETE(13),     // The key is obsolete.
  COMPACTION_KEY_DROP_USER(14),  // user compaction function has dropped the key.

  // Number of keys written to the database via the Put and Write call's
  NUMBER_KEYS_WRITTEN(15),
  // Number of Keys read,
  NUMBER_KEYS_READ(16),
  // Number keys updated, if inplace update is enabled
  NUMBER_KEYS_UPDATED(17),
  // Bytes written / read
  BYTES_WRITTEN(18),
  BYTES_READ(19),
  NO_FILE_CLOSES(20),
  NO_FILE_OPENS(21),
  NO_FILE_ERRORS(22),
  // Time system had to wait to do LO-L1 compactions
  STALL_L0_SLOWDOWN_MICROS(23),
  // Time system had to wait to move memtable to L1.
  STALL_MEMTABLE_COMPACTION_MICROS(24),
  // write throttle because of too many files in L0
  STALL_L0_NUM_FILES_MICROS(25),
  RATE_LIMIT_DELAY_MILLIS(26),
  NO_ITERATORS(27),  // number of iterators currently open

  // Number of MultiGet calls, keys read, and bytes read
  NUMBER_MULTIGET_CALLS(28),
  NUMBER_MULTIGET_KEYS_READ(29),
  NUMBER_MULTIGET_BYTES_READ(30),

  // Number of deletes records that were not required to be
  // written to storage because key does not exist
  NUMBER_FILTERED_DELETES(31),
  NUMBER_MERGE_FAILURES(32),
  SEQUENCE_NUMBER(33),

  // number of times bloom was checked before creating iterator on a
  // file, and the number of times the check was useful in avoiding
  // iterator creation (and thus likely IOPs).
  BLOOM_FILTER_PREFIX_CHECKED(34),
  BLOOM_FILTER_PREFIX_USEFUL(35),

  // Number of times we had to reseek inside an iteration to skip
  // over large number of keys with same userkey.
  NUMBER_OF_RESEEKS_IN_ITERATION(36),

  // Record the number of calls to GetUpadtesSince. Useful to keep track of
  // transaction log iterator refreshes
  GET_UPDATES_SINCE_CALLS(37),
  BLOCK_CACHE_COMPRESSED_MISS(38),  // miss in the compressed block cache
  BLOCK_CACHE_COMPRESSED_HIT(39),   // hit in the compressed block cache
  WAL_FILE_SYNCED(40),              // Number of times WAL sync is done
  WAL_FILE_BYTES(41),               // Number of bytes written to WAL

  // Writes can be processed by requesting thread or by the thread at the
  // head of the writers queue.
  WRITE_DONE_BY_SELF(42),
  WRITE_DONE_BY_OTHER(43),
  WRITE_WITH_WAL(44),       // Number of Write calls that request WAL
  COMPACT_READ_BYTES(45),   // Bytes read during compaction
  COMPACT_WRITE_BYTES(46),  // Bytes written during compaction

  // Number of table's properties loaded directly from file, without creating
  // table reader object.
  NUMBER_DIRECT_LOAD_TABLE_PROPERTIES(47),
  NUMBER_SUPERVERSION_ACQUIRES(48),
  NUMBER_SUPERVERSION_RELEASES(49),
  NUMBER_SUPERVERSION_CLEANUPS(50);

  private final int value_;

  private TickerType(int value) {
    value_ = value;
  }

  public int getValue() {
    return value_;
  }
}
