// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public enum HistogramType {
  DB_GET(0),
  DB_WRITE(1),
  COMPACTION_TIME(2),
  SUBCOMPACTION_SETUP_TIME(3),
  TABLE_SYNC_MICROS(4),
  COMPACTION_OUTFILE_SYNC_MICROS(5),
  WAL_FILE_SYNC_MICROS(6),
  MANIFEST_FILE_SYNC_MICROS(7),
  // TIME SPENT IN IO DURING TABLE OPEN
  TABLE_OPEN_IO_MICROS(8),
  DB_MULTIGET(9),
  READ_BLOCK_COMPACTION_MICROS(10),
  READ_BLOCK_GET_MICROS(11),
  WRITE_RAW_BLOCK_MICROS(12),
  STALL_L0_SLOWDOWN_COUNT(13),
  STALL_MEMTABLE_COMPACTION_COUNT(14),
  STALL_L0_NUM_FILES_COUNT(15),
  HARD_RATE_LIMIT_DELAY_COUNT(16),
  SOFT_RATE_LIMIT_DELAY_COUNT(17),
  NUM_FILES_IN_SINGLE_COMPACTION(18),
  DB_SEEK(19),
  WRITE_STALL(20),
  SST_READ_MICROS(21),
  NUM_SUBCOMPACTIONS_SCHEDULED(22),
  BYTES_PER_READ(23),
  BYTES_PER_WRITE(24),
  BYTES_PER_MULTIGET(25),
  BYTES_COMPRESSED(26),
  BYTES_DECOMPRESSED(27),
  COMPRESSION_TIMES_NANOS(28),
  DECOMPRESSION_TIMES_NANOS(29),
  READ_NUM_MERGE_OPERANDS(30);

  private final int value_;

  private HistogramType(int value) {
    value_ = value;
  }

  public int getValue() {
    return value_;
  }
}
