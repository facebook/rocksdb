// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The types of histogram.
 */
public enum HistogramType {
  /**
   * DB Get.
   */
  DB_GET((byte) 0x0),

  /**
   * DB Write.
   */
  DB_WRITE((byte) 0x1),

  /**
   * Time spent in compaction.
   */
  COMPACTION_TIME((byte) 0x2),

  /**
   * CPU time spent in compaction.
   */
  COMPACTION_CPU_TIME((byte) 0x3),

  /**
   * Time spent in setting up sub-compaction.
   */
  SUBCOMPACTION_SETUP_TIME((byte) 0x4),

  /**
   * Time spent in IO during table sync.
   * Measured in microseconds.
   */
  TABLE_SYNC_MICROS((byte) 0x5),

  /**
   * Time spent in IO during compaction of outfile.
   * Measured in microseconds.
   */
  COMPACTION_OUTFILE_SYNC_MICROS((byte) 0x6),

  /**
   * Time spent in IO during WAL file sync.
   * Measured in microseconds.
   */
  WAL_FILE_SYNC_MICROS((byte) 0x7),

  /**
   * Time spent in IO during manifest file sync.
   * Measured in microseconds.
   */
  MANIFEST_FILE_SYNC_MICROS((byte) 0x8),

  /**
   * Time spent in IO during table open.
   * Measured in microseconds.
   */
  TABLE_OPEN_IO_MICROS((byte) 0x9),

  /**
   * DB Multi-Get.
   */
  DB_MULTIGET((byte) 0xA),

  /**
   * Time spent in block reads during compaction.
   * Measured in microseconds.
   */
  READ_BLOCK_COMPACTION_MICROS((byte) 0xB),

  /**
   * Time spent in block reads.
   * Measured in microseconds.
   */
  READ_BLOCK_GET_MICROS((byte) 0xC),

  /**
   * Time spent in raw block writes.
   * Measured in microseconds.
   */
  WRITE_RAW_BLOCK_MICROS((byte) 0xD),

  /**
   * Number of files in a single compaction.
   */
  NUM_FILES_IN_SINGLE_COMPACTION((byte) 0xE),

  /**
   * DB Seek.
   */
  DB_SEEK((byte) 0xF),

  /**
   * Write stall.
   */
  WRITE_STALL((byte) 0x10),

  /**
   * Time spent in SST reads.
   * Measured in microseconds.
   */
  SST_READ_MICROS((byte) 0x11),

  /**
   * File read during flush.
   * Measured in microseconds.
   */
  FILE_READ_FLUSH_MICROS((byte) 0x12),

  /**
   * File read during compaction.
   * Measured in microseconds.
   */
  FILE_READ_COMPACTION_MICROS((byte) 0x13),

  /**
   * File read during DB Open.
   * Measured in microseconds.
   */
  FILE_READ_DB_OPEN_MICROS((byte) 0x14),

  /**
   * File read during DB Get.
   * Measured in microseconds.
   */
  FILE_READ_GET_MICROS((byte) 0x15),

  /**
   * File read during DB Multi-Get.
   * Measured in microseconds.
   */
  FILE_READ_MULTIGET_MICROS((byte) 0x16),

  /**
   * File read during DB Iterator.
   * Measured in microseconds.
   */
  FILE_READ_DB_ITERATOR_MICROS((byte) 0x17),

  /**
   * File read during DB checksum validation.
   * Measured in microseconds.
   */
  FILE_READ_VERIFY_DB_CHECKSUM_MICROS((byte) 0x18),

  /**
   * File read during file checksum validation.
   * Measured in microseconds.
   */
  FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS((byte) 0x19),

  /**
   * Time spent writing SST files.
   * Measured in microseconds.
   */
  SST_WRITE_MICROS((byte) 0x1A),

  /**
   * Time spent in writing SST table (currently only block-based table) or blob file for flush.
   * Measured in microseconds.
   */
  FILE_WRITE_FLUSH_MICROS((byte) 0x1B),

  /**
   * Time spent in writing SST table (currently only block-based table) for compaction.
   * Measured in microseconds.
   */
  FILE_WRITE_COMPACTION_MICROS((byte) 0x1C),

  /**
   * Time spent in writing SST table (currently only block-based table) or blob file for db open.
   * Measured in microseconds.
   */
  FILE_WRITE_DB_OPEN_MICROS((byte) 0x1D),

  /**
   * The number of subcompactions actually scheduled during a compaction.
   */
  NUM_SUBCOMPACTIONS_SCHEDULED((byte) 0x1E),

  /**
   * Value size distribution in each operation.
   */
  BYTES_PER_READ((byte) 0x1F),

  /**
   * Bytes per write.
   * Value size distribution in each operation.
   */
  BYTES_PER_WRITE((byte) 0x20),

  /**
   * Bytes per Multi-Get.
   * Value size distribution in each operation.
   */
  BYTES_PER_MULTIGET((byte) 0x21),

  /**
   * Time spent in compression.
   * Measured in nanoseconds.
   */
  COMPRESSION_TIMES_NANOS((byte) 0x22),

  /**
   * Time spent in decompression.
   * Measured in nanoseconds.
   */
  DECOMPRESSION_TIMES_NANOS((byte) 0x23),

  /**
   * Number of merge operands for read.
   */
  READ_NUM_MERGE_OPERANDS((byte) 0x24),

  /**
   * Size of keys written to BlobDB.
   */
  BLOB_DB_KEY_SIZE((byte) 0x25),

  /**
   * Size of values written to BlobDB.
   */
  BLOB_DB_VALUE_SIZE((byte) 0x26),

  /**
   * BlobDB Put/PutWithTTL/PutUntil/Write latency.
   * Measured in microseconds.
   */
  BLOB_DB_WRITE_MICROS((byte) 0x27),

  /**
   * BlobDB Get lagency.
   * Measured in microseconds.
   */
  BLOB_DB_GET_MICROS((byte) 0x28),

  /**
   * BlobDB MultiGet latency.
   * Measured in microseconds.
   */
  BLOB_DB_MULTIGET_MICROS((byte) 0x29),

  /**
   * BlobDB Seek/SeekToFirst/SeekToLast/SeekForPrev latency.
   * Measured in microseconds.
   */
  BLOB_DB_SEEK_MICROS((byte) 0x2A),

  /**
   * BlobDB Next latency.
   * Measured in microseconds.
   */
  BLOB_DB_NEXT_MICROS((byte) 0x2B),

  /**
   * BlobDB Prev latency.
   * Measured in microseconds.
   */
  BLOB_DB_PREV_MICROS((byte) 0x2C),

  /**
   * Blob file write latency.
   * Measured in microseconds.
   */
  BLOB_DB_BLOB_FILE_WRITE_MICROS((byte) 0x2D),

  /**
   * Blob file read latency.
   * Measured in microseconds.
   */
  BLOB_DB_BLOB_FILE_READ_MICROS((byte) 0x2E),

  /**
   * Blob file sync latency.
   * Measured in microseconds.
   */
  BLOB_DB_BLOB_FILE_SYNC_MICROS((byte) 0x2F),

  /**
   * BlobDB compression time.
   * Measured in microseconds.
   */
  BLOB_DB_COMPRESSION_MICROS((byte) 0x30),

  /**
   * BlobDB decompression time.
   * Measured in microseconds.
   */
  BLOB_DB_DECOMPRESSION_MICROS((byte) 0x31),

  /**
   * Time spent flushing memtable to disk.
   */
  FLUSH_TIME((byte) 0x32),

  /**
   * Number of MultiGet batch keys overlapping a file.
   */
  SST_BATCH_SIZE((byte) 0x33),

  /**
   * Size of a single IO batch issued by MultiGet.
   */
  MULTIGET_IO_BATCH_SIZE((byte) 0x34),

  /**
   * Num of Index and Filter blocks read from file system per level in MultiGet request.
   */
  NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL((byte) 0x35),

  /**
   * Num of SST files read from file system per level in MultiGet request.
   */
  NUM_SST_READ_PER_LEVEL((byte) 0x36),

  /**
   * Num of LSM levels read from file system per MultiGet request.
   */
  NUM_LEVEL_READ_PER_MULTIGET((byte) 0x37),

  /**
   * The number of retry in auto resume.
   */
  ERROR_HANDLER_AUTORESUME_RETRY_COUNT((byte) 0x38),

  /**
   * Bytes read asynchronously.
   */
  ASYNC_READ_BYTES((byte) 0x39),

  /**
   * Wait time for polling.
   * Measured in microseconds.
   */
  POLL_WAIT_MICROS((byte) 0x3A),

  /**
   * Number of prefetched bytes discarded by RocksDB.
   */
  PREFETCHED_BYTES_DISCARDED((byte) 0x3B),

  /**
   * Wait time for aborting async read in FilePrefetchBuffer destructor.
   * Measured in microseconds.
   */
  ASYNC_PREFETCH_ABORT_MICROS((byte) 0x3C),

  /**
   * Number of bytes read for RocksDB's prefetching contents (as opposed to file system's prefetch)
   * from the end of SST table during block based table open.
   */
  TABLE_OPEN_PREFETCH_TAIL_READ_BYTES((byte) 0x3D),

  /**
   * Bytes prefetched during compaction.
   */
  COMPACTION_PREFETCH_BYTES((byte) 0x3F),

  /**
   * MultiScan histogram statistics
   */

  /**
   * Time spent in Iterator::Prepare() for multi-scan (microseconds).
   * Measured in microseconds.
   */
  MULTISCAN_PREPARE_MICROS((byte) 0x40),

  /**
   * Number of blocks per multi-scan Prepare() call.
   */
  MULTISCAN_BLOCKS_PER_PREPARE((byte) 0x41),

  // 0x3E is reserved for backwards compatibility on current minor version.
  HISTOGRAM_ENUM_MAX((byte) 0x3E);

  private final byte value;

  HistogramType(final byte value) {
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
   * Get Histogram type by byte value.
   *
   * @param value byte representation of HistogramType.
   *
   * @return {@link org.rocksdb.HistogramType} instance.
   * @throws java.lang.IllegalArgumentException if an invalid
   *     value is provided.
   */
  public static HistogramType getHistogramType(final byte value) {
    for (final HistogramType histogramType : HistogramType.values()) {
      if (histogramType.getValue() == value) {
        return histogramType;
      }
    }
    throw new IllegalArgumentException(
        "Illegal value provided for HistogramType.");
  }
}
