// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#pragma once

#include <jni.h>

#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
// The portal class for org.rocksdb.HistogramType
class HistogramTypeJni {
 public:
  // Returns the equivalent org.rocksdb.HistogramType for the provided
  // C++ ROCKSDB_NAMESPACE::Histograms enum
  static jbyte toJavaHistogramsType(
      const ROCKSDB_NAMESPACE::Histograms& histograms) {
    switch (histograms) {
      case ROCKSDB_NAMESPACE::Histograms::DB_GET:
        return 0x0;
      case ROCKSDB_NAMESPACE::Histograms::DB_WRITE:
        return 0x1;
      case ROCKSDB_NAMESPACE::Histograms::COMPACTION_TIME:
        return 0x2;
      case ROCKSDB_NAMESPACE::Histograms::COMPACTION_CPU_TIME:
        return 0x3;
      case ROCKSDB_NAMESPACE::Histograms::SUBCOMPACTION_SETUP_TIME:
        return 0x4;
      case ROCKSDB_NAMESPACE::Histograms::TABLE_SYNC_MICROS:
        return 0x5;
      case ROCKSDB_NAMESPACE::Histograms::COMPACTION_OUTFILE_SYNC_MICROS:
        return 0x6;
      case ROCKSDB_NAMESPACE::Histograms::WAL_FILE_SYNC_MICROS:
        return 0x7;
      case ROCKSDB_NAMESPACE::Histograms::MANIFEST_FILE_SYNC_MICROS:
        return 0x8;
      case ROCKSDB_NAMESPACE::Histograms::TABLE_OPEN_IO_MICROS:
        return 0x9;
      case ROCKSDB_NAMESPACE::Histograms::DB_MULTIGET:
        return 0xA;
      case ROCKSDB_NAMESPACE::Histograms::READ_BLOCK_COMPACTION_MICROS:
        return 0xB;
      case ROCKSDB_NAMESPACE::Histograms::READ_BLOCK_GET_MICROS:
        return 0xC;
      case ROCKSDB_NAMESPACE::Histograms::WRITE_RAW_BLOCK_MICROS:
        return 0xD;
      case ROCKSDB_NAMESPACE::Histograms::NUM_FILES_IN_SINGLE_COMPACTION:
        return 0xE;
      case ROCKSDB_NAMESPACE::Histograms::DB_SEEK:
        return 0xF;
      case ROCKSDB_NAMESPACE::Histograms::WRITE_STALL:
        return 0x10;
      case ROCKSDB_NAMESPACE::Histograms::SST_READ_MICROS:
        return 0x11;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_FLUSH_MICROS:
        return 0x12;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_COMPACTION_MICROS:
        return 0x13;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_DB_OPEN_MICROS:
        return 0x14;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_GET_MICROS:
        return 0x15;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_MULTIGET_MICROS:
        return 0x16;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_DB_ITERATOR_MICROS:
        return 0x17;
      case ROCKSDB_NAMESPACE::Histograms::FILE_READ_VERIFY_DB_CHECKSUM_MICROS:
        return 0x18;
      case ROCKSDB_NAMESPACE::Histograms::
          FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS:
        return 0x19;
      case ROCKSDB_NAMESPACE::Histograms::SST_WRITE_MICROS:
        return 0x1A;
      case ROCKSDB_NAMESPACE::Histograms::FILE_WRITE_FLUSH_MICROS:
        return 0x1B;
      case ROCKSDB_NAMESPACE::Histograms::FILE_WRITE_COMPACTION_MICROS:
        return 0x1C;
      case ROCKSDB_NAMESPACE::Histograms::FILE_WRITE_DB_OPEN_MICROS:
        return 0x1D;
      case ROCKSDB_NAMESPACE::Histograms::NUM_SUBCOMPACTIONS_SCHEDULED:
        return 0x1E;
      case ROCKSDB_NAMESPACE::Histograms::BYTES_PER_READ:
        return 0x1F;
      case ROCKSDB_NAMESPACE::Histograms::BYTES_PER_WRITE:
        return 0x20;
      case ROCKSDB_NAMESPACE::Histograms::BYTES_PER_MULTIGET:
        return 0x21;
      case ROCKSDB_NAMESPACE::Histograms::COMPRESSION_TIMES_NANOS:
        return 0x22;
      case ROCKSDB_NAMESPACE::Histograms::DECOMPRESSION_TIMES_NANOS:
        return 0x23;
      case ROCKSDB_NAMESPACE::Histograms::READ_NUM_MERGE_OPERANDS:
        return 0x24;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_KEY_SIZE:
        return 0x25;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_VALUE_SIZE:
        return 0x26;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_WRITE_MICROS:
        return 0x27;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_GET_MICROS:
        return 0x28;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_MULTIGET_MICROS:
        return 0x29;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_SEEK_MICROS:
        return 0x2A;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_NEXT_MICROS:
        return 0x2B;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_PREV_MICROS:
        return 0x2C;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS:
        return 0x2D;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_BLOB_FILE_READ_MICROS:
        return 0x2E;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_BLOB_FILE_SYNC_MICROS:
        return 0x2F;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_COMPRESSION_MICROS:
        return 0x30;
      case ROCKSDB_NAMESPACE::Histograms::BLOB_DB_DECOMPRESSION_MICROS:
        return 0x31;
      // 0x20 to skip 0x1F so TICKER_ENUM_MAX remains unchanged for minor
      // version compatibility.
      case ROCKSDB_NAMESPACE::Histograms::FLUSH_TIME:
        return 0x32;
      case ROCKSDB_NAMESPACE::Histograms::SST_BATCH_SIZE:
        return 0x33;
      case ROCKSDB_NAMESPACE::Histograms::MULTIGET_IO_BATCH_SIZE:
        return 0x34;
      case ROCKSDB_NAMESPACE::Histograms::
          NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL:
        return 0x35;
      case ROCKSDB_NAMESPACE::Histograms::NUM_SST_READ_PER_LEVEL:
        return 0x36;
      case ROCKSDB_NAMESPACE::Histograms::NUM_LEVEL_READ_PER_MULTIGET:
        return 0x37;
      case ROCKSDB_NAMESPACE::Histograms::ERROR_HANDLER_AUTORESUME_RETRY_COUNT:
        return 0x38;
      case ROCKSDB_NAMESPACE::Histograms::ASYNC_READ_BYTES:
        return 0x39;
      case ROCKSDB_NAMESPACE::Histograms::POLL_WAIT_MICROS:
        return 0x3A;
      case ROCKSDB_NAMESPACE::Histograms::PREFETCHED_BYTES_DISCARDED:
        return 0x3B;
      case ASYNC_PREFETCH_ABORT_MICROS:
        return 0x3C;
      case ROCKSDB_NAMESPACE::Histograms::TABLE_OPEN_PREFETCH_TAIL_READ_BYTES:
        return 0x3D;
      case ROCKSDB_NAMESPACE::Histograms::COMPACTION_PREFETCH_BYTES:
        return 0x3F;
      case ROCKSDB_NAMESPACE::Histograms::HISTOGRAM_ENUM_MAX:
        // 0x3E is reserved for backwards compatibility on current minor
        // version.
        return 0x3E;
      default:
        // undefined/default
        return 0x0;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::Histograms enum for the
  // provided Java org.rocksdb.HistogramsType
  static ROCKSDB_NAMESPACE::Histograms toCppHistograms(jbyte jhistograms_type) {
    switch (jhistograms_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::Histograms::DB_GET;
      case 0x1:
        return ROCKSDB_NAMESPACE::Histograms::DB_WRITE;
      case 0x2:
        return ROCKSDB_NAMESPACE::Histograms::COMPACTION_TIME;
      case 0x3:
        return ROCKSDB_NAMESPACE::Histograms::COMPACTION_CPU_TIME;
      case 0x4:
        return ROCKSDB_NAMESPACE::Histograms::SUBCOMPACTION_SETUP_TIME;
      case 0x5:
        return ROCKSDB_NAMESPACE::Histograms::TABLE_SYNC_MICROS;
      case 0x6:
        return ROCKSDB_NAMESPACE::Histograms::COMPACTION_OUTFILE_SYNC_MICROS;
      case 0x7:
        return ROCKSDB_NAMESPACE::Histograms::WAL_FILE_SYNC_MICROS;
      case 0x8:
        return ROCKSDB_NAMESPACE::Histograms::MANIFEST_FILE_SYNC_MICROS;
      case 0x9:
        return ROCKSDB_NAMESPACE::Histograms::TABLE_OPEN_IO_MICROS;
      case 0xA:
        return ROCKSDB_NAMESPACE::Histograms::DB_MULTIGET;
      case 0xB:
        return ROCKSDB_NAMESPACE::Histograms::READ_BLOCK_COMPACTION_MICROS;
      case 0xC:
        return ROCKSDB_NAMESPACE::Histograms::READ_BLOCK_GET_MICROS;
      case 0xD:
        return ROCKSDB_NAMESPACE::Histograms::WRITE_RAW_BLOCK_MICROS;
      case 0xE:
        return ROCKSDB_NAMESPACE::Histograms::NUM_FILES_IN_SINGLE_COMPACTION;
      case 0xF:
        return ROCKSDB_NAMESPACE::Histograms::DB_SEEK;
      case 0x10:
        return ROCKSDB_NAMESPACE::Histograms::WRITE_STALL;
      case 0x11:
        return ROCKSDB_NAMESPACE::Histograms::SST_READ_MICROS;
      case 0x12:
        return ROCKSDB_NAMESPACE::Histograms::FILE_READ_FLUSH_MICROS;
      case 0x13:
        return ROCKSDB_NAMESPACE::Histograms::FILE_READ_COMPACTION_MICROS;
      case 0x14:
        return ROCKSDB_NAMESPACE::Histograms::FILE_READ_DB_OPEN_MICROS;
      case 0x15:
        return ROCKSDB_NAMESPACE::Histograms::FILE_READ_GET_MICROS;
      case 0x16:
        return ROCKSDB_NAMESPACE::Histograms::FILE_READ_MULTIGET_MICROS;
      case 0x17:
        return ROCKSDB_NAMESPACE::Histograms::FILE_READ_DB_ITERATOR_MICROS;
      case 0x18:
        return ROCKSDB_NAMESPACE::Histograms::
            FILE_READ_VERIFY_DB_CHECKSUM_MICROS;
      case 0x19:
        return ROCKSDB_NAMESPACE::Histograms::
            FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS;
      case 0x1A:
        return ROCKSDB_NAMESPACE::Histograms::SST_WRITE_MICROS;
      case 0x1B:
        return ROCKSDB_NAMESPACE::Histograms::FILE_WRITE_FLUSH_MICROS;
      case 0x1C:
        return ROCKSDB_NAMESPACE::Histograms::FILE_WRITE_COMPACTION_MICROS;
      case 0x1D:
        return ROCKSDB_NAMESPACE::Histograms::FILE_WRITE_DB_OPEN_MICROS;
      case 0x1E:
        return ROCKSDB_NAMESPACE::Histograms::NUM_SUBCOMPACTIONS_SCHEDULED;
      case 0x1F:
        return ROCKSDB_NAMESPACE::Histograms::BYTES_PER_READ;
      case 0x20:
        return ROCKSDB_NAMESPACE::Histograms::BYTES_PER_WRITE;
      case 0x21:
        return ROCKSDB_NAMESPACE::Histograms::BYTES_PER_MULTIGET;
      case 0x22:
        return ROCKSDB_NAMESPACE::Histograms::COMPRESSION_TIMES_NANOS;
      case 0x23:
        return ROCKSDB_NAMESPACE::Histograms::DECOMPRESSION_TIMES_NANOS;
      case 0x24:
        return ROCKSDB_NAMESPACE::Histograms::READ_NUM_MERGE_OPERANDS;
      case 0x25:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_KEY_SIZE;
      case 0x26:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_VALUE_SIZE;
      case 0x27:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_WRITE_MICROS;
      case 0x28:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_GET_MICROS;
      case 0x29:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_MULTIGET_MICROS;
      case 0x2A:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_SEEK_MICROS;
      case 0x2B:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_NEXT_MICROS;
      case 0x2C:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_PREV_MICROS;
      case 0x2D:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS;
      case 0x2E:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_BLOB_FILE_READ_MICROS;
      case 0x2F:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_BLOB_FILE_SYNC_MICROS;
      case 0x30:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_COMPRESSION_MICROS;
      case 0x31:
        return ROCKSDB_NAMESPACE::Histograms::BLOB_DB_DECOMPRESSION_MICROS;
      // 0x20 to skip 0x1F so TICKER_ENUM_MAX remains unchanged for minor
      // version compatibility.
      case 0x32:
        return ROCKSDB_NAMESPACE::Histograms::FLUSH_TIME;
      case 0x33:
        return ROCKSDB_NAMESPACE::Histograms::SST_BATCH_SIZE;
      case 0x34:
        return ROCKSDB_NAMESPACE::Histograms::MULTIGET_IO_BATCH_SIZE;
      case 0x35:
        return ROCKSDB_NAMESPACE::Histograms::
            NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL;
      case 0x36:
        return ROCKSDB_NAMESPACE::Histograms::NUM_SST_READ_PER_LEVEL;
      case 0x37:
        return ROCKSDB_NAMESPACE::Histograms::NUM_LEVEL_READ_PER_MULTIGET;
      case 0x38:
        return ROCKSDB_NAMESPACE::Histograms::
            ERROR_HANDLER_AUTORESUME_RETRY_COUNT;
      case 0x39:
        return ROCKSDB_NAMESPACE::Histograms::ASYNC_READ_BYTES;
      case 0x3A:
        return ROCKSDB_NAMESPACE::Histograms::POLL_WAIT_MICROS;
      case 0x3B:
        return ROCKSDB_NAMESPACE::Histograms::PREFETCHED_BYTES_DISCARDED;
      case 0x3C:
        return ROCKSDB_NAMESPACE::Histograms::ASYNC_PREFETCH_ABORT_MICROS;
      case 0x3D:
        return ROCKSDB_NAMESPACE::Histograms::
            TABLE_OPEN_PREFETCH_TAIL_READ_BYTES;
      case 0x3F:
        return ROCKSDB_NAMESPACE::Histograms::COMPACTION_PREFETCH_BYTES;
      case 0x3E:
        // 0x3E is reserved for backwards compatibility on current minor
        // version.
        return ROCKSDB_NAMESPACE::Histograms::HISTOGRAM_ENUM_MAX;

      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::Histograms::DB_GET;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
