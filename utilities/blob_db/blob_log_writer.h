//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#ifndef ROCKSDB_LITE

#include <cstdint>
#include <memory>
#include <string>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "utilities/blob_db/blob_log_format.h"

namespace rocksdb {

class WritableFileWriter;

namespace blob_db {

/**
 * Writer is the blob log stream writer. It provides an append-only
 * abstraction for writing blob data.
 *
 *
 * Look at blob_db_format.h to see the details of the record formats.
 */

class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(std::unique_ptr<WritableFileWriter>&& dest,
                  uint64_t log_number, uint64_t bpsync, bool use_fsync,
                  uint64_t boffset = 0);
  ~Writer();

  static void ConstructBlobHeader(std::string* headerbuf, const Slice& key,
                                  const Slice& val, int32_t ttl, int64_t ts);

  Status AddRecord(const Slice& key, const Slice& val, uint64_t* key_offset,
                   uint64_t* blob_offset);

  Status AddRecord(const Slice& key, const Slice& val, uint64_t* key_offset,
                   uint64_t* blob_offset, uint32_t ttl);

  Status EmitPhysicalRecord(const std::string& headerbuf, const Slice& key,
                            const Slice& val, uint64_t* key_offset,
                            uint64_t* blob_offset);

  Status AddRecordFooter(const SequenceNumber& sn);

  Status AppendFooter(const BlobLogFooter& footer);

  Status WriteHeader(const BlobLogHeader& header);

  WritableFileWriter* file() { return dest_.get(); }

  const WritableFileWriter* file() const { return dest_.get(); }

  uint64_t get_log_number() const { return log_number_; }

  bool ShouldSync() const { return block_offset_ > next_sync_offset_; }

  void Sync();

  void ResetSyncPointer() { next_sync_offset_ += bytes_per_sync_; }

 private:
  std::unique_ptr<WritableFileWriter> dest_;
  uint64_t log_number_;
  uint64_t block_offset_;  // Current offset in block
  uint64_t bytes_per_sync_;
  uint64_t next_sync_offset_;
  bool use_fsync_;

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];

  // No copying allowed
  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

 public:
  enum ElemType { kEtNone, kEtFileHdr, kEtRecord, kEtFooter, kEtFileFooter };
  ElemType last_elem_type_;
};

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
