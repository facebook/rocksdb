//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include <memory>
#include <string>

#include "db/blob/blob_log_format.h"
#include "file/random_access_file_reader.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class SequentialFileReader;
class Logger;

/**
 * BlobLogReader is a general purpose log stream reader implementation. The
 * actual job of reading from the device is implemented by the SequentialFile
 * interface.
 *
 * Please see Writer for details on the file and record layout.
 */
class BlobLogReader {
 public:
  enum ReadLevel {
    kReadHeader,
    kReadHeaderKey,
    kReadHeaderKeyBlob,
  };

  // Create a reader that will return log records from "*file".
  // "*file" must remain live while this BlobLogReader is in use.
  BlobLogReader(std::unique_ptr<RandomAccessFileReader>&& file_reader, Env* env,
                Statistics* statistics);
  // No copying allowed
  BlobLogReader(const BlobLogReader&) = delete;
  BlobLogReader& operator=(const BlobLogReader&) = delete;

  ~BlobLogReader() = default;

  Status ReadHeader(BlobLogHeader* header);

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  // If blob_offset is non-null, return offset of the blob through it.
  Status ReadRecord(BlobLogRecord* record, ReadLevel level = kReadHeader,
                    uint64_t* blob_offset = nullptr);

  Status ReadFooter(BlobLogFooter* footer);

  void ResetNextByte() { next_byte_ = 0; }

  uint64_t GetNextByte() const { return next_byte_; }

 private:
  Status ReadSlice(uint64_t size, Slice* slice, char* buf);

  const std::unique_ptr<RandomAccessFileReader> file_;
  Env* env_;
  Statistics* statistics_;

  Slice buffer_;
  char header_buf_[BlobLogRecord::kHeaderSize];

  // which byte to read next. For asserting proper usage
  uint64_t next_byte_;
};

}  // namespace ROCKSDB_NAMESPACE
