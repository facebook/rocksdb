// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_WRITER_H_
#define STORAGE_LEVELDB_DB_LOG_WRITER_H_

#include <memory>
#include <stdint.h>
#include "db/log_format.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace rocksdb {

class WritableFile;

using std::unique_ptr;

namespace log {

class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(unique_ptr<WritableFile>&& dest);
  ~Writer();

  Status AddRecord(const Slice& slice);

  WritableFile* file() { return dest_.get(); }
  const WritableFile* file() const { return dest_.get(); }

 private:
  unique_ptr<WritableFile> dest_;
  int block_offset_;       // Current offset in block

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];

  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  // No copying allowed
  Writer(const Writer&);
  void operator=(const Writer&);
};

}  // namespace log
}  // namespace rocksdb

#endif  // STORAGE_LEVELDB_DB_LOG_WRITER_H_
