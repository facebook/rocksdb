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

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "utilities/blob_db/blob_log_format.h"

namespace rocksdb {

class SequentialFileReader;
class Logger;

namespace blob_db {

/**
 * Reader is a general purpose log stream reader implementation. The actual job
 * of reading from the device is implemented by the SequentialFile interface.
 *
 * Please see Writer for details on the file and record layout.
 */
class Reader {
 public:
  enum ReadLevel {
    kReadHdrFooter,
    kReadHdrKeyFooter,
    kReadHdrKeyBlobFooter,
  };

  // Create a reader that will return log records from "*file".
  // "*file" must remain live while this Reader is in use.
  //
  // If "reporter" is non-nullptr, it is notified whenever some data is
  // dropped due to a detected corruption.  "*reporter" must remain
  // live while this Reader is in use.
  //
  // If "checksum" is true, verify checksums if available.
  //
  // The Reader will start reading at the first record located at physical
  // position >= initial_offset within the file.
  Reader(std::shared_ptr<Logger> info_log,
         std::unique_ptr<SequentialFileReader>&& file);

  ~Reader();

  Status ReadHeader(BlobLogHeader* header);

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  Status ReadRecord(BlobLogRecord* record, ReadLevel level = kReadHdrFooter,
                    WALRecoveryMode wal_recovery_mode =
                        WALRecoveryMode::kTolerateCorruptedTailRecords);

  SequentialFileReader* file() { return file_.get(); }

  void ResetNextByte() { next_byte_ = 0; }

  uint64_t GetNextByte() const { return next_byte_; }

  const SequentialFileReader* file_reader() const { return file_.get(); }

 private:
  char* GetReadBuffer() { return &(backing_store_[0]); }

 private:
  std::shared_ptr<Logger> info_log_;
  const std::unique_ptr<SequentialFileReader> file_;

  std::string backing_store_;
  Slice buffer_;

  // which byte to read next. For asserting proper usage
  uint64_t next_byte_;

  // No copying allowed
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;
};

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
