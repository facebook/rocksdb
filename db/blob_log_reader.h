//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <memory>
#include <stdint.h>

#include "db/blob_log_format.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/options.h"

namespace rocksdb {

class SequentialFileReader;
class Logger;
using std::unique_ptr;

namespace blob_log {

/**
 * Reader is a general purpose log stream reader implementation. The actual job
 * of reading from the device is implemented by the SequentialFile interface.
 *
 * Please see Writer for details on the file and record layout.
 */
class Reader {
 public:
  // Interface for reporting errors.
  class Reporter {
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    virtual void Corruption(size_t bytes, const Status& status) = 0;
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
	 unique_ptr<SequentialFileReader>&& file,
         Reporter* reporter, bool checksum, uint64_t initial_offset,
         uint64_t log_num);

  ~Reader();

  Status ReadHeader(blob_log::BlobLogHeader& header);

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  Status ReadRecord(blob_log::BlobLogRecord& record,
    int level = 0, WALRecoveryMode wal_recovery_mode =
    WALRecoveryMode::kTolerateCorruptedTailRecords);

  SequentialFileReader* file() { return file_.get(); }

 private:
  std::shared_ptr<Logger> info_log_;
  const unique_ptr<SequentialFileReader> file_;
  Reporter* const reporter_;

  char* backing_store_;
  uint64_t bs_size_;
  Slice buffer_;

  // Offset at which to start looking for the first record to return
  uint64_t const initial_offset_;

  // which log number this is
  uint64_t const log_number_;

  // No copying allowed
  Reader(const Reader&);
  void operator=(const Reader&);
};

}  // namespace blob_log
}  // namespace rocksdb
