// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

namespace rocksdb {

class WriteBatch;

// WALFilter allows an application to inspect write-ahead-log (WAL)
// records or modify their processing on recovery.
// Please see the details below.
class WalFilter {
public:
  enum class WalProcessingOption {
    // Continue processing as usual
    kContinueProcessing = 0,
    // Ignore the current record but continue processing of log(s)
    kIgnoreCurrentRecord = 1,
    // Stop replay of logs and discard logs
    // Logs won't be replayed on subsequent recovery
    kStopReplay = 2,
    // Corrupted record detected by filter
    kCorruptedRecord = 3,
    // Marker for enum count
    kWalProcessingOptionMax = 4
  };

  virtual ~WalFilter() { };

  // LogRecord is invoked for each log record encountered for all the logs
  // during replay on logs on recovery. This method can be used to:
  //  * inspect the record (using the batch parameter)
  //  * ignoring current record 
  //    (by returning WalProcessingOption::kIgnoreCurrentRecord)
  //  * reporting corrupted record
  //    (by returning WalProcessingOption::kCorruptedRecord)
  //  * stop log replay
  //    (by returning kStop replay) - please note that this implies
  //    discarding the logs from current record onwards.
  virtual WalProcessingOption LogRecord(const WriteBatch& batch,
    WriteBatch* new_batch, bool* batch_changed) const = 0;

  // Returns a name that identifies this WAL filter.
  // The name will be printed to LOG file on start up for diagnosis.
  virtual const char* Name() const = 0;
};

}  // namespace rocksdb
