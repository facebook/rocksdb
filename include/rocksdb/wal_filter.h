// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_ROCKSDB_INCLUDE_WAL_FILTER_H_
#define STORAGE_ROCKSDB_INCLUDE_WAL_FILTER_H_

namespace rocksdb {

class WriteBatch;

// WALFilter allows an application to inspect write-ahead-log (WAL)
// records or modify their processing on recovery.
// Please see the details below.
class WALFilter {
public:
  enum class WALProcessingOption {
    // Continue processing as usual
    kContinueProcessing = 0,
    // Ignore the current record but continue processing of log(s)
    kIgnoreCurrentRecord = 1,
    // Stop replay of logs and discard logs
    // Logs won't be replayed on subsequent recovery
    kStopReplay = 2,
    // Marker for enum count
    kWALProcessingOptionMax = 3
  };

  virtual ~WALFilter() { };

  // LogRecord is invoked for each log record encountered for all the logs
  // during replay on logs on recovery. This method can be used to:
  //  * inspect the record (using the batch parameter)
  //  * ignoring current record 
  //    (by returning WALProcessingOption::kIgnoreCurrentRecord)
  //  * stop log replay
  //    (by returning kStop replay) - please note that this implies
  //    discarding the logs from current record onwards.
  virtual WALProcessingOption LogRecord(const WriteBatch & batch) const = 0;

  // Returns a name that identifies this WAL filter.
  // The name will be printed to LOG file on start up for diagnosis.
  virtual const char* Name() const = 0;
};

// Default implementation of WALFilter that does not alter WAL processing
class DefaultWALFilter : WALFilter {
  virtual WALProcessingOption LogRecord(const WriteBatch & batch) const override {
    return WALProcessingOption::kContinueProcessing;
  }

  virtual const char* Name() const override {
    return "DefaultWALFilter";
  }
};

}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_INCLUDE_WAL_FILTER_H_
