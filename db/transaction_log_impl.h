//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "rocksdb/transaction_log.h"
#include "db/db_impl.h"
#include "db/log_reader.h"
#include "db/filename.h"

namespace rocksdb {

struct LogReporter : public log::Reader::Reporter {
  Env* env;
  Logger* info_log;
  virtual void Corruption(size_t bytes, const Status& s) {
    Log(info_log, "dropping %zu bytes; %s", bytes, s.ToString().c_str());
  }
  virtual void Info(const char* s) {
    Log(info_log, "%s", s);
  }
};

class LogFileImpl : public LogFile {
 public:
  LogFileImpl(uint64_t logNum, WalFileType logType, SequenceNumber startSeq,
              uint64_t sizeBytes) :
    logNumber_(logNum),
    type_(logType),
    startSequence_(startSeq),
    sizeFileBytes_(sizeBytes) {
  }

  std::string PathName() const {
    if (type_ == kArchivedLogFile) {
      return ArchivedLogFileName("", logNumber_);
    }
    return LogFileName("", logNumber_);
  }

  uint64_t LogNumber() const { return logNumber_; }

  WalFileType Type() const { return type_; }

  SequenceNumber StartSequence() const { return startSequence_; }

  uint64_t SizeFileBytes() const { return sizeFileBytes_; }

  bool operator < (const LogFile& that) const {
    return LogNumber() < that.LogNumber();
  }

 private:
  uint64_t logNumber_;
  WalFileType type_;
  SequenceNumber startSequence_;
  uint64_t sizeFileBytes_;

};

class TransactionLogIteratorImpl : public TransactionLogIterator {
 public:
  TransactionLogIteratorImpl(const std::string& dir,
                             const Options* options,
                             const EnvOptions& soptions,
                             const SequenceNumber seqNum,
                             std::unique_ptr<VectorLogPtr> files,
                             DBImpl const * const dbimpl);

  virtual bool Valid();

  virtual void Next();

  virtual Status status();

  virtual BatchResult GetBatch();

 private:
  const std::string& dir_;
  const Options* options_;
  const EnvOptions& soptions_;
  SequenceNumber startingSequenceNumber_;
  std::unique_ptr<VectorLogPtr> files_;
  bool started_;
  bool isValid_;  // not valid when it starts of.
  Status currentStatus_;
  size_t currentFileIndex_;
  std::unique_ptr<WriteBatch> currentBatch_;
  unique_ptr<log::Reader> currentLogReader_;
  Status OpenLogFile(const LogFile* logFile, unique_ptr<SequentialFile>* file);
  LogReporter reporter_;
  SequenceNumber currentBatchSeq_; // sequence number at start of current batch
  SequenceNumber currentLastSeq_; // last sequence in the current batch
  DBImpl const * const dbimpl_; // The db on whose log files this iterates

  // Reads from transaction log only if the writebatch record has been written
  bool RestrictedRead(Slice* record, std::string* scratch);
  // Seeks to startingSequenceNumber reading from startFileIndex in files_.
  // If strict is set,then must get a batch starting with startingSequenceNumber
  void SeekToStartSequence(uint64_t startFileIndex = 0, bool strict = false);
  // Implementation of Next. SeekToStartSequence calls it internally with
  // internal=true to let it find next entry even if it has to jump gaps because
  // the iterator may start off from the first available entry but promises to
  // be continuous after that
  void NextImpl(bool internal = false);
  // Check if batch is expected, else return false
  bool IsBatchExpected(const WriteBatch* batch, SequenceNumber expectedSeq);
  // Update current batch if a continuous batch is found, else return false
  void UpdateCurrentWriteBatch(const Slice& record);
  Status OpenLogReader(const LogFile* file);
};
}  //  namespace rocksdb
