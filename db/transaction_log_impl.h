// Copyright 2008-present Facebook. All Rights Reserved.
#pragma once
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "rocksdb/transaction_log.h"
#include "db/log_reader.h"
#include "db/filename.h"

namespace rocksdb {

struct LogReporter : public log::Reader::Reporter {
  Env* env;
  Logger* info_log;
  virtual void Corruption(size_t bytes, const Status& s) {
    Log(info_log, "dropping %zu bytes; %s", bytes, s.ToString().c_str());
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
                             SequenceNumber const * const lastFlushedSequence);

  virtual bool Valid();

  virtual void Next();

  virtual Status status();

  virtual BatchResult GetBatch();

 private:
  const std::string& dir_;
  const Options* options_;
  const EnvOptions& soptions_;
  const SequenceNumber startingSequenceNumber_;
  std::unique_ptr<VectorLogPtr> files_;
  bool started_;
  bool isValid_;  // not valid when it starts of.
  Status currentStatus_;
  size_t currentFileIndex_;
  std::unique_ptr<WriteBatch> currentBatch_;
  unique_ptr<log::Reader> currentLogReader_;
  Status OpenLogFile(const LogFile* logFile, unique_ptr<SequentialFile>* file);
  LogReporter reporter_;
  SequenceNumber const * const lastFlushedSequence_;
  // represents the sequence number being read currently.
  SequenceNumber currentSequence_;

  void UpdateCurrentWriteBatch(const Slice& record);
  Status OpenLogReader(const LogFile* file);
};
}  //  namespace rocksdb
