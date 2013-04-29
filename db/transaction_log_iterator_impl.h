// Copyright 2008-present Facebook. All Rights Reserved.
#ifndef STORAGE_LEVELDB_INCLUDE_WRITES_ITERATOR_IMPL_H_
#define STORAGE_LEVELDB_INCLUDE_WRITES_ITERATOR_IMPL_H_

#include <vector>

#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/types.h"
#include "leveldb/transaction_log_iterator.h"
#include "db/log_file.h"
#include "db/log_reader.h"
#include "util/storage_options.h"

namespace leveldb {

struct LogReporter : public log::Reader::Reporter {
  Env* env;
  Logger* info_log;
  virtual void Corruption(size_t bytes, const Status& s) {
    Log(info_log, "dropping %zu bytes; %s", bytes, s.ToString().c_str());
  }
};

class TransactionLogIteratorImpl : public TransactionLogIterator {
 public:
  TransactionLogIteratorImpl(const std::string& dbname,
                             const Options* options,
                             const StorageOptions& soptions,
                             SequenceNumber& seqNum,
                             std::unique_ptr<std::vector<LogFile>> files,
                             SequenceNumber const * const lastFlushedSequence);

  virtual bool Valid();

  virtual void Next();

  virtual Status status();

  virtual BatchResult GetBatch();

 private:
  const std::string& dbname_;
  const Options* options_;
  const StorageOptions& soptions_;
  const uint64_t startingSequenceNumber_;
  std::unique_ptr<std::vector<LogFile>> files_;
  bool started_;
  bool isValid_;  // not valid when it starts of.
  Status currentStatus_;
  size_t currentFileIndex_;
  std::unique_ptr<WriteBatch> currentBatch_;
  unique_ptr<log::Reader> currentLogReader_;
  Status OpenLogFile(const LogFile& logFile, unique_ptr<SequentialFile>* file);
  LogReporter reporter_;
  SequenceNumber const * const lastFlushedSequence_;
  // represents the sequence number being read currently.
  SequenceNumber currentSequence_;

  void UpdateCurrentWriteBatch(const Slice& record);
  Status OpenLogReader(const LogFile& file);
};



}  //  namespace leveldb
#endif  //  STORAGE_LEVELDB_INCLUDE_WRITES_ITERATOR_IMPL_H_
