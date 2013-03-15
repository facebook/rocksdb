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
  uint64_t log_number;
  virtual void Corruption(size_t bytes, const Status& s) {
    Log(info_log, "%ld: dropping %d bytes; %s",
        log_number, static_cast<int>(bytes), s.ToString().c_str());
  }
};

class TransactionLogIteratorImpl : public TransactionLogIterator {
 public:
  TransactionLogIteratorImpl(const std::string& dbname,
                             const Options* options,
                             const StorageOptions& soptions,
                             SequenceNumber& seqNum,
                             std::vector<LogFile>* files,
                             SequenceNumber const * const lastFlushedSequence);

  virtual ~TransactionLogIteratorImpl() {
    //  TODO move to cc file.
    delete files_;
  }

  virtual bool Valid();

  virtual void Next();

  virtual Status status();

  virtual BatchResult GetBatch();

 private:
  const std::string& dbname_;
  const Options* options_;
  const StorageOptions& soptions_;
  const uint64_t sequenceNumber_;
  const std::vector<LogFile>* files_;
  bool started_;
  bool isValid_;  // not valid when it starts of.
  Status currentStatus_;
  size_t currentFileIndex_;
  std::unique_ptr<WriteBatch> currentBatch_;
  unique_ptr<log::Reader> currentLogReader_;
  Status OpenLogFile(const LogFile& logFile, unique_ptr<SequentialFile>* file);
  LogReporter NewLogReporter(uint64_t logNumber);
  SequenceNumber const * const lastFlushedSequence_;
  // represents the sequence number being read currently.
  SequenceNumber currentSequence_;

  void UpdateCurrentWriteBatch(const Slice& record);
};



}  //  namespace leveldb
#endif  //  STORAGE_LEVELDB_INCLUDE_WRITES_ITERATOR_IMPL_H_
