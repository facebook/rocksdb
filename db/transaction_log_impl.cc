//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "db/transaction_log_impl.h"
#include "db/write_batch_internal.h"

namespace rocksdb {

TransactionLogIteratorImpl::TransactionLogIteratorImpl(
                           const std::string& dir,
                           const Options* options,
                           const EnvOptions& soptions,
                           const SequenceNumber seq,
                           std::unique_ptr<VectorLogPtr> files,
                           SequenceNumber const * const lastFlushedSequence) :
    dir_(dir),
    options_(options),
    soptions_(soptions),
    startingSequenceNumber_(seq),
    files_(std::move(files)),
    started_(false),
    isValid_(false),
    currentFileIndex_(0),
    lastFlushedSequence_(lastFlushedSequence) {
  assert(startingSequenceNumber_ <= *lastFlushedSequence_);
  assert(files_ != nullptr);

  reporter_.env = options_->env;
  reporter_.info_log = options_->info_log.get();
  SeekToStartSequence(); // Seek till starting sequence
}

Status TransactionLogIteratorImpl::OpenLogFile(
  const LogFile* logFile,
  unique_ptr<SequentialFile>* file) {
  Env* env = options_->env;
  if (logFile->Type() == kArchivedLogFile) {
    std::string fname = ArchivedLogFileName(dir_, logFile->LogNumber());
    return env->NewSequentialFile(fname, file, soptions_);
  } else {
    std::string fname = LogFileName(dir_, logFile->LogNumber());
    Status status = env->NewSequentialFile(fname, file, soptions_);
    if (!status.ok()) {
      //  If cannot open file in DB directory.
      //  Try the archive dir, as it could have moved in the meanwhile.
      fname = ArchivedLogFileName(dir_, logFile->LogNumber());
      status = env->NewSequentialFile(fname, file, soptions_);
      if (!status.ok()) {
        return Status::IOError(" Requested file not present in the dir");
      }
    }
    return status;
  }
}

BatchResult TransactionLogIteratorImpl::GetBatch()  {
  assert(isValid_);  //  cannot call in a non valid state.
  BatchResult result;
  result.sequence = currentBatchSeq_;
  result.writeBatchPtr = std::move(currentBatch_);
  return result;
}

Status TransactionLogIteratorImpl::status() {
  return currentStatus_;
}

bool TransactionLogIteratorImpl::Valid() {
  return started_ && isValid_;
}

void TransactionLogIteratorImpl::SeekToStartSequence() {
    std::string scratch;
    Slice record;
    isValid_ = false;
    if (startingSequenceNumber_ > *lastFlushedSequence_) {
      currentStatus_ = Status::IOError("Looking for a sequence, "
                                       "which is not flushed yet.");
      return;
    }
    if (files_->size() == 0) {
      return;
    }
    Status s = OpenLogReader(files_->at(0).get());
    if (!s.ok()) {
      currentStatus_ = s;
      return;
    }
    while (currentLogReader_->ReadRecord(&record, &scratch)) {
      if (record.size() < 12) {
        reporter_.Corruption(
          record.size(), Status::Corruption("log record too small"));
        continue;
      }
      UpdateCurrentWriteBatch(record);
      if (currentBatchSeq_ + currentBatchCount_ - 1 >=
          startingSequenceNumber_) {
        assert(currentBatchSeq_ <= *lastFlushedSequence_);
        isValid_ = true;
        started_ = true; // set started_ as we could seek till starting sequence
        return;
      } else {
        isValid_ = false;
      }
    }
    // Could not find start sequence in first file. Normally this must be the
    // only file. Otherwise log the error and let the iterator return next entry
    if (files_->size() != 1) {
      currentStatus_ = Status::Corruption("Start sequence was not found, "
                                          "skipping to the next available");
      reporter_.Corruption(0, currentStatus_);
      started_ = true; // Let Next find next available entry
      Next();
    }
}

void TransactionLogIteratorImpl::Next() {
  // TODO:Next() says that it requires Valid to be true but this is not true
  // assert(Valid());
  std::string scratch;
  Slice record;
  isValid_ = false;
  if (!started_) {  // Runs every time until we can seek to the start sequence
    return SeekToStartSequence();
  }
  while(true) {
    assert(currentLogReader_);
    if (currentBatchSeq_ < *lastFlushedSequence_) {
      if (currentLogReader_->IsEOF()) {
        currentLogReader_->UnmarkEOF();
      }
      while (currentLogReader_->ReadRecord(&record, &scratch)) {
        if (record.size() < 12) {
          reporter_.Corruption(
            record.size(), Status::Corruption("log record too small"));
          continue;
        } else {
          UpdateCurrentWriteBatch(record);
          return;
        }
      }
    }

    // Open the next file
    if (currentFileIndex_ < files_->size() - 1) {
      ++currentFileIndex_;
      Status status =OpenLogReader(files_->at(currentFileIndex_).get());
      if (!status.ok()) {
        isValid_ = false;
        currentStatus_ = status;
        return;
      }
    } else {
      isValid_ = false;
      if (currentBatchSeq_ == *lastFlushedSequence_) {
        currentStatus_ = Status::OK();
      } else {
        currentStatus_ = Status::IOError(" NO MORE DATA LEFT");
      }
      return;
    }
  }
}

void TransactionLogIteratorImpl::UpdateCurrentWriteBatch(const Slice& record) {
  WriteBatch* batch = new WriteBatch();
  WriteBatchInternal::SetContents(batch, record);
  currentBatchSeq_ = WriteBatchInternal::Sequence(batch);
  currentBatchCount_ = WriteBatchInternal::Count(batch);
  currentBatch_.reset(batch);
  isValid_ = true;
  currentStatus_ = Status::OK();
}

Status TransactionLogIteratorImpl::OpenLogReader(const LogFile* logFile) {
  unique_ptr<SequentialFile> file;
  Status status = OpenLogFile(logFile, &file);
  if (!status.ok()) {
    return status;
  }
  assert(file);
  currentLogReader_.reset(
    new log::Reader(std::move(file), &reporter_, true, 0)
  );
  return Status::OK();
}
}  //  namespace rocksdb
