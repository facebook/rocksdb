#include "db/transaction_log_iterator_impl.h"
#include "db/write_batch_internal.h"
#include "db/filename.h"

namespace leveldb {

TransactionLogIteratorImpl::TransactionLogIteratorImpl(
                           const std::string& dbname,
                           const Options* options,
                           const StorageOptions& soptions,
                           SequenceNumber& seq,
                           std::unique_ptr<std::vector<LogFile>> files,
                           SequenceNumber const * const lastFlushedSequence) :
  dbname_(dbname),
  options_(options),
  soptions_(soptions),
  startingSequenceNumber_(seq),
  files_(std::move(files)),
  started_(false),
  isValid_(false),
  currentFileIndex_(0),
  lastFlushedSequence_(lastFlushedSequence) {
  assert(files_.get() != nullptr);
  assert(lastFlushedSequence_);

  reporter_.env = options_->env;
  reporter_.info_log = options_->info_log.get();
}

Status TransactionLogIteratorImpl::OpenLogFile(
  const LogFile& logFile,
  unique_ptr<SequentialFile>* file) {
  Env* env = options_->env;
  if (logFile.type == kArchivedLogFile) {
    std::string fname = ArchivedLogFileName(dbname_, logFile.logNumber);
    return env->NewSequentialFile(fname, file, soptions_);
  } else {
    std::string fname = LogFileName(dbname_, logFile.logNumber);
    Status status = env->NewSequentialFile(fname, file, soptions_);
    if (!status.ok()) {
      //  If cannot open file in DB directory.
      //  Try the archive dir, as it could have moved in the meanwhile.
      fname = ArchivedLogFileName(dbname_, logFile.logNumber);
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
  result.sequence = currentSequence_;
  result.writeBatchPtr = std::move(currentBatch_);
  return result;
}

Status TransactionLogIteratorImpl::status() {
  return currentStatus_;
}

bool TransactionLogIteratorImpl::Valid() {
  return started_ && isValid_;
}

void TransactionLogIteratorImpl::Next() {
  LogFile currentLogFile = files_.get()->at(currentFileIndex_);

//  First seek to the given seqNo. in the current file.
  std::string scratch;
  Slice record;
  if (!started_) {
    started_ = true;  // this piece only runs onced.
    isValid_ = false;
    if (startingSequenceNumber_ > *lastFlushedSequence_) {
      currentStatus_ = Status::IOError("Looking for a sequence, "
                                        "which is not flushed yet.");
      return;
    }
    Status s = OpenLogReader(currentLogFile);
    if (!s.ok()) {
      currentStatus_ = s;
      isValid_ = false;
      return;
    }
    while (currentLogReader_->ReadRecord(&record, &scratch)) {
      if (record.size() < 12) {
        reporter_.Corruption(
          record.size(), Status::Corruption("log record too small"));
        continue;
      }
      UpdateCurrentWriteBatch(record);
      if (currentSequence_ >= startingSequenceNumber_) {
        assert(currentSequence_ <= *lastFlushedSequence_);
        isValid_ = true;
        break;
      } else {
        isValid_ = false;
      }
    }
    if (isValid_) {
      // Done for this iteration
      return;
    }
  }
  bool openNextFile = true;
  while(openNextFile) {
    assert(currentLogReader_);
    if (currentSequence_ < *lastFlushedSequence_) {
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
          openNextFile = false;
          break;
        }
      }
    }

    if (openNextFile) {
      if (currentFileIndex_ < files_.get()->size() - 1) {
        ++currentFileIndex_;
        Status status = OpenLogReader(files_.get()->at(currentFileIndex_));
        if (!status.ok()) {
          isValid_ = false;
          currentStatus_ = status;
          return;
        }
      } else {
        isValid_ = false;
        openNextFile = false;
        if (currentSequence_ == *lastFlushedSequence_) {
          currentStatus_ = Status::OK();
        } else {
          currentStatus_ = Status::IOError(" NO MORE DATA LEFT");
        }
      }
    }
  }
}

void TransactionLogIteratorImpl::UpdateCurrentWriteBatch(const Slice& record) {
  WriteBatch* batch = new WriteBatch();
  WriteBatchInternal::SetContents(batch, record);
  currentSequence_ = WriteBatchInternal::Sequence(batch);
  currentBatch_.reset(batch);
  isValid_ = true;
  currentStatus_ = Status::OK();
}

Status TransactionLogIteratorImpl::OpenLogReader(const LogFile& logFile) {
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
}  //  namespace leveldb
