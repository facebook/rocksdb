#include "db/transaction_log_iterator_impl.h"
#include "db/write_batch_internal.h"
#include "db/filename.h"

namespace leveldb {

TransactionLogIteratorImpl::TransactionLogIteratorImpl(
                           const std::string& dbname,
                           const Options* options,
                           const StorageOptions& soptions,
                           SequenceNumber& seq,
                           std::vector<LogFile>* files,
                           SequenceNumber const * const lastFlushedSequence) :
  dbname_(dbname),
  options_(options),
  soptions_(soptions),
  sequenceNumber_(seq),
  files_(files),
  started_(false),
  isValid_(true),
  currentFileIndex_(0),
  lastFlushedSequence_(lastFlushedSequence) {
  assert(files_ != nullptr);
  assert(lastFlushedSequence_);
}

LogReporter
TransactionLogIteratorImpl::NewLogReporter(const uint64_t logNumber) {
  LogReporter reporter;
  reporter.env = options_->env;
  reporter.info_log = options_->info_log.get();
  reporter.log_number = logNumber;
  return reporter;
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
//  First seek to the given seqNo. in the current file.
  LogFile currentLogFile = files_->at(currentFileIndex_);
  LogReporter reporter = NewLogReporter(currentLogFile.logNumber);
  std::string scratch;
  Slice record;

  if (!started_) {
    isValid_ = false;
    if (sequenceNumber_ > *lastFlushedSequence_) {
      currentStatus_ = Status::IOError("Looking for a sequence, "
                                        "which is not flushed yet.");
      return;
    }
    unique_ptr<SequentialFile> file;
    Status status = OpenLogFile(currentLogFile, &file);
    if (!status.ok()) {
      currentStatus_ = status;
      return;
    }
    assert(file);
    unique_ptr<log::Reader> reader(
      new log::Reader(std::move(file), &reporter, true, 0));
    assert(reader);
    while (reader->ReadRecord(&record, &scratch)) {
      if (record.size() < 12) {
        reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
        continue;
      }
      UpdateCurrentWriteBatch(record);
      if (currentSequence_ >= sequenceNumber_) {
        assert(currentSequence_ <= *lastFlushedSequence_);
        isValid_ = true;
        currentLogReader_ = std::move(reader);
        break;
      }
    }
    if (!isValid_) {
      //  TODO read the entire first file. and did not find the seq number.
      //  Error out.
      currentStatus_ =
        Status::NotFound("Did not find the Seq no. in first file");
    }
    started_ = true;
  } else {
LOOK_NEXT_FILE:
    assert(currentLogReader_);
    bool openNextFile = true;
    if (currentSequence_ < *lastFlushedSequence_) {
      if (currentLogReader_->IsEOF()) {
        currentLogReader_->UnmarkEOF();
      }
      while (currentLogReader_->ReadRecord(&record, &scratch)) {
        if (record.size() < 12) {
          reporter.Corruption(
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
      if (currentFileIndex_ < files_->size() - 1) {
        ++currentFileIndex_;
        currentLogReader_.reset();
        unique_ptr<SequentialFile> file;
        Status status = OpenLogFile(files_->at(currentFileIndex_), &file);
        if (!status.ok()) {
          isValid_ = false;
          currentStatus_ = status;
          return;
        }
        currentLogReader_.reset(
          new log::Reader(std::move(file), &reporter, true, 0));
        goto LOOK_NEXT_FILE;
      } else if (currentSequence_ == *lastFlushedSequence_) {
        // The last update has been read. and next is being called.
        isValid_ = false;
        currentStatus_ = Status::OK();
      } else {
        //  LOOKED AT FILES. WE ARE DONE HERE.
        isValid_ = false;
        currentStatus_ = Status::IOError(" NO MORE DATA LEFT");
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
}

}  //  namespace leveldb
