//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "db/transaction_log_impl.h"

#include <cinttypes>

#include "db/write_batch_internal.h"
#include "file/sequence_file_reader.h"
#include "util/defer.h"

namespace ROCKSDB_NAMESPACE {

TransactionLogIteratorImpl::TransactionLogIteratorImpl(
    const std::string& dir, const ImmutableDBOptions* options,
    const TransactionLogIterator::ReadOptions& read_options,
    const EnvOptions& soptions, const SequenceNumber seq,
    std::unique_ptr<VectorLogPtr> files, VersionSet const* const versions,
    const bool seq_per_batch, const std::shared_ptr<IOTracer>& io_tracer)
    : dir_(dir),
      options_(options),
      read_options_(read_options),
      soptions_(soptions),
      starting_sequence_number_(seq),
      files_(std::move(files)),
      versions_(versions),
      seq_per_batch_(seq_per_batch),
      io_tracer_(io_tracer),
      started_(false),
      is_valid_(false),
      current_file_index_(0),
      current_batch_seq_(0),
      current_last_seq_(0) {
  assert(files_ != nullptr);
  assert(versions_ != nullptr);
  assert(!seq_per_batch_);
  current_status_.PermitUncheckedError();  // Clear on start
  reporter_.env = options_->env;
  reporter_.info_log = options_->info_log.get();
  SeekToStartSequence();  // Seek till starting sequence
}

Status TransactionLogIteratorImpl::OpenLogFile(
    const LogFile* log_file,
    std::unique_ptr<SequentialFileReader>* file_reader) {
  FileSystemPtr fs(options_->fs, io_tracer_);
  std::unique_ptr<FSSequentialFile> file;
  std::string fname;
  Status s;
  EnvOptions optimized_env_options = fs->OptimizeForLogRead(soptions_);
  if (log_file->Type() == kArchivedLogFile) {
    fname = ArchivedLogFileName(dir_, log_file->LogNumber());
    s = fs->NewSequentialFile(fname, optimized_env_options, &file, nullptr);
  } else {
    fname = LogFileName(dir_, log_file->LogNumber());
    s = fs->NewSequentialFile(fname, optimized_env_options, &file, nullptr);
    if (!s.ok()) {
      //  If cannot open file in DB directory.
      //  Try the archive dir, as it could have moved in the meanwhile.
      fname = ArchivedLogFileName(dir_, log_file->LogNumber());
      s = fs->NewSequentialFile(fname, optimized_env_options, &file, nullptr);
    }
  }
  if (s.ok()) {
    file_reader->reset(new SequentialFileReader(std::move(file), fname,
                                                io_tracer_, options_->listeners,
                                                options_->rate_limiter.get()));
  }
  return s;
}

BatchResult TransactionLogIteratorImpl::GetBatch() {
  assert(is_valid_);  //  cannot call in a non valid state.
  BatchResult result;
  result.sequence = current_batch_seq_;
  result.writeBatchPtr = std::move(current_batch_);
  return result;
}

Status TransactionLogIteratorImpl::status() { return current_status_; }

bool TransactionLogIteratorImpl::Valid() { return started_ && is_valid_; }

bool TransactionLogIteratorImpl::RestrictedRead(Slice* record) {
  // Don't read if no more complete entries to read from logs
  if (current_last_seq_ >= versions_->LastSequence()) {
    return false;
  }
  return current_log_reader_->ReadRecord(record, &scratch_);
}

void TransactionLogIteratorImpl::SeekToStartSequence(uint64_t start_file_index,
                                                     bool strict) {
  Slice record;
  started_ = false;
  is_valid_ = false;
  // Check invariant of TransactionLogIterator when SeekToStartSequence()
  // succeeds.
  const Defer defer([this]() {
    if (is_valid_) {
      assert(current_status_.ok());
      if (starting_sequence_number_ > current_batch_seq_) {
        assert(current_batch_seq_ < current_last_seq_);
        assert(current_last_seq_ >= starting_sequence_number_);
      }
    }
  });
  if (files_->size() <= start_file_index) {
    return;
  } else if (!current_status_.ok()) {
    return;
  }
  Status s =
      OpenLogReader(files_->at(static_cast<size_t>(start_file_index)).get());
  if (!s.ok()) {
    current_status_ = s;
    reporter_.Info(current_status_.ToString().c_str());
    return;
  }
  while (RestrictedRead(&record)) {
    if (record.size() < WriteBatchInternal::kHeader) {
      reporter_.Corruption(record.size(),
                           Status::Corruption("very small log record"));
      continue;
    }
    UpdateCurrentWriteBatch(record);
    if (current_last_seq_ >= starting_sequence_number_) {
      if (strict && current_batch_seq_ != starting_sequence_number_) {
        current_status_ = Status::Corruption(
            "Gap in sequence number. Could not "
            "seek to required sequence number");
        reporter_.Info(current_status_.ToString().c_str());
        return;
      } else if (strict) {
        reporter_.Info(
            "Could seek required sequence number. Iterator will "
            "continue.");
      }
      is_valid_ = true;
      started_ = true;  // set started_ as we could seek till starting sequence
      return;
    } else {
      is_valid_ = false;
    }
  }

  // Could not find start sequence in first file. Normally this must be the
  // only file. Otherwise log the error and let the iterator return next entry
  // If strict is set, we want to seek exactly till the start sequence and it
  // should have been present in the file we scanned above
  if (strict) {
    current_status_ = Status::Corruption(
        "Gap in sequence number. Could not "
        "seek to required sequence number");
    reporter_.Info(current_status_.ToString().c_str());
  } else if (files_->size() != 1) {
    current_status_ = Status::Corruption(
        "Start sequence was not found, "
        "skipping to the next available");
    reporter_.Info(current_status_.ToString().c_str());
    // Let NextImpl find the next available entry. started_ remains false
    // because we don't want to check for gaps while moving to start sequence
    NextImpl(true);
  }
}

void TransactionLogIteratorImpl::Next() {
  if (!current_status_.ok()) {
    return;
  }
  return NextImpl(false);
}

void TransactionLogIteratorImpl::NextImpl(bool internal) {
  Slice record;
  is_valid_ = false;
  if (!internal && !started_) {
    // Runs every time until we can seek to the start sequence
    SeekToStartSequence();
  }
  while (true) {
    assert(current_log_reader_);
    if (current_log_reader_->IsEOF()) {
      current_log_reader_->UnmarkEOF();
    }
    while (RestrictedRead(&record)) {
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter_.Corruption(record.size(),
                             Status::Corruption("very small log record"));
        continue;
      } else {
        // started_ should be true if called by application
        assert(internal || started_);
        // started_ should be false if called internally
        assert(!internal || !started_);
        UpdateCurrentWriteBatch(record);
        if (internal && !started_) {
          started_ = true;
        }
        return;
      }
    }

    // Open the next file
    if (current_file_index_ < files_->size() - 1) {
      ++current_file_index_;
      Status s = OpenLogReader(files_->at(current_file_index_).get());
      if (!s.ok()) {
        is_valid_ = false;
        current_status_ = s;
        return;
      }
    } else {
      is_valid_ = false;
      if (current_last_seq_ == versions_->LastSequence()) {
        current_status_ = Status::OK();
      } else {
        const char* msg = "Create a new iterator to fetch the new tail.";
        current_status_ = Status::TryAgain(msg);
      }
      return;
    }
  }
}

bool TransactionLogIteratorImpl::IsBatchExpected(
    const WriteBatch* batch, const SequenceNumber expected_seq) {
  assert(batch);
  SequenceNumber batchSeq = WriteBatchInternal::Sequence(batch);
  if (batchSeq != expected_seq) {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "Discontinuity in log records. Got seq=%" PRIu64
             ", Expected seq=%" PRIu64 ", Last flushed seq=%" PRIu64
             ".Log iterator will reseek the correct batch.",
             batchSeq, expected_seq, versions_->LastSequence());
    reporter_.Info(buf);
    return false;
  }
  return true;
}

void TransactionLogIteratorImpl::UpdateCurrentWriteBatch(const Slice& record) {
  std::unique_ptr<WriteBatch> batch(new WriteBatch());
  Status s = WriteBatchInternal::SetContents(batch.get(), record);
  s.PermitUncheckedError();  // TODO: What should we do with this error?

  SequenceNumber expected_seq = current_last_seq_ + 1;
  // If the iterator has started, then confirm that we get continuous batches
  if (started_ && !IsBatchExpected(batch.get(), expected_seq)) {
    // Seek to the batch having expected sequence number
    if (expected_seq < files_->at(current_file_index_)->StartSequence()) {
      // Expected batch must lie in the previous log file
      // Avoid underflow.
      if (current_file_index_ != 0) {
        current_file_index_--;
      }
    }
    starting_sequence_number_ = expected_seq;
    // currentStatus_ will be set to Ok if reseek succeeds
    // Note: this is still ok in seq_pre_batch_ && two_write_queuesp_ mode
    // that allows gaps in the WAL since it will still skip over the gap.
    current_status_ = Status::NotFound("Gap in sequence numbers");
    // In seq_per_batch_ mode, gaps in the seq are possible so the strict mode
    // should be disabled
    return SeekToStartSequence(current_file_index_, !seq_per_batch_);
  }

  current_batch_seq_ = WriteBatchInternal::Sequence(batch.get());
  assert(!seq_per_batch_);
  current_last_seq_ =
      current_batch_seq_ + WriteBatchInternal::Count(batch.get()) - 1;
  // currentBatchSeq_ can only change here
  assert(current_last_seq_ <= versions_->LastSequence());

  current_batch_ = std::move(batch);
  is_valid_ = true;
  current_status_ = Status::OK();
}

Status TransactionLogIteratorImpl::OpenLogReader(const LogFile* log_file) {
  std::unique_ptr<SequentialFileReader> file;
  Status s = OpenLogFile(log_file, &file);
  if (!s.ok()) {
    return s;
  }
  assert(file);
  current_log_reader_.reset(
      new log::Reader(options_->info_log, std::move(file), &reporter_,
                      read_options_.verify_checksums_, log_file->LogNumber()));
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
