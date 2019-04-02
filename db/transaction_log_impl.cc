//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "db/transaction_log_impl.h"
#include <inttypes.h>
#include "db/write_batch_internal.h"
#include "util/file_reader_writer.h"

namespace rocksdb {

TransactionLogIteratorImpl::TransactionLogIteratorImpl(
    const std::string& dir, const ImmutableDBOptions* options,
    const TransactionLogIterator::ReadOptions& read_options,
    const EnvOptions& soptions, const SequenceNumber seq,
    std::unique_ptr<VectorLogPtr> files, VersionSet const* const versions,
    const bool seq_per_batch)
    : dir_(dir),
      options_(options),
      read_options_(read_options),
      soptions_(soptions),
      starting_sequence_number_(seq),
      files_(std::move(files)),
      started_(false),
      is_valid_(false),
      current_file_index_(0),
      current_batch_seq_(0),
      current_last_seq_(0),
      versions_(versions),
      seq_per_batch_(seq_per_batch) {
  assert(files_ != nullptr);
  assert(versions_ != nullptr);

  reporter_.env = options_->env;
  reporter_.info_log = options_->info_log.get();
  SeekToStartSequence(); // Seek till starting sequence
}

Status TransactionLogIteratorImpl::OpenLogFile(
    const LogFile* log_file,
    std::unique_ptr<SequentialFileReader>* file_reader) {
  Env* env = options_->env;
  std::unique_ptr<SequentialFile> file;
  std::string fname;
  Status s;
  EnvOptions optimized_env_options = env->OptimizeForLogRead(soptions_);
  if (log_file->Type() == kArchivedLogFile) {
    fname = ArchivedLogFileName(dir_, log_file->LogNumber());
    s = env->NewSequentialFile(fname, &file, optimized_env_options);
  } else {
    fname = LogFileName(dir_, log_file->LogNumber());
    s = env->NewSequentialFile(fname, &file, optimized_env_options);
    if (!s.ok()) {
      //  If cannot open file in DB directory.
      //  Try the archive dir, as it could have moved in the meanwhile.
      fname = ArchivedLogFileName(dir_, log_file->LogNumber());
      s = env->NewSequentialFile(fname, &file, optimized_env_options);
    }
  }
  if (s.ok()) {
    file_reader->reset(new SequentialFileReader(std::move(file), fname));
  }
  return s;
}

BatchResult TransactionLogIteratorImpl::GetBatch()  {
  assert(is_valid_);  //  cannot call in a non valid state.
  BatchResult result;
  result.sequence = current_batch_seq_;
  result.writeBatchPtr = std::move(current_batch_);
  return result;
}

Status TransactionLogIteratorImpl::status() { return current_status_; }

bool TransactionLogIteratorImpl::Valid() { return started_ && is_valid_; }

bool TransactionLogIteratorImpl::RestrictedRead(
    Slice* record,
    std::string* scratch) {
  // Don't read if no more complete entries to read from logs
  if (current_last_seq_ >= versions_->LastSequence()) {
    return false;
  }
  return current_log_reader_->ReadRecord(record, scratch);
}

void TransactionLogIteratorImpl::SeekToStartSequence(uint64_t start_file_index,
                                                     bool strict) {
  std::string scratch;
  Slice record;
  started_ = false;
  is_valid_ = false;
  if (files_->size() <= start_file_index) {
    return;
  }
  Status s =
      OpenLogReader(files_->at(static_cast<size_t>(start_file_index)).get());
  if (!s.ok()) {
    current_status_ = s;
    reporter_.Info(current_status_.ToString().c_str());
    return;
  }
  while (RestrictedRead(&record, &scratch)) {
    if (record.size() < WriteBatchInternal::kHeader) {
      reporter_.Corruption(
        record.size(), Status::Corruption("very small log record"));
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
        reporter_.Info("Could seek required sequence number. Iterator will "
                       "continue.");
      }
      is_valid_ = true;
      started_ = true; // set started_ as we could seek till starting sequence
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
  return NextImpl(false);
}

void TransactionLogIteratorImpl::NextImpl(bool internal) {
  std::string scratch;
  Slice record;
  is_valid_ = false;
  if (!internal && !started_) {
    // Runs every time until we can seek to the start sequence
    return SeekToStartSequence();
  }
  while(true) {
    assert(current_log_reader_);
    if (current_log_reader_->IsEOF()) {
      current_log_reader_->UnmarkEOF();
    }
    while (RestrictedRead(&record, &scratch)) {
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter_.Corruption(
          record.size(), Status::Corruption("very small log record"));
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
        current_status_ = Status::Corruption("NO MORE DATA LEFT");
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
  WriteBatchInternal::SetContents(batch.get(), record);

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

  struct BatchCounter : public WriteBatch::Handler {
    SequenceNumber sequence_;
    BatchCounter(SequenceNumber sequence) : sequence_(sequence) {}
    Status MarkNoop(bool empty_batch) override {
      if (!empty_batch) {
        sequence_++;
      }
      return Status::OK();
    }
    Status MarkEndPrepare(const Slice&) override {
      sequence_++;
      return Status::OK();
    }
    Status MarkCommit(const Slice&) override {
      sequence_++;
      return Status::OK();
    }

    Status PutCF(uint32_t /*cf*/, const Slice& /*key*/,
                 const Slice& /*val*/) override {
      return Status::OK();
    }
    Status DeleteCF(uint32_t /*cf*/, const Slice& /*key*/) override {
      return Status::OK();
    }
    Status SingleDeleteCF(uint32_t /*cf*/, const Slice& /*key*/) override {
      return Status::OK();
    }
    Status MergeCF(uint32_t /*cf*/, const Slice& /*key*/,
                   const Slice& /*val*/) override {
      return Status::OK();
    }
    Status MarkBeginPrepare(bool) override { return Status::OK(); }
    Status MarkRollback(const Slice&) override { return Status::OK(); }
  };

  current_batch_seq_ = WriteBatchInternal::Sequence(batch.get());
  if (seq_per_batch_) {
    BatchCounter counter(current_batch_seq_);
    batch->Iterate(&counter);
    current_last_seq_ = counter.sequence_;
  } else {
    current_last_seq_ =
        current_batch_seq_ + WriteBatchInternal::Count(batch.get()) - 1;
  }
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
}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
