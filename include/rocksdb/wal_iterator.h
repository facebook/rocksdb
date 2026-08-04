//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <vector>

#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {

class WalFile;
using VectorWalPtr = std::vector<std::unique_ptr<WalFile>>;
// DEPRECATED old name
using VectorLogPtr = VectorWalPtr;

enum WalFileType {
  /* Indicates that WAL file is in archive directory. WAL files are moved from
   * the main db directory to archive directory once they are not live and stay
   * there until cleaned up. Files are cleaned depending on archive size
   * (Options::WAL_size_limit_MB) and time since last cleaning
   * (Options::WAL_ttl_seconds).
   */
  kArchivedLogFile = 0,

  /* Indicates that WAL file is live and resides in the main db directory */
  kAliveLogFile = 1
};

class WalFile {
 public:
  WalFile() {}
  virtual ~WalFile() {}

  // Returns log file's pathname relative to the main db dir
  // Eg. For a live-log-file = /000003.log
  //     For an archived-log-file = /archive/000003.log
  virtual std::string PathName() const = 0;

  // Primary identifier for log file.
  // This is directly proportional to creation time of the log file
  virtual uint64_t LogNumber() const = 0;

  // Log file can be either alive or archived
  virtual WalFileType Type() const = 0;

  // Starting sequence number of writebatch written in this log file
  virtual SequenceNumber StartSequence() const = 0;

  // The position of the last flushed write to the file (which for
  // recycled WAL files is typically less than the full file size).
  virtual uint64_t SizeFileBytes() const = 0;
};

// DEPRECATED old name for WalFile. (Confusing with "Logger" etc.)
using LogFile = WalFile;

struct BatchResult {
  // Sequence number of the *first* update in `writeBatchPtr`. The batch
  // covers the sequence number range
  //   [sequence, sequence + writeBatchPtr->Count() - 1]
  // so the next expected sequence number is
  //   sequence + writeBatchPtr->Count()
  // See WalIterator for why a caller might need to compute that itself.
  SequenceNumber sequence = 0;
  std::unique_ptr<WriteBatch> writeBatchPtr;

  // Add empty __ctor and __dtor for the rule of five
  // However, preserve the original semantics and prohibit copying
  // as the std::unique_ptr member does not copy.
  BatchResult() {}

  ~BatchResult() {}

  BatchResult(const BatchResult&) = delete;

  BatchResult& operator=(const BatchResult&) = delete;

  BatchResult(BatchResult&& bResult)
      : sequence(std::move(bResult.sequence)),
        writeBatchPtr(std::move(bResult.writeBatchPtr)) {}

  BatchResult& operator=(BatchResult&& bResult) {
    sequence = std::move(bResult.sequence);
    writeBatchPtr = std::move(bResult.writeBatchPtr);
    return *this;
  }
};

// A WalIterator reads WriteBatches out of a DB's write-ahead log, in
// increasing sequence number order. It is obtained from DB::GetUpdatesSince()
// and is the basis of "WAL tailing": following a DB's writes as they happen,
// for replication, change data capture, and similar.
//
// SOURCE OF DATA -- THIS ONLY SEES THE WAL
//
// This iterator returns exactly what was recorded in the WAL, which is not
// the same as everything that happened to the DB. Notably, these advance the
// DB's sequence number without writing any WAL record:
//   * writes with WriteOptions::disableWAL = true
//   * DB::IngestExternalFile(), when it assigns a sequence number to the
//     ingested file
// These leave permanent holes in the sequence numbers visible here, which
// this API reports as an error (see STATES below) rather than skipping. A DB
// that uses either of them can still be followed up to the first such hole,
// but cannot be followed continuously.
//
// The WAL is also not retained indefinitely. Set Options::WAL_ttl_seconds
// and/or Options::WAL_size_limit_MB large enough to cover how far behind a
// consumer may fall; otherwise WAL files are recycled or deleted aggressively
// and the data a consumer still needs may be gone before it is read.
//
// Not supported for TransactionDB with the WritePrepared or WriteUnprepared
// write policies; GetUpdatesSince() returns Status::NotSupported() for those.
// (Despite the historical name TransactionLogIterator, this API has nothing
// to do with TransactionDB. "Transaction log" is an old synonym for
// write-ahead log.)
//
// STATES
//
// An iterator is in exactly one of three states:
//
//   1. Valid() == true
//      Positioned at a WriteBatch. status() is OK and GetBatch() may be
//      called. Next() advances.
//
//   2. Valid() == false, status() is OK
//      Caught up: everything currently in the WAL at or after the requested
//      sequence number has been returned. This is NOT the end of iteration.
//      More writes may arrive, so a consumer that wants to keep following the
//      DB may call Next() again later, and Valid() may become true again.
//      This is the intended way to tail a DB, but note that it is polling:
//      Next() never waits for new writes, and there is no notification when
//      they arrive. A caught-up Next() returns promptly and leaves the
//      iterator in this same state, so the consumer chooses its own retry
//      interval.
//
//   3. Valid() == false, status() is not OK
//      The run is over and the iterator is spent. status() will not change
//      and Next() has no effect. To continue, discard this iterator and call
//      DB::GetUpdatesSince() again -- but see GAPS below first.
//
// Statuses that end a run:
//   * Status::TryAgain -- not an error. The set of WAL files this iterator
//     was built over has been exhausted while the DB has moved on (typically
//     the WAL was rotated). Build a new iterator to pick up the rest.
//   * Status::NotFound("Gap in sequence numbers") -- a discontinuity was
//     found in the WAL. See GAPS.
//   * Status::Corruption and I/O errors -- the WAL could not be read.
//
// GAPS AND RESUMING A RUN
//
// Within a single run, this iterator stops rather than skipping over a gap in
// sequence numbers, so a run is contiguous and a consumer need not re-check
// that.
//
// What is not guaranteed is the seam between runs. Starting a run is
// permissive: DB::GetUpdatesSince(seq) positions at the next available
// WriteBatch when seq itself is no longer available, and reports no error for
// doing so. Because recovering from any of the run-ending statuses above
// means calling GetUpdatesSince() again, the recovery step is exactly the
// step that can silently skip data.
//
// This matters because a consumer applying these batches to a copy of the DB
// diverges permanently and undetectably if it misses one. So on the first
// batch of each new iterator, check that it resumes where the previous one
// stopped: that BatchResult::sequence equals the last delivered batch's
// sequence plus its WriteBatch::Count(). On a mismatch the intervening
// updates are gone from the WAL, so re-seed from a checkpoint rather than
// resuming.
class WalIterator {
 public:
  WalIterator() {}
  virtual ~WalIterator() {}

  // An iterator is either positioned at a WriteBatch or not valid.
  // This method returns true if the iterator is valid.
  // Can read data from a valid iterator.
  virtual bool Valid() = 0;

  // Moves the iterator to the next WriteBatch.
  //
  // Unlike most RocksDB iterators, this does not require Valid(). Calling
  // Next() on a !Valid() iterator whose status() is OK is how a consumer
  // polls for writes that have happened since it caught up (state 2 above),
  // and is the intended way to tail a DB. It does not wait for new writes.
  // Calling Next() on a !Valid() iterator whose status() is not OK (state 3)
  // has no effect.
  virtual void Next() = 0;

  // Returns OK while the iterator is usable, including when it is merely
  // caught up. Returns the reason the run ended otherwise; see STATES above.
  virtual Status status() = 0;

  // Returns the current write batch and the sequence number of the first
  // update it contains.
  // ONLY use if Valid() is true.
  virtual BatchResult GetBatch() = 0;

  // The read options for WalIterator.
  struct ReadOptions {
    // If true, all data read from underlying storage will be
    // verified against corresponding checksums.
    // Default: true
    bool verify_checksums_;

    ReadOptions() : verify_checksums_(true) {}

    explicit ReadOptions(bool verify_checksums)
        : verify_checksums_(verify_checksums) {}
  };
};

// DEPRECATED old name for WalIterator. This API reads the write-ahead log and
// is unrelated to TransactionDB; "transaction log" is an old synonym for WAL.
using TransactionLogIterator = WalIterator;

}  // namespace ROCKSDB_NAMESPACE
