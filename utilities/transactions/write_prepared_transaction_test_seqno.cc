//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Test to verify that sequence numbers remain consistent during error recovery
// with WritePrepared TransactionDB and two_write_queues=true.
//
// The fix: SyncLastSequenceWithAllocated() is called during ResumeImpl to
// ensure that allocated-but-not-published sequence numbers are accounted for
// before creating new memtables/WALs, preventing "sequence number going
// backwards" corruption on subsequent recovery.

#include <atomic>
#include <memory>
#include <string>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/version_set.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

class WritePreparedTransactionSeqnoTest : public ::testing::Test {
 public:
  WritePreparedTransactionSeqnoTest()
      : db_(nullptr),
        special_env_(Env::Default()),
        fault_fs_(new FaultInjectionTestFS(FileSystem::Default())),
        env_(new CompositeEnvWrapper(&special_env_, fault_fs_)) {
    options_.create_if_missing = true;
    options_.max_write_buffer_number = 2;
    options_.write_buffer_size = 4 * 1024;
    options_.level0_file_num_compaction_trigger = 2;
    options_.env = env_.get();
    // Use two_write_queues which is typical for WritePrepared
    options_.two_write_queues = true;
    // Enable auto recovery from retryable errors
    options_.max_bgerror_resume_count = 2;
    options_.bgerror_resume_retry_interval = 100000;  // 100ms

    dbname_ = test::PerThreadDBPath("write_prepared_seqno_test");
    EXPECT_OK(DestroyDB(dbname_, options_));

    txn_db_options_.transaction_lock_timeout = 0;
    txn_db_options_.default_lock_timeout = 0;
    txn_db_options_.write_policy = TxnDBWritePolicy::WRITE_PREPARED;
  }

  ~WritePreparedTransactionSeqnoTest() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    if (db_) {
      for (auto h : handles_) {
        if (h) {
          EXPECT_OK(db_->DestroyColumnFamilyHandle(h));
        }
      }
      handles_.clear();
      delete db_;
      db_ = nullptr;
    }
  }

  Status Open() {
    return TransactionDB::Open(options_, txn_db_options_, dbname_, &db_);
  }

  void Close() {
    for (auto h : handles_) {
      if (h) {
        EXPECT_OK(db_->DestroyColumnFamilyHandle(h));
      }
    }
    handles_.clear();
    delete db_;
    db_ = nullptr;
  }

  DBImpl* dbimpl() { return static_cast_with_check<DBImpl>(db_->GetRootDB()); }

 protected:
  TransactionDB* db_;
  SpecialEnv special_env_;
  std::shared_ptr<FaultInjectionTestFS> fault_fs_;
  std::unique_ptr<Env> env_;
  std::string dbname_;
  Options options_;
  TransactionDBOptions txn_db_options_;
  std::vector<ColumnFamilyHandle*> handles_;
};

// Regression test: verify that after error recovery with two_write_queues,
// the DB can be closed and reopened without sequence number corruption.
TEST_F(WritePreparedTransactionSeqnoTest,
       SeqnoGoesBackwardsDuringErrorRecovery) {
  ASSERT_OK(Open());

  // Write some initial data and flush to establish baseline
  WriteOptions write_opts;
  TransactionOptions txn_opts;
  for (int i = 0; i < 10; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("txn" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Write more data - these will allocate sequence numbers
  for (int i = 10; i < 20; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("txn" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }

  // Set up sync point dependency chain for deterministic recovery
  // synchronization, following the pattern from
  // ManifestWriteRetryableErrorAutoRecover in error_handler_fs_test.cc.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:BeforeStart",
        "SeqnoGoesBackwardsDuringErrorRecovery:0"},
       {"SeqnoGoesBackwardsDuringErrorRecovery:1",
        "RecoverFromRetryableBGIOError:BeforeWait1"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "SeqnoGoesBackwardsDuringErrorRecovery:2"}});

  // Inject a retryable MANIFEST write error on the next flush
  IOStatus error_to_inject = IOStatus::IOError("Injected MANIFEST error");
  error_to_inject.SetRetryable(true);
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_to_inject); });
  SyncPoint::GetInstance()->EnableProcessing();

  // Trigger a flush that will fail due to MANIFEST write error
  Status s = db_->Flush(FlushOptions());
  ASSERT_NOK(s);

  // Wait for recovery to start, then re-enable filesystem and let it proceed
  TEST_SYNC_POINT("SeqnoGoesBackwardsDuringErrorRecovery:0");
  fault_fs_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::LogAndApply:WriteManifest");
  TEST_SYNC_POINT("SeqnoGoesBackwardsDuringErrorRecovery:1");

  // Wait for recovery to complete
  TEST_SYNC_POINT("SeqnoGoesBackwardsDuringErrorRecovery:2");
  SyncPoint::GetInstance()->DisableProcessing();

  // Write some more data after recovery
  for (int i = 20; i < 30; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("txn_after_" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }

  // Close and reopen - this would fail with "sequence number going backwards"
  // before the fix.
  Close();

  Status reopen_s = Open();
  ASSERT_OK(reopen_s);

  // Verify data integrity
  ReadOptions read_opts;
  for (int i = 0; i < 20; i++) {
    std::string value;
    ASSERT_OK(db_->Get(read_opts, "key" + std::to_string(i), &value));
    ASSERT_EQ(value, "value" + std::to_string(i));
  }

  Close();
}

// Test that verifies the sequence number discrepancy is resolved by checking
// that LastSequence >= LastAllocatedSequence after recovery completes.
TEST_F(WritePreparedTransactionSeqnoTest, SeqnoDiscrepancyDuringErrorRecovery) {
  ASSERT_OK(Open());

  WriteOptions write_opts;
  TransactionOptions txn_opts;

  // Write initial data and flush
  for (int i = 0; i < 5; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("init_txn" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Write more transactions with two_write_queues to potentially create a gap
  // between allocated and published sequence numbers. These must be written
  // before installing the error injection callback, since the small write
  // buffer (4KB) could trigger an automatic flush during these writes.
  for (int i = 5; i < 10; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("txn" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }

  // Track sequence numbers at key points
  std::atomic<uint64_t> last_seq_after_recovery{0};
  std::atomic<uint64_t> last_allocated_seq_after_recovery{0};
  std::atomic<bool> captured_seqs_after{false};

  IOStatus error_to_inject = IOStatus::IOError("Injected error");
  error_to_inject.SetRetryable(true);

  // Set up sync point dependency chain for deterministic synchronization
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:BeforeStart",
        "SeqnoDiscrepancyDuringErrorRecovery:0"},
       {"SeqnoDiscrepancyDuringErrorRecovery:1",
        "RecoverFromRetryableBGIOError:BeforeWait1"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "SeqnoDiscrepancyDuringErrorRecovery:2"}});

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_to_inject); });

  // Capture sequence numbers after recovery completes to verify the fix
  SyncPoint::GetInstance()->SetCallBack(
      "RecoverFromRetryableBGIOError:RecoverSuccess", [&](void*) {
        DBImpl* db_impl = dbimpl();
        if (db_impl) {
          VersionSet* vs = db_impl->GetVersionSet();
          if (vs) {
            last_seq_after_recovery.store(vs->LastSequence());
            last_allocated_seq_after_recovery.store(
                vs->LastAllocatedSequence());
            captured_seqs_after.store(true);
          }
        }
      });

  SyncPoint::GetInstance()->EnableProcessing();

  // Trigger a flush that will fail
  Status flush_s = db_->Flush(FlushOptions());
  ASSERT_NOK(flush_s);

  // Wait for recovery to start, re-enable filesystem, let it proceed
  TEST_SYNC_POINT("SeqnoDiscrepancyDuringErrorRecovery:0");
  fault_fs_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::LogAndApply:WriteManifest");
  TEST_SYNC_POINT("SeqnoDiscrepancyDuringErrorRecovery:1");

  // Wait for recovery to complete
  TEST_SYNC_POINT("SeqnoDiscrepancyDuringErrorRecovery:2");
  SyncPoint::GetInstance()->DisableProcessing();

  // Verify that sequences were captured and are in sync after recovery
  ASSERT_TRUE(captured_seqs_after.load());
  ASSERT_GE(last_seq_after_recovery.load(),
            last_allocated_seq_after_recovery.load())
      << "LastSequence should be >= LastAllocatedSequence after recovery";

  // Close and reopen should succeed without corruption
  Close();
  ASSERT_OK(Open());

  // Verify data integrity
  ReadOptions read_opts;
  for (int i = 0; i < 10; i++) {
    std::string value;
    ASSERT_OK(db_->Get(read_opts, "key" + std::to_string(i), &value));
    ASSERT_EQ(value, "value" + std::to_string(i));
  }

  Close();
}

// Test that verifies SyncLastSequenceWithAllocated is called during ResumeImpl
// by checking sequence numbers before and after the sync point.
TEST_F(WritePreparedTransactionSeqnoTest, ConcurrentWritesDuringErrorRecovery) {
  ASSERT_OK(Open());

  WriteOptions write_opts;
  TransactionOptions txn_opts;

  // Write initial data and flush
  for (int i = 0; i < 5; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("init_txn" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Write more transactions. These must be written before installing the error
  // injection callback, since the small write buffer (4KB) could trigger an
  // automatic flush during these writes.
  for (int i = 5; i < 10; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("txn" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }

  // Track sequence numbers at key points during recovery
  std::atomic<uint64_t> seq_before_resume{0};
  std::atomic<uint64_t> alloc_seq_before_resume{0};
  std::atomic<uint64_t> seq_after_resume{0};
  std::atomic<uint64_t> alloc_seq_after_resume{0};

  IOStatus error_to_inject = IOStatus::IOError("Injected error");
  error_to_inject.SetRetryable(true);

  // Set up sync point dependency chain for deterministic synchronization
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:BeforeStart",
        "ConcurrentWritesDuringErrorRecovery:0"},
       {"ConcurrentWritesDuringErrorRecovery:1",
        "RecoverFromRetryableBGIOError:BeforeWait1"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "ConcurrentWritesDuringErrorRecovery:2"}});

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_to_inject); });

  // Capture sequences right before ResumeImpl runs the sync
  SyncPoint::GetInstance()->SetCallBack("DBImpl::ResumeImpl:Start", [&](void*) {
    DBImpl* db_impl = dbimpl();
    if (db_impl) {
      VersionSet* vs = db_impl->GetVersionSet();
      if (vs) {
        seq_before_resume.store(vs->LastSequence());
        alloc_seq_before_resume.store(vs->LastAllocatedSequence());
      }
    }
  });

  // Capture sequences right after ResumeImpl syncs them
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ResumeImpl:AfterSyncSeq", [&](void*) {
        DBImpl* db_impl = dbimpl();
        if (db_impl) {
          VersionSet* vs = db_impl->GetVersionSet();
          if (vs) {
            seq_after_resume.store(vs->LastSequence());
            alloc_seq_after_resume.store(vs->LastAllocatedSequence());
          }
        }
      });

  SyncPoint::GetInstance()->EnableProcessing();

  // Trigger a flush that will fail
  Status flush_s = db_->Flush(FlushOptions());
  ASSERT_NOK(flush_s);

  // Wait for recovery to start, re-enable filesystem, let it proceed
  TEST_SYNC_POINT("ConcurrentWritesDuringErrorRecovery:0");
  fault_fs_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::LogAndApply:WriteManifest");
  TEST_SYNC_POINT("ConcurrentWritesDuringErrorRecovery:1");

  // Wait for recovery to complete
  TEST_SYNC_POINT("ConcurrentWritesDuringErrorRecovery:2");
  SyncPoint::GetInstance()->DisableProcessing();

  // Verify that the AfterSyncSeq callback fired and sequences are in sync
  ASSERT_GT(seq_after_resume.load(), 0u)
      << "DBImpl::ResumeImpl:AfterSyncSeq callback should have fired";
  ASSERT_EQ(seq_after_resume.load(), alloc_seq_after_resume.load())
      << "Fix should have synced sequences";

  // Close and reopen
  Close();
  ASSERT_OK(Open());

  // Verify data integrity
  ReadOptions read_opts;
  for (int i = 0; i < 10; i++) {
    std::string value;
    ASSERT_OK(db_->Get(read_opts, "key" + std::to_string(i), &value));
    ASSERT_EQ(value, "value" + std::to_string(i));
  }

  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
