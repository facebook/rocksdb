//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Introduction of SyncPoint effectively disabled building and running this test
// in Release build.
// which is a pity, it is a good test
#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "log_format.h"
#include "port/stack_trace.h"
#include "rocksdb/status.h"
#include "rocksdb/transaction_log.h"
#include "test_util/testharness.h"
#include "util/atomic.h"

namespace ROCKSDB_NAMESPACE {

class DBTestXactLogIterator : public DBTestBase {
 public:
  DBTestXactLogIterator()
      : DBTestBase("db_log_iter_test", /*env_do_fsync=*/true) {}

  std::unique_ptr<TransactionLogIterator> OpenTransactionLogIter(
      const SequenceNumber seq, TransactionLogIterator::ReadOptions ro = {}) {
    std::unique_ptr<TransactionLogIterator> iter;
    Status status = dbfull()->GetUpdatesSince(seq, &iter, ro);
    EXPECT_OK(status);
    EXPECT_TRUE(iter->Valid());
    return iter;
  }
};

namespace {
SequenceNumber ReadRecords(std::unique_ptr<TransactionLogIterator>& iter,
                           int& count, bool expect_ok = true) {
  count = 0;
  SequenceNumber lastSequence = 0;
  BatchResult res;
  while (iter->Valid()) {
    res = iter->GetBatch();
    EXPECT_TRUE(res.sequence > lastSequence);
    ++count;
    lastSequence = res.sequence;
    EXPECT_OK(iter->status());
    iter->Next();
  }
  if (expect_ok) {
    EXPECT_OK(iter->status());
  } else {
    EXPECT_NOK(iter->status());
  }
  return res.sequence;
}

void ExpectRecords(const int expected_no_records,
                   std::unique_ptr<TransactionLogIterator>& iter) {
  int num_records;
  ReadRecords(iter, num_records);
  ASSERT_EQ(num_records, expected_no_records);
}

Status GetLogOffset(Options options, std::string wal_file, uint64_t log_num,
                    uint64_t seq, uint64_t* offset) {
  const auto& fs = options.env->GetFileSystem();
  std::unique_ptr<SequentialFileReader> wal_file_reader;
  Status s = SequentialFileReader::Create(fs, wal_file, FileOptions(options),
                                          &wal_file_reader, nullptr, nullptr);
  if (!s.ok()) {
    return s;
  }
  log::Reader reader(options.info_log, std::move(wal_file_reader), nullptr,
                     true, log_num);
  Slice record;
  WriteBatch batch;
  std::string scratch;
  while (reader.ReadRecord(&record, &scratch)) {
    s = WriteBatchInternal::SetContents(&batch, record);
    if (!s.ok()) {
      break;
    }
    auto cur_seq = WriteBatchInternal::Sequence(&batch);
    if (cur_seq > seq) {
      break;
    }
    if (WriteBatchInternal::Sequence(&batch) == seq) {
      *offset = reader.LastRecordOffset();
      return Status::OK();
    }
  }
  return Status::NotFound();
}
}  // anonymous namespace

TEST_F(DBTestXactLogIterator, TransactionLogIterator) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(Put(0, "key1", DummyString(1024)));
    ASSERT_OK(Put(1, "key2", DummyString(1024)));
    ASSERT_OK(Put(1, "key2", DummyString(1024)));
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3U);
    {
      auto iter = OpenTransactionLogIter(0);
      ExpectRecords(3, iter);
    }
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    env_->SleepForMicroseconds(2 * 1000 * 1000);
    {
      ASSERT_OK(Put(0, "key4", DummyString(1024)));
      ASSERT_OK(Put(1, "key5", DummyString(1024)));
      ASSERT_OK(Put(0, "key6", DummyString(1024)));
    }
    {
      auto iter = OpenTransactionLogIter(0);
      ExpectRecords(6, iter);
    }
  } while (ChangeCompactOptions());
}

#ifndef NDEBUG  // sync point is not included with DNDEBUG build
TEST_F(DBTestXactLogIterator, TransactionLogIteratorRace) {
  static const int LOG_ITERATOR_RACE_TEST_COUNT = 2;
  static const char* sync_points[LOG_ITERATOR_RACE_TEST_COUNT][4] = {
      {"WalManager::GetSortedWalFiles:1", "WalManager::PurgeObsoleteFiles:1",
       "WalManager::PurgeObsoleteFiles:2", "WalManager::GetSortedWalFiles:2"},
      {"WalManager::GetSortedWalsOfType:1", "WalManager::PurgeObsoleteFiles:1",
       "WalManager::PurgeObsoleteFiles:2",
       "WalManager::GetSortedWalsOfType:2"}};
  for (int test = 0; test < LOG_ITERATOR_RACE_TEST_COUNT; ++test) {
    // Setup sync point dependency to reproduce the race condition of
    // a log file moved to archived dir, in the middle of GetSortedWalFiles
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
        {sync_points[test][0], sync_points[test][1]},
        {sync_points[test][2], sync_points[test][3]},
    });

    do {
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
      Options options = OptionsForLogIterTest();
      DestroyAndReopen(options);
      ASSERT_OK(Put("key1", DummyString(1024)));
      ASSERT_OK(dbfull()->Flush(FlushOptions()));
      ASSERT_OK(Put("key2", DummyString(1024)));
      ASSERT_OK(dbfull()->Flush(FlushOptions()));
      ASSERT_OK(Put("key3", DummyString(1024)));
      ASSERT_OK(dbfull()->Flush(FlushOptions()));
      ASSERT_OK(Put("key4", DummyString(1024)));
      ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 4U);
      ASSERT_OK(dbfull()->FlushWAL(false));

      {
        auto iter = OpenTransactionLogIter(0);
        ExpectRecords(4, iter);
      }

      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
      // trigger async flush, and log move. Well, log move will
      // wait until the GetSortedWalFiles:1 to reproduce the race
      // condition
      FlushOptions flush_options;
      flush_options.wait = false;
      ASSERT_OK(dbfull()->Flush(flush_options));

      // "key5" would be written in a new memtable and log
      ASSERT_OK(Put("key5", DummyString(1024)));
      ASSERT_OK(dbfull()->FlushWAL(false));
      {
        // this iter would miss "key4" if not fixed
        auto iter = OpenTransactionLogIter(0);
        ExpectRecords(5, iter);
      }
    } while (ChangeCompactOptions());
  }
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorCheckWhenArchive) {
  RelaxedAtomic<bool> callback_hit{};
  do {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    ColumnFamilyHandle* cf;
    auto s = dbfull()->CreateColumnFamily(ColumnFamilyOptions(), "CF", &cf);
    ASSERT_TRUE(s.ok());

    ASSERT_OK(dbfull()->Put(WriteOptions(), cf, "key1", DummyString(1024)));

    ASSERT_OK(dbfull()->Put(WriteOptions(), "key2", DummyString(1024)));

    ASSERT_OK(dbfull()->Flush(FlushOptions()));

    ASSERT_OK(dbfull()->Put(WriteOptions(), "key3", DummyString(1024)));

    ASSERT_OK(dbfull()->Flush(FlushOptions()));

    ASSERT_OK(dbfull()->Put(WriteOptions(), "key4", DummyString(1024)));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));

    callback_hit.StoreRelaxed(false);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "WalManager::PurgeObsoleteFiles:1", [&](void*) {
          auto iter = OpenTransactionLogIter(0);
          ExpectRecords(4, iter);
          callback_hit.StoreRelaxed(true);
        });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(dbfull()->Flush(FlushOptions(), cf));
    delete cf;
    // Normally hit several times; WART: perhaps more in parallel after flush
    // FIXME: this test is flaky
    // ASSERT_TRUE(callback_hit.LoadRelaxed());
  } while (ChangeCompactOptions());
  Close();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}
#endif

TEST_F(DBTestXactLogIterator, TransactionLogIteratorStallAtLastRecord) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", DummyString(1024)));
    auto iter = OpenTransactionLogIter(0);
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_OK(Put("key2", DummyString(1024)));
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
  } while (ChangeCompactOptions());
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorCheckAfterRestart) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", DummyString(1024)));
    ASSERT_OK(Put("key2", DummyString(1023)));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    Reopen(options);
    auto iter = OpenTransactionLogIter(0);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorCorruptedLog) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);

    for (int i = 0; i < 1024; i++) {
      ASSERT_OK(Put("key" + std::to_string(i), DummyString(10)));
    }

    ASSERT_OK(Flush());
    ASSERT_OK(db_->FlushWAL(false));

    // Corrupt this log to create a gap
    ASSERT_OK(db_->DisableFileDeletions());

    VectorLogPtr wal_files;
    ASSERT_OK(db_->GetSortedWalFiles(wal_files));
    ASSERT_FALSE(wal_files.empty());

    const auto logfile_path = dbname_ + "/" + wal_files.front()->PathName();
    ASSERT_OK(test::TruncateFile(env_, logfile_path,
                                 wal_files.front()->SizeFileBytes() / 2));

    ASSERT_OK(db_->EnableFileDeletions());

    // Insert a new entry to a new log file
    ASSERT_OK(Put("key1025", DummyString(10)));
    ASSERT_OK(db_->FlushWAL(false));

    // Try to read from the beginning. Should stop before the gap and read less
    // than 1025 entries
    auto iter = OpenTransactionLogIter(0);
    int count = 0;
    SequenceNumber last_sequence_read = ReadRecords(iter, count, false);
    ASSERT_LT(last_sequence_read, 1025U);

    // Try to read past the gap, should be able to seek to key1025
    auto iter2 = OpenTransactionLogIter(last_sequence_read + 1);
    ExpectRecords(1, iter2);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorBatchOperations) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    WriteBatch batch;
    ASSERT_OK(batch.Put(handles_[1], "key1", DummyString(1024)));
    ASSERT_OK(batch.Put(handles_[0], "key2", DummyString(1024)));
    ASSERT_OK(batch.Put(handles_[1], "key3", DummyString(1024)));
    ASSERT_OK(batch.Delete(handles_[0], "key2"));
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Flush(0));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_OK(Put(1, "key4", DummyString(1024)));
    auto iter = OpenTransactionLogIter(3);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorBlobs) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  {
    WriteBatch batch;
    ASSERT_OK(batch.Put(handles_[1], "key1", DummyString(1024)));
    ASSERT_OK(batch.Put(handles_[0], "key2", DummyString(1024)));
    ASSERT_OK(batch.PutLogData(Slice("blob1")));
    ASSERT_OK(batch.Put(handles_[1], "key3", DummyString(1024)));
    ASSERT_OK(batch.PutLogData(Slice("blob2")));
    ASSERT_OK(batch.Delete(handles_[0], "key2"));
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
  }

  auto res = OpenTransactionLogIter(0)->GetBatch();
  struct Handler : public WriteBatch::Handler {
    std::string seen;
    Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override {
      seen += "Put(" + std::to_string(cf) + ", " + key.ToString() + ", " +
              std::to_string(value.size()) + ")";
      return Status::OK();
    }
    Status MergeCF(uint32_t cf, const Slice& key, const Slice& value) override {
      seen += "Merge(" + std::to_string(cf) + ", " + key.ToString() + ", " +
              std::to_string(value.size()) + ")";
      return Status::OK();
    }
    void LogData(const Slice& blob) override {
      seen += "LogData(" + blob.ToString() + ")";
    }
    Status DeleteCF(uint32_t cf, const Slice& key) override {
      seen += "Delete(" + std::to_string(cf) + ", " + key.ToString() + ")";
      return Status::OK();
    }
  } handler;
  ASSERT_OK(res.writeBatchPtr->Iterate(&handler));
  ASSERT_EQ(
      "Put(1, key1, 1024)"
      "Put(0, key2, 1024)"
      "LogData(blob1)"
      "Put(1, key3, 1024)"
      "LogData(blob2)"
      "Delete(0, key2)",
      handler.seen);
}

TEST_F(DBTestXactLogIterator, TransactionIteratorCache) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  ASSERT_OK(dbfull()->Put({}, "key1", DummyString(log::kBlockSize, 'a')));
  ASSERT_OK(dbfull()->Put({}, "key2", DummyString(log::kBlockSize, 'b')));
  ASSERT_OK(dbfull()->Put({}, "key3", DummyString(log::kBlockSize, 'c')));
  std::atomic_bool hit_cache = false;
  std::atomic_uint64_t sequence = 2;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TransactionLogIteratorImpl:OpenLogReader:Skip", [&](void* arg) {
        hit_cache = true;
        std::unique_ptr<WalFile> wal_file;
        EXPECT_OK(dbfull()->GetCurrentWalFile(&wal_file));
        uint64_t offset = 0;
        EXPECT_OK(GetLogOffset(options, dbname_ + "/" + wal_file->PathName(),
                               wal_file->LogNumber(), sequence, &offset));
        auto skipped_offset = *static_cast<uint64_t*>(arg);
        EXPECT_EQ(skipped_offset, offset / log::kBlockSize * log::kBlockSize);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  TransactionLogIterator::ReadOptions ro{};
  ro.with_cache_ = true;
  std::string batch_data;
  {
    auto iter = OpenTransactionLogIter(sequence, ro);
    ASSERT_TRUE(!hit_cache);
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    batch_data = iter->GetBatch().writeBatchPtr->Data();
  }
  {
    // cache should be hit at the start sequence
    auto iter = OpenTransactionLogIter(sequence, ro);
    ASSERT_TRUE(hit_cache);
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->GetBatch().writeBatchPtr->Data(), batch_data);
    // move on iterator to the end sequence
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    auto batch = iter->GetBatch();
    sequence = batch.sequence;
    batch_data = batch.writeBatchPtr->Data();
    hit_cache = false;
  }
  {
    // cache should be hit at the end sequence
    auto iter = OpenTransactionLogIter(sequence, ro);
    ASSERT_TRUE(hit_cache);
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->GetBatch().writeBatchPtr->Data(), batch_data);
  }
}

}  // namespace ROCKSDB_NAMESPACE


int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
