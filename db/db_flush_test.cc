//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>
#include <limits>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/sync_point.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

// This is a static filter used for filtering
// kvs during the compaction process.
static std::string NEW_VALUE = "NewValue";

class DBFlushTest : public DBTestBase {
 public:
  DBFlushTest() : DBTestBase("db_flush_test", /*env_do_fsync=*/true) {}
};

class DBFlushDirectIOTest : public DBFlushTest,
                            public ::testing::WithParamInterface<bool> {
 public:
  DBFlushDirectIOTest() : DBFlushTest() {}
};

class DBAtomicFlushTest : public DBFlushTest,
                          public ::testing::WithParamInterface<bool> {
 public:
  DBAtomicFlushTest() : DBFlushTest() {}
};

// We had issue when two background threads trying to flush at the same time,
// only one of them get committed. The test verifies the issue is fixed.
TEST_F(DBFlushTest, FlushWhileWritingManifest) {
  Options options;
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  options.env = env_;
  Reopen(options);
  FlushOptions no_wait;
  no_wait.wait = false;
  no_wait.allow_write_stall = true;

  SyncPoint::GetInstance()->LoadDependency(
      {{"VersionSet::LogAndApply:WriteManifest",
        "DBFlushTest::FlushWhileWritingManifest:1"},
       {"MemTableList::TryInstallMemtableFlushResults:InProgress",
        "VersionSet::LogAndApply:WriteManifestDone"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("foo", "v"));
  ASSERT_OK(dbfull()->Flush(no_wait));
  TEST_SYNC_POINT("DBFlushTest::FlushWhileWritingManifest:1");
  ASSERT_OK(Put("bar", "v"));
  ASSERT_OK(dbfull()->Flush(no_wait));
  // If the issue is hit we will wait here forever.
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
#ifndef ROCKSDB_LITE
  ASSERT_EQ(2, TotalTableFiles());
#endif  // ROCKSDB_LITE
}

// Disable this test temporarily on Travis as it fails intermittently.
// Github issue: #4151
TEST_F(DBFlushTest, SyncFail) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options;
  options.disable_auto_compactions = true;
  options.env = fault_injection_env.get();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBFlushTest::SyncFail:1", "DBImpl::SyncClosedLogs:Start"},
       {"DBImpl::SyncClosedLogs:Failed", "DBFlushTest::SyncFail:2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_OK(Put("key", "value"));
  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_options));
  // Flush installs a new super-version. Get the ref count after that.
  fault_injection_env->SetFilesystemActive(false);
  TEST_SYNC_POINT("DBFlushTest::SyncFail:1");
  TEST_SYNC_POINT("DBFlushTest::SyncFail:2");
  fault_injection_env->SetFilesystemActive(true);
  // Now the background job will do the flush; wait for it.
  // Returns the IO error happend during flush.
  ASSERT_NOK(dbfull()->TEST_WaitForFlushMemTable());
#ifndef ROCKSDB_LITE
  ASSERT_EQ("", FilesPerLevel());  // flush failed.
#endif                             // ROCKSDB_LITE
  Destroy(options);
}

TEST_F(DBFlushTest, SyncSkip) {
  Options options = CurrentOptions();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBFlushTest::SyncSkip:1", "DBImpl::SyncClosedLogs:Skip"},
       {"DBImpl::SyncClosedLogs:Skip", "DBFlushTest::SyncSkip:2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);
  ASSERT_OK(Put("key", "value"));

  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_options));

  TEST_SYNC_POINT("DBFlushTest::SyncSkip:1");
  TEST_SYNC_POINT("DBFlushTest::SyncSkip:2");

  // Now the background job will do the flush; wait for it.
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  Destroy(options);
}

TEST_F(DBFlushTest, FlushInLowPriThreadPool) {
  // Verify setting an empty high-pri (flush) thread pool causes flushes to be
  // scheduled in the low-pri (compaction) thread pool.
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  Reopen(options);
  env_->SetBackgroundThreads(0, Env::HIGH);

  std::thread::id tid;
  int num_flushes = 0, num_compactions = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkFlush", [&](void* /*arg*/) {
        if (tid == std::thread::id()) {
          tid = std::this_thread::get_id();
        } else {
          ASSERT_EQ(tid, std::this_thread::get_id());
        }
        ++num_flushes;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkCompaction", [&](void* /*arg*/) {
        ASSERT_EQ(tid, std::this_thread::get_id());
        ++num_compactions;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key", "val"));
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key", "val"));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(4, num_flushes);
  ASSERT_EQ(1, num_compactions);
}

// Test when flush job is submitted to low priority thread pool and when DB is
// closed in the meanwhile, CloseHelper doesn't hang.
TEST_F(DBFlushTest, CloseDBWhenFlushInLowPri) {
  Options options = CurrentOptions();
  options.max_background_flushes = 1;
  options.max_total_wal_size = 8192;

  DestroyAndReopen(options);
  CreateColumnFamilies({"cf1", "cf2"}, options);

  env_->SetBackgroundThreads(0, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  int num_flushes = 0;

  SyncPoint::GetInstance()->SetCallBack("DBImpl::BGWorkFlush",
                                        [&](void* /*arg*/) { ++num_flushes; });

  int num_low_flush_unscheduled = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::UnscheduleLowFlushCallback", [&](void* /*arg*/) {
        num_low_flush_unscheduled++;
        // There should be one flush job in low pool that needs to be
        // unscheduled
        ASSERT_EQ(num_low_flush_unscheduled, 1);
      });

  int num_high_flush_unscheduled = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::UnscheduleHighFlushCallback", [&](void* /*arg*/) {
        num_high_flush_unscheduled++;
        // There should be no flush job in high pool
        ASSERT_EQ(num_high_flush_unscheduled, 0);
      });

  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(0, "key1", DummyString(8192)));
  // Block thread so that flush cannot be run and can be removed from the queue
  // when called Unschedule.
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  sleeping_task_low.WaitUntilSleeping();

  // Trigger flush and flush job will be scheduled to LOW priority thread.
  ASSERT_OK(Put(0, "key2", DummyString(8192)));

  // Close DB and flush job in low priority queue will be removed without
  // running.
  Close();
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
  ASSERT_EQ(0, num_flushes);

  TryReopenWithColumnFamilies({"default", "cf1", "cf2"}, options);
  ASSERT_OK(Put(0, "key3", DummyString(8192)));
  ASSERT_OK(Flush(0));
  ASSERT_EQ(1, num_flushes);
}

TEST_F(DBFlushTest, ManualFlushWithMinWriteBufferNumberToMerge) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  Reopen(options);

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkFlush",
        "DBFlushTest::ManualFlushWithMinWriteBufferNumberToMerge:1"},
       {"DBFlushTest::ManualFlushWithMinWriteBufferNumberToMerge:2",
        "FlushJob::WriteLevel0Table"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key1", "value1"));

  port::Thread t([&]() {
    // The call wait for flush to finish, i.e. with flush_options.wait = true.
    ASSERT_OK(Flush());
  });

  // Wait for flush start.
  TEST_SYNC_POINT("DBFlushTest::ManualFlushWithMinWriteBufferNumberToMerge:1");
  // Insert a second memtable before the manual flush finish.
  // At the end of the manual flush job, it will check if further flush
  // is needed, but it will not trigger flush of the second memtable because
  // min_write_buffer_number_to_merge is not reached.
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  TEST_SYNC_POINT("DBFlushTest::ManualFlushWithMinWriteBufferNumberToMerge:2");

  // Manual flush should return, without waiting for flush indefinitely.
  t.join();
}

TEST_F(DBFlushTest, ScheduleOnlyOneBgThread) {
  Options options = CurrentOptions();
  Reopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  int called = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MaybeScheduleFlushOrCompaction:AfterSchedule:0", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto unscheduled_flushes = *reinterpret_cast<int*>(arg);
        ASSERT_EQ(0, unscheduled_flushes);
        ++called;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("a", "foo"));
  FlushOptions flush_opts;
  ASSERT_OK(dbfull()->Flush(flush_opts));
  ASSERT_EQ(1, called);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// The following 3 tests are designed for testing garbage statistics at flush
// time.
//
// ======= General Information ======= (from GitHub Wiki).
// There are three scenarios where memtable flush can be triggered:
//
// 1 - Memtable size exceeds ColumnFamilyOptions::write_buffer_size
//     after a write.
// 2 - Total memtable size across all column families exceeds
// DBOptions::db_write_buffer_size,
//     or DBOptions::write_buffer_manager signals a flush. In this scenario
//     the largest memtable will be flushed.
// 3 - Total WAL file size exceeds DBOptions::max_total_wal_size.
//     In this scenario the memtable with the oldest data will be flushed,
//     in order to allow the WAL file with data from this memtable to be
//     purged.
//
// As a result, a memtable can be flushed before it is full. This is one
// reason the generated SST file can be smaller than the corresponding
// memtable. Compression is another factor to make SST file smaller than
// corresponding memtable, since data in memtable is uncompressed.

TEST_F(DBFlushTest, StatisticsGarbageBasic) {
  Options options = CurrentOptions();

  // The following options are used to enforce several values that
  // may already exist as default values to make this test resilient
  // to default value updates in the future.
  options.statistics = CreateDBStatistics();

  // Record all statistics.
  options.statistics->set_stats_level(StatsLevel::kAll);

  // create the DB if it's not already present
  options.create_if_missing = true;

  // Useful for now as we are trying to compare uncompressed data savings on
  // flush().
  options.compression = kNoCompression;

  // Prevent memtable in place updates. Should already be disabled
  // (from Wiki:
  //  In place updates can be enabled by toggling on the bool
  //  inplace_update_support flag. However, this flag is by default set to
  //  false
  //  because this thread-safe in-place update support is not compatible
  //  with concurrent memtable writes. Note that the bool
  //  allow_concurrent_memtable_write is set to true by default )
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 64MB (64MB = 67108864 bytes).
  options.write_buffer_size = 64 << 20;

  ASSERT_OK(TryReopen(options));

  // Put multiple times the same key-values.
  // The encoded length of a db entry in the memtable is
  // defined in db/memtable.cc (MemTable::Add) as the variable:
  // encoded_len=  VarintLength(internal_key_size)  --> =
  // log_256(internal_key).
  // Min # of bytes
  //                                                       necessary to
  //                                                       store
  //                                                       internal_key_size.
  //             + internal_key_size                --> = actual key string,
  //             (size key_size: w/o term null char)
  //                                                      + 8 bytes for
  //                                                      fixed uint64 "seq
  //                                                      number
  // +
  //                                                      insertion type"
  //             + VarintLength(val_size)           --> = min # of bytes to
  //             store val_size
  //             + val_size                         --> = actual value
  //             string
  // For example, in our situation, "key1" : size 4, "value1" : size 6
  // (the terminating null characters are not copied over to the memtable).
  // And therefore encoded_len = 1 + (4+8) + 1 + 6 = 20 bytes per entry.
  // However in terms of raw data contained in the memtable, and written
  // over to the SSTable, we only count internal_key_size and val_size,
  // because this is the only raw chunk of bytes that contains everything
  // necessary to reconstruct a user entry: sequence number, insertion type,
  // key, and value.

  // To test the relevance of our Memtable garbage statistics,
  // namely MEMTABLE_PAYLOAD_BYTES_AT_FLUSH and MEMTABLE_GARBAGE_BYTES_AT_FLUSH,
  // we insert K-V pairs with 3 distinct keys (of length 4),
  // and random values of arbitrary length RAND_VALUES_LENGTH,
  // and we repeat this step NUM_REPEAT times total.
  // At the end, we insert 3 final K-V pairs with the same 3 keys
  // and known values (these will be the final values, of length 6).
  // I chose NUM_REPEAT=2,000 such that no automatic flush is
  // triggered (the number of bytes in the memtable is therefore
  // well below any meaningful heuristic for a memtable of size 64MB).
  // As a result, since each K-V pair is inserted as a payload
  // of N meaningful bytes (sequence number, insertion type,
  // key, and value = 8 + 4 + RAND_VALUE_LENGTH),
  // MEMTABLE_GARBAGE_BYTES_AT_FLUSH should be equal to 2,000 * N bytes
  // and MEMTABLE_PAYLAOD_BYTES_AT_FLUSH = MEMTABLE_GARBAGE_BYTES_AT_FLUSH +
  // (3*(8 + 4 + 6)) bytes. For RAND_VALUE_LENGTH = 172 (arbitrary value), we
  // expect:
  //      N = 8 + 4 + 172 = 184 bytes
  //      MEMTABLE_GARBAGE_BYTES_AT_FLUSH = 2,000 * 184 = 368,000 bytes.
  //      MEMTABLE_PAYLOAD_BYTES_AT_FLUSH = 368,000 + 3*18 = 368,054 bytes.

  const size_t NUM_REPEAT = 2000;
  const size_t RAND_VALUES_LENGTH = 172;
  const std::string KEY1 = "key1";
  const std::string KEY2 = "key2";
  const std::string KEY3 = "key3";
  const std::string VALUE1 = "value1";
  const std::string VALUE2 = "value2";
  const std::string VALUE3 = "value3";
  uint64_t EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH = 0;
  uint64_t EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH = 0;

  Random rnd(301);
  // Insertion of of K-V pairs, multiple times.
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    std::string p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEY1, p_v1));
    ASSERT_OK(Put(KEY2, p_v2));
    ASSERT_OK(Put(KEY3, p_v3));
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY1.size() + p_v1.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY2.size() + p_v2.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY3.size() + p_v3.size() + sizeof(uint64_t);
  }

  // The memtable data bytes includes the "garbage"
  // bytes along with the useful payload.
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH =
      EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH;

  ASSERT_OK(Put(KEY1, VALUE1));
  ASSERT_OK(Put(KEY2, VALUE2));
  ASSERT_OK(Put(KEY3, VALUE3));

  // Add useful payload to the memtable data bytes:
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH +=
      KEY1.size() + VALUE1.size() + KEY2.size() + VALUE2.size() + KEY3.size() +
      VALUE3.size() + 3 * sizeof(uint64_t);

  // We assert that the last K-V pairs have been successfully inserted,
  // and that the valid values are VALUE1, VALUE2, VALUE3.
  PinnableSlice value;
  ASSERT_OK(Get(KEY1, &value));
  ASSERT_EQ(value.ToString(), VALUE1);
  ASSERT_OK(Get(KEY2, &value));
  ASSERT_EQ(value.ToString(), VALUE2);
  ASSERT_OK(Get(KEY3, &value));
  ASSERT_EQ(value.ToString(), VALUE3);

  // Force flush to SST. Increments the statistics counter.
  ASSERT_OK(Flush());

  // Collect statistics.
  uint64_t mem_data_bytes =
      TestGetTickerCount(options, MEMTABLE_PAYLOAD_BYTES_AT_FLUSH);
  uint64_t mem_garbage_bytes =
      TestGetTickerCount(options, MEMTABLE_GARBAGE_BYTES_AT_FLUSH);

  EXPECT_EQ(mem_data_bytes, EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH);
  EXPECT_EQ(mem_garbage_bytes, EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH);

  Close();
}

TEST_F(DBFlushTest, StatisticsGarbageInsertAndDeletes) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;
  options.write_buffer_size = 67108864;

  ASSERT_OK(TryReopen(options));

  const size_t NUM_REPEAT = 2000;
  const size_t RAND_VALUES_LENGTH = 37;
  const std::string KEY1 = "key1";
  const std::string KEY2 = "key2";
  const std::string KEY3 = "key3";
  const std::string KEY4 = "key4";
  const std::string KEY5 = "key5";
  const std::string KEY6 = "key6";

  uint64_t EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH = 0;
  uint64_t EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH = 0;

  WriteBatch batch;

  Random rnd(301);
  // Insertion of of K-V pairs, multiple times.
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    std::string p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEY1, p_v1));
    ASSERT_OK(Put(KEY2, p_v2));
    ASSERT_OK(Put(KEY3, p_v3));
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY1.size() + p_v1.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY2.size() + p_v2.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY3.size() + p_v3.size() + sizeof(uint64_t);
    ASSERT_OK(Delete(KEY1));
    ASSERT_OK(Delete(KEY2));
    ASSERT_OK(Delete(KEY3));
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY1.size() + KEY2.size() + KEY3.size() + 3 * sizeof(uint64_t);
  }

  // The memtable data bytes includes the "garbage"
  // bytes along with the useful payload.
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH =
      EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH;

  // Note : one set of delete for KEY1, KEY2, KEY3 is written to
  // SSTable to propagate the delete operations to K-V pairs
  // that could have been inserted into the database during past Flush
  // opeartions.
  EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH -=
      KEY1.size() + KEY2.size() + KEY3.size() + 3 * sizeof(uint64_t);

  // Additional useful paylaod.
  ASSERT_OK(Delete(KEY4));
  ASSERT_OK(Delete(KEY5));
  ASSERT_OK(Delete(KEY6));

  // // Add useful payload to the memtable data bytes:
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH +=
      KEY4.size() + KEY5.size() + KEY6.size() + 3 * sizeof(uint64_t);

  // We assert that the K-V pairs have been successfully deleted.
  PinnableSlice value;
  ASSERT_NOK(Get(KEY1, &value));
  ASSERT_NOK(Get(KEY2, &value));
  ASSERT_NOK(Get(KEY3, &value));

  // Force flush to SST. Increments the statistics counter.
  ASSERT_OK(Flush());

  // Collect statistics.
  uint64_t mem_data_bytes =
      TestGetTickerCount(options, MEMTABLE_PAYLOAD_BYTES_AT_FLUSH);
  uint64_t mem_garbage_bytes =
      TestGetTickerCount(options, MEMTABLE_GARBAGE_BYTES_AT_FLUSH);

  EXPECT_EQ(mem_data_bytes, EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH);
  EXPECT_EQ(mem_garbage_bytes, EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH);

  Close();
}

TEST_F(DBFlushTest, StatisticsGarbageRangeDeletes) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;
  options.write_buffer_size = 67108864;

  ASSERT_OK(TryReopen(options));

  const size_t NUM_REPEAT = 1000;
  const size_t RAND_VALUES_LENGTH = 42;
  const std::string KEY1 = "key1";
  const std::string KEY2 = "key2";
  const std::string KEY3 = "key3";
  const std::string KEY4 = "key4";
  const std::string KEY5 = "key5";
  const std::string KEY6 = "key6";
  const std::string VALUE3 = "value3";

  uint64_t EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH = 0;
  uint64_t EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH = 0;

  Random rnd(301);
  // Insertion of of K-V pairs, multiple times.
  // Also insert DeleteRange
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    std::string p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEY1, p_v1));
    ASSERT_OK(Put(KEY2, p_v2));
    ASSERT_OK(Put(KEY3, p_v3));
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY1.size() + p_v1.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY2.size() + p_v2.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY3.size() + p_v3.size() + sizeof(uint64_t);
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), KEY1,
                               KEY2));
    // Note: DeleteRange have an exclusive upper bound, e.g. here: [KEY2,KEY3)
    // is deleted.
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), KEY2,
                               KEY3));
    // Delete ranges are stored as a regular K-V pair, with key=STARTKEY,
    // value=ENDKEY.
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        (KEY1.size() + KEY2.size() + sizeof(uint64_t)) +
        (KEY2.size() + KEY3.size() + sizeof(uint64_t));
  }

  // The memtable data bytes includes the "garbage"
  // bytes along with the useful payload.
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH =
      EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH;

  // Note : one set of deleteRange for (KEY1, KEY2) and (KEY2, KEY3) is written
  // to SSTable to propagate the deleteRange operations to K-V pairs that could
  // have been inserted into the database during past Flush opeartions.
  EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH -=
      (KEY1.size() + KEY2.size() + sizeof(uint64_t)) +
      (KEY2.size() + KEY3.size() + sizeof(uint64_t));

  // Overwrite KEY3 with known value (VALUE3)
  // Note that during the whole time KEY3 has never been deleted
  // by the RangeDeletes.
  ASSERT_OK(Put(KEY3, VALUE3));
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH +=
      KEY3.size() + VALUE3.size() + sizeof(uint64_t);

  // Additional useful paylaod.
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), KEY4, KEY5));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), KEY5, KEY6));

  // Add useful payload to the memtable data bytes:
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH +=
      (KEY4.size() + KEY5.size() + sizeof(uint64_t)) +
      (KEY5.size() + KEY6.size() + sizeof(uint64_t));

  // We assert that the K-V pairs have been successfully deleted.
  PinnableSlice value;
  ASSERT_NOK(Get(KEY1, &value));
  ASSERT_NOK(Get(KEY2, &value));
  // And that KEY3's value is correct.
  ASSERT_OK(Get(KEY3, &value));
  ASSERT_EQ(value, VALUE3);

  // Force flush to SST. Increments the statistics counter.
  ASSERT_OK(Flush());

  // Collect statistics.
  uint64_t mem_data_bytes =
      TestGetTickerCount(options, MEMTABLE_PAYLOAD_BYTES_AT_FLUSH);
  uint64_t mem_garbage_bytes =
      TestGetTickerCount(options, MEMTABLE_GARBAGE_BYTES_AT_FLUSH);

  EXPECT_EQ(mem_data_bytes, EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH);
  EXPECT_EQ(mem_garbage_bytes, EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH);

  Close();
}

#ifndef ROCKSDB_LITE
// This simple Listener can only handle one flush at a time.
class TestFlushListener : public EventListener {
 public:
  TestFlushListener(Env* env, DBFlushTest* test)
      : slowdown_count(0), stop_count(0), db_closed(), env_(env), test_(test) {
    db_closed = false;
  }

  ~TestFlushListener() override {
    prev_fc_info_.status.PermitUncheckedError();  // Ignore the status
  }

  void OnTableFileCreated(const TableFileCreationInfo& info) override {
    // remember the info for later checking the FlushJobInfo.
    prev_fc_info_ = info;
    ASSERT_GT(info.db_name.size(), 0U);
    ASSERT_GT(info.cf_name.size(), 0U);
    ASSERT_GT(info.file_path.size(), 0U);
    ASSERT_GT(info.job_id, 0);
    ASSERT_GT(info.table_properties.data_size, 0U);
    ASSERT_GT(info.table_properties.raw_key_size, 0U);
    ASSERT_GT(info.table_properties.raw_value_size, 0U);
    ASSERT_GT(info.table_properties.num_data_blocks, 0U);
    ASSERT_GT(info.table_properties.num_entries, 0U);
    ASSERT_EQ(info.file_checksum, kUnknownFileChecksum);
    ASSERT_EQ(info.file_checksum_func_name, kUnknownFileChecksumFuncName);
  }

  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    flushed_dbs_.push_back(db);
    flushed_column_family_names_.push_back(info.cf_name);
    if (info.triggered_writes_slowdown) {
      slowdown_count++;
    }
    if (info.triggered_writes_stop) {
      stop_count++;
    }
    // verify whether the previously created file matches the flushed file.
    ASSERT_EQ(prev_fc_info_.db_name, db->GetName());
    ASSERT_EQ(prev_fc_info_.cf_name, info.cf_name);
    ASSERT_EQ(prev_fc_info_.job_id, info.job_id);
    ASSERT_EQ(prev_fc_info_.file_path, info.file_path);
    ASSERT_EQ(TableFileNameToNumber(info.file_path), info.file_number);

    // Note: the following chunk relies on the notification pertaining to the
    // database pointed to by DBTestBase::db_, and is thus bypassed when
    // that assumption does not hold (see the test case MultiDBMultiListeners
    // below).
    ASSERT_TRUE(test_);
    if (db == test_->db_) {
      std::vector<std::vector<FileMetaData>> files_by_level;
      test_->dbfull()->TEST_GetFilesMetaData(db->DefaultColumnFamily(),
                                             &files_by_level);

      ASSERT_FALSE(files_by_level.empty());
      auto it = std::find_if(files_by_level[0].begin(), files_by_level[0].end(),
                             [&](const FileMetaData& meta) {
                               return meta.fd.GetNumber() == info.file_number;
                             });
      ASSERT_NE(it, files_by_level[0].end());
      ASSERT_EQ(info.oldest_blob_file_number, it->oldest_blob_file_number);
    }

    ASSERT_EQ(db->GetEnv()->GetThreadID(), info.thread_id);
    ASSERT_GT(info.thread_id, 0U);
  }

  std::vector<std::string> flushed_column_family_names_;
  std::vector<DB*> flushed_dbs_;
  int slowdown_count;
  int stop_count;
  bool db_closing;
  std::atomic_bool db_closed;
  TableFileCreationInfo prev_fc_info_;

 protected:
  Env* env_;
  DBFlushTest* test_;
};
#endif  // !ROCKSDB_LITE

// RocksDB lite does not support GetLiveFiles()
#ifndef ROCKSDB_LITE
TEST_F(DBFlushTest, FixFlushReasonRaceFromConcurrentFlushes) {
  Options options = CurrentOptions();
  options.atomic_flush = true;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"cf1"}, options);

  for (int idx = 0; idx < 1; ++idx) {
    ASSERT_OK(Put(0, Key(idx), std::string(1, 'v')));
    ASSERT_OK(Put(1, Key(idx), std::string(1, 'v')));
  }

  // To coerce a manual flush happenning in the middle of GetLiveFiles's flush,
  // we need to pause background flush thread and enable it later.
  std::shared_ptr<test::SleepingBackgroundTask> sleeping_task =
      std::make_shared<test::SleepingBackgroundTask>();
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 sleeping_task.get(), Env::Priority::HIGH);
  sleeping_task->WaitUntilSleeping();

  // Coerce a manual flush happenning in the middle of GetLiveFiles's flush
  bool get_live_files_paused_at_sync_point = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AtomicFlushMemTables:AfterScheduleFlush", [&](void* /* arg */) {
        if (get_live_files_paused_at_sync_point) {
          // To prevent non-GetLiveFiles() flush from pausing at this sync point
          return;
        }
        get_live_files_paused_at_sync_point = true;

        FlushOptions fo;
        fo.wait = false;
        fo.allow_write_stall = true;
        ASSERT_OK(dbfull()->Flush(fo));

        // Resume background flush thread so GetLiveFiles() can finish
        sleeping_task->WakeUp();
        sleeping_task->WaitUntilDone();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<std::string> files;
  uint64_t manifest_file_size;
  // Before the fix, a race condition on default cf's flush reason due to
  // concurrent GetLiveFiles's flush and manual flush will fail
  // an internal assertion.
  // After the fix, such race condition is fixed and there is no assertion
  // failure.
  ASSERT_OK(db_->GetLiveFiles(files, &manifest_file_size, /*flush*/ true));
  ASSERT_TRUE(get_live_files_paused_at_sync_point);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}
#endif  // !ROCKSDB_LITE

TEST_F(DBFlushTest, MemPurgeBasic) {
  Options options = CurrentOptions();

  // The following options are used to enforce several values that
  // may already exist as default values to make this test resilient
  // to default value updates in the future.
  options.statistics = CreateDBStatistics();

  // Record all statistics.
  options.statistics->set_stats_level(StatsLevel::kAll);

  // create the DB if it's not already present
  options.create_if_missing = true;

  // Useful for now as we are trying to compare uncompressed data savings on
  // flush().
  options.compression = kNoCompression;

  // Prevent memtable in place updates. Should already be disabled
  // (from Wiki:
  //  In place updates can be enabled by toggling on the bool
  //  inplace_update_support flag. However, this flag is by default set to
  //  false
  //  because this thread-safe in-place update support is not compatible
  //  with concurrent memtable writes. Note that the bool
  //  allow_concurrent_memtable_write is set to true by default )
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 64MB (64MB = 67108864 bytes).
  options.write_buffer_size = 1 << 20;
#ifndef ROCKSDB_LITE
  // Initially deactivate the MemPurge prototype.
  options.experimental_mempurge_threshold = 0.0;
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  options.listeners.emplace_back(listener);
#else
  // Activate directly the MemPurge prototype.
  // (RocksDB lite does not support dynamic options)
  options.experimental_mempurge_threshold = 1.0;
#endif  // !ROCKSDB_LITE
  ASSERT_OK(TryReopen(options));

  // RocksDB lite does not support dynamic options
#ifndef ROCKSDB_LITE
  // Dynamically activate the MemPurge prototype without restarting the DB.
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  ASSERT_OK(db_->SetOptions(cfh, {{"experimental_mempurge_threshold", "1.0"}}));
#endif

  std::atomic<uint32_t> mempurge_count{0};
  std::atomic<uint32_t> sst_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { mempurge_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::string KEY1 = "IamKey1";
  std::string KEY2 = "IamKey2";
  std::string KEY3 = "IamKey3";
  std::string KEY4 = "IamKey4";
  std::string KEY5 = "IamKey5";
  std::string KEY6 = "IamKey6";
  std::string KEY7 = "IamKey7";
  std::string KEY8 = "IamKey8";
  std::string KEY9 = "IamKey9";
  std::string RNDKEY1, RNDKEY2, RNDKEY3;
  const std::string NOT_FOUND = "NOT_FOUND";

  // Heavy overwrite workload,
  // more than would fit in maximum allowed memtables.
  Random rnd(719);
  const size_t NUM_REPEAT = 100;
  const size_t RAND_KEYS_LENGTH = 57;
  const size_t RAND_VALUES_LENGTH = 10240;
  std::string p_v1, p_v2, p_v3, p_v4, p_v5, p_v6, p_v7, p_v8, p_v9, p_rv1,
      p_rv2, p_rv3;

  // Insert a very first set of keys that will be
  // mempurged at least once.
  p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v4 = rnd.RandomString(RAND_VALUES_LENGTH);
  ASSERT_OK(Put(KEY1, p_v1));
  ASSERT_OK(Put(KEY2, p_v2));
  ASSERT_OK(Put(KEY3, p_v3));
  ASSERT_OK(Put(KEY4, p_v4));
  ASSERT_EQ(Get(KEY1), p_v1);
  ASSERT_EQ(Get(KEY2), p_v2);
  ASSERT_EQ(Get(KEY3), p_v3);
  ASSERT_EQ(Get(KEY4), p_v4);

  // Insertion of of K-V pairs, multiple times (overwrites).
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    p_v5 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v6 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v7 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v8 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v9 = rnd.RandomString(RAND_VALUES_LENGTH);

    ASSERT_OK(Put(KEY5, p_v5));
    ASSERT_OK(Put(KEY6, p_v6));
    ASSERT_OK(Put(KEY7, p_v7));
    ASSERT_OK(Put(KEY8, p_v8));
    ASSERT_OK(Put(KEY9, p_v9));

    ASSERT_EQ(Get(KEY1), p_v1);
    ASSERT_EQ(Get(KEY2), p_v2);
    ASSERT_EQ(Get(KEY3), p_v3);
    ASSERT_EQ(Get(KEY4), p_v4);
    ASSERT_EQ(Get(KEY5), p_v5);
    ASSERT_EQ(Get(KEY6), p_v6);
    ASSERT_EQ(Get(KEY7), p_v7);
    ASSERT_EQ(Get(KEY8), p_v8);
    ASSERT_EQ(Get(KEY9), p_v9);
  }

  // Check that there was at least one mempurge
  const uint32_t EXPECTED_MIN_MEMPURGE_COUNT = 1;
  // Check that there was no SST files created during flush.
  const uint32_t EXPECTED_SST_COUNT = 0;

  EXPECT_GE(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);
  EXPECT_EQ(sst_count.exchange(0), EXPECTED_SST_COUNT);

  // Insertion of of K-V pairs, no overwrites.
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    RNDKEY1 = rnd.RandomString(RAND_KEYS_LENGTH);
    RNDKEY2 = rnd.RandomString(RAND_KEYS_LENGTH);
    RNDKEY3 = rnd.RandomString(RAND_KEYS_LENGTH);
    p_rv1 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_rv2 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_rv3 = rnd.RandomString(RAND_VALUES_LENGTH);

    ASSERT_OK(Put(RNDKEY1, p_rv1));
    ASSERT_OK(Put(RNDKEY2, p_rv2));
    ASSERT_OK(Put(RNDKEY3, p_rv3));

    ASSERT_EQ(Get(KEY1), p_v1);
    ASSERT_EQ(Get(KEY2), p_v2);
    ASSERT_EQ(Get(KEY3), p_v3);
    ASSERT_EQ(Get(KEY4), p_v4);
    ASSERT_EQ(Get(KEY5), p_v5);
    ASSERT_EQ(Get(KEY6), p_v6);
    ASSERT_EQ(Get(KEY7), p_v7);
    ASSERT_EQ(Get(KEY8), p_v8);
    ASSERT_EQ(Get(KEY9), p_v9);
    ASSERT_EQ(Get(RNDKEY1), p_rv1);
    ASSERT_EQ(Get(RNDKEY2), p_rv2);
    ASSERT_EQ(Get(RNDKEY3), p_rv3);
  }

  // Assert that at least one flush to storage has been performed
  EXPECT_GT(sst_count.exchange(0), EXPECTED_SST_COUNT);
  // (which will consequently increase the number of mempurges recorded too).
  EXPECT_GE(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);

  // Assert that there is no data corruption, even with
  // a flush to storage.
  ASSERT_EQ(Get(KEY1), p_v1);
  ASSERT_EQ(Get(KEY2), p_v2);
  ASSERT_EQ(Get(KEY3), p_v3);
  ASSERT_EQ(Get(KEY4), p_v4);
  ASSERT_EQ(Get(KEY5), p_v5);
  ASSERT_EQ(Get(KEY6), p_v6);
  ASSERT_EQ(Get(KEY7), p_v7);
  ASSERT_EQ(Get(KEY8), p_v8);
  ASSERT_EQ(Get(KEY9), p_v9);
  ASSERT_EQ(Get(RNDKEY1), p_rv1);
  ASSERT_EQ(Get(RNDKEY2), p_rv2);
  ASSERT_EQ(Get(RNDKEY3), p_rv3);

  Close();
}

// RocksDB lite does not support dynamic options
#ifndef ROCKSDB_LITE
TEST_F(DBFlushTest, MemPurgeBasicToggle) {
  Options options = CurrentOptions();

  // The following options are used to enforce several values that
  // may already exist as default values to make this test resilient
  // to default value updates in the future.
  options.statistics = CreateDBStatistics();

  // Record all statistics.
  options.statistics->set_stats_level(StatsLevel::kAll);

  // create the DB if it's not already present
  options.create_if_missing = true;

  // Useful for now as we are trying to compare uncompressed data savings on
  // flush().
  options.compression = kNoCompression;

  // Prevent memtable in place updates. Should already be disabled
  // (from Wiki:
  //  In place updates can be enabled by toggling on the bool
  //  inplace_update_support flag. However, this flag is by default set to
  //  false
  //  because this thread-safe in-place update support is not compatible
  //  with concurrent memtable writes. Note that the bool
  //  allow_concurrent_memtable_write is set to true by default )
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 64MB (64MB = 67108864 bytes).
  options.write_buffer_size = 1 << 20;
  // Initially deactivate the MemPurge prototype.
  // (negative values are equivalent to 0.0).
  options.experimental_mempurge_threshold = -25.3;
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  options.listeners.emplace_back(listener);

  ASSERT_OK(TryReopen(options));
  // Dynamically activate the MemPurge prototype without restarting the DB.
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  // Values greater than 1.0 are equivalent to 1.0
  ASSERT_OK(
      db_->SetOptions(cfh, {{"experimental_mempurge_threshold", "3.7898"}}));
  std::atomic<uint32_t> mempurge_count{0};
  std::atomic<uint32_t> sst_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { mempurge_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  const size_t KVSIZE = 3;
  std::vector<std::string> KEYS(KVSIZE);
  for (size_t k = 0; k < KVSIZE; k++) {
    KEYS[k] = "IamKey" + std::to_string(k);
  }

  std::vector<std::string> RNDVALS(KVSIZE);
  const std::string NOT_FOUND = "NOT_FOUND";

  // Heavy overwrite workload,
  // more than would fit in maximum allowed memtables.
  Random rnd(719);
  const size_t NUM_REPEAT = 100;
  const size_t RAND_VALUES_LENGTH = 10240;

  // Insertion of of K-V pairs, multiple times (overwrites).
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    for (size_t j = 0; j < KEYS.size(); j++) {
      RNDVALS[j] = rnd.RandomString(RAND_VALUES_LENGTH);
      ASSERT_OK(Put(KEYS[j], RNDVALS[j]));
      ASSERT_EQ(Get(KEYS[j]), RNDVALS[j]);
    }
    for (size_t j = 0; j < KEYS.size(); j++) {
      ASSERT_EQ(Get(KEYS[j]), RNDVALS[j]);
    }
  }

  // Check that there was at least one mempurge
  const uint32_t EXPECTED_MIN_MEMPURGE_COUNT = 1;
  // Check that there was no SST files created during flush.
  const uint32_t EXPECTED_SST_COUNT = 0;

  EXPECT_GE(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);
  EXPECT_EQ(sst_count.exchange(0), EXPECTED_SST_COUNT);

  // Dynamically deactivate MemPurge.
  ASSERT_OK(
      db_->SetOptions(cfh, {{"experimental_mempurge_threshold", "-1023.0"}}));

  // Insertion of of K-V pairs, multiple times (overwrites).
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    for (size_t j = 0; j < KEYS.size(); j++) {
      RNDVALS[j] = rnd.RandomString(RAND_VALUES_LENGTH);
      ASSERT_OK(Put(KEYS[j], RNDVALS[j]));
      ASSERT_EQ(Get(KEYS[j]), RNDVALS[j]);
    }
    for (size_t j = 0; j < KEYS.size(); j++) {
      ASSERT_EQ(Get(KEYS[j]), RNDVALS[j]);
    }
  }

  // Check that there was at least one mempurge
  const uint32_t ZERO = 0;
  // Assert that at least one flush to storage has been performed
  EXPECT_GT(sst_count.exchange(0), EXPECTED_SST_COUNT);
  // The mempurge count is expected to be set to 0 when the options are updated.
  // We expect no mempurge at all.
  EXPECT_EQ(mempurge_count.exchange(0), ZERO);

  Close();
}
// Closes the "#ifndef ROCKSDB_LITE"
// End of MemPurgeBasicToggle, which is not
// supported with RocksDB LITE because it
// relies on dynamically changing the option
// flag experimental_mempurge_threshold.
#endif

// At the moment, MemPurge feature is deactivated
// when atomic_flush is enabled. This is because the level
// of garbage between Column Families is not guaranteed to
// be consistent, therefore a CF could hypothetically
// trigger a MemPurge while another CF would trigger
// a regular Flush.
TEST_F(DBFlushTest, MemPurgeWithAtomicFlush) {
  Options options = CurrentOptions();

  // The following options are used to enforce several values that
  // may already exist as default values to make this test resilient
  // to default value updates in the future.
  options.statistics = CreateDBStatistics();

  // Record all statistics.
  options.statistics->set_stats_level(StatsLevel::kAll);

  // create the DB if it's not already present
  options.create_if_missing = true;

  // Useful for now as we are trying to compare uncompressed data savings on
  // flush().
  options.compression = kNoCompression;

  // Prevent memtable in place updates. Should already be disabled
  // (from Wiki:
  //  In place updates can be enabled by toggling on the bool
  //  inplace_update_support flag. However, this flag is by default set to
  //  false
  //  because this thread-safe in-place update support is not compatible
  //  with concurrent memtable writes. Note that the bool
  //  allow_concurrent_memtable_write is set to true by default )
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 64KB (64KB = 65,536 bytes).
  options.write_buffer_size = 1 << 20;
  // Activate the MemPurge prototype.
  options.experimental_mempurge_threshold = 153.245;
  // Activate atomic_flush.
  options.atomic_flush = true;

  const std::vector<std::string> new_cf_names = {"pikachu", "eevie"};
  CreateColumnFamilies(new_cf_names, options);

  Close();

  // 3 CFs: default will be filled with overwrites (would normally trigger
  // mempurge)
  //        new_cf_names[1] will be filled with random values (would trigger
  //        flush) new_cf_names[2] not filled with anything.
  ReopenWithColumnFamilies(
      {kDefaultColumnFamilyName, new_cf_names[0], new_cf_names[1]}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(2, "bar", "baz"));

  std::atomic<uint32_t> mempurge_count{0};
  std::atomic<uint32_t> sst_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { mempurge_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  const size_t KVSIZE = 3;
  std::vector<std::string> KEYS(KVSIZE);
  for (size_t k = 0; k < KVSIZE; k++) {
    KEYS[k] = "IamKey" + std::to_string(k);
  }

  std::string RNDKEY;
  std::vector<std::string> RNDVALS(KVSIZE);
  const std::string NOT_FOUND = "NOT_FOUND";

  // Heavy overwrite workload,
  // more than would fit in maximum allowed memtables.
  Random rnd(106);
  const size_t NUM_REPEAT = 100;
  const size_t RAND_KEY_LENGTH = 128;
  const size_t RAND_VALUES_LENGTH = 10240;

  // Insertion of of K-V pairs, multiple times (overwrites).
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    for (size_t j = 0; j < KEYS.size(); j++) {
      RNDKEY = rnd.RandomString(RAND_KEY_LENGTH);
      RNDVALS[j] = rnd.RandomString(RAND_VALUES_LENGTH);
      ASSERT_OK(Put(KEYS[j], RNDVALS[j]));
      ASSERT_OK(Put(1, RNDKEY, RNDVALS[j]));
      ASSERT_EQ(Get(KEYS[j]), RNDVALS[j]);
      ASSERT_EQ(Get(1, RNDKEY), RNDVALS[j]);
    }
  }

  // Check that there was no mempurge because atomic_flush option is true.
  const uint32_t EXPECTED_MIN_MEMPURGE_COUNT = 0;
  // Check that there was at least one SST files created during flush.
  const uint32_t EXPECTED_SST_COUNT = 1;

  EXPECT_EQ(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);
  EXPECT_GE(sst_count.exchange(0), EXPECTED_SST_COUNT);

  Close();
}

TEST_F(DBFlushTest, MemPurgeDeleteAndDeleteRange) {
  Options options = CurrentOptions();

  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;
#ifndef ROCKSDB_LITE
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  options.listeners.emplace_back(listener);
#endif  // !ROCKSDB_LITE
  // Enforce size of a single MemTable to 64MB (64MB = 67108864 bytes).
  options.write_buffer_size = 1 << 20;
  // Activate the MemPurge prototype.
  options.experimental_mempurge_threshold = 15.0;

  ASSERT_OK(TryReopen(options));

  std::atomic<uint32_t> mempurge_count{0};
  std::atomic<uint32_t> sst_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { mempurge_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::string KEY1 = "ThisIsKey1";
  std::string KEY2 = "ThisIsKey2";
  std::string KEY3 = "ThisIsKey3";
  std::string KEY4 = "ThisIsKey4";
  std::string KEY5 = "ThisIsKey5";
  const std::string NOT_FOUND = "NOT_FOUND";

  Random rnd(117);
  const size_t NUM_REPEAT = 100;
  const size_t RAND_VALUES_LENGTH = 10240;

  std::string key, value, p_v1, p_v2, p_v3, p_v3b, p_v4, p_v5;
  int count = 0;
  const int EXPECTED_COUNT_FORLOOP = 3;
  const int EXPECTED_COUNT_END = 4;

  ReadOptions ropt;
  ropt.pin_data = true;
  ropt.total_order_seek = true;
  Iterator* iter = nullptr;

  // Insertion of of K-V pairs, multiple times.
  // Also insert DeleteRange
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v3b = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v4 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v5 = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEY1, p_v1));
    ASSERT_OK(Put(KEY2, p_v2));
    ASSERT_OK(Put(KEY3, p_v3));
    ASSERT_OK(Put(KEY4, p_v4));
    ASSERT_OK(Put(KEY5, p_v5));
    ASSERT_OK(Delete(KEY2));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), KEY2,
                               KEY4));
    ASSERT_OK(Put(KEY3, p_v3b));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), KEY1,
                               KEY3));
    ASSERT_OK(Delete(KEY1));

    ASSERT_EQ(Get(KEY1), NOT_FOUND);
    ASSERT_EQ(Get(KEY2), NOT_FOUND);
    ASSERT_EQ(Get(KEY3), p_v3b);
    ASSERT_EQ(Get(KEY4), p_v4);
    ASSERT_EQ(Get(KEY5), p_v5);

    iter = db_->NewIterator(ropt);
    iter->SeekToFirst();
    count = 0;
    for (; iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      key = (iter->key()).ToString(false);
      value = (iter->value()).ToString(false);
      if (key.compare(KEY3) == 0)
        ASSERT_EQ(value, p_v3b);
      else if (key.compare(KEY4) == 0)
        ASSERT_EQ(value, p_v4);
      else if (key.compare(KEY5) == 0)
        ASSERT_EQ(value, p_v5);
      else
        ASSERT_EQ(value, NOT_FOUND);
      count++;
    }

    // Expected count here is 3: KEY3, KEY4, KEY5.
    ASSERT_EQ(count, EXPECTED_COUNT_FORLOOP);
    if (iter) {
      delete iter;
    }
  }

  // Check that there was at least one mempurge
  const uint32_t EXPECTED_MIN_MEMPURGE_COUNT = 1;
  // Check that there was no SST files created during flush.
  const uint32_t EXPECTED_SST_COUNT = 0;

  EXPECT_GE(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);
  EXPECT_EQ(sst_count.exchange(0), EXPECTED_SST_COUNT);

  // Additional test for the iterator+memPurge.
  ASSERT_OK(Put(KEY2, p_v2));
  iter = db_->NewIterator(ropt);
  iter->SeekToFirst();
  ASSERT_OK(Put(KEY4, p_v4));
  count = 0;
  for (; iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    key = (iter->key()).ToString(false);
    value = (iter->value()).ToString(false);
    if (key.compare(KEY2) == 0)
      ASSERT_EQ(value, p_v2);
    else if (key.compare(KEY3) == 0)
      ASSERT_EQ(value, p_v3b);
    else if (key.compare(KEY4) == 0)
      ASSERT_EQ(value, p_v4);
    else if (key.compare(KEY5) == 0)
      ASSERT_EQ(value, p_v5);
    else
      ASSERT_EQ(value, NOT_FOUND);
    count++;
  }

  // Expected count here is 4: KEY2, KEY3, KEY4, KEY5.
  ASSERT_EQ(count, EXPECTED_COUNT_END);
  if (iter) delete iter;

  Close();
}

// Create a Compaction Fitler that will be invoked
// at flush time and will update the value of a KV pair
// if the key string is "lower" than the filter_key_ string.
class ConditionalUpdateFilter : public CompactionFilter {
 public:
  explicit ConditionalUpdateFilter(const std::string* filtered_key)
      : filtered_key_(filtered_key) {}
  bool Filter(int /*level*/, const Slice& key, const Slice& /*value*/,
              std::string* new_value, bool* value_changed) const override {
    // If key<filtered_key_, update the value of the KV-pair.
    if (key.compare(*filtered_key_) < 0) {
      assert(new_value != nullptr);
      *new_value = NEW_VALUE;
      *value_changed = true;
    }
    return false /*do not remove this KV-pair*/;
  }

  const char* Name() const override { return "ConditionalUpdateFilter"; }

 private:
  const std::string* filtered_key_;
};

class ConditionalUpdateFilterFactory : public CompactionFilterFactory {
 public:
  explicit ConditionalUpdateFilterFactory(const Slice& filtered_key)
      : filtered_key_(filtered_key.ToString()) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::unique_ptr<CompactionFilter>(
        new ConditionalUpdateFilter(&filtered_key_));
  }

  const char* Name() const override { return "ConditionalUpdateFilterFactory"; }

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    // This compaction filter will be invoked
    // at flush time (and therefore at MemPurge time).
    return (reason == TableFileCreationReason::kFlush);
  }

 private:
  std::string filtered_key_;
};

TEST_F(DBFlushTest, MemPurgeAndCompactionFilter) {
  Options options = CurrentOptions();

  std::string KEY1 = "ThisIsKey1";
  std::string KEY2 = "ThisIsKey2";
  std::string KEY3 = "ThisIsKey3";
  std::string KEY4 = "ThisIsKey4";
  std::string KEY5 = "ThisIsKey5";
  std::string KEY6 = "ThisIsKey6";
  std::string KEY7 = "ThisIsKey7";
  std::string KEY8 = "ThisIsKey8";
  std::string KEY9 = "ThisIsKey9";
  const std::string NOT_FOUND = "NOT_FOUND";

  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;
#ifndef ROCKSDB_LITE
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  options.listeners.emplace_back(listener);
#endif  // !ROCKSDB_LITE
  // Create a ConditionalUpdate compaction filter
  // that will update all the values of the KV pairs
  // where the keys are "lower" than KEY4.
  options.compaction_filter_factory =
      std::make_shared<ConditionalUpdateFilterFactory>(KEY4);

  // Enforce size of a single MemTable to 64MB (64MB = 67108864 bytes).
  options.write_buffer_size = 1 << 20;
  // Activate the MemPurge prototype.
  options.experimental_mempurge_threshold = 26.55;

  ASSERT_OK(TryReopen(options));

  std::atomic<uint32_t> mempurge_count{0};
  std::atomic<uint32_t> sst_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { mempurge_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(53);
  const size_t NUM_REPEAT = 1000;
  const size_t RAND_VALUES_LENGTH = 10240;
  std::string p_v1, p_v2, p_v3, p_v4, p_v5, p_v6, p_v7, p_v8, p_v9;

  p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v4 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v5 = rnd.RandomString(RAND_VALUES_LENGTH);
  ASSERT_OK(Put(KEY1, p_v1));
  ASSERT_OK(Put(KEY2, p_v2));
  ASSERT_OK(Put(KEY3, p_v3));
  ASSERT_OK(Put(KEY4, p_v4));
  ASSERT_OK(Put(KEY5, p_v5));
  ASSERT_OK(Delete(KEY1));

  // Insertion of of K-V pairs, multiple times.
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary
    // length RAND_VALUES_LENGTH bytes.
    p_v6 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v7 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v8 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v9 = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEY6, p_v6));
    ASSERT_OK(Put(KEY7, p_v7));
    ASSERT_OK(Put(KEY8, p_v8));
    ASSERT_OK(Put(KEY9, p_v9));

    ASSERT_OK(Delete(KEY7));
  }

  // Check that there was at least one mempurge
  const uint32_t EXPECTED_MIN_MEMPURGE_COUNT = 1;
  // Check that there was no SST files created during flush.
  const uint32_t EXPECTED_SST_COUNT = 0;

  EXPECT_GE(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);
  EXPECT_EQ(sst_count.exchange(0), EXPECTED_SST_COUNT);

  // Verify that the ConditionalUpdateCompactionFilter
  // updated the values of KEY2 and KEY3, and not KEY4 and KEY5.
  ASSERT_EQ(Get(KEY1), NOT_FOUND);
  ASSERT_EQ(Get(KEY2), NEW_VALUE);
  ASSERT_EQ(Get(KEY3), NEW_VALUE);
  ASSERT_EQ(Get(KEY4), p_v4);
  ASSERT_EQ(Get(KEY5), p_v5);
}

TEST_F(DBFlushTest, DISABLED_MemPurgeWALSupport) {
  Options options = CurrentOptions();

  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 128KB.
  options.write_buffer_size = 128 << 10;
  // Activate the MemPurge prototype
  // (values >1.0 are equivalent to 1.0).
  options.experimental_mempurge_threshold = 2.5;

  ASSERT_OK(TryReopen(options));

  const size_t KVSIZE = 10;

  do {
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "baz", "v5"));

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ("v1", Get(1, "foo"));

    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v5", Get(1, "baz"));
    ASSERT_OK(Put(0, "bar", "v2"));
    ASSERT_OK(Put(1, "bar", "v2"));
    ASSERT_OK(Put(1, "foo", "v3"));
    std::atomic<uint32_t> mempurge_count{0};
    std::atomic<uint32_t> sst_count{0};
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::FlushJob:MemPurgeSuccessful",
        [&](void* /*arg*/) { mempurge_count++; });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    std::vector<std::string> keys;
    for (size_t k = 0; k < KVSIZE; k++) {
      keys.push_back("IamKey" + std::to_string(k));
    }

    std::string RNDKEY, RNDVALUE;
    const std::string NOT_FOUND = "NOT_FOUND";

    // Heavy overwrite workload,
    // more than would fit in maximum allowed memtables.
    Random rnd(719);
    const size_t NUM_REPEAT = 100;
    const size_t RAND_KEY_LENGTH = 4096;
    const size_t RAND_VALUES_LENGTH = 1024;
    std::vector<std::string> values_default(KVSIZE), values_pikachu(KVSIZE);

    // Insert a very first set of keys that will be
    // mempurged at least once.
    for (size_t k = 0; k < KVSIZE / 2; k++) {
      values_default[k] = rnd.RandomString(RAND_VALUES_LENGTH);
      values_pikachu[k] = rnd.RandomString(RAND_VALUES_LENGTH);
    }

    // Insert keys[0:KVSIZE/2] to
    // both 'default' and 'pikachu' CFs.
    for (size_t k = 0; k < KVSIZE / 2; k++) {
      ASSERT_OK(Put(0, keys[k], values_default[k]));
      ASSERT_OK(Put(1, keys[k], values_pikachu[k]));
    }

    // Check that the insertion was seamless.
    for (size_t k = 0; k < KVSIZE / 2; k++) {
      ASSERT_EQ(Get(0, keys[k]), values_default[k]);
      ASSERT_EQ(Get(1, keys[k]), values_pikachu[k]);
    }

    // Insertion of of K-V pairs, multiple times (overwrites)
    // into 'default' CF. Will trigger mempurge.
    for (size_t j = 0; j < NUM_REPEAT; j++) {
      // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
      for (size_t k = KVSIZE / 2; k < KVSIZE; k++) {
        values_default[k] = rnd.RandomString(RAND_VALUES_LENGTH);
      }

      // Insert K-V into default CF.
      for (size_t k = KVSIZE / 2; k < KVSIZE; k++) {
        ASSERT_OK(Put(0, keys[k], values_default[k]));
      }

      // Check key validity, for all keys, both in
      // default and pikachu CFs.
      for (size_t k = 0; k < KVSIZE; k++) {
        ASSERT_EQ(Get(0, keys[k]), values_default[k]);
      }
      // Note that at this point, only keys[0:KVSIZE/2]
      // have been inserted into Pikachu.
      for (size_t k = 0; k < KVSIZE / 2; k++) {
        ASSERT_EQ(Get(1, keys[k]), values_pikachu[k]);
      }
    }

    // Insertion of of K-V pairs, multiple times (overwrites)
    // into 'pikachu' CF. Will trigger mempurge.
    // Check that we keep the older logs for 'default' imm().
    for (size_t j = 0; j < NUM_REPEAT; j++) {
      // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
      for (size_t k = KVSIZE / 2; k < KVSIZE; k++) {
        values_pikachu[k] = rnd.RandomString(RAND_VALUES_LENGTH);
      }

      // Insert K-V into pikachu CF.
      for (size_t k = KVSIZE / 2; k < KVSIZE; k++) {
        ASSERT_OK(Put(1, keys[k], values_pikachu[k]));
      }

      // Check key validity, for all keys,
      // both in default and pikachu.
      for (size_t k = 0; k < KVSIZE; k++) {
        ASSERT_EQ(Get(0, keys[k]), values_default[k]);
        ASSERT_EQ(Get(1, keys[k]), values_pikachu[k]);
      }
    }

    // Check that there was at least one mempurge
    const uint32_t EXPECTED_MIN_MEMPURGE_COUNT = 1;
    // Check that there was no SST files created during flush.
    const uint32_t EXPECTED_SST_COUNT = 0;

    EXPECT_GE(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);
    if (options.experimental_mempurge_threshold ==
        std::numeric_limits<double>::max()) {
      EXPECT_EQ(sst_count.exchange(0), EXPECTED_SST_COUNT);
    }

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    // Check that there was no data corruption anywhere,
    // not in 'default' nor in 'Pikachu' CFs.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v4"));
    ASSERT_EQ("v4", Get(1, "foo"));
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v5", Get(1, "baz"));
    // Check keys in 'Default' and 'Pikachu'.
    // keys[0:KVSIZE/2] were for sure contained
    // in the imm() at Reopen/recovery time.
    for (size_t k = 0; k < KVSIZE; k++) {
      ASSERT_EQ(Get(0, keys[k]), values_default[k]);
      ASSERT_EQ(Get(1, keys[k]), values_pikachu[k]);
    }
    // Insertion of random K-V pairs to trigger
    // a flush in the Pikachu CF.
    for (size_t j = 0; j < NUM_REPEAT; j++) {
      RNDKEY = rnd.RandomString(RAND_KEY_LENGTH);
      RNDVALUE = rnd.RandomString(RAND_VALUES_LENGTH);
      ASSERT_OK(Put(1, RNDKEY, RNDVALUE));
    }
    // ASsert than there was at least one flush to storage.
    EXPECT_GT(sst_count.exchange(0), EXPECTED_SST_COUNT);
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ("v4", Get(1, "foo"));
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v5", Get(1, "baz"));
    // Since values in default are held in mutable mem()
    // and imm(), check if the flush in pikachu didn't
    // affect these values.
    for (size_t k = 0; k < KVSIZE; k++) {
      ASSERT_EQ(Get(0, keys[k]), values_default[k]);
      ASSERT_EQ(Get(1, keys[k]), values_pikachu[k]);
    }
    ASSERT_EQ(Get(1, RNDKEY), RNDVALUE);
  } while (ChangeWalOptions());
}

TEST_F(DBFlushTest, MemPurgeCorrectLogNumberAndSSTFileCreation) {
  // Before our bug fix, we noticed that when 2 memtables were
  // being flushed (with one memtable being the output of a
  // previous MemPurge and one memtable being a newly-sealed memtable),
  // the SST file created was not properly added to the DB version
  // (via the VersionEdit obj), leading to data loss (the SST file
  // was later being purged as an obsolete file).
  // Therefore, we reproduce this scenario to test our fix.
  Options options = CurrentOptions();

  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 1MB (64MB = 1048576 bytes).
  options.write_buffer_size = 1 << 20;
  // Activate the MemPurge prototype.
  options.experimental_mempurge_threshold = 1.0;

  // Force to have more than one memtable to trigger a flush.
  // For some reason this option does not seem to be enforced,
  // so the following test is designed to make sure that we
  // are testing the correct test case.
  options.min_write_buffer_number_to_merge = 3;
  options.max_write_buffer_number = 5;
  options.max_write_buffer_size_to_maintain = 2 * (options.write_buffer_size);
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  std::atomic<uint32_t> mempurge_count{0};
  std::atomic<uint32_t> sst_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { mempurge_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Dummy variable used for the following callback function.
  uint64_t ZERO = 0;
  // We will first execute mempurge operations exclusively.
  // Therefore, when the first flush is triggered, we want to make
  // sure there is at least 2 memtables being flushed: one output
  // from a previous mempurge, and one newly sealed memtable.
  // This is when we observed in the past that some SST files created
  // were not properly added to the DB version (via the VersionEdit obj).
  std::atomic<uint64_t> num_memtable_at_first_flush(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:num_memtables", [&](void* arg) {
        uint64_t* mems_size = reinterpret_cast<uint64_t*>(arg);
        // atomic_compare_exchange_strong sometimes updates the value
        // of ZERO (the "expected" object), so we make sure ZERO is indeed...
        // zero.
        ZERO = 0;
        std::atomic_compare_exchange_strong(&num_memtable_at_first_flush, &ZERO,
                                            *mems_size);
      });

  const std::vector<std::string> KEYS = {
      "ThisIsKey1", "ThisIsKey2", "ThisIsKey3", "ThisIsKey4", "ThisIsKey5",
      "ThisIsKey6", "ThisIsKey7", "ThisIsKey8", "ThisIsKey9"};
  const std::string NOT_FOUND = "NOT_FOUND";

  Random rnd(117);
  const uint64_t NUM_REPEAT_OVERWRITES = 100;
  const uint64_t NUM_RAND_INSERTS = 500;
  const uint64_t RAND_VALUES_LENGTH = 10240;

  std::string key, value;
  std::vector<std::string> values(9, "");

  // Keys used to check that no SST file disappeared.
  for (uint64_t k = 0; k < 5; k++) {
    values[k] = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEYS[k], values[k]));
  }

  // Insertion of of K-V pairs, multiple times.
  // Trigger at least one mempurge and no SST file creation.
  for (size_t i = 0; i < NUM_REPEAT_OVERWRITES; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    for (uint64_t k = 5; k < values.size(); k++) {
      values[k] = rnd.RandomString(RAND_VALUES_LENGTH);
      ASSERT_OK(Put(KEYS[k], values[k]));
    }
    // Check database consistency.
    for (uint64_t k = 0; k < values.size(); k++) {
      ASSERT_EQ(Get(KEYS[k]), values[k]);
    }
  }

  // Check that there was at least one mempurge
  uint32_t expected_min_mempurge_count = 1;
  // Check that there was no SST files created during flush.
  uint32_t expected_sst_count = 0;
  EXPECT_GE(mempurge_count.load(), expected_min_mempurge_count);
  EXPECT_EQ(sst_count.load(), expected_sst_count);

  // Trigger an SST file creation and no mempurge.
  for (size_t i = 0; i < NUM_RAND_INSERTS; i++) {
    key = rnd.RandomString(RAND_VALUES_LENGTH);
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    value = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(key, value));
    // Check database consistency.
    for (uint64_t k = 0; k < values.size(); k++) {
      ASSERT_EQ(Get(KEYS[k]), values[k]);
    }
    ASSERT_EQ(Get(key), value);
  }

  // Check that there was at least one SST files created during flush.
  expected_sst_count = 1;
  EXPECT_GE(sst_count.load(), expected_sst_count);

  // Oddly enough, num_memtable_at_first_flush is not enforced to be
  // equal to min_write_buffer_number_to_merge. So by asserting that
  // the first SST file creation comes from one output memtable
  // from a previous mempurge, and one newly sealed memtable. This
  // is the scenario where we observed that some SST files created
  // were not properly added to the DB version before our bug fix.
  ASSERT_GE(num_memtable_at_first_flush.load(), 2);

  // Check that no data was lost after SST file creation.
  for (uint64_t k = 0; k < values.size(); k++) {
    ASSERT_EQ(Get(KEYS[k]), values[k]);
  }
  // Extra check of database consistency.
  ASSERT_EQ(Get(key), value);

  Close();
}

TEST_P(DBFlushDirectIOTest, DirectIO) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  options.use_direct_io_for_flush_and_compaction = GetParam();
  options.env = MockEnv::Create(Env::Default());
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:create_file", [&](void* arg) {
        bool* use_direct_writes = static_cast<bool*>(arg);
        ASSERT_EQ(*use_direct_writes,
                  options.use_direct_io_for_flush_and_compaction);
      });

  SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  ASSERT_OK(Put("foo", "v"));
  FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_OK(dbfull()->Flush(flush_options));
  Destroy(options);
  delete options.env;
}

TEST_F(DBFlushTest, FlushError) {
  Options options;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  options.disable_auto_compactions = true;
  options.env = fault_injection_env.get();
  Reopen(options);

  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  fault_injection_env->SetFilesystemActive(false);
  Status s = dbfull()->TEST_SwitchMemtable();
  fault_injection_env->SetFilesystemActive(true);
  Destroy(options);
  ASSERT_NE(s, Status::OK());
}

TEST_F(DBFlushTest, ManualFlushFailsInReadOnlyMode) {
  // Regression test for bug where manual flush hangs forever when the DB
  // is in read-only mode. Verify it now at least returns, despite failing.
  Options options;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  options.env = fault_injection_env.get();
  options.max_write_buffer_number = 2;
  Reopen(options);

  // Trigger a first flush but don't let it run
  ASSERT_OK(db_->PauseBackgroundWork());
  ASSERT_OK(Put("key1", "value1"));
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(db_->Flush(flush_opts));

  // Write a key to the second memtable so we have something to flush later
  // after the DB is in read-only mode.
  ASSERT_OK(Put("key2", "value2"));

  // Let the first flush continue, hit an error, and put the DB in read-only
  // mode.
  fault_injection_env->SetFilesystemActive(false);
  ASSERT_OK(db_->ContinueBackgroundWork());
  // We ingested the error to env, so the returned status is not OK.
  ASSERT_NOK(dbfull()->TEST_WaitForFlushMemTable());
#ifndef ROCKSDB_LITE
  uint64_t num_bg_errors;
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kBackgroundErrors, &num_bg_errors));
  ASSERT_GT(num_bg_errors, 0);
#endif  // ROCKSDB_LITE

  // In the bug scenario, triggering another flush would cause the second flush
  // to hang forever. After the fix we expect it to return an error.
  ASSERT_NOK(db_->Flush(FlushOptions()));

  Close();
}

TEST_F(DBFlushTest, CFDropRaceWithWaitForFlushMemTables) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  CreateAndReopenWithCF({"pikachu"}, options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:AfterScheduleFlush",
        "DBFlushTest::CFDropRaceWithWaitForFlushMemTables:BeforeDrop"},
       {"DBFlushTest::CFDropRaceWithWaitForFlushMemTables:AfterFree",
        "DBImpl::BackgroundCallFlush:start"},
       {"DBImpl::BackgroundCallFlush:start",
        "DBImpl::FlushMemTable:BeforeWaitForBgFlush"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_EQ(2, handles_.size());
  ASSERT_OK(Put(1, "key", "value"));
  auto* cfd = static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  port::Thread drop_cf_thr([&]() {
    TEST_SYNC_POINT(
        "DBFlushTest::CFDropRaceWithWaitForFlushMemTables:BeforeDrop");
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[1]));
    handles_.resize(1);
    TEST_SYNC_POINT(
        "DBFlushTest::CFDropRaceWithWaitForFlushMemTables:AfterFree");
  });
  FlushOptions flush_opts;
  flush_opts.allow_write_stall = true;
  ASSERT_NOK(dbfull()->TEST_FlushMemTable(cfd, flush_opts));
  drop_cf_thr.join();
  Close();
  SyncPoint::GetInstance()->DisableProcessing();
}

#ifndef ROCKSDB_LITE
TEST_F(DBFlushTest, FireOnFlushCompletedAfterCommittedResult) {
  class TestListener : public EventListener {
   public:
    void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
      // There's only one key in each flush.
      ASSERT_EQ(info.smallest_seqno, info.largest_seqno);
      ASSERT_NE(0, info.smallest_seqno);
      if (info.smallest_seqno == seq1) {
        // First flush completed
        ASSERT_FALSE(completed1);
        completed1 = true;
        CheckFlushResultCommitted(db, seq1);
      } else {
        // Second flush completed
        ASSERT_FALSE(completed2);
        completed2 = true;
        ASSERT_EQ(info.smallest_seqno, seq2);
        CheckFlushResultCommitted(db, seq2);
      }
    }

    void CheckFlushResultCommitted(DB* db, SequenceNumber seq) {
      DBImpl* db_impl = static_cast_with_check<DBImpl>(db);
      InstrumentedMutex* mutex = db_impl->mutex();
      mutex->Lock();
      auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(
                      db->DefaultColumnFamily())
                      ->cfd();
      ASSERT_LT(seq, cfd->imm()->current()->GetEarliestSequenceNumber());
      mutex->Unlock();
    }

    std::atomic<SequenceNumber> seq1{0};
    std::atomic<SequenceNumber> seq2{0};
    std::atomic<bool> completed1{false};
    std::atomic<bool> completed2{false};
  };
  std::shared_ptr<TestListener> listener = std::make_shared<TestListener>();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTableToOutputFile:AfterPickMemtables",
        "DBFlushTest::FireOnFlushCompletedAfterCommittedResult:WaitFirst"},
       {"DBImpl::FlushMemTableToOutputFile:Finish",
        "DBFlushTest::FireOnFlushCompletedAfterCommittedResult:WaitSecond"}});
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table", [&listener](void* arg) {
        // Wait for the second flush finished, out of mutex.
        auto* mems = reinterpret_cast<autovector<MemTable*>*>(arg);
        if (mems->front()->GetEarliestSequenceNumber() == listener->seq1 - 1) {
          TEST_SYNC_POINT(
              "DBFlushTest::FireOnFlushCompletedAfterCommittedResult:"
              "WaitSecond");
        }
      });

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.listeners.push_back(listener);
  // Setting max_flush_jobs = max_background_jobs / 4 = 2.
  options.max_background_jobs = 8;
  // Allow 2 immutable memtables.
  options.max_write_buffer_number = 3;
  Reopen(options);
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("foo", "v"));
  listener->seq1 = db_->GetLatestSequenceNumber();
  // t1 will wait for the second flush complete before committing flush result.
  auto t1 = port::Thread([&]() {
    // flush_opts.wait = true
    ASSERT_OK(db_->Flush(FlushOptions()));
  });
  // Wait for first flush started.
  TEST_SYNC_POINT(
      "DBFlushTest::FireOnFlushCompletedAfterCommittedResult:WaitFirst");
  // The second flush will exit early without commit its result. The work
  // is delegated to the first flush.
  ASSERT_OK(Put("bar", "v"));
  listener->seq2 = db_->GetLatestSequenceNumber();
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(db_->Flush(flush_opts));
  t1.join();
  // Ensure background work is fully finished including listener callbacks
  // before accessing listener state.
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ASSERT_TRUE(listener->completed1);
  ASSERT_TRUE(listener->completed2);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif  // !ROCKSDB_LITE

TEST_F(DBFlushTest, FlushWithBlob) {
  constexpr uint64_t min_blob_size = 10;

  Options options;
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;
  options.disable_auto_compactions = true;
  options.env = env_;

  Reopen(options);

  constexpr char short_value[] = "short";
  static_assert(sizeof(short_value) - 1 < min_blob_size,
                "short_value too long");

  constexpr char long_value[] = "long_value";
  static_assert(sizeof(long_value) - 1 >= min_blob_size,
                "long_value too short");

  ASSERT_OK(Put("key1", short_value));
  ASSERT_OK(Put("key2", long_value));

  ASSERT_OK(Flush());

  ASSERT_EQ(Get("key1"), short_value);
  ASSERT_EQ(Get("key2"), long_value);

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  Version* const current = cfd->current();
  assert(current);

  const VersionStorageInfo* const storage_info = current->storage_info();
  assert(storage_info);

  const auto& l0_files = storage_info->LevelFiles(0);
  ASSERT_EQ(l0_files.size(), 1);

  const FileMetaData* const table_file = l0_files[0];
  assert(table_file);

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1);

  const auto& blob_file = blob_files.front();
  assert(blob_file);

  ASSERT_EQ(table_file->smallest.user_key(), "key1");
  ASSERT_EQ(table_file->largest.user_key(), "key2");
  ASSERT_EQ(table_file->fd.smallest_seqno, 1);
  ASSERT_EQ(table_file->fd.largest_seqno, 2);
  ASSERT_EQ(table_file->oldest_blob_file_number,
            blob_file->GetBlobFileNumber());

  ASSERT_EQ(blob_file->GetTotalBlobCount(), 1);

#ifndef ROCKSDB_LITE
  const InternalStats* const internal_stats = cfd->internal_stats();
  assert(internal_stats);

  const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
  ASSERT_FALSE(compaction_stats.empty());
  ASSERT_EQ(compaction_stats[0].bytes_written, table_file->fd.GetFileSize());
  ASSERT_EQ(compaction_stats[0].bytes_written_blob,
            blob_file->GetTotalBlobBytes());
  ASSERT_EQ(compaction_stats[0].num_output_files, 1);
  ASSERT_EQ(compaction_stats[0].num_output_files_blob, 1);

  const uint64_t* const cf_stats_value = internal_stats->TEST_GetCFStatsValue();
  ASSERT_EQ(cf_stats_value[InternalStats::BYTES_FLUSHED],
            compaction_stats[0].bytes_written +
                compaction_stats[0].bytes_written_blob);
#endif  // ROCKSDB_LITE
}

TEST_F(DBFlushTest, FlushWithChecksumHandoff1) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  options.disable_auto_compactions = true;
  options.env = fault_fs_env.get();
  options.checksum_handoff_file_types.Add(FileType::kTableFile);
  Reopen(options);

  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  // The hash does not match, write fails
  // fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
  // Since the file system returns IOStatus::Corruption, it is an
  // unrecoverable error.
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
  });
  ASSERT_OK(Put("key3", "value3"));
  ASSERT_OK(Put("key4", "value4"));
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = Flush();
  ASSERT_EQ(s.severity(),
            ROCKSDB_NAMESPACE::Status::Severity::kUnrecoverableError);
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
  Reopen(options);

  // The file system does not support checksum handoff. The check
  // will be ignored.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
  ASSERT_OK(Put("key5", "value5"));
  ASSERT_OK(Put("key6", "value6"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  // Each write will be similated as corrupted.
  // Since the file system returns IOStatus::Corruption, it is an
  // unrecoverable error.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs->IngestDataCorruptionBeforeWrite();
  });
  ASSERT_OK(Put("key7", "value7"));
  ASSERT_OK(Put("key8", "value8"));
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(),
            ROCKSDB_NAMESPACE::Status::Severity::kUnrecoverableError);
  SyncPoint::GetInstance()->DisableProcessing();

  Destroy(options);
}

TEST_F(DBFlushTest, FlushWithChecksumHandoff2) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  options.disable_auto_compactions = true;
  options.env = fault_fs_env.get();
  Reopen(options);

  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(Flush());

  // options is not set, the checksum handoff will not be triggered
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
  });
  ASSERT_OK(Put("key3", "value3"));
  ASSERT_OK(Put("key4", "value4"));
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
  Reopen(options);

  // The file system does not support checksum handoff. The check
  // will be ignored.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
  ASSERT_OK(Put("key5", "value5"));
  ASSERT_OK(Put("key6", "value6"));
  ASSERT_OK(Flush());

  // options is not set, the checksum handoff will not be triggered
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs->IngestDataCorruptionBeforeWrite();
  });
  ASSERT_OK(Put("key7", "value7"));
  ASSERT_OK(Put("key8", "value8"));
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();

  Destroy(options);
}

TEST_F(DBFlushTest, FlushWithChecksumHandoffManifest1) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  options.disable_auto_compactions = true;
  options.env = fault_fs_env.get();
  options.checksum_handoff_file_types.Add(FileType::kDescriptorFile);
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  Reopen(options);

  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(Flush());

  // The hash does not match, write fails
  // fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
  // Since the file system returns IOStatus::Corruption, it is mapped to
  // kFatalError error.
  ASSERT_OK(Put("key3", "value3"));
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
      });
  ASSERT_OK(Put("key3", "value3"));
  ASSERT_OK(Put("key4", "value4"));
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kFatalError);
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
}

TEST_F(DBFlushTest, FlushWithChecksumHandoffManifest2) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  options.disable_auto_compactions = true;
  options.env = fault_fs_env.get();
  options.checksum_handoff_file_types.Add(FileType::kDescriptorFile);
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
  Reopen(options);
  // The file system does not support checksum handoff. The check
  // will be ignored.
  ASSERT_OK(Put("key5", "value5"));
  ASSERT_OK(Put("key6", "value6"));
  ASSERT_OK(Flush());

  // Each write will be similated as corrupted.
  // Since the file system returns IOStatus::Corruption, it is mapped to
  // kFatalError error.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs->IngestDataCorruptionBeforeWrite(); });
  ASSERT_OK(Put("key7", "value7"));
  ASSERT_OK(Put("key8", "value8"));
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kFatalError);
  SyncPoint::GetInstance()->DisableProcessing();

  Destroy(options);
}

TEST_F(DBFlushTest, PickRightMemtables) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  options.create_if_missing = true;

  const std::string test_cf_name = "test_cf";
  options.max_write_buffer_number = 128;
  CreateColumnFamilies({test_cf_name}, options);

  Close();

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, test_cf_name}, options);

  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));

  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "key", "value"));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncClosedLogs:BeforeReLock", [&](void* /*arg*/) {
        ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "what", "v"));
        auto* cfhi =
            static_cast_with_check<ColumnFamilyHandleImpl>(handles_[1]);
        assert(cfhi);
        ASSERT_OK(dbfull()->TEST_SwitchMemtable(cfhi->cfd()));
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTableToOutputFile:AfterPickMemtables", [&](void* arg) {
        auto* job = reinterpret_cast<FlushJob*>(arg);
        assert(job);
        const auto& mems = job->GetMemTables();
        assert(mems.size() == 1);
        assert(mems[0]);
        ASSERT_EQ(1, mems[0]->GetID());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->Flush(FlushOptions(), handles_[1]));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class DBFlushTestBlobError : public DBFlushTest,
                             public testing::WithParamInterface<std::string> {
 public:
  DBFlushTestBlobError() : sync_point_(GetParam()) {}

  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(DBFlushTestBlobError, DBFlushTestBlobError,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileBuilder::WriteBlobToFile:AddRecord",
                            "BlobFileBuilder::WriteBlobToFile:AppendFooter"}));

TEST_P(DBFlushTestBlobError, FlushError) {
  Options options;
  options.enable_blob_files = true;
  options.disable_auto_compactions = true;
  options.env = env_;

  Reopen(options);

  ASSERT_OK(Put("key", "blob"));

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* arg) {
    Status* const s = static_cast<Status*>(arg);
    assert(s);

    (*s) = Status::IOError(sync_point_);
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_NOK(Flush());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  Version* const current = cfd->current();
  assert(current);

  const VersionStorageInfo* const storage_info = current->storage_info();
  assert(storage_info);

  const auto& l0_files = storage_info->LevelFiles(0);
  ASSERT_TRUE(l0_files.empty());

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_TRUE(blob_files.empty());

  // Make sure the files generated by the failed job have been deleted
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  for (const auto& file : files) {
    uint64_t number = 0;
    FileType type = kTableFile;

    if (!ParseFileName(file, &number, &type)) {
      continue;
    }

    ASSERT_NE(type, kTableFile);
    ASSERT_NE(type, kBlobFile);
  }

#ifndef ROCKSDB_LITE
  const InternalStats* const internal_stats = cfd->internal_stats();
  assert(internal_stats);

  const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
  ASSERT_FALSE(compaction_stats.empty());

  if (sync_point_ == "BlobFileBuilder::WriteBlobToFile:AddRecord") {
    ASSERT_EQ(compaction_stats[0].bytes_written, 0);
    ASSERT_EQ(compaction_stats[0].bytes_written_blob, 0);
    ASSERT_EQ(compaction_stats[0].num_output_files, 0);
    ASSERT_EQ(compaction_stats[0].num_output_files_blob, 0);
  } else {
    // SST file writing succeeded; blob file writing failed (during Finish)
    ASSERT_GT(compaction_stats[0].bytes_written, 0);
    ASSERT_EQ(compaction_stats[0].bytes_written_blob, 0);
    ASSERT_EQ(compaction_stats[0].num_output_files, 1);
    ASSERT_EQ(compaction_stats[0].num_output_files_blob, 0);
  }

  const uint64_t* const cf_stats_value = internal_stats->TEST_GetCFStatsValue();
  ASSERT_EQ(cf_stats_value[InternalStats::BYTES_FLUSHED],
            compaction_stats[0].bytes_written +
                compaction_stats[0].bytes_written_blob);
#endif  // ROCKSDB_LITE
}

#ifndef ROCKSDB_LITE
TEST_F(DBFlushTest, TombstoneVisibleInSnapshot) {
  class SimpleTestFlushListener : public EventListener {
   public:
    explicit SimpleTestFlushListener(DBFlushTest* _test) : test_(_test) {}
    ~SimpleTestFlushListener() override {}

    void OnFlushBegin(DB* db, const FlushJobInfo& info) override {
      ASSERT_EQ(static_cast<uint32_t>(0), info.cf_id);

      ASSERT_OK(db->Delete(WriteOptions(), "foo"));
      snapshot_ = db->GetSnapshot();
      ASSERT_OK(db->Put(WriteOptions(), "foo", "value"));

      auto* dbimpl = static_cast_with_check<DBImpl>(db);
      assert(dbimpl);

      ColumnFamilyHandle* cfh = db->DefaultColumnFamily();
      auto* cfhi = static_cast_with_check<ColumnFamilyHandleImpl>(cfh);
      assert(cfhi);
      ASSERT_OK(dbimpl->TEST_SwitchMemtable(cfhi->cfd()));
    }

    DBFlushTest* test_ = nullptr;
    const Snapshot* snapshot_ = nullptr;
  };

  Options options = CurrentOptions();
  options.create_if_missing = true;
  auto* listener = new SimpleTestFlushListener(this);
  options.listeners.emplace_back(listener);
  DestroyAndReopen(options);

  ASSERT_OK(db_->Put(WriteOptions(), "foo", "value0"));

  ManagedSnapshot snapshot_guard(db_);

  ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();
  ASSERT_OK(db_->Flush(FlushOptions(), default_cf));

  const Snapshot* snapshot = listener->snapshot_;
  assert(snapshot);

  ReadOptions read_opts;
  read_opts.snapshot = snapshot;

  // Using snapshot should not see "foo".
  {
    std::string value;
    Status s = db_->Get(read_opts, "foo", &value);
    ASSERT_TRUE(s.IsNotFound());
  }

  db_->ReleaseSnapshot(snapshot);
}

TEST_P(DBAtomicFlushTest, ManualFlushUnder2PC) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.allow_2pc = true;
  options.atomic_flush = GetParam();
  // 64MB so that memtable flush won't be trigger by the small writes.
  options.write_buffer_size = (static_cast<size_t>(64) << 20);
  auto flush_listener = std::make_shared<FlushCounterListener>();
  flush_listener->expected_flush_reason = FlushReason::kManualFlush;
  options.listeners.push_back(flush_listener);
  // Destroy the DB to recreate as a TransactionDB.
  Close();
  Destroy(options, true);

  // Create a TransactionDB.
  TransactionDB* txn_db = nullptr;
  TransactionDBOptions txn_db_opts;
  txn_db_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  ASSERT_OK(TransactionDB::Open(options, txn_db_opts, dbname_, &txn_db));
  ASSERT_NE(txn_db, nullptr);
  db_ = txn_db;

  // Create two more columns other than default CF.
  std::vector<std::string> cfs = {"puppy", "kitty"};
  CreateColumnFamilies(cfs, options);
  ASSERT_EQ(handles_.size(), 2);
  ASSERT_EQ(handles_[0]->GetName(), cfs[0]);
  ASSERT_EQ(handles_[1]->GetName(), cfs[1]);
  const size_t kNumCfToFlush = options.atomic_flush ? 2 : 1;

  WriteOptions wopts;
  TransactionOptions txn_opts;
  // txn1 only prepare, but does not commit.
  // The WAL containing the prepared but uncommitted data must be kept.
  Transaction* txn1 = txn_db->BeginTransaction(wopts, txn_opts, nullptr);
  // txn2 not only prepare, but also commit.
  Transaction* txn2 = txn_db->BeginTransaction(wopts, txn_opts, nullptr);
  ASSERT_NE(txn1, nullptr);
  ASSERT_NE(txn2, nullptr);
  for (size_t i = 0; i < kNumCfToFlush; i++) {
    ASSERT_OK(txn1->Put(handles_[i], "k1", "v1"));
    ASSERT_OK(txn2->Put(handles_[i], "k2", "v2"));
  }
  // A txn must be named before prepare.
  ASSERT_OK(txn1->SetName("txn1"));
  ASSERT_OK(txn2->SetName("txn2"));
  // Prepare writes to WAL, but not to memtable. (WriteCommitted)
  ASSERT_OK(txn1->Prepare());
  ASSERT_OK(txn2->Prepare());
  // Commit writes to memtable.
  ASSERT_OK(txn2->Commit());
  delete txn1;
  delete txn2;

  // There are still data in memtable not flushed.
  // But since data is small enough to reside in the active memtable,
  // there are no immutable memtable.
  for (size_t i = 0; i < kNumCfToFlush; i++) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_FALSE(cfh->cfd()->mem()->IsEmpty());
  }

  // Atomic flush memtables,
  // the min log with prepared data should be written to MANIFEST.
  std::vector<ColumnFamilyHandle*> cfs_to_flush(kNumCfToFlush);
  for (size_t i = 0; i < kNumCfToFlush; i++) {
    cfs_to_flush[i] = handles_[i];
  }
  ASSERT_OK(txn_db->Flush(FlushOptions(), cfs_to_flush));

  // There are no remaining data in memtable after flush.
  for (size_t i = 0; i < kNumCfToFlush; i++) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
  }

  // The recovered min log number with prepared data should be non-zero.
  // In 2pc mode, MinLogNumberToKeep returns the
  // VersionSet::min_log_number_to_keep recovered from MANIFEST, if it's 0,
  // it means atomic flush didn't write the min_log_number_to_keep to MANIFEST.
  cfs.push_back(kDefaultColumnFamilyName);
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db_);
  ASSERT_TRUE(db_impl->allow_2pc());
  ASSERT_NE(db_impl->MinLogNumberToKeep(), 0);
}

TEST_P(DBAtomicFlushTest, ManualAtomicFlush) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = GetParam();
  options.write_buffer_size = (static_cast<size_t>(64) << 20);
  auto flush_listener = std::make_shared<FlushCounterListener>();
  flush_listener->expected_flush_reason = FlushReason::kManualFlush;
  options.listeners.push_back(flush_listener);

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  for (size_t i = 0; i != num_cfs; ++i) {
    ASSERT_OK(Put(static_cast<int>(i) /*cf*/, "key", "value", wopts));
  }

  for (size_t i = 0; i != num_cfs; ++i) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_FALSE(cfh->cfd()->mem()->IsEmpty());
  }

  std::vector<int> cf_ids;
  for (size_t i = 0; i != num_cfs; ++i) {
    cf_ids.emplace_back(static_cast<int>(i));
  }
  ASSERT_OK(Flush(cf_ids));

  for (size_t i = 0; i != num_cfs; ++i) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
  }
}
#endif  // ROCKSDB_LITE

TEST_P(DBAtomicFlushTest, PrecomputeMinLogNumberToKeepNon2PC) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = GetParam();
  options.write_buffer_size = (static_cast<size_t>(64) << 20);
  CreateAndReopenWithCF({"pikachu"}, options);

  const size_t num_cfs = handles_.size();
  ASSERT_EQ(num_cfs, 2);
  WriteOptions wopts;
  for (size_t i = 0; i != num_cfs; ++i) {
    ASSERT_OK(Put(static_cast<int>(i) /*cf*/, "key", "value", wopts));
  }

  {
    // Flush the default CF only.
    std::vector<int> cf_ids{0};
    ASSERT_OK(Flush(cf_ids));

    autovector<ColumnFamilyData*> flushed_cfds;
    autovector<autovector<VersionEdit*>> flush_edits;
    auto flushed_cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[0]);
    flushed_cfds.push_back(flushed_cfh->cfd());
    flush_edits.push_back({});
    auto unflushed_cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[1]);

    ASSERT_EQ(PrecomputeMinLogNumberToKeepNon2PC(dbfull()->GetVersionSet(),
                                                 flushed_cfds, flush_edits),
              unflushed_cfh->cfd()->GetLogNumber());
  }

  {
    // Flush all CFs.
    std::vector<int> cf_ids;
    for (size_t i = 0; i != num_cfs; ++i) {
      cf_ids.emplace_back(static_cast<int>(i));
    }
    ASSERT_OK(Flush(cf_ids));
    uint64_t log_num_after_flush = dbfull()->TEST_GetCurrentLogNumber();

    uint64_t min_log_number_to_keep = std::numeric_limits<uint64_t>::max();
    autovector<ColumnFamilyData*> flushed_cfds;
    autovector<autovector<VersionEdit*>> flush_edits;
    for (size_t i = 0; i != num_cfs; ++i) {
      auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
      flushed_cfds.push_back(cfh->cfd());
      flush_edits.push_back({});
      min_log_number_to_keep =
          std::min(min_log_number_to_keep, cfh->cfd()->GetLogNumber());
    }
    ASSERT_EQ(min_log_number_to_keep, log_num_after_flush);
    ASSERT_EQ(PrecomputeMinLogNumberToKeepNon2PC(dbfull()->GetVersionSet(),
                                                 flushed_cfds, flush_edits),
              min_log_number_to_keep);
  }
}

TEST_P(DBAtomicFlushTest, AtomicFlushTriggeredByMemTableFull) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = GetParam();
  // 4KB so that we can easily trigger auto flush.
  options.write_buffer_size = 4096;

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCallFlush:FlushFinish:0",
        "DBAtomicFlushTest::AtomicFlushTriggeredByMemTableFull:BeforeCheck"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  for (size_t i = 0; i != num_cfs; ++i) {
    ASSERT_OK(Put(static_cast<int>(i) /*cf*/, "key", "value", wopts));
  }
  // Keep writing to one of them column families to trigger auto flush.
  for (int i = 0; i != 4000; ++i) {
    ASSERT_OK(Put(static_cast<int>(num_cfs) - 1 /*cf*/,
                  "key" + std::to_string(i), "value" + std::to_string(i),
                  wopts));
  }

  TEST_SYNC_POINT(
      "DBAtomicFlushTest::AtomicFlushTriggeredByMemTableFull:BeforeCheck");
  if (options.atomic_flush) {
    for (size_t i = 0; i + 1 != num_cfs; ++i) {
      auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
      ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
      ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
    }
  } else {
    for (size_t i = 0; i + 1 != num_cfs; ++i) {
      auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
      ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
      ASSERT_FALSE(cfh->cfd()->mem()->IsEmpty());
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBAtomicFlushTest, AtomicFlushRollbackSomeJobs) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  options.env = fault_injection_env.get();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:1",
        "DBAtomicFlushTest::AtomicFlushRollbackSomeJobs:1"},
       {"DBAtomicFlushTest::AtomicFlushRollbackSomeJobs:2",
        "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_OK(Put(cf_id, "key", "value", wopts));
  }
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_opts, handles_));
  TEST_SYNC_POINT("DBAtomicFlushTest::AtomicFlushRollbackSomeJobs:1");
  fault_injection_env->SetFilesystemActive(false);
  TEST_SYNC_POINT("DBAtomicFlushTest::AtomicFlushRollbackSomeJobs:2");
  for (auto* cfh : handles_) {
    // Returns the IO error happend during flush.
    ASSERT_NOK(dbfull()->TEST_WaitForFlushMemTable(cfh));
  }
  for (size_t i = 0; i != num_cfs; ++i) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ASSERT_EQ(1, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
  }
  fault_injection_env->SetFilesystemActive(true);
  Destroy(options);
}

TEST_P(DBAtomicFlushTest, FlushMultipleCFs_DropSomeBeforeRequestFlush) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  std::vector<int> cf_ids;
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_OK(Put(cf_id, "key", "value", wopts));
    cf_ids.push_back(cf_id);
  }
  ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
  ASSERT_TRUE(Flush(cf_ids).IsColumnFamilyDropped());
  Destroy(options);
}

TEST_P(DBAtomicFlushTest,
       FlushMultipleCFs_DropSomeAfterScheduleFlushBeforeFlushJobRun) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::AtomicFlushMemTables:AfterScheduleFlush",
        "DBAtomicFlushTest::BeforeDropCF"},
       {"DBAtomicFlushTest::AfterDropCF",
        "DBImpl::BackgroundCallFlush:start"}});
  SyncPoint::GetInstance()->EnableProcessing();

  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_OK(Put(cf_id, "key", "value", wopts));
  }
  port::Thread user_thread([&]() {
    TEST_SYNC_POINT("DBAtomicFlushTest::BeforeDropCF");
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    TEST_SYNC_POINT("DBAtomicFlushTest::AfterDropCF");
  });
  FlushOptions flush_opts;
  flush_opts.wait = true;
  ASSERT_OK(dbfull()->Flush(flush_opts, handles_));
  user_thread.join();
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_EQ("value", Get(cf_id, "key"));
  }

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "eevee"}, options);
  num_cfs = handles_.size();
  ASSERT_EQ(2, num_cfs);
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_EQ("value", Get(cf_id, "key"));
  }
  Destroy(options);
}

TEST_P(DBAtomicFlushTest, TriggerFlushAndClose) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  const int kNumKeysTriggerFlush = 4;
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysTriggerFlush));
  CreateAndReopenWithCF({"pikachu"}, options);

  for (int i = 0; i != kNumKeysTriggerFlush; ++i) {
    ASSERT_OK(Put(0, "key" + std::to_string(i), "value" + std::to_string(i)));
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(0, "key", "value"));
  Close();

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  ASSERT_EQ("value", Get(0, "key"));
}

TEST_P(DBAtomicFlushTest, PickMemtablesRaceWithBackgroundFlush) {
  bool atomic_flush = GetParam();
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  options.max_write_buffer_number = 4;
  // Set min_write_buffer_number_to_merge to be greater than 1, so that
  // a column family with one memtable in the imm will not cause IsFlushPending
  // to return true when flush_requested_ is false.
  options.min_write_buffer_number_to_merge = 2;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(2, handles_.size());
  ASSERT_OK(dbfull()->PauseBackgroundWork());
  ASSERT_OK(Put(0, "key00", "value00"));
  ASSERT_OK(Put(1, "key10", "value10"));
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_opts, handles_));
  ASSERT_OK(Put(0, "key01", "value01"));
  // Since max_write_buffer_number is 4, the following flush won't cause write
  // stall.
  ASSERT_OK(dbfull()->Flush(flush_opts));
  ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
  ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[1]));
  handles_[1] = nullptr;
  ASSERT_OK(dbfull()->ContinueBackgroundWork());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[0]));
  delete handles_[0];
  handles_.clear();
}

TEST_P(DBAtomicFlushTest, CFDropRaceWithWaitForFlushMemTables) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  CreateAndReopenWithCF({"pikachu"}, options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::AtomicFlushMemTables:AfterScheduleFlush",
        "DBAtomicFlushTest::CFDropRaceWithWaitForFlushMemTables:BeforeDrop"},
       {"DBAtomicFlushTest::CFDropRaceWithWaitForFlushMemTables:AfterFree",
        "DBImpl::BackgroundCallFlush:start"},
       {"DBImpl::BackgroundCallFlush:start",
        "DBImpl::AtomicFlushMemTables:BeforeWaitForBgFlush"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_EQ(2, handles_.size());
  ASSERT_OK(Put(0, "key", "value"));
  ASSERT_OK(Put(1, "key", "value"));
  auto* cfd_default =
      static_cast<ColumnFamilyHandleImpl*>(dbfull()->DefaultColumnFamily())
          ->cfd();
  auto* cfd_pikachu = static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  port::Thread drop_cf_thr([&]() {
    TEST_SYNC_POINT(
        "DBAtomicFlushTest::CFDropRaceWithWaitForFlushMemTables:BeforeDrop");
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    delete handles_[1];
    handles_.resize(1);
    TEST_SYNC_POINT(
        "DBAtomicFlushTest::CFDropRaceWithWaitForFlushMemTables:AfterFree");
  });
  FlushOptions flush_opts;
  flush_opts.allow_write_stall = true;
  ASSERT_OK(dbfull()->TEST_AtomicFlushMemTables({cfd_default, cfd_pikachu},
                                                flush_opts));
  drop_cf_thr.join();
  Close();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBAtomicFlushTest, RollbackAfterFailToInstallResults) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  auto fault_injection_env = std::make_shared<FaultInjectionTestEnv>(env_);
  Options options = CurrentOptions();
  options.env = fault_injection_env.get();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(2, handles_.size());
  for (size_t cf = 0; cf < handles_.size(); ++cf) {
    ASSERT_OK(Put(static_cast<int>(cf), "a", "value"));
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:0",
      [&](void* /*arg*/) { fault_injection_env->SetFilesystemActive(false); });
  SyncPoint::GetInstance()->EnableProcessing();
  FlushOptions flush_opts;
  Status s = db_->Flush(flush_opts, handles_);
  ASSERT_NOK(s);
  fault_injection_env->SetFilesystemActive(true);
  Close();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// In atomic flush, concurrent bg flush threads commit to the MANIFEST in
// serial, in the order of their picked memtables for each column family.
// Only when a bg flush thread finds out that its memtables are the earliest
// unflushed ones for all the included column families will this bg flush
// thread continue to commit to MANIFEST.
// This unit test uses sync point to coordinate the execution of two bg threads
// executing the same sequence of functions. The interleaving are as follows.
// time            bg1                            bg2
//  |   pick memtables to flush
//  |   flush memtables cf1_m1, cf2_m1
//  |   join MANIFEST write queue
//  |                                     pick memtabls to flush
//  |                                     flush memtables cf1_(m1+1)
//  |                                     join MANIFEST write queue
//  |                                     wait to write MANIFEST
//  |   write MANIFEST
//  |   IO error
//  |                                     detect IO error and stop waiting
//  V
TEST_P(DBAtomicFlushTest, BgThreadNoWaitAfterManifestError) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  auto fault_injection_env = std::make_shared<FaultInjectionTestEnv>(env_);
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.atomic_flush = true;
  options.env = fault_injection_env.get();
  // Set a larger value than default so that RocksDB can schedule concurrent
  // background flush threads.
  options.max_background_jobs = 8;
  options.max_write_buffer_number = 8;
  CreateAndReopenWithCF({"pikachu"}, options);

  assert(2 == handles_.size());

  WriteOptions write_opts;
  write_opts.disableWAL = true;

  ASSERT_OK(Put(0, "a", "v_0_a", write_opts));
  ASSERT_OK(Put(1, "a", "v_1_a", write_opts));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  SyncPoint::GetInstance()->LoadDependency({
      {"BgFlushThr2:WaitToCommit", "BgFlushThr1:BeforeWriteManifest"},
  });

  std::thread::id bg_flush_thr1, bg_flush_thr2;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void*) {
        if (bg_flush_thr1 == std::thread::id()) {
          bg_flush_thr1 = std::this_thread::get_id();
        } else if (bg_flush_thr2 == std::thread::id()) {
          bg_flush_thr2 = std::this_thread::get_id();
        }
      });

  int called = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AtomicFlushMemTablesToOutputFiles:WaitToCommit", [&](void* arg) {
        if (std::this_thread::get_id() == bg_flush_thr2) {
          const auto* ptr = reinterpret_cast<std::pair<Status, bool>*>(arg);
          assert(ptr);
          if (0 == called) {
            // When bg flush thread 2 reaches here for the first time.
            ASSERT_OK(ptr->first);
            ASSERT_TRUE(ptr->second);
          } else if (1 == called) {
            // When bg flush thread 2 reaches here for the second time.
            ASSERT_TRUE(ptr->first.IsIOError());
            ASSERT_FALSE(ptr->second);
          }
          ++called;
          TEST_SYNC_POINT("BgFlushThr2:WaitToCommit");
        }
      });

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:0",
      [&](void*) {
        if (std::this_thread::get_id() == bg_flush_thr1) {
          TEST_SYNC_POINT("BgFlushThr1:BeforeWriteManifest");
        }
      });

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (std::this_thread::get_id() != bg_flush_thr1) {
          return;
        }
        ASSERT_OK(db_->Put(write_opts, "b", "v_1_b"));

        FlushOptions flush_opts;
        flush_opts.wait = false;
        std::vector<ColumnFamilyHandle*> cfhs(1, db_->DefaultColumnFamily());
        ASSERT_OK(dbfull()->Flush(flush_opts, cfhs));
      });

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        auto* ptr = reinterpret_cast<IOStatus*>(arg);
        assert(ptr);
        *ptr = IOStatus::IOError("Injected failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(dbfull()->Flush(FlushOptions(), handles_).IsIOError());

  Close();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBAtomicFlushTest, NoWaitWhenWritesStopped) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.atomic_flush = GetParam();
  options.max_write_buffer_number = 2;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(1));

  Reopen(options);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::DelayWrite:Start",
        "DBAtomicFlushTest::NoWaitWhenWritesStopped:0"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->PauseBackgroundWork());
  for (int i = 0; i < options.max_write_buffer_number; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v" + std::to_string(i)));
  }
  std::thread stalled_writer([&]() { ASSERT_OK(Put("k", "v")); });

  TEST_SYNC_POINT("DBAtomicFlushTest::NoWaitWhenWritesStopped:0");

  {
    FlushOptions flush_opts;
    flush_opts.wait = false;
    flush_opts.allow_write_stall = true;
    ASSERT_TRUE(db_->Flush(flush_opts).IsTryAgain());
  }

  ASSERT_OK(dbfull()->ContinueBackgroundWork());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  stalled_writer.join();

  SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(DBFlushDirectIOTest, DBFlushDirectIOTest,
                        testing::Bool());

INSTANTIATE_TEST_CASE_P(DBAtomicFlushTest, DBAtomicFlushTest, testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
