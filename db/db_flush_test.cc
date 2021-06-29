//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

class DBFlushTest : public DBTestBase {
 public:
  DBFlushTest() : DBTestBase("/db_flush_test", /*env_do_fsync=*/true) {}
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
  no_wait.allow_write_stall=true;

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
  options.memtable_factory.reset(new SpecialSkipListFactory(1));
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

TEST_P(DBFlushDirectIOTest, DirectIO) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  options.use_direct_io_for_flush_and_compaction = GetParam();
  options.env = new MockEnv(Env::Default());
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
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBackgroundErrors,
                                  &num_bg_errors));
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
      {{"DBImpl::BackgroundCallFlush:start",
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

  VersionSet* const versions = dbfull()->TEST_GetVersionSet();
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

  const auto& blob_file = blob_files.begin()->second;
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

  VersionSet* const versions = dbfull()->TEST_GetVersionSet();
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
TEST_P(DBAtomicFlushTest, ManualFlushUnder2PC) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.allow_2pc = true;
  options.atomic_flush = GetParam();
  // 64MB so that memtable flush won't be trigger by the small writes.
  options.write_buffer_size = (static_cast<size_t>(64) << 20);

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
    ASSERT_EQ(cfh->cfd()->GetFlushReason(), FlushReason::kManualFlush);
  }

  // The recovered min log number with prepared data should be non-zero.
  // In 2pc mode, MinLogNumberToKeep returns the
  // VersionSet::min_log_number_to_keep_2pc recovered from MANIFEST, if it's 0,
  // it means atomic flush didn't write the min_log_number_to_keep to MANIFEST.
  cfs.push_back(kDefaultColumnFamilyName);
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db_);
  ASSERT_TRUE(db_impl->allow_2pc());
  ASSERT_NE(db_impl->MinLogNumberToKeep(), 0);
}
#endif  // ROCKSDB_LITE

TEST_P(DBAtomicFlushTest, ManualAtomicFlush) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = GetParam();
  options.write_buffer_size = (static_cast<size_t>(64) << 20);

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
    ASSERT_EQ(cfh->cfd()->GetFlushReason(), FlushReason::kManualFlush);
    ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
  }
}

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

    ASSERT_EQ(PrecomputeMinLogNumberToKeepNon2PC(dbfull()->TEST_GetVersionSet(),
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

    uint64_t min_log_number_to_keep = port::kMaxUint64;
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
    ASSERT_EQ(PrecomputeMinLogNumberToKeepNon2PC(dbfull()->TEST_GetVersionSet(),
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
      new SpecialSkipListFactory(kNumKeysTriggerFlush));
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

INSTANTIATE_TEST_CASE_P(DBFlushDirectIOTest, DBFlushDirectIOTest,
                        testing::Bool());

INSTANTIATE_TEST_CASE_P(DBAtomicFlushTest, DBAtomicFlushTest, testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
