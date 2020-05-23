//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/debug.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "test_util/fault_injection_test_env.h"
#if !defined(ROCKSDB_LITE)
#include "test_util/sync_point.h"
#endif

namespace ROCKSDB_NAMESPACE {

class DBBasicTest : public DBTestBase {
 public:
  DBBasicTest() : DBTestBase("/db_basic_test") {}
};

TEST_F(DBBasicTest, OpenWhenOpen) {
  Options options = CurrentOptions();
  options.env = env_;
  ROCKSDB_NAMESPACE::DB* db2 = nullptr;
  ROCKSDB_NAMESPACE::Status s = DB::Open(options, dbname_, &db2);

  ASSERT_EQ(Status::Code::kIOError, s.code());
  ASSERT_EQ(Status::SubCode::kNone, s.subcode());
  ASSERT_TRUE(strstr(s.getState(), "lock ") != nullptr);

  delete db2;
}

#ifndef ROCKSDB_LITE
TEST_F(DBBasicTest, ReadOnlyDB) {
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Put("foo", "v3"));
  Close();

  auto options = CurrentOptions();
  assert(options.env == env_);
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  Iterator* iter = db_->NewIterator(ReadOptions());
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    ++count;
  }
  ASSERT_EQ(count, 2);
  delete iter;
  Close();

  // Reopen and flush memtable.
  Reopen(options);
  Flush();
  Close();
  // Now check keys in read only mode.
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  ASSERT_TRUE(db_->SyncWAL().IsNotSupported());
}

TEST_F(DBBasicTest, ReadOnlyDBWithWriteDBIdToManifestSet) {
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Put("foo", "v3"));
  Close();

  auto options = CurrentOptions();
  options.write_dbid_to_manifest = true;
  assert(options.env == env_);
  ASSERT_OK(ReadOnlyReopen(options));
  std::string db_id1;
  db_->GetDbIdentity(db_id1);
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  Iterator* iter = db_->NewIterator(ReadOptions());
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    ++count;
  }
  ASSERT_EQ(count, 2);
  delete iter;
  Close();

  // Reopen and flush memtable.
  Reopen(options);
  Flush();
  Close();
  // Now check keys in read only mode.
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  ASSERT_TRUE(db_->SyncWAL().IsNotSupported());
  std::string db_id2;
  db_->GetDbIdentity(db_id2);
  ASSERT_EQ(db_id1, db_id2);
}

TEST_F(DBBasicTest, CompactedDB) {
  const uint64_t kFileSize = 1 << 20;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.write_buffer_size = kFileSize;
  options.target_file_size_base = kFileSize;
  options.max_bytes_for_level_base = 1 << 30;
  options.compression = kNoCompression;
  Reopen(options);
  // 1 L0 file, use CompactedDB if max_open_files = -1
  ASSERT_OK(Put("aaa", DummyString(kFileSize / 2, '1')));
  Flush();
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  Status s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");
  ASSERT_EQ(DummyString(kFileSize / 2, '1'), Get("aaa"));
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReadOnlyReopen(options));
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported in compacted db mode.");
  ASSERT_EQ(DummyString(kFileSize / 2, '1'), Get("aaa"));
  Close();
  Reopen(options);
  // Add more L0 files
  ASSERT_OK(Put("bbb", DummyString(kFileSize / 2, '2')));
  Flush();
  ASSERT_OK(Put("aaa", DummyString(kFileSize / 2, 'a')));
  Flush();
  ASSERT_OK(Put("bbb", DummyString(kFileSize / 2, 'b')));
  ASSERT_OK(Put("eee", DummyString(kFileSize / 2, 'e')));
  Flush();
  Close();

  ASSERT_OK(ReadOnlyReopen(options));
  // Fallback to read-only DB
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");
  Close();

  // Full compaction
  Reopen(options);
  // Add more keys
  ASSERT_OK(Put("fff", DummyString(kFileSize / 2, 'f')));
  ASSERT_OK(Put("hhh", DummyString(kFileSize / 2, 'h')));
  ASSERT_OK(Put("iii", DummyString(kFileSize / 2, 'i')));
  ASSERT_OK(Put("jjj", DummyString(kFileSize / 2, 'j')));
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(3, NumTableFilesAtLevel(1));
  Close();

  // CompactedDB
  ASSERT_OK(ReadOnlyReopen(options));
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported in compacted db mode.");
  ASSERT_EQ("NOT_FOUND", Get("abc"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'a'), Get("aaa"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'b'), Get("bbb"));
  ASSERT_EQ("NOT_FOUND", Get("ccc"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'e'), Get("eee"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'f'), Get("fff"));
  ASSERT_EQ("NOT_FOUND", Get("ggg"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'h'), Get("hhh"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'i'), Get("iii"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'j'), Get("jjj"));
  ASSERT_EQ("NOT_FOUND", Get("kkk"));

  // MultiGet
  std::vector<std::string> values;
  std::vector<Status> status_list = dbfull()->MultiGet(
      ReadOptions(),
      std::vector<Slice>({Slice("aaa"), Slice("ccc"), Slice("eee"),
                          Slice("ggg"), Slice("iii"), Slice("kkk")}),
      &values);
  ASSERT_EQ(status_list.size(), static_cast<uint64_t>(6));
  ASSERT_EQ(values.size(), static_cast<uint64_t>(6));
  ASSERT_OK(status_list[0]);
  ASSERT_EQ(DummyString(kFileSize / 2, 'a'), values[0]);
  ASSERT_TRUE(status_list[1].IsNotFound());
  ASSERT_OK(status_list[2]);
  ASSERT_EQ(DummyString(kFileSize / 2, 'e'), values[2]);
  ASSERT_TRUE(status_list[3].IsNotFound());
  ASSERT_OK(status_list[4]);
  ASSERT_EQ(DummyString(kFileSize / 2, 'i'), values[4]);
  ASSERT_TRUE(status_list[5].IsNotFound());

  Reopen(options);
  // Add a key
  ASSERT_OK(Put("fff", DummyString(kFileSize / 2, 'f')));
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");
}

TEST_F(DBBasicTest, LevelLimitReopen) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  const std::string value(1024 * 1024, ' ');
  int i = 0;
  while (NumTableFilesAtLevel(2, 1) == 0) {
    ASSERT_OK(Put(1, Key(i++), value));
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }

  options.num_levels = 1;
  options.max_bytes_for_level_multiplier_additional.resize(1, 1);
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ(s.IsInvalidArgument(), true);
  ASSERT_EQ(s.ToString(),
            "Invalid argument: db has more levels than options.num_levels");

  options.num_levels = 10;
  options.max_bytes_for_level_multiplier_additional.resize(10, 1);
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
}
#endif  // ROCKSDB_LITE

TEST_F(DBBasicTest, PutDeleteGet) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ASSERT_EQ("v2", Get(1, "foo"));
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
  } while (ChangeOptions());
}

TEST_F(DBBasicTest, PutSingleDeleteGet) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo2", "v2"));
    ASSERT_EQ("v2", Get(1, "foo2"));
    ASSERT_OK(SingleDelete(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
    // Ski FIFO and universal compaction because they do not apply to the test
    // case. Skip MergePut because single delete does not get removed when it
    // encounters a merge.
  } while (ChangeOptions(kSkipFIFOCompaction | kSkipUniversalCompaction |
                         kSkipMergePut));
}

TEST_F(DBBasicTest, EmptyFlush) {
  // It is possible to produce empty flushes when using single deletes. Tests
  // whether empty flushes cause issues.
  do {
    Random rnd(301);

    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    Put(1, "a", Slice());
    SingleDelete(1, "a");
    ASSERT_OK(Flush(1));

    ASSERT_EQ("[ ]", AllEntriesFor("a", 1));
    // Skip FIFO and  universal compaction as they do not apply to the test
    // case. Skip MergePut because merges cannot be combined with single
    // deletions.
  } while (ChangeOptions(kSkipFIFOCompaction | kSkipUniversalCompaction |
                         kSkipMergePut));
}

TEST_F(DBBasicTest, GetFromVersions) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
  } while (ChangeOptions());
}

#ifndef ROCKSDB_LITE
TEST_F(DBBasicTest, GetSnapshot) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    // Try with both a short key and a long key
    for (int i = 0; i < 2; i++) {
      std::string key = (i == 0) ? std::string("foo") : std::string(200, 'x');
      ASSERT_OK(Put(1, key, "v1"));
      const Snapshot* s1 = db_->GetSnapshot();
      ASSERT_OK(Put(1, key, "v2"));
      ASSERT_EQ("v2", Get(1, key));
      ASSERT_EQ("v1", Get(1, key, s1));
      ASSERT_OK(Flush(1));
      ASSERT_EQ("v2", Get(1, key));
      ASSERT_EQ("v1", Get(1, key, s1));
      db_->ReleaseSnapshot(s1);
    }
  } while (ChangeOptions());
}
#endif  // ROCKSDB_LITE

TEST_F(DBBasicTest, CheckLock) {
  do {
    DB* localdb;
    Options options = CurrentOptions();
    ASSERT_OK(TryReopen(options));

    // second open should fail
    Status s = DB::Open(options, dbname_, &localdb);
    ASSERT_NOK(s);
#ifdef OS_LINUX
    ASSERT_TRUE(s.ToString().find("lock hold by current process") !=
                std::string::npos);
#endif  // OS_LINUX
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, FlushMultipleMemtable) {
  do {
    Options options = CurrentOptions();
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    options.max_write_buffer_number = 4;
    options.min_write_buffer_number_to_merge = 3;
    options.max_write_buffer_size_to_maintain = -1;
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));
    ASSERT_OK(Flush(1));
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, FlushEmptyColumnFamily) {
  // Block flush thread and disable compaction thread
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);

  Options options = CurrentOptions();
  // disable compaction
  options.disable_auto_compactions = true;
  WriteOptions writeOpt = WriteOptions();
  writeOpt.disableWAL = true;
  options.max_write_buffer_number = 2;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_size_to_maintain =
      static_cast<int64_t>(options.write_buffer_size);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Compaction can still go through even if no thread can flush the
  // mem table.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  // Insert can go through
  ASSERT_OK(dbfull()->Put(writeOpt, handles_[0], "foo", "v1"));
  ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

  ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("v1", Get(1, "bar"));

  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();

  // Flush can still go through.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}

TEST_F(DBBasicTest, Flush) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    SetPerfLevel(kEnableTime);
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    // this will now also flush the last 2 writes
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    get_perf_context()->Reset();
    Get(1, "foo");
    ASSERT_TRUE((int)get_perf_context()->get_from_output_files_time > 0);
    ASSERT_EQ(2, (int)get_perf_context()->get_read_bytes);

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));
    ASSERT_OK(Flush(1));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v2", Get(1, "bar"));
    get_perf_context()->Reset();
    ASSERT_EQ("v2", Get(1, "foo"));
    ASSERT_TRUE((int)get_perf_context()->get_from_output_files_time > 0);

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));
    ASSERT_OK(Flush(1));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // 'foo' should be there because its put
    // has WAL enabled.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "bar"));

    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, ManifestRollOver) {
  do {
    Options options;
    options.max_manifest_file_size = 10;  // 10 bytes
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    {
      ASSERT_OK(Put(1, "manifest_key1", std::string(1000, '1')));
      ASSERT_OK(Put(1, "manifest_key2", std::string(1000, '2')));
      ASSERT_OK(Put(1, "manifest_key3", std::string(1000, '3')));
      uint64_t manifest_before_flush = dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_OK(Flush(1));  // This should trigger LogAndApply.
      uint64_t manifest_after_flush = dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_GT(manifest_after_flush, manifest_before_flush);
      ReopenWithColumnFamilies({"default", "pikachu"}, options);
      ASSERT_GT(dbfull()->TEST_Current_Manifest_FileNo(), manifest_after_flush);
      // check if a new manifest file got inserted or not.
      ASSERT_EQ(std::string(1000, '1'), Get(1, "manifest_key1"));
      ASSERT_EQ(std::string(1000, '2'), Get(1, "manifest_key2"));
      ASSERT_EQ(std::string(1000, '3'), Get(1, "manifest_key3"));
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, IdentityAcrossRestarts1) {
  do {
    std::string id1;
    ASSERT_OK(db_->GetDbIdentity(id1));

    Options options = CurrentOptions();
    Reopen(options);
    std::string id2;
    ASSERT_OK(db_->GetDbIdentity(id2));
    // id1 should match id2 because identity was not regenerated
    ASSERT_EQ(id1.compare(id2), 0);

    std::string idfilename = IdentityFileName(dbname_);
    ASSERT_OK(env_->DeleteFile(idfilename));
    Reopen(options);
    std::string id3;
    ASSERT_OK(db_->GetDbIdentity(id3));
    if (options.write_dbid_to_manifest) {
      ASSERT_EQ(id1.compare(id3), 0);
    } else {
      // id1 should NOT match id3 because identity was regenerated
      ASSERT_NE(id1.compare(id3), 0);
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, IdentityAcrossRestarts2) {
  do {
    std::string id1;
    ASSERT_OK(db_->GetDbIdentity(id1));

    Options options = CurrentOptions();
    options.write_dbid_to_manifest = true;
    Reopen(options);
    std::string id2;
    ASSERT_OK(db_->GetDbIdentity(id2));
    // id1 should match id2 because identity was not regenerated
    ASSERT_EQ(id1.compare(id2), 0);

    std::string idfilename = IdentityFileName(dbname_);
    ASSERT_OK(env_->DeleteFile(idfilename));
    Reopen(options);
    std::string id3;
    ASSERT_OK(db_->GetDbIdentity(id3));
    // id1 should NOT match id3 because identity was regenerated
    ASSERT_EQ(id1, id3);
  } while (ChangeCompactOptions());
}

#ifndef ROCKSDB_LITE
TEST_F(DBBasicTest, Snapshot) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    Put(0, "foo", "0v1");
    Put(1, "foo", "1v1");

    const Snapshot* s1 = db_->GetSnapshot();
    ASSERT_EQ(1U, GetNumSnapshots());
    uint64_t time_snap1 = GetTimeOldestSnapshots();
    ASSERT_GT(time_snap1, 0U);
    ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());
    Put(0, "foo", "0v2");
    Put(1, "foo", "1v2");

    env_->addon_time_.fetch_add(1);

    const Snapshot* s2 = db_->GetSnapshot();
    ASSERT_EQ(2U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());
    Put(0, "foo", "0v3");
    Put(1, "foo", "1v3");

    {
      ManagedSnapshot s3(db_);
      ASSERT_EQ(3U, GetNumSnapshots());
      ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
      ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());

      Put(0, "foo", "0v4");
      Put(1, "foo", "1v4");
      ASSERT_EQ("0v1", Get(0, "foo", s1));
      ASSERT_EQ("1v1", Get(1, "foo", s1));
      ASSERT_EQ("0v2", Get(0, "foo", s2));
      ASSERT_EQ("1v2", Get(1, "foo", s2));
      ASSERT_EQ("0v3", Get(0, "foo", s3.snapshot()));
      ASSERT_EQ("1v3", Get(1, "foo", s3.snapshot()));
      ASSERT_EQ("0v4", Get(0, "foo"));
      ASSERT_EQ("1v4", Get(1, "foo"));
    }

    ASSERT_EQ(2U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());
    ASSERT_EQ("0v1", Get(0, "foo", s1));
    ASSERT_EQ("1v1", Get(1, "foo", s1));
    ASSERT_EQ("0v2", Get(0, "foo", s2));
    ASSERT_EQ("1v2", Get(1, "foo", s2));
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));

    db_->ReleaseSnapshot(s1);
    ASSERT_EQ("0v2", Get(0, "foo", s2));
    ASSERT_EQ("1v2", Get(1, "foo", s2));
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));
    ASSERT_EQ(1U, GetNumSnapshots());
    ASSERT_LT(time_snap1, GetTimeOldestSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), s2->GetSequenceNumber());

    db_->ReleaseSnapshot(s2);
    ASSERT_EQ(0U, GetNumSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), 0);
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));
  } while (ChangeOptions());
}

#endif  // ROCKSDB_LITE

TEST_F(DBBasicTest, CompactBetweenSnapshots) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    Options options = CurrentOptions(options_override);
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);
    Random rnd(301);
    FillLevels("a", "z", 1);

    Put(1, "foo", "first");
    const Snapshot* snapshot1 = db_->GetSnapshot();
    Put(1, "foo", "second");
    Put(1, "foo", "third");
    Put(1, "foo", "fourth");
    const Snapshot* snapshot2 = db_->GetSnapshot();
    Put(1, "foo", "fifth");
    Put(1, "foo", "sixth");

    // All entries (including duplicates) exist
    // before any compaction or flush is triggered.
    ASSERT_EQ(AllEntriesFor("foo", 1),
              "[ sixth, fifth, fourth, third, second, first ]");
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ("fourth", Get(1, "foo", snapshot2));
    ASSERT_EQ("first", Get(1, "foo", snapshot1));

    // After a flush, "second", "third" and "fifth" should
    // be removed
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth, fourth, first ]");

    // after we release the snapshot1, only two values left
    db_->ReleaseSnapshot(snapshot1);
    FillLevels("a", "z", 1);
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);

    // We have only one valid snapshot snapshot2. Since snapshot1 is
    // not valid anymore, "first" should be removed by a compaction.
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ("fourth", Get(1, "foo", snapshot2));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth, fourth ]");

    // after we release the snapshot2, only one value should be left
    db_->ReleaseSnapshot(snapshot2);
    FillLevels("a", "z", 1);
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth ]");
  } while (ChangeOptions(kSkipFIFOCompaction));
}

TEST_F(DBBasicTest, DBOpen_Options) {
  Options options = CurrentOptions();
  Close();
  Destroy(options);

  // Does not exist, and create_if_missing == false: error
  DB* db = nullptr;
  options.create_if_missing = false;
  Status s = DB::Open(options, dbname_, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "does not exist") != nullptr);
  ASSERT_TRUE(db == nullptr);

  // Does not exist, and create_if_missing == true: OK
  options.create_if_missing = true;
  s = DB::Open(options, dbname_, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  delete db;
  db = nullptr;

  // Does exist, and error_if_exists == true: error
  options.create_if_missing = false;
  options.error_if_exists = true;
  s = DB::Open(options, dbname_, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "exists") != nullptr);
  ASSERT_TRUE(db == nullptr);

  // Does exist, and error_if_exists == false: OK
  options.create_if_missing = true;
  options.error_if_exists = false;
  s = DB::Open(options, dbname_, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  delete db;
  db = nullptr;
}

TEST_F(DBBasicTest, CompactOnFlush) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    Options options = CurrentOptions(options_override);
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    Put(1, "foo", "v1");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v1 ]");

    // Write two new keys
    Put(1, "a", "begin");
    Put(1, "z", "end");
    Flush(1);

    // Case1: Delete followed by a put
    Delete(1, "foo");
    Put(1, "foo", "v2");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, DEL, v1 ]");

    // After the current memtable is flushed, the DEL should
    // have been removed
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, v1 ]");

    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2 ]");

    // Case 2: Delete followed by another delete
    Delete(1, "foo");
    Delete(1, "foo");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, DEL, v2 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v2 ]");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 3: Put followed by a delete
    Put(1, "foo", "v3");
    Delete(1, "foo");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v3 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL ]");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 4: Put followed by another Put
    Put(1, "foo", "v4");
    Put(1, "foo", "v5");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5, v4 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5 ]");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5 ]");

    // clear database
    Delete(1, "foo");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 5: Put followed by snapshot followed by another Put
    // Both puts should remain.
    Put(1, "foo", "v6");
    const Snapshot* snapshot = db_->GetSnapshot();
    Put(1, "foo", "v7");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v7, v6 ]");
    db_->ReleaseSnapshot(snapshot);

    // clear database
    Delete(1, "foo");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 5: snapshot followed by a put followed by another Put
    // Only the last put should remain.
    const Snapshot* snapshot1 = db_->GetSnapshot();
    Put(1, "foo", "v8");
    Put(1, "foo", "v9");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v9 ]");
    db_->ReleaseSnapshot(snapshot1);
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, FlushOneColumnFamily) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "pikachu", "pikachu"));
  ASSERT_OK(Put(2, "ilya", "ilya"));
  ASSERT_OK(Put(3, "muromec", "muromec"));
  ASSERT_OK(Put(4, "dobrynia", "dobrynia"));
  ASSERT_OK(Put(5, "nikitich", "nikitich"));
  ASSERT_OK(Put(6, "alyosha", "alyosha"));
  ASSERT_OK(Put(7, "popovich", "popovich"));

  for (int i = 0; i < 8; ++i) {
    Flush(i);
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), i + 1U);
  }
}

TEST_F(DBBasicTest, MultiGetSimple) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    SetPerfLevel(kEnableCount);
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k5", "v5"));
    ASSERT_OK(Delete(1, "no_key"));

    std::vector<Slice> keys({"k1", "k2", "k3", "k4", "k5", "no_key"});

    std::vector<std::string> values(20, "Temporary data to be overwritten");
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);

    get_perf_context()->Reset();
    std::vector<Status> s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(values[0], "v1");
    ASSERT_EQ(values[1], "v2");
    ASSERT_EQ(values[2], "v3");
    ASSERT_EQ(values[4], "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(8, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_OK(s[0]);
    ASSERT_OK(s[1]);
    ASSERT_OK(s[2]);
    ASSERT_TRUE(s[3].IsNotFound());
    ASSERT_OK(s[4]);
    ASSERT_TRUE(s[5].IsNotFound());
    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, MultiGetEmpty) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    // Empty Key Set
    std::vector<Slice> keys;
    std::vector<std::string> values;
    std::vector<ColumnFamilyHandle*> cfs;
    std::vector<Status> s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(s.size(), 0U);

    // Empty Database, Empty Key Set
    Options options = CurrentOptions();
    options.create_if_missing = true;
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(s.size(), 0U);

    // Empty Database, Search for Keys
    keys.resize(2);
    keys[0] = "a";
    keys[1] = "b";
    cfs.push_back(handles_[0]);
    cfs.push_back(handles_[1]);
    s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(static_cast<int>(s.size()), 2);
    ASSERT_TRUE(s[0].IsNotFound() && s[1].IsNotFound());
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, ChecksumTest) {
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();
  // change when new checksum type added
  int max_checksum = static_cast<int>(kxxHash64);
  const int kNumPerFile = 2;

  // generate one table with each type of checksum
  for (int i = 0; i <= max_checksum; ++i) {
    table_options.checksum = static_cast<ChecksumType>(i);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    Reopen(options);
    for (int j = 0; j < kNumPerFile; ++j) {
      ASSERT_OK(Put(Key(i * kNumPerFile + j), Key(i * kNumPerFile + j)));
    }
    ASSERT_OK(Flush());
  }

  // with each valid checksum type setting...
  for (int i = 0; i <= max_checksum; ++i) {
    table_options.checksum = static_cast<ChecksumType>(i);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    Reopen(options);
    // verify every type of checksum (should be regardless of that setting)
    for (int j = 0; j < (max_checksum + 1) * kNumPerFile; ++j) {
      ASSERT_EQ(Key(j), Get(Key(j)));
    }
  }
}

// On Windows you can have either memory mapped file or a file
// with unbuffered access. So this asserts and does not make
// sense to run
#ifndef OS_WIN
TEST_F(DBBasicTest, MmapAndBufferOptions) {
  if (!IsMemoryMappedAccessSupported()) {
    return;
  }
  Options options = CurrentOptions();

  options.use_direct_reads = true;
  options.allow_mmap_reads = true;
  ASSERT_NOK(TryReopen(options));

  // All other combinations are acceptable
  options.use_direct_reads = false;
  ASSERT_OK(TryReopen(options));

  if (IsDirectIOSupported()) {
    options.use_direct_reads = true;
    options.allow_mmap_reads = false;
    ASSERT_OK(TryReopen(options));
  }

  options.use_direct_reads = false;
  ASSERT_OK(TryReopen(options));
}
#endif

class TestEnv : public EnvWrapper {
 public:
  explicit TestEnv(Env* base_env) : EnvWrapper(base_env), close_count(0) {}

  class TestLogger : public Logger {
   public:
    using Logger::Logv;
    explicit TestLogger(TestEnv* env_ptr) : Logger() { env = env_ptr; }
    ~TestLogger() override {
      if (!closed_) {
        CloseHelper();
      }
    }
    void Logv(const char* /*format*/, va_list /*ap*/) override {}

   protected:
    Status CloseImpl() override { return CloseHelper(); }

   private:
    Status CloseHelper() {
      env->CloseCountInc();
      ;
      return Status::IOError();
    }
    TestEnv* env;
  };

  void CloseCountInc() { close_count++; }

  int GetCloseCount() { return close_count; }

  Status NewLogger(const std::string& /*fname*/,
                   std::shared_ptr<Logger>* result) override {
    result->reset(new TestLogger(this));
    return Status::OK();
  }

 private:
  int close_count;
};

TEST_F(DBBasicTest, DBClose) {
  Options options = GetDefaultOptions();
  std::string dbname = test::PerThreadDBPath("db_close_test");
  ASSERT_OK(DestroyDB(dbname, options));

  DB* db = nullptr;
  TestEnv* env = new TestEnv(env_);
  std::unique_ptr<TestEnv> local_env_guard(env);
  options.create_if_missing = true;
  options.env = env;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  s = db->Close();
  ASSERT_EQ(env->GetCloseCount(), 1);
  ASSERT_EQ(s, Status::IOError());

  delete db;
  ASSERT_EQ(env->GetCloseCount(), 1);

  // Do not call DB::Close() and ensure our logger Close() still gets called
  s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  delete db;
  ASSERT_EQ(env->GetCloseCount(), 2);

  // Provide our own logger and ensure DB::Close() does not close it
  options.info_log.reset(new TestEnv::TestLogger(env));
  options.create_if_missing = false;
  s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  s = db->Close();
  ASSERT_EQ(s, Status::OK());
  delete db;
  ASSERT_EQ(env->GetCloseCount(), 2);
  options.info_log.reset();
  ASSERT_EQ(env->GetCloseCount(), 3);
}

TEST_F(DBBasicTest, DBCloseFlushError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.manual_wal_flush = true;
  options.write_buffer_size = 100;
  options.env = fault_injection_env.get();

  Reopen(options);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("key3", "value3"));
  fault_injection_env->SetFilesystemActive(false);
  Status s = dbfull()->Close();
  fault_injection_env->SetFilesystemActive(true);
  ASSERT_NE(s, Status::OK());

  Destroy(options);
}

class DBMultiGetTestWithParam : public DBBasicTest,
                                public testing::WithParamInterface<bool> {};

TEST_P(DBMultiGetTestWithParam, MultiGetMultiCF) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);
  // <CF, key, value> tuples
  std::vector<std::tuple<int, std::string, std::string>> cf_kv_vec;
  static const int num_keys = 24;
  cf_kv_vec.reserve(num_keys);

  for (int i = 0; i < num_keys; ++i) {
    int cf = i / 3;
    int cf_key = 1 % 3;
    cf_kv_vec.emplace_back(std::make_tuple(
        cf, "cf" + std::to_string(cf) + "_key_" + std::to_string(cf_key),
        "cf" + std::to_string(cf) + "_val_" + std::to_string(cf_key)));
    ASSERT_OK(Put(std::get<0>(cf_kv_vec[i]), std::get<1>(cf_kv_vec[i]),
                  std::get<2>(cf_kv_vec[i])));
  }

  int get_sv_count = 0;
  ROCKSDB_NAMESPACE::DBImpl* db = reinterpret_cast<DBImpl*>(db_);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiGet::AfterRefSV", [&](void* /*arg*/) {
        if (++get_sv_count == 2) {
          // After MultiGet refs a couple of CFs, flush all CFs so MultiGet
          // is forced to repeat the process
          for (int i = 0; i < num_keys; ++i) {
            int cf = i / 3;
            int cf_key = i % 8;
            if (cf_key == 0) {
              ASSERT_OK(Flush(cf));
            }
            ASSERT_OK(Put(std::get<0>(cf_kv_vec[i]), std::get<1>(cf_kv_vec[i]),
                          std::get<2>(cf_kv_vec[i]) + "_2"));
          }
        }
        if (get_sv_count == 11) {
          for (int i = 0; i < 8; ++i) {
            auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(
                            db->GetColumnFamilyHandle(i))
                            ->cfd();
            ASSERT_EQ(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<int> cfs;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 0; i < num_keys; ++i) {
    cfs.push_back(std::get<0>(cf_kv_vec[i]));
    keys.push_back(std::get<1>(cf_kv_vec[i]));
  }

  values = MultiGet(cfs, keys, nullptr, GetParam());
  ASSERT_EQ(values.size(), num_keys);
  for (unsigned int j = 0; j < values.size(); ++j) {
    ASSERT_EQ(values[j], std::get<2>(cf_kv_vec[j]) + "_2");
  }

  keys.clear();
  cfs.clear();
  cfs.push_back(std::get<0>(cf_kv_vec[0]));
  keys.push_back(std::get<1>(cf_kv_vec[0]));
  cfs.push_back(std::get<0>(cf_kv_vec[3]));
  keys.push_back(std::get<1>(cf_kv_vec[3]));
  cfs.push_back(std::get<0>(cf_kv_vec[4]));
  keys.push_back(std::get<1>(cf_kv_vec[4]));
  values = MultiGet(cfs, keys, nullptr, GetParam());
  ASSERT_EQ(values[0], std::get<2>(cf_kv_vec[0]) + "_2");
  ASSERT_EQ(values[1], std::get<2>(cf_kv_vec[3]) + "_2");
  ASSERT_EQ(values[2], std::get<2>(cf_kv_vec[4]) + "_2");

  keys.clear();
  cfs.clear();
  cfs.push_back(std::get<0>(cf_kv_vec[7]));
  keys.push_back(std::get<1>(cf_kv_vec[7]));
  cfs.push_back(std::get<0>(cf_kv_vec[6]));
  keys.push_back(std::get<1>(cf_kv_vec[6]));
  cfs.push_back(std::get<0>(cf_kv_vec[1]));
  keys.push_back(std::get<1>(cf_kv_vec[1]));
  values = MultiGet(cfs, keys, nullptr, GetParam());
  ASSERT_EQ(values[0], std::get<2>(cf_kv_vec[7]) + "_2");
  ASSERT_EQ(values[1], std::get<2>(cf_kv_vec[6]) + "_2");
  ASSERT_EQ(values[2], std::get<2>(cf_kv_vec[1]) + "_2");

  for (int cf = 0; cf < 8; ++cf) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(
                    reinterpret_cast<DBImpl*>(db_)->GetColumnFamilyHandle(cf))
                    ->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVObsolete);
  }
}

TEST_P(DBMultiGetTestWithParam, MultiGetMultiCFMutex) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  int get_sv_count = 0;
  int retries = 0;
  bool last_try = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiGet::LastTry", [&](void* /*arg*/) {
        last_try = true;
        ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiGet::AfterRefSV", [&](void* /*arg*/) {
        if (last_try) {
          return;
        }
        if (++get_sv_count == 2) {
          ++retries;
          get_sv_count = 0;
          for (int i = 0; i < 8; ++i) {
            ASSERT_OK(Flush(i));
            ASSERT_OK(Put(
                i, "cf" + std::to_string(i) + "_key",
                "cf" + std::to_string(i) + "_val" + std::to_string(retries)));
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<int> cfs;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 0; i < 8; ++i) {
    cfs.push_back(i);
    keys.push_back("cf" + std::to_string(i) + "_key");
  }

  values = MultiGet(cfs, keys, nullptr, GetParam());
  ASSERT_TRUE(last_try);
  ASSERT_EQ(values.size(), 8);
  for (unsigned int j = 0; j < values.size(); ++j) {
    ASSERT_EQ(values[j],
              "cf" + std::to_string(j) + "_val" + std::to_string(retries));
  }
  for (int i = 0; i < 8; ++i) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(
                    reinterpret_cast<DBImpl*>(db_)->GetColumnFamilyHandle(i))
                    ->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
  }
}

TEST_P(DBMultiGetTestWithParam, MultiGetMultiCFSnapshot) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  int get_sv_count = 0;
  ROCKSDB_NAMESPACE::DBImpl* db = reinterpret_cast<DBImpl*>(db_);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiGet::AfterRefSV", [&](void* /*arg*/) {
        if (++get_sv_count == 2) {
          for (int i = 0; i < 8; ++i) {
            ASSERT_OK(Flush(i));
            ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                          "cf" + std::to_string(i) + "_val2"));
          }
        }
        if (get_sv_count == 8) {
          for (int i = 0; i < 8; ++i) {
            auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(
                            db->GetColumnFamilyHandle(i))
                            ->cfd();
            ASSERT_TRUE(
                (cfd->TEST_GetLocalSV()->Get() == SuperVersion::kSVInUse) ||
                (cfd->TEST_GetLocalSV()->Get() == SuperVersion::kSVObsolete));
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<int> cfs;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 0; i < 8; ++i) {
    cfs.push_back(i);
    keys.push_back("cf" + std::to_string(i) + "_key");
  }

  const Snapshot* snapshot = db_->GetSnapshot();
  values = MultiGet(cfs, keys, snapshot, GetParam());
  db_->ReleaseSnapshot(snapshot);
  ASSERT_EQ(values.size(), 8);
  for (unsigned int j = 0; j < values.size(); ++j) {
    ASSERT_EQ(values[j], "cf" + std::to_string(j) + "_val");
  }
  for (int i = 0; i < 8; ++i) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(
                    reinterpret_cast<DBImpl*>(db_)->GetColumnFamilyHandle(i))
                    ->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
  }
}

INSTANTIATE_TEST_CASE_P(DBMultiGetTestWithParam, DBMultiGetTestWithParam,
                        testing::Bool());

TEST_F(DBBasicTest, MultiGetBatchedSimpleUnsorted) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    SetPerfLevel(kEnableCount);
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k5", "v5"));
    ASSERT_OK(Delete(1, "no_key"));

    get_perf_context()->Reset();

    std::vector<Slice> keys({"no_key", "k5", "k4", "k3", "k2", "k1"});
    std::vector<PinnableSlice> values(keys.size());
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
    std::vector<Status> s(keys.size());

    db_->MultiGet(ReadOptions(), handles_[1], keys.size(), keys.data(),
                  values.data(), s.data(), false);

    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(std::string(values[5].data(), values[5].size()), "v1");
    ASSERT_EQ(std::string(values[4].data(), values[4].size()), "v2");
    ASSERT_EQ(std::string(values[3].data(), values[3].size()), "v3");
    ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(8, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_TRUE(s[0].IsNotFound());
    ASSERT_OK(s[1]);
    ASSERT_TRUE(s[2].IsNotFound());
    ASSERT_OK(s[3]);
    ASSERT_OK(s[4]);
    ASSERT_OK(s[5]);

    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, MultiGetBatchedSortedMultiFile) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    SetPerfLevel(kEnableCount);
    // To expand the power of this test, generate > 1 table file and
    // mix with memtable
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    Flush(1);
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    Flush(1);
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k5", "v5"));
    ASSERT_OK(Delete(1, "no_key"));

    get_perf_context()->Reset();

    std::vector<Slice> keys({"k1", "k2", "k3", "k4", "k5", "no_key"});
    std::vector<PinnableSlice> values(keys.size());
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
    std::vector<Status> s(keys.size());

    db_->MultiGet(ReadOptions(), handles_[1], keys.size(), keys.data(),
                  values.data(), s.data(), true);

    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(std::string(values[0].data(), values[0].size()), "v1");
    ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v2");
    ASSERT_EQ(std::string(values[2].data(), values[2].size()), "v3");
    ASSERT_EQ(std::string(values[4].data(), values[4].size()), "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(8, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_OK(s[0]);
    ASSERT_OK(s[1]);
    ASSERT_OK(s[2]);
    ASSERT_TRUE(s[3].IsNotFound());
    ASSERT_OK(s[4]);
    ASSERT_TRUE(s[5].IsNotFound());

    SetPerfLevel(kDisable);
  } while (ChangeOptions());
}

TEST_F(DBBasicTest, MultiGetBatchedMultiLevel) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);
  int num_keys = 0;

  for (int i = 0; i < 128; ++i) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l2_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      Flush();
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    Flush();
    num_keys = 0;
  }
  MoveFilesToLevel(2);

  for (int i = 0; i < 128; i += 3) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l1_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      Flush();
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    Flush();
    num_keys = 0;
  }
  MoveFilesToLevel(1);

  for (int i = 0; i < 128; i += 5) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l0_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      Flush();
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    Flush();
    num_keys = 0;
  }
  ASSERT_EQ(0, num_keys);

  for (int i = 0; i < 128; i += 9) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_mem_" + std::to_string(i)));
  }

  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 64; i < 80; ++i) {
    keys.push_back("key_" + std::to_string(i));
  }

  values = MultiGet(keys, nullptr);
  ASSERT_EQ(values.size(), 16);
  for (unsigned int j = 0; j < values.size(); ++j) {
    int key = j + 64;
    if (key % 9 == 0) {
      ASSERT_EQ(values[j], "val_mem_" + std::to_string(key));
    } else if (key % 5 == 0) {
      ASSERT_EQ(values[j], "val_l0_" + std::to_string(key));
    } else if (key % 3 == 0) {
      ASSERT_EQ(values[j], "val_l1_" + std::to_string(key));
    } else {
      ASSERT_EQ(values[j], "val_l2_" + std::to_string(key));
    }
  }
}

TEST_F(DBBasicTest, MultiGetBatchedMultiLevelMerge) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);
  int num_keys = 0;

  for (int i = 0; i < 128; ++i) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l2_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      Flush();
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    Flush();
    num_keys = 0;
  }
  MoveFilesToLevel(2);

  for (int i = 0; i < 128; i += 3) {
    ASSERT_OK(Merge("key_" + std::to_string(i), "val_l1_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      Flush();
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    Flush();
    num_keys = 0;
  }
  MoveFilesToLevel(1);

  for (int i = 0; i < 128; i += 5) {
    ASSERT_OK(Merge("key_" + std::to_string(i), "val_l0_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      Flush();
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    Flush();
    num_keys = 0;
  }
  ASSERT_EQ(0, num_keys);

  for (int i = 0; i < 128; i += 9) {
    ASSERT_OK(
        Merge("key_" + std::to_string(i), "val_mem_" + std::to_string(i)));
  }

  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 32; i < 80; ++i) {
    keys.push_back("key_" + std::to_string(i));
  }

  values = MultiGet(keys, nullptr);
  ASSERT_EQ(values.size(), keys.size());
  for (unsigned int j = 0; j < 48; ++j) {
    int key = j + 32;
    std::string value;
    value.append("val_l2_" + std::to_string(key));
    if (key % 3 == 0) {
      value.append(",");
      value.append("val_l1_" + std::to_string(key));
    }
    if (key % 5 == 0) {
      value.append(",");
      value.append("val_l0_" + std::to_string(key));
    }
    if (key % 9 == 0) {
      value.append(",");
      value.append("val_mem_" + std::to_string(key));
    }
    ASSERT_EQ(values[j], value);
  }
}

// Test class for batched MultiGet with prefix extractor
// Param bool - If true, use partitioned filters
//              If false, use full filter block
class MultiGetPrefixExtractorTest : public DBBasicTest,
                                    public ::testing::WithParamInterface<bool> {
};

TEST_P(MultiGetPrefixExtractorTest, Batched) {
  Options options = CurrentOptions();
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_prefix_bloom_size_ratio = 10;
  BlockBasedTableOptions bbto;
  if (GetParam()) {
    bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    bbto.partition_filters = true;
  }
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;
  bbto.cache_index_and_filter_blocks = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  SetPerfLevel(kEnableCount);
  get_perf_context()->Reset();

  // First key is not in the prefix_extractor domain
  ASSERT_OK(Put("k", "v0"));
  ASSERT_OK(Put("kk1", "v1"));
  ASSERT_OK(Put("kk2", "v2"));
  ASSERT_OK(Put("kk3", "v3"));
  ASSERT_OK(Put("kk4", "v4"));
  std::vector<std::string> mem_keys(
      {"k", "kk1", "kk2", "kk3", "kk4", "rofl", "lmho"});
  std::vector<std::string> inmem_values;
  inmem_values = MultiGet(mem_keys, nullptr);
  ASSERT_EQ(inmem_values[0], "v0");
  ASSERT_EQ(inmem_values[1], "v1");
  ASSERT_EQ(inmem_values[2], "v2");
  ASSERT_EQ(inmem_values[3], "v3");
  ASSERT_EQ(inmem_values[4], "v4");
  ASSERT_EQ(get_perf_context()->bloom_memtable_miss_count, 2);
  ASSERT_EQ(get_perf_context()->bloom_memtable_hit_count, 5);
  ASSERT_OK(Flush());

  std::vector<std::string> keys({"k", "kk1", "kk2", "kk3", "kk4"});
  std::vector<std::string> values;
  get_perf_context()->Reset();
  values = MultiGet(keys, nullptr);
  ASSERT_EQ(values[0], "v0");
  ASSERT_EQ(values[1], "v1");
  ASSERT_EQ(values[2], "v2");
  ASSERT_EQ(values[3], "v3");
  ASSERT_EQ(values[4], "v4");
  // Filter hits for 4 in-domain keys
  ASSERT_EQ(get_perf_context()->bloom_sst_hit_count, 4);
}

INSTANTIATE_TEST_CASE_P(MultiGetPrefix, MultiGetPrefixExtractorTest,
                        ::testing::Bool());

#ifndef ROCKSDB_LITE
class DBMultiGetRowCacheTest : public DBBasicTest,
                               public ::testing::WithParamInterface<bool> {};

TEST_P(DBMultiGetRowCacheTest, MultiGetBatched) {
  do {
    option_config_ = kRowCache;
    Options options = CurrentOptions();
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    CreateAndReopenWithCF({"pikachu"}, options);
    SetPerfLevel(kEnableCount);
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    Flush(1);
    ASSERT_OK(Put(1, "k5", "v5"));
    const Snapshot* snap1 = dbfull()->GetSnapshot();
    ASSERT_OK(Delete(1, "k4"));
    Flush(1);
    const Snapshot* snap2 = dbfull()->GetSnapshot();

    get_perf_context()->Reset();

    std::vector<Slice> keys({"no_key", "k5", "k4", "k3", "k1"});
    std::vector<PinnableSlice> values(keys.size());
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
    std::vector<Status> s(keys.size());

    ReadOptions ro;
    bool use_snapshots = GetParam();
    if (use_snapshots) {
      ro.snapshot = snap2;
    }
    db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                  s.data(), false);

    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(std::string(values[4].data(), values[4].size()), "v1");
    ASSERT_EQ(std::string(values[3].data(), values[3].size()), "v3");
    ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(6, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_TRUE(s[0].IsNotFound());
    ASSERT_OK(s[1]);
    ASSERT_TRUE(s[2].IsNotFound());
    ASSERT_OK(s[3]);
    ASSERT_OK(s[4]);

    // Call MultiGet() again with some intersection with the previous set of
    // keys. Those should already be in the row cache.
    keys.assign({"no_key", "k5", "k3", "k2"});
    for (size_t i = 0; i < keys.size(); ++i) {
      values[i].Reset();
      s[i] = Status::OK();
    }
    get_perf_context()->Reset();

    if (use_snapshots) {
      ro.snapshot = snap1;
    }
    db_->MultiGet(ReadOptions(), handles_[1], keys.size(), keys.data(),
                  values.data(), s.data(), false);

    ASSERT_EQ(std::string(values[3].data(), values[3].size()), "v2");
    ASSERT_EQ(std::string(values[2].data(), values[2].size()), "v3");
    ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(6, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_TRUE(s[0].IsNotFound());
    ASSERT_OK(s[1]);
    ASSERT_OK(s[2]);
    ASSERT_OK(s[3]);
    if (use_snapshots) {
      // Only reads from the first SST file would have been cached, since
      // snapshot seq no is > fd.largest_seqno
      ASSERT_EQ(1, TestGetTickerCount(options, ROW_CACHE_HIT));
    } else {
      ASSERT_EQ(2, TestGetTickerCount(options, ROW_CACHE_HIT));
    }

    SetPerfLevel(kDisable);
    dbfull()->ReleaseSnapshot(snap1);
    dbfull()->ReleaseSnapshot(snap2);
  } while (ChangeCompactOptions());
}

INSTANTIATE_TEST_CASE_P(DBMultiGetRowCacheTest, DBMultiGetRowCacheTest,
                        testing::Values(true, false));

TEST_F(DBBasicTest, GetAllKeyVersions) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(2, handles_.size());
  const size_t kNumInserts = 4;
  const size_t kNumDeletes = 4;
  const size_t kNumUpdates = 4;

  // Check default column family
  for (size_t i = 0; i != kNumInserts; ++i) {
    ASSERT_OK(Put(std::to_string(i), "value"));
  }
  for (size_t i = 0; i != kNumUpdates; ++i) {
    ASSERT_OK(Put(std::to_string(i), "value1"));
  }
  for (size_t i = 0; i != kNumDeletes; ++i) {
    ASSERT_OK(Delete(std::to_string(i)));
  }
  std::vector<KeyVersion> key_versions;
  ASSERT_OK(ROCKSDB_NAMESPACE::GetAllKeyVersions(
      db_, Slice(), Slice(), std::numeric_limits<size_t>::max(),
      &key_versions));
  ASSERT_EQ(kNumInserts + kNumDeletes + kNumUpdates, key_versions.size());
  ASSERT_OK(ROCKSDB_NAMESPACE::GetAllKeyVersions(
      db_, handles_[0], Slice(), Slice(), std::numeric_limits<size_t>::max(),
      &key_versions));
  ASSERT_EQ(kNumInserts + kNumDeletes + kNumUpdates, key_versions.size());

  // Check non-default column family
  for (size_t i = 0; i != kNumInserts - 1; ++i) {
    ASSERT_OK(Put(1, std::to_string(i), "value"));
  }
  for (size_t i = 0; i != kNumUpdates - 1; ++i) {
    ASSERT_OK(Put(1, std::to_string(i), "value1"));
  }
  for (size_t i = 0; i != kNumDeletes - 1; ++i) {
    ASSERT_OK(Delete(1, std::to_string(i)));
  }
  ASSERT_OK(ROCKSDB_NAMESPACE::GetAllKeyVersions(
      db_, handles_[1], Slice(), Slice(), std::numeric_limits<size_t>::max(),
      &key_versions));
  ASSERT_EQ(kNumInserts + kNumDeletes + kNumUpdates - 3, key_versions.size());
}
#endif  // !ROCKSDB_LITE

TEST_F(DBBasicTest, MultiGetIOBufferOverrun) {
  Options options = CurrentOptions();
  Random rnd(301);
  BlockBasedTableOptions table_options;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  table_options.block_size = 16 * 1024;
  ASSERT_TRUE(table_options.block_size >
            BlockBasedTable::kMultiGetReadStackBufSize);
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  Reopen(options);

  std::string zero_str(128, '\0');
  for (int i = 0; i < 100; ++i) {
    // Make the value compressible. A purely random string doesn't compress
    // and the resultant data block will not be compressed
    std::string value(RandomString(&rnd, 128) + zero_str);
    assert(Put(Key(i), value) == Status::OK());
  }
  Flush();

  std::vector<std::string> key_data(10);
  std::vector<Slice> keys;
  // We cannot resize a PinnableSlice vector, so just set initial size to
  // largest we think we will need
  std::vector<PinnableSlice> values(10);
  std::vector<Status> statuses;
  ReadOptions ro;

  // Warm up the cache first
  key_data.emplace_back(Key(0));
  keys.emplace_back(Slice(key_data.back()));
  key_data.emplace_back(Key(50));
  keys.emplace_back(Slice(key_data.back()));
  statuses.resize(keys.size());

  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
}

TEST_F(DBBasicTest, IncrementalRecoveryNoCorrupt) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions write_opts;
  write_opts.disableWAL = true;
  for (size_t cf = 0; cf != num_cfs; ++cf) {
    for (size_t i = 0; i != 10000; ++i) {
      std::string key_str = Key(static_cast<int>(i));
      std::string value_str = std::to_string(cf) + "_" + std::to_string(i);

      ASSERT_OK(Put(static_cast<int>(cf), key_str, value_str));
      if (0 == (i % 1000)) {
        ASSERT_OK(Flush(static_cast<int>(cf)));
      }
    }
  }
  for (size_t cf = 0; cf != num_cfs; ++cf) {
    ASSERT_OK(Flush(static_cast<int>(cf)));
  }
  Close();
  options.best_efforts_recovery = true;
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu", "eevee"},
                           options);
  num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  for (size_t cf = 0; cf != num_cfs; ++cf) {
    for (int i = 0; i != 10000; ++i) {
      std::string key_str = Key(static_cast<int>(i));
      std::string expected_value_str =
          std::to_string(cf) + "_" + std::to_string(i);
      ASSERT_EQ(expected_value_str, Get(static_cast<int>(cf), key_str));
    }
  }
}

TEST_F(DBBasicTest, BestEffortsRecoveryWithVersionBuildingFailure) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistencyBeforeReturn", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        *(reinterpret_cast<Status*>(arg)) =
            Status::Corruption("Inject corruption");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  options.best_efforts_recovery = true;
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsCorruption());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

#ifndef ROCKSDB_LITE
namespace {
class TableFileListener : public EventListener {
 public:
  void OnTableFileCreated(const TableFileCreationInfo& info) override {
    InstrumentedMutexLock lock(&mutex_);
    cf_to_paths_[info.cf_name].push_back(info.file_path);
  }
  std::vector<std::string>& GetFiles(const std::string& cf_name) {
    InstrumentedMutexLock lock(&mutex_);
    return cf_to_paths_[cf_name];
  }

 private:
  InstrumentedMutex mutex_;
  std::unordered_map<std::string, std::vector<std::string>> cf_to_paths_;
};
}  // namespace

TEST_F(DBBasicTest, RecoverWithMissingFiles) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  TableFileListener* listener = new TableFileListener();
  // Disable auto compaction to simplify SST file name tracking.
  options.disable_auto_compactions = true;
  options.listeners.emplace_back(listener);
  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  std::vector<std::string> all_cf_names = {kDefaultColumnFamilyName, "pikachu",
                                           "eevee"};
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  for (size_t cf = 0; cf != num_cfs; ++cf) {
    ASSERT_OK(Put(static_cast<int>(cf), "a", "0_value"));
    ASSERT_OK(Flush(static_cast<int>(cf)));
    ASSERT_OK(Put(static_cast<int>(cf), "b", "0_value"));
    ASSERT_OK(Flush(static_cast<int>(cf)));
    ASSERT_OK(Put(static_cast<int>(cf), "c", "0_value"));
    ASSERT_OK(Flush(static_cast<int>(cf)));
  }

  // Delete and corrupt files
  for (size_t i = 0; i < all_cf_names.size(); ++i) {
    std::vector<std::string>& files = listener->GetFiles(all_cf_names[i]);
    ASSERT_EQ(3, files.size());
    std::string corrupted_data;
    ASSERT_OK(ReadFileToString(env_, files[files.size() - 1], &corrupted_data));
    ASSERT_OK(WriteStringToFile(
        env_, corrupted_data.substr(0, corrupted_data.size() - 2),
        files[files.size() - 1], /*should_sync=*/true));
    for (int j = static_cast<int>(files.size() - 2); j >= static_cast<int>(i);
         --j) {
      ASSERT_OK(env_->DeleteFile(files[j]));
    }
  }
  options.best_efforts_recovery = true;
  ReopenWithColumnFamilies(all_cf_names, options);
  // Verify data
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts, handles_[0]));
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    iter.reset(db_->NewIterator(read_opts, handles_[1]));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a", iter->key());
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    iter.reset(db_->NewIterator(read_opts, handles_[2]));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a", iter->key());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("b", iter->key());
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }
}

TEST_F(DBBasicTest, BestEffortsRecoveryTryMultipleManifests) {
  Options options = CurrentOptions();
  options.env = env_;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value0"));
  ASSERT_OK(Flush());
  Close();
  {
    // Hack by adding a new MANIFEST with high file number
    std::string garbage(10, '\0');
    ASSERT_OK(WriteStringToFile(env_, garbage, dbname_ + "/MANIFEST-001000",
                                /*should_sync=*/true));
  }
  {
    // Hack by adding a corrupted SST not referenced by any MANIFEST
    std::string garbage(10, '\0');
    ASSERT_OK(WriteStringToFile(env_, garbage, dbname_ + "/001001.sst",
                                /*should_sync=*/true));
  }

  options.best_efforts_recovery = true;

  Reopen(options);
  ASSERT_OK(Put("bar", "value"));
}

TEST_F(DBBasicTest, RecoverWithNoCurrentFile) {
  Options options = CurrentOptions();
  options.env = env_;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  options.best_efforts_recovery = true;
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  ASSERT_EQ(2, handles_.size());
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(Put(1, "bar", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(Flush(1));
  Close();
  ASSERT_OK(env_->DeleteFile(CurrentFileName(dbname_)));
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  std::vector<std::string> cf_names;
  ASSERT_OK(DB::ListColumnFamilies(DBOptions(options), dbname_, &cf_names));
  ASSERT_EQ(2, cf_names.size());
  for (const auto& name : cf_names) {
    ASSERT_TRUE(name == kDefaultColumnFamilyName || name == "pikachu");
  }
}

TEST_F(DBBasicTest, SkipWALIfMissingTableFiles) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  TableFileListener* listener = new TableFileListener();
  options.listeners.emplace_back(listener);
  CreateAndReopenWithCF({"pikachu"}, options);
  std::vector<std::string> kAllCfNames = {kDefaultColumnFamilyName, "pikachu"};
  size_t num_cfs = handles_.size();
  ASSERT_EQ(2, num_cfs);
  for (int cf = 0; cf < static_cast<int>(kAllCfNames.size()); ++cf) {
    ASSERT_OK(Put(cf, "a", "0_value"));
    ASSERT_OK(Flush(cf));
    ASSERT_OK(Put(cf, "b", "0_value"));
  }
  // Delete files
  for (size_t i = 0; i < kAllCfNames.size(); ++i) {
    std::vector<std::string>& files = listener->GetFiles(kAllCfNames[i]);
    ASSERT_EQ(1, files.size());
    for (int j = static_cast<int>(files.size() - 1); j >= static_cast<int>(i);
         --j) {
      ASSERT_OK(env_->DeleteFile(files[j]));
    }
  }
  options.best_efforts_recovery = true;
  ReopenWithColumnFamilies(kAllCfNames, options);
  // Verify WAL is not applied
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts, handles_[0]));
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  iter.reset(db_->NewIterator(read_opts, handles_[1]));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("a", iter->key());
  iter->Next();
  ASSERT_FALSE(iter->Valid());
}
#endif  // !ROCKSDB_LITE

class DBBasicTestMultiGet : public DBTestBase {
 public:
  DBBasicTestMultiGet(std::string test_dir, int num_cfs, bool compressed_cache,
                      bool uncompressed_cache, bool compression_enabled,
                      bool fill_cache, uint32_t compression_parallel_threads)
      : DBTestBase(test_dir) {
    compression_enabled_ = compression_enabled;
    fill_cache_ = fill_cache;

    if (compressed_cache) {
      std::shared_ptr<Cache> cache = NewLRUCache(1048576);
      compressed_cache_ = std::make_shared<MyBlockCache>(cache);
    }
    if (uncompressed_cache) {
      std::shared_ptr<Cache> cache = NewLRUCache(1048576);
      uncompressed_cache_ = std::make_shared<MyBlockCache>(cache);
    }

    env_->count_random_reads_ = true;

    Options options = CurrentOptions();
    Random rnd(301);
    BlockBasedTableOptions table_options;

#ifndef ROCKSDB_LITE
    if (compression_enabled_) {
      std::vector<CompressionType> compression_types;
      compression_types = GetSupportedCompressions();
      // Not every platform may have compression libraries available, so
      // dynamically pick based on what's available
      CompressionType tmp_type = kNoCompression;
      for (auto c_type : compression_types) {
        if (c_type != kNoCompression) {
          tmp_type = c_type;
          break;
        }
      }
      if (tmp_type != kNoCompression) {
        options.compression = tmp_type;
      } else {
        compression_enabled_ = false;
      }
    }
#else
    // GetSupportedCompressions() is not available in LITE build
    if (!Snappy_Supported()) {
      compression_enabled_ = false;
    }
#endif  // ROCKSDB_LITE

    table_options.block_cache = uncompressed_cache_;
    if (table_options.block_cache == nullptr) {
      table_options.no_block_cache = true;
    } else {
      table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    }
    table_options.block_cache_compressed = compressed_cache_;
    table_options.flush_block_policy_factory.reset(
        new MyFlushBlockPolicyFactory());
    options.table_factory.reset(new BlockBasedTableFactory(table_options));
    if (!compression_enabled_) {
      options.compression = kNoCompression;
    } else {
      options.compression_opts.parallel_threads = compression_parallel_threads;
    }
    Reopen(options);

    if (num_cfs > 1) {
      for (int cf = 0; cf < num_cfs; ++cf) {
        cf_names_.emplace_back("cf" + std::to_string(cf));
      }
      CreateColumnFamilies(cf_names_, options);
      cf_names_.emplace_back("default");
    }

    std::string zero_str(128, '\0');
    for (int cf = 0; cf < num_cfs; ++cf) {
      for (int i = 0; i < 100; ++i) {
        // Make the value compressible. A purely random string doesn't compress
        // and the resultant data block will not be compressed
        values_.emplace_back(RandomString(&rnd, 128) + zero_str);
        assert(((num_cfs == 1) ? Put(Key(i), values_[i])
                               : Put(cf, Key(i), values_[i])) == Status::OK());
      }
      if (num_cfs == 1) {
        Flush();
      } else {
        dbfull()->Flush(FlushOptions(), handles_[cf]);
      }

      for (int i = 0; i < 100; ++i) {
        // block cannot gain space by compression
        uncompressable_values_.emplace_back(RandomString(&rnd, 256) + '\0');
        std::string tmp_key = "a" + Key(i);
        assert(((num_cfs == 1) ? Put(tmp_key, uncompressable_values_[i])
                               : Put(cf, tmp_key, uncompressable_values_[i])) ==
               Status::OK());
      }
      if (num_cfs == 1) {
        Flush();
      } else {
        dbfull()->Flush(FlushOptions(), handles_[cf]);
      }
    }
  }

  bool CheckValue(int i, const std::string& value) {
    if (values_[i].compare(value) == 0) {
      return true;
    }
    return false;
  }

  bool CheckUncompressableValue(int i, const std::string& value) {
    if (uncompressable_values_[i].compare(value) == 0) {
      return true;
    }
    return false;
  }

  const std::vector<std::string>& GetCFNames() const { return cf_names_; }

  int num_lookups() { return uncompressed_cache_->num_lookups(); }
  int num_found() { return uncompressed_cache_->num_found(); }
  int num_inserts() { return uncompressed_cache_->num_inserts(); }

  int num_lookups_compressed() { return compressed_cache_->num_lookups(); }
  int num_found_compressed() { return compressed_cache_->num_found(); }
  int num_inserts_compressed() { return compressed_cache_->num_inserts(); }

  bool fill_cache() { return fill_cache_; }
  bool compression_enabled() { return compression_enabled_; }
  bool has_compressed_cache() { return compressed_cache_ != nullptr; }
  bool has_uncompressed_cache() { return uncompressed_cache_ != nullptr; }

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

 protected:
  class MyFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
   public:
    MyFlushBlockPolicyFactory() {}

    virtual const char* Name() const override {
      return "MyFlushBlockPolicyFactory";
    }

    virtual FlushBlockPolicy* NewFlushBlockPolicy(
        const BlockBasedTableOptions& /*table_options*/,
        const BlockBuilder& data_block_builder) const override {
      return new MyFlushBlockPolicy(data_block_builder);
    }
  };

  class MyFlushBlockPolicy : public FlushBlockPolicy {
   public:
    explicit MyFlushBlockPolicy(const BlockBuilder& data_block_builder)
        : num_keys_(0), data_block_builder_(data_block_builder) {}

    bool Update(const Slice& /*key*/, const Slice& /*value*/) override {
      if (data_block_builder_.empty()) {
        // First key in this block
        num_keys_ = 1;
        return false;
      }
      // Flush every 10 keys
      if (num_keys_ == 10) {
        num_keys_ = 1;
        return true;
      }
      num_keys_++;
      return false;
    }

   private:
    int num_keys_;
    const BlockBuilder& data_block_builder_;
  };

  class MyBlockCache : public CacheWrapper {
   public:
    explicit MyBlockCache(std::shared_ptr<Cache> target)
        : CacheWrapper(target),
          num_lookups_(0),
          num_found_(0),
          num_inserts_(0) {}

    const char* Name() const override { return "MyBlockCache"; }

    Status Insert(const Slice& key, void* value, size_t charge,
                  void (*deleter)(const Slice& key, void* value),
                  Handle** handle = nullptr,
                  Priority priority = Priority::LOW) override {
      num_inserts_++;
      return target_->Insert(key, value, charge, deleter, handle, priority);
    }

    Handle* Lookup(const Slice& key, Statistics* stats = nullptr) override {
      num_lookups_++;
      Handle* handle = target_->Lookup(key, stats);
      if (handle != nullptr) {
        num_found_++;
      }
      return handle;
    }
    int num_lookups() { return num_lookups_; }

    int num_found() { return num_found_; }

    int num_inserts() { return num_inserts_; }

   private:
    int num_lookups_;
    int num_found_;
    int num_inserts_;
  };

  std::shared_ptr<MyBlockCache> compressed_cache_;
  std::shared_ptr<MyBlockCache> uncompressed_cache_;
  bool compression_enabled_;
  std::vector<std::string> values_;
  std::vector<std::string> uncompressable_values_;
  bool fill_cache_;
  std::vector<std::string> cf_names_;
};

class DBBasicTestWithParallelIO
    : public DBBasicTestMultiGet,
      public testing::WithParamInterface<
          std::tuple<bool, bool, bool, bool, uint32_t>> {
 public:
  DBBasicTestWithParallelIO()
      : DBBasicTestMultiGet("/db_basic_test_with_parallel_io", 1,
                            std::get<0>(GetParam()), std::get<1>(GetParam()),
                            std::get<2>(GetParam()), std::get<3>(GetParam()),
                            std::get<4>(GetParam())) {}
};

TEST_P(DBBasicTestWithParallelIO, MultiGet) {
  std::vector<std::string> key_data(10);
  std::vector<Slice> keys;
  // We cannot resize a PinnableSlice vector, so just set initial size to
  // largest we think we will need
  std::vector<PinnableSlice> values(10);
  std::vector<Status> statuses;
  ReadOptions ro;
  ro.fill_cache = fill_cache();

  // Warm up the cache first
  key_data.emplace_back(Key(0));
  keys.emplace_back(Slice(key_data.back()));
  key_data.emplace_back(Key(50));
  keys.emplace_back(Slice(key_data.back()));
  statuses.resize(keys.size());

  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  ASSERT_TRUE(CheckValue(0, values[0].ToString()));
  ASSERT_TRUE(CheckValue(50, values[1].ToString()));

  int random_reads = env_->random_read_counter_.Read();
  key_data[0] = Key(1);
  key_data[1] = Key(51);
  keys[0] = Slice(key_data[0]);
  keys[1] = Slice(key_data[1]);
  values[0].Reset();
  values[1].Reset();
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  ASSERT_TRUE(CheckValue(1, values[0].ToString()));
  ASSERT_TRUE(CheckValue(51, values[1].ToString()));

  bool read_from_cache = false;
  if (fill_cache()) {
    if (has_uncompressed_cache()) {
      read_from_cache = true;
    } else if (has_compressed_cache() && compression_enabled()) {
      read_from_cache = true;
    }
  }

  int expected_reads = random_reads + (read_from_cache ? 0 : 2);
  ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);

  keys.resize(10);
  statuses.resize(10);
  std::vector<int> key_ints{1, 2, 15, 16, 55, 81, 82, 83, 84, 85};
  for (size_t i = 0; i < key_ints.size(); ++i) {
    key_data[i] = Key(key_ints[i]);
    keys[i] = Slice(key_data[i]);
    statuses[i] = Status::OK();
    values[i].Reset();
  }
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  for (size_t i = 0; i < key_ints.size(); ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_TRUE(CheckValue(key_ints[i], values[i].ToString()));
  }
  if (compression_enabled() && !has_compressed_cache()) {
    expected_reads += (read_from_cache ? 2 : 3);
  } else {
    expected_reads += (read_from_cache ? 2 : 4);
  }
  ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);

  keys.resize(10);
  statuses.resize(10);
  std::vector<int> key_uncmp{1, 2, 15, 16, 55, 81, 82, 83, 84, 85};
  for (size_t i = 0; i < key_uncmp.size(); ++i) {
    key_data[i] = "a" + Key(key_uncmp[i]);
    keys[i] = Slice(key_data[i]);
    statuses[i] = Status::OK();
    values[i].Reset();
  }
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  for (size_t i = 0; i < key_uncmp.size(); ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_TRUE(CheckUncompressableValue(key_uncmp[i], values[i].ToString()));
  }
  if (compression_enabled() && !has_compressed_cache()) {
    expected_reads += (read_from_cache ? 3 : 3);
  } else {
    expected_reads += (read_from_cache ? 4 : 4);
  }
  ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);

  keys.resize(5);
  statuses.resize(5);
  std::vector<int> key_tr{1, 2, 15, 16, 55};
  for (size_t i = 0; i < key_tr.size(); ++i) {
    key_data[i] = "a" + Key(key_tr[i]);
    keys[i] = Slice(key_data[i]);
    statuses[i] = Status::OK();
    values[i].Reset();
  }
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  for (size_t i = 0; i < key_tr.size(); ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_TRUE(CheckUncompressableValue(key_tr[i], values[i].ToString()));
  }
  if (compression_enabled() && !has_compressed_cache()) {
    expected_reads += (read_from_cache ? 0 : 2);
    ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);
  } else {
    if (has_uncompressed_cache()) {
      expected_reads += (read_from_cache ? 0 : 3);
      ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);
    } else {
      // A rare case, even we enable the block compression but some of data
      // blocks are not compressed due to content. If user only enable the
      // compressed cache, the uncompressed blocks will not tbe cached, and
      // block reads will be triggered. The number of reads is related to
      // the compression algorithm.
      ASSERT_TRUE(env_->random_read_counter_.Read() >= expected_reads);
    }
  }
}

TEST_P(DBBasicTestWithParallelIO, MultiGetWithChecksumMismatch) {
  std::vector<std::string> key_data(10);
  std::vector<Slice> keys;
  // We cannot resize a PinnableSlice vector, so just set initial size to
  // largest we think we will need
  std::vector<PinnableSlice> values(10);
  std::vector<Status> statuses;
  int read_count = 0;
  ReadOptions ro;
  ro.fill_cache = fill_cache();

  SyncPoint::GetInstance()->SetCallBack(
      "RetrieveMultipleBlocks:VerifyChecksum", [&](void* status) {
        Status* s = static_cast<Status*>(status);
        read_count++;
        if (read_count == 2) {
          *s = Status::Corruption();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Warm up the cache first
  key_data.emplace_back(Key(0));
  keys.emplace_back(Slice(key_data.back()));
  key_data.emplace_back(Key(50));
  keys.emplace_back(Slice(key_data.back()));
  statuses.resize(keys.size());

  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  ASSERT_TRUE(CheckValue(0, values[0].ToString()));
  // ASSERT_TRUE(CheckValue(50, values[1].ToString()));
  ASSERT_EQ(statuses[0], Status::OK());
  ASSERT_EQ(statuses[1], Status::Corruption());

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBBasicTestWithParallelIO, MultiGetWithMissingFile) {
  std::vector<std::string> key_data(10);
  std::vector<Slice> keys;
  // We cannot resize a PinnableSlice vector, so just set initial size to
  // largest we think we will need
  std::vector<PinnableSlice> values(10);
  std::vector<Status> statuses;
  ReadOptions ro;
  ro.fill_cache = fill_cache();

  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::MultiGet:FindTable", [&](void* status) {
        Status* s = static_cast<Status*>(status);
        *s = Status::IOError();
      });
  // DB open will create table readers unless we reduce the table cache
  // capacity.
  // SanitizeOptions will set max_open_files to minimum of 20. Table cache
  // is allocated with max_open_files - 10 as capacity. So override
  // max_open_files to 11 so table cache capacity will become 1. This will
  // prevent file open during DB open and force the file to be opened
  // during MultiGet
  SyncPoint::GetInstance()->SetCallBack(
      "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
        int* max_open_files = (int*)arg;
        *max_open_files = 11;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Reopen(CurrentOptions());

  // Warm up the cache first
  key_data.emplace_back(Key(0));
  keys.emplace_back(Slice(key_data.back()));
  key_data.emplace_back(Key(50));
  keys.emplace_back(Slice(key_data.back()));
  statuses.resize(keys.size());

  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  ASSERT_EQ(statuses[0], Status::IOError());
  ASSERT_EQ(statuses[1], Status::IOError());

  SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(ParallelIO, DBBasicTestWithParallelIO,
                        // Params are as follows -
                        // Param 0 - Compressed cache enabled
                        // Param 1 - Uncompressed cache enabled
                        // Param 2 - Data compression enabled
                        // Param 3 - ReadOptions::fill_cache
                        // Param 4 - CompressionOptions::parallel_threads
                        ::testing::Combine(::testing::Bool(), ::testing::Bool(),
                                           ::testing::Bool(), ::testing::Bool(),
                                           ::testing::Values(1, 4)));

// A test class for intercepting random reads and injecting artificial
// delays. Used for testing the deadline/timeout feature
class DBBasicTestMultiGetDeadline : public DBBasicTestMultiGet {
 public:
  DBBasicTestMultiGetDeadline()
      : DBBasicTestMultiGet("db_basic_test_multiget_deadline" /*Test dir*/,
                             10 /*# of column families*/,
                             false /*compressed cache enabled*/,
                             true /*uncompressed cache enabled*/,
                             true /*compression enabled*/,
                             true /*ReadOptions.fill_cache*/,
                             1 /*# of parallel compression threads*/) {}

  // Forward declaration
  class DeadlineFS;

  class DeadlineRandomAccessFile : public FSRandomAccessFileWrapper {
   public:
    DeadlineRandomAccessFile(DeadlineFS& fs, SpecialEnv* env,
                             std::unique_ptr<FSRandomAccessFile>& file)
        : FSRandomAccessFileWrapper(file.get()),
          fs_(fs),
          file_(std::move(file)),
          env_(env) {}

    IOStatus Read(uint64_t offset, size_t len, const IOOptions& opts,
          Slice* result, char* scratch, IODebugContext* dbg) const override {
      int delay;
      const std::chrono::microseconds deadline = fs_.GetDeadline();
      if (deadline.count()) {
        AssertDeadline(deadline, opts);
      }
      if (fs_.ShouldDelay(&delay)) {
        env_->SleepForMicroseconds(delay);
      }
      return FSRandomAccessFileWrapper::Read(offset, len, opts, result, scratch,
                                             dbg);
    }

    IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
          const IOOptions& options, IODebugContext* dbg) override {
      int delay;
      const std::chrono::microseconds deadline = fs_.GetDeadline();
      if (deadline.count()) {
        AssertDeadline(deadline, options);
      }
      if (fs_.ShouldDelay(&delay)) {
        env_->SleepForMicroseconds(delay);
      }
      return FSRandomAccessFileWrapper::MultiRead(reqs, num_reqs, options, dbg);
    }

   private:
    void AssertDeadline(const std::chrono::microseconds deadline,
                        const IOOptions& opts) const {
      // Give a leeway of +- 10us as it can take some time for the Get/
      // MultiGet call to reach here, in order to avoid false alarms
      std::chrono::microseconds now =
          std::chrono::microseconds(env_->NowMicros());
      ASSERT_EQ(deadline - now, opts.timeout);
    }
    DeadlineFS& fs_;
    std::unique_ptr<FSRandomAccessFile> file_;
    SpecialEnv* env_;
  };

  class DeadlineFS : public FileSystemWrapper {
   public:
    DeadlineFS(SpecialEnv* env)
        : FileSystemWrapper(FileSystem::Default()),
          delay_idx_(0),
          deadline_(std::chrono::microseconds::zero()),
          env_(env) {}
    ~DeadlineFS() = default;

    IOStatus NewRandomAccessFile(const std::string& fname,
                                 const FileOptions& opts,
                                 std::unique_ptr<FSRandomAccessFile>* result,
                                 IODebugContext* dbg) override {
      std::unique_ptr<FSRandomAccessFile> file;
      IOStatus s;

      s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
      result->reset(new DeadlineRandomAccessFile(*this, env_, file));
      return s;
    }

    // Set a vector of {IO counter, delay in microseconds} pairs that control
    // when to inject a delay and duration of the delay
    void SetDelaySequence(const std::chrono::microseconds deadline,
                          const std::vector<std::pair<int, int>>&& seq) {
      int total_delay = 0;
      for (auto& seq_iter : seq) {
        // Ensure no individual delay is > 500ms
        ASSERT_LT(seq_iter.second, 500000);
        total_delay += seq_iter.second;
      }
      // ASSERT total delay is < 1s. This is mainly to keep the test from
      // timing out in CI test frameworks
      ASSERT_LT(total_delay, 1000000);
      delay_seq_ = seq;
      delay_idx_ = 0;
      io_count_ = 0;
      deadline_ = deadline;
    }

    // Increment the IO counter and return a delay in microseconds
    bool ShouldDelay(int* delay) {
      if (delay_idx_ < delay_seq_.size() &&
          delay_seq_[delay_idx_].first == io_count_++) {
        *delay = delay_seq_[delay_idx_].second;
        delay_idx_++;
        return true;
      }
      return false;
    }

    const std::chrono::microseconds GetDeadline() { return deadline_; }

   private:
    std::vector<std::pair<int, int>> delay_seq_;
    size_t delay_idx_;
    int io_count_;
    std::chrono::microseconds deadline_;
    SpecialEnv* env_;
  };

  inline void CheckStatus(std::vector<Status>& statuses, size_t num_ok) {
    for (size_t i = 0; i < statuses.size(); ++i) {
      if (i < num_ok) {
        EXPECT_OK(statuses[i]);
      } else {
        EXPECT_EQ(statuses[i], Status::TimedOut());
      }
    }
  }
};

TEST_F(DBBasicTestMultiGetDeadline, MultiGetDeadlineExceeded) {
  std::shared_ptr<DBBasicTestMultiGetDeadline::DeadlineFS> fs(
      new DBBasicTestMultiGetDeadline::DeadlineFS(env_));
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options = CurrentOptions();
  env_->SetTimeElapseOnlySleep(&options);

  std::shared_ptr<Cache> cache = NewLRUCache(1048576);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  options.env = env.get();
  ReopenWithColumnFamilies(GetCFNames(), options);

  // Test the non-batched version of MultiGet with multiple column
  // families
  std::vector<std::string> key_str;
  size_t i;
  for (i = 0; i < 5; ++i) {
    key_str.emplace_back(Key(static_cast<int>(i)));
  }
  std::vector<ColumnFamilyHandle*> cfs(key_str.size());
  ;
  std::vector<Slice> keys(key_str.size());
  std::vector<std::string> values(key_str.size());
  for (i = 0; i < key_str.size(); ++i) {
    cfs[i] = handles_[i];
    keys[i] = Slice(key_str[i].data(), key_str[i].size());
  }

  ReadOptions ro;
  ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
  // Delay the first IO by 200ms
  fs->SetDelaySequence(ro.deadline, {{0, 20000}});

  std::vector<Status> statuses = dbfull()->MultiGet(ro, cfs, keys, &values);
  // The first key is successful because we check after the lookup, but
  // subsequent keys fail due to deadline exceeded
  CheckStatus(statuses, 1);

  // Clear the cache
  cache->SetCapacity(0);
  cache->SetCapacity(1048576);
  // Test non-batched Multiget with multiple column families and
  // introducing an IO delay in one of the middle CFs
  key_str.clear();
  for (i = 0; i < 10; ++i) {
    key_str.emplace_back(Key(static_cast<int>(i)));
  }
  cfs.resize(key_str.size());
  keys.resize(key_str.size());
  values.resize(key_str.size());
  for (i = 0; i < key_str.size(); ++i) {
    // 2 keys per CF
    cfs[i] = handles_[i / 2];
    keys[i] = Slice(key_str[i].data(), key_str[i].size());
  }
  ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
  fs->SetDelaySequence(ro.deadline, {{1, 20000}});
  statuses = dbfull()->MultiGet(ro, cfs, keys, &values);
  CheckStatus(statuses, 3);

  // Test batched MultiGet with an IO delay in the first data block read.
  // Both keys in the first CF should succeed as they're in the same data
  // block and would form one batch, and we check for deadline between
  // batches.
  std::vector<PinnableSlice> pin_values(keys.size());
  cache->SetCapacity(0);
  cache->SetCapacity(1048576);
  statuses.clear();
  statuses.resize(keys.size());
  ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
  fs->SetDelaySequence(ro.deadline, {{0, 20000}});
  dbfull()->MultiGet(ro, keys.size(), cfs.data(), keys.data(),
                     pin_values.data(), statuses.data());
  CheckStatus(statuses, 2);

  // Similar to the previous one, but an IO delay in the third CF data block
  // read
  for (PinnableSlice& value : pin_values) {
    value.Reset();
  }
  cache->SetCapacity(0);
  cache->SetCapacity(1048576);
  statuses.clear();
  statuses.resize(keys.size());
  ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
  fs->SetDelaySequence(ro.deadline, {{2, 20000}});
  dbfull()->MultiGet(ro, keys.size(), cfs.data(), keys.data(),
                     pin_values.data(), statuses.data());
  CheckStatus(statuses, 6);

  // Similar to the previous one, but an IO delay in the last but one CF
  for (PinnableSlice& value : pin_values) {
    value.Reset();
  }
  cache->SetCapacity(0);
  cache->SetCapacity(1048576);
  statuses.clear();
  statuses.resize(keys.size());
  ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
  fs->SetDelaySequence(ro.deadline, {{3, 20000}});
  dbfull()->MultiGet(ro, keys.size(), cfs.data(), keys.data(),
                     pin_values.data(), statuses.data());
  CheckStatus(statuses, 8);

  // Test batched MultiGet with single CF and lots of keys. Inject delay
  // into the second batch of keys. As each batch is 32, the first 64 keys,
  // i.e first two batches, should succeed and the rest should time out
  for (PinnableSlice& value : pin_values) {
    value.Reset();
  }
  cache->SetCapacity(0);
  cache->SetCapacity(1048576);
  key_str.clear();
  for (i = 0; i < 100; ++i) {
    key_str.emplace_back(Key(static_cast<int>(i)));
  }
  keys.resize(key_str.size());
  pin_values.clear();
  pin_values.resize(key_str.size());
  for (i = 0; i < key_str.size(); ++i) {
    keys[i] = Slice(key_str[i].data(), key_str[i].size());
  }
  statuses.clear();
  statuses.resize(keys.size());
  ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
  fs->SetDelaySequence(ro.deadline, {{1, 20000}});
  dbfull()->MultiGet(ro, handles_[0], keys.size(), keys.data(),
                     pin_values.data(), statuses.data());
  CheckStatus(statuses, 64);
  Close();
}

}  // namespace ROCKSDB_NAMESPACE

#ifdef ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS
extern "C" {
void RegisterCustomObjects(int argc, char** argv);
}
#else
void RegisterCustomObjects(int /*argc*/, char** /*argv*/) {}
#endif  // !ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
