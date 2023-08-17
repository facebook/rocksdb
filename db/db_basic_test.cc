//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstring>

#include "db/db_test_util.h"
#include "options/options_helper.h"
#include "port/stack_trace.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/debug.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#if !defined(ROCKSDB_LITE)
#include "test_util/sync_point.h"
#endif
#include "util/file_checksum_helper.h"
#include "util/random.h"
#include "utilities/counted_fs.h"
#include "utilities/fault_injection_env.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend.h"

namespace ROCKSDB_NAMESPACE {

static bool enable_io_uring = true;
extern "C" bool RocksDbIOUringEnable() { return enable_io_uring; }

class DBBasicTest : public DBTestBase {
 public:
  DBBasicTest() : DBTestBase("db_basic_test", /*env_do_fsync=*/false) {}
};

TEST_F(DBBasicTest, OpenWhenOpen) {
  Options options = CurrentOptions();
  options.env = env_;
  DB* db2 = nullptr;
  Status s = DB::Open(options, dbname_, &db2);
  ASSERT_NOK(s) << [db2]() {
    delete db2;
    return "db2 open: ok";
  }();
  ASSERT_EQ(Status::Code::kIOError, s.code());
  ASSERT_EQ(Status::SubCode::kNone, s.subcode());
  ASSERT_TRUE(strstr(s.getState(), "lock ") != nullptr);

  delete db2;
}

TEST_F(DBBasicTest, EnableDirectIOWithZeroBuf) {
  if (!IsDirectIOSupported()) {
    ROCKSDB_GTEST_BYPASS("Direct IO not supported");
    return;
  }
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.use_direct_io_for_flush_and_compaction = true;
  options.writable_file_max_buffer_size = 0;
  ASSERT_TRUE(TryReopen(options).IsInvalidArgument());

  options.writable_file_max_buffer_size = 1024;
  Reopen(options);
  const std::unordered_map<std::string, std::string> new_db_opts = {
      {"writable_file_max_buffer_size", "0"}};
  ASSERT_TRUE(db_->SetDBOptions(new_db_opts).IsInvalidArgument());
}

TEST_F(DBBasicTest, UniqueSession) {
  Options options = CurrentOptions();
  std::string sid1, sid2, sid3, sid4;

  ASSERT_OK(db_->GetDbSessionId(sid1));
  Reopen(options);
  ASSERT_OK(db_->GetDbSessionId(sid2));
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(db_->GetDbSessionId(sid4));
  Reopen(options);
  ASSERT_OK(db_->GetDbSessionId(sid3));

  ASSERT_NE(sid1, sid2);
  ASSERT_NE(sid1, sid3);
  ASSERT_NE(sid2, sid3);

  ASSERT_EQ(sid2, sid4);

  // Expected compact format for session ids (see notes in implementation)
  TestRegex expected("[0-9A-Z]{20}");
  EXPECT_MATCHES_REGEX(sid1, expected);
  EXPECT_MATCHES_REGEX(sid2, expected);
  EXPECT_MATCHES_REGEX(sid3, expected);

#ifndef ROCKSDB_LITE
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_OK(db_->GetDbSessionId(sid1));
  // Test uniqueness between readonly open (sid1) and regular open (sid3)
  ASSERT_NE(sid1, sid3);
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_OK(db_->GetDbSessionId(sid2));
  ASSERT_EQ("v1", Get("foo"));
  ASSERT_OK(db_->GetDbSessionId(sid3));

  ASSERT_NE(sid1, sid2);

  ASSERT_EQ(sid2, sid3);
#endif  // ROCKSDB_LITE

  CreateAndReopenWithCF({"goku"}, options);
  ASSERT_OK(db_->GetDbSessionId(sid1));
  ASSERT_OK(Put("bar", "e1"));
  ASSERT_OK(db_->GetDbSessionId(sid2));
  ASSERT_EQ("e1", Get("bar"));
  ASSERT_OK(db_->GetDbSessionId(sid3));
  ReopenWithColumnFamilies({"default", "goku"}, options);
  ASSERT_OK(db_->GetDbSessionId(sid4));

  ASSERT_EQ(sid1, sid2);
  ASSERT_EQ(sid2, sid3);

  ASSERT_NE(sid1, sid4);
}

#ifndef ROCKSDB_LITE
TEST_F(DBBasicTest, ReadOnlyDB) {
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Put("foo", "v3"));
  Close();

  auto verify_one_iter = [&](Iterator* iter) {
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      ++count;
    }
    // Always expect two keys: "foo" and "bar"
    ASSERT_EQ(count, 2);
  };

  auto verify_all_iters = [&]() {
    Iterator* iter = db_->NewIterator(ReadOptions());
    verify_one_iter(iter);
    delete iter;

    std::vector<Iterator*> iters;
    ASSERT_OK(db_->NewIterators(ReadOptions(),
                                {dbfull()->DefaultColumnFamily()}, &iters));
    ASSERT_EQ(static_cast<uint64_t>(1), iters.size());
    verify_one_iter(iters[0]);
    delete iters[0];
  };

  auto options = CurrentOptions();
  assert(options.env == env_);
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  verify_all_iters();
  Close();

  // Reopen and flush memtable.
  Reopen(options);
  ASSERT_OK(Flush());
  Close();
  // Now check keys in read only mode.
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  verify_all_iters();
  ASSERT_TRUE(db_->SyncWAL().IsNotSupported());
}

// TODO akanksha: Update the test to check that combination
// does not actually write to FS (use open read-only with
// CompositeEnvWrapper+ReadOnlyFileSystem).
TEST_F(DBBasicTest, DISABLED_ReadOnlyDBWithWriteDBIdToManifestSet) {
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Put("foo", "v3"));
  Close();

  auto options = CurrentOptions();
  options.write_dbid_to_manifest = true;
  assert(options.env == env_);
  ASSERT_OK(ReadOnlyReopen(options));
  std::string db_id1;
  ASSERT_OK(db_->GetDbIdentity(db_id1));
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
  ASSERT_OK(Flush());
  Close();
  // Now check keys in read only mode.
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  ASSERT_TRUE(db_->SyncWAL().IsNotSupported());
  std::string db_id2;
  ASSERT_OK(db_->GetDbIdentity(db_id2));
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
  ASSERT_OK(Flush());
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
  ASSERT_OK(Flush());
  ASSERT_OK(Put("aaa", DummyString(kFileSize / 2, 'a')));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("bbb", DummyString(kFileSize / 2, 'b')));
  ASSERT_OK(Put("eee", DummyString(kFileSize / 2, 'e')));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("something_not_flushed", "x"));
  Close();

  ASSERT_OK(ReadOnlyReopen(options));
  // Fallback to read-only DB
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");

  // TODO: validate that other write ops return NotImplemented
  // (DBImplReadOnly is missing some overrides)

  // Ensure no deadlock on flush triggered by another API function
  // (Old deadlock bug depends on something_not_flushed above.)
  std::vector<std::string> files;
  uint64_t manifest_file_size;
  ASSERT_OK(db_->GetLiveFiles(files, &manifest_file_size, /*flush*/ true));
  LiveFilesStorageInfoOptions lfsi_opts;
  lfsi_opts.wal_size_for_flush = 0;  // always
  std::vector<LiveFileStorageInfo> files2;
  ASSERT_OK(db_->GetLiveFilesStorageInfo(lfsi_opts, &files2));

  Close();

  // Full compaction
  Reopen(options);
  // Add more keys
  ASSERT_OK(Put("fff", DummyString(kFileSize / 2, 'f')));
  ASSERT_OK(Put("hhh", DummyString(kFileSize / 2, 'h')));
  ASSERT_OK(Put("iii", DummyString(kFileSize / 2, 'i')));
  ASSERT_OK(Put("jjj", DummyString(kFileSize / 2, 'j')));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
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

  // TODO: validate that other write ops return NotImplemented
  // (CompactedDB is missing some overrides)

  // Ensure no deadlock on flush triggered by another API function
  ASSERT_OK(db_->GetLiveFiles(files, &manifest_file_size, /*flush*/ true));
  ASSERT_OK(db_->GetLiveFilesStorageInfo(lfsi_opts, &files2));

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
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
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

    ASSERT_OK(Put(1, "a", Slice()));
    ASSERT_OK(SingleDelete(1, "a"));
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
    DB* localdb = nullptr;
    Options options = CurrentOptions();
    ASSERT_OK(TryReopen(options));

    // second open should fail
    Status s = DB::Open(options, dbname_, &localdb);
    ASSERT_NOK(s) << [localdb]() {
      delete localdb;
      return "localdb open: ok";
    }();
#ifdef OS_LINUX
    ASSERT_TRUE(s.ToString().find("lock ") != std::string::npos);
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

TEST_F(DBBasicTest, IdentityAcrossRestarts) {
  constexpr size_t kMinIdSize = 10;
  do {
    for (bool with_manifest : {false, true}) {
      std::string idfilename = IdentityFileName(dbname_);
      std::string id1, tmp;
      ASSERT_OK(db_->GetDbIdentity(id1));
      ASSERT_GE(id1.size(), kMinIdSize);

      Options options = CurrentOptions();
      options.write_dbid_to_manifest = with_manifest;
      Reopen(options);
      std::string id2;
      ASSERT_OK(db_->GetDbIdentity(id2));
      // id2 should match id1 because identity was not regenerated
      ASSERT_EQ(id1, id2);
      ASSERT_OK(ReadFileToString(env_, idfilename, &tmp));
      ASSERT_EQ(tmp, id2);

      // Recover from deleted/missing IDENTITY
      ASSERT_OK(env_->DeleteFile(idfilename));
      Reopen(options);
      std::string id3;
      ASSERT_OK(db_->GetDbIdentity(id3));
      if (with_manifest) {
        // id3 should match id1 because identity was restored from manifest
        ASSERT_EQ(id1, id3);
      } else {
        // id3 should NOT match id1 because identity was regenerated
        ASSERT_NE(id1, id3);
        ASSERT_GE(id3.size(), kMinIdSize);
      }
      ASSERT_OK(ReadFileToString(env_, idfilename, &tmp));
      ASSERT_EQ(tmp, id3);

      // Recover from truncated IDENTITY
      {
        std::unique_ptr<WritableFile> w;
        ASSERT_OK(env_->NewWritableFile(idfilename, &w, EnvOptions()));
        ASSERT_OK(w->Close());
      }
      Reopen(options);
      std::string id4;
      ASSERT_OK(db_->GetDbIdentity(id4));
      if (with_manifest) {
        // id4 should match id1 because identity was restored from manifest
        ASSERT_EQ(id1, id4);
      } else {
        // id4 should NOT match id1 because identity was regenerated
        ASSERT_NE(id1, id4);
        ASSERT_GE(id4.size(), kMinIdSize);
      }
      ASSERT_OK(ReadFileToString(env_, idfilename, &tmp));
      ASSERT_EQ(tmp, id4);

      // Recover from overwritten IDENTITY
      std::string silly_id = "asdf123456789";
      {
        std::unique_ptr<WritableFile> w;
        ASSERT_OK(env_->NewWritableFile(idfilename, &w, EnvOptions()));
        ASSERT_OK(w->Append(silly_id));
        ASSERT_OK(w->Close());
      }
      Reopen(options);
      std::string id5;
      ASSERT_OK(db_->GetDbIdentity(id5));
      if (with_manifest) {
        // id4 should match id1 because identity was restored from manifest
        ASSERT_EQ(id1, id5);
      } else {
        ASSERT_EQ(id5, silly_id);
      }
      ASSERT_OK(ReadFileToString(env_, idfilename, &tmp));
      ASSERT_EQ(tmp, id5);
    }
  } while (ChangeCompactOptions());
}

#ifndef ROCKSDB_LITE
TEST_F(DBBasicTest, Snapshot) {
  env_->SetMockSleep();
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    ASSERT_OK(Put(0, "foo", "0v1"));
    ASSERT_OK(Put(1, "foo", "1v1"));

    const Snapshot* s1 = db_->GetSnapshot();
    ASSERT_EQ(1U, GetNumSnapshots());
    uint64_t time_snap1 = GetTimeOldestSnapshots();
    ASSERT_GT(time_snap1, 0U);
    ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());
    ASSERT_OK(Put(0, "foo", "0v2"));
    ASSERT_OK(Put(1, "foo", "1v2"));

    env_->MockSleepForSeconds(1);

    const Snapshot* s2 = db_->GetSnapshot();
    ASSERT_EQ(2U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());
    ASSERT_OK(Put(0, "foo", "0v3"));
    ASSERT_OK(Put(1, "foo", "1v3"));

    {
      ManagedSnapshot s3(db_);
      ASSERT_EQ(3U, GetNumSnapshots());
      ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
      ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());

      ASSERT_OK(Put(0, "foo", "0v4"));
      ASSERT_OK(Put(1, "foo", "1v4"));
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

class DBBasicMultiConfigs : public DBBasicTest,
                            public ::testing::WithParamInterface<int> {
 public:
  DBBasicMultiConfigs() { option_config_ = GetParam(); }

  static std::vector<int> GenerateOptionConfigs() {
    std::vector<int> option_configs;
    for (int option_config = kDefault; option_config < kEnd; ++option_config) {
      if (!ShouldSkipOptions(option_config, kSkipFIFOCompaction)) {
        option_configs.push_back(option_config);
      }
    }
    return option_configs;
  }
};

TEST_P(DBBasicMultiConfigs, CompactBetweenSnapshots) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  Options options = CurrentOptions(options_override);
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  Random rnd(301);
  FillLevels("a", "z", 1);

  ASSERT_OK(Put(1, "foo", "first"));
  const Snapshot* snapshot1 = db_->GetSnapshot();
  ASSERT_OK(Put(1, "foo", "second"));
  ASSERT_OK(Put(1, "foo", "third"));
  ASSERT_OK(Put(1, "foo", "fourth"));
  const Snapshot* snapshot2 = db_->GetSnapshot();
  ASSERT_OK(Put(1, "foo", "fifth"));
  ASSERT_OK(Put(1, "foo", "sixth"));

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
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                   nullptr));

  // We have only one valid snapshot snapshot2. Since snapshot1 is
  // not valid anymore, "first" should be removed by a compaction.
  ASSERT_EQ("sixth", Get(1, "foo"));
  ASSERT_EQ("fourth", Get(1, "foo", snapshot2));
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth, fourth ]");

  // after we release the snapshot2, only one value should be left
  db_->ReleaseSnapshot(snapshot2);
  FillLevels("a", "z", 1);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                   nullptr));
  ASSERT_EQ("sixth", Get(1, "foo"));
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth ]");
}

INSTANTIATE_TEST_CASE_P(
    DBBasicMultiConfigs, DBBasicMultiConfigs,
    ::testing::ValuesIn(DBBasicMultiConfigs::GenerateOptionConfigs()));

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

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v1 ]");

    // Write two new keys
    ASSERT_OK(Put(1, "a", "begin"));
    ASSERT_OK(Put(1, "z", "end"));
    ASSERT_OK(Flush(1));

    // Case1: Delete followed by a put
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, DEL, v1 ]");

    // After the current memtable is flushed, the DEL should
    // have been removed
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, v1 ]");

    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2 ]");

    // Case 2: Delete followed by another delete
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, DEL, v2 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v2 ]");
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 3: Put followed by a delete
    ASSERT_OK(Put(1, "foo", "v3"));
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v3 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL ]");
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 4: Put followed by another Put
    ASSERT_OK(Put(1, "foo", "v4"));
    ASSERT_OK(Put(1, "foo", "v5"));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5, v4 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5 ]");
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5 ]");

    // clear database
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 5: Put followed by snapshot followed by another Put
    // Both puts should remain.
    ASSERT_OK(Put(1, "foo", "v6"));
    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(Put(1, "foo", "v7"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v7, v6 ]");
    db_->ReleaseSnapshot(snapshot);

    // clear database
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 5: snapshot followed by a put followed by another Put
    // Only the last put should remain.
    const Snapshot* snapshot1 = db_->GetSnapshot();
    ASSERT_OK(Put(1, "foo", "v8"));
    ASSERT_OK(Put(1, "foo", "v9"));
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
    ASSERT_OK(Flush(i));
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

class DBBlockChecksumTest : public DBBasicTest,
                            public testing::WithParamInterface<uint32_t> {};

INSTANTIATE_TEST_CASE_P(FormatVersions, DBBlockChecksumTest,
                        testing::ValuesIn(test::kFooterFormatVersionsToTest));

TEST_P(DBBlockChecksumTest, BlockChecksumTest) {
  BlockBasedTableOptions table_options;
  table_options.format_version = GetParam();
  Options options = CurrentOptions();
  const int kNumPerFile = 2;

  const auto algs = GetSupportedChecksums();
  const int algs_size = static_cast<int>(algs.size());

  // generate one table with each type of checksum
  for (int i = 0; i < algs_size; ++i) {
    table_options.checksum = algs[i];
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    Reopen(options);
    for (int j = 0; j < kNumPerFile; ++j) {
      ASSERT_OK(Put(Key(i * kNumPerFile + j), Key(i * kNumPerFile + j)));
    }
    ASSERT_OK(Flush());
  }

  // with each valid checksum type setting...
  for (int i = 0; i < algs_size; ++i) {
    table_options.checksum = algs[i];
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    Reopen(options);
    // verify every type of checksum (should be regardless of that setting)
    for (int j = 0; j < algs_size * kNumPerFile; ++j) {
      ASSERT_EQ(Key(j), Get(Key(j)));
    }
  }

  // Now test invalid checksum type
  table_options.checksum = static_cast<ChecksumType>(123);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_TRUE(TryReopen(options).IsInvalidArgument());
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
  static const char* kClassName() { return "TestEnv"; }
  const char* Name() const override { return kClassName(); }

  class TestLogger : public Logger {
   public:
    using Logger::Logv;
    explicit TestLogger(TestEnv* env_ptr) : Logger() { env = env_ptr; }
    ~TestLogger() override {
      if (!closed_) {
        CloseHelper().PermitUncheckedError();
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

TEST_F(DBBasicTest, DBCloseAllDirectoryFDs) {
  Options options = GetDefaultOptions();
  std::string dbname = test::PerThreadDBPath("db_close_all_dir_fds_test");
  // Configure a specific WAL directory
  options.wal_dir = dbname + "_wal_dir";
  // Configure 3 different data directories
  options.db_paths.emplace_back(dbname + "_1", 512 * 1024);
  options.db_paths.emplace_back(dbname + "_2", 4 * 1024 * 1024);
  options.db_paths.emplace_back(dbname + "_3", 1024 * 1024 * 1024);

  ASSERT_OK(DestroyDB(dbname, options));

  DB* db = nullptr;
  std::unique_ptr<Env> env = NewCompositeEnv(
      std::make_shared<CountedFileSystem>(FileSystem::Default()));
  options.create_if_missing = true;
  options.env = env.get();
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  // Explicitly close the database to ensure the open and close counter for
  // directories are equivalent
  s = db->Close();
  auto* counted_fs =
      options.env->GetFileSystem()->CheckedCast<CountedFileSystem>();
  ASSERT_TRUE(counted_fs != nullptr);
  ASSERT_EQ(counted_fs->counters()->dir_opens,
            counted_fs->counters()->dir_closes);
  ASSERT_OK(s);
  delete db;
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
  ASSERT_NE(s, Status::OK());
  // retry should return the same error
  s = dbfull()->Close();
  ASSERT_NE(s, Status::OK());
  fault_injection_env->SetFilesystemActive(true);
  // retry close() is no-op even the system is back. Could be improved if
  // Close() is retry-able: #9029
  s = dbfull()->Close();
  ASSERT_NE(s, Status::OK());
  Destroy(options);
}

class DBMultiGetTestWithParam
    : public DBBasicTest,
      public testing::WithParamInterface<std::tuple<bool, bool>> {};

TEST_P(DBMultiGetTestWithParam, MultiGetMultiCF) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
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
  ROCKSDB_NAMESPACE::DBImpl* db = static_cast_with_check<DBImpl>(db_);
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
            auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(
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

  values = MultiGet(cfs, keys, nullptr, std::get<0>(GetParam()),
                    std::get<1>(GetParam()));
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
  values = MultiGet(cfs, keys, nullptr, std::get<0>(GetParam()),
                    std::get<1>(GetParam()));
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
  values = MultiGet(cfs, keys, nullptr, std::get<0>(GetParam()),
                    std::get<1>(GetParam()));
  ASSERT_EQ(values[0], std::get<2>(cf_kv_vec[7]) + "_2");
  ASSERT_EQ(values[1], std::get<2>(cf_kv_vec[6]) + "_2");
  ASSERT_EQ(values[2], std::get<2>(cf_kv_vec[1]) + "_2");

  for (int cf = 0; cf < 8; ++cf) {
    auto* cfd =
        static_cast_with_check<ColumnFamilyHandleImpl>(
            static_cast_with_check<DBImpl>(db_)->GetColumnFamilyHandle(cf))
            ->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVObsolete);
  }
}

TEST_P(DBMultiGetTestWithParam, MultiGetMultiCFMutex) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
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

  values = MultiGet(cfs, keys, nullptr, std::get<0>(GetParam()),
                    std::get<1>(GetParam()));
  ASSERT_TRUE(last_try);
  ASSERT_EQ(values.size(), 8);
  for (unsigned int j = 0; j < values.size(); ++j) {
    ASSERT_EQ(values[j],
              "cf" + std::to_string(j) + "_val" + std::to_string(retries));
  }
  for (int i = 0; i < 8; ++i) {
    auto* cfd =
        static_cast_with_check<ColumnFamilyHandleImpl>(
            static_cast_with_check<DBImpl>(db_)->GetColumnFamilyHandle(i))
            ->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
  }
}

TEST_P(DBMultiGetTestWithParam, MultiGetMultiCFSnapshot) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  int get_sv_count = 0;
  ROCKSDB_NAMESPACE::DBImpl* db = static_cast_with_check<DBImpl>(db_);
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
            auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(
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
  values = MultiGet(cfs, keys, snapshot, std::get<0>(GetParam()),
                    std::get<1>(GetParam()));
  db_->ReleaseSnapshot(snapshot);
  ASSERT_EQ(values.size(), 8);
  for (unsigned int j = 0; j < values.size(); ++j) {
    ASSERT_EQ(values[j], "cf" + std::to_string(j) + "_val");
  }
  for (int i = 0; i < 8; ++i) {
    auto* cfd =
        static_cast_with_check<ColumnFamilyHandleImpl>(
            static_cast_with_check<DBImpl>(db_)->GetColumnFamilyHandle(i))
            ->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
  }
}

TEST_P(DBMultiGetTestWithParam, MultiGetMultiCFUnsorted) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(2, "baz", "xyz"));
  ASSERT_OK(Put(1, "abc", "def"));

  // Note: keys for the same CF do not form a consecutive range
  std::vector<int> cfs{1, 2, 1};
  std::vector<std::string> keys{"foo", "baz", "abc"};
  std::vector<std::string> values;

  values = MultiGet(cfs, keys, /* snapshot */ nullptr,
                    /* batched */ std::get<0>(GetParam()),
                    /* async */ std::get<1>(GetParam()));

  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "bar");
  ASSERT_EQ(values[1], "xyz");
  ASSERT_EQ(values[2], "def");
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedSimpleUnsorted) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
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

    ReadOptions ro;
    ro.async_io = std::get<1>(GetParam());
    db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                  s.data(), false);

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

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedSortedMultiFile) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    SetPerfLevel(kEnableCount);
    // To expand the power of this test, generate > 1 table file and
    // mix with memtable
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k5", "v5"));
    ASSERT_OK(Delete(1, "no_key"));

    get_perf_context()->Reset();

    std::vector<Slice> keys({"k1", "k2", "k3", "k4", "k5", "no_key"});
    std::vector<PinnableSlice> values(keys.size());
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
    std::vector<Status> s(keys.size());

    ReadOptions ro;
    ro.async_io = std::get<1>(GetParam());
    db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                  s.data(), true);

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

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedDuplicateKeys) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options opts = CurrentOptions();
  opts.merge_operator = MergeOperators::CreateStringAppendOperator();
  CreateAndReopenWithCF({"pikachu"}, opts);
  SetPerfLevel(kEnableCount);
  // To expand the power of this test, generate > 1 table file and
  // mix with memtable
  ASSERT_OK(Merge(1, "k1", "v1"));
  ASSERT_OK(Merge(1, "k2", "v2"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);
  ASSERT_OK(Merge(1, "k3", "v3"));
  ASSERT_OK(Merge(1, "k4", "v4"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);
  ASSERT_OK(Merge(1, "k4", "v4_2"));
  ASSERT_OK(Merge(1, "k6", "v6"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);
  ASSERT_OK(Merge(1, "k7", "v7"));
  ASSERT_OK(Merge(1, "k8", "v8"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);

  get_perf_context()->Reset();

  std::vector<Slice> keys({"k8", "k8", "k8", "k4", "k4", "k1", "k3"});
  std::vector<PinnableSlice> values(keys.size());
  std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
  std::vector<Status> s(keys.size());

  ReadOptions ro;
  ro.async_io = std::get<1>(GetParam());
  db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                s.data(), false);

  ASSERT_EQ(values.size(), keys.size());
  ASSERT_EQ(std::string(values[0].data(), values[0].size()), "v8");
  ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v8");
  ASSERT_EQ(std::string(values[2].data(), values[2].size()), "v8");
  ASSERT_EQ(std::string(values[3].data(), values[3].size()), "v4,v4_2");
  ASSERT_EQ(std::string(values[4].data(), values[4].size()), "v4,v4_2");
  ASSERT_EQ(std::string(values[5].data(), values[5].size()), "v1");
  ASSERT_EQ(std::string(values[6].data(), values[6].size()), "v3");
  ASSERT_EQ(24, (int)get_perf_context()->multiget_read_bytes);

  for (Status& status : s) {
    ASSERT_OK(status);
  }

  SetPerfLevel(kDisable);
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedMultiLevel) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);
  int num_keys = 0;

  for (int i = 0; i < 128; ++i) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l2_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  MoveFilesToLevel(2);

  for (int i = 0; i < 128; i += 3) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l1_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  MoveFilesToLevel(1);

  for (int i = 0; i < 128; i += 5) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l0_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
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

  values = MultiGet(keys, nullptr, std::get<1>(GetParam()));
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

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedMultiLevelMerge) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
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
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  MoveFilesToLevel(2);

  for (int i = 0; i < 128; i += 3) {
    ASSERT_OK(Merge("key_" + std::to_string(i), "val_l1_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  MoveFilesToLevel(1);

  for (int i = 0; i < 128; i += 5) {
    ASSERT_OK(Merge("key_" + std::to_string(i), "val_l0_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
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

  values = MultiGet(keys, nullptr, std::get<1>(GetParam()));
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

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedValueSizeInMemory) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  SetPerfLevel(kEnableCount);
  ASSERT_OK(Put(1, "k1", "v_1"));
  ASSERT_OK(Put(1, "k2", "v_2"));
  ASSERT_OK(Put(1, "k3", "v_3"));
  ASSERT_OK(Put(1, "k4", "v_4"));
  ASSERT_OK(Put(1, "k5", "v_5"));
  ASSERT_OK(Put(1, "k6", "v_6"));
  std::vector<Slice> keys = {"k1", "k2", "k3", "k4", "k5", "k6"};
  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> s(keys.size());
  std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);

  get_perf_context()->Reset();
  ReadOptions ro;
  ro.value_size_soft_limit = 11;
  ro.async_io = std::get<1>(GetParam());
  db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                s.data(), false);

  ASSERT_EQ(values.size(), keys.size());
  for (unsigned int i = 0; i < 4; i++) {
    ASSERT_EQ(std::string(values[i].data(), values[i].size()),
              "v_" + std::to_string(i + 1));
  }

  for (unsigned int i = 4; i < 6; i++) {
    ASSERT_TRUE(s[i].IsAborted());
  }

  ASSERT_EQ(12, (int)get_perf_context()->multiget_read_bytes);
  SetPerfLevel(kDisable);
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedValueSize) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    return;
  }
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    SetPerfLevel(kEnableCount);

    ASSERT_OK(Put(1, "k6", "v6"));
    ASSERT_OK(Put(1, "k7", "v7_"));
    ASSERT_OK(Put(1, "k3", "v3_"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k11", "v11"));
    ASSERT_OK(Delete(1, "no_key"));
    ASSERT_OK(Put(1, "k8", "v8_"));
    ASSERT_OK(Put(1, "k13", "v13"));
    ASSERT_OK(Put(1, "k14", "v14"));
    ASSERT_OK(Put(1, "k15", "v15"));
    ASSERT_OK(Put(1, "k16", "v16"));
    ASSERT_OK(Put(1, "k17", "v17"));
    ASSERT_OK(Flush(1));

    ASSERT_OK(Put(1, "k1", "v1_"));
    ASSERT_OK(Put(1, "k2", "v2_"));
    ASSERT_OK(Put(1, "k5", "v5_"));
    ASSERT_OK(Put(1, "k9", "v9_"));
    ASSERT_OK(Put(1, "k10", "v10"));
    ASSERT_OK(Delete(1, "k2"));
    ASSERT_OK(Delete(1, "k6"));

    get_perf_context()->Reset();

    std::vector<Slice> keys({"k1", "k10", "k11", "k12", "k13", "k14", "k15",
                             "k16", "k17", "k2", "k3", "k4", "k5", "k6", "k7",
                             "k8", "k9", "no_key"});
    std::vector<PinnableSlice> values(keys.size());
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
    std::vector<Status> s(keys.size());

    ReadOptions ro;
    ro.value_size_soft_limit = 20;
    ro.async_io = std::get<1>(GetParam());
    db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                  s.data(), false);

    ASSERT_EQ(values.size(), keys.size());

    // In memory keys
    ASSERT_EQ(std::string(values[0].data(), values[0].size()), "v1_");
    ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v10");
    ASSERT_TRUE(s[9].IsNotFound());  // k2
    ASSERT_EQ(std::string(values[12].data(), values[12].size()), "v5_");
    ASSERT_TRUE(s[13].IsNotFound());  // k6
    ASSERT_EQ(std::string(values[16].data(), values[16].size()), "v9_");

    // In sst files
    ASSERT_EQ(std::string(values[2].data(), values[1].size()), "v11");
    ASSERT_EQ(std::string(values[4].data(), values[4].size()), "v13");
    ASSERT_EQ(std::string(values[5].data(), values[5].size()), "v14");

    // Remaining aborted after value_size exceeds.
    ASSERT_TRUE(s[3].IsAborted());
    ASSERT_TRUE(s[6].IsAborted());
    ASSERT_TRUE(s[7].IsAborted());
    ASSERT_TRUE(s[8].IsAborted());
    ASSERT_TRUE(s[10].IsAborted());
    ASSERT_TRUE(s[11].IsAborted());
    ASSERT_TRUE(s[14].IsAborted());
    ASSERT_TRUE(s[15].IsAborted());
    ASSERT_TRUE(s[17].IsAborted());

    // 6 kv pairs * 3 bytes per value (i.e. 18)
    ASSERT_EQ(21, (int)get_perf_context()->multiget_read_bytes);
    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedValueSizeMultiLevelMerge) {
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test needs to be fixed for async IO");
    return;
  }
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);
  int num_keys = 0;

  for (int i = 0; i < 64; ++i) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l2_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  MoveFilesToLevel(2);

  for (int i = 0; i < 64; i += 3) {
    ASSERT_OK(Merge("key_" + std::to_string(i), "val_l1_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  MoveFilesToLevel(1);

  for (int i = 0; i < 64; i += 5) {
    ASSERT_OK(Merge("key_" + std::to_string(i), "val_l0_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  ASSERT_EQ(0, num_keys);

  for (int i = 0; i < 64; i += 9) {
    ASSERT_OK(
        Merge("key_" + std::to_string(i), "val_mem_" + std::to_string(i)));
  }

  std::vector<std::string> keys_str;
  for (int i = 10; i < 50; ++i) {
    keys_str.push_back("key_" + std::to_string(i));
  }

  std::vector<Slice> keys(keys_str.size());
  for (int i = 0; i < 40; i++) {
    keys[i] = Slice(keys_str[i]);
  }

  std::vector<PinnableSlice> values(keys_str.size());
  std::vector<Status> statuses(keys_str.size());
  ReadOptions read_options;
  read_options.verify_checksums = true;
  read_options.value_size_soft_limit = 380;
  read_options.async_io = std::get<1>(GetParam());
  db_->MultiGet(read_options, dbfull()->DefaultColumnFamily(), keys.size(),
                keys.data(), values.data(), statuses.data());

  ASSERT_EQ(values.size(), keys.size());

  for (unsigned int j = 0; j < 26; ++j) {
    int key = j + 10;
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
    ASSERT_OK(statuses[j]);
  }

  // All remaning keys status is set Status::Abort
  for (unsigned int j = 26; j < 40; j++) {
    ASSERT_TRUE(statuses[j].IsAborted());
  }
}

INSTANTIATE_TEST_CASE_P(DBMultiGetTestWithParam, DBMultiGetTestWithParam,
                        testing::Combine(testing::Bool(), testing::Bool()));

#if USE_COROUTINES
class DBMultiGetAsyncIOTest : public DBBasicTest,
                              public ::testing::WithParamInterface<bool> {
 public:
  DBMultiGetAsyncIOTest()
      : DBBasicTest(), statistics_(ROCKSDB_NAMESPACE::CreateDBStatistics()) {
    BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(NewBloomFilterPolicy(10));
    options_ = CurrentOptions();
    options_.disable_auto_compactions = true;
    options_.statistics = statistics_;
    options_.table_factory.reset(NewBlockBasedTableFactory(bbto));
    options_.env = Env::Default();
    Reopen(options_);
    int num_keys = 0;

    // Put all keys in the bottommost level, and overwrite some keys
    // in L0 and L1
    for (int i = 0; i < 256; ++i) {
      EXPECT_OK(Put(Key(i), "val_l2_" + std::to_string(i)));
      num_keys++;
      if (num_keys == 8) {
        EXPECT_OK(Flush());
        num_keys = 0;
      }
    }
    if (num_keys > 0) {
      EXPECT_OK(Flush());
      num_keys = 0;
    }
    MoveFilesToLevel(2);

    for (int i = 0; i < 128; i += 3) {
      EXPECT_OK(Put(Key(i), "val_l1_" + std::to_string(i)));
      num_keys++;
      if (num_keys == 8) {
        EXPECT_OK(Flush());
        num_keys = 0;
      }
    }
    if (num_keys > 0) {
      EXPECT_OK(Flush());
      num_keys = 0;
    }
    // Put some range deletes in L1
    for (int i = 128; i < 256; i += 32) {
      std::string range_begin = Key(i);
      std::string range_end = Key(i + 16);
      EXPECT_OK(dbfull()->DeleteRange(WriteOptions(),
                                      dbfull()->DefaultColumnFamily(),
                                      range_begin, range_end));
      // Also do some Puts to force creation of bloom filter
      for (int j = i + 16; j < i + 32; ++j) {
        if (j % 3 == 0) {
          EXPECT_OK(Put(Key(j), "val_l1_" + std::to_string(j)));
        }
      }
      EXPECT_OK(Flush());
    }
    MoveFilesToLevel(1);

    for (int i = 0; i < 128; i += 5) {
      EXPECT_OK(Put(Key(i), "val_l0_" + std::to_string(i)));
      num_keys++;
      if (num_keys == 8) {
        EXPECT_OK(Flush());
        num_keys = 0;
      }
    }
    if (num_keys > 0) {
      EXPECT_OK(Flush());
      num_keys = 0;
    }
    EXPECT_EQ(0, num_keys);
  }

  const std::shared_ptr<Statistics>& statistics() { return statistics_; }

 protected:
  void PrepareDBForTest() {
#ifdef ROCKSDB_IOURING_PRESENT
    Reopen(options_);
#else   // ROCKSDB_IOURING_PRESENT
    // Warm up the block cache so we don't need to use the IO uring
    Iterator* iter = dbfull()->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid() && iter->status().ok();
         iter->Next())
      ;
    EXPECT_OK(iter->status());
    delete iter;
#endif  // ROCKSDB_IOURING_PRESENT
  }

  void ReopenDB() { Reopen(options_); }

 private:
  std::shared_ptr<Statistics> statistics_;
  Options options_;
};

TEST_P(DBMultiGetAsyncIOTest, GetFromL0) {
  // All 3 keys in L0. The L0 files should be read serially.
  std::vector<std::string> key_strs{Key(0), Key(40), Key(80)};
  std::vector<Slice> keys{key_strs[0], key_strs[1], key_strs[2]};
  std::vector<PinnableSlice> values(key_strs.size());
  std::vector<Status> statuses(key_strs.size());

  PrepareDBForTest();

  ReadOptions ro;
  ro.async_io = true;
  ro.optimize_multiget_for_io = GetParam();
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data());
  ASSERT_EQ(values.size(), 3);
  ASSERT_OK(statuses[0]);
  ASSERT_OK(statuses[1]);
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[0], "val_l0_" + std::to_string(0));
  ASSERT_EQ(values[1], "val_l0_" + std::to_string(40));
  ASSERT_EQ(values[2], "val_l0_" + std::to_string(80));

  HistogramData multiget_io_batch_size;

  statistics()->histogramData(MULTIGET_IO_BATCH_SIZE, &multiget_io_batch_size);

  // With async IO, lookups will happen in parallel for each key
#ifdef ROCKSDB_IOURING_PRESENT
  if (GetParam()) {
    ASSERT_EQ(multiget_io_batch_size.count, 1);
    ASSERT_EQ(multiget_io_batch_size.max, 3);
    ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT), 3);
  } else {
    // Without Async IO, MultiGet will call MultiRead 3 times, once for each
    // L0 file
    ASSERT_EQ(multiget_io_batch_size.count, 3);
  }
#else   // ROCKSDB_IOURING_PRESENT
  if (GetParam()) {
    ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT), 3);
  }
#endif  // ROCKSDB_IOURING_PRESENT
}

TEST_P(DBMultiGetAsyncIOTest, GetFromL1) {
  std::vector<std::string> key_strs;
  std::vector<Slice> keys;
  std::vector<PinnableSlice> values;
  std::vector<Status> statuses;

  key_strs.push_back(Key(33));
  key_strs.push_back(Key(54));
  key_strs.push_back(Key(102));
  keys.push_back(key_strs[0]);
  keys.push_back(key_strs[1]);
  keys.push_back(key_strs[2]);
  values.resize(keys.size());
  statuses.resize(keys.size());

  PrepareDBForTest();

  ReadOptions ro;
  ro.async_io = true;
  ro.optimize_multiget_for_io = GetParam();
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data());
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(statuses[0], Status::OK());
  ASSERT_EQ(statuses[1], Status::OK());
  ASSERT_EQ(statuses[2], Status::OK());
  ASSERT_EQ(values[0], "val_l1_" + std::to_string(33));
  ASSERT_EQ(values[1], "val_l1_" + std::to_string(54));
  ASSERT_EQ(values[2], "val_l1_" + std::to_string(102));

#ifdef ROCKSDB_IOURING_PRESENT
  HistogramData multiget_io_batch_size;

  statistics()->histogramData(MULTIGET_IO_BATCH_SIZE, &multiget_io_batch_size);

  // A batch of 3 async IOs is expected, one for each overlapping file in L1
  ASSERT_EQ(multiget_io_batch_size.count, 1);
  ASSERT_EQ(multiget_io_batch_size.max, 3);
#endif  // ROCKSDB_IOURING_PRESENT
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT), 3);
}

#ifdef ROCKSDB_IOURING_PRESENT
TEST_P(DBMultiGetAsyncIOTest, GetFromL1Error) {
  std::vector<std::string> key_strs;
  std::vector<Slice> keys;
  std::vector<PinnableSlice> values;
  std::vector<Status> statuses;

  key_strs.push_back(Key(33));
  key_strs.push_back(Key(54));
  key_strs.push_back(Key(102));
  keys.push_back(key_strs[0]);
  keys.push_back(key_strs[1]);
  keys.push_back(key_strs[2]);
  values.resize(keys.size());
  statuses.resize(keys.size());

  int count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::GetTableReader:BeforeOpenFile", [&](void* status) {
        count++;
        // Fail the last table reader open, which is the 6th SST file
        // since 3 overlapping L0 files + 3 L1 files containing the keys
        if (count == 6) {
          Status* s = static_cast<Status*>(status);
          *s = Status::IOError();
        }
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

  PrepareDBForTest();

  ReadOptions ro;
  ro.async_io = true;
  ro.optimize_multiget_for_io = GetParam();
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(statuses[0], Status::OK());
  ASSERT_EQ(statuses[1], Status::OK());
  ASSERT_EQ(statuses[2], Status::IOError());

  HistogramData multiget_io_batch_size;

  statistics()->histogramData(MULTIGET_IO_BATCH_SIZE, &multiget_io_batch_size);

  // A batch of 3 async IOs is expected, one for each overlapping file in L1
  ASSERT_EQ(multiget_io_batch_size.count, 1);
  ASSERT_EQ(multiget_io_batch_size.max, 2);
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT), 2);
}
#endif  // ROCKSDB_IOURING_PRESENT

TEST_P(DBMultiGetAsyncIOTest, LastKeyInFile) {
  std::vector<std::string> key_strs;
  std::vector<Slice> keys;
  std::vector<PinnableSlice> values;
  std::vector<Status> statuses;

  // 21 is the last key in the first L1 file
  key_strs.push_back(Key(21));
  key_strs.push_back(Key(54));
  key_strs.push_back(Key(102));
  keys.push_back(key_strs[0]);
  keys.push_back(key_strs[1]);
  keys.push_back(key_strs[2]);
  values.resize(keys.size());
  statuses.resize(keys.size());

  PrepareDBForTest();

  ReadOptions ro;
  ro.async_io = true;
  ro.optimize_multiget_for_io = GetParam();
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data());
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(statuses[0], Status::OK());
  ASSERT_EQ(statuses[1], Status::OK());
  ASSERT_EQ(statuses[2], Status::OK());
  ASSERT_EQ(values[0], "val_l1_" + std::to_string(21));
  ASSERT_EQ(values[1], "val_l1_" + std::to_string(54));
  ASSERT_EQ(values[2], "val_l1_" + std::to_string(102));

#ifdef ROCKSDB_IOURING_PRESENT
  HistogramData multiget_io_batch_size;

  statistics()->histogramData(MULTIGET_IO_BATCH_SIZE, &multiget_io_batch_size);

  // Since the first MultiGet key is the last key in a file, the MultiGet is
  // expected to lookup in that file first, before moving on to other files.
  // So the first file lookup will issue one async read, and the next lookup
  // will lookup 2 files in parallel and issue 2 async reads
  ASSERT_EQ(multiget_io_batch_size.count, 2);
  ASSERT_EQ(multiget_io_batch_size.max, 2);
#endif  // ROCKSDB_IOURING_PRESENT
}

TEST_P(DBMultiGetAsyncIOTest, GetFromL1AndL2) {
  std::vector<std::string> key_strs;
  std::vector<Slice> keys;
  std::vector<PinnableSlice> values;
  std::vector<Status> statuses;

  // 33 and 102 are in L1, and 56 is in L2
  key_strs.push_back(Key(33));
  key_strs.push_back(Key(56));
  key_strs.push_back(Key(102));
  keys.push_back(key_strs[0]);
  keys.push_back(key_strs[1]);
  keys.push_back(key_strs[2]);
  values.resize(keys.size());
  statuses.resize(keys.size());

  PrepareDBForTest();

  ReadOptions ro;
  ro.async_io = true;
  ro.optimize_multiget_for_io = GetParam();
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data());
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(statuses[0], Status::OK());
  ASSERT_EQ(statuses[1], Status::OK());
  ASSERT_EQ(statuses[2], Status::OK());
  ASSERT_EQ(values[0], "val_l1_" + std::to_string(33));
  ASSERT_EQ(values[1], "val_l2_" + std::to_string(56));
  ASSERT_EQ(values[2], "val_l1_" + std::to_string(102));

#ifdef ROCKSDB_IOURING_PRESENT
  HistogramData multiget_io_batch_size;

  statistics()->histogramData(MULTIGET_IO_BATCH_SIZE, &multiget_io_batch_size);

  // There are 2 keys in L1 in twp separate files, and 1 in L2. With
  // optimize_multiget_for_io, all three lookups will happen in parallel.
  // Otherwise, the L2 lookup will happen after L1.
  ASSERT_EQ(multiget_io_batch_size.count, GetParam() ? 1 : 2);
  ASSERT_EQ(multiget_io_batch_size.max, GetParam() ? 3 : 2);
#endif  // ROCKSDB_IOURING_PRESENT
}

TEST_P(DBMultiGetAsyncIOTest, GetFromL2WithRangeOverlapL0L1) {
  std::vector<std::string> key_strs;
  std::vector<Slice> keys;
  std::vector<PinnableSlice> values;
  std::vector<Status> statuses;

  // 19 and 26 are in L2, but overlap with L0 and L1 file ranges
  key_strs.push_back(Key(19));
  key_strs.push_back(Key(26));
  keys.push_back(key_strs[0]);
  keys.push_back(key_strs[1]);
  values.resize(keys.size());
  statuses.resize(keys.size());

  PrepareDBForTest();

  ReadOptions ro;
  ro.async_io = true;
  ro.optimize_multiget_for_io = GetParam();
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data());
  ASSERT_EQ(values.size(), 2);
  ASSERT_EQ(statuses[0], Status::OK());
  ASSERT_EQ(statuses[1], Status::OK());
  ASSERT_EQ(values[0], "val_l2_" + std::to_string(19));
  ASSERT_EQ(values[1], "val_l2_" + std::to_string(26));

  // Bloom filters in L0/L1 will avoid the coroutine calls in those levels
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT), 2);
}

#ifdef ROCKSDB_IOURING_PRESENT
TEST_P(DBMultiGetAsyncIOTest, GetFromL2WithRangeDelInL1) {
  std::vector<std::string> key_strs;
  std::vector<Slice> keys;
  std::vector<PinnableSlice> values;
  std::vector<Status> statuses;

  // 139 and 163 are in L2, but overlap with a range deletes in L1
  key_strs.push_back(Key(139));
  key_strs.push_back(Key(163));
  keys.push_back(key_strs[0]);
  keys.push_back(key_strs[1]);
  values.resize(keys.size());
  statuses.resize(keys.size());

  PrepareDBForTest();

  ReadOptions ro;
  ro.async_io = true;
  ro.optimize_multiget_for_io = GetParam();
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data());
  ASSERT_EQ(values.size(), 2);
  ASSERT_EQ(statuses[0], Status::NotFound());
  ASSERT_EQ(statuses[1], Status::NotFound());

  // Bloom filters in L0/L1 will avoid the coroutine calls in those levels
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT), 2);
}

TEST_P(DBMultiGetAsyncIOTest, GetFromL1AndL2WithRangeDelInL1) {
  std::vector<std::string> key_strs;
  std::vector<Slice> keys;
  std::vector<PinnableSlice> values;
  std::vector<Status> statuses;

  // 139 and 163 are in L2, but overlap with a range deletes in L1
  key_strs.push_back(Key(139));
  key_strs.push_back(Key(144));
  key_strs.push_back(Key(163));
  keys.push_back(key_strs[0]);
  keys.push_back(key_strs[1]);
  keys.push_back(key_strs[2]);
  values.resize(keys.size());
  statuses.resize(keys.size());

  PrepareDBForTest();

  ReadOptions ro;
  ro.async_io = true;
  ro.optimize_multiget_for_io = GetParam();
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data());
  ASSERT_EQ(values.size(), keys.size());
  ASSERT_EQ(statuses[0], Status::NotFound());
  ASSERT_EQ(statuses[1], Status::OK());
  ASSERT_EQ(values[1], "val_l1_" + std::to_string(144));
  ASSERT_EQ(statuses[2], Status::NotFound());

  // Bloom filters in L0/L1 will avoid the coroutine calls in those levels
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT), 3);
}
#endif  // ROCKSDB_IOURING_PRESENT

TEST_P(DBMultiGetAsyncIOTest, GetNoIOUring) {
  std::vector<std::string> key_strs;
  std::vector<Slice> keys;
  std::vector<PinnableSlice> values;
  std::vector<Status> statuses;

  key_strs.push_back(Key(33));
  key_strs.push_back(Key(54));
  key_strs.push_back(Key(102));
  keys.push_back(key_strs[0]);
  keys.push_back(key_strs[1]);
  keys.push_back(key_strs[2]);
  values.resize(keys.size());
  statuses.resize(keys.size());

  enable_io_uring = false;
  ReopenDB();

  ReadOptions ro;
  ro.async_io = true;
  ro.optimize_multiget_for_io = GetParam();
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data());
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(statuses[0], Status::NotSupported());
  ASSERT_EQ(statuses[1], Status::NotSupported());
  ASSERT_EQ(statuses[2], Status::NotSupported());

  HistogramData multiget_io_batch_size;

  statistics()->histogramData(MULTIGET_IO_BATCH_SIZE, &multiget_io_batch_size);

  // A batch of 3 async IOs is expected, one for each overlapping file in L1
  ASSERT_EQ(multiget_io_batch_size.count, 1);
  ASSERT_EQ(multiget_io_batch_size.max, 3);
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT), 3);
}

INSTANTIATE_TEST_CASE_P(DBMultiGetAsyncIOTest, DBMultiGetAsyncIOTest,
                        testing::Bool());
#endif  // USE_COROUTINES

TEST_F(DBBasicTest, MultiGetStats) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.env = env_;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_options.partition_filters = true;
  table_options.no_block_cache = true;
  table_options.cache_index_and_filter_blocks = false;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  int total_keys = 2000;
  std::vector<std::string> keys_str(total_keys);
  std::vector<Slice> keys(total_keys);
  static size_t kMultiGetBatchSize = 100;
  std::vector<PinnableSlice> values(kMultiGetBatchSize);
  std::vector<Status> s(kMultiGetBatchSize);
  ReadOptions read_opts;

  Random rnd(309);
  // Create Multiple SST files at multiple levels.
  for (int i = 0; i < 500; ++i) {
    keys_str[i] = "k" + std::to_string(i);
    keys[i] = Slice(keys_str[i]);
    ASSERT_OK(Put(1, "k" + std::to_string(i), rnd.RandomString(1000)));
    if (i % 100 == 0) {
      ASSERT_OK(Flush(1));
    }
  }
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);

  for (int i = 501; i < 1000; ++i) {
    keys_str[i] = "k" + std::to_string(i);
    keys[i] = Slice(keys_str[i]);
    ASSERT_OK(Put(1, "k" + std::to_string(i), rnd.RandomString(1000)));
    if (i % 100 == 0) {
      ASSERT_OK(Flush(1));
    }
  }

  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);

  for (int i = 1001; i < total_keys; ++i) {
    keys_str[i] = "k" + std::to_string(i);
    keys[i] = Slice(keys_str[i]);
    ASSERT_OK(Put(1, "k" + std::to_string(i), rnd.RandomString(1000)));
    if (i % 100 == 0) {
      ASSERT_OK(Flush(1));
    }
  }
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(1, 1);
  Close();

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_OK(options.statistics->Reset());

  db_->MultiGet(read_opts, handles_[1], kMultiGetBatchSize, &keys[1250],
                values.data(), s.data(), false);

  ASSERT_EQ(values.size(), kMultiGetBatchSize);
  HistogramData hist_level;
  HistogramData hist_index_and_filter_blocks;
  HistogramData hist_sst;

  options.statistics->histogramData(NUM_LEVEL_READ_PER_MULTIGET, &hist_level);
  options.statistics->histogramData(NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL,
                                    &hist_index_and_filter_blocks);
  options.statistics->histogramData(NUM_SST_READ_PER_LEVEL, &hist_sst);

  // Maximum number of blocks read from a file system in a level.
  ASSERT_EQ(hist_level.max, 1);
  ASSERT_GT(hist_index_and_filter_blocks.max, 0);
  // Maximum number of sst files read from file system in a level.
  ASSERT_EQ(hist_sst.max, 2);

  // Minimun number of blocks read in a level.
  ASSERT_EQ(hist_level.min, 1);
  ASSERT_GT(hist_index_and_filter_blocks.min, 0);
  // Minimun number of sst files read in a level.
  ASSERT_EQ(hist_sst.min, 1);

  for (PinnableSlice& value : values) {
    value.Reset();
  }
  for (Status& status : s) {
    status = Status::OK();
  }
  db_->MultiGet(read_opts, handles_[1], kMultiGetBatchSize, &keys[950],
                values.data(), s.data(), false);
  options.statistics->histogramData(NUM_LEVEL_READ_PER_MULTIGET, &hist_level);
  ASSERT_EQ(hist_level.max, 2);
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

  ASSERT_OK(Put("k", "v0"));
  ASSERT_OK(Put("kk1", "v1"));
  ASSERT_OK(Put("kk2", "v2"));
  ASSERT_OK(Put("kk3", "v3"));
  ASSERT_OK(Put("kk4", "v4"));
  std::vector<std::string> keys(
      {"k", "kk1", "kk2", "kk3", "kk4", "rofl", "lmho"});
  std::vector<std::string> expected(
      {"v0", "v1", "v2", "v3", "v4", "NOT_FOUND", "NOT_FOUND"});
  std::vector<std::string> values;
  values = MultiGet(keys, nullptr);
  ASSERT_EQ(values, expected);
  // One key ("k") is not queried against the filter because it is outside
  // the prefix_extractor domain, leaving 6 keys with queried prefixes.
  ASSERT_EQ(get_perf_context()->bloom_memtable_miss_count, 2);
  ASSERT_EQ(get_perf_context()->bloom_memtable_hit_count, 4);
  ASSERT_OK(Flush());

  get_perf_context()->Reset();
  values = MultiGet(keys, nullptr);
  ASSERT_EQ(values, expected);
  ASSERT_EQ(get_perf_context()->bloom_sst_miss_count, 2);
  ASSERT_EQ(get_perf_context()->bloom_sst_hit_count, 4);

  // Also check Get stat
  get_perf_context()->Reset();
  for (size_t i = 0; i < keys.size(); ++i) {
    values[i] = Get(keys[i]);
  }
  ASSERT_EQ(values, expected);
  ASSERT_EQ(get_perf_context()->bloom_sst_miss_count, 2);
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
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "k5", "v5"));
    const Snapshot* snap1 = dbfull()->GetSnapshot();
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Flush(1));
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
  ASSERT_OK(GetAllKeyVersions(db_, Slice(), Slice(),
                              std::numeric_limits<size_t>::max(),
                              &key_versions));
  ASSERT_EQ(kNumInserts + kNumDeletes + kNumUpdates, key_versions.size());
  for (size_t i = 0; i < kNumInserts + kNumDeletes + kNumUpdates; i++) {
    if (i % 3 == 0) {
      ASSERT_EQ(key_versions[i].GetTypeName(), "TypeDeletion");
    } else {
      ASSERT_EQ(key_versions[i].GetTypeName(), "TypeValue");
    }
  }
  ASSERT_OK(GetAllKeyVersions(db_, handles_[0], Slice(), Slice(),
                              std::numeric_limits<size_t>::max(),
                              &key_versions));
  ASSERT_EQ(kNumInserts + kNumDeletes + kNumUpdates, key_versions.size());

  // Check non-default column family
  for (size_t i = 0; i + 1 != kNumInserts; ++i) {
    ASSERT_OK(Put(1, std::to_string(i), "value"));
  }
  for (size_t i = 0; i + 1 != kNumUpdates; ++i) {
    ASSERT_OK(Put(1, std::to_string(i), "value1"));
  }
  for (size_t i = 0; i + 1 != kNumDeletes; ++i) {
    ASSERT_OK(Delete(1, std::to_string(i)));
  }
  ASSERT_OK(GetAllKeyVersions(db_, handles_[1], Slice(), Slice(),
                              std::numeric_limits<size_t>::max(),
                              &key_versions));
  ASSERT_EQ(kNumInserts + kNumDeletes + kNumUpdates - 3, key_versions.size());
}

TEST_F(DBBasicTest, ValueTypeString) {
  KeyVersion key_version;
  // when adding new type, please also update `value_type_string_map`
  for (unsigned char i = ValueType::kTypeDeletion; i < ValueType::kTypeMaxValid;
       i++) {
    key_version.type = i;
    ASSERT_TRUE(key_version.GetTypeName() != "Invalid");
  }
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
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  std::string zero_str(128, '\0');
  for (int i = 0; i < 100; ++i) {
    // Make the value compressible. A purely random string doesn't compress
    // and the resultant data block will not be compressed
    std::string value(rnd.RandomString(128) + zero_str);
    assert(Put(Key(i), value) == Status::OK());
  }
  ASSERT_OK(Flush());

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
}  // anonymous namespace

TEST_F(DBBasicTest, LastSstFileNotInManifest) {
  // If the last sst file is not tracked in MANIFEST,
  // or the VersionEdit for the last sst file is not synced,
  // on recovery, the last sst file should be deleted,
  // and new sst files shouldn't reuse its file number.
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  Close();

  // Manually add a sst file.
  constexpr uint64_t kSstFileNumber = 100;
  const std::string kSstFile = MakeTableFileName(dbname_, kSstFileNumber);
  ASSERT_OK(WriteStringToFile(env_, /* data = */ "bad sst file content",
                              /* fname = */ kSstFile,
                              /* should_sync = */ true));
  ASSERT_OK(env_->FileExists(kSstFile));

  TableFileListener* listener = new TableFileListener();
  options.listeners.emplace_back(listener);
  Reopen(options);
  // kSstFile should already be deleted.
  ASSERT_TRUE(env_->FileExists(kSstFile).IsNotFound());

  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  // New sst file should have file number > kSstFileNumber.
  std::vector<std::string>& files =
      listener->GetFiles(kDefaultColumnFamilyName);
  ASSERT_EQ(files.size(), 1);
  const std::string fname = files[0].erase(0, (dbname_ + "/").size());
  uint64_t number = 0;
  FileType type = kTableFile;
  ASSERT_TRUE(ParseFileName(fname, &number, &type));
  ASSERT_EQ(type, kTableFile);
  ASSERT_GT(number, kSstFileNumber);
}

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
    ASSERT_OK(iter->status());
    iter.reset(db_->NewIterator(read_opts, handles_[1]));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a", iter->key());
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    iter.reset(db_->NewIterator(read_opts, handles_[2]));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a", iter->key());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("b", iter->key());
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
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

TEST_F(DBBasicTest, RecoverWithNoManifest) {
  Options options = CurrentOptions();
  options.env = env_;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(Flush());
  Close();
  {
    // Delete all MANIFEST.
    std::vector<std::string> files;
    ASSERT_OK(env_->GetChildren(dbname_, &files));
    for (const auto& file : files) {
      uint64_t number = 0;
      FileType type = kWalFile;
      if (ParseFileName(file, &number, &type) && type == kDescriptorFile) {
        ASSERT_OK(env_->DeleteFile(dbname_ + "/" + file));
      }
    }
  }
  options.best_efforts_recovery = true;
  options.create_if_missing = false;
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsInvalidArgument());
  options.create_if_missing = true;
  Reopen(options);
  // Since no MANIFEST exists, best-efforts recovery creates a new, empty db.
  ASSERT_EQ("NOT_FOUND", Get("foo"));
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
  ASSERT_OK(iter->status());
  iter.reset(db_->NewIterator(read_opts, handles_[1]));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("a", iter->key());
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_F(DBBasicTest, DisableTrackWal) {
  // If WAL tracking was enabled, and then disabled during reopen,
  // the previously tracked WALs should be removed from MANIFEST.

  Options options = CurrentOptions();
  options.track_and_verify_wals_in_manifest = true;
  // extremely small write buffer size,
  // so that new WALs are created more frequently.
  options.write_buffer_size = 100;
  options.env = env_;
  DestroyAndReopen(options);
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put("foo" + std::to_string(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(db_->SyncWAL());
  // Some WALs are tracked.
  ASSERT_FALSE(dbfull()->GetVersionSet()->GetWalSet().GetWals().empty());
  Close();

  // Disable WAL tracking.
  options.track_and_verify_wals_in_manifest = false;
  options.create_if_missing = false;
  ASSERT_OK(TryReopen(options));
  // Previously tracked WALs are cleared.
  ASSERT_TRUE(dbfull()->GetVersionSet()->GetWalSet().GetWals().empty());
  Close();

  // Re-enable WAL tracking again.
  options.track_and_verify_wals_in_manifest = true;
  options.create_if_missing = false;
  ASSERT_OK(TryReopen(options));
  ASSERT_TRUE(dbfull()->GetVersionSet()->GetWalSet().GetWals().empty());
  Close();
}
#endif  // !ROCKSDB_LITE

TEST_F(DBBasicTest, ManifestChecksumMismatch) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("bar", "value"));
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "LogWriter::EmitPhysicalRecord:BeforeEncodeChecksum", [&](void* arg) {
        auto* crc = reinterpret_cast<uint32_t*>(arg);
        *crc = *crc + 1;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions write_opts;
  write_opts.disableWAL = true;
  Status s = db_->Put(write_opts, "foo", "value");
  ASSERT_OK(s);
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(Put("foo", "value1"));
  ASSERT_OK(Flush());
  s = TryReopen(options);
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(DBBasicTest, ConcurrentlyCloseDB) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  std::vector<std::thread> workers;
  for (int i = 0; i < 10; i++) {
    workers.push_back(std::thread([&]() {
      auto s = db_->Close();
      ASSERT_OK(s);
    }));
  }
  for (auto& w : workers) {
    w.join();
  }
}

#ifndef ROCKSDB_LITE
class DBBasicTestTrackWal : public DBTestBase,
                            public testing::WithParamInterface<bool> {
 public:
  DBBasicTestTrackWal()
      : DBTestBase("db_basic_test_track_wal", /*env_do_fsync=*/false) {}

  int CountWalFiles() {
    VectorLogPtr log_files;
    EXPECT_OK(dbfull()->GetSortedWalFiles(log_files));
    return static_cast<int>(log_files.size());
  };
};

TEST_P(DBBasicTestTrackWal, DoNotTrackObsoleteWal) {
  // If a WAL becomes obsolete after flushing, but is not deleted from disk yet,
  // then if SyncWAL is called afterwards, the obsolete WAL should not be
  // tracked in MANIFEST.

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.track_and_verify_wals_in_manifest = true;
  options.atomic_flush = GetParam();

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"cf"}, options);
  ASSERT_EQ(handles_.size(), 2);  // default, cf
  // Do not delete WALs.
  ASSERT_OK(db_->DisableFileDeletions());
  constexpr int n = 10;
  std::vector<std::unique_ptr<LogFile>> wals(n);
  for (size_t i = 0; i < n; i++) {
    // Generate a new WAL for each key-value.
    const int cf = i % 2;
    ASSERT_OK(db_->GetCurrentWalFile(&wals[i]));
    ASSERT_OK(Put(cf, "k" + std::to_string(i), "v" + std::to_string(i)));
    ASSERT_OK(Flush({0, 1}));
  }
  ASSERT_EQ(CountWalFiles(), n);
  // Since all WALs are obsolete, no WAL should be tracked in MANIFEST.
  ASSERT_OK(db_->SyncWAL());

  // Manually delete all WALs.
  Close();
  for (const auto& wal : wals) {
    ASSERT_OK(env_->DeleteFile(LogFileName(dbname_, wal->LogNumber())));
  }

  // If SyncWAL tracks the obsolete WALs in MANIFEST,
  // reopen will fail because the WALs are missing from disk.
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "cf"}, options));
  Destroy(options);
}

INSTANTIATE_TEST_CASE_P(DBBasicTestTrackWal, DBBasicTestTrackWal,
                        testing::Bool());
#endif  // ROCKSDB_LITE

class DBBasicTestMultiGet : public DBTestBase {
 public:
  DBBasicTestMultiGet(std::string test_dir, int num_cfs, bool compressed_cache,
                      bool uncompressed_cache, bool _compression_enabled,
                      bool _fill_cache, uint32_t compression_parallel_threads)
      : DBTestBase(test_dir, /*env_do_fsync=*/false) {
    compression_enabled_ = _compression_enabled;
    fill_cache_ = _fill_cache;

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
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    if (!compression_enabled_) {
      options.compression = kNoCompression;
    } else {
      options.compression_opts.parallel_threads = compression_parallel_threads;
    }
    options_ = options;
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
        values_.emplace_back(rnd.RandomString(128) + zero_str);
        assert(((num_cfs == 1) ? Put(Key(i), values_[i])
                               : Put(cf, Key(i), values_[i])) == Status::OK());
      }
      if (num_cfs == 1) {
        EXPECT_OK(Flush());
      } else {
        EXPECT_OK(dbfull()->Flush(FlushOptions(), handles_[cf]));
      }

      for (int i = 0; i < 100; ++i) {
        // block cannot gain space by compression
        uncompressable_values_.emplace_back(rnd.RandomString(256) + '\0');
        std::string tmp_key = "a" + Key(i);
        assert(((num_cfs == 1) ? Put(tmp_key, uncompressable_values_[i])
                               : Put(cf, tmp_key, uncompressable_values_[i])) ==
               Status::OK());
      }
      if (num_cfs == 1) {
        EXPECT_OK(Flush());
      } else {
        EXPECT_OK(dbfull()->Flush(FlushOptions(), handles_[cf]));
      }
    }
    // Clear compressed cache, which is always pre-populated
    if (compressed_cache_) {
      compressed_cache_->SetCapacity(0);
      compressed_cache_->SetCapacity(1048576);
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
  Options get_options() { return options_; }

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

    Status Insert(const Slice& key, Cache::ObjectPtr value,
                  const CacheItemHelper* helper, size_t charge,
                  Handle** handle = nullptr,
                  Priority priority = Priority::LOW) override {
      num_inserts_++;
      return target_->Insert(key, value, helper, charge, handle, priority);
    }

    Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                   CreateContext* create_context,
                   Priority priority = Priority::LOW, bool wait = true,
                   Statistics* stats = nullptr) override {
      num_lookups_++;
      Handle* handle =
          target_->Lookup(key, helper, create_context, priority, wait, stats);
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
  Options options_;
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

#ifndef ROCKSDB_LITE
TEST_P(DBBasicTestWithParallelIO, MultiGetDirectIO) {
  class FakeDirectIOEnv : public EnvWrapper {
    class FakeDirectIOSequentialFile;
    class FakeDirectIORandomAccessFile;

   public:
    FakeDirectIOEnv(Env* env) : EnvWrapper(env) {}
    static const char* kClassName() { return "FakeDirectIOEnv"; }
    const char* Name() const override { return kClassName(); }

    Status NewRandomAccessFile(const std::string& fname,
                               std::unique_ptr<RandomAccessFile>* result,
                               const EnvOptions& options) override {
      std::unique_ptr<RandomAccessFile> file;
      assert(options.use_direct_reads);
      EnvOptions opts = options;
      opts.use_direct_reads = false;
      Status s = target()->NewRandomAccessFile(fname, &file, opts);
      if (!s.ok()) {
        return s;
      }
      result->reset(new FakeDirectIORandomAccessFile(std::move(file)));
      return s;
    }

   private:
    class FakeDirectIOSequentialFile : public SequentialFileWrapper {
     public:
      FakeDirectIOSequentialFile(std::unique_ptr<SequentialFile>&& file)
          : SequentialFileWrapper(file.get()), file_(std::move(file)) {}
      ~FakeDirectIOSequentialFile() {}

      bool use_direct_io() const override { return true; }
      size_t GetRequiredBufferAlignment() const override { return 1; }

     private:
      std::unique_ptr<SequentialFile> file_;
    };

    class FakeDirectIORandomAccessFile : public RandomAccessFileWrapper {
     public:
      FakeDirectIORandomAccessFile(std::unique_ptr<RandomAccessFile>&& file)
          : RandomAccessFileWrapper(file.get()), file_(std::move(file)) {}
      ~FakeDirectIORandomAccessFile() {}

      bool use_direct_io() const override { return true; }
      size_t GetRequiredBufferAlignment() const override { return 1; }

     private:
      std::unique_ptr<RandomAccessFile> file_;
    };
  };

  std::unique_ptr<FakeDirectIOEnv> env(new FakeDirectIOEnv(env_));
  Options opts = get_options();
  opts.env = env.get();
  opts.use_direct_reads = true;
  Reopen(opts);

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
  if (uncompressed_cache_) {
    uncompressed_cache_->SetCapacity(0);
    uncompressed_cache_->SetCapacity(1048576);
  }
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

  int expected_reads = random_reads;
  if (!compression_enabled() || !has_compressed_cache()) {
    expected_reads += 2;
  } else {
    expected_reads += (read_from_cache ? 0 : 2);
  }
  if (env_->random_read_counter_.Read() != expected_reads) {
    ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);
  }
  Close();
}
#endif  // ROCKSDB_LITE

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

// Forward declaration
class DeadlineFS;

class DeadlineRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
 public:
  DeadlineRandomAccessFile(DeadlineFS& fs,
                           std::unique_ptr<FSRandomAccessFile>& file)
      : FSRandomAccessFileOwnerWrapper(std::move(file)), fs_(fs) {}

  IOStatus Read(uint64_t offset, size_t len, const IOOptions& opts,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override;

  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(const FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override;

 private:
  DeadlineFS& fs_;
  std::unique_ptr<FSRandomAccessFile> file_;
};

class DeadlineFS : public FileSystemWrapper {
 public:
  // The error_on_delay parameter specifies whether a IOStatus::TimedOut()
  // status should be returned after delaying the IO to exceed the timeout,
  // or to simply delay but return success anyway. The latter mimics the
  // behavior of PosixFileSystem, which does not enforce any timeout
  explicit DeadlineFS(SpecialEnv* env, bool error_on_delay)
      : FileSystemWrapper(env->GetFileSystem()),
        deadline_(std::chrono::microseconds::zero()),
        io_timeout_(std::chrono::microseconds::zero()),
        env_(env),
        timedout_(false),
        ignore_deadline_(false),
        error_on_delay_(error_on_delay) {}

  static const char* kClassName() { return "DeadlineFileSystem"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
    EXPECT_OK(s);
    result->reset(new DeadlineRandomAccessFile(*this, file));

    const std::chrono::microseconds deadline = GetDeadline();
    const std::chrono::microseconds io_timeout = GetIOTimeout();
    if (deadline.count() || io_timeout.count()) {
      AssertDeadline(deadline, io_timeout, opts.io_options);
    }
    return ShouldDelay(opts.io_options);
  }

  // Set a vector of {IO counter, delay in microseconds, return status} tuples
  // that control when to inject a delay and duration of the delay
  void SetDelayTrigger(const std::chrono::microseconds deadline,
                       const std::chrono::microseconds io_timeout,
                       const int trigger) {
    delay_trigger_ = trigger;
    io_count_ = 0;
    deadline_ = deadline;
    io_timeout_ = io_timeout;
    timedout_ = false;
  }

  // Increment the IO counter and return a delay in microseconds
  IOStatus ShouldDelay(const IOOptions& opts) {
    if (timedout_) {
      return IOStatus::TimedOut();
    } else if (!deadline_.count() && !io_timeout_.count()) {
      return IOStatus::OK();
    }
    if (!ignore_deadline_ && delay_trigger_ == io_count_++) {
      env_->SleepForMicroseconds(static_cast<int>(opts.timeout.count() + 1));
      timedout_ = true;
      if (error_on_delay_) {
        return IOStatus::TimedOut();
      }
    }
    return IOStatus::OK();
  }

  const std::chrono::microseconds GetDeadline() {
    return ignore_deadline_ ? std::chrono::microseconds::zero() : deadline_;
  }

  const std::chrono::microseconds GetIOTimeout() {
    return ignore_deadline_ ? std::chrono::microseconds::zero() : io_timeout_;
  }

  bool TimedOut() { return timedout_; }

  void IgnoreDeadline(bool ignore) { ignore_deadline_ = ignore; }

  void AssertDeadline(const std::chrono::microseconds deadline,
                      const std::chrono::microseconds io_timeout,
                      const IOOptions& opts) const {
    // Give a leeway of +- 10us as it can take some time for the Get/
    // MultiGet call to reach here, in order to avoid false alarms
    std::chrono::microseconds now =
        std::chrono::microseconds(env_->NowMicros());
    std::chrono::microseconds timeout;
    if (deadline.count()) {
      timeout = deadline - now;
      if (io_timeout.count()) {
        timeout = std::min(timeout, io_timeout);
      }
    } else {
      timeout = io_timeout;
    }
    if (opts.timeout != timeout) {
      ASSERT_EQ(timeout, opts.timeout);
    }
  }

 private:
  // The number of IOs to trigger the delay after
  int delay_trigger_;
  // Current IO count
  int io_count_;
  // ReadOptions deadline for the Get/MultiGet/Iterator
  std::chrono::microseconds deadline_;
  // ReadOptions io_timeout for the Get/MultiGet/Iterator
  std::chrono::microseconds io_timeout_;
  SpecialEnv* env_;
  // Flag to indicate whether we injected a delay
  bool timedout_;
  // Temporarily ignore deadlines/timeouts
  bool ignore_deadline_;
  // Return IOStatus::TimedOut() or IOStatus::OK()
  bool error_on_delay_;
};

IOStatus DeadlineRandomAccessFile::Read(uint64_t offset, size_t len,
                                        const IOOptions& opts, Slice* result,
                                        char* scratch,
                                        IODebugContext* dbg) const {
  const std::chrono::microseconds deadline = fs_.GetDeadline();
  const std::chrono::microseconds io_timeout = fs_.GetIOTimeout();
  IOStatus s;
  if (deadline.count() || io_timeout.count()) {
    fs_.AssertDeadline(deadline, io_timeout, opts);
  }
  if (s.ok()) {
    s = FSRandomAccessFileWrapper::Read(offset, len, opts, result, scratch,
                                        dbg);
  }
  if (s.ok()) {
    s = fs_.ShouldDelay(opts);
  }
  return s;
}

IOStatus DeadlineRandomAccessFile::ReadAsync(
    FSReadRequest& req, const IOOptions& opts,
    std::function<void(const FSReadRequest&, void*)> cb, void* cb_arg,
    void** io_handle, IOHandleDeleter* del_fn, IODebugContext* dbg) {
  const std::chrono::microseconds deadline = fs_.GetDeadline();
  const std::chrono::microseconds io_timeout = fs_.GetIOTimeout();
  IOStatus s;
  if (deadline.count() || io_timeout.count()) {
    fs_.AssertDeadline(deadline, io_timeout, opts);
  }
  if (s.ok()) {
    s = FSRandomAccessFileWrapper::ReadAsync(req, opts, cb, cb_arg, io_handle,
                                             del_fn, dbg);
  }
  if (s.ok()) {
    s = fs_.ShouldDelay(opts);
  }
  return s;
}

IOStatus DeadlineRandomAccessFile::MultiRead(FSReadRequest* reqs,
                                             size_t num_reqs,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  const std::chrono::microseconds deadline = fs_.GetDeadline();
  const std::chrono::microseconds io_timeout = fs_.GetIOTimeout();
  IOStatus s;
  if (deadline.count() || io_timeout.count()) {
    fs_.AssertDeadline(deadline, io_timeout, options);
  }
  if (s.ok()) {
    s = FSRandomAccessFileWrapper::MultiRead(reqs, num_reqs, options, dbg);
  }
  if (s.ok()) {
    s = fs_.ShouldDelay(options);
  }
  return s;
}

// A test class for intercepting random reads and injecting artificial
// delays. Used for testing the MultiGet deadline feature
class DBBasicTestMultiGetDeadline : public DBBasicTestMultiGet,
                                    public testing::WithParamInterface<bool> {
 public:
  DBBasicTestMultiGetDeadline()
      : DBBasicTestMultiGet(
            "db_basic_test_multiget_deadline" /*Test dir*/,
            10 /*# of column families*/, false /*compressed cache enabled*/,
            true /*uncompressed cache enabled*/, true /*compression enabled*/,
            true /*ReadOptions.fill_cache*/,
            1 /*# of parallel compression threads*/) {}

  inline void CheckStatus(std::vector<Status>& statuses, size_t num_ok) {
    for (size_t i = 0; i < statuses.size(); ++i) {
      if (i < num_ok) {
        EXPECT_OK(statuses[i]);
      } else {
        if (statuses[i] != Status::TimedOut()) {
          EXPECT_EQ(statuses[i], Status::TimedOut());
        }
      }
    }
  }
};

TEST_P(DBBasicTestMultiGetDeadline, MultiGetDeadlineExceeded) {
#ifndef USE_COROUTINES
  if (GetParam()) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  std::shared_ptr<DeadlineFS> fs = std::make_shared<DeadlineFS>(env_, false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options = CurrentOptions();

  std::shared_ptr<Cache> cache = NewLRUCache(1048576);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = env.get();
  SetTimeElapseOnlySleepOnReopen(&options);
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
  ro.async_io = GetParam();
  // Delay the first IO
  fs->SetDelayTrigger(ro.deadline, ro.io_timeout, 0);

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
  fs->SetDelayTrigger(ro.deadline, ro.io_timeout, 1);
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
  fs->SetDelayTrigger(ro.deadline, ro.io_timeout, 0);
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
  fs->SetDelayTrigger(ro.deadline, ro.io_timeout, 2);
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
  fs->SetDelayTrigger(ro.deadline, ro.io_timeout, 3);
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
  fs->SetDelayTrigger(ro.deadline, ro.io_timeout, 1);
  dbfull()->MultiGet(ro, handles_[0], keys.size(), keys.data(),
                     pin_values.data(), statuses.data());
  CheckStatus(statuses, 64);
  Close();
}

INSTANTIATE_TEST_CASE_P(DeadlineIO, DBBasicTestMultiGetDeadline,
                        ::testing::Bool());

TEST_F(DBBasicTest, ManifestWriteFailure) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.env = env_;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto* s = reinterpret_cast<Status*>(arg);
        ASSERT_OK(*s);
        // Manually overwrite return status
        *s = Status::IOError();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("key", "value"));
  ASSERT_NOK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
}

TEST_F(DBBasicTest, DestroyDefaultCfHandle) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  for (const auto* h : handles_) {
    ASSERT_NE(db_->DefaultColumnFamily(), h);
  }

  // We have two handles to the default column family. The two handles point to
  // different ColumnFamilyHandle objects.
  assert(db_->DefaultColumnFamily());
  ASSERT_EQ(0U, db_->DefaultColumnFamily()->GetID());
  assert(handles_[0]);
  ASSERT_EQ(0U, handles_[0]->GetID());

  // You can destroy handles_[...].
  for (auto* h : handles_) {
    ASSERT_OK(db_->DestroyColumnFamilyHandle(h));
  }
  handles_.clear();

  // But you should not destroy db_->DefaultColumnFamily(), since it's going to
  // be deleted in `DBImpl::CloseHelper()`. Before that, it may be used
  // elsewhere internally too.
  ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();
  ASSERT_TRUE(db_->DestroyColumnFamilyHandle(default_cf).IsInvalidArgument());
}

TEST_F(DBBasicTest, FailOpenIfLoggerCreationFail) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "rocksdb::CreateLoggerFromOptions:AfterGetPath", [&](void* arg) {
        auto* s = reinterpret_cast<Status*>(arg);
        assert(s);
        *s = Status::IOError("Injected");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  ASSERT_EQ(nullptr, options.info_log);
  ASSERT_TRUE(s.IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

#ifndef ROCKSDB_LITE
TEST_F(DBBasicTest, VerifyFileChecksums) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = env_;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "value"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(db_->VerifyFileChecksums(ReadOptions()).IsInvalidArgument());

  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  Reopen(options);
  ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));

  // Write an L0 with checksum computed.
  ASSERT_OK(Put("b", "value"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));

  // Does the right thing but with the wrong name -- using it should lead to an
  // error.
  class MisnamedFileChecksumGenerator : public FileChecksumGenCrc32c {
   public:
    MisnamedFileChecksumGenerator(const FileChecksumGenContext& context)
        : FileChecksumGenCrc32c(context) {}

    const char* Name() const override { return "sha1"; }
  };

  class MisnamedFileChecksumGenFactory : public FileChecksumGenCrc32cFactory {
   public:
    std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
        const FileChecksumGenContext& context) override {
      return std::unique_ptr<FileChecksumGenerator>(
          new MisnamedFileChecksumGenerator(context));
    }
  };

  options.file_checksum_gen_factory.reset(new MisnamedFileChecksumGenFactory());
  Reopen(options);
  ASSERT_TRUE(db_->VerifyFileChecksums(ReadOptions()).IsInvalidArgument());
}

// TODO: re-enable after we provide finer-grained control for WAL tracking to
// meet the needs of different use cases, durability levels and recovery modes.
TEST_F(DBBasicTest, DISABLED_ManualWalSync) {
  Options options = CurrentOptions();
  options.track_and_verify_wals_in_manifest = true;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  DestroyAndReopen(options);

  ASSERT_OK(Put("x", "y"));
  // This does not create a new WAL.
  ASSERT_OK(db_->SyncWAL());
  EXPECT_FALSE(dbfull()->GetVersionSet()->GetWalSet().GetWals().empty());

  std::unique_ptr<LogFile> wal;
  Status s = db_->GetCurrentWalFile(&wal);
  ASSERT_OK(s);
  Close();

  EXPECT_OK(env_->DeleteFile(LogFileName(dbname_, wal->LogNumber())));

  ASSERT_TRUE(TryReopen(options).IsCorruption());
}
#endif  // !ROCKSDB_LITE

// A test class for intercepting random reads and injecting artificial
// delays. Used for testing the deadline/timeout feature
class DBBasicTestDeadline
    : public DBBasicTest,
      public testing::WithParamInterface<std::tuple<bool, bool>> {};

TEST_P(DBBasicTestDeadline, PointLookupDeadline) {
  std::shared_ptr<DeadlineFS> fs = std::make_shared<DeadlineFS>(env_, true);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  bool set_deadline = std::get<0>(GetParam());
  bool set_timeout = std::get<1>(GetParam());

  for (int option_config = kDefault; option_config < kEnd; ++option_config) {
    if (ShouldSkipOptions(option_config, kSkipPlainTable | kSkipMmapReads)) {
      continue;
    }
    option_config_ = option_config;
    Options options = CurrentOptions();
    if (options.use_direct_reads) {
      continue;
    }
    options.env = env.get();
    options.disable_auto_compactions = true;
    Cache* block_cache = nullptr;
    // Fileter block reads currently don't cause the request to get
    // aborted on a read timeout, so its possible those block reads
    // may get issued even if the deadline is past
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTable::Get:BeforeFilterMatch",
        [&](void* /*arg*/) { fs->IgnoreDeadline(true); });
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTable::Get:AfterFilterMatch",
        [&](void* /*arg*/) { fs->IgnoreDeadline(false); });
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

    SetTimeElapseOnlySleepOnReopen(&options);
    Reopen(options);

    if (options.table_factory) {
      block_cache = options.table_factory->GetOptions<Cache>(
          TableFactory::kBlockCacheOpts());
    }

    Random rnd(301);
    for (int i = 0; i < 400; ++i) {
      std::string key = "k" + std::to_string(i);
      ASSERT_OK(Put(key, rnd.RandomString(100)));
    }
    ASSERT_OK(Flush());

    bool timedout = true;
    // A timeout will be forced when the IO counter reaches this value
    int io_deadline_trigger = 0;
    // Keep incrementing io_deadline_trigger and call Get() until there is an
    // iteration that doesn't cause a timeout. This ensures that we cover
    // all file reads in the point lookup path that can potentially timeout
    // and cause the Get() to fail.
    while (timedout) {
      ReadOptions ro;
      if (set_deadline) {
        ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
      }
      if (set_timeout) {
        ro.io_timeout = std::chrono::microseconds{5000};
      }
      fs->SetDelayTrigger(ro.deadline, ro.io_timeout, io_deadline_trigger);

      block_cache->SetCapacity(0);
      block_cache->SetCapacity(1048576);

      std::string value;
      Status s = dbfull()->Get(ro, "k50", &value);
      if (fs->TimedOut()) {
        ASSERT_EQ(s, Status::TimedOut());
      } else {
        timedout = false;
        ASSERT_OK(s);
      }
      io_deadline_trigger++;
    }
    // Reset the delay sequence in order to avoid false alarms during Reopen
    fs->SetDelayTrigger(std::chrono::microseconds::zero(),
                        std::chrono::microseconds::zero(), 0);
  }
  Close();
}

TEST_P(DBBasicTestDeadline, IteratorDeadline) {
  std::shared_ptr<DeadlineFS> fs = std::make_shared<DeadlineFS>(env_, true);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  bool set_deadline = std::get<0>(GetParam());
  bool set_timeout = std::get<1>(GetParam());

  for (int option_config = kDefault; option_config < kEnd; ++option_config) {
    if (ShouldSkipOptions(option_config, kSkipPlainTable | kSkipMmapReads)) {
      continue;
    }
    Options options = CurrentOptions();
    if (options.use_direct_reads) {
      continue;
    }
    options.env = env.get();
    options.disable_auto_compactions = true;
    Cache* block_cache = nullptr;
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

    SetTimeElapseOnlySleepOnReopen(&options);
    Reopen(options);

    if (options.table_factory) {
      block_cache = options.table_factory->GetOptions<Cache>(
          TableFactory::kBlockCacheOpts());
    }

    Random rnd(301);
    for (int i = 0; i < 400; ++i) {
      std::string key = "k" + std::to_string(i);
      ASSERT_OK(Put(key, rnd.RandomString(100)));
    }
    ASSERT_OK(Flush());

    bool timedout = true;
    // A timeout will be forced when the IO counter reaches this value
    int io_deadline_trigger = 0;
    // Keep incrementing io_deadline_trigger and call Get() until there is an
    // iteration that doesn't cause a timeout. This ensures that we cover
    // all file reads in the point lookup path that can potentially timeout
    while (timedout) {
      ReadOptions ro;
      if (set_deadline) {
        ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
      }
      if (set_timeout) {
        ro.io_timeout = std::chrono::microseconds{5000};
      }
      fs->SetDelayTrigger(ro.deadline, ro.io_timeout, io_deadline_trigger);

      block_cache->SetCapacity(0);
      block_cache->SetCapacity(1048576);

      Iterator* iter = dbfull()->NewIterator(ro);
      int count = 0;
      iter->Seek("k50");
      while (iter->Valid() && count++ < 100) {
        iter->Next();
      }
      if (fs->TimedOut()) {
        ASSERT_FALSE(iter->Valid());
        ASSERT_EQ(iter->status(), Status::TimedOut());
      } else {
        timedout = false;
        ASSERT_OK(iter->status());
      }
      delete iter;
      io_deadline_trigger++;
    }
    // Reset the delay sequence in order to avoid false alarms during Reopen
    fs->SetDelayTrigger(std::chrono::microseconds::zero(),
                        std::chrono::microseconds::zero(), 0);
  }
  Close();
}

// Param 0: If true, set read_options.deadline
// Param 1: If true, set read_options.io_timeout
INSTANTIATE_TEST_CASE_P(DBBasicTestDeadline, DBBasicTestDeadline,
                        ::testing::Values(std::make_tuple(true, false),
                                          std::make_tuple(false, true),
                                          std::make_tuple(true, true)));
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
