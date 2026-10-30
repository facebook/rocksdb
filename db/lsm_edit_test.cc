//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/lsm_edit.h"

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/wide_columns.h"
#include "test_util/testharness.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

namespace {
class SnapshotOnFileIOListener : public EventListener {
 public:
  bool ShouldBeNotifiedOnFileIO() override { return true; }
  void OnFileFlushFinish(const FileOperationInfo& info) override {
    if (kind == 0 && info.path.find("MANIFEST") != std::string::npos) {
      Capture(info.path, info.start_ts.time_since_epoch().count());
    }
  }
  void OnFileReadFinish(const FileOperationInfo& info) override {
    if (kind == 1 && info.path.find(".sst") != std::string::npos) {
      Capture(info.path, info.start_ts.time_since_epoch().count());
    }
  }
  void OnIOError(const IOErrorInfo& info) override {
    if (kind == 2 && info.file_path.find("MANIFEST") != std::string::npos) {
      Capture(info.file_path, 1);
    }
  }
  void OnExternalFileIngested(DB*, const ExternalFileIngestionInfo&) override {
    if (kind != 2) {
      EXPECT_EQ(1, calls.load());
    }
  }

  std::atomic<DB*> db{nullptr};
  std::atomic<bool> armed{false};
  std::atomic<int> calls{0};
  int kind = 0;
  std::string observed;
  std::string path;

 private:
  void Capture(const std::string& event_path, int64_t event_time) {
    DB* source = db.load();
    if (source == nullptr || !armed.exchange(false)) {
      return;
    }
    EXPECT_GT(event_time, 0);
    path = event_path;
    const Snapshot* snapshot = source->GetSnapshot();
    EXPECT_NE(snapshot, nullptr);
    if (snapshot) {
      ReadOptions read;
      read.snapshot = snapshot;
      Status status = source->Get(read, "m1", &observed);
      if (status.IsNotFound()) {
        observed = "NOT_FOUND";
      } else {
        EXPECT_OK(status);
      }
      source->ReleaseSnapshot(snapshot);
    }
    ++calls;
  }
};

class OrderedFileIOListener : public EventListener {
 public:
  bool ShouldBeNotifiedOnFileIO() override { return true; }
  void OnFileFlushFinish(const FileOperationInfo& info) override {
    if (!armed.load() || info.path.find("MANIFEST") == std::string::npos ||
        !info.status.ok() ||
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            info.start_ts.time_since_epoch())
                .count() < arm_time.load()) {
      return;
    }
    const Snapshot* snapshot = db->GetSnapshot();
    EXPECT_NE(nullptr, snapshot);
    if (snapshot) {
      db->ReleaseSnapshot(snapshot);
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (events.empty() || events.back() != "io") {
      events.push_back("io");
    }
  }
  void OnIOError(const IOErrorInfo& info) override {
    if (armed.load() && info.file_path.find("MANIFEST") != std::string::npos) {
      ++errors;
    }
  }
  void OnExternalFileIngested(DB*,
                              const ExternalFileIngestionInfo& info) override {
    if (armed.load()) {
      std::lock_guard<std::mutex> lock(mutex);
      events.push_back(info.external_file_path);
    }
  }
  std::vector<std::string> Events() {
    std::lock_guard<std::mutex> lock(mutex);
    return events;
  }

  DB* db = nullptr;
  std::atomic<bool> armed{false};
  std::atomic<int64_t> arm_time{0};
  std::atomic<int> errors{0};

 private:
  std::mutex mutex;
  std::vector<std::string> events;
};

class EditOnFileIOListener : public EventListener {
 public:
  bool ShouldBeNotifiedOnFileIO() override { return true; }
  void OnFileReadFinish(const FileOperationInfo& info) override {
    if (kind == 0 && info.path.find(".sst") != std::string::npos) {
      Apply();
    }
  }
  void OnFileFlushFinish(const FileOperationInfo& info) override {
    if ((kind == 1 || kind == 3) &&
        info.path.find("MANIFEST") != std::string::npos) {
      Apply();
    }
  }
  void OnIOError(const IOErrorInfo& info) override {
    if (kind == 2 && info.file_path.find("MANIFEST") != std::string::npos) {
      Apply();
    }
  }

  DB* db = nullptr;
  LsmEdit edit;
  Status result;
  std::atomic<bool> armed{false};
  std::atomic<int> calls{0};
  int kind = 0;

 private:
  void Apply() {
    if (armed.exchange(false)) {
      result = db->ApplyLsmEdit(LsmEditOptions(), {edit});
      ++calls;
    }
  }
};
}  // namespace

class LsmEditTest : public DBTestBase {
 public:
  LsmEditTest() : DBTestBase("lsm_edit_test", /*env_do_fsync=*/true) {
    sst_files_dir_ = dbname_ + "_sst_files/";
    EXPECT_OK(DestroyDir(env_, sst_files_dir_));
    EXPECT_OK(env_->CreateDir(sst_files_dir_));
  }

  ~LsmEditTest() override {
    EXPECT_OK(DestroyDir(env_, sst_files_dir_));
    for (const auto& name : source_dbnames_) {
      EXPECT_OK(DestroyDB(name, CurrentOptions()));
    }
  }

  // Writes `kvs` to a standalone SST file and returns an LsmEditFile that
  // installs it at `level`.
  LsmEditFile MakeFile(const std::string& name, int level,
                       const std::map<std::string, std::string>& kvs) {
    LsmEditFile file;
    file.path = sst_files_dir_ + name;
    file.level = level;
    SstFileWriter writer(EnvOptions(), CurrentOptions());
    EXPECT_OK(writer.Open(file.path));
    for (const auto& [key, value] : kvs) {
      EXPECT_OK(writer.Put(key, value));
    }
    EXPECT_OK(writer.Finish());
    return file;
  }

  LsmEditFile MakeRangeDeletionFile(const std::string& name, int level,
                                    const Slice& start, const Slice& limit) {
    LsmEditFile file;
    file.path = sst_files_dir_ + name;
    file.level = level;
    SstFileWriter writer(EnvOptions(), CurrentOptions());
    EXPECT_OK(writer.Open(file.path));
    EXPECT_OK(writer.DeleteRange(start, limit));
    EXPECT_OK(writer.Finish());
    return file;
  }

  LsmEditFile MakePointFile(const std::string& name, int level, ValueType type,
                            const std::string& key, const std::string& value) {
    LsmEditFile file;
    file.path = sst_files_dir_ + name;
    file.level = level;
    SstFileWriter writer(EnvOptions(), CurrentOptions());
    EXPECT_OK(writer.Open(file.path));
    if (type == kTypeMerge) {
      EXPECT_OK(writer.Merge(key, value));
    } else if (type == kTypeDeletion) {
      EXPECT_OK(writer.Delete(key));
    } else {
      EXPECT_OK(writer.Put(key, value));
    }
    EXPECT_OK(writer.Finish());
    return file;
  }

  void ExpectVisibleEntries(
      const std::vector<std::pair<std::string, std::string>>& expected) {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    std::vector<std::pair<std::string, std::string>> actual;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      actual.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ(expected, actual);
    actual.clear();
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      actual.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ((std::vector<std::pair<std::string, std::string>>(
                  expected.rbegin(), expected.rend())),
              actual);
    for (size_t i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(expected[i].second, Get(expected[i].first));
      iter->Seek(expected[i].first);
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(expected[i].second, iter->value().ToString());
      iter->Next();
      if (i + 1 < expected.size()) {
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(expected[i + 1].first, iter->key().ToString());
        iter->Prev();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(expected[i].first, iter->key().ToString());
      } else {
        EXPECT_FALSE(iter->Valid());
      }
      iter->SeekForPrev(expected[i].first);
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(expected[i].second, iter->value().ToString());
      iter->Prev();
      if (i > 0) {
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(expected[i - 1].first, iter->key().ToString());
        iter->Next();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(expected[i].first, iter->key().ToString());
      } else {
        EXPECT_FALSE(iter->Valid());
      }
      ASSERT_OK(iter->status());
    }
  }

  LsmEditFile MakeDbFile(const std::string& name, int level,
                         const std::map<std::string, std::string>& kvs,
                         bool compact = false) {
    const std::string source_name = dbname_ + "_source_" + name;
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.disable_auto_compactions = true;
    EXPECT_OK(DestroyDB(source_name, options));
    source_dbnames_.push_back(source_name);
    std::unique_ptr<DB> source;
    EXPECT_OK(DB::Open(options, source_name, &source));
    if (!source) {
      return {};
    }
    for (const auto& [key, value] : kvs) {
      EXPECT_OK(source->Put(WriteOptions(), key, value));
    }
    EXPECT_OK(source->Flush(FlushOptions()));
    if (compact) {
      CompactRangeOptions compact_options;
      compact_options.change_level = true;
      compact_options.target_level = options.num_levels - 1;
      compact_options.bottommost_level_compaction =
          BottommostLevelCompaction::kForce;
      EXPECT_OK(source->CompactRange(compact_options, nullptr, nullptr));
      TablePropertiesCollection properties;
      EXPECT_OK(source->GetPropertiesOfAllTables(&properties));
      for (const auto& [path, props] : properties) {
        EXPECT_EQ(0, props->key_largest_seqno);
      }
    }
    ColumnFamilyMetaData metadata;
    source->GetColumnFamilyMetaData(&metadata);
    LsmEditFile result;
    result.level = level;
    for (const auto& source_level : metadata.levels) {
      for (const auto& file : source_level.files) {
        EXPECT_TRUE(result.path.empty());
        result.path = file.db_path + file.name;
        if (!compact) {
          EXPECT_GT(file.largest_seqno, 0);
        }
      }
    }
    EXPECT_OK(source->Close());
    return result;
  }

  static LsmEdit MakeEdit(ColumnFamilyHandle* cfh, const Slice& start,
                          const Slice& limit,
                          std::vector<LsmEditFile> add_files) {
    LsmEdit edit;
    edit.column_family = cfh;
    edit.range = RangeOpt(start, limit);
    edit.add_files = std::move(add_files);
    return edit;
  }

  // "<level>:<file count>" for each non-empty level of the default column
  // family, e.g. "0:2 1:1".
  static std::string ShapeOf(DB* db) {
    ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);
    std::string shape;
    for (const auto& level : cf_meta.levels) {
      if (level.files.empty()) {
        continue;
      }
      if (!shape.empty()) {
        shape += " ";
      }
      shape += std::to_string(level.level) + ":" +
               std::to_string(level.files.size());
    }
    return shape;
  }

  // File numbers currently in the default column family, per level. Used to
  // show that an edit left existing files alone rather than rewriting them.
  std::vector<std::vector<uint64_t>> FileNumbersPerLevel() {
    ColumnFamilyMetaData cf_meta;
    db_->GetColumnFamilyMetaData(&cf_meta);
    std::vector<std::vector<uint64_t>> result;
    for (const auto& level : cf_meta.levels) {
      result.emplace_back();
      for (const auto& file : level.files) {
        result.back().push_back(file.file_number);
      }
    }
    return result;
  }

  static Status RunBounded(const std::function<Status()>& operation) {
    std::future<Status> result = std::async(std::launch::async, operation);
    return AwaitBounded(result);
  }

  static Status AwaitBounded(std::future<Status>& result) {
    if (result.wait_for(std::chrono::seconds(20)) !=
        std::future_status::ready) {
      ADD_FAILURE() << "LSM edit did not complete within the deadline";
      std::_Exit(1);
    }
    return result.get();
  }

  void ExerciseLiveDeletionReservation(int level, int files,
                                       bool manifest_failure);

  static void WaitBounded(std::condition_variable& cv,
                          std::unique_lock<std::mutex>& lock,
                          const std::function<bool()>& ready) {
    if (!cv.wait_for(lock, std::chrono::seconds(10), ready)) {
      ADD_FAILURE() << "LSM edit synchronization did not complete";
      std::_Exit(1);
    }
  }

 protected:
  std::string sst_files_dir_;
  std::vector<std::string> source_dbnames_;
};

// The MyRocks index-rebuild shape: install files into a key range that holds
// no data but is not aligned to the boundaries of the file covering it.
TEST_F(LsmEditTest, GraftIntoEmptyRangeUnderStraddlingFile) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // One bottom-level file spanning the whole key space, with a hole in the
  // middle where the edit will go.
  ASSERT_OK(Put("a1", "old_a"));
  ASSERT_OK(Put("z1", "old_z"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
  ASSERT_EQ(1, NumTableFilesAtLevel(6));

  // The straddling file sits at L6, so the graft declares L5. Two levels of
  // the source shape are preserved as two levels here.
  std::vector<LsmEditFile> files;
  files.push_back(MakeDbFile("graft_l5", 5, {{"m1", "new_m1"}}));
  files.push_back(MakeDbFile("graft_l4", 4, {{"m2", "new_m2"}}));
  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n", std::move(files))}));

  EXPECT_EQ("old_a", Get("a1"));
  EXPECT_EQ("old_z", Get("z1"));
  EXPECT_EQ("new_m1", Get("m1"));
  EXPECT_EQ("new_m2", Get("m2"));
  EXPECT_EQ("0,0,0,0,1,1,1", FilesPerLevel());

  // The straddling file was retained untouched: no rewrite of live data was
  // needed to make the range file-aligned.
  const std::vector<std::vector<uint64_t>> after = FileNumbersPerLevel();
  EXPECT_EQ(before[6], after[6]);
}

TEST_F(LsmEditTest, RejectsRangeThatStillHoldsKeys) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "old_a"));
  ASSERT_OK(Put("m5", "old_m5"));
  ASSERT_OK(Put("z1", "old_z"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeDbFile("graft", 5, {{"m1", "new_m1"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("range is not empty"));
  EXPECT_EQ("old_m5", Get("m5"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, RejectsRangeTombstoneReachingIntoRange) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "old_a"));
  ASSERT_OK(Put("z1", "old_z"));
  // Covers the edit range without leaving any point key in it. Grafted data
  // carries its own (lower) sequence numbers, so this tombstone would silently
  // delete it.
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "y"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeDbFile("graft", 5, {{"m1", "new_m1"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("range is not empty"));
}

TEST_F(LsmEditTest, RejectsStraddlingFileWhenProbeDisabled) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "old_a"));
  ASSERT_OK(Put("z1", "old_z"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  LsmEditOptions edit_options;
  edit_options.probe_straddling_files = false;
  const Status s = db_->ApplyLsmEdit(
      edit_options, {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                              {MakeDbFile("graft", 5, {{"m1", "v"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("partially overlaps"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementDeletesStraddlingKeys) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  const SequenceNumber last_seqno = db_->GetLatestSequenceNumber();

  LsmEditOptions edit_options;
  edit_options.link_files = true;
  edit_options.probe_straddling_files = false;
  ASSERT_OK(db_->ApplyLsmEdit(
      edit_options,
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("replacement.sst", 4, {{"m1", "new"}})})}));
  EXPECT_EQ(before[6], FileNumbersPerLevel()[6]);
  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("right", Get("z1"));

  std::vector<std::string> values;
  const auto statuses = db_->MultiGet(ReadOptions(), {"m1", "m5"}, &values);
  ASSERT_EQ(2, statuses.size());
  EXPECT_OK(statuses[0]);
  EXPECT_TRUE(statuses[1].IsNotFound());
  EXPECT_EQ("new", values[0]);
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    std::vector<std::string> entries;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      entries.push_back(iter->key().ToString() + ":" +
                        iter->value().ToString());
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ((std::vector<std::string>{"a1:left", "m1:new", "z1:right"}),
              entries);
  }

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(1, metadata.levels[4].files.size());
  ASSERT_EQ(1, metadata.levels[5].files.size());
  EXPECT_EQ(last_seqno + 1, metadata.levels[5].files[0].largest_seqno);
  EXPECT_EQ(last_seqno + 2, metadata.levels[4].files[0].largest_seqno);
  EXPECT_EQ(last_seqno + 2, db_->GetLatestSequenceNumber());

  Reopen(options);
  EXPECT_EQ(last_seqno + 2, db_->GetLatestSequenceNumber());
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  EXPECT_EQ("new", Get("m1"));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("right", Get("z1"));
  ASSERT_OK(Put("m1", "later"));
  EXPECT_EQ("later", Get("m1"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementRelocatesAboveTombstone) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  LsmEditOptions edit_options;
  edit_options.allow_replacement_file_relocation = true;
  edit_options.probe_straddling_files = false;
  ASSERT_OK(db_->ApplyLsmEdit(
      edit_options,
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("replacement.sst", 5, {{"m1", "new"}})})}));
  EXPECT_EQ(before[6], FileNumbersPerLevel()[6]);
  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("right", Get("z1"));
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(1, metadata.levels[4].files.size());
  ASSERT_EQ(1, metadata.levels[5].files.size());
  EXPECT_GT(metadata.levels[4].files[0].largest_seqno,
            metadata.levels[5].files[0].largest_seqno);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementRequiresRoomWithoutRelocation) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("replacement.sst", 5, {{"m1", "new"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos,
            s.ToString().find("allow_replacement_file_relocation"));
  EXPECT_EQ("old", Get("m5"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementRelocatesAboveFlushedStraddler) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  LsmEditOptions edit_options;
  edit_options.allow_replacement_file_relocation = true;
  ASSERT_OK(db_->ApplyLsmEdit(
      edit_options,
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("replacement.sst", 5, {{"m1", "new"}})})}));
  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("right", Get("z1"));
  EXPECT_EQ(3, NumTableFilesAtLevel(0));
  Reopen(options);
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementProbesWhenTombstoneCannotFit) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("replacement.sst", 5, {{"m1", "new"}})})}));
  EXPECT_EQ(before[6], FileNumbersPerLevel()[6]);
  EXPECT_EQ(0, NumTableFilesAtLevel(0));
  EXPECT_EQ(1, NumTableFilesAtLevel(5));
  EXPECT_EQ("new", Get("m1"));
}

TEST_F(LsmEditTest, ZeroSequenceRelocationPreservesRunOrderInLevelZero) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  LsmEditOptions edit_options;
  edit_options.allow_replacement_file_relocation = true;
  ASSERT_OK(db_->ApplyLsmEdit(
      edit_options, {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                              {MakeFile("upper.sst", 5, {{"m1", "upper"}}),
                               MakeFile("lower.sst", 6, {{"m1", "lower"}})})}));
  EXPECT_EQ("upper", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ("upper", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementOverridesStraddlingTombstone) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "y"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("replacement.sst", 4, {{"m1", "new"}})})}));
  EXPECT_EQ("new", Get("m1"));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("right", Get("z1"));
}

TEST_F(LsmEditTest, DbGeneratedZeroSequenceReplacementStillProbes) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeDbFile("compacted", 5, {{"m1", "new"}}, true)})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("range is not empty"));
  EXPECT_EQ("old", Get("m5"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, MixedSequenceReplacementStillProbes) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("zero.sst", 5, {{"m1", "new"}}),
                 MakeDbFile("nonzero", 4, {{"m2", "new"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("range is not empty"));
  EXPECT_EQ("old", Get("m5"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m2"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementOrdersDeclaredLevels) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("upper.sst", 5, {{"m1", "upper"}}),
                 MakeFile("lower.sst", 6, {{"m1", "lower"}}),
                 MakeFile("l0_old.sst", 0, {{"m1", "l0_old"}}),
                 MakeFile("l0_new.sst", 0, {{"m1", "l0_new"}})})}));
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  EXPECT_GT(metadata.levels[5].files[0].largest_seqno,
            metadata.levels[6].files[0].largest_seqno);
  EXPECT_EQ("l0_new", Get("m1"));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ("l0_new", Get("m1"));
}

TEST_F(LsmEditTest, RejectsDeclaredLevelOccupiedByRetainedFile) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "old_a"));
  ASSERT_OK(Put("z1", "old_z"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  // The retained file's key range still covers L6 across the edit range, so
  // the grafted file cannot share that level even though the range is empty.
  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(), {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                                  {MakeDbFile("graft", 6, {{"m1", "v"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("declares level 6"));
}

// A file the edit removes must not block a declared level.
TEST_F(LsmEditTest, DeclaredLevelReusesSpaceOfRemovedFile) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  LsmEdit initial;
  initial.column_family = db_->DefaultColumnFamily();
  initial.add_files = {
      MakeFile("left.sst", 6, {{"a1", "left"}}),
      MakeFile("old.sst", 6, {{"m1", "old_m1"}, {"m9", "old_m9"}}),
      MakeFile("right.sst", 6, {{"z1", "right"}}),
  };
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {std::move(initial)}));
  ASSERT_EQ(3, NumTableFilesAtLevel(6));

  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("graft.sst", 6, {{"m5", "new_m5"}})})}));

  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m9"));
  EXPECT_EQ("new_m5", Get("m5"));
  EXPECT_EQ("right", Get("z1"));
  EXPECT_EQ("0,0,0,0,0,0,3", FilesPerLevel());
}

TEST_F(LsmEditTest, ExplicitEditPreservesUnlistedAndStraddlingFiles) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  LsmEdit initial;
  initial.column_family = db_->DefaultColumnFamily();
  initial.range = RangeOpt("a", "zz");
  initial.add_files = {
      MakeFile("straddling.sst", 6, {{"a1", "left"}, {"z1", "right"}}),
      MakeFile("old_middle.sst", 5, {{"m1", "old_m1"}, {"m9", "old_m9"}}),
  };
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {std::move(initial)}));
  const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
  ASSERT_EQ(1, before[5].size());
  ASSERT_EQ(1, before[6].size());

  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.range = RangeOpt("m", "n");
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.delete_files.push_back({5, before[5][0]});
  edit.add_files.push_back(MakeFile("new_middle.sst", 5, {{"m5", "new_m5"}}));
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {std::move(edit)}));

  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m9"));
  EXPECT_EQ("new_m5", Get("m5"));
  EXPECT_EQ("right", Get("z1"));
  const std::vector<std::vector<uint64_t>> after = FileNumbersPerLevel();
  EXPECT_EQ(before[6], after[6]);
  ASSERT_EQ(1, after[5].size());
  EXPECT_NE(before[5][0], after[5][0]);
}

TEST_F(LsmEditTest, ExplicitDeletionOnly) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(), {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                                  {MakeFile("old.sst", 5, {{"m1", "old"}})})}));
  const uint64_t file_number = FileNumbersPerLevel()[5][0];

  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.range = RangeOpt("m", "n");
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.delete_files.push_back({5, file_number});
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {std::move(edit)}));

  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ(0, NumTableFilesAtLevel(5));
}

TEST_F(LsmEditTest, RejectsExplicitDeletionOutsideRangeAtomically) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "a", "b",
                {MakeFile("outside.sst", 5, {{"a1", "old"}})})}));
  const uint64_t file_number = FileNumbersPerLevel()[5][0];

  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.range = RangeOpt("m", "n");
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.delete_files.push_back({5, file_number});
  edit.add_files.push_back(MakeFile("new.sst", 6, {{"m1", "new"}}));
  const Status s = db_->ApplyLsmEdit(LsmEditOptions(), {std::move(edit)});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos,
            s.ToString().find("does not contain explicitly deleted file"));
  EXPECT_EQ("old", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, ReplaceWholeColumnFamily) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "old_a"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b1", "old_b"));
  ASSERT_OK(Flush());
  ASSERT_EQ(2, NumTableFilesAtLevel(0));

  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.add_files.push_back(MakeFile("all.sst", 6, {{"c1", "new_c"}}));
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));

  EXPECT_EQ("NOT_FOUND", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("b1"));
  EXPECT_EQ("new_c", Get("c1"));
  EXPECT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
}

TEST_F(LsmEditTest, AtomicAcrossColumnFamilies) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"other"}, options);

  std::vector<LsmEdit> edits;
  edits.push_back(MakeEdit(handles_[0], "m", "n",
                           {MakeFile("cf0.sst", 6, {{"m1", "v0"}})}));
  edits.push_back(MakeEdit(handles_[1], "m", "n",
                           {MakeFile("cf1.sst", 6, {{"m1", "v1"}})}));
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), edits));

  EXPECT_EQ("v0", Get(0, "m1"));
  EXPECT_EQ("v1", Get(1, "m1"));
}

// One bad edit leaves every column family untouched.
TEST_F(LsmEditTest, RollsBackAllColumnFamiliesOnFailure) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"other"}, options);

  // A file straddling the edit range with a key inside it, so this column
  // family's edit cannot be satisfied.
  ASSERT_OK(Put(1, "a1", "old_a"));
  ASSERT_OK(Put(1, "m5", "old_m5"));
  ASSERT_OK(Put(1, "z1", "old_z"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(6, 1);

  std::vector<LsmEdit> edits;
  edits.push_back(
      MakeEdit(handles_[0], "m", "n", {MakeFile("ok.sst", 6, {{"m1", "v0"}})}));
  edits.push_back(MakeEdit(handles_[1], "m", "n",
                           {MakeFile("bad.sst", 6, {{"m1", "v1"}})}));
  const Status s = db_->ApplyLsmEdit(LsmEditOptions(), edits);
  ASSERT_NOK(s);

  EXPECT_EQ("NOT_FOUND", Get(0, "m1"));
  EXPECT_EQ("NOT_FOUND", Get(1, "m1"));
  EXPECT_EQ("old_m5", Get(1, "m5"));
}

TEST_F(LsmEditTest, PreservesLevelZeroOrdering) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Two overlapping level 0 files: a shape file ingestion cannot express,
  // because it would have to assign them sequence numbers to disambiguate.
  std::vector<LsmEditFile> files;
  files.push_back(MakeFile("older.sst", 0, {{"m2", "older"}}));
  files.push_back(MakeFile("newer.sst", 0, {{"m1", "newer"}, {"m2", "newer"}}));
  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n", std::move(files))}));

  EXPECT_EQ(2, NumTableFilesAtLevel(0));
  EXPECT_EQ("newer", Get("m2"));
}

TEST_F(LsmEditTest, RejectsOverlappingFilesAtSameLevel) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  std::vector<LsmEditFile> files;
  files.push_back(MakeFile("one.sst", 6, {{"m1", "v"}, {"m5", "v"}}));
  files.push_back(MakeFile("two.sst", 6, {{"m3", "v"}}));
  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n", std::move(files))});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("overlap each other"));
}

TEST_F(LsmEditTest, RejectsOlderSequenceNumberInHigherLevel) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string src_dbname = dbname_ + "_source";
  ASSERT_OK(DestroyDB(src_dbname, options));
  std::string older_file_path;
  std::string newer_file_path;
  {
    Options src_options = options;
    src_options.create_if_missing = true;
    std::unique_ptr<DB> src;
    ASSERT_OK(DB::Open(src_options, src_dbname, &src));
    assert(src != nullptr);
    DB& source = *src;

    ASSERT_OK(source.Put(WriteOptions(), "m1", "older"));
    ASSERT_OK(source.Flush(FlushOptions()));
    ColumnFamilyMetaData src_meta;
    source.GetColumnFamilyMetaData(&src_meta);
    ASSERT_EQ(1, src_meta.levels[0].files.size());
    older_file_path =
        src_meta.levels[0].files[0].db_path + src_meta.levels[0].files[0].name;

    ASSERT_OK(source.Put(WriteOptions(), "m1", "newer"));
    ASSERT_OK(source.Flush(FlushOptions()));
    source.GetColumnFamilyMetaData(&src_meta);
    ASSERT_EQ(2, src_meta.levels[0].files.size());
    newer_file_path =
        src_meta.levels[0].files[0].db_path + src_meta.levels[0].files[0].name;
    ASSERT_OK(source.Close());
  }

  LsmEditFile older_file;
  older_file.path = std::move(older_file_path);
  older_file.level = 5;
  LsmEditFile newer_file;
  newer_file.path = std::move(newer_file_path);
  newer_file.level = 6;
  LsmEditOptions edit_options;
  edit_options.link_files = true;
  const Status s = db_->ApplyLsmEdit(
      edit_options, {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                              {std::move(older_file), std::move(newer_file)})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos,
            s.ToString().find("invalid sequence number ordering"));
}

TEST_F(LsmEditTest, RejectsOlderAddedFileAboveSurvivingFile) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("m1", "survivor"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                          {MakeFile("older.sst", 5, {{"m1", "older"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  const Status s = db_->ApplyLsmEdit(LsmEditOptions(), {std::move(edit)});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos,
            s.ToString().find("invalid sequence number ordering"));
  EXPECT_EQ("survivor", Get("m1"));
}

TEST_F(LsmEditTest, AllowsInterleavedSequenceRangesForDistinctKeys) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string src_dbname = dbname_ + "_source";
  ASSERT_OK(DestroyDB(src_dbname, options));
  std::string wide_file_path;
  std::string middle_file_path;
  {
    Options src_options = options;
    src_options.create_if_missing = true;
    std::unique_ptr<DB> src;
    ASSERT_OK(DB::Open(src_options, src_dbname, &src));
    assert(src != nullptr);
    DB& source = *src;

    ASSERT_OK(source.Put(WriteOptions(), "a1", "left"));
    ASSERT_OK(source.Put(WriteOptions(), "z1", "right"));
    ASSERT_OK(source.Flush(FlushOptions()));
    ColumnFamilyMetaData src_meta;
    source.GetColumnFamilyMetaData(&src_meta);
    ASSERT_EQ(1, src_meta.levels[0].files.size());
    wide_file_path =
        src_meta.levels[0].files[0].db_path + src_meta.levels[0].files[0].name;

    ASSERT_OK(source.Put(WriteOptions(), "m1", "middle"));
    ASSERT_OK(source.Flush(FlushOptions()));
    source.GetColumnFamilyMetaData(&src_meta);
    ASSERT_EQ(2, src_meta.levels[0].files.size());
    middle_file_path =
        src_meta.levels[0].files[0].db_path + src_meta.levels[0].files[0].name;
    ASSERT_OK(source.Close());
  }

  LsmEditFile wide_file;
  wide_file.path = std::move(wide_file_path);
  wide_file.level = 5;
  LsmEditFile middle_file;
  middle_file.path = std::move(middle_file_path);
  middle_file.level = 6;
  LsmEditOptions edit_options;
  edit_options.link_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(
      edit_options,
      {MakeEdit(db_->DefaultColumnFamily(), "a", "zz",
                {std::move(wide_file), std::move(middle_file)})}));
  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("middle", Get("m1"));
  EXPECT_EQ("right", Get("z1"));
}

TEST_F(LsmEditTest, RejectsOlderRangeTombstoneInHigherLevel) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string src_dbname = dbname_ + "_source";
  ASSERT_OK(DestroyDB(src_dbname, options));
  std::string tombstone_file_path;
  std::string point_file_path;
  {
    Options src_options = options;
    src_options.create_if_missing = true;
    std::unique_ptr<DB> src;
    ASSERT_OK(DB::Open(src_options, src_dbname, &src));
    assert(src != nullptr);
    DB& source = *src;

    ASSERT_OK(source.DeleteRange(WriteOptions(), source.DefaultColumnFamily(),
                                 "m", "n"));
    ASSERT_OK(source.Flush(FlushOptions()));
    ColumnFamilyMetaData src_meta;
    source.GetColumnFamilyMetaData(&src_meta);
    ASSERT_EQ(1, src_meta.levels[0].files.size());
    tombstone_file_path =
        src_meta.levels[0].files[0].db_path + src_meta.levels[0].files[0].name;

    ASSERT_OK(source.Put(WriteOptions(), "m1", "newer"));
    ASSERT_OK(source.Flush(FlushOptions()));
    source.GetColumnFamilyMetaData(&src_meta);
    ASSERT_EQ(2, src_meta.levels[0].files.size());
    point_file_path =
        src_meta.levels[0].files[0].db_path + src_meta.levels[0].files[0].name;
    ASSERT_OK(source.Close());
  }

  LsmEditFile tombstone_file;
  tombstone_file.path = std::move(tombstone_file_path);
  tombstone_file.level = 5;
  LsmEditFile point_file;
  point_file.path = std::move(point_file_path);
  point_file.level = 6;
  LsmEditOptions edit_options;
  edit_options.link_files = true;
  const Status s = db_->ApplyLsmEdit(
      edit_options,
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {std::move(tombstone_file), std::move(point_file)})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos,
            s.ToString().find("invalid sequence number ordering"));
}

TEST_F(LsmEditTest, AllowsZeroSequenceNumberAcrossLevels) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                          {MakeFile("upper.sst", 5, {{"m1", "upper"}}),
                           MakeFile("lower.sst", 6, {{"m1", "lower"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
  EXPECT_EQ("upper", Get("m1"));
}

TEST_F(LsmEditTest, LiveTablePropertiesAvoidEqualZeroSequenceScan) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.max_open_files = 10;
  DestroyAndReopen(options);
  LsmEdit old = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                         {MakeFile("properties_old.sst", 6, {{"m", "old"}})});
  old.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {old}));
  Reopen(options);
  int skipped = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::SurvivingFileSequenceOrder:SkipScan",
      [&](void* arg) {
        if (*static_cast<bool*>(arg)) {
          ++skipped;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  LsmEdit newer = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                           {MakeFile("properties_new.sst", 5, {{"m", "new"}})});
  newer.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {newer}));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_EQ(1, skipped);
  ExpectVisibleEntries({{"m", "new"}});
  Reopen(options);
  ExpectVisibleEntries({{"m", "new"}});
}

TEST_F(LsmEditTest, EqualZeroPointRunsHaveStableReadAndCompactionPriority) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  for (int scenario = 0; scenario < 6; ++scenario) {
    SCOPED_TRACE(scenario);
    DestroyAndReopen(options);
    const ValueType upper_type = scenario == 2   ? kTypeDeletion
                                 : scenario == 3 ? kTypeMerge
                                                 : kTypeValue;
    const ValueType lower_type = scenario == 4 ? kTypeMerge : kTypeValue;
    LsmEditFile upper =
        MakePointFile("scan_upper.sst", 4, upper_type, "m", "new");
    LsmEditFile lower =
        MakePointFile("scan_lower.sst", 6, lower_type, "m", "old");
    std::vector<LsmEditFile> files{upper, lower};
    if (scenario != 1) {
      files.push_back(MakeFile("scan_prefix.sst", 5, {{"a", "prefix"}}));
    }
    if (scenario == 5) {
      for (LsmEditFile& file : files) {
        file.level = 0;
      }
      std::swap(files[0], files[1]);
    }
    LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "a", "n", files);
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
    std::vector<std::pair<std::string, std::string>> expected;
    if (scenario != 1) {
      expected.emplace_back("a", "prefix");
    }
    if (scenario != 2) {
      expected.emplace_back("m", scenario == 3 ? "old,new" : "new");
    }
    ExpectVisibleEntries(expected);
    Reopen(options);
    ExpectVisibleEntries(expected);
    CompactRangeOptions compact;
    compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
    ExpectVisibleEntries(expected);
  }
}

TEST_F(LsmEditTest, WidePointAndRangeStartKeepPriorityAfterCompaction) {
  Options options = CurrentOptions();
  options.num_levels = 4;
  options.disable_auto_compactions = true;
  for (bool level_zero : {false, true}) {
    for (bool delete_winner : {false, true}) {
      SCOPED_TRACE(level_zero);
      SCOPED_TRACE(delete_winner);
      DestroyAndReopen(options);
      LsmEditFile old;
      old.path = sst_files_dir_ + "wide_old.sst";
      old.level = level_zero ? 0 : 3;
      SstFileWriter writer(EnvOptions(), options);
      ASSERT_OK(writer.Open(old.path));
      ASSERT_OK(writer.PutEntity("a", {{kDefaultWideColumnName, "prefix"}}));
      ASSERT_OK(writer.PutEntity("m", {{kDefaultWideColumnName, "old"}}));
      ASSERT_OK(writer.DeleteRange("m", "z"));
      ASSERT_OK(writer.Finish());
      const LsmEditFile middle =
          MakeFile("wide_middle.sst", level_zero ? 0 : 2, {{"m", "middle"}});
      const LsmEditFile newest =
          MakePointFile("wide_new.sst", level_zero ? 0 : 1,
                        delete_winner ? kTypeDeletion : kTypeValue, "m", "new");
      LsmEdit edit;
      edit.column_family = db_->DefaultColumnFamily();
      edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      edit.add_files = level_zero
                           ? std::vector<LsmEditFile>{old, middle, newest}
                           : std::vector<LsmEditFile>{newest, middle, old};
      ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
      std::vector<std::pair<std::string, std::string>> expected{
          {"a", "prefix"}};
      if (!delete_winner) {
        expected.emplace_back("m", "new");
      }
      const auto& verify = [&]() {
        ExpectVisibleEntries(expected);
        EXPECT_EQ(delete_winner ? "NOT_FOUND" : "new", Get("m"));
      };
      verify();
      ColumnFamilyMetaData metadata;
      db_->GetColumnFamilyMetaData(&metadata);
      std::vector<std::string> names;
      for (const auto& level : metadata.levels) {
        for (const auto& file : level.files) {
          names.push_back(file.name);
        }
      }
      ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                                  db_->DefaultColumnFamily(), names, 3));
      verify();
      Reopen(options);
      verify();
    }
  }
}

TEST_F(LsmEditTest, UnresolvedEqualSequenceOperandsFailWithoutLosingInputs) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  for (bool with_base : {false, true}) {
    SCOPED_TRACE(with_base);
    DestroyAndReopen(options);
    LsmEdit edit;
    edit.column_family = db_->DefaultColumnFamily();
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    if (with_base) {
      edit.add_files.push_back(
          MakeFile("operand_base.sst", 5, {{"m1", "base"}}));
    }
    edit.add_files.push_back(
        MakePointFile("operand_old.sst", 5, kTypeMerge, "m1", "old"));
    edit.add_files.push_back(
        MakePointFile("operand_new.sst", 5, kTypeMerge, "m1", "new"));
    LsmEditOptions edit_options;
    edit_options.merge_incoming_files = true;
    edit_options.move_files = true;
    const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
    const Status status = db_->ApplyLsmEdit(edit_options, {edit});
    if (with_base) {
      ASSERT_OK(status);
      ExpectVisibleEntries({{"m1", "base,old,new"}});
      Reopen(options);
      ExpectVisibleEntries({{"m1", "base,old,new"}});
    } else {
      ASSERT_TRUE(status.IsNotSupported()) << status.ToString();
      EXPECT_EQ(before, FileNumbersPerLevel());
      EXPECT_EQ("NOT_FOUND", Get("m1"));
      for (const LsmEditFile& file : edit.add_files) {
        ASSERT_OK(env_->FileExists(file.path));
      }
      ASSERT_OK(Put("m1", "fresh"));
      EXPECT_EQ("fresh", Get("m1"));
    }
  }
}

TEST_F(LsmEditTest, EqualSequenceMergeOperandsCannotRemainInSeparateRuns) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  for (bool survivor : {false, true}) {
    for (bool merged_outputs : {false, true}) {
      SCOPED_TRACE(survivor);
      SCOPED_TRACE(merged_outputs);
      DestroyAndReopen(options);
      const LsmEditFile old =
          MakePointFile("separate_old.sst", 6, kTypeMerge, "m1", "old");
      if (survivor) {
        LsmEdit first = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {old});
        first.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
        ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {first}));
        EXPECT_EQ("old", Get("m1"));
      }
      const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
      const LsmEditFile newest =
          MakePointFile("separate_new.sst", 5, kTypeMerge, "m1", "new");
      LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                              survivor ? std::vector<LsmEditFile>{newest}
                                       : std::vector<LsmEditFile>{newest, old});
      edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      LsmEditOptions edit_options;
      edit_options.merge_incoming_files = merged_outputs;
      const Status status = db_->ApplyLsmEdit(edit_options, {edit});
      ASSERT_TRUE(status.IsNotSupported()) << status.ToString();
      EXPECT_EQ(before, FileNumbersPerLevel());
      EXPECT_EQ(survivor ? "old" : "NOT_FOUND", Get("m1"));
    }
  }
}

TEST_F(LsmEditTest, FileIOCallbacksCanSnapshotAfterInstallationAndFailure) {
  for (int kind = 0; kind < 3; ++kind) {
    SCOPED_TRACE(kind);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.max_open_files = -1;
    const std::shared_ptr<SnapshotOnFileIOListener> listener =
        std::make_shared<SnapshotOnFileIOListener>();
    listener->kind = kind;
    options.listeners.push_back(listener);
    DestroyAndReopen(options);
    const LsmEdit edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 {MakeFile("callback_one.sst", 5, {{"m1", "new"}}),
                  MakeFile("callback_two.sst", 5, {{"m2", "other"}})});
    listener->db.store(db_.get());
    std::thread::id committing_thread;
    std::mutex state_mutex;
    std::condition_variable state_cv;
    int worker_opens = 0;
    auto* cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
          listener->armed.store(true);
          if (kind == 2) {
            env_->manifest_write_error_.store(true);
          }
        });
    if (kind == 1) {
      SyncPoint::GetInstance()->SetCallBack(
          "VersionBuilder::Rep::LoadTableHandlers::BeforeFindTable",
          [&](void* arg) {
            std::unique_lock<std::mutex> lock(state_mutex);
            if (std::this_thread::get_id() == committing_thread) {
              WaitBounded(state_cv, lock, [&]() { return worker_opens > 0; });
            } else {
              ++worker_opens;
              state_cv.notify_all();
            }
            auto* file = static_cast<FileMetaData*>(arg);
            TableCache::Evict(cfd->table_cache()->get_cache().get(),
                              file->fd.GetNumber());
          });
    }
    SyncPoint::GetInstance()->EnableProcessing();
    const Status status = RunBounded([&]() {
      committing_thread = std::this_thread::get_id();
      LsmEditOptions edit_options;
      edit_options.file_opening_threads = 4;
      return db_->ApplyLsmEdit(edit_options, {edit});
    });
    env_->manifest_write_error_.store(false);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_EQ(1, listener->calls.load());
    EXPECT_FALSE(listener->path.empty());
    if (kind == 2) {
      ASSERT_NOK(status);
      EXPECT_EQ("NOT_FOUND", listener->observed);
      EXPECT_EQ("NOT_FOUND", Get("m1"));
      listener->db.store(nullptr);
      Reopen(options);
      listener->db.store(db_.get());
    } else {
      ASSERT_OK(status);
      EXPECT_EQ("new", listener->observed);
      EXPECT_EQ("new", Get("m1"));
    }
    if (kind == 1) {
      EXPECT_GT(worker_opens, 0);
    }
    ASSERT_OK(Put("post", "write"));
    EXPECT_EQ("write", Get("post"));
    listener->db.store(nullptr);
  }
}

TEST_F(LsmEditTest, ConcurrentEditsDeliverTheirOwnFileIOBeforeCompletion) {
  for (bool manifest_failure : {false, true}) {
    SCOPED_TRACE(manifest_failure);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    const std::shared_ptr<OrderedFileIOListener> listener =
        std::make_shared<OrderedFileIOListener>();
    options.listeners.push_back(listener);
    DestroyAndReopen(options);
    listener->db = db_.get();
    LsmEdit first = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                             {MakeFile("first_io.sst", 5, {{"m1", "first"}})});
    LsmEdit second =
        MakeEdit(db_->DefaultColumnFamily(), "p", "q",
                 {MakeFile("second_io.sst", 5, {{"p1", "second"}})});
    std::mutex state_mutex;
    std::condition_variable state_cv;
    std::thread::id first_thread;
    std::thread::id second_thread;
    bool first_paused = false;
    bool second_attempted = false;
    bool first_finished = false;
    SyncPoint::GetInstance()->SetCallBack(
        "ScopedDeferredFileIOListeners::BeforeWaitForScopes", [&](void*) {
          std::lock_guard<std::mutex> lock(state_mutex);
          if (std::this_thread::get_id() == second_thread) {
            second_attempted = true;
            state_cv.notify_all();
          }
        });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeJobsRun:0", [&](void*) {
          std::unique_lock<std::mutex> lock(state_mutex);
          if (std::this_thread::get_id() == first_thread) {
            first_paused = true;
            state_cv.notify_all();
            WaitBounded(state_cv, lock, [&]() { return second_attempted; });
          } else if (std::this_thread::get_id() == second_thread) {
            WaitBounded(state_cv, lock, [&]() { return first_finished; });
          }
        });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
          if (std::this_thread::get_id() == first_thread) {
            listener->arm_time.store(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
            listener->armed.store(true);
          }
          if (manifest_failure && std::this_thread::get_id() == second_thread) {
            env_->manifest_write_error_.store(true);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    std::future<Status> first_result = std::async(std::launch::async, [&]() {
      {
        std::lock_guard<std::mutex> lock(state_mutex);
        first_thread = std::this_thread::get_id();
      }
      return db_->ApplyLsmEdit(LsmEditOptions(), {first});
    });
    {
      std::unique_lock<std::mutex> lock(state_mutex);
      WaitBounded(state_cv, lock, [&]() { return first_paused; });
    }
    std::future<Status> second_result = std::async(std::launch::async, [&]() {
      {
        std::lock_guard<std::mutex> lock(state_mutex);
        second_thread = std::this_thread::get_id();
      }
      return db_->ApplyLsmEdit(LsmEditOptions(), {second});
    });
    const Status first_status = AwaitBounded(first_result);
    EXPECT_EQ((std::vector<std::string>{"io", first.add_files[0].path}),
              listener->Events());
    {
      std::lock_guard<std::mutex> lock(state_mutex);
      first_finished = true;
      state_cv.notify_all();
    }
    const Status second_status = AwaitBounded(second_result);
    env_->manifest_write_error_.store(false);
    listener->armed.store(false);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(first_status);
    if (manifest_failure) {
      ASSERT_NOK(second_status);
      EXPECT_GT(listener->errors.load(), 0);
      EXPECT_EQ((std::vector<std::string>{"io", first.add_files[0].path}),
                listener->Events());
      ASSERT_OK(env_->FileExists(second.add_files[0].path));
      Reopen(options);
    } else {
      ASSERT_OK(second_status);
      EXPECT_EQ((std::vector<std::string>{"io", first.add_files[0].path, "io",
                                          second.add_files[0].path}),
                listener->Events());
    }
    EXPECT_EQ("first", Get("m1"));
    EXPECT_EQ(manifest_failure ? "NOT_FOUND" : "second", Get("p1"));
    ASSERT_OK(Put("post", "write"));
  }
}

TEST_F(LsmEditTest, DifferentListenersDoNotSerializeUnrelatedEdits) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.listeners.push_back(std::make_shared<OrderedFileIOListener>());
  DestroyAndReopen(options);
  Options other_options = options;
  other_options.listeners = {std::make_shared<OrderedFileIOListener>()};
  other_options.create_if_missing = true;
  const std::string other_name = dbname_ + "_independent_listener";
  source_dbnames_.push_back(other_name);
  ASSERT_OK(DestroyDB(other_name, other_options));
  std::unique_ptr<DB> other;
  ASSERT_OK(DB::Open(other_options, other_name, &other));
  const LsmEdit first =
      MakeEdit(db_->DefaultColumnFamily(), "m", "n",
               {MakeFile("independent_first.sst", 5, {{"m1", "first"}})});
  const LsmEdit second =
      MakeEdit(other->DefaultColumnFamily(), "p", "q",
               {MakeFile("independent_second.sst", 5, {{"p1", "second"}})});
  std::mutex state_mutex;
  std::condition_variable state_cv;
  std::thread::id first_thread;
  bool first_paused = false;
  bool release = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFiles:BeforeJobsRun:0", [&](void*) {
        std::unique_lock<std::mutex> lock(state_mutex);
        if (std::this_thread::get_id() == first_thread) {
          first_paused = true;
          state_cv.notify_all();
          WaitBounded(state_cv, lock, [&]() { return release; });
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::future<Status> first_result = std::async(std::launch::async, [&]() {
    {
      std::lock_guard<std::mutex> lock(state_mutex);
      first_thread = std::this_thread::get_id();
    }
    return db_->ApplyLsmEdit(LsmEditOptions(), {first});
  });
  {
    std::unique_lock<std::mutex> lock(state_mutex);
    WaitBounded(state_cv, lock, [&]() { return first_paused; });
  }
  const Status second_status = RunBounded(
      [&]() { return other->ApplyLsmEdit(LsmEditOptions(), {second}); });
  std::string value;
  EXPECT_OK(other->Get(ReadOptions(), "p1", &value));
  EXPECT_EQ("second", value);
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  {
    std::lock_guard<std::mutex> lock(state_mutex);
    release = true;
    state_cv.notify_all();
  }
  const Status first_status = AwaitBounded(first_result);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(first_status);
  ASSERT_OK(second_status);
  EXPECT_EQ("first", Get("m1"));
  ASSERT_OK(other->Close());
}

TEST_F(LsmEditTest, FileIOCallbacksRejectReentrantEditsWithoutHanging) {
  for (int kind = 0; kind < 4; ++kind) {
    SCOPED_TRACE(kind);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    const std::shared_ptr<EditOnFileIOListener> listener =
        std::make_shared<EditOnFileIOListener>();
    listener->kind = kind;
    options.listeners.push_back(listener);
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "seed"));
    ASSERT_OK(Flush());
    listener->db = db_.get();
    listener->edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 {MakeFile("reentrant_io.sst", 5, {{"m1", "imported"}})});
    listener->edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
    const LsmEdit outer =
        MakeEdit(db_->DefaultColumnFamily(), "p", "q",
                 {MakeFile("outer_io.sst", 5, {{"p1", "outer"}})});
    if (kind == 1 || kind == 2) {
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
            listener->armed.store(true);
            if (kind == 2) {
              env_->manifest_write_error_.store(true);
            }
          });
      SyncPoint::GetInstance()->EnableProcessing();
    }
    listener->armed.store(kind == 0 || kind == 3);
    std::string value;
    const Status status = RunBounded([&]() {
      if (kind == 0) {
        ReadOptions read;
        read.fill_cache = false;
        return db_->Get(read, "a", &value);
      }
      return db_->ApplyLsmEdit(LsmEditOptions(), {outer});
    });
    env_->manifest_write_error_.store(false);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_EQ(1, listener->calls.load());
    EXPECT_TRUE(listener->result.IsNotSupported())
        << listener->result.ToString();
    EXPECT_NE(std::string::npos,
              listener->result.ToString().find("file I/O callback"));
    ASSERT_OK(env_->FileExists(listener->edit.add_files[0].path));
    EXPECT_EQ("NOT_FOUND", Get("m1"));
    if (kind == 2) {
      ASSERT_NOK(status);
      EXPECT_EQ(before, FileNumbersPerLevel());
      Reopen(options);
      listener->db = db_.get();
      listener->edit.column_family = db_->DefaultColumnFamily();
    } else {
      ASSERT_OK(status);
      if (kind == 0) {
        EXPECT_EQ("seed", value);
        EXPECT_EQ(before, FileNumbersPerLevel());
      } else {
        EXPECT_EQ("outer", Get("p1"));
      }
    }
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {listener->edit}));
    EXPECT_EQ("imported", Get("m1"));
    ASSERT_OK(Put("post", "write"));
  }
}

TEST_F(LsmEditTest, DeletionJobDestructionDoesNotRaceSetOptions) {
  for (bool manifest_failure : {false, true}) {
    SCOPED_TRACE(manifest_failure);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.periodic_compaction_seconds = 0;
    DestroyAndReopen(options);
    ASSERT_OK(Put("m1", "old"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(2);
    ASSERT_OK(Put("z1", "keep"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(2);
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    ASSERT_EQ(2, metadata.levels[2].files.size());
    LsmEdit edit;
    edit.column_family = db_->DefaultColumnFamily();
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    edit.delete_files = {{2, metadata.levels[2].files.front().file_number}};
    std::mutex state_mutex;
    std::condition_variable state_cv;
    bool destructor_paused = false;
    bool updater_ready = false;
    bool release = false;
    SyncPoint::GetInstance()->SetCallBack(
        "FileIngestionHandleImpl::~FileIngestionHandleImpl:"
        "BeforeJobDestruction",
        [&](void*) {
          std::unique_lock<std::mutex> lock(state_mutex);
          destructor_paused = true;
          state_cv.notify_all();
          WaitBounded(state_cv, lock, [&]() { return release; });
        });
    if (manifest_failure) {
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::IngestExternalFiles:BeforeLogAndApply",
          [&](void*) { env_->manifest_write_error_.store(true); });
    }
    SyncPoint::GetInstance()->EnableProcessing();
    std::future<Status> edit_result = std::async(std::launch::async, [&]() {
      return db_->ApplyLsmEdit(LsmEditOptions(), {edit});
    });
    {
      std::unique_lock<std::mutex> lock(state_mutex);
      WaitBounded(state_cv, lock, [&]() { return destructor_paused; });
    }
    std::future<Status> options_result = std::async(std::launch::async, [&]() {
      {
        std::unique_lock<std::mutex> lock(state_mutex);
        updater_ready = true;
        state_cv.notify_all();
        WaitBounded(state_cv, lock, [&]() { return release; });
      }
      for (int i = 0; i < 16; ++i) {
        const Status status = db_->SetOptions(
            {{"periodic_compaction_seconds", std::to_string(100 + i)}});
        if (!status.ok()) {
          return status;
        }
      }
      return Status::OK();
    });
    {
      std::unique_lock<std::mutex> lock(state_mutex);
      WaitBounded(state_cv, lock, [&]() { return updater_ready; });
      release = true;
      state_cv.notify_all();
    }
    const Status edit_status = AwaitBounded(edit_result);
    const Status options_status = AwaitBounded(options_result);
    env_->manifest_write_error_.store(false);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(options_status);
    if (manifest_failure) {
      ASSERT_NOK(edit_status);
      Reopen(options);
    } else {
      ASSERT_OK(edit_status);
    }
    EXPECT_EQ(manifest_failure ? "old" : "NOT_FOUND", Get("m1"));
    EXPECT_EQ("keep", Get("z1"));
    ASSERT_OK(Put("post", "write"));
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    EXPECT_EQ("keep", Get("z1"));
  }
}

void LsmEditTest::ExerciseLiveDeletionReservation(int level, int files,
                                                  bool manifest_failure) {
  struct Scenario {
    int level;
    int files;
    bool manifest_failure;
  };
  const Scenario scenario{level, files, manifest_failure};
  {
    SCOPED_TRACE(scenario.level);
    SCOPED_TRACE(scenario.files);
    SCOPED_TRACE(scenario.manifest_failure);
    Options options = CurrentOptions();
    options.num_levels = 4;
    options.disable_auto_compactions = true;
    options.max_background_compactions = 1;
    options.compression = kNoCompression;
    options.compression_per_level.clear();
    options.level_compaction_dynamic_level_bytes = false;
    options.level0_file_num_compaction_trigger = scenario.files == 1 ? 1 : 4;
    options.max_bytes_for_level_base = 1000000000;
    options.periodic_compaction_seconds = 0;
    env_->SetMockSleep(true);
    DestroyAndReopen(options);
    if (scenario.level == 0) {
      ASSERT_OK(Put("a", "base"));
      ASSERT_OK(Flush());
      MoveFilesToLevel(1);
    }
    for (int i = 0; i < scenario.files; ++i) {
      ASSERT_OK(Put("m1", "v" + std::to_string(i)));
      ASSERT_OK(Flush());
    }
    if (scenario.level > 0) {
      MoveFilesToLevel(scenario.level);
      env_->MockSleepForMicroseconds(20000000);
      ASSERT_OK(db_->SetOptions({{"periodic_compaction_seconds", "10"}}));
    }
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    const uint64_t target =
        metadata.levels[scenario.level].files.back().file_number;
    LsmEdit edit;
    edit.column_family = db_->DefaultColumnFamily();
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    edit.delete_files = {{scenario.level, target}};
    std::mutex state_mutex;
    std::condition_variable state_cv;
    bool worker_queued = false;
    bool allow_picker = false;
    bool picker_finished = false;
    std::atomic<bool> installing{false};
    std::thread::id committing_thread;
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BGWorkCompaction", [&](void*) {
          std::unique_lock<std::mutex> lock(state_mutex);
          worker_queued = true;
          state_cv.notify_all();
          WaitBounded(state_cv, lock, [&]() { return allow_picker; });
        });
    const auto& picker_done = [&](void*) {
      std::lock_guard<std::mutex> lock(state_mutex);
      picker_finished = true;
      state_cv.notify_all();
    };
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BackgroundCompaction():AfterPickCompaction", picker_done);
    SyncPoint::GetInstance()->SetCallBack("BackgroundCallCompaction:1",
                                          picker_done);
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeLogAndApply",
        [&](void*) { installing.store(true); });
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::LogAndApply:WriteManifest", [&](void*) {
          if (!installing.load() ||
              std::this_thread::get_id() != committing_thread) {
            return;
          }
          std::unique_lock<std::mutex> lock(state_mutex);
          allow_picker = true;
          state_cv.notify_all();
          WaitBounded(state_cv, lock, [&]() { return picker_finished; });
          if (scenario.manifest_failure) {
            env_->manifest_write_error_.store(true);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(db_->EnableAutoCompaction({db_->DefaultColumnFamily()}));
    {
      std::unique_lock<std::mutex> lock(state_mutex);
      WaitBounded(state_cv, lock, [&]() { return worker_queued; });
    }
    const Status status = RunBounded([&]() {
      committing_thread = std::this_thread::get_id();
      return db_->ApplyLsmEdit(LsmEditOptions(), {edit});
    });
    env_->manifest_write_error_.store(false);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    if (scenario.manifest_failure) {
      ASSERT_NOK(status);
      uint64_t pending = 0;
      ASSERT_TRUE(db_->GetIntProperty("rocksdb.compaction-pending", &pending));
      EXPECT_EQ(1, pending);
      EXPECT_EQ("v0", Get("m1"));
      Reopen(options);
    } else {
      ASSERT_OK(status);
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    if (scenario.files > 1) {
      EXPECT_EQ("v5", Get("m1"));
    } else if (scenario.manifest_failure) {
      EXPECT_EQ("v0", Get("m1"));
    } else {
      EXPECT_EQ("NOT_FOUND", Get("m1"));
    }
    ASSERT_OK(Put("post", "write"));
    EXPECT_EQ("write", Get("post"));
  }
}

TEST_F(LsmEditTest, L0DeletionReservationsExcludeQueuedAutomaticPickers) {
  ExerciseLiveDeletionReservation(0, 1, false);
  ExerciseLiveDeletionReservation(0, 6, false);
}

TEST_F(LsmEditTest, PeriodicDeletionReservationsRefreshCandidatesAndRollback) {
  ExerciseLiveDeletionReservation(2, 1, false);
  ExerciseLiveDeletionReservation(2, 1, true);
}

TEST_F(LsmEditTest, EqualL0RangeTombstoneCannotHideAnOlderPoint) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (bool survivor : {false, true}) {
    DestroyAndReopen(options);
    LsmEditFile point = MakeFile("l0_point.sst", 0, {{"m1", "point"}});
    if (survivor) {
      LsmEdit first = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {point});
      first.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {first}));
    }
    const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
    LsmEditFile tombstone = MakeRangeDeletionFile("l0_range.sst", 0, "m", "n");
    LsmEdit edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 survivor ? std::vector<LsmEditFile>{tombstone}
                          : std::vector<LsmEditFile>{point, tombstone});
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    const Status status = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_EQ(before, FileNumbersPerLevel());
    EXPECT_EQ(survivor ? "point" : "NOT_FOUND", Get("m1"));
  }
}

TEST_F(LsmEditTest, UnknownSurvivorStatisticsCannotBypassTombstoneValidation) {
  for (int max_open_files : {20, -1}) {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.max_open_files = max_open_files;
    DestroyAndReopen(options);
    std::vector<LsmEditFile> files;
    for (int i = 0; i < 21; ++i) {
      const std::string key = "m" + std::to_string(100 + i);
      files.push_back(MakeFile(key + ".sst", 6, {{key, "point"}}));
    }
    LsmEdit first = MakeEdit(db_->DefaultColumnFamily(), "m", "n", files);
    first.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {first}));
    std::vector<std::vector<FileMetaData>> metadata;
    dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &metadata);
    std::string key = "m120";
    bool unknown = false;
    for (const FileMetaData& file : metadata[6]) {
      if (!file.init_stats_from_file) {
        key = file.smallest.user_key().ToString();
        unknown = true;
        break;
      }
    }
    if (max_open_files != -1) {
      ASSERT_TRUE(unknown);
    }
    const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
    LsmEdit edit = MakeEdit(
        db_->DefaultColumnFamily(), "m", "n",
        {MakeRangeDeletionFile("unknown_stats.sst", 5, key, key + "~")});
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    const Status status = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_EQ(before, FileNumbersPerLevel());
    EXPECT_EQ("point", Get(key));
  }
}

TEST_F(LsmEditTest, DeclaredLevelAllowsExclusiveRangeEndAdjacency) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (bool range_first : {false, true}) {
    DestroyAndReopen(options);
    const LsmEditFile point =
        MakeFile("adjacent_point.sst", 5, {{"m", "point"}});
    const LsmEditFile range =
        MakeRangeDeletionFile("adjacent_range.sst", 5, "a", "m");
    LsmEdit first = MakeEdit(db_->DefaultColumnFamily(), "a", "n",
                             {range_first ? range : point});
    first.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {first}));
    LsmEdit second = MakeEdit(db_->DefaultColumnFamily(), "a", "n",
                              {range_first ? point : range});
    second.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {second}));
    EXPECT_EQ(2, NumTableFilesAtLevel(5));
    EXPECT_EQ("point", Get("m"));
    const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
    LsmEdit overlap = MakeEdit(db_->DefaultColumnFamily(), "a", "n",
                               {MakeFile("interior.sst", 5, {{"b", "bad"}})});
    overlap.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    EXPECT_TRUE(
        db_->ApplyLsmEdit(LsmEditOptions(), {overlap}).IsInvalidArgument());
    EXPECT_EQ(before, FileNumbersPerLevel());
    EXPECT_EQ("NOT_FOUND", Get("b"));
  }
}

TEST_F(LsmEditTest,
       WritePreparedBlockingFlushDoesNotWaitOnItsOwnSnapshotFence) {
  Close();
  for (bool two_queues : {false, true}) {
    for (bool atomic_flush : {false, true}) {
      for (bool active_user_snapshot : {false, true}) {
        SCOPED_TRACE(two_queues);
        SCOPED_TRACE(atomic_flush);
        SCOPED_TRACE(active_user_snapshot);
        Options options = CurrentOptions();
        options.disable_auto_compactions = true;
        options.two_write_queues = two_queues;
        options.atomic_flush = atomic_flush;
        ASSERT_OK(DestroyDB(dbname_, options));
        options.create_if_missing = true;
        TransactionDBOptions transaction_options;
        transaction_options.write_policy = WRITE_PREPARED;
        TransactionDB* raw_db = nullptr;
        ASSERT_OK(TransactionDB::Open(options, transaction_options, dbname_,
                                      &raw_db));
        std::unique_ptr<TransactionDB> transaction_db(raw_db);
        ASSERT_OK(transaction_db->Put(WriteOptions(), "m0", "old"));
        const Snapshot* user_snapshot =
            active_user_snapshot ? transaction_db->GetSnapshot() : nullptr;
        if (active_user_snapshot) {
          ASSERT_NE(nullptr, user_snapshot);
        }
        LsmEdit edit =
            MakeEdit(transaction_db->DefaultColumnFamily(), "m", "n",
                     {MakeFile("blocking_flush.sst", 0, {{"m1", "new"}})});
        std::mutex state_mutex;
        std::condition_variable state_cv;
        bool cleanup_paused = false;
        bool release_cleanup = false;
        bool cleanup_wait_seen = false;
        bool edit_finished = false;
        const auto& wait_for_installed_flush = [&](void*) {
          std::unique_lock<std::mutex> lock(state_mutex);
          WaitBounded(state_cv, lock, [&]() { return cleanup_paused; });
        };
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::FlushMemTable:BeforeWaitForBgFlush",
            wait_for_installed_flush);
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::AtomicFlushMemTables:BeforeWaitForBgFlush",
            wait_for_installed_flush);
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::BackgroundCallFlush:FilesFound", [&](void*) {
              std::unique_lock<std::mutex> lock(state_mutex);
              cleanup_paused = true;
              state_cv.notify_all();
              WaitBounded(state_cv, lock, [&]() { return release_cleanup; });
            });
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::IngestExternalFile:WaitForFlushSnapshotCleanup",
            [&](void*) {
              std::lock_guard<std::mutex> lock(state_mutex);
              cleanup_wait_seen = true;
              state_cv.notify_all();
            });
        SyncPoint::GetInstance()->EnableProcessing();
        std::future<Status> result = std::async(std::launch::async, [&]() {
          const Status status =
              transaction_db->ApplyLsmEdit(LsmEditOptions(), {edit});
          std::lock_guard<std::mutex> lock(state_mutex);
          edit_finished = true;
          state_cv.notify_all();
          return status;
        });
        {
          std::unique_lock<std::mutex> lock(state_mutex);
          WaitBounded(state_cv, lock, [&]() { return cleanup_paused; });
          WaitBounded(state_cv, lock,
                      [&]() { return cleanup_wait_seen || edit_finished; });
          EXPECT_TRUE(cleanup_wait_seen);
          EXPECT_FALSE(edit_finished);
          release_cleanup = true;
          state_cv.notify_all();
        }
        if (result.wait_for(std::chrono::seconds(10)) !=
            std::future_status::ready) {
          ADD_FAILURE() << "LSM edit blocked its required WritePrepared flush";
          std::_Exit(1);
        }
        const Status status = result.get();
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        std::string value;
        if (active_user_snapshot) {
          EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
          EXPECT_NE(std::string::npos,
                    status.ToString().find("snapshots are active"));
          EXPECT_TRUE(
              transaction_db->Get(ReadOptions(), "m1", &value).IsNotFound());
          ASSERT_OK(transaction_db->Get(ReadOptions(), "m0", &value));
          EXPECT_EQ("old", value);
          transaction_db->ReleaseSnapshot(user_snapshot);
        } else {
          ASSERT_OK(status);
          ASSERT_OK(transaction_db->Get(ReadOptions(), "m1", &value));
          EXPECT_EQ("new", value);
        }
        ASSERT_OK(transaction_db->Put(WriteOptions(), "post", "write"));
        ASSERT_OK(transaction_db->Get(ReadOptions(), "post", &value));
        EXPECT_EQ("write", value);
        ASSERT_OK(transaction_db->Close());
      }
    }
  }
}

TEST_F(LsmEditTest, BlockingFlushWaitDoesNotIncludeLaterCleanup) {
  Close();
  const int original_workers = env_->GetBackgroundThreads(Env::HIGH);
  env_->SetBackgroundThreads(2, Env::HIGH);
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  ASSERT_OK(DestroyDB(dbname_, options));
  options.create_if_missing = true;
  TransactionDBOptions transaction_options;
  transaction_options.write_policy = WRITE_PREPARED;
  TransactionDB* raw = nullptr;
  ASSERT_OK(TransactionDB::Open(options, transaction_options, dbname_, &raw));
  std::unique_ptr<TransactionDB> db(raw);
  ColumnFamilyHandle* later_cf = nullptr;
  ASSERT_OK(
      db->CreateColumnFamily(ColumnFamilyOptions(options), "later", &later_cf));
  ASSERT_OK(db->Put(WriteOptions(), later_cf, "z", "later"));
  ASSERT_OK(db->Put(WriteOptions(), "m0", "old"));
  LsmEdit edit = MakeEdit(db->DefaultColumnFamily(), "m", "n",
                          {MakeFile("cleanup_limit.sst", 0, {{"m1", "new"}})});

  std::mutex mutex;
  std::condition_variable cv;
  std::thread::id later_thread;
  bool later_started = false;
  bool release_main_start = false;
  bool release_later_start = false;
  bool main_paused = false;
  bool release_main = false;
  bool later_cleaned = false;
  bool release_later_cleanup = false;
  bool wait_seen = false;
  int starts = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void*) {
        std::unique_lock<std::mutex> lock(mutex);
        if (++starts == 1) {
          SCOPED_TRACE("release main start");
          WaitBounded(cv, lock, [&]() { return release_main_start; });
        } else if (starts == 2) {
          later_thread = std::this_thread::get_id();
          later_started = true;
          cv.notify_all();
          SCOPED_TRACE("release later start");
          WaitBounded(cv, lock, [&]() { return release_later_start; });
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:FilesFound", [&](void*) {
        std::unique_lock<std::mutex> lock(mutex);
        if (std::this_thread::get_id() != later_thread) {
          main_paused = true;
          cv.notify_all();
          SCOPED_TRACE("release main cleanup");
          WaitBounded(cv, lock, [&]() { return release_main; });
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:SnapshotCleanedUp", [&](void*) {
        std::unique_lock<std::mutex> lock(mutex);
        if (std::this_thread::get_id() == later_thread) {
          later_cleaned = true;
          cv.notify_all();
          SCOPED_TRACE("release later cleanup");
          WaitBounded(cv, lock, [&]() { return release_later_cleanup; });
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFile:WaitForFlushSnapshotCleanup", [&](void*) {
        std::lock_guard<std::mutex> lock(mutex);
        wait_seen = true;
        cv.notify_all();
      });
  const auto& await_main_flush = [&](void*) {
    SCOPED_TRACE("main flush installation");
    std::unique_lock<std::mutex> lock(mutex);
    release_main_start = true;
    cv.notify_all();
    WaitBounded(cv, lock, [&]() { return main_paused; });
  };
  SyncPoint::GetInstance()->EnableProcessing();
  FlushOptions flush;
  flush.wait = false;
  ASSERT_OK(db->Flush(flush));
  ASSERT_OK(db->Flush(flush, later_cf));
  {
    SCOPED_TRACE("setup later worker");
    std::unique_lock<std::mutex> lock(mutex);
    WaitBounded(cv, lock, [&]() { return later_started; });
  }
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTable:BeforeWaitForBgFlush", await_main_flush);
  std::future<Status> result = std::async(std::launch::async, [&]() {
    return db->ApplyLsmEdit(LsmEditOptions(), {edit});
  });
  {
    SCOPED_TRACE("edit cleanup wait and later cleanup");
    std::unique_lock<std::mutex> lock(mutex);
    WaitBounded(cv, lock, [&]() { return wait_seen; });
    release_later_start = true;
    cv.notify_all();
    WaitBounded(cv, lock, [&]() { return later_cleaned; });
    release_main = true;
    cv.notify_all();
  }
  const bool completed =
      result.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
  EXPECT_TRUE(completed)
      << "Edit waited for a flush started after its cleanup fence";
  {
    std::lock_guard<std::mutex> lock(mutex);
    release_later_cleanup = true;
    cv.notify_all();
  }
  ASSERT_OK(AwaitBounded(result));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "m1", &value));
  EXPECT_EQ("new", value);
  ASSERT_OK(db->Get(ReadOptions(), later_cf, "z", &value));
  EXPECT_EQ("later", value);
  ASSERT_OK(db->DestroyColumnFamilyHandle(later_cf));
  ASSERT_OK(db->Close());
  env_->SetBackgroundThreads(original_workers, Env::HIGH);
}

TEST_F(LsmEditTest, RejectsEqualSequenceTombstoneAbovePoint) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                          {MakeRangeDeletionFile("tombstone.sst", 5, "m", "n"),
                           MakeFile("point.sst", 6, {{"m1", "point"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  const Status s = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos,
            s.ToString().find("invalid sequence number ordering"));
}

TEST_F(LsmEditTest, RejectsActiveSnapshots) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_NE(nullptr, snapshot);

  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("snapshot.sst", 6, {{"m1", "value"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("snapshots are active"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(LsmEditTest, SnapshotsWaitForManifestInstallation) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (bool timestamped : {false, true}) {
    DestroyAndReopen(options);
    const LsmEdit edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 {MakeFile("snapshot_install.sst", 6, {{"m1", "value"}})});

    SyncPoint::GetInstance()->LoadDependency({
        {"DBImpl::IngestExternalFiles:BeforeLogAndApply",
         "LsmEditTest::BeforeSnapshot"},
    });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeLogAndApply", [](void*) {
          SyncPoint::GetInstance()->LoadDependency({
              {"VersionSet::LogAndApply:WriteManifestStart",
               "LsmEditTest::BeforeSnapshot"},
              {"DBImpl::GetSnapshotImpl:WaitForLsmEdit",
               "VersionSet::LogAndApply:WriteManifest"},
          });
        });
    SyncPoint::GetInstance()->EnableProcessing();
    std::thread snapshot_thread([&]() {
      TEST_SYNC_POINT("LsmEditTest::BeforeSnapshot");
      std::shared_ptr<const Snapshot> timestamped_snapshot;
      ReadOptions read_options;
      if (timestamped) {
        auto result =
            dbfull()->CreateTimestampedSnapshot(kMaxSequenceNumber, 1);
        EXPECT_OK(result.first);
        timestamped_snapshot = std::move(result.second);
        read_options.snapshot = timestamped_snapshot.get();
      } else {
        read_options.snapshot = db_->GetSnapshot();
      }
      EXPECT_NE(nullptr, read_options.snapshot);
      std::string value;
      EXPECT_OK(db_->Get(read_options, "m1", &value));
      EXPECT_EQ("value", value);
      if (!timestamped) {
        db_->ReleaseSnapshot(read_options.snapshot);
      }
    });
    const Status status = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
    snapshot_thread.join();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->ClearTrace();
    ASSERT_OK(status);
  }
}

TEST_F(LsmEditTest, RejectsFileOutsideRange) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(), {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                                  {MakeFile("out.sst", 6, {{"z1", "v"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST_F(LsmEditTest, RejectsBadArguments) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = 7;
  DestroyAndReopen(options);

  EXPECT_TRUE(db_->ApplyLsmEdit(LsmEditOptions(), {}).IsInvalidArgument());

  LsmEditOptions conflicting_file_actions;
  conflicting_file_actions.move_files = true;
  conflicting_file_actions.link_files = true;
  const Status conflicting_actions = db_->ApplyLsmEdit(
      conflicting_file_actions,
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("conflicting_actions.sst", 6, {{"m1", "v"}})})});
  ASSERT_TRUE(conflicting_actions.IsInvalidArgument())
      << conflicting_actions.ToString();
  EXPECT_NE(std::string::npos,
            conflicting_actions.ToString().find(
                "move_files` and `link_files` can not both be true"));

  LsmEdit no_files;
  no_files.column_family = db_->DefaultColumnFamily();
  no_files.range = RangeOpt(Slice("m"), Slice("n"));
  EXPECT_TRUE(
      db_->ApplyLsmEdit(LsmEditOptions(), {no_files}).IsInvalidArgument());

  LsmEdit half_range;
  half_range.column_family = db_->DefaultColumnFamily();
  half_range.range.start = Slice("m");
  half_range.add_files.push_back(MakeFile("half.sst", 6, {{"m1", "v"}}));
  EXPECT_TRUE(
      db_->ApplyLsmEdit(LsmEditOptions(), {half_range}).IsNotSupported());

  const Status bad_level = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("deep.sst", options.num_levels, {{"m1", "v"}})})});
  ASSERT_TRUE(bad_level.IsInvalidArgument()) << bad_level.ToString();
  EXPECT_NE(std::string::npos, bad_level.ToString().find("outside [0, 7)"));
}

TEST_F(LsmEditTest, FifoCompactionRequiresLevelZero) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleFIFO;
  options.disable_auto_compactions = true;
  options.max_open_files = -1;
  DestroyAndReopen(options);

  const Status nonzero_level = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("fifo_l1.sst", 1, {{"m1", "bad"}})})});
  ASSERT_TRUE(nonzero_level.IsInvalidArgument()) << nonzero_level.ToString();
  EXPECT_NE(std::string::npos, nonzero_level.ToString().find("FIFO"));

  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("fifo_l0.sst", 0, {{"m1", "good"}})})}));
  EXPECT_EQ("good", Get("m1"));
  EXPECT_EQ("1", FilesPerLevel());
}

TEST_F(LsmEditTest, RejectsIncompleteChecksums) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  LsmEditFile checksum_only = MakeFile("checksum_only.sst", 6, {{"m1", "v"}});
  checksum_only.checksum = "checksum";
  const Status missing_func = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n", {checksum_only})});
  ASSERT_TRUE(missing_func.IsInvalidArgument()) << missing_func.ToString();
  EXPECT_NE(std::string::npos,
            missing_func.ToString().find("must both be set or both be empty"));

  LsmEditFile checksum_and_func = checksum_only;
  checksum_and_func.checksum_func_name = "checksum_func";
  LsmEditFile no_checksum = MakeFile("no_checksum.sst", 5, {{"m2", "v"}});
  const Status partial = db_->ApplyLsmEdit(
      LsmEditOptions(), {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                                  {checksum_and_func, no_checksum})});
  ASSERT_TRUE(partial.IsInvalidArgument()) << partial.ToString();
  EXPECT_NE(std::string::npos,
            partial.ToString().find("checksums for all files or none"));
}

// Transplant a whole multi-level LSM, overlapping level 0 and all, from a
// separate DB. This is what lets a bulk loader skip flattening its scratch
// instance into a single sorted run before handing the files over.
TEST_F(LsmEditTest, GraftLsmBuiltInAnotherDb) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string src_dbname = dbname_ + "_source";
  ASSERT_OK(DestroyDB(src_dbname, options));
  std::vector<LsmEditFile> files;
  std::string src_shape;
  {
    Options src_options = options;
    src_options.create_if_missing = true;
    std::unique_ptr<DB> src;
    ASSERT_OK(DB::Open(src_options, src_dbname, &src));

    // Bottom level: the bulk of the data.
    for (int i = 0; i < 8; i++) {
      ASSERT_OK(src->Put(WriteOptions(), "m" + std::to_string(i), "base"));
    }
    ASSERT_OK(src->Flush(FlushOptions()));
    ASSERT_OK(src->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    // Two overlapping level 0 runs on top of it, oldest written first.
    ASSERT_OK(src->Put(WriteOptions(), "m3", "middle"));
    ASSERT_OK(src->Flush(FlushOptions()));
    ASSERT_OK(src->Put(WriteOptions(), "m3", "newest"));
    ASSERT_OK(src->Put(WriteOptions(), "m5", "newest"));
    ASSERT_OK(src->Flush(FlushOptions()));

    ColumnFamilyMetaData src_meta;
    src->GetColumnFamilyMetaData(&src_meta);
    for (const auto& level : src_meta.levels) {
      std::vector<LsmEditFile> level_files;
      for (const auto& file : level.files) {
        LsmEditFile edit_file;
        edit_file.path = file.db_path + file.name;
        edit_file.level = level.level;
        level_files.push_back(std::move(edit_file));
      }
      // Level 0 metadata is newest first, but ApplyLsmEdit() treats later
      // entries as newer, so put the oldest run first.
      if (level.level == 0) {
        std::reverse(level_files.begin(), level_files.end());
      }
      for (auto& edit_file : level_files) {
        files.push_back(std::move(edit_file));
      }
    }
    ASSERT_EQ(3, files.size());
    src_shape = ShapeOf(src.get());
    ASSERT_OK(src->Close());
  }

  LsmEditOptions edit_options;
  edit_options.link_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(
      edit_options,
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n", std::move(files))}));

  EXPECT_EQ("base", Get("m0"));
  EXPECT_EQ("newest", Get("m3"));
  EXPECT_EQ("newest", Get("m5"));
  // The scratch instance's shape came across as-is: same files, same levels,
  // no compaction on either side.
  EXPECT_EQ(src_shape, ShapeOf(db_.get()));
}

TEST_F(LsmEditTest, SurvivesReopen) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(), {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                                  {MakeFile("graft.sst", 4, {{"m1", "v"}})})}));
  Reopen(options);

  EXPECT_EQ("v", Get("m1"));
  EXPECT_EQ("0,0,0,0,1", FilesPerLevel());
}

TEST_F(LsmEditTest, MergesOnlyIncomingFilesAtDeclaredLevel) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "outside"));
  ASSERT_OK(Put("z", "outside"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  const auto older =
      MakeFile("merge_older.sst", 5, {{"m1", "old"}, {"m2", "keep"}});
  const auto newer =
      MakeFile("merge_newer.sst", 5, {{"m1", "new"}, {"m3", "add"}});
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {older, newer});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.link_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("keep", Get("m2"));
  EXPECT_EQ("add", Get("m3"));
  EXPECT_EQ("outside", Get("a"));
  EXPECT_EQ(before[6], FileNumbersPerLevel()[6]);
  EXPECT_EQ(1, NumTableFilesAtLevel(5));
  EXPECT_OK(env_->FileExists(older.path));
  EXPECT_OK(env_->FileExists(newer.path));
  Reopen(options);
  EXPECT_EQ("new", Get("m1"));
}

TEST_F(LsmEditTest, MergeDoesNotResolveDestinationOverlap) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "outside"));
  ASSERT_OK(Put("z", "outside"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);
  const auto before = FileNumbersPerLevel();
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("overlap.sst", 5, {{"m1", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
  EXPECT_EQ(before, FileNumbersPerLevel());
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, MergeExistingReplacesStraddlingL6File) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "outside-left"));
  ASSERT_OK(Put("m5", "retained-middle"));
  ASSERT_OK(Put("z", "outside-right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("existing_merge_1.sst", 6, {{"m1", "one"}}),
                        MakeFile("existing_merge_2.sst", 6, {{"m2", "two"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.merge_existing_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("outside-left", Get("a"));
  EXPECT_EQ("retained-middle", Get("m5"));
  EXPECT_EQ("outside-right", Get("z"));
  EXPECT_EQ("one", Get("m1"));
  EXPECT_EQ("two", Get("m2"));
  EXPECT_EQ(1, NumTableFilesAtLevel(6));
  EXPECT_NE(before[6], FileNumbersPerLevel()[6]);
  EXPECT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  Reopen(options);
  EXPECT_EQ("outside-left", Get("a"));
  EXPECT_EQ("retained-middle", Get("m5"));
  EXPECT_EQ("two", Get("m2"));
}

TEST_F(LsmEditTest, MergedFifoFilesKeepExpiryAges) {
  env_->SetMockSleep();
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleFIFO;
  options.num_levels = 1;
  options.max_open_files = -1;
  options.disable_auto_compactions = true;
  options.ttl = 10;
  for (bool merge_existing : {false, true}) {
    DestroyAndReopen(options);
    if (merge_existing) {
      ASSERT_OK(Put("a", "left"));
      ASSERT_OK(Put("z", "right"));
      ASSERT_OK(Flush());
    }
    env_->MockSleepForSeconds(1);
    LsmEdit edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 {MakeDbFile("fifo_native", 0, {{"m1", "native"}}),
                  MakeFile("fifo_external.sst", 0, {{"m2", "external"}})});
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    LsmEditOptions edit_options;
    edit_options.merge_incoming_files = true;
    edit_options.merge_existing_files = merge_existing;
    ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
    EXPECT_EQ("native", Get("m1"));
    EXPECT_EQ("external", Get("m2"));
    std::vector<std::vector<FileMetaData>> metadata;
    dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &metadata);
    ASSERT_EQ(1, metadata[0].size());
    TablePropertiesCollection tables;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&tables));
    ASSERT_EQ(1, tables.size());
    const TableProperties& properties = *tables.begin()->second;
    EXPECT_GT(properties.newest_key_time, 0);
    EXPECT_GT(properties.file_creation_time, 0);
    EXPECT_GT(properties.oldest_ancestor_time, 0);
    EXPECT_EQ(properties.file_creation_time, metadata[0][0].file_creation_time);
    EXPECT_EQ(properties.oldest_ancestor_time,
              metadata[0][0].oldest_ancestor_time);
    Reopen(options);
    env_->MockSleepForSeconds(20);
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    EXPECT_EQ(0, NumTableFilesAtLevel(0));
    EXPECT_EQ("NOT_FOUND", Get("m1"));
    EXPECT_EQ("NOT_FOUND", Get("m2"));
    if (merge_existing) {
      EXPECT_EQ("NOT_FOUND", Get("a"));
      EXPECT_EQ("NOT_FOUND", Get("z"));
    }
  }
}

TEST_F(LsmEditTest, DestinationMergeRegistersGeneratedFileSize) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.sst_file_manager.reset(NewSstFileManager(env_));
  ASSERT_NE(nullptr, options.sst_file_manager);
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "left"));
  ASSERT_OK(Put("z", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                          {MakeFile("accounted.sst", 6, {{"m1", "incoming"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.merge_existing_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  const std::unordered_map<std::string, uint64_t> tracked =
      options.sst_file_manager->GetTrackedFiles();
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  uint64_t live_size = 0;
  for (const LevelMetaData& level : metadata.levels) {
    for (const SstFileMetaData& file : level.files) {
      const std::string path = file.db_path + file.name;
      ASSERT_NE(tracked.end(), tracked.find(path));
      EXPECT_EQ(file.size, tracked.at(path));
      live_size += file.size;
    }
  }
  ASSERT_OK(Put("zz", "trigger obsolete-file deletion"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  db_->GetColumnFamilyMetaData(&metadata);
  live_size = 0;
  for (const LevelMetaData& level : metadata.levels) {
    for (const SstFileMetaData& file : level.files) {
      live_size += file.size;
    }
  }
  EXPECT_EQ(live_size, options.sst_file_manager->GetTotalSize());
  EXPECT_EQ("incoming", Get("m1"));
  EXPECT_EQ("left", Get("a"));
  EXPECT_EQ("right", Get("z"));
}

TEST_F(LsmEditTest, FailedDestinationMergeUntracksGeneratedOutput) {
  for (int failure = 0; failure < 4; ++failure) {
    SCOPED_TRACE(failure);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.sst_file_manager.reset(NewSstFileManager(env_));
    ASSERT_NE(nullptr, options.sst_file_manager);
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "left"));
    ASSERT_OK(Put("z", "right"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(6);
    const std::unordered_map<std::string, uint64_t> before =
        options.sst_file_manager->GetTrackedFiles();
    if (failure == 0) {
      options.sst_file_manager->SetMaxAllowedSpaceUsage(
          options.sst_file_manager->GetTotalSize() + 1);
    } else if (failure == 1) {
      SyncPoint::GetInstance()->SetCallBack(
          "ExternalSstFileIngestionJob::BuildMergedFile:BeforeOutputFileInfo",
          [](void* arg) {
            *static_cast<Status*>(arg) =
                Status::IOError("output metadata read");
          });
      SyncPoint::GetInstance()->EnableProcessing();
    } else if (failure == 2) {
      env_->manifest_write_error_.store(true);
    } else {
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::IngestExternalFiles:BeforeJobsRun:0", [&](void*) {
            options.sst_file_manager->SetMaxAllowedSpaceUsage(
                options.sst_file_manager->GetTotalSize() - 1);
          });
      SyncPoint::GetInstance()->EnableProcessing();
    }
    LsmEdit edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 {MakeFile("failed_accounted.sst", 6, {{"m1", "incoming"}})});
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    LsmEditOptions edit_options;
    edit_options.merge_incoming_files = true;
    edit_options.merge_existing_files = true;
    const Status status = db_->ApplyLsmEdit(edit_options, {edit});
    env_->manifest_write_error_.store(false);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_TRUE(failure == 0 || failure == 3
                    ? status.subcode() == Status::kSpaceLimit
                    : status.IsIOError())
        << status.ToString();
    EXPECT_EQ(before, options.sst_file_manager->GetTrackedFiles());
    EXPECT_EQ("left", Get("a"));
    EXPECT_EQ("right", Get("z"));
    EXPECT_EQ("NOT_FOUND", Get("m1"));
  }
}

TEST_F(LsmEditTest, MergeExistingIncludesGapBetweenIncomingRanges) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("m", "gap-data"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "a", "z",
                       {MakeFile("existing_gap_a.sst", 6, {{"b", "left"}}),
                        MakeFile("existing_gap_z.sst", 6, {{"x", "right"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.merge_existing_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("left", Get("b"));
  EXPECT_EQ("gap-data", Get("m"));
  EXPECT_EQ("right", Get("x"));
  EXPECT_EQ(1, NumTableFilesAtLevel(6));
}

TEST_F(LsmEditTest, MergeExistingRejectsStaleDestination) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "left"));
  ASSERT_OK(Put("z", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("stale_existing.sst", 6, {{"m1", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.merge_existing_files = true;
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::MergeExisting:Selected", [&](void*) {
        CompactRangeOptions compact;
        compact.change_level = true;
        compact.target_level = 6;
        compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
        ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  const auto status = db_->ApplyLsmEdit(edit_options, {edit});
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(status.IsTryAgain()) << status.ToString();
  EXPECT_EQ("left", Get("a"));
  EXPECT_EQ("right", Get("z"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, MergeExistingRetainsSnapshotHistory) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z", "right"));
  ASSERT_OK(Flush());
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("m5", "current"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact;
  compact.change_level = true;
  compact.target_level = 6;
  compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("snapshot_existing.sst", 6, {{"m1", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.merge_existing_files = true;
  edit_options.snapshot_consistency = false;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  ReadOptions read;
  read.snapshot = snapshot;
  std::string value;
  EXPECT_OK(db_->Get(read, "m5", &value));
  EXPECT_EQ("old", value);
  EXPECT_EQ("current", Get("m5"));
  EXPECT_EQ("new", Get("m1"));
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(LsmEditTest, MergeExistingPreservesManifestGlobalSequenceForSnapshots) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("zz", "unrelated"));
  ASSERT_OK(Flush());
  ManagedSnapshot snapshot(db_.get());
  const auto external =
      MakeFile("manifest_global.sst", 6, {{"a", "left"}, {"z", "right"}});
  IngestExternalFileOptions ingestion;
  ASSERT_OK(db_->IngestExternalFile({external.path}, ingestion));
  ASSERT_EQ(1, NumTableFilesAtLevel(6));
  ReadOptions old_read;
  old_read.snapshot = snapshot.snapshot();
  std::string value;
  EXPECT_TRUE(db_->Get(old_read, "a", &value).IsNotFound());
  EXPECT_TRUE(db_->Get(old_read, "z", &value).IsNotFound());
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("manifest_merge.sst", 6, {{"m1", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.merge_existing_files = true;
  edit_options.snapshot_consistency = false;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("left", Get("a"));
  EXPECT_EQ("right", Get("z"));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_TRUE(db_->Get(old_read, "a", &value).IsNotFound());
  EXPECT_TRUE(db_->Get(old_read, "z", &value).IsNotFound());
}

TEST_F(LsmEditTest, MergeExistingRequiresAddOnlyMergeEdit) {
  LsmEditOptions options;
  options.merge_existing_files = true;
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("existing_options.sst", 6, {{"m1", "v"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  EXPECT_TRUE(db_->ApplyLsmEdit(options, {edit}).IsInvalidArgument());
  options.merge_incoming_files = true;
  edit.deletion_mode = LsmEditDeletionMode::kFilesInRange;
  EXPECT_TRUE(db_->ApplyLsmEdit(options, {edit}).IsInvalidArgument());
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.delete_files.push_back({6, 999});
  EXPECT_TRUE(db_->ApplyLsmEdit(options, {edit}).IsInvalidArgument());
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, MergeResetsScratchSequencesButPreservesOnlineUpdates) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  auto snapshot = MakeDbFile("snapshot", 5,
                             {{"m0", "seed"},
                              {"m1", "snapshot"},
                              {"m2", "unchanged"},
                              {"m3", "snapshot"}});
  snapshot.zero_sequence_numbers = true;
  const auto events = MakeDbFile("events", 5, {{"m1", "online"}});
  auto edit =
      MakeEdit(db_->DefaultColumnFamily(), "m", "n", {snapshot, events});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("online", Get("m1"));
  EXPECT_EQ("unchanged", Get("m2"));
  EXPECT_EQ(1, NumTableFilesAtLevel(5));
  EXPECT_EQ(1, db_->GetLatestSequenceNumber());
  ASSERT_OK(Put("m1", "after"));
  EXPECT_EQ("after", Get("m1"));
  ASSERT_OK(Flush());
  Reopen(options);
  EXPECT_EQ("after", Get("m1"));
}

TEST_F(LsmEditTest, MergeHonorsEqualSequenceDeletionAndMergeOperands) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);
  const auto base = MakeFile("base.sst", 5, {{"m1", "old"}, {"m2", "a"}});
  LsmEditFile delta;
  delta.path = sst_files_dir_ + "delta.sst";
  delta.level = 5;
  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.Open(delta.path));
  ASSERT_OK(writer.Delete("m1"));
  ASSERT_OK(writer.Merge("m2", "b"));
  ASSERT_OK(writer.Finish());
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {base, delta});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.move_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("a,b", Get("m2"));
  EXPECT_EQ(1, NumTableFilesAtLevel(5));
  EXPECT_TRUE(env_->FileExists(base.path).IsNotFound());
  EXPECT_TRUE(env_->FileExists(delta.path).IsNotFound());
  Reopen(options);
  EXPECT_EQ("a,b", Get("m2"));
}

TEST_F(LsmEditTest, MergeRetainsIncomingRangeTombstones) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  const std::string source_name = dbname_ + "_range_source";
  source_dbnames_.push_back(source_name);
  ASSERT_OK(DestroyDB(source_name, options));
  std::unique_ptr<DB> source;
  ASSERT_OK(DB::Open(options, source_name, &source));
  ASSERT_OK(source->Put(WriteOptions(), "m1", "old"));
  ASSERT_OK(source->Put(WriteOptions(), "m2", "keep"));
  ASSERT_OK(source->Flush(FlushOptions()));
  ASSERT_OK(source->DeleteRange(WriteOptions(), source->DefaultColumnFamily(),
                                "m1", "m2"));
  ASSERT_OK(source->Flush(FlushOptions()));
  ColumnFamilyMetaData metadata;
  source->GetColumnFamilyMetaData(&metadata);
  std::vector<LsmEditFile> files;
  for (auto file = metadata.levels[0].files.rbegin();
       file != metadata.levels[0].files.rend(); ++file) {
    files.push_back({.path = file->db_path + file->name, .level = 5});
  }
  ASSERT_OK(source->Close());
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", files);
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("keep", Get("m2"));
  EXPECT_EQ(3, db_->GetLatestSequenceNumber());
  Reopen(options);
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("keep", Get("m2"));
}

TEST_F(LsmEditTest, MergeResetRequiresSnapshotPointInputs) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  auto tombstone = MakeRangeDeletionFile("reset_range.sst", 5, "m", "n");
  tombstone.zero_sequence_numbers = true;
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {tombstone});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  EXPECT_TRUE(db_->ApplyLsmEdit(LsmEditOptions(), {edit}).IsInvalidArgument());
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
  EXPECT_OK(env_->FileExists(tombstone.path));
  EXPECT_EQ("", ShapeOf(db_.get()));
}

TEST_F(LsmEditTest, AddOnlyMayOptOutOfSnapshotConsistency) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "old"));
  ASSERT_OK(Flush());
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("a", "new"));
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("snapshot_optout.sst", 5, {{"m1", "add"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
  edit_options.snapshot_consistency = false;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("old", Get("a", snapshot));
  EXPECT_EQ("new", Get("a"));
  EXPECT_EQ("add", Get("m1"));
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(LsmEditTest, MergeRetainsVersionsNeededByKnownSnapshots) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "seed"));
  const Snapshot* snapshot = db_->GetSnapshot();
  auto base = MakeFile("history_base.sst", 5, {{"m1", "before"}});
  auto events =
      MakeDbFile("history_events", 5, {{"m0", "seed"}, {"m1", "after"}});
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {base, events});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.snapshot_consistency = false;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("before", Get("m1", snapshot));
  EXPECT_EQ("after", Get("m1"));
  EXPECT_EQ(1, NumTableFilesAtLevel(5));
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(LsmEditTest, SnapshotOptOutCannotDeleteFiles) {
  const auto file = MakeFile("cannot_delete.sst", 5, {{"m1", "add"}});
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {file});
  LsmEditOptions edit_options;
  edit_options.snapshot_consistency = false;
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.delete_files.push_back({.level = 0, .file_number = 1});
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
}

TEST_F(LsmEditTest, ProbedEmptyRangeMergeMayKeepActiveSnapshots) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "old-left"));
  ASSERT_OK(Put("z", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("a", "new-left"));

  LsmEditFile base = MakeDbFile("probed_range_base", 5, {{"m1", "before"}});
  base.zero_sequence_numbers = true;
  const LsmEditFile events =
      MakeDbFile("probed_range_events", 5,
                 {{"m0", "seed"}, {"m01", "seed"}, {"m1", "after"}});
  const LsmEdit edit =
      MakeEdit(db_->DefaultColumnFamily(), "m", "n", {base, events});
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.snapshot_consistency = false;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ(before[6], FileNumbersPerLevel()[6]);
  EXPECT_EQ(1, NumTableFilesAtLevel(5));
  EXPECT_EQ("old-left", Get("a", snapshot));
  EXPECT_EQ("new-left", Get("a"));
  EXPECT_EQ("before", Get("m1", snapshot));
  EXPECT_EQ("after", Get("m1"));
  db_->ReleaseSnapshot(snapshot);
  ASSERT_OK(Put("m1", "post-install"));
  EXPECT_EQ("post-install", Get("m1"));
  Reopen(options);
  EXPECT_EQ("post-install", Get("m1"));
  EXPECT_EQ("right", Get("z"));
}

TEST_F(LsmEditTest, ProbedRangeSnapshotOptOutRejectsDestinationData) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (int scenario = 0; scenario < 4; ++scenario) {
    SCOPED_TRACE(scenario);
    DestroyAndReopen(options);
    if (scenario == 1 || scenario == 2) {
      ASSERT_OK(Put("a", "left"));
      ASSERT_OK(Put("z", "right"));
    }
    if (scenario == 2) {
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 "b", "y"));
    } else {
      ASSERT_OK(Put("m5", "existing"));
    }
    if (scenario != 3) {
      ASSERT_OK(Flush());
      MoveFilesToLevel(6);
    }
    const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
    const Snapshot* snapshot = db_->GetSnapshot();
    LsmEditOptions edit_options;
    edit_options.merge_incoming_files = true;
    edit_options.snapshot_consistency = false;
    edit_options.allow_blocking_flush = false;
    const Status s = db_->ApplyLsmEdit(
        edit_options,
        {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                  {MakeFile("nonempty_range.sst", 5, {{"m1", "incoming"}})})});
    EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
    EXPECT_NE(std::string::npos,
              s.ToString().find(scenario == 3 ? "requires flush"
                                              : "range is not empty"));
    EXPECT_EQ(before, FileNumbersPerLevel());
    EXPECT_EQ("NOT_FOUND", Get("m1"));
    EXPECT_EQ(scenario == 2 ? "NOT_FOUND" : "existing", Get("m5", snapshot));
    db_->ReleaseSnapshot(snapshot);
  }
}

TEST_F(LsmEditTest, RangeSnapshotOptOutRequiresBoundsAndProbing) {
  const LsmEditFile file =
      MakeFile("range_optout.sst", 5, {{"m1", "incoming"}});
  LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {file});
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.snapshot_consistency = false;
  edit_options.probe_straddling_files = false;
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
  edit_options.probe_straddling_files = true;
  edit.range = RangeOpt();
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("", ShapeOf(db_.get()));
}

TEST_F(LsmEditTest, MergeResolvesEmbeddedBlobsAndWideColumns) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.compaction_readahead_size = 64 * 1024;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);
  const std::string scalar(1024, 'x');
  const std::string wide_default(1024, 'y');
  const std::string wide_other(1024, 'z');
  const WideColumns columns{{kDefaultWideColumnName, wide_default},
                            {"big", wide_other}};
  LsmEditFile base;
  base.path = sst_files_dir_ + "embedded_base.sst";
  base.level = 5;
  base.zero_sequence_numbers = true;
  SstFileWriterEmbeddedBlobOptions embedded_options;
  embedded_options.min_blob_size = 8;
  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(base.path, embedded_options));
  ASSERT_OK(writer.Put("m1", scalar));
  ASSERT_OK(writer.PutEntity("m2", columns));
  ASSERT_OK(writer.Finish());
  LsmEditFile delta;
  delta.path = sst_files_dir_ + "embedded_delta.sst";
  delta.level = 5;
  SstFileWriter delta_writer(EnvOptions(), options);
  ASSERT_OK(delta_writer.Open(delta.path));
  ASSERT_OK(delta_writer.Merge("m1", "tail"));
  ASSERT_OK(delta_writer.Merge("m2", "tail"));
  ASSERT_OK(delta_writer.Finish());
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {base, delta});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ(scalar + ",tail", Get("m1"));
  const std::string merged_default = wide_default + ",tail";
  const WideColumns expected{{kDefaultWideColumnName, merged_default},
                             {"big", wide_other}};
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  iter->Seek("m2");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(expected, iter->columns());
  iter.reset();
  Reopen(options);
  EXPECT_EQ(scalar + ",tail", Get("m1"));
  EXPECT_EQ(merged_default, Get("m2"));
}

TEST_F(LsmEditTest, RejectsReadOnlyAndSecondaryInstances) {
  Options options = CurrentOptions();
  ASSERT_OK(Put("seed", "value"));
  ASSERT_OK(Flush());

  Options read_only_options = options;
  read_only_options.max_open_files = -1;
  std::unique_ptr<DB> read_only_db;
  ASSERT_OK(DB::OpenForReadOnly(read_only_options, dbname_, &read_only_db));
  assert(read_only_db != nullptr);
  DB& read_only = *read_only_db;
  EXPECT_TRUE(read_only.ApplyLsmEdit(LsmEditOptions(), {}).IsNotSupported());
  ASSERT_OK(read_only.Close());

  const std::string secondary_path = dbname_ + "_secondary";
  ASSERT_OK(DestroyDB(secondary_path, options));
  std::unique_ptr<DB> secondary_db;
  ASSERT_OK(
      DB::OpenAsSecondary(options, dbname_, secondary_path, &secondary_db));
  assert(secondary_db != nullptr);
  DB& secondary = *secondary_db;
  EXPECT_TRUE(secondary.ApplyLsmEdit(LsmEditOptions(), {}).IsNotSupported());
  ASSERT_OK(secondary.Close());
  ASSERT_OK(DestroyDB(secondary_path, options));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
