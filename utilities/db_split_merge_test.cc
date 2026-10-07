//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/db_split_merge.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "port/stack_trace.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {
namespace {

class TestDB {
 public:
  TestDB() = default;
  TestDB(const TestDB&) = delete;
  TestDB& operator=(const TestDB&) = delete;
  TestDB(TestDB&&) = delete;
  TestDB& operator=(TestDB&&) = delete;

  ~TestDB() { Close(); }

  Status Open(const std::string& path, const std::vector<std::string>& cf_names,
              const Options& options) {
    Close();
    std::vector<ColumnFamilyDescriptor> descriptors;
    descriptors.reserve(cf_names.size() + 1);
    descriptors.emplace_back(kDefaultColumnFamilyName, options);
    for (const std::string& name : cf_names) {
      descriptors.emplace_back(name, options);
    }
    return DB::Open(options, path, descriptors, &handles_, &db_);
  }

  void Close() {
    for (ColumnFamilyHandle* handle : handles_) {
      delete handle;
    }
    handles_.clear();
    db_.reset();
  }

  DB* db() const { return db_.get(); }
  ColumnFamilyHandle* cf(size_t index) const { return handles_[index]; }

 private:
  std::unique_ptr<DB> db_;
  std::vector<ColumnFamilyHandle*> handles_;
};

class FailNewDirectoryOnceFS : public FileSystemWrapper {
 public:
  explicit FailNewDirectoryOnceFS(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}

  static const char* kClassName() { return "FailNewDirectoryOnceFS"; }
  const char* Name() const override { return kClassName(); }

  void FailNextNewDirectory(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    fail_path_ = path;
  }

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!fail_path_.empty() && name == fail_path_) {
        fail_path_.clear();
        return IOStatus::IOError("injected NewDirectory failure");
      }
    }
    return FileSystemWrapper::NewDirectory(name, io_opts, result, dbg);
  }

 private:
  std::mutex mutex_;
  std::string fail_path_;
};

class DropKeyCFilter : public CompactionFilter {
 public:
  bool Filter(int /*level*/, const Slice& key, const Slice& /*existing_value*/,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    return key == "c";
  }
  const char* Name() const override { return "DropKeyCFilter"; }
};

class DropKeyCFilterFactory : public CompactionFilterFactory {
 public:
  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<DropKeyCFilter>();
  }
  const char* Name() const override { return "DropKeyCFilterFactory"; }
};

class CompactionCountingListener : public EventListener {
 public:
  void OnCompactionCompleted(DB* /*db*/,
                             const CompactionJobInfo& /*info*/) override {
    compactions_.fetch_add(1);
  }
  int compactions() const { return compactions_.load(); }

 private:
  std::atomic<int> compactions_{0};
};

class DBSplitMergeTest : public testing::Test {
 protected:
  void SetUp() override {
    options_.create_if_missing = true;
    options_.create_missing_column_families = true;
    source_path_ = test::PerThreadDBPath("db_split_merge_source");
    destination_path_ = test::PerThreadDBPath("db_split_merge_destination");
    checkpoint_path_ = test::PerThreadDBPath("db_split_merge_checkpoint");
    ASSERT_OK(DestroyDB(source_path_, Options()));
    ASSERT_OK(DestroyDB(destination_path_, Options()));
    ASSERT_OK(DestroyDB(checkpoint_path_, Options()));
  }

  void TearDown() override {
    source_.Close();
    destination_.Close();
    EXPECT_OK(DestroyDB(source_path_, Options()));
    EXPECT_OK(DestroyDB(destination_path_, Options()));
    EXPECT_OK(DestroyDB(checkpoint_path_, Options()));
  }

  void PutFiveKeys(DB* db, ColumnFamilyHandle* cf,
                   const std::string& value_prefix) {
    ASSERT_NE(db, nullptr);
    ASSERT_NE(cf, nullptr);
    if (db == nullptr || cf == nullptr) {
      return;
    }
    for (const char* key : {"a", "b", "c", "d", "e"}) {
      ASSERT_OK(db->Put(WriteOptions(), cf, key, value_prefix + key));
    }
  }

  void ExpectValue(DB* db, ColumnFamilyHandle* cf, const std::string& key,
                   const std::string& expected) {
    ASSERT_NE(db, nullptr);
    ASSERT_NE(cf, nullptr);
    if (db == nullptr || cf == nullptr) {
      return;
    }
    std::string value;
    ASSERT_OK(db->Get(ReadOptions(), cf, key, &value));
    ASSERT_EQ(expected, value);
  }

  void ExpectNotFound(DB* db, ColumnFamilyHandle* cf, const std::string& key) {
    ASSERT_NE(db, nullptr);
    ASSERT_NE(cf, nullptr);
    if (db == nullptr || cf == nullptr) {
      return;
    }
    std::string value;
    ASSERT_TRUE(db->Get(ReadOptions(), cf, key, &value).IsNotFound());
  }

  // Declared before the DBs so it outlives any DB opened with it.
  std::unique_ptr<Env> fault_env_;
  Options options_;
  TestDB source_;
  TestDB destination_;
  std::string source_path_;
  std::string destination_path_;
  std::string checkpoint_path_;

  void ExpectPathNotFound(const std::string& path) {
    ASSERT_NE(options_.env, nullptr);
    if (options_.env != nullptr) {
      ASSERT_TRUE(options_.env->FileExists(path).IsNotFound());
    }
  }
};

TEST_F(DBSplitMergeTest, SplitMultipleColumnFamilies) {
  ASSERT_OK(source_.Open(source_path_, {"one", "two", "unlisted"}, options_));
  // One default-CF key below L0 and one still in the memtable, which the
  // checkpoint flushes to L0.
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(0), "default-key",
                              "default-value"));
  ASSERT_OK(source_.db()->CompactRange(CompactRangeOptions(), source_.cf(0),
                                       nullptr, nullptr));
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(0),
                              "default-memtable-key",
                              "default-memtable-value"));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");
  PutFiveKeys(source_.db(), source_.cf(2), "two-");
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(3), "unlisted-key",
                              "unlisted-value"));

  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {
      {"b", "d", source_.cf(1)},
      {"c", "e", source_.cf(2)},
  };
  SplitMergeResult result;
  ASSERT_OK(SplitDB(source_.db(), split_options, &result));
  ASSERT_GT(result.checkpoint_sequence, 0);
  ASSERT_GT(result.transferred_files, 0);
  ExpectValue(source_.db(), source_.cf(1), "b", "one-b");
  ExpectValue(source_.db(), source_.cf(2), "c", "two-c");

  // Must run before anything reopens the split DB and rewrites its OPTIONS.
  ConfigOptions config_options;
  DBOptions loaded_db_options;
  std::vector<ColumnFamilyDescriptor> loaded_descriptors;
  ASSERT_OK(LoadLatestOptions(config_options, destination_path_,
                              &loaded_db_options, &loaded_descriptors));
  ASSERT_EQ(3, loaded_descriptors.size());
  for (const ColumnFamilyDescriptor& descriptor : loaded_descriptors) {
    ASSERT_FALSE(descriptor.options.disable_auto_compactions)
        << descriptor.name;
  }

  ASSERT_OK(DeleteSplitRanges(source_.db(), split_options));
  ASSERT_OK(DeleteSplitRanges(source_.db(), split_options));

  ExpectValue(source_.db(), source_.cf(1), "a", "one-a");
  ExpectNotFound(source_.db(), source_.cf(1), "b");
  ExpectNotFound(source_.db(), source_.cf(1), "c");
  ExpectValue(source_.db(), source_.cf(1), "d", "one-d");
  ExpectNotFound(source_.db(), source_.cf(2), "c");
  ExpectNotFound(source_.db(), source_.cf(2), "d");
  ExpectValue(source_.db(), source_.cf(2), "e", "two-e");
  ExpectValue(source_.db(), source_.cf(3), "unlisted-key", "unlisted-value");
  ExpectValue(source_.db(), source_.cf(0), "default-key", "default-value");
  ExpectValue(source_.db(), source_.cf(0), "default-memtable-key",
              "default-memtable-value");

  TestDB split_destination;
  Options destination_options = options_;
  destination_options.create_if_missing = false;
  destination_options.create_missing_column_families = false;
  ASSERT_OK(split_destination.Open(destination_path_, {"one", "two"},
                                   destination_options));
  ExpectNotFound(split_destination.db(), split_destination.cf(0),
                 "default-key");
  ExpectNotFound(split_destination.db(), split_destination.cf(0),
                 "default-memtable-key");
  ExpectNotFound(split_destination.db(), split_destination.cf(1), "a");
  ExpectValue(split_destination.db(), split_destination.cf(1), "b", "one-b");
  ExpectValue(split_destination.db(), split_destination.cf(1), "c", "one-c");
  ExpectNotFound(split_destination.db(), split_destination.cf(1), "d");
  ExpectNotFound(split_destination.db(), split_destination.cf(2), "b");
  ExpectValue(split_destination.db(), split_destination.cf(2), "c", "two-c");
  ExpectValue(split_destination.db(), split_destination.cf(2), "d", "two-d");
  ExpectNotFound(split_destination.db(), split_destination.cf(2), "e");
}

TEST_F(DBSplitMergeTest, SplitSelectedDefaultColumnFamily) {
  ASSERT_OK(source_.Open(source_path_, {}, options_));
  PutFiveKeys(source_.db(), source_.cf(0), "default-");

  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {{"b", "d", source_.cf(0)}};
  SplitMergeResult result;
  ASSERT_OK(SplitDB(source_.db(), split_options, &result));

  TestDB split_destination;
  ASSERT_OK(split_destination.Open(destination_path_, {}, options_));
  ExpectNotFound(split_destination.db(), split_destination.cf(0), "a");
  ExpectValue(split_destination.db(), split_destination.cf(0), "b",
              "default-b");
  ExpectValue(split_destination.db(), split_destination.cf(0), "c",
              "default-c");
  ExpectNotFound(split_destination.db(), split_destination.cf(0), "d");
}

TEST_F(DBSplitMergeTest, SplitSkipsSourceCompactionFilterAndListeners) {
  const std::shared_ptr<CompactionCountingListener> listener =
      std::make_shared<CompactionCountingListener>();
  options_.listeners.push_back(listener);
  options_.compaction_filter_factory =
      std::make_shared<DropKeyCFilterFactory>();
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");

  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {{"b", "d", source_.cf(1)}};
  SplitMergeResult result;
  ASSERT_OK(SplitDB(source_.db(), split_options, &result));
  ASSERT_EQ(0, listener->compactions());

  std::string options_file;
  ASSERT_OK(
      GetLatestOptionsFileName(destination_path_, options_.env, &options_file));
  std::string options_text;
  ASSERT_OK(ReadFileToString(
      options_.env, destination_path_ + "/" + options_file, &options_text));
  ASSERT_NE(std::string::npos, options_text.find("DropKeyCFilterFactory"));

  TestDB split_destination;
  ASSERT_OK(split_destination.Open(destination_path_, {"one"}, options_));
  ExpectValue(split_destination.db(), split_destination.cf(1), "b", "one-b");
  ExpectValue(split_destination.db(), split_destination.cf(1), "c", "one-c");
}

TEST_F(DBSplitMergeTest, SplitRejectsDuplicateSourceColumnFamily) {
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {
      {"a", "c", source_.cf(1)},
      {"c", "e", source_.cf(1)},
  };
  SplitMergeResult result;
  ASSERT_TRUE(
      SplitDB(source_.db(), split_options, &result).IsInvalidArgument());
  ExpectPathNotFound(destination_path_);
}

TEST_F(DBSplitMergeTest, SplitRejectsSourceAtStagingPath) {
  source_path_ = destination_path_ + ".tmp";
  ASSERT_OK(DestroyDB(source_path_, Options()));
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");
  ASSERT_OK(source_.db()->Flush(FlushOptions(), source_.cf(1)));

  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {{"b", "d", source_.cf(1)}};
  SplitMergeResult result;
  ASSERT_TRUE(
      SplitDB(source_.db(), split_options, &result).IsInvalidArgument());
  ExpectPathNotFound(destination_path_);
  ExpectValue(source_.db(), source_.cf(1), "b", "one-b");

  source_.Close();
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  ExpectValue(source_.db(), source_.cf(1), "b", "one-b");
}

TEST_F(DBSplitMergeTest, SplitCleansUpPublishedDestinationOnFailure) {
  std::shared_ptr<FailNewDirectoryOnceFS> fault_fs =
      std::make_shared<FailNewDirectoryOnceFS>(FileSystem::Default());
  fault_env_ = NewCompositeEnv(fault_fs);
  Options options = options_;
  options.env = fault_env_.get();
  ASSERT_OK(source_.Open(source_path_, {"one"}, options));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");

  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {{"b", "d", source_.cf(1)}};
  SplitMergeResult result;
  // Checkpoint opens the final directory to fsync it after the rename.
  fault_fs->FailNextNewDirectory(destination_path_);
  ASSERT_TRUE(SplitDB(source_.db(), split_options, &result).IsIOError());
  ExpectPathNotFound(destination_path_);
  ExpectPathNotFound(destination_path_ + ".tmp");

  ASSERT_OK(SplitDB(source_.db(), split_options, &result));
  ASSERT_GT(result.transferred_files, 0);
}

TEST_F(DBSplitMergeTest, MergeMultipleColumnFamilies) {
  ASSERT_OK(source_.Open(source_path_, {"one", "two"}, options_));
  ASSERT_OK(
      destination_.Open(destination_path_, {"dst-one", "dst-two"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-old-");
  PutFiveKeys(source_.db(), source_.cf(2), "two-");
  ASSERT_OK(
      source_.db()->Flush(FlushOptions(), {source_.cf(1), source_.cf(2)}));
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(1), "c", "one-new-c"));
  ASSERT_OK(source_.db()->Flush(FlushOptions(), source_.cf(1)));

  ASSERT_OK(destination_.db()->Put(WriteOptions(), destination_.cf(1), "a",
                                   "destination-a"));
  ASSERT_OK(destination_.db()->Put(WriteOptions(), destination_.cf(2), "z",
                                   "destination-z"));
  ASSERT_OK(destination_.db()->Flush(FlushOptions(),
                                     {destination_.cf(1), destination_.cf(2)}));

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "e", source_.cf(1), destination_.cf(1)},
      {"c", "f", source_.cf(2), destination_.cf(2)},
  };
  SplitMergeResult result;
  ASSERT_OK(MergeDB(source_.db(), destination_.db(), merge_options, &result));
  ASSERT_GT(result.checkpoint_sequence, 0);
  ASSERT_GT(result.transferred_files, 0);
  ExpectPathNotFound(checkpoint_path_);

  ExpectValue(destination_.db(), destination_.cf(1), "a", "destination-a");
  ExpectValue(destination_.db(), destination_.cf(1), "b", "one-old-b");
  ExpectValue(destination_.db(), destination_.cf(1), "c", "one-new-c");
  ExpectValue(destination_.db(), destination_.cf(1), "d", "one-old-d");
  ExpectNotFound(destination_.db(), destination_.cf(1), "e");
  ExpectNotFound(destination_.db(), destination_.cf(2), "b");
  ExpectValue(destination_.db(), destination_.cf(2), "c", "two-c");
  ExpectValue(destination_.db(), destination_.cf(2), "d", "two-d");
  ExpectValue(destination_.db(), destination_.cf(2), "e", "two-e");
  ExpectValue(destination_.db(), destination_.cf(2), "z", "destination-z");

  ExpectValue(source_.db(), source_.cf(1), "b", "one-old-b");
  ExpectValue(source_.db(), source_.cf(1), "c", "one-new-c");
  ExpectValue(source_.db(), source_.cf(2), "e", "two-e");
}

TEST_F(DBSplitMergeTest, MergeRejectsRepeatedSourceOrDestination) {
  ASSERT_OK(source_.Open(source_path_, {"one", "two"}, options_));
  ASSERT_OK(
      destination_.Open(destination_path_, {"dst-one", "dst-two"}, options_));

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"a", "c", source_.cf(1), destination_.cf(1)},
      {"c", "e", source_.cf(1), destination_.cf(2)},
  };
  SplitMergeResult result;
  ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                  .IsInvalidArgument());

  merge_options.column_family_merges = {
      {"a", "c", source_.cf(1), destination_.cf(1)},
      {"c", "e", source_.cf(2), destination_.cf(1)},
  };
  ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                  .IsInvalidArgument());
  ExpectPathNotFound(checkpoint_path_);
}

TEST_F(DBSplitMergeTest, MergeRejectsExistingStagingPath) {
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  ASSERT_OK(destination_.Open(destination_path_, {"dst-one"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");

  Env* env = options_.env;
  ASSERT_NE(env, nullptr);
  if (env == nullptr) {
    return;
  }
  const std::string staging_path = checkpoint_path_ + ".tmp";
  const std::string sentinel_path = staging_path + "/sentinel";
  ASSERT_OK(env->CreateDirIfMissing(staging_path));
  ASSERT_OK(WriteStringToFile(env, "sentinel", sentinel_path));

  MergeDBOptions merge_options;
  // The trailing slash must not change the derived staging path.
  merge_options.checkpoint_directory = checkpoint_path_ + "/";
  merge_options.column_family_merges = {
      {"b", "e", source_.cf(1), destination_.cf(1)}};
  SplitMergeResult result;
  const Status status =
      MergeDB(source_.db(), destination_.db(), merge_options, &result);
  ASSERT_OK(env->FileExists(sentinel_path));
  ASSERT_OK(env->DeleteFile(sentinel_path));
  ASSERT_OK(env->DeleteDir(staging_path));
  ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
  ExpectPathNotFound(checkpoint_path_);
  ExpectNotFound(destination_.db(), destination_.cf(1), "b");
}

TEST_F(DBSplitMergeTest, MergeRejectsDestinationSnapshot) {
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  ASSERT_OK(destination_.Open(destination_path_, {"dst-one"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");
  // Advance destination sequence numbers past the source keys so a snapshot
  // would cover them if they kept their source sequence numbers.
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(destination_.db()->Put(WriteOptions(), destination_.cf(0),
                                     "advance-" + std::to_string(i), "v"));
  }

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "e", source_.cf(1), destination_.cf(1)}};
  SplitMergeResult result;
  {
    ManagedSnapshot snapshot(destination_.db());
    ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                    .IsInvalidArgument());
    ReadOptions snapshot_read;
    snapshot_read.snapshot = snapshot.snapshot();
    std::string value;
    ASSERT_TRUE(destination_.db()
                    ->Get(snapshot_read, destination_.cf(1), "b", &value)
                    .IsNotFound());
    ExpectNotFound(destination_.db(), destination_.cf(1), "b");
    ExpectPathNotFound(checkpoint_path_);
  }

  ASSERT_OK(MergeDB(source_.db(), destination_.db(), merge_options, &result));
  ExpectValue(destination_.db(), destination_.cf(1), "b", "one-b");
}

TEST_F(DBSplitMergeTest, MergeRejectsOverlapWithoutPartialIngestion) {
  ASSERT_OK(source_.Open(source_path_, {"one", "two"}, options_));
  ASSERT_OK(
      destination_.Open(destination_path_, {"dst-one", "dst-two"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");
  PutFiveKeys(source_.db(), source_.cf(2), "two-");
  ASSERT_OK(destination_.db()->Put(WriteOptions(), destination_.cf(2), "d",
                                   "existing-d"));
  ASSERT_OK(destination_.db()->Flush(FlushOptions(), destination_.cf(2)));

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "e", source_.cf(1), destination_.cf(1)},
      {"c", "f", source_.cf(2), destination_.cf(2)},
  };
  SplitMergeResult result;
  ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                  .IsInvalidArgument());
  ExpectNotFound(destination_.db(), destination_.cf(1), "b");
  ExpectNotFound(destination_.db(), destination_.cf(1), "c");
  ExpectValue(destination_.db(), destination_.cf(2), "d", "existing-d");
  ExpectPathNotFound(checkpoint_path_);
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
