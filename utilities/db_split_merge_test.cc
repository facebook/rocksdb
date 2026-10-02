//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/db_split_merge.h"

#include <memory>
#include <string>
#include <vector>

#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
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
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(0), "default-key",
                              "default-value"));
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

  ExpectValue(source_.db(), source_.cf(1), "a", "one-a");
  ExpectNotFound(source_.db(), source_.cf(1), "b");
  ExpectNotFound(source_.db(), source_.cf(1), "c");
  ExpectValue(source_.db(), source_.cf(1), "d", "one-d");
  ExpectNotFound(source_.db(), source_.cf(2), "c");
  ExpectNotFound(source_.db(), source_.cf(2), "d");
  ExpectValue(source_.db(), source_.cf(2), "e", "two-e");
  ExpectValue(source_.db(), source_.cf(3), "unlisted-key", "unlisted-value");

  TestDB split_destination;
  Options destination_options = options_;
  destination_options.create_if_missing = false;
  destination_options.create_missing_column_families = false;
  ASSERT_OK(split_destination.Open(destination_path_, {"one", "two"},
                                   destination_options));
  ExpectValue(split_destination.db(), split_destination.cf(0), "default-key",
              "default-value");
  ExpectNotFound(split_destination.db(), split_destination.cf(1), "a");
  ExpectValue(split_destination.db(), split_destination.cf(1), "b", "one-b");
  ExpectValue(split_destination.db(), split_destination.cf(1), "c", "one-c");
  ExpectNotFound(split_destination.db(), split_destination.cf(1), "d");
  ExpectNotFound(split_destination.db(), split_destination.cf(2), "b");
  ExpectValue(split_destination.db(), split_destination.cf(2), "c", "two-c");
  ExpectValue(split_destination.db(), split_destination.cf(2), "d", "two-d");
  ExpectNotFound(split_destination.db(), split_destination.cf(2), "e");
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
