// Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "test_util/testharness.h"

#ifdef OS_LINUX
#include "env/io_posix.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
class EnvWithCustomLogicalBlockSizeCache : public EnvWrapper {
 public:
  EnvWithCustomLogicalBlockSizeCache(Env* env, LogicalBlockSizeCache* cache)
      : EnvWrapper(env), cache_(cache) {}

  Status RegisterDbPaths(const std::vector<std::string>& paths) override {
    return cache_->RefAndCacheLogicalBlockSize(paths);
  }

  Status UnregisterDbPaths(const std::vector<std::string>& paths) override {
    cache_->UnrefAndTryRemoveCachedLogicalBlockSize(paths);
    return Status::OK();
  }

 private:
  LogicalBlockSizeCache* cache_;
};

class DBLogicalBlockSizeCacheTest : public testing::Test {
 public:
  DBLogicalBlockSizeCacheTest()
      : dbname_(test::PerThreadDBPath("logical_block_size_cache_test")),
        data_path_0_(dbname_ + "/data_path_0"),
        data_path_1_(dbname_ + "/data_path_1"),
        cf_path_0_(dbname_ + "/cf_path_0"),
        cf_path_1_(dbname_ + "/cf_path_1") {
    auto get_fd_block_size = [&](int fd) { return fd; };
    auto get_dir_block_size = [&](const std::string& /*dir*/, size_t* size) {
      *size = 1024;
      return Status::OK();
    };
    cache_.reset(
        new LogicalBlockSizeCache(get_fd_block_size, get_dir_block_size));
    env_.reset(
        new EnvWithCustomLogicalBlockSizeCache(Env::Default(), cache_.get()));
  }

 protected:
  std::string dbname_;
  std::string data_path_0_;
  std::string data_path_1_;
  std::string cf_path_0_;
  std::string cf_path_1_;
  std::unique_ptr<LogicalBlockSizeCache> cache_;
  std::unique_ptr<Env> env_;
};

TEST_F(DBLogicalBlockSizeCacheTest, OpenClose) {
  // Tests that Open will cache the logical block size for data paths,
  // and Close will remove the cached sizes.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();
  options.db_paths = {{data_path_0_, 2048}, {data_path_1_, 2048}};

  for (int i = 0; i < 2; i++) {
    DB* db;
    if (!i) {
      printf("Open\n");
      ASSERT_OK(DB::Open(options, dbname_, &db));
    } else {
#ifdef ROCKSDB_LITE
      break;
#else
      printf("OpenForReadOnly\n");
      ASSERT_OK(DB::OpenForReadOnly(options, dbname_, &db));
#endif
    }
    ASSERT_EQ(2, cache_->Size());
    ASSERT_TRUE(cache_->Contains(data_path_0_));
    ASSERT_EQ(1, cache_->GetRefCount(data_path_0_));
    ASSERT_TRUE(cache_->Contains(data_path_1_));
    ASSERT_EQ(1, cache_->GetRefCount(data_path_1_));
    ASSERT_OK(db->Close());
    ASSERT_EQ(0, cache_->Size());
    delete db;
  }
  ASSERT_OK(DestroyDB(dbname_, options, {}));
}

TEST_F(DBLogicalBlockSizeCacheTest, OpenDelete) {
  // Tests that Open will cache the logical block size for data paths,
  // and delete the db pointer will remove the cached sizes.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();

  for (int i = 0; i < 2; i++) {
    DB* db;
    if (!i) {
      printf("Open\n");
      ASSERT_OK(DB::Open(options, dbname_, &db));
    } else {
#ifdef ROCKSDB_LITE
      break;
#else
      printf("OpenForReadOnly\n");
      ASSERT_OK(DB::OpenForReadOnly(options, dbname_, &db));
#endif
    }
    ASSERT_EQ(1, cache_->Size());
    ASSERT_TRUE(cache_->Contains(dbname_));
    ASSERT_EQ(1, cache_->GetRefCount(dbname_));
    delete db;
    ASSERT_EQ(0, cache_->Size());
  }
  ASSERT_OK(DestroyDB(dbname_, options, {}));
}

TEST_F(DBLogicalBlockSizeCacheTest, CreateColumnFamily) {
  // Tests that CreateColumnFamily will cache the cf_paths,
  // drop the column family handle won't drop the cache,
  // drop and then delete the column family handle will drop the cache.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();
  ColumnFamilyOptions cf_options;
  cf_options.cf_paths = {{cf_path_0_, 1024}, {cf_path_1_, 2048}};

  DB* db;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  ASSERT_EQ(1, cache_->Size());
  ASSERT_TRUE(cache_->Contains(dbname_));
  ASSERT_EQ(1, cache_->GetRefCount(dbname_));

  ColumnFamilyHandle* cf = nullptr;
  ASSERT_OK(db->CreateColumnFamily(cf_options, "cf", &cf));
  ASSERT_EQ(3, cache_->Size());
  ASSERT_TRUE(cache_->Contains(dbname_));
  ASSERT_EQ(1, cache_->GetRefCount(dbname_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));
  ASSERT_TRUE(cache_->Contains(cf_path_1_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_1_));

  // Drop column family does not drop cache.
  ASSERT_OK(db->DropColumnFamily(cf));
  ASSERT_EQ(3, cache_->Size());
  ASSERT_TRUE(cache_->Contains(dbname_));
  ASSERT_EQ(1, cache_->GetRefCount(dbname_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));
  ASSERT_TRUE(cache_->Contains(cf_path_1_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_1_));

  // Delete handle will drop cache.
  ASSERT_OK(db->DestroyColumnFamilyHandle(cf));
  ASSERT_TRUE(cache_->Contains(dbname_));
  ASSERT_EQ(1, cache_->GetRefCount(dbname_));

  delete db;
  ASSERT_EQ(0, cache_->Size());
  ASSERT_OK(DestroyDB(dbname_, options, {{"cf", cf_options}}));
}

TEST_F(DBLogicalBlockSizeCacheTest, CreateColumnFamilies) {
  // Tests that CreateColumnFamilies will cache the cf_paths,
  // drop the column family handle won't drop the cache,
  // drop and then delete the column family handle will drop the cache.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();
  ColumnFamilyOptions cf_options;
  cf_options.cf_paths = {{cf_path_0_, 1024}};

  DB* db;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  ASSERT_EQ(1, cache_->Size());
  ASSERT_TRUE(cache_->Contains(dbname_));
  ASSERT_EQ(1, cache_->GetRefCount(dbname_));

  std::vector<ColumnFamilyHandle*> cfs;
  ASSERT_OK(db->CreateColumnFamilies(cf_options, {"cf1", "cf2"}, &cfs));
  ASSERT_EQ(2, cache_->Size());
  ASSERT_TRUE(cache_->Contains(dbname_));
  ASSERT_EQ(1, cache_->GetRefCount(dbname_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(2, cache_->GetRefCount(cf_path_0_));

  // Drop column family does not drop cache.
  for (ColumnFamilyHandle* cf : cfs) {
    ASSERT_OK(db->DropColumnFamily(cf));
    ASSERT_EQ(2, cache_->Size());
    ASSERT_TRUE(cache_->Contains(dbname_));
    ASSERT_EQ(1, cache_->GetRefCount(dbname_));
    ASSERT_TRUE(cache_->Contains(cf_path_0_));
    ASSERT_EQ(2, cache_->GetRefCount(cf_path_0_));
  }

  // Delete one handle will not drop cache because another handle is still
  // referencing cf_path_0_.
  ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[0]));
  ASSERT_EQ(2, cache_->Size());
  ASSERT_TRUE(cache_->Contains(dbname_));
  ASSERT_EQ(1, cache_->GetRefCount(dbname_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));

  // Delete the last handle will drop cache.
  ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[1]));
  ASSERT_EQ(1, cache_->Size());
  ASSERT_TRUE(cache_->Contains(dbname_));
  ASSERT_EQ(1, cache_->GetRefCount(dbname_));

  delete db;
  ASSERT_EQ(0, cache_->Size());
  ASSERT_OK(DestroyDB(dbname_, options,
      {{"cf1", cf_options}, {"cf2", cf_options}}));
}

TEST_F(DBLogicalBlockSizeCacheTest, OpenWithColumnFamilies) {
  // Tests that Open two column families with the same cf_path will cache the
  // cf_path and have 2 references to the cached size,
  // drop the column family handle won't drop the cache,
  // drop and then delete the column family handle will drop the cache.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();

  ColumnFamilyOptions cf_options;
  cf_options.cf_paths = {{cf_path_0_, 1024}};

  for (int i = 0; i < 2; i++) {
    DB* db;
    ColumnFamilyHandle* cf1 = nullptr;
    ColumnFamilyHandle* cf2 = nullptr;
    ASSERT_OK(DB::Open(options, dbname_, &db));
    ASSERT_OK(db->CreateColumnFamily(cf_options, "cf1", &cf1));
    ASSERT_OK(db->CreateColumnFamily(cf_options, "cf2", &cf2));
    ASSERT_OK(db->DestroyColumnFamilyHandle(cf1));
    ASSERT_OK(db->DestroyColumnFamilyHandle(cf2));
    delete db;
    ASSERT_EQ(0, cache_->Size());

    std::vector<ColumnFamilyHandle*> cfs;
    if (!i) {
      printf("Open\n");
      ASSERT_OK(DB::Open(options, dbname_,
                         {{"cf1", cf_options},
                          {"cf2", cf_options},
                          {"default", ColumnFamilyOptions()}},
                         &cfs, &db));
    } else {
#ifdef ROCKSDB_LITE
      break;
#else
      printf("OpenForReadOnly\n");
      ASSERT_OK(DB::OpenForReadOnly(options, dbname_,
                                    {{"cf1", cf_options},
                                     {"cf2", cf_options},
                                     {"default", ColumnFamilyOptions()}},
                                    &cfs, &db));
#endif
    }

    // Logical block sizes of dbname_ and cf_path_0_ are cached during Open.
    ASSERT_EQ(2, cache_->Size());
    ASSERT_TRUE(cache_->Contains(dbname_));
    ASSERT_EQ(1, cache_->GetRefCount(dbname_));
    ASSERT_TRUE(cache_->Contains(cf_path_0_));
    ASSERT_EQ(2, cache_->GetRefCount(cf_path_0_));

    // Drop handles won't drop the cache.
    ASSERT_OK(db->DropColumnFamily(cfs[0]));
    ASSERT_OK(db->DropColumnFamily(cfs[1]));
    ASSERT_EQ(2, cache_->Size());
    ASSERT_TRUE(cache_->Contains(dbname_));
    ASSERT_EQ(1, cache_->GetRefCount(dbname_));
    ASSERT_TRUE(cache_->Contains(cf_path_0_));
    ASSERT_EQ(2, cache_->GetRefCount(cf_path_0_));

    // Delete 1st handle won't drop the cache for cf_path_0_.
    ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[0]));
    ASSERT_EQ(2, cache_->Size());
    ASSERT_TRUE(cache_->Contains(dbname_));
    ASSERT_EQ(1, cache_->GetRefCount(dbname_));
    ASSERT_TRUE(cache_->Contains(cf_path_0_));
    ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));

    // Delete 2nd handle will drop the cache for cf_path_0_.
    ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[1]));
    ASSERT_EQ(1, cache_->Size());
    ASSERT_TRUE(cache_->Contains(dbname_));
    ASSERT_EQ(1, cache_->GetRefCount(dbname_));

    // Delete the default handle won't affect the cache because db still refers
    // to the default CF.
    ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[2]));
    ASSERT_EQ(1, cache_->Size());
    ASSERT_TRUE(cache_->Contains(dbname_));
    ASSERT_EQ(1, cache_->GetRefCount(dbname_));

    delete db;
    ASSERT_EQ(0, cache_->Size());
  }
  ASSERT_OK(DestroyDB(dbname_, options,
      {{"cf1", cf_options}, {"cf2", cf_options}}));
}

TEST_F(DBLogicalBlockSizeCacheTest, DestroyColumnFamilyHandle) {
  // Tests that destroy column family without dropping won't drop the cache,
  // because compaction and flush might still need to get logical block size
  // when opening new files.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();
  ColumnFamilyOptions cf_options;
  cf_options.cf_paths = {{cf_path_0_, 1024}};

  DB* db;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  ASSERT_EQ(1, cache_->Size());
  ASSERT_TRUE(cache_->Contains(dbname_));
  ASSERT_EQ(1, cache_->GetRefCount(dbname_));
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_OK(db->CreateColumnFamily(cf_options, "cf", &cf));
  ASSERT_EQ(2, cache_->Size());
  ASSERT_TRUE(cache_->Contains(dbname_));
  ASSERT_EQ(1, cache_->GetRefCount(dbname_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));

  // Delete handle won't drop cache.
  ASSERT_OK(db->DestroyColumnFamilyHandle(cf));
  ASSERT_EQ(2, cache_->Size());
  ASSERT_TRUE(cache_->Contains(dbname_));
  ASSERT_EQ(1, cache_->GetRefCount(dbname_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));

  delete db;
  ASSERT_EQ(0, cache_->Size());

  // Open with column families.
  std::vector<ColumnFamilyHandle*> cfs;
  for (int i = 0; i < 2; i++) {
    if (!i) {
      printf("Open\n");
      ASSERT_OK(DB::Open(
          options, dbname_,
          {{"cf", cf_options}, {"default", ColumnFamilyOptions()}}, &cfs, &db));
    } else {
#ifdef ROCKSDB_LITE
      break;
#else
      printf("OpenForReadOnly\n");
      ASSERT_OK(DB::OpenForReadOnly(
          options, dbname_,
          {{"cf", cf_options}, {"default", ColumnFamilyOptions()}}, &cfs, &db));
#endif
    }
    // cf_path_0_ and dbname_ are cached.
    ASSERT_EQ(2, cache_->Size());
    ASSERT_TRUE(cache_->Contains(dbname_));
    ASSERT_EQ(1, cache_->GetRefCount(dbname_));
    ASSERT_TRUE(cache_->Contains(cf_path_0_));
    ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));

    // Deleting handle won't drop cache.
    ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[0]));
    ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[1]));
    ASSERT_EQ(2, cache_->Size());
    ASSERT_TRUE(cache_->Contains(dbname_));
    ASSERT_EQ(1, cache_->GetRefCount(dbname_));
    ASSERT_TRUE(cache_->Contains(cf_path_0_));
    ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));

    delete db;
    ASSERT_EQ(0, cache_->Size());
  }
  ASSERT_OK(DestroyDB(dbname_, options, {{"cf", cf_options}}));
}

TEST_F(DBLogicalBlockSizeCacheTest, MultiDBWithDifferentPaths) {
  // Tests the cache behavior when there are multiple DBs sharing the same env
  // with different db_paths and cf_paths.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();

  ASSERT_OK(env_->CreateDirIfMissing(dbname_));

  DB* db0;
  ASSERT_OK(DB::Open(options, data_path_0_, &db0));
  ASSERT_EQ(1, cache_->Size());
  ASSERT_TRUE(cache_->Contains(data_path_0_));

  ColumnFamilyOptions cf_options0;
  cf_options0.cf_paths = {{cf_path_0_, 1024}};
  ColumnFamilyHandle* cf0;
  ASSERT_OK(db0->CreateColumnFamily(cf_options0, "cf", &cf0));
  ASSERT_EQ(2, cache_->Size());
  ASSERT_TRUE(cache_->Contains(data_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(data_path_0_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));

  DB* db1;
  ASSERT_OK(DB::Open(options, data_path_1_, &db1));
  ASSERT_EQ(3, cache_->Size());
  ASSERT_TRUE(cache_->Contains(data_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(data_path_0_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));
  ASSERT_TRUE(cache_->Contains(data_path_1_));
  ASSERT_EQ(1, cache_->GetRefCount(data_path_1_));

  ColumnFamilyOptions cf_options1;
  cf_options1.cf_paths = {{cf_path_1_, 1024}};
  ColumnFamilyHandle* cf1;
  ASSERT_OK(db1->CreateColumnFamily(cf_options1, "cf", &cf1));
  ASSERT_EQ(4, cache_->Size());
  ASSERT_TRUE(cache_->Contains(data_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(data_path_0_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));
  ASSERT_TRUE(cache_->Contains(data_path_1_));
  ASSERT_EQ(1, cache_->GetRefCount(data_path_1_));
  ASSERT_TRUE(cache_->Contains(cf_path_1_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_1_));

  ASSERT_OK(db0->DestroyColumnFamilyHandle(cf0));
  delete db0;
  ASSERT_EQ(2, cache_->Size());
  ASSERT_TRUE(cache_->Contains(data_path_1_));
  ASSERT_EQ(1, cache_->GetRefCount(data_path_1_));
  ASSERT_TRUE(cache_->Contains(cf_path_1_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_1_));
  ASSERT_OK(DestroyDB(data_path_0_, options, {{"cf", cf_options0}}));

  ASSERT_OK(db1->DestroyColumnFamilyHandle(cf1));
  delete db1;
  ASSERT_EQ(0, cache_->Size());
  ASSERT_OK(DestroyDB(data_path_1_, options, {{"cf", cf_options1}}));
}

TEST_F(DBLogicalBlockSizeCacheTest, MultiDBWithSamePaths) {
  // Tests the cache behavior when there are multiple DBs sharing the same env
  // with the same db_paths and cf_paths.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();
  options.db_paths = {{data_path_0_, 1024}};
  ColumnFamilyOptions cf_options;
  cf_options.cf_paths = {{cf_path_0_, 1024}};

  ASSERT_OK(env_->CreateDirIfMissing(dbname_));

  DB* db0;
  ASSERT_OK(DB::Open(options, dbname_ + "/db0", &db0));
  ASSERT_EQ(1, cache_->Size());
  ASSERT_TRUE(cache_->Contains(data_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(data_path_0_));

  ColumnFamilyHandle* cf0;
  ASSERT_OK(db0->CreateColumnFamily(cf_options, "cf", &cf0));
  ASSERT_EQ(2, cache_->Size());
  ASSERT_TRUE(cache_->Contains(data_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(data_path_0_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));

  DB* db1;
  ASSERT_OK(DB::Open(options, dbname_ + "/db1", &db1));
  ASSERT_EQ(2, cache_->Size());
  ASSERT_TRUE(cache_->Contains(data_path_0_));
  ASSERT_EQ(2, cache_->GetRefCount(data_path_0_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));

  ColumnFamilyHandle* cf1;
  ASSERT_OK(db1->CreateColumnFamily(cf_options, "cf", &cf1));
  ASSERT_EQ(2, cache_->Size());
  ASSERT_TRUE(cache_->Contains(data_path_0_));
  ASSERT_EQ(2, cache_->GetRefCount(data_path_0_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(2, cache_->GetRefCount(cf_path_0_));

  ASSERT_OK(db0->DestroyColumnFamilyHandle(cf0));
  delete db0;
  ASSERT_EQ(2, cache_->Size());
  ASSERT_TRUE(cache_->Contains(data_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(data_path_0_));
  ASSERT_TRUE(cache_->Contains(cf_path_0_));
  ASSERT_EQ(1, cache_->GetRefCount(cf_path_0_));
  ASSERT_OK(DestroyDB(dbname_ + "/db0", options, {{"cf", cf_options}}));

  ASSERT_OK(db1->DestroyColumnFamilyHandle(cf1));
  delete db1;
  ASSERT_EQ(0, cache_->Size());
  ASSERT_OK(DestroyDB(dbname_ + "/db1", options, {{"cf", cf_options}}));
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // OS_LINUX

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
