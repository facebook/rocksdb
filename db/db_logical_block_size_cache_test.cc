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
        cf_path_1_(dbname_ + "/cf_path_1"),
        kDirFds({
          {dbname_, 0},
          {data_path_0_, 10},
          {data_path_1_, 11},
          {cf_path_0_, 20},
          {cf_path_1_, 21}
        }) {
    auto get_fd_block_size = [&](int fd) {
      ncall_++;
      return fd;
    };
    auto get_dir_block_size = [&](const std::string& dir, size_t* size) {
      ncall_++;
      *size = kDirFds.at(dir);
      return Status::OK();
    };
    cache_.reset(new LogicalBlockSizeCache(
        get_fd_block_size, get_dir_block_size));
    env_.reset(new EnvWithCustomLogicalBlockSizeCache(
        Env::Default(), cache_.get()));
  }

 protected:
  std::string dbname_;
  std::string data_path_0_;
  std::string data_path_1_;
  std::string cf_path_0_;
  std::string cf_path_1_;

  const std::map<std::string, int> kDirFds;

  int ncall_ = 0;
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
    ncall_ = 0;
    DB* db;
    if (!i) {
      printf("Open\n");
      ASSERT_OK(DB::Open(options, dbname_, &db));
    } else {
      printf("OpenForReadOnly\n");
      ASSERT_OK(DB::OpenForReadOnly(options, dbname_, &db));
    }
    // Logical block size of data paths are cached during Open.
    ASSERT_EQ(2, ncall_);
    ASSERT_EQ(10, cache_->GetLogicalBlockSize(data_path_0_ + "/sst", 100));
    ASSERT_EQ(2, ncall_);
    ASSERT_EQ(11, cache_->GetLogicalBlockSize(data_path_1_ + "/sst", 200));
    ASSERT_EQ(2, ncall_);
    ASSERT_OK(db->Close());
    ASSERT_EQ(2, ncall_);
    // Logical block size of data paths are removed from cache during Close.
    ASSERT_EQ(100, cache_->GetLogicalBlockSize(data_path_0_ + "/sst", 100));
    ASSERT_EQ(3, ncall_);
    ASSERT_EQ(200, cache_->GetLogicalBlockSize(data_path_1_ + "/sst", 200));
    ASSERT_EQ(4, ncall_);
    delete db;
  }
}

TEST_F(DBLogicalBlockSizeCacheTest, OpenDelete) {
  // Tests that Open will cache the logical block size for data paths,
  // and delete the db pointer will remove the cached sizes.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();

  DB* db;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  // Logical block size of dbname_ is cached during Open.
  ASSERT_EQ(1, ncall_);
  ASSERT_EQ(0, cache_->GetLogicalBlockSize(dbname_ + "/sst", 100));
  ASSERT_EQ(1, ncall_);

  delete db;
  ASSERT_EQ(1, ncall_);
  // Logical block size of dbname_ is removed from cache during Close.
  ASSERT_EQ(100, cache_->GetLogicalBlockSize(dbname_ + "/sst", 100));
  ASSERT_EQ(2, ncall_);
}

TEST_F(DBLogicalBlockSizeCacheTest, CreateColumnFamily) {
  // Tests that CreateColumnFamily will cache the cf_paths,
  // drop the column family handle won't drop the cache,
  // drop and then delete the column family handle will drop the cache.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();

  DB* db;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  ASSERT_EQ(1, ncall_);

  ColumnFamilyOptions cf_options;
  cf_options.cf_paths = {{cf_path_0_, 1024}, {cf_path_1_, 2048}};
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_OK(db->CreateColumnFamily(cf_options, "cf", &cf));
  // cf_path_0_ and cf_path_1_ are cached.
  ASSERT_EQ(3, ncall_);
  ASSERT_EQ(20, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst", 100));
  ASSERT_EQ(3, ncall_);
  ASSERT_EQ(21, cache_->GetLogicalBlockSize(cf_path_1_ + "/sst", 200));
  ASSERT_EQ(3, ncall_);

  // Drop column family does not drop cache.
  ASSERT_OK(db->DropColumnFamily(cf));
  ASSERT_EQ(20, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst1", 300));
  ASSERT_EQ(3, ncall_);
  ASSERT_EQ(21, cache_->GetLogicalBlockSize(cf_path_1_ + "/sst1", 400));
  ASSERT_EQ(3, ncall_);

  // Delete handle will drop cache.
  ASSERT_OK(db->DestroyColumnFamilyHandle(cf));
  ASSERT_EQ(100, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst", 100));
  ASSERT_EQ(4, ncall_);
  ASSERT_EQ(200, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst", 200));
  ASSERT_EQ(5, ncall_);

  delete db;
}

TEST_F(DBLogicalBlockSizeCacheTest, CreateColumnFamilies) {
  // Tests that CreateColumnFamilies will cache the cf_paths,
  // drop the column family handle won't drop the cache,
  // drop and then delete the column family handle will drop the cache.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();

  DB* db;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  ASSERT_EQ(1, ncall_);

  ColumnFamilyOptions cf_options;
  cf_options.cf_paths = {{cf_path_0_, 1024}};
  std::vector<ColumnFamilyHandle*> cfs;
  ASSERT_OK(db->CreateColumnFamilies(cf_options, {"cf1", "cf2"}, &cfs));
  // cf_path_0_ is cached.
  ASSERT_EQ(2, ncall_);
  ASSERT_EQ(20, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst", 100));
  ASSERT_EQ(2, ncall_);

  // Drop column family does not drop cache.
  for (ColumnFamilyHandle* cf : cfs) {
    ASSERT_OK(db->DropColumnFamily(cf));
    ASSERT_EQ(20, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst1", 200));
    ASSERT_EQ(2, ncall_);
  }

  // Delete one handle will not drop cache because another handle is still
  // referencing cf_path_0_.
  ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[0]));
  ASSERT_EQ(20, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst2", 300));
  ASSERT_EQ(2, ncall_);

  // Delete the last handle will drop cache.
  ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[1]));
  ASSERT_EQ(100, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst", 100));
  ASSERT_EQ(3, ncall_);

  delete db;
}

TEST_F(DBLogicalBlockSizeCacheTest, OpenWithColumnFamilies) {
  // Tests that Open two column families with the same cf_path will cache the
  // cf_path and have 2 references to the cached size,
  // drop the column family handle won't drop the cache,
  // drop and then delete the column family handle will drop the cache.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();

  ColumnFamilyOptions default_cf_options;
  default_cf_options.cf_paths = {{dbname_, 2048}};

  for (int i = 0; i < 2; i++) {
    DB* db;
    ColumnFamilyHandle* cf1 = nullptr;
    ColumnFamilyHandle* cf2 = nullptr;
    ASSERT_OK(DB::Open(options, dbname_, &db));
    ColumnFamilyOptions cf_options;
    cf_options.cf_paths = {{cf_path_0_, 1024}};
    ASSERT_OK(db->CreateColumnFamily(cf_options, "cf1", &cf1));
    ASSERT_OK(db->CreateColumnFamily(cf_options, "cf2", &cf2));
    ASSERT_OK(db->DestroyColumnFamilyHandle(cf1));
    ASSERT_OK(db->DestroyColumnFamilyHandle(cf2));
    delete db;

    // Neither dbname_ nor cf_path_0_ is cached.
    ASSERT_EQ(100, cache_->GetLogicalBlockSize(dbname_ + "/sst", 100));
    ASSERT_EQ(200, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst", 200));

    ncall_ = 0;
    std::vector<ColumnFamilyHandle*> cfs;
    if (!i) {
      printf("Open\n");
      ASSERT_OK(DB::Open(options, dbname_,
          {{"cf1", cf_options},
          {"cf2", cf_options},
          {"default", default_cf_options}},
          &cfs, &db));
    } else {
      printf("OpenForReadOnly\n");
      ASSERT_OK(DB::OpenForReadOnly(options, dbname_,
          {{"cf1", cf_options},
          {"cf2", cf_options},
          {"default", default_cf_options}},
          &cfs, &db));
    }

    // Logical block sizes of dbname_ and cf_path_0_ are cached during Open.
    ASSERT_EQ(2, ncall_);
    ASSERT_EQ(0, cache_->GetLogicalBlockSize(dbname_ + "/sst", 100));
    ASSERT_EQ(2, ncall_);
    ASSERT_EQ(20, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst", 200));
    ASSERT_EQ(2, ncall_);

    // Drop handles won't drop the cache.
    ASSERT_OK(db->DropColumnFamily(cfs[0]));
    ASSERT_OK(db->DropColumnFamily(cfs[1]));
    ASSERT_EQ(20, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst1", 300));
    ASSERT_EQ(2, ncall_);

    // Delete one handle won't drop the cache.
    ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[0]));
    ASSERT_EQ(20, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst2", 400));
    ASSERT_EQ(2, ncall_);

    // Delete the last handle will drop the cache.
    ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[1]));
    ASSERT_EQ(100, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst", 100));
    ASSERT_EQ(3, ncall_);

    ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[2]));
    // db is not deleted yet, so dbname_ is still cached.
    ASSERT_EQ(0, cache_->GetLogicalBlockSize(dbname_ + "/sst1", 400));
    ASSERT_EQ(3, ncall_);

    // db is deleted, cache for dbname_ is dropped.
    delete db;
    ASSERT_EQ(100, cache_->GetLogicalBlockSize(dbname_ + "/sst", 100));
    ASSERT_EQ(4, ncall_);
  }
}

TEST_F(DBLogicalBlockSizeCacheTest, DestroyColumnFamilyHandle) {
  // Tests that destroy column family without dropping won't drop the cache,
  // because compaction and flush might still need to get logical block size
  // when opening new files.
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();

  DB* db;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  ColumnFamilyOptions cf_options;
  cf_options.cf_paths = {{cf_path_0_, 1024}};
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_OK(db->CreateColumnFamily(cf_options, "cf", &cf));
  // cf_path_0_ is cached.
  ASSERT_EQ(2, ncall_);
  ASSERT_EQ(20, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst", 100));
  ASSERT_EQ(2, ncall_);

  // Delete handle won't drop cache.
  ASSERT_OK(db->DestroyColumnFamilyHandle(cf));
  ASSERT_EQ(20, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst1", 200));
  ASSERT_EQ(2, ncall_);

  delete db;

  ColumnFamilyOptions default_cf_options;
  default_cf_options.cf_paths = {{dbname_, 2048}};

  std::vector<ColumnFamilyHandle*> cfs;
  for (int i = 0; i < 2; i++) {
    ncall_ = 0;
    if (!i) {
      printf("Open\n");
      ASSERT_OK(DB::Open(options, dbname_,
          {{"cf", cf_options},
          {"default", default_cf_options}},
          &cfs, &db));
    } else {
      printf("OpenForReadOnly\n");
      ASSERT_OK(DB::OpenForReadOnly(options, dbname_,
          {{"cf", cf_options},
          {"default", default_cf_options}},
          &cfs, &db));
    }
    // cf_path_0_ and dbname_ are cached.
    ASSERT_EQ(2, ncall_);
    ASSERT_EQ(20, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst", 100));
    ASSERT_EQ(2, ncall_);
    ASSERT_EQ(0, cache_->GetLogicalBlockSize(dbname_ + "/sst", 101));
    ASSERT_EQ(2, ncall_);

    // Deleting handle won't drop cache.
    ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[0]));
    ASSERT_EQ(20, cache_->GetLogicalBlockSize(cf_path_0_ + "/sst1", 200));
    ASSERT_EQ(2, ncall_);
    ASSERT_OK(db->DestroyColumnFamilyHandle(cfs[1]));
    ASSERT_EQ(0, cache_->GetLogicalBlockSize(dbname_ + "/sst1", 201));
    ASSERT_EQ(2, ncall_);

    delete db;
  }
}

}  // namespace ROCKSDB_NAMESPACE
#endif // OS_LINUX

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
