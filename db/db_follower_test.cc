//  Copyright (c) 2024-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

#ifdef OS_LINUX

class DBFollowerTest : public DBTestBase {
 public:
  // Create directories for leader and follower
  // Create the leader DB object
  DBFollowerTest() : DBTestBase("/db_follower_test", /*env_do_fsync*/ false) {
    follower_name_ = dbname_ + "/follower";
    Close();
    Destroy(CurrentOptions());
    EXPECT_EQ(env_->CreateDirIfMissing(dbname_), Status::OK());
    dbname_ = dbname_ + "/leader";
    Reopen(CurrentOptions());
  }

  ~DBFollowerTest() {
    follower_.reset();
    EXPECT_EQ(DestroyDB(follower_name_, CurrentOptions()), Status::OK());
  }

 protected:
  Status OpenAsFollower() {
    return DB::OpenAsFollower(CurrentOptions(), follower_name_, dbname_,
                              &follower_);
  }
  DB* follower() { return follower_.get(); }

 private:
  std::string follower_name_;
  std::unique_ptr<DB> follower_;
};

TEST_F(DBFollowerTest, Basic) {
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Flush());

  ASSERT_OK(OpenAsFollower());
  std::string val;
  ASSERT_OK(follower()->Get(ReadOptions(), "k1", &val));
  ASSERT_EQ(val, "v1");
}

#endif
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
