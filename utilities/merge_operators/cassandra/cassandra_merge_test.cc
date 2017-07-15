// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// This source code is also licensed under the GPLv2 license found in the
// COPYING file in the root directory of this source tree.

#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "util/testharness.h"
#include "util/random.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/cassandra/merge_operator.h"
#include "utilities/merge_operators/cassandra/test_utils.h"

using namespace rocksdb;

namespace rocksdb {
namespace cassandra {

// Path to the database on file system
const std::string kDbName = test::TmpDir() + "/cassandramerge_test";

class CassandraStore {
 public:
  explicit CassandraStore(std::shared_ptr<DB> db)
      : db_(db),
        merge_option_(),
        get_option_() {
    assert(db);
  }

  bool Append(const std::string& key, const RowValue& val){
    std::string result;
    val.Serialize(&result);
    Slice valSlice(result.data(), result.size());
    auto s = db_->Merge(merge_option_, key, valSlice);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << "ERROR " << s.ToString() << std::endl;
      return false;
    }
  }

  std::tuple<bool, RowValue> Get(const std::string& key){
    std::string result;
    auto s = db_->Get(get_option_, key, &result);

    if (s.ok()) {
      return std::make_tuple(true,
                             RowValue::Deserialize(result.data(),
                                                   result.size()));
    }

    if (!s.IsNotFound()) {
      std::cerr << "ERROR " << s.ToString() << std::endl;
    }

    return std::make_tuple(false, RowValue(0, 0));
  }

 private:
  std::shared_ptr<DB> db_;
  WriteOptions merge_option_;
  ReadOptions get_option_;
};


// The class for unit-testing
class CassandraMergeTest : public testing::Test {
 public:
  CassandraMergeTest() {
    DestroyDB(kDbName, Options());    // Start each test with a fresh DB
  }

  std::shared_ptr<DB> OpenDb() {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.merge_operator.reset(new CassandraValueMergeOperator());
    EXPECT_OK(DB::Open(options, kDbName, &db));
    return std::shared_ptr<DB>(db);
  }
};

// THE TEST CASES BEGIN HERE

TEST_F(CassandraMergeTest, SimpleTest) {
  auto db = OpenDb();
  CassandraStore store(db);

  store.Append("k1", CreateTestRowValue({
    std::make_tuple(kTombstone, 0, 5),
    std::make_tuple(kColumn, 1, 8),
    std::make_tuple(kExpiringColumn, 2, 5),
  }));
  store.Append("k1",CreateTestRowValue({
    std::make_tuple(kColumn, 0, 2),
    std::make_tuple(kExpiringColumn, 1, 5),
    std::make_tuple(kTombstone, 2, 7),
    std::make_tuple(kExpiringColumn, 7, 17),
  }));
  store.Append("k1", CreateTestRowValue({
    std::make_tuple(kExpiringColumn, 0, 6),
    std::make_tuple(kTombstone, 1, 5),
    std::make_tuple(kColumn, 2, 4),
    std::make_tuple(kTombstone, 11, 11),
  }));

  auto ret = store.Get("k1");

  ASSERT_TRUE(std::get<0>(ret));
  RowValue& merged = std::get<1>(ret);
  EXPECT_EQ(merged.columns_.size(), 5);
  VerifyRowValueColumns(merged.columns_, 0, kExpiringColumn, 0, 6);
  VerifyRowValueColumns(merged.columns_, 1, kColumn, 1, 8);
  VerifyRowValueColumns(merged.columns_, 2, kTombstone, 2, 7);
  VerifyRowValueColumns(merged.columns_, 3, kExpiringColumn, 7, 17);
  VerifyRowValueColumns(merged.columns_, 4, kTombstone, 11, 11);
}


} // namespace cassandra
} // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
