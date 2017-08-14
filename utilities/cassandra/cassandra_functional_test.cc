// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iostream>
#include "rocksdb/db.h"
#include "db/db_impl.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "util/testharness.h"
#include "util/random.h"
#include "utilities/merge_operators.h"
#include "utilities/cassandra/cassandra_compaction_filter.h"
#include "utilities/cassandra/merge_operator.h"
#include "utilities/cassandra/test_utils.h"

using namespace rocksdb;

namespace rocksdb {
namespace cassandra {

// Path to the database on file system
const std::string kDbName = test::TmpDir() + "/cassandra_functional_test";

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

  void Flush() {
    dbfull()->TEST_FlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }

  void Compact() {
    dbfull()->TEST_CompactRange(
      0, nullptr, nullptr, db_->DefaultColumnFamily());
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

  DBImpl* dbfull() { return reinterpret_cast<DBImpl*>(db_.get()); }

};

class TestCompactionFilterFactory : public CompactionFilterFactory {
public:
  explicit TestCompactionFilterFactory(bool purge_ttl_on_expiration)
    : purge_ttl_on_expiration_(purge_ttl_on_expiration) {}

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    return unique_ptr<CompactionFilter>(new CassandraCompactionFilter(purge_ttl_on_expiration_));
  }

  virtual const char* Name() const override {
    return "TestCompactionFilterFactory";
  }

private:
  bool purge_ttl_on_expiration_;
};


// The class for unit-testing
class CassandraFunctionalTest : public testing::Test {
public:
  CassandraFunctionalTest() {
    DestroyDB(kDbName, Options());    // Start each test with a fresh DB
  }

  std::shared_ptr<DB> OpenDb() {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.merge_operator.reset(new CassandraValueMergeOperator());
    auto* cf_factory = new TestCompactionFilterFactory(purge_ttl_on_expiration_);
    options.compaction_filter_factory.reset(cf_factory);
    EXPECT_OK(DB::Open(options, kDbName, &db));
    return std::shared_ptr<DB>(db);
  }

  bool purge_ttl_on_expiration_ = false;
};

// THE TEST CASES BEGIN HERE

TEST_F(CassandraFunctionalTest, SimpleMergeTest) {
  CassandraStore store(OpenDb());

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

TEST_F(CassandraFunctionalTest,
       CompactionShouldConvertExpiredColumnsToTombstone) {
  CassandraStore store(OpenDb());
  int64_t now= time(nullptr);

  store.Append("k1", CreateTestRowValue({
    std::make_tuple(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 20)), //expired
    std::make_tuple(kExpiringColumn, 1, ToMicroSeconds(now - kTtl + 10)), // not expired
    std::make_tuple(kTombstone, 3, ToMicroSeconds(now))
  }));

  store.Flush();

  store.Append("k1",CreateTestRowValue({
    std::make_tuple(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 10)), //expired
    std::make_tuple(kColumn, 2, ToMicroSeconds(now))
  }));

  store.Flush();
  store.Compact();

  auto ret = store.Get("k1");
  ASSERT_TRUE(std::get<0>(ret));
  RowValue& merged = std::get<1>(ret);
  EXPECT_EQ(merged.columns_.size(), 4);
  VerifyRowValueColumns(merged.columns_, 0, kTombstone, 0, ToMicroSeconds(now - 10));
  VerifyRowValueColumns(merged.columns_, 1, kExpiringColumn, 1, ToMicroSeconds(now - kTtl + 10));
  VerifyRowValueColumns(merged.columns_, 2, kColumn, 2, ToMicroSeconds(now));
  VerifyRowValueColumns(merged.columns_, 3, kTombstone, 3, ToMicroSeconds(now));
}


TEST_F(CassandraFunctionalTest,
       CompactionShouldPurgeExpiredColumnsIfPurgeTtlIsOn) {
  purge_ttl_on_expiration_ = true;
  CassandraStore store(OpenDb());
  int64_t now = time(nullptr);

  store.Append("k1", CreateTestRowValue({
    std::make_tuple(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 20)), //expired
    std::make_tuple(kExpiringColumn, 1, ToMicroSeconds(now)), // not expired
    std::make_tuple(kTombstone, 3, ToMicroSeconds(now))
  }));

  store.Flush();

  store.Append("k1",CreateTestRowValue({
    std::make_tuple(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 10)), //expired
    std::make_tuple(kColumn, 2, ToMicroSeconds(now))
  }));

  store.Flush();
  store.Compact();

  auto ret = store.Get("k1");
  ASSERT_TRUE(std::get<0>(ret));
  RowValue& merged = std::get<1>(ret);
  EXPECT_EQ(merged.columns_.size(), 3);
  VerifyRowValueColumns(merged.columns_, 0, kExpiringColumn, 1, ToMicroSeconds(now));
  VerifyRowValueColumns(merged.columns_, 1, kColumn, 2, ToMicroSeconds(now));
  VerifyRowValueColumns(merged.columns_, 2, kTombstone, 3, ToMicroSeconds(now));
}

TEST_F(CassandraFunctionalTest,
       CompactionShouldRemoveRowWhenAllColumnsExpiredIfPurgeTtlIsOn) {
  purge_ttl_on_expiration_ = true;
  CassandraStore store(OpenDb());
  int64_t now = time(nullptr);

  store.Append("k1", CreateTestRowValue({
    std::make_tuple(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 20)),
    std::make_tuple(kExpiringColumn, 1, ToMicroSeconds(now - kTtl - 20)),
  }));

  store.Flush();

  store.Append("k1",CreateTestRowValue({
    std::make_tuple(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 10)),
  }));

  store.Flush();
  store.Compact();
  ASSERT_FALSE(std::get<0>(store.Get("k1")));
}

} // namespace cassandra
} // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
