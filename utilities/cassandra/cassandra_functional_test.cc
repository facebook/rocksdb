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
#include "util/string_util.h"
#include "utilities/merge_operators.h"
#include "utilities/cassandra/cassandra_compaction_filter.h"
#include "utilities/cassandra/merge_operator.h"
#include "utilities/cassandra/test_utils.h"

using namespace rocksdb;

namespace rocksdb {
namespace cassandra {

// Path to the database on file system
const std::string kDbName = test::PerThreadDBPath("cassandra_functional_test");

class CassandraStore {
 public:
  explicit CassandraStore(std::shared_ptr<DB> db)
      : db_(db), write_option_(), get_option_() {
    assert(db);
  }

  bool Append(const std::string& key, const RowValue& val){
    std::string result;
    val.Serialize(&result);
    Slice valSlice(result.data(), result.size());
    auto s = db_->Merge(write_option_, key, valSlice);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << "ERROR " << s.ToString() << std::endl;
      return false;
    }
  }

  bool Put(const std::string& key, const RowValue& val) {
    std::string result;
    val.Serialize(&result);
    Slice valSlice(result.data(), result.size());
    auto s = db_->Put(write_option_, key, valSlice);
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
  WriteOptions write_option_;
  ReadOptions get_option_;

  DBImpl* dbfull() { return reinterpret_cast<DBImpl*>(db_.get()); }
};
// The class for unit-testing
class CassandraFunctionalTest : public testing::Test {
public:
  static std::string GetGracePeriod(int32_t period) {
    std::string property = "gc_grace_seconds";
    return property + "=" + ToString(period);
  }
  
  static std::string GetTTL(bool purge) {
    std::string property = "purge_ttl_on_expiration";
    return property + ((purge) ? "=true" : "=false");
  }
  
  static std::string GetMergeOperands(size_t limit) {
    std::string property = "merge_operands_limit";
    return property + "=" + ToString(limit);
  }			       
  
public:
  CassandraFunctionalTest() {
    DestroyDB(kDbName, Options());    // Start each test with a fresh DB
  }

  std::shared_ptr<DB> OpenDb() {
    DB* db;
    Options options;
    options.AddExtensionLibrary("", "loadCassandraExtensions", "cassandra");
    options.create_if_missing = true;
    options.SetMergeOperator("cassandra", GetGracePeriod(gc_grace_period_in_seconds_));
    options.SetCompactionFilterFactory("cassandra",
				       GetGracePeriod(gc_grace_period_in_seconds_) + ";" +
				       GetTTL(purge_ttl_on_expiration_));
    options.merge_operator.reset(new CassandraValueMergeOperator(gc_grace_period_in_seconds_));
    auto* cf_factory = new CassandraCompactionFilterFactory(
        purge_ttl_on_expiration_, gc_grace_period_in_seconds_);
    options.compaction_filter_factory.reset(cf_factory);
    EXPECT_OK(DB::Open(options, kDbName, &db));
    return std::shared_ptr<DB>(db);
  }

  bool purge_ttl_on_expiration_ = false;
  int32_t gc_grace_period_in_seconds_ = 100;
};

// THE TEST CASES BEGIN HERE

TEST_F(CassandraFunctionalTest, SimpleMergeTest) {
  CassandraStore store(OpenDb());
  int64_t now = time(nullptr);

  store.Append("k1", CreateTestRowValue({
    CreateTestColumnSpec(kTombstone, 0, ToMicroSeconds(now + 5)),
    CreateTestColumnSpec(kColumn, 1, ToMicroSeconds(now + 8)),
    CreateTestColumnSpec(kExpiringColumn, 2, ToMicroSeconds(now + 5)),
  }));
  store.Append("k1",CreateTestRowValue({
    CreateTestColumnSpec(kColumn, 0, ToMicroSeconds(now + 2)),
    CreateTestColumnSpec(kExpiringColumn, 1, ToMicroSeconds(now + 5)),
    CreateTestColumnSpec(kTombstone, 2, ToMicroSeconds(now + 7)),
    CreateTestColumnSpec(kExpiringColumn, 7, ToMicroSeconds(now + 17)),
  }));
  store.Append("k1", CreateTestRowValue({
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now + 6)),
    CreateTestColumnSpec(kTombstone, 1, ToMicroSeconds(now + 5)),
    CreateTestColumnSpec(kColumn, 2, ToMicroSeconds(now + 4)),
    CreateTestColumnSpec(kTombstone, 11, ToMicroSeconds(now + 11)),
  }));

  auto ret = store.Get("k1");

  ASSERT_TRUE(std::get<0>(ret));
  RowValue& merged = std::get<1>(ret);
  EXPECT_EQ(merged.columns_.size(), 5);
  VerifyRowValueColumns(merged.columns_, 0, kExpiringColumn, 0, ToMicroSeconds(now + 6));
  VerifyRowValueColumns(merged.columns_, 1, kColumn, 1, ToMicroSeconds(now + 8));
  VerifyRowValueColumns(merged.columns_, 2, kTombstone, 2, ToMicroSeconds(now + 7));
  VerifyRowValueColumns(merged.columns_, 3, kExpiringColumn, 7, ToMicroSeconds(now + 17));
  VerifyRowValueColumns(merged.columns_, 4, kTombstone, 11, ToMicroSeconds(now + 11));
}

TEST_F(CassandraFunctionalTest,
       CompactionShouldConvertExpiredColumnsToTombstone) {
  CassandraStore store(OpenDb());
  int64_t now= time(nullptr);

  store.Append("k1", CreateTestRowValue({
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 20)), //expired
    CreateTestColumnSpec(kExpiringColumn, 1, ToMicroSeconds(now - kTtl + 10)), // not expired
    CreateTestColumnSpec(kTombstone, 3, ToMicroSeconds(now))
  }));

  store.Flush();

  store.Append("k1",CreateTestRowValue({
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 10)), //expired
    CreateTestColumnSpec(kColumn, 2, ToMicroSeconds(now))
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
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 20)), //expired
    CreateTestColumnSpec(kExpiringColumn, 1, ToMicroSeconds(now)), // not expired
    CreateTestColumnSpec(kTombstone, 3, ToMicroSeconds(now))
  }));

  store.Flush();

  store.Append("k1",CreateTestRowValue({
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 10)), //expired
    CreateTestColumnSpec(kColumn, 2, ToMicroSeconds(now))
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
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 20)),
    CreateTestColumnSpec(kExpiringColumn, 1, ToMicroSeconds(now - kTtl - 20)),
  }));

  store.Flush();

  store.Append("k1",CreateTestRowValue({
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 10)),
  }));

  store.Flush();
  store.Compact();
  ASSERT_FALSE(std::get<0>(store.Get("k1")));
}

TEST_F(CassandraFunctionalTest,
       CompactionShouldRemoveTombstoneExceedingGCGracePeriod) {
  purge_ttl_on_expiration_ = true;
  CassandraStore store(OpenDb());
  int64_t now = time(nullptr);

  store.Append("k1", CreateTestRowValue({
    CreateTestColumnSpec(kTombstone, 0, ToMicroSeconds(now - gc_grace_period_in_seconds_ - 1)),
    CreateTestColumnSpec(kColumn, 1, ToMicroSeconds(now))
  }));

  store.Append("k2", CreateTestRowValue({
    CreateTestColumnSpec(kColumn, 0, ToMicroSeconds(now))
  }));

  store.Flush();

  store.Append("k1",CreateTestRowValue({
    CreateTestColumnSpec(kColumn, 1, ToMicroSeconds(now)),
  }));

  store.Flush();
  store.Compact();

  auto ret = store.Get("k1");
  ASSERT_TRUE(std::get<0>(ret));
  RowValue& gced = std::get<1>(ret);
  EXPECT_EQ(gced.columns_.size(), 1);
  VerifyRowValueColumns(gced.columns_, 0, kColumn, 1, ToMicroSeconds(now));
}

TEST_F(CassandraFunctionalTest, CompactionShouldRemoveTombstoneFromPut) {
  purge_ttl_on_expiration_ = true;
  CassandraStore store(OpenDb());
  int64_t now = time(nullptr);

  store.Put("k1", CreateTestRowValue({
    CreateTestColumnSpec(kTombstone, 0, ToMicroSeconds(now - gc_grace_period_in_seconds_ - 1)),
  }));

  store.Flush();
  store.Compact();
  ASSERT_FALSE(std::get<0>(store.Get("k1")));
}

TEST_F(CassandraFunctionalTest, LoadMergeOperator) {
    Options options;
    ASSERT_OK(options.AddExtensionLibrary("", "loadCassandraExtensions", "cassandra"));
    std::string cassandraPrefix = "cassandra.rocksdb.";
    ASSERT_OK(options.SetMergeOperator("cassandra", GetGracePeriod(gc_grace_period_in_seconds_)));
    ASSERT_NE(options.merge_operator, nullptr);
    ASSERT_EQ(options.merge_operator->Name(), std::string("CassandraValueMergeOperator"));
    ASSERT_OK(options.merge_operator->ConfigureFromString(cassandraPrefix + GetGracePeriod(gc_grace_period_in_seconds_)));
    ASSERT_OK(options.merge_operator->ConfigureFromString(cassandraPrefix + GetMergeOperands(123)));
    ASSERT_EQ(options.merge_operator->ConfigureFromString(cassandraPrefix + GetTTL(true)), Status::NotFound());
}


TEST_F(CassandraFunctionalTest, LoadCompactionFilterFactory) {
    Options options;
    ASSERT_OK(options.AddExtensionLibrary("", "loadCassandraExtensions", "cassandra"));
    std::string cassandraPrefix = "cassandra.rocksdb.";
    ASSERT_OK(options.SetCompactionFilterFactory("cassandra", GetGracePeriod(gc_grace_period_in_seconds_)));
    ASSERT_NE(options.compaction_filter_factory, nullptr);
    ASSERT_EQ(options.compaction_filter_factory->Name(), std::string("CassandraCompactionFilterFactory"));
    ASSERT_OK(options.compaction_filter_factory->ConfigureFromString(cassandraPrefix + GetGracePeriod(111)));
    ASSERT_OK(options.compaction_filter_factory->ConfigureFromString(cassandraPrefix + GetTTL(true)));
    ASSERT_EQ(options.compaction_filter_factory->ConfigureFromString(cassandraPrefix + GetMergeOperands(123)),
	      Status::NotFound());
}

TEST_F(CassandraFunctionalTest, LoadCompactionFilter) {
    Options options;
    ASSERT_OK(options.AddExtensionLibrary("", "loadCassandraExtensions", "cassandra"));
    std::string cassandraPrefix = "cassandra.rocksdb.";
    ASSERT_OK(options.SetCompactionFilter("cassandra", GetGracePeriod(gc_grace_period_in_seconds_)));
    ASSERT_NE(options.compaction_filter, nullptr);
    ASSERT_EQ(options.compaction_filter->Name(), std::string("CassandraCompactionFilter"));
    const CompactionFilter *old_filter = options.compaction_filter;
    ASSERT_EQ(options.SetCompactionFilter("cassandra", cassandraPrefix + GetMergeOperands(123)), Status::NotFound());
    ASSERT_EQ(options.compaction_filter, old_filter);
    
    ASSERT_OK(options.SetCompactionFilter("cassandra", cassandraPrefix + GetGracePeriod(111) + ";" + GetTTL(true)));
    ASSERT_NE(options.compaction_filter, nullptr);
    ASSERT_NE(options.compaction_filter, old_filter);
    ASSERT_EQ(options.compaction_filter->Name(), std::string("CassandraCompactionFilter"));

    delete old_filter;
    delete options.compaction_filter;
}
} // namespace cassandra
} // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
