//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

/**
 * This file has no main() - it contributes tests to merge_operators_test
 *
 */

#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "test_util/testharness.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

class Int64AddMergeOperatorTest : public testing::Test {
 public:
  Int64AddMergeOperatorTest() {
    options_.merge_operator = MergeOperators::CreateFromStringId("int64add");
    options_.create_if_missing = true;
    dbname_ = test::PerThreadDBPath("int64add_merge_operator_test");
    DestroyDB(dbname_, options_);
  }

  ~Int64AddMergeOperatorTest() {
    if (db_ != nullptr) {
      delete db_;
      DestroyDB(dbname_, options_);
    }
  }

  Status OpenDB() { return DB::Open(options_, dbname_, &db_); }

 public:
  DB* db_;
  std::string dbname_;
  Options options_;
  WriteOptions write_opts_;
  ReadOptions read_opts_;
};

class EmptyDbTest : public Int64AddMergeOperatorTest,
                    public testing::WithParamInterface<int64_t> {
 public:
  EmptyDbTest() : Int64AddMergeOperatorTest() {}
};

class NonEmptyDbTest
    : public Int64AddMergeOperatorTest,
      public testing::WithParamInterface<std::tuple<int64_t, int64_t>> {
 public:
  NonEmptyDbTest() : Int64AddMergeOperatorTest() {}
};

TEST_P(EmptyDbTest, MergeEmptyDb) {
  const int64_t merge_num = GetParam();

  std::string value;

  ASSERT_OK(OpenDB());

  PutVarsignedint64(&value, merge_num);
  Status s =
      db_->Merge(write_opts_, "key", value);  // Merging merge_num under key
  ASSERT_OK(s);

  value.clear();

  s = db_->Get(read_opts_, "key", &value);
  ASSERT_OK(s);
  Slice read_slice(value);
  int64_t read_value = 0;
  const bool read_ok = GetVarsignedint64(&read_slice, &read_value);

  ASSERT_TRUE(read_ok);
  const int64_t expected = 0 + merge_num;
  ASSERT_EQ(read_value,
            expected);  // Merge operators should have been applied on empty db,
                        // i.e. 0 and then added merge_num.
}

TEST_P(EmptyDbTest, MergeEmptyDbCf) {
  const int64_t merge_num = GetParam();

  std::string value;

  ASSERT_OK(OpenDB());
  ColumnFamilyOptions cf_opts;
  cf_opts.merge_operator = options_.merge_operator;
  ColumnFamilyHandle* cf1;
  Status s = db_->CreateColumnFamily(cf_opts, "cf1", &cf1);
  ASSERT_OK(s);

  PutVarsignedint64(&value, merge_num);
  s = db_->Merge(write_opts_, cf1, "key",
                 value);  // Merging merge_num under key
  ASSERT_OK(s);

  value.clear();

  s = db_->Get(read_opts_, cf1, "key", &value);
  ASSERT_OK(s);
  Slice read_slice(value);
  int64_t read_value = 0;
  const bool read_ok = GetVarsignedint64(&read_slice, &read_value);

  ASSERT_OK(db_->DropColumnFamily(cf1));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf1));

  ASSERT_TRUE(read_ok);
  const int64_t expected = 0 + merge_num;
  ASSERT_EQ(read_value,
            expected);  // Merge operators should have been applied on empty db,
                        // i.e. 0 and then added merge_num.
}

TEST_P(NonEmptyDbTest, MergeNonEmptyDb) {
  const int64_t initial_db_num = std::get<0>(GetParam());
  const int64_t merge_num = std::get<1>(GetParam());

  std::string value;

  ASSERT_OK(OpenDB());

  PutVarsignedint64(&value, initial_db_num);
  Status s =
      db_->Put(write_opts_, "key", value);  // Put initial_db_num under key
  ASSERT_OK(s);

  value.clear();

  PutVarsignedint64(&value, merge_num);
  s = db_->Merge(write_opts_, "key", value);  // Merging merge_num under key
  ASSERT_OK(s);

  value.clear();

  s = db_->Get(read_opts_, "key", &value);
  ASSERT_OK(s);
  Slice read_slice(value);
  int64_t read_value = 0;
  const bool read_ok = GetVarsignedint64(&read_slice, &read_value);

  ASSERT_TRUE(read_ok);
  const int64_t expected = initial_db_num + merge_num;
  ASSERT_EQ(read_value,
            expected);  // Merge operators should have been applied on non-empty
                        // db, and then merge added merge_num.
}

TEST_P(NonEmptyDbTest, MergeNonEmptyDbCf) {
  const int64_t initial_db_num = std::get<0>(GetParam());
  const int64_t merge_num = std::get<1>(GetParam());

  std::string value;

  ASSERT_OK(OpenDB());
  ColumnFamilyOptions cf_opts;
  cf_opts.merge_operator = options_.merge_operator;
  ColumnFamilyHandle* cf1;
  Status s = db_->CreateColumnFamily(cf_opts, "cf1", &cf1);

  PutVarsignedint64(&value, initial_db_num);
  s = db_->Put(write_opts_, cf1, "key", value);  // Put initial_db_num under key
  ASSERT_OK(s);

  value.clear();

  PutVarsignedint64(&value, merge_num);
  s = db_->Merge(write_opts_, cf1, "key",
                 value);  // Merging merge_num under key
  ASSERT_OK(s);

  value.clear();

  s = db_->Get(read_opts_, cf1, "key", &value);
  ASSERT_OK(s);
  Slice read_slice(value);
  int64_t read_value = 0;
  const bool read_ok = GetVarsignedint64(&read_slice, &read_value);

  ASSERT_OK(db_->DropColumnFamily(cf1));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf1));

  ASSERT_TRUE(read_ok);
  const int64_t expected = initial_db_num + merge_num;
  ASSERT_EQ(read_value,
            expected);  // Merge operators should have been applied on non-empty
                        // db, and then merge added merge_num.
}

TEST_F(Int64AddMergeOperatorTest, MergeMultipleValues) {
  std::string value;

  ASSERT_OK(OpenDB());

  PutVarsignedint64(&value, 123);
  Status s = db_->Merge(write_opts_, "key", value);  // Merging 123 under key
  ASSERT_OK(s);
  value.clear();

  PutVarsignedint64(&value, -1234);
  s = db_->Merge(write_opts_, "key", value);  // Merging -1234 under key
  ASSERT_OK(s);
  value.clear();

  PutVarsignedint64(&value, 99);
  s = db_->Merge(write_opts_, "key", value);  // Merging 99 under key
  ASSERT_OK(s);
  value.clear();

  PutVarsignedint64(&value, -101);
  s = db_->Merge(write_opts_, "key", value);  // Merging -101 under key
  ASSERT_OK(s);
  value.clear();

  s = db_->Get(read_opts_, "key", &value);
  ASSERT_OK(s);
  Slice read_slice(value);
  int64_t read_value = 0;
  const bool read_ok = GetVarsignedint64(&read_slice, &read_value);

  ASSERT_TRUE(read_ok);
  const int64_t expected = 0 + 123 + (-1234) + 99 + (-101);
  ASSERT_EQ(read_value,
            expected);  // Merge operators should have been applied on non-empty
                        // db, and then merge added merge_num.
}

INSTANTIATE_TEST_CASE_P(Int64AddMergeOperatorTest, EmptyDbTest,
                        testing::Values(-255, -2, -1, 0, 1, 2, 255));
INSTANTIATE_TEST_CASE_P(
    Int64AddMergeOperatorTest, NonEmptyDbTest,
    testing::Combine(testing::Values(-255, -2, -1, 0, 1, 2, 255),
                     testing::Values(-255, -2, -1, 0, 1, 2, 255)));

}  // namespace ROCKSDB_NAMESPACE
