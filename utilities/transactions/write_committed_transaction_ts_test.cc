//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/testutil.h"
#include "utilities/merge_operators.h"
#include "utilities/transactions/transaction_test.h"

namespace ROCKSDB_NAMESPACE {

INSTANTIATE_TEST_CASE_P(
    DBAsBaseDB, WriteCommittedTxnWithTsTest,
    ::testing::Values(std::make_tuple(false, /*two_write_queue=*/false,
                                      /*enable_indexing=*/false),
                      std::make_tuple(false, /*two_write_queue=*/true,
                                      /*enable_indexing=*/false),
                      std::make_tuple(false, /*two_write_queue=*/false,
                                      /*enable_indexing=*/true),
                      std::make_tuple(false, /*two_write_queue=*/true,
                                      /*enable_indexing=*/true)));

INSTANTIATE_TEST_CASE_P(
    DBAsStackableDB, WriteCommittedTxnWithTsTest,
    ::testing::Values(std::make_tuple(true, /*two_write_queue=*/false,
                                      /*enable_indexing=*/false),
                      std::make_tuple(true, /*two_write_queue=*/true,
                                      /*enable_indexing=*/false),
                      std::make_tuple(true, /*two_write_queue=*/false,
                                      /*enable_indexing=*/true),
                      std::make_tuple(true, /*two_write_queue=*/true,
                                      /*enable_indexing=*/true)));

TEST_P(WriteCommittedTxnWithTsTest, SanityChecks) {
  ASSERT_OK(ReOpenNoDelete());

  ColumnFamilyOptions cf_opts;
  cf_opts.comparator = test::BytewiseComparatorWithU64TsWrapper();
  const std::string test_cf_name = "test_cf";
  ColumnFamilyHandle* cfh = nullptr;
  assert(db);
  ASSERT_OK(db->CreateColumnFamily(cf_opts, test_cf_name, &cfh));
  delete cfh;
  cfh = nullptr;

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  cf_descs.emplace_back(test_cf_name, cf_opts);
  ASSERT_OK(ReOpenNoDelete(cf_descs, &handles_));

  std::unique_ptr<Transaction> txn(
      NewTxn(WriteOptions(), TransactionOptions()));
  assert(txn);
  ASSERT_OK(txn->Put(handles_[1], "foo", "value"));
  ASSERT_TRUE(txn->Commit().IsInvalidArgument());

  auto* pessimistic_txn =
      static_cast_with_check<PessimisticTransaction>(txn.get());
  ASSERT_TRUE(
      pessimistic_txn->CommitBatch(/*batch=*/nullptr).IsInvalidArgument());

  {
    WriteBatchWithIndex* wbwi = txn->GetWriteBatch();
    assert(wbwi);
    WriteBatch* wb = wbwi->GetWriteBatch();
    assert(wb);
    // Write a key to the batch for nonexisting cf.
    ASSERT_OK(WriteBatchInternal::Put(wb, /*column_family_id=*/10, /*key=*/"",
                                      /*value=*/""));
  }

  ASSERT_OK(txn->SetCommitTimestamp(20));

  ASSERT_TRUE(txn->Commit().IsInvalidArgument());
  txn.reset();

  std::unique_ptr<Transaction> txn1(
      NewTxn(WriteOptions(), TransactionOptions()));
  assert(txn1);
  ASSERT_OK(txn1->SetName("txn1"));
  ASSERT_OK(txn1->Put(handles_[1], "foo", "value"));
  {
    WriteBatchWithIndex* wbwi = txn1->GetWriteBatch();
    assert(wbwi);
    WriteBatch* wb = wbwi->GetWriteBatch();
    assert(wb);
    // Write a key to the batch for non-existing cf.
    ASSERT_OK(WriteBatchInternal::Put(wb, /*column_family_id=*/10, /*key=*/"",
                                      /*value=*/""));
  }
  ASSERT_OK(txn1->Prepare());
  ASSERT_OK(txn1->SetCommitTimestamp(21));
  ASSERT_TRUE(txn1->Commit().IsInvalidArgument());
  txn1.reset();
}

void CheckKeyValueTsWithIterator(
    Iterator* iter,
    std::vector<std::tuple<std::string, std::string, std::string>> entries) {
  size_t num_entries = entries.size();
  // test forward iteration
  for (size_t i = 0; i < num_entries; i++) {
    auto [key, value, timestamp] = entries[i];
    if (i == 0) {
      iter->Seek(key);
    } else {
      iter->Next();
    }
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), key);
    ASSERT_EQ(iter->value(), value);
    ASSERT_EQ(iter->timestamp(), timestamp);
  }
  // test backward iteration
  for (size_t i = 0; i < num_entries; i++) {
    auto [key, value, timestamp] = entries[num_entries - 1 - i];
    if (i == 0) {
      iter->SeekForPrev(key);
    } else {
      iter->Prev();
    }
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), key);
    ASSERT_EQ(iter->value(), value);
    ASSERT_EQ(iter->timestamp(), timestamp);
  }
}

TEST_P(WriteCommittedTxnWithTsTest, ReOpenWithTimestamp) {
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  ASSERT_OK(ReOpenNoDelete());

  ColumnFamilyOptions cf_opts;
  cf_opts.comparator = test::BytewiseComparatorWithU64TsWrapper();
  const std::string test_cf_name = "test_cf";
  ColumnFamilyHandle* cfh = nullptr;
  assert(db);
  ASSERT_OK(db->CreateColumnFamily(cf_opts, test_cf_name, &cfh));
  delete cfh;
  cfh = nullptr;

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  cf_descs.emplace_back(test_cf_name, cf_opts);
  ASSERT_OK(ReOpenNoDelete(cf_descs, &handles_));

  std::unique_ptr<Transaction> txn0(
      NewTxn(WriteOptions(), TransactionOptions()));
  assert(txn0);
  ASSERT_OK(txn0->Put(handles_[1], "foo", "value"));
  ASSERT_OK(txn0->SetName("txn0"));
  ASSERT_OK(txn0->Prepare());
  ASSERT_TRUE(txn0->Commit().IsInvalidArgument());
  txn0.reset();

  std::unique_ptr<Transaction> txn1(
      NewTxn(WriteOptions(), TransactionOptions()));
  assert(txn1);

  std::string write_ts;
  uint64_t write_ts_int = 23;
  PutFixed64(&write_ts, write_ts_int);
  ReadOptions read_opts;
  std::string read_ts;
  PutFixed64(&read_ts, write_ts_int + 1);
  Slice read_ts_slice = read_ts;
  read_opts.timestamp = &read_ts_slice;

  ASSERT_OK(txn1->Put(handles_[1], "bar", "value0"));
  ASSERT_OK(txn1->Put(handles_[1], "foo", "value1"));
  // (key, value, ts) pairs to check.
  std::vector<std::tuple<std::string, std::string, std::string>>
      entries_to_check;
  entries_to_check.emplace_back("bar", "value0", "");
  entries_to_check.emplace_back("foo", "value1", "");

  {
    std::string buf;
    PutFixed64(&buf, 23);
    ASSERT_OK(txn1->Put("id", buf));
    ASSERT_OK(txn1->Merge("id", buf));
  }

  // Check (key, value, ts) with overwrites in txn before `SetCommitTimestamp`.
  if (std::get<2>(GetParam())) {  // enable_indexing = true
    std::unique_ptr<Iterator> iter(txn1->GetIterator(read_opts, handles_[1]));
    CheckKeyValueTsWithIterator(iter.get(), entries_to_check);
  }

  ASSERT_OK(txn1->SetName("txn1"));
  ASSERT_OK(txn1->Prepare());
  ASSERT_OK(txn1->SetCommitTimestamp(write_ts_int));

  // Check (key, value, ts) with overwrites in txn after `SetCommitTimestamp`.
  if (std::get<2>(GetParam())) {  // enable_indexing = true
    std::unique_ptr<Iterator> iter(txn1->GetIterator(read_opts, handles_[1]));
    CheckKeyValueTsWithIterator(iter.get(), entries_to_check);
  }

  ASSERT_OK(txn1->Commit());
  entries_to_check.clear();
  entries_to_check.emplace_back("bar", "value0", write_ts);
  entries_to_check.emplace_back("foo", "value1", write_ts);

  // Check (key, value, ts) pairs with overwrites in txn after `Commit`.
  {
    std::unique_ptr<Iterator> iter(txn1->GetIterator(read_opts, handles_[1]));
    CheckKeyValueTsWithIterator(iter.get(), entries_to_check);
  }
  txn1.reset();

  {
    std::string value;
    const Status s =
        GetFromDb(ReadOptions(), handles_[1], "foo", /*ts=*/23, &value);
    ASSERT_OK(s);
    ASSERT_EQ("value1", value);
  }

  {
    std::string value;
    const Status s = db->Get(ReadOptions(), handles_[0], "id", &value);
    ASSERT_OK(s);
    uint64_t ival = 0;
    Slice value_slc = value;
    bool result = GetFixed64(&value_slc, &ival);
    assert(result);
    ASSERT_EQ(46, ival);
  }

  // Check (key, value, ts) pairs without overwrites in txn.
  {
    std::unique_ptr<Transaction> txn2(
        NewTxn(WriteOptions(), TransactionOptions()));
    std::unique_ptr<Iterator> iter(txn2->GetIterator(read_opts, handles_[1]));
    CheckKeyValueTsWithIterator(iter.get(), entries_to_check);
  }
}

TEST_P(WriteCommittedTxnWithTsTest, RecoverFromWal) {
  ASSERT_OK(ReOpenNoDelete());

  ColumnFamilyOptions cf_opts;
  cf_opts.comparator = test::BytewiseComparatorWithU64TsWrapper();
  const std::string test_cf_name = "test_cf";
  ColumnFamilyHandle* cfh = nullptr;
  assert(db);
  ASSERT_OK(db->CreateColumnFamily(cf_opts, test_cf_name, &cfh));
  delete cfh;
  cfh = nullptr;

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  cf_descs.emplace_back(test_cf_name, cf_opts);
  options.avoid_flush_during_shutdown = true;
  ASSERT_OK(ReOpenNoDelete(cf_descs, &handles_));

  std::unique_ptr<Transaction> txn0(
      NewTxn(WriteOptions(), TransactionOptions()));
  assert(txn0);
  ASSERT_OK(txn0->Put(handles_[1], "foo", "foo_value"));
  ASSERT_OK(txn0->SetName("txn0"));
  ASSERT_OK(txn0->Prepare());

  WriteOptions write_opts;
  write_opts.sync = true;
  std::unique_ptr<Transaction> txn1(NewTxn(write_opts, TransactionOptions()));
  assert(txn1);
  ASSERT_OK(txn1->Put("bar", "bar_value_1"));
  ASSERT_OK(txn1->Put(handles_[1], "bar", "bar_value_1"));
  ASSERT_OK(txn1->SetName("txn1"));
  ASSERT_OK(txn1->Prepare());
  ASSERT_OK(txn1->SetCommitTimestamp(/*ts=*/23));
  ASSERT_OK(txn1->Commit());
  txn1.reset();

  std::unique_ptr<Transaction> txn2(NewTxn(write_opts, TransactionOptions()));
  assert(txn2);
  ASSERT_OK(txn2->Put("key1", "value_3"));
  ASSERT_OK(txn2->Put(handles_[1], "key1", "value_3"));
  ASSERT_OK(txn2->SetCommitTimestamp(/*ts=*/24));
  ASSERT_OK(txn2->Commit());
  txn2.reset();

  txn0.reset();

  ASSERT_OK(ReOpenNoDelete(cf_descs, &handles_));

  {
    std::string value;
    Status s = GetFromDb(ReadOptions(), handles_[1], "foo", /*ts=*/23, &value);
    ASSERT_TRUE(s.IsNotFound());

    s = db->Get(ReadOptions(), handles_[0], "bar", &value);
    ASSERT_OK(s);
    ASSERT_EQ("bar_value_1", value);

    value.clear();
    s = GetFromDb(ReadOptions(), handles_[1], "bar", /*ts=*/23, &value);
    ASSERT_OK(s);
    ASSERT_EQ("bar_value_1", value);

    s = GetFromDb(ReadOptions(), handles_[1], "key1", /*ts=*/23, &value);
    ASSERT_TRUE(s.IsNotFound());

    s = db->Get(ReadOptions(), handles_[0], "key1", &value);
    ASSERT_OK(s);
    ASSERT_EQ("value_3", value);

    s = GetFromDb(ReadOptions(), handles_[1], "key1", /*ts=*/24, &value);
    ASSERT_OK(s);
    ASSERT_EQ("value_3", value);
  }
}

TEST_P(WriteCommittedTxnWithTsTest, TransactionDbLevelApi) {
  ASSERT_OK(ReOpenNoDelete());

  ColumnFamilyOptions cf_options;
  cf_options.merge_operator = MergeOperators::CreateStringAppendOperator();
  cf_options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  const std::string test_cf_name = "test_cf";
  ColumnFamilyHandle* cfh = nullptr;
  assert(db);
  ASSERT_OK(db->CreateColumnFamily(cf_options, test_cf_name, &cfh));
  delete cfh;
  cfh = nullptr;

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  cf_descs.emplace_back(test_cf_name, cf_options);

  ASSERT_OK(ReOpenNoDelete(cf_descs, &handles_));

  std::string key_str = "tes_key";
  std::string ts_str;
  std::string value_str = "test_value";
  PutFixed64(&ts_str, 100);
  Slice value = value_str;

  assert(db);
  ASSERT_TRUE(
      db->Put(WriteOptions(), handles_[1], "foo", "bar").IsNotSupported());
  ASSERT_TRUE(db->Delete(WriteOptions(), handles_[1], "foo").IsNotSupported());
  ASSERT_TRUE(
      db->SingleDelete(WriteOptions(), handles_[1], "foo").IsNotSupported());
  ASSERT_TRUE(
      db->Merge(WriteOptions(), handles_[1], "foo", "+1").IsNotSupported());
  WriteBatch wb1(/*reserved_bytes=*/0, /*max_bytes=*/0,
                 /*protection_bytes_per_key=*/0, /*default_cf_ts_sz=*/0);
  ASSERT_OK(wb1.Put(handles_[1], key_str, ts_str, value));
  ASSERT_TRUE(db->Write(WriteOptions(), &wb1).IsNotSupported());
  ASSERT_TRUE(db->Write(WriteOptions(), TransactionDBWriteOptimizations(), &wb1)
                  .IsNotSupported());
  auto* pessimistic_txn_db =
      static_cast_with_check<PessimisticTransactionDB>(db);
  assert(pessimistic_txn_db);
  ASSERT_TRUE(
      pessimistic_txn_db->WriteWithConcurrencyControl(WriteOptions(), &wb1)
          .IsNotSupported());

  ASSERT_OK(db->Put(WriteOptions(), "foo", "value"));
  ASSERT_OK(db->Put(WriteOptions(), "bar", "value"));
  ASSERT_OK(db->Delete(WriteOptions(), "bar"));
  ASSERT_OK(db->SingleDelete(WriteOptions(), "foo"));
  ASSERT_OK(db->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db->Merge(WriteOptions(), "key", "_more"));
  WriteBatch wb2(/*reserved_bytes=*/0, /*max_bytes=*/0,
                 /*protection_bytes_per_key=*/0, /*default_cf_ts_sz=*/0);
  ASSERT_OK(wb2.Put(key_str, value));
  ASSERT_OK(db->Write(WriteOptions(), &wb2));
  ASSERT_OK(db->Write(WriteOptions(), TransactionDBWriteOptimizations(), &wb2));
  ASSERT_OK(
      pessimistic_txn_db->WriteWithConcurrencyControl(WriteOptions(), &wb2));

  std::unique_ptr<Transaction> txn(
      NewTxn(WriteOptions(), TransactionOptions()));
  assert(txn);

  WriteBatch wb3(/*reserved_bytes=*/0, /*max_bytes=*/0,
                 /*protection_bytes_per_key=*/0, /*default_cf_ts_sz=*/0);

  ASSERT_OK(wb3.Put(handles_[1], "key", "value"));
  auto* pessimistic_txn =
      static_cast_with_check<PessimisticTransaction>(txn.get());
  assert(pessimistic_txn);
  ASSERT_TRUE(pessimistic_txn->CommitBatch(&wb3).IsNotSupported());

  txn.reset();
}

TEST_P(WriteCommittedTxnWithTsTest, Merge) {
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  ASSERT_OK(ReOpenNoDelete());

  ColumnFamilyOptions cf_options;
  cf_options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  cf_options.merge_operator = MergeOperators::CreateStringAppendOperator();
  const std::string test_cf_name = "test_cf";
  ColumnFamilyHandle* cfh = nullptr;
  assert(db);
  ASSERT_OK(db->CreateColumnFamily(cf_options, test_cf_name, &cfh));
  delete cfh;
  cfh = nullptr;

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  cf_descs.emplace_back(test_cf_name, Options(DBOptions(), cf_options));
  options.avoid_flush_during_shutdown = true;

  ASSERT_OK(ReOpenNoDelete(cf_descs, &handles_));

  std::unique_ptr<Transaction> txn(
      NewTxn(WriteOptions(), TransactionOptions()));
  assert(txn);
  ASSERT_OK(txn->Put(handles_[1], "foo", "bar"));
  ASSERT_OK(txn->Merge(handles_[1], "foo", "1"));
  ASSERT_OK(txn->SetCommitTimestamp(24));
  ASSERT_OK(txn->Commit());
  txn.reset();
  {
    std::string value;
    const Status s =
        GetFromDb(ReadOptions(), handles_[1], "foo", /*ts=*/24, &value);
    ASSERT_OK(s);
    ASSERT_EQ("bar,1", value);
  }
}

TEST_P(WriteCommittedTxnWithTsTest, GetForUpdate) {
  ASSERT_OK(ReOpenNoDelete());

  ColumnFamilyOptions cf_options;
  cf_options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  const std::string test_cf_name = "test_cf";
  ColumnFamilyHandle* cfh = nullptr;
  assert(db);
  ASSERT_OK(db->CreateColumnFamily(cf_options, test_cf_name, &cfh));
  delete cfh;
  cfh = nullptr;

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  cf_descs.emplace_back(test_cf_name, Options(DBOptions(), cf_options));
  options.avoid_flush_during_shutdown = true;

  ASSERT_OK(ReOpenNoDelete(cf_descs, &handles_));

  std::unique_ptr<Transaction> txn0(
      NewTxn(WriteOptions(), TransactionOptions()));

  // Not set read timestamp, use blind write
  std::unique_ptr<Transaction> txn1(
      NewTxn(WriteOptions(), TransactionOptions()));
  ASSERT_OK(txn1->Put(handles_[1], "key", "value1"));
  ASSERT_OK(txn1->Put(handles_[1], "foo", "value1"));
  ASSERT_OK(txn1->SetCommitTimestamp(24));
  ASSERT_OK(txn1->Commit());
  txn1.reset();

  // Set read timestamp, use it for validation in GetForUpdate, validation fail
  // with conflict: timestamp from db(24) > read timestamp(23)
  std::string value;
  ASSERT_OK(txn0->SetReadTimestampForValidation(23));
  ASSERT_TRUE(
      txn0->GetForUpdate(ReadOptions(), handles_[1], "key", &value).IsBusy());
  ASSERT_OK(txn0->Rollback());
  txn0.reset();

  // Set read timestamp, use it for validation in GetForUpdate, validation pass
  // with no conflict: timestamp from db(24) < read timestamp (25)
  std::unique_ptr<Transaction> txn2(
      NewTxn(WriteOptions(), TransactionOptions()));
  ASSERT_OK(txn2->SetReadTimestampForValidation(25));
  ASSERT_OK(txn2->GetForUpdate(ReadOptions(), handles_[1], "key", &value));
  // Use a different read timestamp in ReadOptions while doing validation is
  // invalid.
  ReadOptions read_options;
  std::string read_timestamp;
  Slice diff_read_ts = EncodeU64Ts(24, &read_timestamp);
  read_options.timestamp = &diff_read_ts;
  ASSERT_TRUE(txn2->GetForUpdate(read_options, handles_[1], "foo", &value)
                  .IsInvalidArgument());
  ASSERT_OK(txn2->SetCommitTimestamp(26));
  ASSERT_OK(txn2->Commit());
  txn2.reset();

  // Set read timestamp, call GetForUpdate without validation, invalid
  std::unique_ptr<Transaction> txn3(
      NewTxn(WriteOptions(), TransactionOptions()));
  ASSERT_OK(txn3->SetReadTimestampForValidation(27));
  ASSERT_TRUE(txn3->GetForUpdate(ReadOptions(), handles_[1], "key", &value,
                                 /*exclusive=*/true, /*do_validate=*/false)
                  .IsInvalidArgument());
  ASSERT_OK(txn3->Rollback());
  txn3.reset();

  // Not set read timestamp, call GetForUpdate with validation, invalid
  std::unique_ptr<Transaction> txn4(
      NewTxn(WriteOptions(), TransactionOptions()));
  // ReadOptions.timestamp is not set, invalid
  ASSERT_TRUE(txn4->GetForUpdate(ReadOptions(), handles_[1], "key", &value)
                  .IsInvalidArgument());
  // ReadOptions.timestamp is set, also invalid.
  // `SetReadTimestampForValidation` must have been called with the same
  // timestamp as in ReadOptions.timestamp for validation.
  Slice read_ts = EncodeU64Ts(27, &read_timestamp);
  read_options.timestamp = &read_ts;
  ASSERT_TRUE(txn4->GetForUpdate(read_options, handles_[1], "key", &value)
                  .IsInvalidArgument());
  ASSERT_OK(txn4->Rollback());
  txn4.reset();

  // Not set read timestamp, call GetForUpdate without validation, pass
  std::unique_ptr<Transaction> txn5(
      NewTxn(WriteOptions(), TransactionOptions()));
  // ReadOptions.timestamp is not set, pass
  ASSERT_OK(txn5->GetForUpdate(ReadOptions(), handles_[1], "key", &value,
                               /*exclusive=*/true, /*do_validate=*/false));
  // ReadOptions.timestamp explicitly set to max timestamp, pass
  Slice max_ts = MaxU64Ts();
  read_options.timestamp = &max_ts;
  ASSERT_OK(txn5->GetForUpdate(read_options, handles_[1], "foo", &value,
                               /*exclusive=*/true, /*do_validate=*/false));
  // NOTE: this commit timestamp is smaller than the db's timestamp (26), but
  // this commit can still go through, that breaks the user-defined timestamp
  // invariant: newer user-defined timestamp should have newer sequence number.
  // So be aware of skipping UDT based validation. Unless users have their own
  // ways to ensure the UDT invariant is met, DO NOT skip it. Ways to ensure
  // the UDT invariant include: manage a monotonically increasing timestamp,
  // commit transactions in a single thread etc.
  ASSERT_OK(txn5->SetCommitTimestamp(3));
  ASSERT_OK(txn5->Commit());
  txn5.reset();
}

TEST_P(WriteCommittedTxnWithTsTest, BlindWrite) {
  ASSERT_OK(ReOpenNoDelete());

  ColumnFamilyOptions cf_options;
  cf_options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  const std::string test_cf_name = "test_cf";
  ColumnFamilyHandle* cfh = nullptr;
  assert(db);
  ASSERT_OK(db->CreateColumnFamily(cf_options, test_cf_name, &cfh));
  delete cfh;
  cfh = nullptr;

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  cf_descs.emplace_back(test_cf_name, Options(DBOptions(), cf_options));
  options.avoid_flush_during_shutdown = true;
  ASSERT_OK(ReOpenNoDelete(cf_descs, &handles_));

  std::unique_ptr<Transaction> txn0(
      NewTxn(WriteOptions(), TransactionOptions()));
  assert(txn0);
  std::unique_ptr<Transaction> txn1(
      NewTxn(WriteOptions(), TransactionOptions()));
  assert(txn1);

  {
    std::string value;
    ASSERT_OK(txn0->SetReadTimestampForValidation(100));
    // Lock "key".
    ASSERT_TRUE(txn0->GetForUpdate(ReadOptions(), handles_[1], "key", &value)
                    .IsNotFound());
  }

  ASSERT_OK(txn0->Put(handles_[1], "key", "value0"));
  ASSERT_OK(txn0->SetCommitTimestamp(101));
  ASSERT_OK(txn0->Commit());

  ASSERT_OK(txn1->Put(handles_[1], "key", "value1"));
  // In reality, caller needs to ensure commit_ts of txn1 is greater than the
  // commit_ts of txn0, which is true for lock-based concurrency control.
  ASSERT_OK(txn1->SetCommitTimestamp(102));
  ASSERT_OK(txn1->Commit());

  txn0.reset();
  txn1.reset();
}

TEST_P(WriteCommittedTxnWithTsTest, RefineReadTimestamp) {
  ASSERT_OK(ReOpenNoDelete());

  ColumnFamilyOptions cf_options;
  cf_options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  const std::string test_cf_name = "test_cf";
  ColumnFamilyHandle* cfh = nullptr;
  assert(db);
  ASSERT_OK(db->CreateColumnFamily(cf_options, test_cf_name, &cfh));
  delete cfh;
  cfh = nullptr;

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  cf_descs.emplace_back(test_cf_name, Options(DBOptions(), cf_options));
  options.avoid_flush_during_shutdown = true;

  ASSERT_OK(ReOpenNoDelete(cf_descs, &handles_));

  std::unique_ptr<Transaction> txn0(
      NewTxn(WriteOptions(), TransactionOptions()));
  assert(txn0);

  std::unique_ptr<Transaction> txn1(
      NewTxn(WriteOptions(), TransactionOptions()));
  assert(txn1);

  {
    ASSERT_OK(txn0->SetReadTimestampForValidation(100));
    // Lock "key0", "key1", ..., "key4".
    for (int i = 0; i < 5; ++i) {
      std::string value;
      ASSERT_TRUE(txn0->GetForUpdate(ReadOptions(), handles_[1],
                                     "key" + std::to_string(i), &value)
                      .IsNotFound());
    }
  }
  ASSERT_OK(txn1->Put(handles_[1], "key5", "value5_0"));
  ASSERT_OK(txn1->SetName("txn1"));
  ASSERT_OK(txn1->Prepare());
  ASSERT_OK(txn1->SetCommitTimestamp(101));
  ASSERT_OK(txn1->Commit());
  txn1.reset();

  {
    std::string value;
    ASSERT_TRUE(txn0->GetForUpdate(ReadOptions(), handles_[1], "key5", &value)
                    .IsBusy());
    ASSERT_OK(txn0->SetReadTimestampForValidation(102));
    ASSERT_OK(txn0->GetForUpdate(ReadOptions(), handles_[1], "key5", &value));
    ASSERT_EQ("value5_0", value);
  }

  for (int i = 0; i < 6; ++i) {
    ASSERT_OK(txn0->Put(handles_[1], "key" + std::to_string(i),
                        "value" + std::to_string(i)));
  }
  ASSERT_OK(txn0->SetName("txn0"));
  ASSERT_OK(txn0->Prepare());
  ASSERT_OK(txn0->SetCommitTimestamp(103));
  ASSERT_OK(txn0->Commit());
  txn0.reset();
}

TEST_P(WriteCommittedTxnWithTsTest, CheckKeysForConflicts) {
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  ASSERT_OK(ReOpen());

  std::unique_ptr<Transaction> txn1(
      db->BeginTransaction(WriteOptions(), TransactionOptions()));
  assert(txn1);

  std::unique_ptr<Transaction> txn2(
      db->BeginTransaction(WriteOptions(), TransactionOptions()));
  assert(txn2);
  ASSERT_OK(txn2->Put("foo", "v0"));
  ASSERT_OK(txn2->SetCommitTimestamp(10));
  ASSERT_OK(txn2->Commit());
  txn2.reset();

  // txn1 takes a snapshot after txn2 commits. The writes of txn2 have
  // a smaller seqno than txn1's snapshot, thus should not affect conflict
  // checking.
  txn1->SetSnapshot();

  std::unique_ptr<Transaction> txn3(
      db->BeginTransaction(WriteOptions(), TransactionOptions()));
  assert(txn3);
  ASSERT_OK(txn3->SetReadTimestampForValidation(20));
  std::string dontcare;
  ASSERT_OK(txn3->GetForUpdate(ReadOptions(), "foo", &dontcare));
  ASSERT_OK(txn3->SingleDelete("foo"));
  ASSERT_OK(txn3->SetName("txn3"));
  ASSERT_OK(txn3->Prepare());
  ASSERT_OK(txn3->SetCommitTimestamp(30));
  // txn3 reads at ts=20 > txn2's commit timestamp, and commits at ts=30.
  // txn3 can commit successfully, leaving a tombstone with ts=30.
  ASSERT_OK(txn3->Commit());
  txn3.reset();

  bool called = false;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::GetLatestSequenceForKey:mem", [&](void* arg) {
        auto* const ts_ptr = static_cast<std::string*>(arg);
        assert(ts_ptr);
        Slice ts_slc = *ts_ptr;
        uint64_t last_ts = 0;
        ASSERT_TRUE(GetFixed64(&ts_slc, &last_ts));
        ASSERT_EQ(30, last_ts);
        called = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // txn1's read timestamp is 25 < 30 (commit timestamp of txn3). Therefore,
  // the tombstone written by txn3 causes the conflict checking to fail.
  ASSERT_OK(txn1->SetReadTimestampForValidation(25));
  ASSERT_TRUE(txn1->GetForUpdate(ReadOptions(), "foo", &dontcare).IsBusy());
  ASSERT_TRUE(called);

  Transaction* reused_txn =
      db->BeginTransaction(WriteOptions(), TransactionOptions(), txn1.get());
  ASSERT_EQ(reused_txn, txn1.get());
  ASSERT_OK(reused_txn->Put("foo", "v1"));
  ASSERT_OK(reused_txn->SetCommitTimestamp(40));
  ASSERT_OK(reused_txn->Commit());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

