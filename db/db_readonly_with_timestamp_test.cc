//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_with_timestamp_test_util.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {
class DBReadOnlyTestWithTimestamp : public DBBasicTestWithTimestampBase {
 public:
  DBReadOnlyTestWithTimestamp()
      : DBBasicTestWithTimestampBase("db_readonly_test_with_timestamp") {}
};

#ifndef ROCKSDB_LITE
TEST_F(DBReadOnlyTestWithTimestamp, IteratorAndGetReadTimestampSizeMismatch) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::string write_timestamp = Timestamp(1, 0);
  WriteOptions write_opts;
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    Status s = db_->Put(write_opts, Key1(key), write_timestamp,
                        "value" + std::to_string(key));
    ASSERT_OK(s);
  }

  // Reopen the database in read only mode to test its timestamp support.
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  ReadOptions read_opts;
  std::string different_size_read_timestamp;
  PutFixed32(&different_size_read_timestamp, 2);
  Slice different_size_read_ts = different_size_read_timestamp;
  read_opts.timestamp = &different_size_read_ts;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsInvalidArgument());
  }

  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    std::string value_from_get;
    std::string timestamp;
    ASSERT_TRUE(db_->Get(read_opts, Key1(key), &value_from_get, &timestamp)
                    .IsInvalidArgument());
  }

  Close();
}

TEST_F(DBReadOnlyTestWithTimestamp,
       IteratorAndGetReadTimestampSpecifiedWithoutWriteTimestamp) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  WriteOptions write_opts;
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    Status s = db_->Put(write_opts, Key1(key), "value" + std::to_string(key));
    ASSERT_OK(s);
  }

  // Reopen the database in read only mode to test its timestamp support.
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  ReadOptions read_opts;
  const std::string read_timestamp = Timestamp(2, 0);
  Slice read_ts = read_timestamp;
  read_opts.timestamp = &read_ts;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsInvalidArgument());
  }

  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    std::string value_from_get;
    std::string timestamp;
    ASSERT_TRUE(db_->Get(read_opts, Key1(key), &value_from_get, &timestamp)
                    .IsInvalidArgument());
  }

  Close();
}

TEST_F(DBReadOnlyTestWithTimestamp, IteratorAndGet) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::vector<uint64_t> start_keys = {1, 0};
  const std::vector<std::string> write_timestamps = {Timestamp(1, 0),
                                                     Timestamp(3, 0)};
  const std::vector<std::string> read_timestamps = {Timestamp(2, 0),
                                                    Timestamp(4, 0)};
  for (size_t i = 0; i < write_timestamps.size(); ++i) {
    WriteOptions write_opts;
    for (uint64_t key = start_keys[i]; key <= kMaxKey; ++key) {
      Status s = db_->Put(write_opts, Key1(key), write_timestamps[i],
                          "value" + std::to_string(i));
      ASSERT_OK(s);
    }
  }

  // Reopen the database in read only mode to test its timestamp support.
  Close();
  ASSERT_OK(ReadOnlyReopen(options));

  auto get_value_and_check = [](DB* db, ReadOptions read_opts, Slice key,
                                Slice expected_value, std::string expected_ts) {
    std::string value_from_get;
    std::string timestamp;
    ASSERT_OK(db->Get(read_opts, key.ToString(), &value_from_get, &timestamp));
    ASSERT_EQ(expected_value, value_from_get);
    ASSERT_EQ(expected_ts, timestamp);
  };
  for (size_t i = 0; i < read_timestamps.size(); ++i) {
    ReadOptions read_opts;
    Slice read_ts = read_timestamps[i];
    read_opts.timestamp = &read_ts;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    int count = 0;
    uint64_t key = 0;
    // Forward iterate.
    for (it->Seek(Key1(0)), key = start_keys[i]; it->Valid();
         it->Next(), ++count, ++key) {
      CheckIterUserEntry(it.get(), Key1(key), kTypeValue,
                         "value" + std::to_string(i), write_timestamps[i]);
      get_value_and_check(db_, read_opts, it->key(), it->value(),
                          write_timestamps[i]);
    }
    size_t expected_count = kMaxKey - start_keys[i] + 1;
    ASSERT_EQ(expected_count, count);

    // Backward iterate.
    count = 0;
    for (it->SeekForPrev(Key1(kMaxKey)), key = kMaxKey; it->Valid();
         it->Prev(), ++count, --key) {
      CheckIterUserEntry(it.get(), Key1(key), kTypeValue,
                         "value" + std::to_string(i), write_timestamps[i]);
      get_value_and_check(db_, read_opts, it->key(), it->value(),
                          write_timestamps[i]);
    }
    ASSERT_EQ(static_cast<size_t>(kMaxKey) - start_keys[i] + 1, count);

    // SeekToFirst()/SeekToLast() with lower/upper bounds.
    // Then iter with lower and upper bounds.
    uint64_t l = 0;
    uint64_t r = kMaxKey + 1;
    while (l < r) {
      std::string lb_str = Key1(l);
      Slice lb = lb_str;
      std::string ub_str = Key1(r);
      Slice ub = ub_str;
      read_opts.iterate_lower_bound = &lb;
      read_opts.iterate_upper_bound = &ub;
      it.reset(db_->NewIterator(read_opts));
      for (it->SeekToFirst(), key = std::max(l, start_keys[i]), count = 0;
           it->Valid(); it->Next(), ++key, ++count) {
        CheckIterUserEntry(it.get(), Key1(key), kTypeValue,
                           "value" + std::to_string(i), write_timestamps[i]);
        get_value_and_check(db_, read_opts, it->key(), it->value(),
                            write_timestamps[i]);
      }
      ASSERT_EQ(r - std::max(l, start_keys[i]), count);

      for (it->SeekToLast(), key = std::min(r, kMaxKey + 1), count = 0;
           it->Valid(); it->Prev(), --key, ++count) {
        CheckIterUserEntry(it.get(), Key1(key - 1), kTypeValue,
                           "value" + std::to_string(i), write_timestamps[i]);
        get_value_and_check(db_, read_opts, it->key(), it->value(),
                            write_timestamps[i]);
      }
      l += (kMaxKey / 100);
      r -= (kMaxKey / 100);
    }
  }
  Close();
}

TEST_F(DBReadOnlyTestWithTimestamp, Iterators) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::string write_timestamp = Timestamp(1, 0);
  const std::string read_timestamp = Timestamp(2, 0);
  WriteOptions write_opts;
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    Status s = db_->Put(write_opts, Key1(key), write_timestamp,
                        "value" + std::to_string(key));
    ASSERT_OK(s);
  }

  // Reopen the database in read only mode to test its timestamp support.
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  ReadOptions read_opts;
  Slice read_ts = read_timestamp;
  read_opts.timestamp = &read_ts;
  std::vector<Iterator*> iters;
  ASSERT_OK(db_->NewIterators(read_opts, {db_->DefaultColumnFamily()}, &iters));
  ASSERT_EQ(static_cast<uint64_t>(1), iters.size());

  int count = 0;
  uint64_t key = 0;
  // Forward iterate.
  for (iters[0]->Seek(Key1(0)), key = 0; iters[0]->Valid();
       iters[0]->Next(), ++count, ++key) {
    CheckIterUserEntry(iters[0], Key1(key), kTypeValue,
                       "value" + std::to_string(key), write_timestamp);
  }

  size_t expected_count = kMaxKey - 0 + 1;
  ASSERT_EQ(expected_count, count);
  delete iters[0];

  Close();
}

TEST_F(DBReadOnlyTestWithTimestamp, IteratorsReadTimestampSizeMismatch) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::string write_timestamp = Timestamp(1, 0);
  WriteOptions write_opts;
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    Status s = db_->Put(write_opts, Key1(key), write_timestamp,
                        "value" + std::to_string(key));
    ASSERT_OK(s);
  }

  // Reopen the database in read only mode to test its timestamp support.
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  ReadOptions read_opts;
  std::string different_size_read_timestamp;
  PutFixed32(&different_size_read_timestamp, 2);
  Slice different_size_read_ts = different_size_read_timestamp;
  read_opts.timestamp = &different_size_read_ts;
  {
    std::vector<Iterator*> iters;
    ASSERT_TRUE(
        db_->NewIterators(read_opts, {db_->DefaultColumnFamily()}, &iters)
            .IsInvalidArgument());
  }

  Close();
}

TEST_F(DBReadOnlyTestWithTimestamp,
       IteratorsReadTimestampSpecifiedWithoutWriteTimestamp) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  WriteOptions write_opts;
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    Status s = db_->Put(write_opts, Key1(key), "value" + std::to_string(key));
    ASSERT_OK(s);
  }

  // Reopen the database in read only mode to test its timestamp support.
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  ReadOptions read_opts;
  const std::string read_timestamp = Timestamp(2, 0);
  Slice read_ts = read_timestamp;
  read_opts.timestamp = &read_ts;
  {
    std::vector<Iterator*> iters;
    ASSERT_TRUE(
        db_->NewIterators(read_opts, {db_->DefaultColumnFamily()}, &iters)
            .IsInvalidArgument());
  }

  Close();
}
#endif  // !ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
