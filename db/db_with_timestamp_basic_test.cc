//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/debug.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#if !defined(ROCKSDB_LITE)
#include "test_util/sync_point.h"
#endif
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {
class DBBasicTestWithTimestampBase : public DBTestBase {
 public:
  explicit DBBasicTestWithTimestampBase(const std::string& dbname)
      : DBTestBase(dbname, /*env_do_fsync=*/true) {}

 protected:
  static std::string Key1(uint64_t k) {
    std::string ret;
    PutFixed64(&ret, k);
    std::reverse(ret.begin(), ret.end());
    return ret;
  }

  static std::string KeyWithPrefix(std::string prefix, uint64_t k) {
    std::string ret;
    PutFixed64(&ret, k);
    std::reverse(ret.begin(), ret.end());
    return prefix + ret;
  }

  static std::vector<Slice> ConvertStrToSlice(
      std::vector<std::string>& strings) {
    std::vector<Slice> ret;
    for (const auto& s : strings) {
      ret.emplace_back(s);
    }
    return ret;
  }

  class TestComparator : public Comparator {
   private:
    const Comparator* cmp_without_ts_;

   public:
    explicit TestComparator(size_t ts_sz)
        : Comparator(ts_sz), cmp_without_ts_(nullptr) {
      cmp_without_ts_ = BytewiseComparator();
    }

    const char* Name() const override { return "TestComparator"; }

    void FindShortSuccessor(std::string*) const override {}

    void FindShortestSeparator(std::string*, const Slice&) const override {}

    int Compare(const Slice& a, const Slice& b) const override {
      int r = CompareWithoutTimestamp(a, b);
      if (r != 0 || 0 == timestamp_size()) {
        return r;
      }
      return -CompareTimestamp(
          Slice(a.data() + a.size() - timestamp_size(), timestamp_size()),
          Slice(b.data() + b.size() - timestamp_size(), timestamp_size()));
    }

    using Comparator::CompareWithoutTimestamp;
    int CompareWithoutTimestamp(const Slice& a, bool a_has_ts, const Slice& b,
                                bool b_has_ts) const override {
      if (a_has_ts) {
        assert(a.size() >= timestamp_size());
      }
      if (b_has_ts) {
        assert(b.size() >= timestamp_size());
      }
      Slice lhs = a_has_ts ? StripTimestampFromUserKey(a, timestamp_size()) : a;
      Slice rhs = b_has_ts ? StripTimestampFromUserKey(b, timestamp_size()) : b;
      return cmp_without_ts_->Compare(lhs, rhs);
    }

    int CompareTimestamp(const Slice& ts1, const Slice& ts2) const override {
      if (!ts1.data() && !ts2.data()) {
        return 0;
      } else if (ts1.data() && !ts2.data()) {
        return 1;
      } else if (!ts1.data() && ts2.data()) {
        return -1;
      }
      assert(ts1.size() == ts2.size());
      uint64_t low1 = 0;
      uint64_t low2 = 0;
      uint64_t high1 = 0;
      uint64_t high2 = 0;
      const size_t kSize = ts1.size();
      std::unique_ptr<char[]> ts1_buf(new char[kSize]);
      memcpy(ts1_buf.get(), ts1.data(), ts1.size());
      std::unique_ptr<char[]> ts2_buf(new char[kSize]);
      memcpy(ts2_buf.get(), ts2.data(), ts2.size());
      Slice ts1_copy = Slice(ts1_buf.get(), kSize);
      Slice ts2_copy = Slice(ts2_buf.get(), kSize);
      auto* ptr1 = const_cast<Slice*>(&ts1_copy);
      auto* ptr2 = const_cast<Slice*>(&ts2_copy);
      if (!GetFixed64(ptr1, &low1) || !GetFixed64(ptr1, &high1) ||
          !GetFixed64(ptr2, &low2) || !GetFixed64(ptr2, &high2)) {
        assert(false);
      }
      if (high1 < high2) {
        return -1;
      } else if (high1 > high2) {
        return 1;
      }
      if (low1 < low2) {
        return -1;
      } else if (low1 > low2) {
        return 1;
      }
      return 0;
    }
  };

  std::string Timestamp(uint64_t low, uint64_t high) {
    std::string ts;
    PutFixed64(&ts, low);
    PutFixed64(&ts, high);
    return ts;
  }

  void CheckIterUserEntry(const Iterator* it, const Slice& expected_key,
                          ValueType expected_value_type,
                          const Slice& expected_value,
                          const Slice& expected_ts) const {
    ASSERT_TRUE(it->Valid());
    ASSERT_OK(it->status());
    ASSERT_EQ(expected_key, it->key());
    if (kTypeValue == expected_value_type) {
      ASSERT_EQ(expected_value, it->value());
    }
    ASSERT_EQ(expected_ts, it->timestamp());
  }

  void CheckIterEntry(const Iterator* it, const Slice& expected_ukey,
                      SequenceNumber expected_seq, ValueType expected_val_type,
                      const Slice& expected_value, const Slice& expected_ts) {
    ASSERT_TRUE(it->Valid());
    ASSERT_OK(it->status());
    std::string ukey_and_ts;
    ukey_and_ts.assign(expected_ukey.data(), expected_ukey.size());
    ukey_and_ts.append(expected_ts.data(), expected_ts.size());
    ParsedInternalKey parsed_ikey;
    ASSERT_OK(
        ParseInternalKey(it->key(), &parsed_ikey, true /* log_err_key */));
    ASSERT_EQ(ukey_and_ts, parsed_ikey.user_key);
    ASSERT_EQ(expected_val_type, parsed_ikey.type);
    ASSERT_EQ(expected_seq, parsed_ikey.sequence);
    if (expected_val_type == kTypeValue) {
      ASSERT_EQ(expected_value, it->value());
    }
    ASSERT_EQ(expected_ts, it->timestamp());
  }

  void CheckIterEntry(const Iterator* it, const Slice& expected_ukey,
                      ValueType expected_val_type, const Slice& expected_value,
                      const Slice& expected_ts) {
    ASSERT_TRUE(it->Valid());
    ASSERT_OK(it->status());
    std::string ukey_and_ts;
    ukey_and_ts.assign(expected_ukey.data(), expected_ukey.size());
    ukey_and_ts.append(expected_ts.data(), expected_ts.size());

    ParsedInternalKey parsed_ikey;
    ASSERT_OK(
        ParseInternalKey(it->key(), &parsed_ikey, true /* log_err_key */));
    ASSERT_EQ(expected_val_type, parsed_ikey.type);
    ASSERT_EQ(Slice(ukey_and_ts), parsed_ikey.user_key);
    if (expected_val_type == kTypeValue) {
      ASSERT_EQ(expected_value, it->value());
    }
    ASSERT_EQ(expected_ts, it->timestamp());
  }
};

class DBBasicTestWithTimestamp : public DBBasicTestWithTimestampBase {
 public:
  DBBasicTestWithTimestamp()
      : DBBasicTestWithTimestampBase("db_basic_test_with_timestamp") {}
};

TEST_F(DBBasicTestWithTimestamp, CompactRangeWithSpecifiedRange) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;

  ASSERT_OK(db_->Put(write_opts, "foo1", "bar"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->Put(write_opts, "foo2", "bar"));
  ASSERT_OK(Flush());

  std::string start_str = "foo";
  std::string end_str = "foo2";
  Slice start(start_str), end(end_str);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));

  Close();
}

TEST_F(DBBasicTestWithTimestamp, UpdateFullHistoryTsLow) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  const std::string kKey = "test kKey";

  // Test set ts_low first and flush()
  int current_ts_low = 5;
  std::string ts_low_str = Timestamp(current_ts_low, 0);
  Slice ts_low = ts_low_str;
  CompactRangeOptions comp_opts;
  comp_opts.full_history_ts_low = &ts_low;
  comp_opts.bottommost_level_compaction = BottommostLevelCompaction::kForce;

  ASSERT_OK(db_->CompactRange(comp_opts, nullptr, nullptr));

  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd();
  auto result_ts_low = cfd->GetFullHistoryTsLow();

  ASSERT_TRUE(test_cmp.CompareTimestamp(ts_low, result_ts_low) == 0);

  for (int i = 0; i < 10; i++) {
    WriteOptions write_opts;
    std::string ts_str = Timestamp(i, 0);
    Slice ts = ts_str;
    write_opts.timestamp = &ts;
    ASSERT_OK(db_->Put(write_opts, kKey, Key(i)));
  }
  ASSERT_OK(Flush());

  for (int i = 0; i < 10; i++) {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(i, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::string value;
    Status status = db_->Get(read_opts, kKey, &value);
    if (i < current_ts_low) {
      ASSERT_TRUE(status.IsNotFound());
    } else {
      ASSERT_OK(status);
      ASSERT_TRUE(value.compare(Key(i)) == 0);
    }
  }

  // Test set ts_low and then trigger compaction
  for (int i = 10; i < 20; i++) {
    WriteOptions write_opts;
    std::string ts_str = Timestamp(i, 0);
    Slice ts = ts_str;
    write_opts.timestamp = &ts;
    ASSERT_OK(db_->Put(write_opts, kKey, Key(i)));
  }

  ASSERT_OK(Flush());

  current_ts_low = 15;
  ts_low_str = Timestamp(current_ts_low, 0);
  ts_low = ts_low_str;
  comp_opts.full_history_ts_low = &ts_low;
  ASSERT_OK(db_->CompactRange(comp_opts, nullptr, nullptr));
  result_ts_low = cfd->GetFullHistoryTsLow();
  ASSERT_TRUE(test_cmp.CompareTimestamp(ts_low, result_ts_low) == 0);

  for (int i = 0; i < 20; i++) {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(i, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::string value;
    Status status = db_->Get(read_opts, kKey, &value);
    if (i < current_ts_low) {
      ASSERT_TRUE(status.IsNotFound());
    } else {
      ASSERT_OK(status);
      ASSERT_TRUE(value.compare(Key(i)) == 0);
    }
  }

  // Test invalid compaction with range
  Slice start(kKey), end(kKey);
  Status s = db_->CompactRange(comp_opts, &start, &end);
  ASSERT_TRUE(s.IsInvalidArgument());
  s = db_->CompactRange(comp_opts, &start, nullptr);
  ASSERT_TRUE(s.IsInvalidArgument());
  s = db_->CompactRange(comp_opts, nullptr, &end);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Test invalid compaction with the decreasing ts_low
  ts_low_str = Timestamp(current_ts_low - 1, 0);
  ts_low = ts_low_str;
  comp_opts.full_history_ts_low = &ts_low;
  s = db_->CompactRange(comp_opts, nullptr, nullptr);
  ASSERT_TRUE(s.IsInvalidArgument());

  Close();
}

TEST_F(DBBasicTestWithTimestamp, GetApproximateSizes) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;  // Large write buffer
  options.compression = kNoCompression;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  auto default_cf = db_->DefaultColumnFamily();

  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;

  const int N = 128;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_OK(db_->Put(write_opts, Key(i), rnd.RandomString(1024)));
  }

  uint64_t size;
  std::string start = Key(50);
  std::string end = Key(60);
  Range r(start, end);
  SizeApproximationOptions size_approx_options;
  size_approx_options.include_memtabtles = true;
  size_approx_options.include_files = true;
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_GT(size, 6000);
  ASSERT_LT(size, 204800);

  // test multiple ranges
  std::vector<Range> ranges;
  std::string start_tmp = Key(10);
  std::string end_tmp = Key(20);
  ranges.emplace_back(Range(start_tmp, end_tmp));
  ranges.emplace_back(Range(start, end));
  uint64_t range_sizes[2];
  ASSERT_OK(db_->GetApproximateSizes(size_approx_options, default_cf,
                                     ranges.data(), 2, range_sizes));

  ASSERT_EQ(range_sizes[1], size);

  // Zero if not including mem table
  ASSERT_OK(db_->GetApproximateSizes(&r, 1, &size));
  ASSERT_EQ(size, 0);

  start = Key(500);
  end = Key(600);
  r = Range(start, end);
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_EQ(size, 0);

  // Test range boundaries
  ASSERT_OK(db_->Put(write_opts, Key(1000), rnd.RandomString(1024)));
  // Should include start key
  start = Key(1000);
  end = Key(1100);
  r = Range(start, end);
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_GT(size, 0);

  // Should exclude end key
  start = Key(900);
  end = Key(1000);
  r = Range(start, end);
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_EQ(size, 0);

  Close();
}

TEST_F(DBBasicTestWithTimestamp, SimpleIterate) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::vector<uint64_t> start_keys = {1, 0};
  const std::vector<std::string> write_timestamps = {Timestamp(1, 0),
                                                     Timestamp(3, 0)};
  const std::vector<std::string> read_timestamps = {Timestamp(2, 0),
                                                    Timestamp(4, 0)};
  for (size_t i = 0; i < write_timestamps.size(); ++i) {
    WriteOptions write_opts;
    Slice write_ts = write_timestamps[i];
    write_opts.timestamp = &write_ts;
    for (uint64_t key = start_keys[i]; key <= kMaxKey; ++key) {
      Status s = db_->Put(write_opts, Key1(key), "value" + std::to_string(i));
      ASSERT_OK(s);
    }
  }
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
    }
    size_t expected_count = kMaxKey - start_keys[i] + 1;
    ASSERT_EQ(expected_count, count);

    // Backward iterate.
    count = 0;
    for (it->SeekForPrev(Key1(kMaxKey)), key = kMaxKey; it->Valid();
         it->Prev(), ++count, --key) {
      CheckIterUserEntry(it.get(), Key1(key), kTypeValue,
                         "value" + std::to_string(i), write_timestamps[i]);
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
      }
      ASSERT_EQ(r - std::max(l, start_keys[i]), count);

      for (it->SeekToLast(), key = std::min(r, kMaxKey + 1), count = 0;
           it->Valid(); it->Prev(), --key, ++count) {
        CheckIterUserEntry(it.get(), Key1(key - 1), kTypeValue,
                           "value" + std::to_string(i), write_timestamps[i]);
      }
      l += (kMaxKey / 100);
      r -= (kMaxKey / 100);
    }
  }
  Close();
}

class DBBasicTestWithTimestampTableOptions
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<BlockBasedTableOptions::IndexType> {
 public:
  explicit DBBasicTestWithTimestampTableOptions()
      : DBBasicTestWithTimestampBase(
            "db_basic_test_with_timestamp_table_options") {}
};

INSTANTIATE_TEST_CASE_P(
    Timestamp, DBBasicTestWithTimestampTableOptions,
    testing::Values(
        BlockBasedTableOptions::IndexType::kBinarySearch,
        BlockBasedTableOptions::IndexType::kHashSearch,
        BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch,
        BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey));

TEST_P(DBBasicTestWithTimestampTableOptions, GetAndMultiGet) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.compression = kNoCompression;
  BlockBasedTableOptions bbto;
  bbto.index_type = GetParam();
  bbto.block_size = 100;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator cmp(kTimestampSize);
  options.comparator = &cmp;
  DestroyAndReopen(options);
  constexpr uint64_t kNumKeys = 1024;
  for (uint64_t k = 0; k < kNumKeys; ++k) {
    WriteOptions write_opts;
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    write_opts.timestamp = &ts;
    ASSERT_OK(db_->Put(write_opts, Key1(k), "value" + std::to_string(k)));
  }
  ASSERT_OK(Flush());
  {
    ReadOptions read_opts;
    read_opts.total_order_seek = true;
    std::string ts_str = Timestamp(2, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    // verify Get()
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      std::string value_from_get;
      std::string key_str(it->key().data(), it->key().size());
      std::string timestamp;
      ASSERT_OK(db_->Get(read_opts, key_str, &value_from_get, &timestamp));
      ASSERT_EQ(it->value(), value_from_get);
      ASSERT_EQ(Timestamp(1, 0), timestamp);
    }

    // verify MultiGet()
    constexpr uint64_t step = 2;
    static_assert(0 == (kNumKeys % step),
                  "kNumKeys must be a multiple of step");
    for (uint64_t k = 0; k < kNumKeys; k += 2) {
      std::vector<std::string> key_strs;
      std::vector<Slice> keys;
      for (size_t i = 0; i < step; ++i) {
        key_strs.push_back(Key1(k + i));
      }
      for (size_t i = 0; i < step; ++i) {
        keys.emplace_back(key_strs[i]);
      }
      std::vector<std::string> values;
      std::vector<std::string> timestamps;
      std::vector<Status> statuses =
          db_->MultiGet(read_opts, keys, &values, &timestamps);
      ASSERT_EQ(step, statuses.size());
      ASSERT_EQ(step, values.size());
      ASSERT_EQ(step, timestamps.size());
      for (uint64_t i = 0; i < step; ++i) {
        ASSERT_OK(statuses[i]);
        ASSERT_EQ("value" + std::to_string(k + i), values[i]);
        ASSERT_EQ(Timestamp(1, 0), timestamps[i]);
      }
    }
  }
  Close();
}

TEST_P(DBBasicTestWithTimestampTableOptions, SeekWithPrefixLessThanKey) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.memtable_whole_key_filtering = true;
  options.memtable_prefix_bloom_size_ratio = 0.1;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = true;
  bbto.index_type = GetParam();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;

  ASSERT_OK(db_->Put(write_opts, "foo1", "bar"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->Put(write_opts, "foo2", "bar"));
  ASSERT_OK(Flush());

  // Move sst file to next level
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_OK(db_->Put(write_opts, "foo3", "bar"));
  ASSERT_OK(Flush());

  ReadOptions read_opts;
  std::string read_ts = Timestamp(1, 0);
  ts = read_ts;
  read_opts.timestamp = &ts;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->Seek("foo");
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());

    iter->Seek("bbb");
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  Close();
}

TEST_P(DBBasicTestWithTimestampTableOptions, SeekWithPrefixLongerThanKey) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(20));
  options.memtable_whole_key_filtering = true;
  options.memtable_prefix_bloom_size_ratio = 0.1;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = true;
  bbto.index_type = GetParam();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;

  ASSERT_OK(db_->Put(write_opts, "foo1", "bar"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->Put(write_opts, "foo2", "bar"));
  ASSERT_OK(Flush());

  // Move sst file to next level
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_OK(db_->Put(write_opts, "foo3", "bar"));
  ASSERT_OK(Flush());

  ReadOptions read_opts;
  std::string read_ts = Timestamp(2, 0);
  ts = read_ts;
  read_opts.timestamp = &ts;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    // Make sure the prefix extractor doesn't include timestamp, otherwise it
    // may return invalid result.
    iter->Seek("foo");
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  Close();
}

TEST_P(DBBasicTestWithTimestampTableOptions, SeekWithBound) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = true;
  bbto.index_type = GetParam();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;

  ASSERT_OK(db_->Put(write_opts, "foo1", "bar1"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->Put(write_opts, "foo2", "bar2"));
  ASSERT_OK(Flush());

  // Move sst file to next level
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (int i = 3; i < 9; ++i) {
    ASSERT_OK(db_->Put(write_opts, "foo" + std::to_string(i),
                       "bar" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  ReadOptions read_opts;
  std::string read_ts = Timestamp(2, 0);
  ts = read_ts;
  read_opts.timestamp = &ts;
  std::string up_bound = "foo5";  // exclusive
  Slice up_bound_slice = up_bound;
  std::string lo_bound = "foo2";  // inclusive
  Slice lo_bound_slice = lo_bound;
  read_opts.iterate_upper_bound = &up_bound_slice;
  read_opts.iterate_lower_bound = &lo_bound_slice;
  read_opts.auto_prefix_mode = true;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    // Make sure the prefix extractor doesn't include timestamp, otherwise it
    // may return invalid result.
    iter->Seek("foo");
    CheckIterUserEntry(iter.get(), lo_bound, kTypeValue, "bar2",
                       Timestamp(1, 0));
    iter->SeekToFirst();
    CheckIterUserEntry(iter.get(), lo_bound, kTypeValue, "bar2",
                       Timestamp(1, 0));
    iter->SeekForPrev("g");
    CheckIterUserEntry(iter.get(), "foo4", kTypeValue, "bar4", Timestamp(1, 0));
    iter->SeekToLast();
    CheckIterUserEntry(iter.get(), "foo4", kTypeValue, "bar4", Timestamp(1, 0));
  }

  Close();
}

TEST_F(DBBasicTestWithTimestamp, ChangeIterationDirection) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = env_;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);
  const std::vector<std::string> timestamps = {Timestamp(1, 1), Timestamp(0, 2),
                                               Timestamp(4, 3)};
  const std::vector<std::tuple<std::string, std::string>> kvs = {
      std::make_tuple("aa", "value1"), std::make_tuple("ab", "value2")};
  for (const auto& ts : timestamps) {
    WriteBatch wb(0, 0, kTimestampSize);
    for (const auto& kv : kvs) {
      const std::string& key = std::get<0>(kv);
      const std::string& value = std::get<1>(kv);
      ASSERT_OK(wb.Put(key, value));
    }

    ASSERT_OK(wb.AssignTimestamp(ts));
    ASSERT_OK(db_->Write(WriteOptions(), &wb));
  }
  std::string read_ts_str = Timestamp(5, 3);
  Slice read_ts = read_ts_str;
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts;
  std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));

  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  it->Prev();
  ASSERT_FALSE(it->Valid());

  it->SeekToLast();
  ASSERT_TRUE(it->Valid());
  uint64_t prev_reseek_count =
      options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION);
  ASSERT_EQ(0, prev_reseek_count);
  it->Next();
  ASSERT_FALSE(it->Valid());
  ASSERT_EQ(1 + prev_reseek_count,
            options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));

  it->Seek(std::get<0>(kvs[0]));
  CheckIterUserEntry(it.get(), std::get<0>(kvs[0]), kTypeValue,
                     std::get<1>(kvs[0]), Timestamp(4, 3));
  it->Next();
  CheckIterUserEntry(it.get(), std::get<0>(kvs[1]), kTypeValue,
                     std::get<1>(kvs[1]), Timestamp(4, 3));
  it->Prev();
  CheckIterUserEntry(it.get(), std::get<0>(kvs[0]), kTypeValue,
                     std::get<1>(kvs[0]), Timestamp(4, 3));

  prev_reseek_count =
      options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION);
  ASSERT_EQ(1, prev_reseek_count);
  it->Next();
  CheckIterUserEntry(it.get(), std::get<0>(kvs[1]), kTypeValue,
                     std::get<1>(kvs[1]), Timestamp(4, 3));
  ASSERT_EQ(1 + prev_reseek_count,
            options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));

  it->SeekForPrev(std::get<0>(kvs[1]));
  CheckIterUserEntry(it.get(), std::get<0>(kvs[1]), kTypeValue,
                     std::get<1>(kvs[1]), Timestamp(4, 3));
  it->Prev();
  CheckIterUserEntry(it.get(), std::get<0>(kvs[0]), kTypeValue,
                     std::get<1>(kvs[0]), Timestamp(4, 3));

  prev_reseek_count =
      options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION);
  it->Next();
  CheckIterUserEntry(it.get(), std::get<0>(kvs[1]), kTypeValue,
                     std::get<1>(kvs[1]), Timestamp(4, 3));
  ASSERT_EQ(1 + prev_reseek_count,
            options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));

  it.reset();
  Close();
}

TEST_F(DBBasicTestWithTimestamp, SimpleForwardIterateLowerTsBound) {
  constexpr int kNumKeysPerFile = 128;
  constexpr uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::vector<std::string> write_timestamps = {Timestamp(1, 0),
                                                     Timestamp(3, 0)};
  const std::vector<std::string> read_timestamps = {Timestamp(2, 0),
                                                    Timestamp(4, 0)};
  const std::vector<std::string> read_timestamps_lb = {Timestamp(1, 0),
                                                       Timestamp(1, 0)};
  for (size_t i = 0; i < write_timestamps.size(); ++i) {
    WriteOptions write_opts;
    Slice write_ts = write_timestamps[i];
    write_opts.timestamp = &write_ts;
    for (uint64_t key = 0; key <= kMaxKey; ++key) {
      Status s = db_->Put(write_opts, Key1(key), "value" + std::to_string(i));
      ASSERT_OK(s);
    }
  }
  for (size_t i = 0; i < read_timestamps.size(); ++i) {
    ReadOptions read_opts;
    Slice read_ts = read_timestamps[i];
    Slice read_ts_lb = read_timestamps_lb[i];
    read_opts.timestamp = &read_ts;
    read_opts.iter_start_ts = &read_ts_lb;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    int count = 0;
    uint64_t key = 0;
    for (it->Seek(Key1(0)), key = 0; it->Valid(); it->Next(), ++count, ++key) {
      CheckIterEntry(it.get(), Key1(key), kTypeValue,
                     "value" + std::to_string(i), write_timestamps[i]);
      if (i > 0) {
        it->Next();
        CheckIterEntry(it.get(), Key1(key), kTypeValue,
                       "value" + std::to_string(i - 1),
                       write_timestamps[i - 1]);
      }
    }
    size_t expected_count = kMaxKey + 1;
    ASSERT_EQ(expected_count, count);
  }
  // Delete all keys@ts=5 and check iteration result with start ts set
  {
    std::string write_timestamp = Timestamp(5, 0);
    WriteOptions write_opts;
    Slice write_ts = write_timestamp;
    write_opts.timestamp = &write_ts;
    for (uint64_t key = 0; key < kMaxKey + 1; ++key) {
      Status s = db_->Delete(write_opts, Key1(key));
      ASSERT_OK(s);
    }

    std::string read_timestamp = Timestamp(6, 0);
    ReadOptions read_opts;
    Slice read_ts = read_timestamp;
    read_opts.timestamp = &read_ts;
    std::string read_timestamp_lb = Timestamp(2, 0);
    Slice read_ts_lb = read_timestamp_lb;
    read_opts.iter_start_ts = &read_ts_lb;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    int count = 0;
    uint64_t key = 0;
    for (it->Seek(Key1(0)), key = 0; it->Valid(); it->Next(), ++count, ++key) {
      CheckIterEntry(it.get(), Key1(key), kTypeDeletionWithTimestamp, Slice(),
                     write_ts);
      // Skip key@ts=3 and land on tombstone key@ts=5
      it->Next();
    }
    ASSERT_EQ(kMaxKey + 1, count);
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, ForwardIterateStartSeqnum) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 0xffffffffffffffff;
  const uint64_t kMinKey = kMaxKey - 1023;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  // Need to disable compaction to bottommost level when sequence number will be
  // zeroed out, causing the verification of sequence number to fail in this
  // test.
  options.disable_auto_compactions = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  std::vector<SequenceNumber> start_seqs;

  const int kNumTimestamps = 4;
  std::vector<std::string> write_ts_list;
  for (int t = 0; t != kNumTimestamps; ++t) {
    write_ts_list.push_back(Timestamp(2 * t, /*do not care*/ 17));
  }
  WriteOptions write_opts;
  for (size_t i = 0; i != write_ts_list.size(); ++i) {
    Slice write_ts = write_ts_list[i];
    write_opts.timestamp = &write_ts;
    for (uint64_t k = kMaxKey; k >= kMinKey; --k) {
      Status s;
      if (k % 2) {
        s = db_->Put(write_opts, Key1(k), "value" + std::to_string(i));
      } else {
        s = db_->Delete(write_opts, Key1(k));
      }
      ASSERT_OK(s);
    }
    start_seqs.push_back(db_->GetLatestSequenceNumber());
  }
  std::vector<std::string> read_ts_list;
  for (int t = 0; t != kNumTimestamps - 1; ++t) {
    read_ts_list.push_back(Timestamp(2 * t + 3, /*do not care*/ 17));
  }

  ReadOptions read_opts;
  // Scan with only read_opts.iter_start_seqnum set.
  for (size_t i = 0; i != read_ts_list.size(); ++i) {
    Slice read_ts = read_ts_list[i];
    read_opts.timestamp = &read_ts;
    read_opts.iter_start_seqnum = start_seqs[i] + 1;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    SequenceNumber expected_seq = start_seqs[i] + (kMaxKey - kMinKey) + 1;
    uint64_t key = kMinKey;
    for (iter->Seek(Key1(kMinKey)); iter->Valid(); iter->Next()) {
      CheckIterEntry(
          iter.get(), Key1(key), expected_seq,
          (key % 2) ? kTypeValue : kTypeDeletionWithTimestamp,
          (key % 2) ? "value" + std::to_string(i + 1) : std::string(),
          write_ts_list[i + 1]);
      ++key;
      --expected_seq;
    }
  }
  // Scan with both read_opts.iter_start_seqnum and read_opts.iter_start_ts set.
  std::vector<std::string> read_ts_lb_list;
  for (int t = 0; t < kNumTimestamps - 1; ++t) {
    read_ts_lb_list.push_back(Timestamp(2 * t, /*do not care*/ 17));
  }
  for (size_t i = 0; i < read_ts_list.size(); ++i) {
    Slice read_ts = read_ts_list[i];
    Slice read_ts_lb = read_ts_lb_list[i];
    read_opts.timestamp = &read_ts;
    read_opts.iter_start_ts = &read_ts_lb;
    read_opts.iter_start_seqnum = start_seqs[i] + 1;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    uint64_t key = kMinKey;
    SequenceNumber expected_seq = start_seqs[i] + (kMaxKey - kMinKey) + 1;
    for (it->Seek(Key1(kMinKey)); it->Valid(); it->Next()) {
      CheckIterEntry(it.get(), Key1(key), expected_seq,
                     (key % 2) ? kTypeValue : kTypeDeletionWithTimestamp,
                     "value" + std::to_string(i + 1), write_ts_list[i + 1]);
      ++key;
      --expected_seq;
    }
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, ReseekToTargetTimestamp) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  constexpr size_t kNumKeys = 16;
  options.max_sequential_skip_in_iterations = kNumKeys / 2;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  // Insert kNumKeys
  WriteOptions write_opts;
  Status s;
  for (size_t i = 0; i != kNumKeys; ++i) {
    std::string ts_str = Timestamp(static_cast<uint64_t>(i + 1), 0);
    Slice ts = ts_str;
    write_opts.timestamp = &ts;
    s = db_->Put(write_opts, "foo", "value" + std::to_string(i));
    ASSERT_OK(s);
  }
  {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->SeekToFirst();
    CheckIterUserEntry(iter.get(), "foo", kTypeValue, "value0", ts_str);
    ASSERT_EQ(
        1, options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));

    ts_str = Timestamp(kNumKeys, 0);
    ts = ts_str;
    read_opts.timestamp = &ts;
    iter.reset(db_->NewIterator(read_opts));
    iter->SeekToLast();
    CheckIterUserEntry(iter.get(), "foo", kTypeValue,
                       "value" + std::to_string(kNumKeys - 1), ts_str);
    ASSERT_EQ(
        2, options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, ReseekToNextUserKey) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  constexpr size_t kNumKeys = 16;
  options.max_sequential_skip_in_iterations = kNumKeys / 2;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  // Write kNumKeys + 1 keys
  WriteOptions write_opts;
  Status s;
  for (size_t i = 0; i != kNumKeys; ++i) {
    std::string ts_str = Timestamp(static_cast<uint64_t>(i + 1), 0);
    Slice ts = ts_str;
    write_opts.timestamp = &ts;
    s = db_->Put(write_opts, "a", "value" + std::to_string(i));
    ASSERT_OK(s);
  }
  {
    std::string ts_str = Timestamp(static_cast<uint64_t>(kNumKeys + 1), 0);
    WriteBatch batch(0, 0, kTimestampSize);
    ASSERT_OK(batch.Put("a", "new_value"));
    ASSERT_OK(batch.Put("b", "new_value"));
    s = batch.AssignTimestamp(ts_str);
    ASSERT_OK(s);
    s = db_->Write(write_opts, &batch);
    ASSERT_OK(s);
  }
  {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(static_cast<uint64_t>(kNumKeys + 1), 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->Seek("a");
    iter->Next();
    CheckIterUserEntry(iter.get(), "b", kTypeValue, "new_value", ts_str);
    ASSERT_EQ(
        1, options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, ReseekToUserKeyBeforeSavedKey) {
  Options options = GetDefaultOptions();
  options.env = env_;
  options.create_if_missing = true;
  constexpr size_t kNumKeys = 16;
  options.max_sequential_skip_in_iterations = kNumKeys / 2;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  for (size_t i = 0; i < kNumKeys; ++i) {
    std::string ts_str = Timestamp(static_cast<uint64_t>(i + 1), 0);
    Slice ts = ts_str;
    WriteOptions write_opts;
    write_opts.timestamp = &ts;
    Status s = db_->Put(write_opts, "b", "value" + std::to_string(i));
    ASSERT_OK(s);
  }
  {
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    WriteOptions write_opts;
    write_opts.timestamp = &ts;
    ASSERT_OK(db_->Put(write_opts, "a", "value"));
  }
  {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->SeekToLast();
    iter->Prev();
    CheckIterUserEntry(iter.get(), "a", kTypeValue, "value", ts_str);
    ASSERT_EQ(
        1, options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, MultiGetWithFastLocalBloom) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  // Write any value
  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;

  ASSERT_OK(db_->Put(write_opts, "foo", "bar"));

  ASSERT_OK(Flush());

  // Read with MultiGet
  ReadOptions read_opts;
  read_opts.timestamp = &ts;
  size_t batch_size = 1;
  std::vector<Slice> keys(batch_size);
  std::vector<PinnableSlice> values(batch_size);
  std::vector<Status> statuses(batch_size);
  keys[0] = "foo";
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  db_->MultiGet(read_opts, cfh, batch_size, keys.data(), values.data(),
                statuses.data());

  ASSERT_OK(statuses[0]);
  Close();
}

TEST_P(DBBasicTestWithTimestampTableOptions, MultiGetWithPrefix) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewCappedPrefixTransform(5));
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = false;
  bbto.index_type = GetParam();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  // Write any value
  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;

  ASSERT_OK(db_->Put(write_opts, "foo", "bar"));

  ASSERT_OK(Flush());

  // Read with MultiGet
  ReadOptions read_opts;
  read_opts.timestamp = &ts;
  size_t batch_size = 1;
  std::vector<Slice> keys(batch_size);
  std::vector<PinnableSlice> values(batch_size);
  std::vector<Status> statuses(batch_size);
  keys[0] = "foo";
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  db_->MultiGet(read_opts, cfh, batch_size, keys.data(), values.data(),
                statuses.data());

  ASSERT_OK(statuses[0]);
  Close();
}

TEST_P(DBBasicTestWithTimestampTableOptions, MultiGetWithMemBloomFilter) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewCappedPrefixTransform(5));
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = false;
  bbto.index_type = GetParam();
  options.memtable_prefix_bloom_size_ratio = 0.1;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  // Write any value
  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;

  ASSERT_OK(db_->Put(write_opts, "foo", "bar"));

  // Read with MultiGet
  ts_str = Timestamp(2, 0);
  ts = ts_str;
  ReadOptions read_opts;
  read_opts.timestamp = &ts;
  size_t batch_size = 1;
  std::vector<Slice> keys(batch_size);
  std::vector<PinnableSlice> values(batch_size);
  std::vector<Status> statuses(batch_size);
  keys[0] = "foo";
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  db_->MultiGet(read_opts, cfh, batch_size, keys.data(), values.data(),
                statuses.data());

  ASSERT_OK(statuses[0]);
  Close();
}

TEST_F(DBBasicTestWithTimestamp, MultiGetRangeFiltering) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = false;
  options.memtable_prefix_bloom_size_ratio = 0.1;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  // Write any value
  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;

  // random data
  for (int i = 0; i < 3; i++) {
    auto key = ToString(i * 10);
    auto value = ToString(i * 10);
    Slice key_slice = key;
    Slice value_slice = value;
    ASSERT_OK(db_->Put(write_opts, key_slice, value_slice));
    ASSERT_OK(Flush());
  }

  // Make num_levels to 2 to do key range filtering of sst files
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_OK(db_->Put(write_opts, "foo", "bar"));

  ASSERT_OK(Flush());

  // Read with MultiGet
  ts_str = Timestamp(2, 0);
  ts = ts_str;
  ReadOptions read_opts;
  read_opts.timestamp = &ts;
  size_t batch_size = 1;
  std::vector<Slice> keys(batch_size);
  std::vector<PinnableSlice> values(batch_size);
  std::vector<Status> statuses(batch_size);
  keys[0] = "foo";
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  db_->MultiGet(read_opts, cfh, batch_size, keys.data(), values.data(),
                statuses.data());

  ASSERT_OK(statuses[0]);
  Close();
}

TEST_P(DBBasicTestWithTimestampTableOptions, MultiGetPrefixFilter) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewCappedPrefixTransform(5));
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = false;
  bbto.index_type = GetParam();
  options.memtable_prefix_bloom_size_ratio = 0.1;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;

  ASSERT_OK(db_->Put(write_opts, "foo", "bar"));

  ASSERT_OK(Flush());
  // Read with MultiGet
  ts_str = Timestamp(2, 0);
  ts = ts_str;
  ReadOptions read_opts;
  read_opts.timestamp = &ts;
  size_t batch_size = 1;
  std::vector<Slice> keys(batch_size);
  std::vector<std::string> values(batch_size);
  std::vector<std::string> timestamps(batch_size);
  keys[0] = "foo";
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  std::vector<ColumnFamilyHandle*> cfhs(keys.size(), cfh);
  std::vector<Status> statuses =
      db_->MultiGet(read_opts, cfhs, keys, &values, &timestamps);

  ASSERT_OK(statuses[0]);
  Close();
}

TEST_F(DBBasicTestWithTimestamp, MaxKeysSkippedDuringNext) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  constexpr size_t max_skippable_internal_keys = 2;
  const size_t kNumKeys = max_skippable_internal_keys + 2;
  WriteOptions write_opts;
  Status s;
  {
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    write_opts.timestamp = &ts;
    ASSERT_OK(db_->Put(write_opts, "a", "value"));
  }
  for (size_t i = 0; i < kNumKeys; ++i) {
    std::string ts_str = Timestamp(static_cast<uint64_t>(i + 1), 0);
    Slice ts = ts_str;
    write_opts.timestamp = &ts;
    s = db_->Put(write_opts, "b", "value" + std::to_string(i));
    ASSERT_OK(s);
  }
  {
    ReadOptions read_opts;
    read_opts.max_skippable_internal_keys = max_skippable_internal_keys;
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->SeekToFirst();
    iter->Next();
    ASSERT_TRUE(iter->status().IsIncomplete());
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, MaxKeysSkippedDuringPrev) {
  Options options = GetDefaultOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  constexpr size_t max_skippable_internal_keys = 2;
  const size_t kNumKeys = max_skippable_internal_keys + 2;
  WriteOptions write_opts;
  Status s;
  {
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    write_opts.timestamp = &ts;
    ASSERT_OK(db_->Put(write_opts, "b", "value"));
  }
  for (size_t i = 0; i < kNumKeys; ++i) {
    std::string ts_str = Timestamp(static_cast<uint64_t>(i + 1), 0);
    Slice ts = ts_str;
    write_opts.timestamp = &ts;
    s = db_->Put(write_opts, "a", "value" + std::to_string(i));
    ASSERT_OK(s);
  }
  {
    ReadOptions read_opts;
    read_opts.max_skippable_internal_keys = max_skippable_internal_keys;
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->SeekToLast();
    iter->Prev();
    ASSERT_TRUE(iter->status().IsIncomplete());
  }
  Close();
}

// Create two L0, and compact them to a new L1. In this test, L1 is L_bottom.
// Two L0s:
//       f1                                  f2
// <a, 1, kTypeValue>    <a, 3, kTypeDeletionWithTimestamp>...<b, 2, kTypeValue>
// Since f2.smallest < f1.largest < f2.largest
// f1 and f2 will be the inputs of a real compaction instead of trivial move.
TEST_F(DBBasicTestWithTimestamp, CompactDeletionWithTimestampMarkerToBottom) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.num_levels = 2;
  options.level0_file_num_compaction_trigger = 2;
  DestroyAndReopen(options);
  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;
  ASSERT_OK(db_->Put(write_opts, "a", "value0"));
  ASSERT_OK(Flush());

  ts_str = Timestamp(2, 0);
  ts = ts_str;
  write_opts.timestamp = &ts;
  ASSERT_OK(db_->Put(write_opts, "b", "value0"));
  ts_str = Timestamp(3, 0);
  ts = ts_str;
  write_opts.timestamp = &ts;
  ASSERT_OK(db_->Delete(write_opts, "a"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ReadOptions read_opts;
  ts_str = Timestamp(1, 0);
  ts = ts_str;
  read_opts.timestamp = &ts;
  std::string value;
  Status s = db_->Get(read_opts, "a", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value0", value);

  ts_str = Timestamp(3, 0);
  ts = ts_str;
  read_opts.timestamp = &ts;
  s = db_->Get(read_opts, "a", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Time-travel to the past before deletion
  ts_str = Timestamp(2, 0);
  ts = ts_str;
  read_opts.timestamp = &ts;
  s = db_->Get(read_opts, "a", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value0", value);
  Close();
}

class DBBasicTestWithTimestampFilterPrefixSettings
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<
          std::tuple<std::shared_ptr<const FilterPolicy>, bool, bool,
                     std::shared_ptr<const SliceTransform>, bool, double,
                     BlockBasedTableOptions::IndexType>> {
 public:
  DBBasicTestWithTimestampFilterPrefixSettings()
      : DBBasicTestWithTimestampBase(
            "db_basic_test_with_timestamp_filter_prefix") {}
};

TEST_P(DBBasicTestWithTimestampFilterPrefixSettings, GetAndMultiGet) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  BlockBasedTableOptions bbto;
  bbto.filter_policy = std::get<0>(GetParam());
  bbto.whole_key_filtering = std::get<1>(GetParam());
  bbto.cache_index_and_filter_blocks = std::get<2>(GetParam());
  bbto.index_type = std::get<6>(GetParam());
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.prefix_extractor = std::get<3>(GetParam());
  options.memtable_whole_key_filtering = std::get<4>(GetParam());
  options.memtable_prefix_bloom_size_ratio = std::get<5>(GetParam());

  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  const int kMaxKey = 1000;

  // Write any value
  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;

  int idx = 0;
  for (; idx < kMaxKey / 4; idx++) {
    ASSERT_OK(db_->Put(write_opts, Key1(idx), "bar"));
    ASSERT_OK(db_->Put(write_opts, KeyWithPrefix("foo", idx), "bar"));
  }

  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (; idx < kMaxKey / 2; idx++) {
    ASSERT_OK(db_->Put(write_opts, Key1(idx), "bar"));
    ASSERT_OK(db_->Put(write_opts, KeyWithPrefix("foo", idx), "bar"));
  }

  ASSERT_OK(Flush());

  for (; idx < kMaxKey; idx++) {
    ASSERT_OK(db_->Put(write_opts, Key1(idx), "bar"));
    ASSERT_OK(db_->Put(write_opts, KeyWithPrefix("foo", idx), "bar"));
  }

  // Read with MultiGet
  ReadOptions read_opts;
  read_opts.timestamp = &ts;

  ReadOptions read_opts_total_order;
  read_opts_total_order.timestamp = &ts;
  read_opts_total_order.total_order_seek = true;

  for (idx = 0; idx < kMaxKey; idx++) {
    size_t batch_size = 4;
    std::vector<std::string> keys_str(batch_size);
    std::vector<PinnableSlice> values(batch_size);
    std::vector<Status> statuses(batch_size);
    ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();

    keys_str[0] = Key1(idx);
    keys_str[1] = KeyWithPrefix("foo", idx);
    keys_str[2] = Key1(kMaxKey + idx);
    keys_str[3] = KeyWithPrefix("foo", kMaxKey + idx);

    auto keys = ConvertStrToSlice(keys_str);

    db_->MultiGet(read_opts, cfh, batch_size, keys.data(), values.data(),
                  statuses.data());

    for (int i = 0; i < 2; i++) {
      ASSERT_OK(statuses[i]);
    }
    for (int i = 2; i < 4; i++) {
      ASSERT_TRUE(statuses[i].IsNotFound());
    }

    for (int i = 0; i < 2; i++) {
      std::string value;
      ASSERT_OK(db_->Get(read_opts, keys[i], &value));
      std::unique_ptr<Iterator> it1(db_->NewIterator(read_opts));
      ASSERT_NE(nullptr, it1);
      ASSERT_OK(it1->status());
      // TODO(zjay) Fix seek with prefix
      // it1->Seek(keys[i]);
      // ASSERT_TRUE(it1->Valid());
    }

    for (int i = 2; i < 4; i++) {
      std::string value;
      Status s = db_->Get(read_opts, keys[i], &value);
      ASSERT_TRUE(s.IsNotFound());
    }
  }
  Close();
}

INSTANTIATE_TEST_CASE_P(
    Timestamp, DBBasicTestWithTimestampFilterPrefixSettings,
    ::testing::Combine(
        ::testing::Values(
            std::shared_ptr<const FilterPolicy>(nullptr),
            std::shared_ptr<const FilterPolicy>(NewBloomFilterPolicy(10, true)),
            std::shared_ptr<const FilterPolicy>(NewBloomFilterPolicy(10,
                                                                     false))),
        ::testing::Bool(), ::testing::Bool(),
        ::testing::Values(
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(1)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(4)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(7)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(8))),
        ::testing::Bool(), ::testing::Values(0, 0.1),
        ::testing::Values(
            BlockBasedTableOptions::IndexType::kBinarySearch,
            BlockBasedTableOptions::IndexType::kHashSearch,
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch,
            BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey)));

class DataVisibilityTest : public DBBasicTestWithTimestampBase {
 public:
  DataVisibilityTest() : DBBasicTestWithTimestampBase("data_visibility_test") {
    // Initialize test data
    for (int i = 0; i < kTestDataSize; i++) {
      test_data_[i].key = "key" + ToString(i);
      test_data_[i].value = "value" + ToString(i);
      test_data_[i].timestamp = Timestamp(i, 0);
      test_data_[i].ts = i;
      test_data_[i].seq_num = kMaxSequenceNumber;
    }
  }

 protected:
  struct TestData {
    std::string key;
    std::string value;
    int ts;
    std::string timestamp;
    SequenceNumber seq_num;
  };

  constexpr static int kTestDataSize = 3;
  TestData test_data_[kTestDataSize];

  void PutTestData(int index, ColumnFamilyHandle* cfh = nullptr) {
    ASSERT_LE(index, kTestDataSize);
    WriteOptions write_opts;
    Slice ts_slice = test_data_[index].timestamp;
    write_opts.timestamp = &ts_slice;

    if (cfh == nullptr) {
      ASSERT_OK(
          db_->Put(write_opts, test_data_[index].key, test_data_[index].value));
      const Snapshot* snap = db_->GetSnapshot();
      test_data_[index].seq_num = snap->GetSequenceNumber();
      if (index > 0) {
        ASSERT_GT(test_data_[index].seq_num, test_data_[index - 1].seq_num);
      }
      db_->ReleaseSnapshot(snap);
    } else {
      ASSERT_OK(db_->Put(write_opts, cfh, test_data_[index].key,
                         test_data_[index].value));
    }
  }

  void AssertVisibility(int ts, SequenceNumber seq,
                        std::vector<Status> statuses) {
    ASSERT_EQ(kTestDataSize, statuses.size());
    for (int i = 0; i < kTestDataSize; i++) {
      if (test_data_[i].seq_num <= seq && test_data_[i].ts <= ts) {
        ASSERT_OK(statuses[i]);
      } else {
        ASSERT_TRUE(statuses[i].IsNotFound());
      }
    }
  }

  std::vector<Slice> GetKeys() {
    std::vector<Slice> ret(kTestDataSize);
    for (int i = 0; i < kTestDataSize; i++) {
      ret[i] = test_data_[i].key;
    }
    return ret;
  }

  void VerifyDefaultCF(int ts, const Snapshot* snap = nullptr) {
    ReadOptions read_opts;
    std::string read_ts = Timestamp(ts, 0);
    Slice read_ts_slice = read_ts;
    read_opts.timestamp = &read_ts_slice;
    read_opts.snapshot = snap;

    ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
    std::vector<ColumnFamilyHandle*> cfs(kTestDataSize, cfh);
    SequenceNumber seq =
        snap ? snap->GetSequenceNumber() : kMaxSequenceNumber - 1;

    // There're several MultiGet interfaces with not exactly the same
    // implementations, query data with all of them.
    auto keys = GetKeys();
    std::vector<std::string> values;
    auto s1 = db_->MultiGet(read_opts, cfs, keys, &values);
    AssertVisibility(ts, seq, s1);

    auto s2 = db_->MultiGet(read_opts, keys, &values);
    AssertVisibility(ts, seq, s2);

    std::vector<std::string> timestamps;
    auto s3 = db_->MultiGet(read_opts, cfs, keys, &values, &timestamps);
    AssertVisibility(ts, seq, s3);

    auto s4 = db_->MultiGet(read_opts, keys, &values, &timestamps);
    AssertVisibility(ts, seq, s4);

    std::vector<PinnableSlice> values_ps5(kTestDataSize);
    std::vector<Status> s5(kTestDataSize);
    db_->MultiGet(read_opts, cfh, kTestDataSize, keys.data(), values_ps5.data(),
                  s5.data());
    AssertVisibility(ts, seq, s5);

    std::vector<PinnableSlice> values_ps6(kTestDataSize);
    std::vector<Status> s6(kTestDataSize);
    std::vector<std::string> timestamps_array(kTestDataSize);
    db_->MultiGet(read_opts, cfh, kTestDataSize, keys.data(), values_ps6.data(),
                  timestamps_array.data(), s6.data());
    AssertVisibility(ts, seq, s6);

    std::vector<PinnableSlice> values_ps7(kTestDataSize);
    std::vector<Status> s7(kTestDataSize);
    db_->MultiGet(read_opts, kTestDataSize, cfs.data(), keys.data(),
                  values_ps7.data(), s7.data());
    AssertVisibility(ts, seq, s7);

    std::vector<PinnableSlice> values_ps8(kTestDataSize);
    std::vector<Status> s8(kTestDataSize);
    db_->MultiGet(read_opts, kTestDataSize, cfs.data(), keys.data(),
                  values_ps8.data(), timestamps_array.data(), s8.data());
    AssertVisibility(ts, seq, s8);
  }

  void VerifyDefaultCF(const Snapshot* snap = nullptr) {
    for (int i = 0; i <= kTestDataSize; i++) {
      VerifyDefaultCF(i, snap);
    }
  }
};
constexpr int DataVisibilityTest::kTestDataSize;

// Application specifies timestamp but not snapshot.
//           reader              writer
//                               ts'=90
//           ts=100
//           seq=10
//                               seq'=11
//                               write finishes
//         GetImpl(ts,seq)
// It is OK to return <k, t1, s1> if ts>=t1 AND seq>=s1. If ts>=1t1 but seq<s1,
// the key should not be returned.
TEST_F(DataVisibilityTest, PointLookupWithoutSnapshot1) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::GetImpl:3",
       "DataVisibilityTest::PointLookupWithoutSnapshot1:BeforePut"},
      {"DataVisibilityTest::PointLookupWithoutSnapshot1:AfterPut",
       "DBImpl::GetImpl:4"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    std::string write_ts_str = Timestamp(1, 0);
    Slice write_ts = write_ts_str;
    WriteOptions write_opts;
    write_opts.timestamp = &write_ts;
    TEST_SYNC_POINT(
        "DataVisibilityTest::PointLookupWithoutSnapshot1:BeforePut");
    Status s = db_->Put(write_opts, "foo", "value");
    ASSERT_OK(s);
    TEST_SYNC_POINT("DataVisibilityTest::PointLookupWithoutSnapshot1:AfterPut");
  });
  ReadOptions read_opts;
  std::string read_ts_str = Timestamp(3, 0);
  Slice read_ts = read_ts_str;
  read_opts.timestamp = &read_ts;
  std::string value;
  Status s = db_->Get(read_opts, "foo", &value);

  writer_thread.join();
  ASSERT_TRUE(s.IsNotFound());
  Close();
}

// Application specifies timestamp but not snapshot.
//           reader              writer
//                               ts'=90
//           ts=100
//           seq=10
//                               seq'=11
//                               write finishes
//                               Flush
//         GetImpl(ts,seq)
// It is OK to return <k, t1, s1> if ts>=t1 AND seq>=s1. If ts>=t1 but seq<s1,
// the key should not be returned.
TEST_F(DataVisibilityTest, PointLookupWithoutSnapshot2) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::GetImpl:3",
       "DataVisibilityTest::PointLookupWithoutSnapshot2:BeforePut"},
      {"DataVisibilityTest::PointLookupWithoutSnapshot2:AfterPut",
       "DBImpl::GetImpl:4"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    std::string write_ts_str = Timestamp(1, 0);
    Slice write_ts = write_ts_str;
    WriteOptions write_opts;
    write_opts.timestamp = &write_ts;
    TEST_SYNC_POINT(
        "DataVisibilityTest::PointLookupWithoutSnapshot2:BeforePut");
    Status s = db_->Put(write_opts, "foo", "value");
    ASSERT_OK(s);
    ASSERT_OK(Flush());

    write_ts_str = Timestamp(2, 0);
    write_ts = write_ts_str;
    write_opts.timestamp = &write_ts;
    s = db_->Put(write_opts, "bar", "value");
    ASSERT_OK(s);
    TEST_SYNC_POINT("DataVisibilityTest::PointLookupWithoutSnapshot2:AfterPut");
  });
  ReadOptions read_opts;
  std::string read_ts_str = Timestamp(3, 0);
  Slice read_ts = read_ts_str;
  read_opts.timestamp = &read_ts;
  std::string value;
  Status s = db_->Get(read_opts, "foo", &value);
  writer_thread.join();
  ASSERT_TRUE(s.IsNotFound());
  Close();
}

// Application specifies both timestamp and snapshot.
//       reader               writer
//       seq=10
//                            ts'=90
//       ts=100
//                            seq'=11
//                            write finishes
//       GetImpl(ts,seq)
// Since application specifies both timestamp and snapshot, application expects
// to see data that visible in BOTH timestamp and sequence number. Therefore,
// <k, t1, s1> can be returned only if t1<=ts AND s1<=seq.
TEST_F(DataVisibilityTest, PointLookupWithSnapshot1) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DataVisibilityTest::PointLookupWithSnapshot1:AfterTakingSnap",
       "DataVisibilityTest::PointLookupWithSnapshot1:BeforePut"},
      {"DataVisibilityTest::PointLookupWithSnapshot1:AfterPut",
       "DBImpl::GetImpl:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    std::string write_ts_str = Timestamp(1, 0);
    Slice write_ts = write_ts_str;
    WriteOptions write_opts;
    write_opts.timestamp = &write_ts;
    TEST_SYNC_POINT("DataVisibilityTest::PointLookupWithSnapshot1:BeforePut");
    Status s = db_->Put(write_opts, "foo", "value");
    TEST_SYNC_POINT("DataVisibilityTest::PointLookupWithSnapshot1:AfterPut");
    ASSERT_OK(s);
  });
  ReadOptions read_opts;
  const Snapshot* snap = db_->GetSnapshot();
  TEST_SYNC_POINT(
      "DataVisibilityTest::PointLookupWithSnapshot1:AfterTakingSnap");
  read_opts.snapshot = snap;
  std::string read_ts_str = Timestamp(3, 0);
  Slice read_ts = read_ts_str;
  read_opts.timestamp = &read_ts;
  std::string value;
  Status s = db_->Get(read_opts, "foo", &value);
  writer_thread.join();

  ASSERT_TRUE(s.IsNotFound());

  db_->ReleaseSnapshot(snap);
  Close();
}

// Application specifies both timestamp and snapshot.
//       reader               writer
//       seq=10
//                            ts'=90
//       ts=100
//                            seq'=11
//                            write finishes
//                            Flush
//       GetImpl(ts,seq)
// Since application specifies both timestamp and snapshot, application expects
// to see data that visible in BOTH timestamp and sequence number. Therefore,
// <k, t1, s1> can be returned only if t1<=ts AND s1<=seq.
TEST_F(DataVisibilityTest, PointLookupWithSnapshot2) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DataVisibilityTest::PointLookupWithSnapshot2:AfterTakingSnap",
       "DataVisibilityTest::PointLookupWithSnapshot2:BeforePut"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    std::string write_ts_str = Timestamp(1, 0);
    Slice write_ts = write_ts_str;
    WriteOptions write_opts;
    write_opts.timestamp = &write_ts;
    TEST_SYNC_POINT("DataVisibilityTest::PointLookupWithSnapshot2:BeforePut");
    Status s = db_->Put(write_opts, "foo", "value1");
    ASSERT_OK(s);
    ASSERT_OK(Flush());

    write_ts_str = Timestamp(2, 0);
    write_ts = write_ts_str;
    write_opts.timestamp = &write_ts;
    s = db_->Put(write_opts, "bar", "value2");
    ASSERT_OK(s);
  });
  const Snapshot* snap = db_->GetSnapshot();
  TEST_SYNC_POINT(
      "DataVisibilityTest::PointLookupWithSnapshot2:AfterTakingSnap");
  writer_thread.join();
  std::string read_ts_str = Timestamp(3, 0);
  Slice read_ts = read_ts_str;
  ReadOptions read_opts;
  read_opts.snapshot = snap;
  read_opts.timestamp = &read_ts;
  std::string value;
  Status s = db_->Get(read_opts, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());
  db_->ReleaseSnapshot(snap);
  Close();
}

// Application specifies timestamp but not snapshot.
//      reader                writer
//                            ts'=90
//      ts=100
//      seq=10
//                            seq'=11
//                            write finishes
//      scan(ts,seq)
// <k, t1, s1> can be seen in scan as long as ts>=t1 AND seq>=s1. If ts>=t1 but
// seq<s1, then the key should not be returned.
TEST_F(DataVisibilityTest, RangeScanWithoutSnapshot) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::NewIterator:3",
       "DataVisibilityTest::RangeScanWithoutSnapshot:BeforePut"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    WriteOptions write_opts;
    TEST_SYNC_POINT("DataVisibilityTest::RangeScanWithoutSnapshot:BeforePut");
    for (int i = 0; i < 3; ++i) {
      std::string write_ts_str = Timestamp(i + 1, 0);
      Slice write_ts = write_ts_str;
      write_opts.timestamp = &write_ts;
      Status s = db_->Put(write_opts, "key" + std::to_string(i),
                          "value" + std::to_string(i));
      ASSERT_OK(s);
    }
  });
  std::string read_ts_str = Timestamp(10, 0);
  Slice read_ts = read_ts_str;
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  read_opts.timestamp = &read_ts;
  Iterator* it = db_->NewIterator(read_opts);
  ASSERT_NE(nullptr, it);
  writer_thread.join();
  it->SeekToFirst();
  ASSERT_FALSE(it->Valid());
  delete it;
  Close();
}

// Application specifies both timestamp and snapshot.
//       reader         writer
//       seq=10
//                      ts'=90
//       ts=100         seq'=11
//                      write finishes
//       scan(ts,seq)
// <k, t1, s1> can be seen by the scan only if t1<=ts AND s1<=seq. If t1<=ts
// but s1>seq, then the key should not be returned.
TEST_F(DataVisibilityTest, RangeScanWithSnapshot) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DataVisibilityTest::RangeScanWithSnapshot:AfterTakingSnapshot",
       "DataVisibilityTest::RangeScanWithSnapshot:BeforePut"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    WriteOptions write_opts;
    TEST_SYNC_POINT("DataVisibilityTest::RangeScanWithSnapshot:BeforePut");
    for (int i = 0; i < 3; ++i) {
      std::string write_ts_str = Timestamp(i + 1, 0);
      Slice write_ts = write_ts_str;
      write_opts.timestamp = &write_ts;
      Status s = db_->Put(write_opts, "key" + std::to_string(i),
                          "value" + std::to_string(i));
      ASSERT_OK(s);
    }
  });
  const Snapshot* snap = db_->GetSnapshot();
  TEST_SYNC_POINT(
      "DataVisibilityTest::RangeScanWithSnapshot:AfterTakingSnapshot");

  writer_thread.join();

  std::string read_ts_str = Timestamp(10, 0);
  Slice read_ts = read_ts_str;
  ReadOptions read_opts;
  read_opts.snapshot = snap;
  read_opts.total_order_seek = true;
  read_opts.timestamp = &read_ts;
  Iterator* it = db_->NewIterator(read_opts);
  ASSERT_NE(nullptr, it);
  it->Seek("key0");
  ASSERT_FALSE(it->Valid());

  delete it;
  db_->ReleaseSnapshot(snap);
  Close();
}

// Application specifies both timestamp and snapshot.
// Query each combination and make sure for MultiGet key <k, t1, s1>, only
// return keys that ts>=t1 AND seq>=s1.
TEST_F(DataVisibilityTest, MultiGetWithTimestamp) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  const Snapshot* snap0 = db_->GetSnapshot();
  PutTestData(0);
  VerifyDefaultCF();
  VerifyDefaultCF(snap0);

  const Snapshot* snap1 = db_->GetSnapshot();
  PutTestData(1);
  VerifyDefaultCF();
  VerifyDefaultCF(snap0);
  VerifyDefaultCF(snap1);

  ASSERT_OK(Flush());

  const Snapshot* snap2 = db_->GetSnapshot();
  PutTestData(2);
  VerifyDefaultCF();
  VerifyDefaultCF(snap0);
  VerifyDefaultCF(snap1);
  VerifyDefaultCF(snap2);

  db_->ReleaseSnapshot(snap0);
  db_->ReleaseSnapshot(snap1);
  db_->ReleaseSnapshot(snap2);

  Close();
}

// Application specifies timestamp but not snapshot.
//           reader              writer
//                               ts'=0, 1
//           ts=3
//           seq=10
//                               seq'=11, 12
//                               write finishes
//         MultiGet(ts,seq)
// For MultiGet <k, t1, s1>, only return keys that ts>=t1 AND seq>=s1.
TEST_F(DataVisibilityTest, MultiGetWithoutSnapshot) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::MultiGet:AfterGetSeqNum1",
       "DataVisibilityTest::MultiGetWithoutSnapshot:BeforePut"},
      {"DataVisibilityTest::MultiGetWithoutSnapshot:AfterPut",
       "DBImpl::MultiGet:AfterGetSeqNum2"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    TEST_SYNC_POINT("DataVisibilityTest::MultiGetWithoutSnapshot:BeforePut");
    PutTestData(0);
    PutTestData(1);
    TEST_SYNC_POINT("DataVisibilityTest::MultiGetWithoutSnapshot:AfterPut");
  });

  ReadOptions read_opts;
  std::string read_ts = Timestamp(kTestDataSize, 0);
  Slice read_ts_slice = read_ts;
  read_opts.timestamp = &read_ts_slice;
  auto keys = GetKeys();
  std::vector<std::string> values;
  auto ss = db_->MultiGet(read_opts, keys, &values);

  writer_thread.join();
  for (auto s : ss) {
    ASSERT_TRUE(s.IsNotFound());
  }
  VerifyDefaultCF();
  Close();
}

TEST_F(DataVisibilityTest, MultiGetCrossCF) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  CreateAndReopenWithCF({"second"}, options);
  ColumnFamilyHandle* second_cf = handles_[1];

  const Snapshot* snap0 = db_->GetSnapshot();
  PutTestData(0);
  PutTestData(0, second_cf);
  VerifyDefaultCF();
  VerifyDefaultCF(snap0);

  const Snapshot* snap1 = db_->GetSnapshot();
  PutTestData(1);
  PutTestData(1, second_cf);
  VerifyDefaultCF();
  VerifyDefaultCF(snap0);
  VerifyDefaultCF(snap1);

  ASSERT_OK(Flush());

  const Snapshot* snap2 = db_->GetSnapshot();
  PutTestData(2);
  PutTestData(2, second_cf);
  VerifyDefaultCF();
  VerifyDefaultCF(snap0);
  VerifyDefaultCF(snap1);
  VerifyDefaultCF(snap2);

  ReadOptions read_opts;
  std::string read_ts = Timestamp(kTestDataSize, 0);
  Slice read_ts_slice = read_ts;
  read_opts.timestamp = &read_ts_slice;
  read_opts.snapshot = snap1;
  auto keys = GetKeys();
  auto keys2 = GetKeys();
  keys.insert(keys.end(), keys2.begin(), keys2.end());
  std::vector<ColumnFamilyHandle*> cfs(kTestDataSize,
                                       db_->DefaultColumnFamily());
  std::vector<ColumnFamilyHandle*> cfs2(kTestDataSize, second_cf);
  cfs.insert(cfs.end(), cfs2.begin(), cfs2.end());

  std::vector<std::string> values;
  auto ss = db_->MultiGet(read_opts, cfs, keys, &values);
  for (int i = 0; i < 2 * kTestDataSize; i++) {
    if (i % 3 == 0) {
      // only the first key for each column family should be returned
      ASSERT_OK(ss[i]);
    } else {
      ASSERT_TRUE(ss[i].IsNotFound());
    }
  }

  db_->ReleaseSnapshot(snap0);
  db_->ReleaseSnapshot(snap1);
  db_->ReleaseSnapshot(snap2);
  Close();
}

class DBBasicTestWithTimestampCompressionSettings
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<
          std::tuple<std::shared_ptr<const FilterPolicy>, CompressionType,
                     uint32_t, uint32_t>> {
 public:
  DBBasicTestWithTimestampCompressionSettings()
      : DBBasicTestWithTimestampBase(
            "db_basic_test_with_timestamp_compression") {}
};

TEST_P(DBBasicTestWithTimestampCompressionSettings, PutAndGet) {
  const int kNumKeysPerFile = 1024;
  const size_t kNumTimestamps = 4;
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.env = env_;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  size_t ts_sz = Timestamp(0, 0).size();
  TestComparator test_cmp(ts_sz);
  options.comparator = &test_cmp;
  BlockBasedTableOptions bbto;
  bbto.filter_policy = std::get<0>(GetParam());
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  const CompressionType comp_type = std::get<1>(GetParam());
#if LZ4_VERSION_NUMBER < 10400  // r124+
  if (comp_type == kLZ4Compression || comp_type == kLZ4HCCompression) {
    return;
  }
#endif  // LZ4_VERSION_NUMBER >= 10400
  if (!ZSTD_Supported() && comp_type == kZSTD) {
    return;
  }
  if (!Zlib_Supported() && comp_type == kZlibCompression) {
    return;
  }

  options.compression = comp_type;
  options.compression_opts.max_dict_bytes = std::get<2>(GetParam());
  if (comp_type == kZSTD) {
    options.compression_opts.zstd_max_train_bytes = std::get<2>(GetParam());
  }
  options.compression_opts.parallel_threads = std::get<3>(GetParam());
  options.target_file_size_base = 1 << 26;  // 64MB
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(2, num_cfs);
  std::vector<std::string> write_ts_list;
  std::vector<std::string> read_ts_list;

  for (size_t i = 0; i != kNumTimestamps; ++i) {
    write_ts_list.push_back(Timestamp(i * 2, 0));
    read_ts_list.push_back(Timestamp(1 + i * 2, 0));
    const Slice write_ts = write_ts_list.back();
    WriteOptions wopts;
    wopts.timestamp = &write_ts;
    for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
      for (size_t j = 0; j != (kNumKeysPerFile - 1) / kNumTimestamps; ++j) {
        ASSERT_OK(Put(cf, Key1(j),
                      "value_" + std::to_string(j) + "_" + std::to_string(i),
                      wopts));
      }
    }
  }
  const auto& verify_db_func = [&]() {
    for (size_t i = 0; i != kNumTimestamps; ++i) {
      ReadOptions ropts;
      const Slice read_ts = read_ts_list[i];
      ropts.timestamp = &read_ts;
      for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
        ColumnFamilyHandle* cfh = handles_[cf];
        for (size_t j = 0; j != (kNumKeysPerFile - 1) / kNumTimestamps; ++j) {
          std::string value;
          ASSERT_OK(db_->Get(ropts, cfh, Key1(j), &value));
          ASSERT_EQ("value_" + std::to_string(j) + "_" + std::to_string(i),
                    value);
        }
      }
    }
  };
  verify_db_func();
  Close();
}

TEST_P(DBBasicTestWithTimestampCompressionSettings, PutDeleteGet) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  const int kNumKeysPerFile = 1024;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  BlockBasedTableOptions bbto;
  bbto.filter_policy = std::get<0>(GetParam());
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  const CompressionType comp_type = std::get<1>(GetParam());
#if LZ4_VERSION_NUMBER < 10400  // r124+
  if (comp_type == kLZ4Compression || comp_type == kLZ4HCCompression) {
    return;
  }
#endif  // LZ4_VERSION_NUMBER >= 10400
  if (!ZSTD_Supported() && comp_type == kZSTD) {
    return;
  }
  if (!Zlib_Supported() && comp_type == kZlibCompression) {
    return;
  }

  options.compression = comp_type;
  options.compression_opts.max_dict_bytes = std::get<2>(GetParam());
  if (comp_type == kZSTD) {
    options.compression_opts.zstd_max_train_bytes = std::get<2>(GetParam());
  }
  options.compression_opts.parallel_threads = std::get<3>(GetParam());
  options.target_file_size_base = 1 << 26;  // 64MB

  DestroyAndReopen(options);

  const size_t kNumL0Files =
      static_cast<size_t>(Options().level0_file_num_compaction_trigger);
  {
    // Generate enough L0 files with ts=1 to trigger compaction to L1
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    WriteOptions wopts;
    wopts.timestamp = &ts;
    for (size_t i = 0; i != kNumL0Files; ++i) {
      for (int j = 0; j != kNumKeysPerFile; ++j) {
        ASSERT_OK(db_->Put(wopts, Key1(j), "value" + std::to_string(i)));
      }
      ASSERT_OK(db_->Flush(FlushOptions()));
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    // Generate another L0 at ts=3
    ts_str = Timestamp(3, 0);
    ts = ts_str;
    wopts.timestamp = &ts;
    for (int i = 0; i != kNumKeysPerFile; ++i) {
      std::string key_str = Key1(i);
      Slice key(key_str);
      if ((i % 3) == 0) {
        ASSERT_OK(db_->Delete(wopts, key));
      } else {
        ASSERT_OK(db_->Put(wopts, key, "new_value"));
      }
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
    // Populate memtable at ts=5
    ts_str = Timestamp(5, 0);
    ts = ts_str;
    wopts.timestamp = &ts;
    for (int i = 0; i != kNumKeysPerFile; ++i) {
      std::string key_str = Key1(i);
      Slice key(key_str);
      if ((i % 3) == 1) {
        ASSERT_OK(db_->Delete(wopts, key));
      } else if ((i % 3) == 2) {
        ASSERT_OK(db_->Put(wopts, key, "new_value_2"));
      }
    }
  }
  {
    std::string ts_str = Timestamp(6, 0);
    Slice ts = ts_str;
    ReadOptions ropts;
    ropts.timestamp = &ts;
    for (uint64_t i = 0; i != static_cast<uint64_t>(kNumKeysPerFile); ++i) {
      std::string value;
      Status s = db_->Get(ropts, Key1(i), &value);
      if ((i % 3) == 2) {
        ASSERT_OK(s);
        ASSERT_EQ("new_value_2", value);
      } else {
        ASSERT_TRUE(s.IsNotFound());
      }
    }
  }
}

#ifndef ROCKSDB_LITE
// A class which remembers the name of each flushed file.
class FlushedFileCollector : public EventListener {
 public:
  FlushedFileCollector() {}
  ~FlushedFileCollector() override {}

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    InstrumentedMutexLock lock(&mutex_);
    flushed_files_.push_back(info.file_path);
  }

  std::vector<std::string> GetFlushedFiles() {
    std::vector<std::string> result;
    {
      InstrumentedMutexLock lock(&mutex_);
      result = flushed_files_;
    }
    return result;
  }

  void ClearFlushedFiles() {
    InstrumentedMutexLock lock(&mutex_);
    flushed_files_.clear();
  }

 private:
  std::vector<std::string> flushed_files_;
  InstrumentedMutex mutex_;
};

TEST_P(DBBasicTestWithTimestampCompressionSettings, PutAndGetWithCompaction) {
  const int kNumKeysPerFile = 1024;
  const size_t kNumTimestamps = 2;
  const size_t kNumKeysPerTimestamp = (kNumKeysPerFile - 1) / kNumTimestamps;
  const size_t kSplitPosBase = kNumKeysPerTimestamp / 2;
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.env = env_;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));

  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);

  size_t ts_sz = Timestamp(0, 0).size();
  TestComparator test_cmp(ts_sz);
  options.comparator = &test_cmp;
  BlockBasedTableOptions bbto;
  bbto.filter_policy = std::get<0>(GetParam());
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  const CompressionType comp_type = std::get<1>(GetParam());
#if LZ4_VERSION_NUMBER < 10400  // r124+
  if (comp_type == kLZ4Compression || comp_type == kLZ4HCCompression) {
    return;
  }
#endif  // LZ4_VERSION_NUMBER >= 10400
  if (!ZSTD_Supported() && comp_type == kZSTD) {
    return;
  }
  if (!Zlib_Supported() && comp_type == kZlibCompression) {
    return;
  }

  options.compression = comp_type;
  options.compression_opts.max_dict_bytes = std::get<2>(GetParam());
  if (comp_type == kZSTD) {
    options.compression_opts.zstd_max_train_bytes = std::get<2>(GetParam());
  }
  options.compression_opts.parallel_threads = std::get<3>(GetParam());
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  size_t num_cfs = handles_.size();
  ASSERT_EQ(2, num_cfs);
  std::vector<std::string> write_ts_list;
  std::vector<std::string> read_ts_list;

  const auto& verify_records_func = [&](size_t i, size_t begin, size_t end,
                                        ColumnFamilyHandle* cfh) {
    std::string value;
    std::string timestamp;

    ReadOptions ropts;
    const Slice read_ts = read_ts_list[i];
    ropts.timestamp = &read_ts;
    std::string expected_timestamp =
        std::string(write_ts_list[i].data(), write_ts_list[i].size());

    for (size_t j = begin; j <= end; ++j) {
      ASSERT_OK(db_->Get(ropts, cfh, Key1(j), &value, &timestamp));
      ASSERT_EQ("value_" + std::to_string(j) + "_" + std::to_string(i), value);
      ASSERT_EQ(expected_timestamp, timestamp);
    }
  };

  for (size_t i = 0; i != kNumTimestamps; ++i) {
    write_ts_list.push_back(Timestamp(i * 2, 0));
    read_ts_list.push_back(Timestamp(1 + i * 2, 0));
    const Slice write_ts = write_ts_list.back();
    WriteOptions wopts;
    wopts.timestamp = &write_ts;
    for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
      size_t memtable_get_start = 0;
      for (size_t j = 0; j != kNumKeysPerTimestamp; ++j) {
        ASSERT_OK(Put(cf, Key1(j),
                      "value_" + std::to_string(j) + "_" + std::to_string(i),
                      wopts));
        if (j == kSplitPosBase + i || j == kNumKeysPerTimestamp - 1) {
          verify_records_func(i, memtable_get_start, j, handles_[cf]);
          memtable_get_start = j + 1;

          // flush all keys with the same timestamp to two sst files, split at
          // incremental positions such that lowerlevel[1].smallest.userkey ==
          // higherlevel[0].largest.userkey
          ASSERT_OK(Flush(cf));

          // compact files (2 at each level) to a lower level such that all
          // keys with the same timestamp is at one level, with newer versions
          // at higher levels.
          CompactionOptions compact_opt;
          compact_opt.compression = kNoCompression;
          ASSERT_OK(db_->CompactFiles(compact_opt, handles_[cf],
                                      collector->GetFlushedFiles(),
                                      static_cast<int>(kNumTimestamps - i)));
          collector->ClearFlushedFiles();
        }
      }
    }
  }
  const auto& verify_db_func = [&]() {
    for (size_t i = 0; i != kNumTimestamps; ++i) {
      ReadOptions ropts;
      const Slice read_ts = read_ts_list[i];
      ropts.timestamp = &read_ts;
      std::string expected_timestamp(write_ts_list[i].data(),
                                     write_ts_list[i].size());
      for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
        ColumnFamilyHandle* cfh = handles_[cf];
        verify_records_func(i, 0, kNumKeysPerTimestamp - 1, cfh);
      }
    }
  };
  verify_db_func();
  Close();
}

TEST_F(DBBasicTestWithTimestamp, BatchWriteAndMultiGet) {
  const int kNumKeysPerFile = 8192;
  const size_t kNumTimestamps = 2;
  const size_t kNumKeysPerTimestamp = (kNumKeysPerFile - 1) / kNumTimestamps;
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.env = env_;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  options.memtable_prefix_bloom_size_ratio = 0.1;
  options.memtable_whole_key_filtering = true;

  size_t ts_sz = Timestamp(0, 0).size();
  TestComparator test_cmp(ts_sz);
  options.comparator = &test_cmp;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(
      10 /*bits_per_key*/, false /*use_block_based_builder*/));
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(2, num_cfs);
  std::vector<std::string> write_ts_list;
  std::vector<std::string> read_ts_list;

  const auto& verify_records_func = [&](size_t i, ColumnFamilyHandle* cfh) {
    std::vector<Slice> keys;
    std::vector<std::string> key_vals;
    std::vector<std::string> values;
    std::vector<std::string> timestamps;

    for (size_t j = 0; j != kNumKeysPerTimestamp; ++j) {
      key_vals.push_back(Key1(j));
    }
    for (size_t j = 0; j != kNumKeysPerTimestamp; ++j) {
      keys.push_back(key_vals[j]);
    }

    ReadOptions ropts;
    const Slice read_ts = read_ts_list[i];
    ropts.timestamp = &read_ts;
    std::string expected_timestamp(write_ts_list[i].data(),
                                   write_ts_list[i].size());

    std::vector<ColumnFamilyHandle*> cfhs(keys.size(), cfh);
    std::vector<Status> statuses =
        db_->MultiGet(ropts, cfhs, keys, &values, &timestamps);
    for (size_t j = 0; j != kNumKeysPerTimestamp; ++j) {
      ASSERT_OK(statuses[j]);
      ASSERT_EQ("value_" + std::to_string(j) + "_" + std::to_string(i),
                values[j]);
      ASSERT_EQ(expected_timestamp, timestamps[j]);
    }
  };

  for (size_t i = 0; i != kNumTimestamps; ++i) {
    write_ts_list.push_back(Timestamp(i * 2, 0));
    read_ts_list.push_back(Timestamp(1 + i * 2, 0));
    const Slice& write_ts = write_ts_list.back();
    for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
      WriteOptions wopts;
      WriteBatch batch(0, 0, ts_sz);
      for (size_t j = 0; j != kNumKeysPerTimestamp; ++j) {
        ASSERT_OK(
            batch.Put(handles_[cf], Key1(j),
                      "value_" + std::to_string(j) + "_" + std::to_string(i)));
      }
      ASSERT_OK(batch.AssignTimestamp(write_ts));
      ASSERT_OK(db_->Write(wopts, &batch));

      verify_records_func(i, handles_[cf]);

      ASSERT_OK(Flush(cf));
    }
  }

  const auto& verify_db_func = [&]() {
    for (size_t i = 0; i != kNumTimestamps; ++i) {
      ReadOptions ropts;
      const Slice read_ts = read_ts_list[i];
      ropts.timestamp = &read_ts;
      for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
        ColumnFamilyHandle* cfh = handles_[cf];
        verify_records_func(i, cfh);
      }
    }
  };
  verify_db_func();
  Close();
}

TEST_F(DBBasicTestWithTimestamp, MultiGetNoReturnTs) {
  Options options = CurrentOptions();
  options.env = env_;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  WriteOptions write_opts;
  std::string ts_str = Timestamp(1, 0);
  Slice ts = ts_str;
  write_opts.timestamp = &ts;
  ASSERT_OK(db_->Put(write_opts, "foo", "value"));
  ASSERT_OK(db_->Put(write_opts, "bar", "value"));
  ASSERT_OK(db_->Put(write_opts, "fooxxxxxxxxxxxxxxxx", "value"));
  ASSERT_OK(db_->Put(write_opts, "barxxxxxxxxxxxxxxxx", "value"));
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  ts_str = Timestamp(2, 0);
  ts = ts_str;
  ReadOptions read_opts;
  read_opts.timestamp = &ts;
  {
    ColumnFamilyHandle* column_families[] = {cfh, cfh};
    Slice keys[] = {"foo", "bar"};
    PinnableSlice values[] = {PinnableSlice(), PinnableSlice()};
    Status statuses[] = {Status::OK(), Status::OK()};
    dbfull()->MultiGet(read_opts, /*num_keys=*/2, &column_families[0], &keys[0],
                       &values[0], &statuses[0], /*sorted_input=*/false);
    for (const auto& s : statuses) {
      ASSERT_OK(s);
    }
  }
  {
    ColumnFamilyHandle* column_families[] = {cfh, cfh, cfh, cfh};
    // Make user keys longer than configured timestamp size (16 bytes) to
    // verify RocksDB does not use the trailing bytes 'x' as timestamp.
    Slice keys[] = {"fooxxxxxxxxxxxxxxxx", "barxxxxxxxxxxxxxxxx", "foo", "bar"};
    PinnableSlice values[] = {PinnableSlice(), PinnableSlice(), PinnableSlice(),
                              PinnableSlice()};
    Status statuses[] = {Status::OK(), Status::OK(), Status::OK(),
                         Status::OK()};
    dbfull()->MultiGet(read_opts, /*num_keys=*/4, &column_families[0], &keys[0],
                       &values[0], &statuses[0], /*sorted_input=*/false);
    for (const auto& s : statuses) {
      ASSERT_OK(s);
    }
  }
  Close();
}

#endif  // !ROCKSDB_LITE

INSTANTIATE_TEST_CASE_P(
    Timestamp, DBBasicTestWithTimestampCompressionSettings,
    ::testing::Combine(
        ::testing::Values(std::shared_ptr<const FilterPolicy>(nullptr),
                          std::shared_ptr<const FilterPolicy>(
                              NewBloomFilterPolicy(10, false))),
        ::testing::Values(kNoCompression, kZlibCompression, kLZ4Compression,
                          kLZ4HCCompression, kZSTD),
        ::testing::Values(0, 1 << 14), ::testing::Values(1, 4)));

class DBBasicTestWithTimestampPrefixSeek
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<
          std::tuple<std::shared_ptr<const SliceTransform>,
                     std::shared_ptr<const FilterPolicy>, bool,
                     BlockBasedTableOptions::IndexType>> {
 public:
  DBBasicTestWithTimestampPrefixSeek()
      : DBBasicTestWithTimestampBase(
            "/db_basic_test_with_timestamp_prefix_seek") {}
};

TEST_P(DBBasicTestWithTimestampPrefixSeek, IterateWithPrefix) {
  const size_t kNumKeysPerFile = 128;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.prefix_extractor = std::get<0>(GetParam());
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  BlockBasedTableOptions bbto;
  bbto.filter_policy = std::get<1>(GetParam());
  bbto.index_type = std::get<3>(GetParam());
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  const uint64_t kMaxKey = 0xffffffffffffffff;
  const uint64_t kMinKey = 0xfffffffffffff000;
  const std::vector<std::string> write_ts_list = {Timestamp(3, 0xffffffff),
                                                  Timestamp(6, 0xffffffff)};
  WriteOptions write_opts;
  {
    for (size_t i = 0; i != write_ts_list.size(); ++i) {
      Slice write_ts = write_ts_list[i];
      write_opts.timestamp = &write_ts;
      for (uint64_t key = kMaxKey; key >= kMinKey; --key) {
        Status s = db_->Put(write_opts, Key1(key), "value" + std::to_string(i));
        ASSERT_OK(s);
      }
    }
  }
  const std::vector<std::string> read_ts_list = {Timestamp(5, 0xffffffff),
                                                 Timestamp(9, 0xffffffff)};
  {
    ReadOptions read_opts;
    read_opts.total_order_seek = false;
    read_opts.prefix_same_as_start = std::get<2>(GetParam());
    fprintf(stdout, "%s %s %d\n", options.prefix_extractor->Name(),
            bbto.filter_policy ? bbto.filter_policy->Name() : "null",
            static_cast<int>(read_opts.prefix_same_as_start));
    for (size_t i = 0; i != read_ts_list.size(); ++i) {
      Slice read_ts = read_ts_list[i];
      read_opts.timestamp = &read_ts;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));

      // Seek to kMaxKey
      iter->Seek(Key1(kMaxKey));
      CheckIterUserEntry(iter.get(), Key1(kMaxKey), kTypeValue,
                         "value" + std::to_string(i), write_ts_list[i]);
      iter->Next();
      ASSERT_FALSE(iter->Valid());

      // Seek to kMinKey
      iter->Seek(Key1(kMinKey));
      CheckIterUserEntry(iter.get(), Key1(kMinKey), kTypeValue,
                         "value" + std::to_string(i), write_ts_list[i]);
      iter->Prev();
      ASSERT_FALSE(iter->Valid());
    }
    const std::vector<uint64_t> targets = {kMinKey, kMinKey + 0x10,
                                           kMinKey + 0x100, kMaxKey};
    const SliceTransform* const pe = options.prefix_extractor.get();
    ASSERT_NE(nullptr, pe);
    const size_t kPrefixShift =
        8 * (Key1(0).size() - pe->Transform(Key1(0)).size());
    const uint64_t kPrefixMask =
        ~((static_cast<uint64_t>(1) << kPrefixShift) - 1);
    const uint64_t kNumKeysWithinPrefix =
        (static_cast<uint64_t>(1) << kPrefixShift);
    for (size_t i = 0; i != read_ts_list.size(); ++i) {
      Slice read_ts = read_ts_list[i];
      read_opts.timestamp = &read_ts;
      std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
      // Forward and backward iterate.
      for (size_t j = 0; j != targets.size(); ++j) {
        std::string start_key = Key1(targets[j]);
        uint64_t expected_ub =
            (targets[j] & kPrefixMask) - 1 + kNumKeysWithinPrefix;
        uint64_t expected_key = targets[j];
        size_t count = 0;
        it->Seek(Key1(targets[j]));
        while (it->Valid()) {
          std::string saved_prev_key;
          saved_prev_key.assign(it->key().data(), it->key().size());

          // Out of prefix
          if (!read_opts.prefix_same_as_start &&
              pe->Transform(saved_prev_key) != pe->Transform(start_key)) {
            break;
          }
          CheckIterUserEntry(it.get(), Key1(expected_key), kTypeValue,
                             "value" + std::to_string(i), write_ts_list[i]);
          ++count;
          ++expected_key;
          it->Next();
        }
        ASSERT_EQ(expected_ub - targets[j] + 1, count);

        count = 0;
        expected_key = targets[j];
        it->SeekForPrev(start_key);
        uint64_t expected_lb = (targets[j] & kPrefixMask);
        while (it->Valid()) {
          // Out of prefix
          if (!read_opts.prefix_same_as_start &&
              pe->Transform(it->key()) != pe->Transform(start_key)) {
            break;
          }
          CheckIterUserEntry(it.get(), Key1(expected_key), kTypeValue,
                             "value" + std::to_string(i), write_ts_list[i]);
          ++count;
          --expected_key;
          it->Prev();
        }
        ASSERT_EQ(targets[j] - std::max(expected_lb, kMinKey) + 1, count);
      }
    }
  }
  Close();
}

// TODO(yanqin): consider handling non-fixed-length prefix extractors, e.g.
// NoopTransform.
INSTANTIATE_TEST_CASE_P(
    Timestamp, DBBasicTestWithTimestampPrefixSeek,
    ::testing::Combine(
        ::testing::Values(
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(1)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(4)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(7)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(8))),
        ::testing::Values(std::shared_ptr<const FilterPolicy>(nullptr),
                          std::shared_ptr<const FilterPolicy>(
                              NewBloomFilterPolicy(10 /*bits_per_key*/, false)),
                          std::shared_ptr<const FilterPolicy>(
                              NewBloomFilterPolicy(20 /*bits_per_key*/,
                                                   false))),
        ::testing::Bool(),
        ::testing::Values(
            BlockBasedTableOptions::IndexType::kBinarySearch,
            BlockBasedTableOptions::IndexType::kHashSearch,
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch,
            BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey)));

class DBBasicTestWithTsIterTombstones
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<
          std::tuple<std::shared_ptr<const SliceTransform>,
                     std::shared_ptr<const FilterPolicy>, int,
                     BlockBasedTableOptions::IndexType>> {
 public:
  DBBasicTestWithTsIterTombstones()
      : DBBasicTestWithTimestampBase("/db_basic_ts_iter_tombstones") {}
};

TEST_P(DBBasicTestWithTsIterTombstones, IterWithDelete) {
  constexpr size_t kNumKeysPerFile = 128;
  Options options = CurrentOptions();
  options.env = env_;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.prefix_extractor = std::get<0>(GetParam());
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  BlockBasedTableOptions bbto;
  bbto.filter_policy = std::get<1>(GetParam());
  bbto.index_type = std::get<3>(GetParam());
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.num_levels = std::get<2>(GetParam());
  DestroyAndReopen(options);
  std::vector<std::string> write_ts_strs = {Timestamp(2, 0), Timestamp(4, 0)};
  constexpr uint64_t kMaxKey = 0xffffffffffffffff;
  constexpr uint64_t kMinKey = 0xfffffffffffff000;
  // Insert kMinKey...kMaxKey
  uint64_t key = kMinKey;
  WriteOptions write_opts;
  Slice ts = write_ts_strs[0];
  write_opts.timestamp = &ts;
  do {
    Status s = db_->Put(write_opts, Key1(key), "value" + std::to_string(key));
    ASSERT_OK(s);
    if (kMaxKey == key) {
      break;
    }
    ++key;
  } while (true);

  ts = write_ts_strs[1];
  write_opts.timestamp = &ts;
  for (key = kMaxKey; key >= kMinKey; --key) {
    Status s;
    if (0 != (key % 2)) {
      s = db_->Put(write_opts, Key1(key), "value1" + std::to_string(key));
    } else {
      s = db_->Delete(write_opts, Key1(key));
    }
    ASSERT_OK(s);
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  {
    std::string read_ts = Timestamp(4, 0);
    ts = read_ts;
    ReadOptions read_opts;
    read_opts.total_order_seek = true;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    size_t count = 0;
    key = kMinKey + 1;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++count, key += 2) {
      ASSERT_EQ(Key1(key), iter->key());
      ASSERT_EQ("value1" + std::to_string(key), iter->value());
    }
    ASSERT_EQ((kMaxKey - kMinKey + 1) / 2, count);

    for (iter->SeekToLast(), count = 0, key = kMaxKey; iter->Valid();
         key -= 2, ++count, iter->Prev()) {
      ASSERT_EQ(Key1(key), iter->key());
      ASSERT_EQ("value1" + std::to_string(key), iter->value());
    }
    ASSERT_EQ((kMaxKey - kMinKey + 1) / 2, count);
  }
  Close();
}

INSTANTIATE_TEST_CASE_P(
    Timestamp, DBBasicTestWithTsIterTombstones,
    ::testing::Combine(
        ::testing::Values(
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(7)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(8))),
        ::testing::Values(std::shared_ptr<const FilterPolicy>(nullptr),
                          std::shared_ptr<const FilterPolicy>(
                              NewBloomFilterPolicy(10, false)),
                          std::shared_ptr<const FilterPolicy>(
                              NewBloomFilterPolicy(20, false))),
        ::testing::Values(2, 6),
        ::testing::Values(
            BlockBasedTableOptions::IndexType::kBinarySearch,
            BlockBasedTableOptions::IndexType::kHashSearch,
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch,
            BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey)));

}  // namespace ROCKSDB_NAMESPACE

#ifdef ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS
extern "C" {
void RegisterCustomObjects(int argc, char** argv);
}
#else
void RegisterCustomObjects(int /*argc*/, char** /*argv*/) {}
#endif  // !ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
