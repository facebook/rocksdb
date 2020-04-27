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
#include "test_util/fault_injection_test_env.h"
#if !defined(ROCKSDB_LITE)
#include "test_util/sync_point.h"
#endif

namespace ROCKSDB_NAMESPACE {
class DBBasicTestWithTimestampBase : public DBTestBase {
 public:
  explicit DBBasicTestWithTimestampBase(const std::string& dbname)
      : DBTestBase(dbname) {}

 protected:
  static std::string Key1(uint64_t k) {
    std::string ret;
    PutFixed64(&ret, k);
    std::reverse(ret.begin(), ret.end());
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
                          const Slice& expected_value,
                          const Slice& expected_ts) const {
    ASSERT_TRUE(it->Valid());
    ASSERT_OK(it->status());
    ASSERT_EQ(expected_key, it->key());
    ASSERT_EQ(expected_value, it->value());
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
    ParsedInternalKey parsed_ikey(ukey_and_ts, expected_seq, expected_val_type);
    std::string ikey;
    AppendInternalKey(&ikey, parsed_ikey);
    ASSERT_EQ(Slice(ikey), it->key());
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

TEST_F(DBBasicTestWithTimestamp, SimpleForwardIterate) {
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
    for (it->Seek(Key1(0)), key = start_keys[i]; it->Valid();
         it->Next(), ++count, ++key) {
      CheckIterUserEntry(it.get(), Key1(key), "value" + std::to_string(i),
                         write_timestamps[i]);
    }
    size_t expected_count = kMaxKey - start_keys[i] + 1;
    ASSERT_EQ(expected_count, count);

    // SeekToFirst() with lower bound.
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
        CheckIterUserEntry(it.get(), Key1(key), "value" + std::to_string(i),
                           write_timestamps[i]);
      }
      ASSERT_EQ(r - std::max(l, start_keys[i]), count);
      l += (kMaxKey / 100);
      r -= (kMaxKey / 100);
    }
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, SimpleForwardIterateLowerTsBound) {
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
      CheckIterUserEntry(it.get(), Key1(key), "value" + std::to_string(i),
                         write_timestamps[i]);
      if (i > 0) {
        it->Next();
        CheckIterUserEntry(it.get(), Key1(key), "value" + std::to_string(i - 1),
                           write_timestamps[i - 1]);
      }
    }
    size_t expected_count = kMaxKey + 1;
    ASSERT_EQ(expected_count, count);
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
    uint64_t k = kMinKey;
    do {
      Status s = db_->Put(write_opts, Key1(k), "value" + std::to_string(i));
      ASSERT_OK(s);
      if (k == kMaxKey) {
        break;
      }
      ++k;
    } while (k != 0);
    start_seqs.push_back(db_->GetLatestSequenceNumber());
  }
  std::vector<std::string> read_ts_list;
  for (int t = 0; t != kNumTimestamps - 1; ++t) {
    read_ts_list.push_back(Timestamp(2 * t + 3, /*do not care*/ 17));
  }
  ReadOptions read_opts;
  for (size_t i = 0; i != read_ts_list.size(); ++i) {
    Slice read_ts = read_ts_list[i];
    read_opts.timestamp = &read_ts;
    read_opts.iter_start_seqnum = start_seqs[i];
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    SequenceNumber expected_seq = start_seqs[i] + 1;
    uint64_t key = kMinKey;
    for (iter->Seek(Key1(kMinKey)); iter->Valid(); iter->Next()) {
      CheckIterEntry(iter.get(), Key1(key), expected_seq, kTypeValue,
                     "value" + std::to_string(i + 1), write_ts_list[i + 1]);
      ++key;
      ++expected_seq;
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
    CheckIterUserEntry(iter.get(), "foo", "value0", ts_str);
    ASSERT_EQ(
        1, options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));
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
    batch.Put("a", "new_value");
    batch.Put("b", "new_value");
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
    CheckIterUserEntry(iter.get(), "b", "new_value", ts_str);
    ASSERT_EQ(
        1, options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, MaxKeysSkipped) {
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
          db_->CompactFiles(compact_opt, handles_[cf],
                            collector->GetFlushedFiles(),
                            static_cast<int>(kNumTimestamps - i));
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
      batch.AssignTimestamp(write_ts);
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
                     std::shared_ptr<const FilterPolicy>, bool>> {
 public:
  DBBasicTestWithTimestampPrefixSeek()
      : DBBasicTestWithTimestampBase(
            "/db_basic_test_with_timestamp_prefix_seek") {}
};

TEST_P(DBBasicTestWithTimestampPrefixSeek, ForwardIterateWithPrefix) {
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
      uint64_t key = kMinKey;
      do {
        Status s = db_->Put(write_opts, Key1(key), "value" + std::to_string(i));
        ASSERT_OK(s);
        if (key == kMaxKey) {
          break;
        }
        ++key;
      } while (true);
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
      CheckIterUserEntry(iter.get(), Key1(kMaxKey), "value" + std::to_string(i),
                         write_ts_list[i]);
      iter->Next();
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
          CheckIterUserEntry(it.get(), Key1(expected_key),
                             "value" + std::to_string(i), write_ts_list[i]);
          ++count;
          ++expected_key;
          it->Next();
        }
        ASSERT_EQ(expected_ub - targets[j] + 1, count);
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
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(4)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(7)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(8))),
        ::testing::Values(std::shared_ptr<const FilterPolicy>(nullptr),
                          std::shared_ptr<const FilterPolicy>(
                              NewBloomFilterPolicy(10 /*bits_per_key*/, false)),
                          std::shared_ptr<const FilterPolicy>(
                              NewBloomFilterPolicy(20 /*bits_per_key*/,
                                                   false))),
        ::testing::Bool()));

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
