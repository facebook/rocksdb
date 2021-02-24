//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_index.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class DBBlobCompactionTest : public DBTestBase {
 public:
  DBBlobCompactionTest()
      : DBTestBase("/db_blob_compaction_test", /*env_do_fsync=*/false) {}
};

namespace {

class DummyStackedBlobDbCompactionFilter : public CompactionFilter {
 public:
  explicit DummyStackedBlobDbCompactionFilter() = default;
  const char* Name() const override {
    return "rocksdb.compaction.filter.dummy.stacked.blobdb";
  }
  bool IsStackedBlobDbInternalCompactionFilter() const override { return true; }
};

class FilterByKeyLength : public CompactionFilter {
 public:
  explicit FilterByKeyLength(size_t len) : length_threshold_(len) {}
  const char* Name() const override {
    return "rocksdb.compaction.filter.by.key.length";
  }
  CompactionFilter::Decision FilterBlobByKey(
      int /*level*/, const Slice& key, std::string* /*new_value*/,
      std::string* /*skip_until*/) const override {
    if (key.size() < length_threshold_) {
      return CompactionFilter::Decision::kRemove;
    }
    return CompactionFilter::Decision::kKeep;
  }

 private:
  size_t length_threshold_;
};

class BadBlobCompactionFilter : public CompactionFilter {
 public:
  explicit BadBlobCompactionFilter(std::string prefix)
      : prefix_(std::move(prefix)) {}
  const char* Name() const override { return "rocksdb.compaction.filter.bad"; }
  CompactionFilter::Decision FilterBlobByKey(
      int /*level*/, const Slice& key, std::string* /*new_value*/,
      std::string* /*skip_until*/) const override {
    if (key.size() >= prefix_.size() &&
        0 == strncmp(prefix_.data(), key.data(), prefix_.size())) {
      return CompactionFilter::Decision::kUndetermined;
    }
    return CompactionFilter::Decision::kChangeBlobIndex;
  }
  CompactionFilter::Decision FilterV2(
      int /*level*/, const Slice& /*key*/, ValueType /*value_type*/,
      const Slice& /*existing_value*/, std::string* /*new_value*/,
      std::string* /*skip_until*/) const override {
    return CompactionFilter::Decision::kChangeBlobIndex;
  }

 private:
  const std::string prefix_;
};

class ValueBlindWriteFilter : public CompactionFilter {
 public:
  explicit ValueBlindWriteFilter(std::string new_val)
      : new_value_(std::move(new_val)) {}
  const char* Name() const override {
    return "rocksdb.compaction.filter.blind.write";
  }
  CompactionFilter::Decision FilterBlobByKey(
      int level, const Slice& key, std::string* new_value,
      std::string* skip_until) const override;

 private:
  const std::string new_value_;
};

CompactionFilter::Decision ValueBlindWriteFilter::FilterBlobByKey(
    int /*level*/, const Slice& /*key*/, std::string* new_value,
    std::string* /*skip_until*/) const {
  assert(new_value);
  new_value->assign(new_value_);
  return CompactionFilter::Decision::kChangeValue;
}

class ValueMutationFilter : public CompactionFilter {
 public:
  explicit ValueMutationFilter(std::string padding)
      : padding_(std::move(padding)) {}
  const char* Name() const override {
    return "rocksdb.compaction.filter.value.mutation";
  }
  CompactionFilter::Decision FilterV2(int level, const Slice& key,
                                      ValueType value_type,
                                      const Slice& existing_value,
                                      std::string* new_value,
                                      std::string* skip_until) const override;

 private:
  const std::string padding_;
};

CompactionFilter::Decision ValueMutationFilter::FilterV2(
    int /*level*/, const Slice& /*key*/, ValueType value_type,
    const Slice& existing_value, std::string* new_value,
    std::string* /*skip_until*/) const {
  assert(CompactionFilter::ValueType::kBlobIndex != value_type);
  if (CompactionFilter::ValueType::kValue != value_type) {
    return CompactionFilter::Decision::kKeep;
  }
  assert(new_value);
  new_value->assign(existing_value.data(), existing_value.size());
  new_value->append(padding_);
  return CompactionFilter::Decision::kChangeValue;
}
}  // anonymous namespace

TEST_F(DBBlobCompactionTest, FilterByKeyLength) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  constexpr size_t kKeyLength = 2;
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new FilterByKeyLength(kKeyLength));
  options.compaction_filter = compaction_filter_guard.get();

  constexpr char short_key[] = "a";
  constexpr char long_key[] = "abc";
  constexpr char blob_value[] = "value";

  DestroyAndReopen(options);
  ASSERT_OK(Put(short_key, blob_value));
  ASSERT_OK(Put(long_key, blob_value));
  ASSERT_OK(Flush());
  CompactRangeOptions cro;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), short_key, &value).IsNotFound());
  value.clear();
  ASSERT_OK(db_->Get(ReadOptions(), long_key, &value));
  ASSERT_EQ("value", value);
  Close();
}

TEST_F(DBBlobCompactionTest, BlindWriteFilter) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  constexpr char new_blob_value[] = "new_blob_value";
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new ValueBlindWriteFilter(new_blob_value));
  options.compaction_filter = compaction_filter_guard.get();
  DestroyAndReopen(options);
  const std::vector<std::string> keys = {"a", "b", "c"};
  const std::vector<std::string> values = {"a_value", "b_value", "c_value"};
  assert(keys.size() == values.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  for (const auto& key : keys) {
    ASSERT_EQ(new_blob_value, Get(key));
  }
  Close();
}

TEST_F(DBBlobCompactionTest, BadDecisionFromCompactionFilter) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  const std::string kPrefix("a");
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new BadBlobCompactionFilter(kPrefix));
  options.compaction_filter = compaction_filter_guard.get();
  DestroyAndReopen(options);
  ASSERT_OK(Put("b", "value"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                                /*end=*/nullptr)
                  .IsNotSupported());
  Close();

  DestroyAndReopen(options);
  std::string key(kPrefix);
  ASSERT_OK(Put(key, "value"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                                /*end=*/nullptr)
                  .IsNotSupported());
  Close();
}

TEST_F(DBBlobCompactionTest, CompactionFilter_InlinedTTLIndex) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new ValueMutationFilter(""));
  options.compaction_filter = compaction_filter_guard.get();
  DestroyAndReopen(options);
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";
  // Fake an inlined TTL blob index.
  std::string blob_index;
  constexpr uint64_t expiration = 1234567890;
  BlobIndex::EncodeInlinedTTL(&blob_index, expiration, blob);
  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 0, key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(Flush());
  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                                /*end=*/nullptr)
                  .IsCorruption());
  Close();
}

TEST_F(DBBlobCompactionTest, CompactionFilter) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  constexpr char padding[] = "_delta";
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new ValueMutationFilter(padding));
  options.compaction_filter = compaction_filter_guard.get();
  DestroyAndReopen(options);
  const std::vector<std::pair<std::string, std::string>> kvs = {
      {"a", "a_value"}, {"b", "b_value"}, {"c", "c_value"}};
  for (const auto& kv : kvs) {
    ASSERT_OK(Put(kv.first, kv.second));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  for (const auto& kv : kvs) {
    ASSERT_EQ(kv.second + std::string(padding), Get(kv.first));
  }
  Close();
}

TEST_F(DBBlobCompactionTest, CorruptedBlobIndex) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new ValueMutationFilter(""));
  options.compaction_filter = compaction_filter_guard.get();
  DestroyAndReopen(options);
  // Mock a corrupted blob index
  constexpr char key[] = "key";
  std::string blob_idx("blob_idx");
  WriteBatch write_batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&write_batch, 0, key, blob_idx));
  ASSERT_OK(db_->Write(WriteOptions(), &write_batch));
  ASSERT_OK(Flush());
  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                                /*end=*/nullptr)
                  .IsCorruption());
  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}