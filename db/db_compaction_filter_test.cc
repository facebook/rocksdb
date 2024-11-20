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

namespace ROCKSDB_NAMESPACE {

static int cfilter_count = 0;
static int cfilter_skips = 0;

// This is a static filter used for filtering
// kvs during the compaction process.
static std::string NEW_VALUE = "NewValue";

class DBTestCompactionFilter : public DBTestBase {
 public:
  DBTestCompactionFilter()
      : DBTestBase("db_compaction_filter_test", /*env_do_fsync=*/true) {}
};

// Param variant of DBTestBase::ChangeCompactOptions
class DBTestCompactionFilterWithCompactParam
    : public DBTestCompactionFilter,
      public ::testing::WithParamInterface<DBTestBase::OptionConfig> {
 public:
  DBTestCompactionFilterWithCompactParam() : DBTestCompactionFilter() {
    option_config_ = GetParam();
    Destroy(last_options_);
    auto options = CurrentOptions();
    if (option_config_ == kDefault || option_config_ == kUniversalCompaction ||
        option_config_ == kUniversalCompactionMultiLevel) {
      options.create_if_missing = true;
    }
    if (option_config_ == kLevelSubcompactions ||
        option_config_ == kUniversalSubcompactions) {
      assert(options.max_subcompactions > 1);
    }
    Reopen(options);
  }
};

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
INSTANTIATE_TEST_CASE_P(
    CompactionFilterWithOption, DBTestCompactionFilterWithCompactParam,
    ::testing::Values(DBTestBase::OptionConfig::kDefault,
                      DBTestBase::OptionConfig::kUniversalCompaction,
                      DBTestBase::OptionConfig::kUniversalCompactionMultiLevel,
                      DBTestBase::OptionConfig::kLevelSubcompactions,
                      DBTestBase::OptionConfig::kUniversalSubcompactions));
#else
// Run fewer cases in non-full valgrind to save time.
INSTANTIATE_TEST_CASE_P(CompactionFilterWithOption,
                        DBTestCompactionFilterWithCompactParam,
                        ::testing::Values(DBTestBase::OptionConfig::kDefault));
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

class KeepFilter : public CompactionFilter {
 public:
  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    cfilter_count++;
    return false;
  }

  const char* Name() const override { return "KeepFilter"; }
};

class DeleteFilter : public CompactionFilter {
 public:
  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    cfilter_count++;
    return true;
  }

  bool FilterMergeOperand(int /*level*/, const Slice& /*key*/,
                          const Slice& /*operand*/) const override {
    return true;
  }

  const char* Name() const override { return "DeleteFilter"; }
};

class DeleteISFilter : public CompactionFilter {
 public:
  bool Filter(int /*level*/, const Slice& key, const Slice& /*value*/,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    cfilter_count++;
    int i = std::stoi(key.ToString());
    if (i > 5 && i <= 105) {
      return true;
    }
    return false;
  }

  bool IgnoreSnapshots() const override { return true; }

  const char* Name() const override { return "DeleteFilter"; }
};

// Skip x if floor(x/10) is even, use range skips. Requires that keys are
// zero-padded to length 10.
class SkipEvenFilter : public CompactionFilter {
 public:
  Decision FilterV2(int /*level*/, const Slice& key, ValueType /*value_type*/,
                    const Slice& /*existing_value*/, std::string* /*new_value*/,
                    std::string* skip_until) const override {
    cfilter_count++;
    int i = std::stoi(key.ToString());
    if (i / 10 % 2 == 0) {
      char key_str[100];
      snprintf(key_str, sizeof(key_str), "%010d", i / 10 * 10 + 10);
      *skip_until = key_str;
      ++cfilter_skips;
      return Decision::kRemoveAndSkipUntil;
    }
    return Decision::kKeep;
  }

  bool IgnoreSnapshots() const override { return true; }

  const char* Name() const override { return "DeleteFilter"; }
};

class ConditionalFilter : public CompactionFilter {
 public:
  explicit ConditionalFilter(const std::string* filtered_value)
      : filtered_value_(filtered_value) {}
  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& value,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    return value.ToString() == *filtered_value_;
  }

  const char* Name() const override { return "ConditionalFilter"; }

 private:
  const std::string* filtered_value_;
};

class ChangeFilter : public CompactionFilter {
 public:
  explicit ChangeFilter() = default;

  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
              std::string* new_value, bool* value_changed) const override {
    assert(new_value != nullptr);
    *new_value = NEW_VALUE;
    *value_changed = true;
    return false;
  }

  const char* Name() const override { return "ChangeFilter"; }
};

class KeepFilterFactory : public CompactionFilterFactory {
 public:
  explicit KeepFilterFactory(bool check_context = false,
                             bool check_context_cf_id = false,
                             bool check_context_input_table_properties = false)
      : check_context_(check_context),
        check_context_cf_id_(check_context_cf_id),
        check_context_input_table_properties_(
            check_context_input_table_properties),
        compaction_filter_created_(false) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    if (check_context_) {
      EXPECT_EQ(expect_full_compaction_.load(), context.is_full_compaction);
      EXPECT_EQ(expect_manual_compaction_.load(), context.is_manual_compaction);
      EXPECT_EQ(expect_input_start_level_.load(), context.input_start_level);
    }
    if (check_context_input_table_properties_) {
      EXPECT_TRUE(expect_input_table_properties_ ==
                  context.input_table_properties);
    }
    if (check_context_cf_id_) {
      EXPECT_EQ(expect_cf_id_.load(), context.column_family_id);
    }
    compaction_filter_created_ = true;
    return std::unique_ptr<CompactionFilter>(new KeepFilter());
  }

  bool compaction_filter_created() const { return compaction_filter_created_; }

  const char* Name() const override { return "KeepFilterFactory"; }
  bool check_context_;
  bool check_context_cf_id_;
  // `check_context_input_table_properties_` can be true only when access to
  // `expect_input_table_properties_` is syncronized since we can't have
  // std::atomic<TablePropertiesCollection> unfortunately
  bool check_context_input_table_properties_;
  std::atomic_bool expect_full_compaction_;
  std::atomic_bool expect_manual_compaction_;
  std::atomic<uint32_t> expect_cf_id_;
  std::atomic<int> expect_input_start_level_;
  TablePropertiesCollection expect_input_table_properties_;
  bool compaction_filter_created_;
};

// This filter factory is configured with a `TableFileCreationReason`. Only
// table files created for that reason will undergo filtering. This
// configurability makes it useful to tests for filtering non-compaction table
// files, such as "CompactionFilterFlush" and "CompactionFilterRecovery".
class DeleteFilterFactory : public CompactionFilterFactory {
 public:
  explicit DeleteFilterFactory(TableFileCreationReason reason)
      : reason_(reason) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    EXPECT_EQ(reason_, context.reason);
    if (context.reason == TableFileCreationReason::kCompaction &&
        !context.is_manual_compaction) {
      // Table files created by automatic compaction do not undergo filtering.
      // Presumably some tests rely on this.
      return std::unique_ptr<CompactionFilter>(nullptr);
    }
    return std::unique_ptr<CompactionFilter>(new DeleteFilter());
  }

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    return reason_ == reason;
  }

  const char* Name() const override { return "DeleteFilterFactory"; }

 private:
  const TableFileCreationReason reason_;
};

// Delete Filter Factory which ignores snapshots
class DeleteISFilterFactory : public CompactionFilterFactory {
 public:
  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    if (context.is_manual_compaction) {
      return std::unique_ptr<CompactionFilter>(new DeleteISFilter());
    } else {
      return std::unique_ptr<CompactionFilter>(nullptr);
    }
  }

  const char* Name() const override { return "DeleteFilterFactory"; }
};

class SkipEvenFilterFactory : public CompactionFilterFactory {
 public:
  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    if (context.is_manual_compaction) {
      return std::unique_ptr<CompactionFilter>(new SkipEvenFilter());
    } else {
      return std::unique_ptr<CompactionFilter>(nullptr);
    }
  }

  const char* Name() const override { return "SkipEvenFilterFactory"; }
};

class ConditionalFilterFactory : public CompactionFilterFactory {
 public:
  explicit ConditionalFilterFactory(const Slice& filtered_value)
      : filtered_value_(filtered_value.ToString()) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::unique_ptr<CompactionFilter>(
        new ConditionalFilter(&filtered_value_));
  }

  const char* Name() const override { return "ConditionalFilterFactory"; }

 private:
  std::string filtered_value_;
};

class ChangeFilterFactory : public CompactionFilterFactory {
 public:
  explicit ChangeFilterFactory() = default;

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::unique_ptr<CompactionFilter>(new ChangeFilter());
  }

  const char* Name() const override { return "ChangeFilterFactory"; }
};

TEST_F(DBTestCompactionFilter, CompactionFilter) {
  Options options = CurrentOptions();
  options.max_open_files = -1;
  options.num_levels = 3;
  options.compaction_filter_factory = std::make_shared<KeepFilterFactory>();
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Write 100K keys, these are written to a few files in L0.
  const std::string value(10, 'x');
  for (int i = 0; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    ASSERT_OK(Put(1, key, value));
  }
  ASSERT_OK(Flush(1));

  // Push all files to the highest level L2. Verify that
  // the compaction is each level invokes the filter for
  // all the keys in that level.
  cfilter_count = 0;
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]));
  ASSERT_EQ(cfilter_count, 100000);
  cfilter_count = 0;
  ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]));
  ASSERT_EQ(cfilter_count, 100000);

  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 1), 0);
  ASSERT_NE(NumTableFilesAtLevel(2, 1), 0);
  cfilter_count = 0;

  // All the files are in the lowest level.
  // Verify that all but the 100001st record
  // has sequence number zero. The 100001st record
  // is at the tip of this snapshot and cannot
  // be zeroed out.
  int count = 0;
  int total = 0;
  Arena arena;
  {
    InternalKeyComparator icmp(options.comparator);
    ReadOptions read_options;
    ScopedArenaPtr<InternalIterator> iter(dbfull()->NewInternalIterator(
        read_options, &arena, kMaxSequenceNumber, handles_[1]));
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    while (iter->Valid()) {
      ParsedInternalKey ikey(Slice(), 0, kTypeValue);
      ASSERT_OK(ParseInternalKey(iter->key(), &ikey, true /* log_err_key */));
      total++;
      if (ikey.sequence != 0) {
        count++;
      }
      iter->Next();
    }
    ASSERT_OK(iter->status());
  }
  ASSERT_EQ(total, 100000);
  ASSERT_EQ(count, 0);

  // overwrite all the 100K keys once again.
  for (int i = 0; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    ASSERT_OK(Put(1, key, value));
  }
  ASSERT_OK(Flush(1));

  // push all files to the highest level L2. This
  // means that all keys should pass at least once
  // via the compaction filter
  cfilter_count = 0;
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]));
  ASSERT_EQ(cfilter_count, 100000);
  cfilter_count = 0;
  ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]));
  ASSERT_EQ(cfilter_count, 100000);
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 1), 0);
  ASSERT_NE(NumTableFilesAtLevel(2, 1), 0);

  // create a new database with the compaction
  // filter in such a way that it deletes all keys
  options.compaction_filter_factory = std::make_shared<DeleteFilterFactory>(
      TableFileCreationReason::kCompaction);
  options.create_if_missing = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // write all the keys once again.
  for (int i = 0; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    ASSERT_OK(Put(1, key, value));
  }
  ASSERT_OK(Flush(1));
  ASSERT_NE(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2, 1), 0);

  // Push all files to the highest level L2. This
  // triggers the compaction filter to delete all keys,
  // verify that at the end of the compaction process,
  // nothing is left.
  cfilter_count = 0;
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]));
  ASSERT_EQ(cfilter_count, 100000);
  cfilter_count = 0;
  ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]));
  ASSERT_EQ(cfilter_count, 0);
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 1), 0);

  {
    // Scan the entire database to ensure that nothing is left
    std::unique_ptr<Iterator> iter(
        db_->NewIterator(ReadOptions(), handles_[1]));
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
      count++;
      iter->Next();
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 0);
  }

  // The sequence number of the remaining record
  // is not zeroed out even though it is at the
  // level Lmax because this record is at the tip
  count = 0;
  {
    InternalKeyComparator icmp(options.comparator);
    ReadOptions read_options;
    ScopedArenaPtr<InternalIterator> iter(dbfull()->NewInternalIterator(
        read_options, &arena, kMaxSequenceNumber, handles_[1]));
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    while (iter->Valid()) {
      ParsedInternalKey ikey(Slice(), 0, kTypeValue);
      ASSERT_OK(ParseInternalKey(iter->key(), &ikey, true /* log_err_key */));
      ASSERT_NE(ikey.sequence, (unsigned)0);
      count++;
      iter->Next();
    }
    ASSERT_EQ(count, 0);
  }
}

// Tests the edge case where compaction does not produce any output -- all
// entries are deleted. The compaction should create bunch of 'DeleteFile'
// entries in VersionEdit, but none of the 'AddFile's.
TEST_F(DBTestCompactionFilter, CompactionFilterDeletesAll) {
  Options options = CurrentOptions();
  options.compaction_filter_factory = std::make_shared<DeleteFilterFactory>(
      TableFileCreationReason::kCompaction);
  options.disable_auto_compactions = true;
  options.create_if_missing = true;
  DestroyAndReopen(options);

  // put some data
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      ASSERT_OK(Put(std::to_string(table * 100 + i), "val"));
    }
    ASSERT_OK(Flush());
  }

  // this will produce empty file (delete compaction filter)
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(0U, CountLiveFiles());

  Reopen(options);

  Iterator* itr = db_->NewIterator(ReadOptions());
  itr->SeekToFirst();
  ASSERT_OK(itr->status());
  // empty db
  ASSERT_TRUE(!itr->Valid());

  delete itr;
}

TEST_F(DBTestCompactionFilter, CompactionFilterFlush) {
  // Tests a `CompactionFilterFactory` that filters when table file is created
  // by flush.
  Options options = CurrentOptions();
  options.compaction_filter_factory =
      std::make_shared<DeleteFilterFactory>(TableFileCreationReason::kFlush);
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen(options);

  // Puts and Merges are purged in flush.
  ASSERT_OK(Put("a", "v"));
  ASSERT_OK(Merge("b", "v"));
  ASSERT_OK(Flush());
  ASSERT_EQ("NOT_FOUND", Get("a"));
  ASSERT_EQ("NOT_FOUND", Get("b"));

  // However, Puts and Merges are preserved by recovery.
  ASSERT_OK(Put("a", "v"));
  ASSERT_OK(Merge("b", "v"));
  Reopen(options);
  ASSERT_EQ("v", Get("a"));
  ASSERT_EQ("v", Get("b"));

  // Likewise, compaction does not apply filtering.
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("v", Get("a"));
  ASSERT_EQ("v", Get("b"));
}

TEST_F(DBTestCompactionFilter, CompactionFilterRecovery) {
  // Tests a `CompactionFilterFactory` that filters when table file is created
  // by recovery.
  Options options = CurrentOptions();
  options.compaction_filter_factory =
      std::make_shared<DeleteFilterFactory>(TableFileCreationReason::kRecovery);
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen(options);

  // Puts and Merges are purged in recovery.
  ASSERT_OK(Put("a", "v"));
  ASSERT_OK(Merge("b", "v"));
  Reopen(options);
  ASSERT_EQ("NOT_FOUND", Get("a"));
  ASSERT_EQ("NOT_FOUND", Get("b"));

  // However, Puts and Merges are preserved by flush.
  ASSERT_OK(Put("a", "v"));
  ASSERT_OK(Merge("b", "v"));
  ASSERT_OK(Flush());
  ASSERT_EQ("v", Get("a"));
  ASSERT_EQ("v", Get("b"));

  // Likewise, compaction does not apply filtering.
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("v", Get("a"));
  ASSERT_EQ("v", Get("b"));
}

TEST_P(DBTestCompactionFilterWithCompactParam,
       CompactionFilterWithValueChange) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.compaction_filter_factory = std::make_shared<ChangeFilterFactory>();
  CreateAndReopenWithCF({"pikachu"}, options);

  // Write 100K+1 keys, these are written to a few files
  // in L0. We do this so that the current snapshot points
  // to the 100001 key.The compaction filter is  not invoked
  // on keys that are visible via a snapshot because we
  // anyways cannot delete it.
  const std::string value(10, 'x');
  for (int i = 0; i < 100001; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    ASSERT_OK(Put(1, key, value));
  }

  // push all files to  lower levels
  ASSERT_OK(Flush(1));
  if (option_config_ != kUniversalCompactionMultiLevel &&
      option_config_ != kUniversalSubcompactions) {
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]));
    ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]));
  } else {
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
  }

  // re-write all data again
  for (int i = 0; i < 100001; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    ASSERT_OK(Put(1, key, value));
  }

  // push all files to  lower levels. This should
  // invoke the compaction filter for all 100000 keys.
  ASSERT_OK(Flush(1));
  if (option_config_ != kUniversalCompactionMultiLevel &&
      option_config_ != kUniversalSubcompactions) {
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]));
    ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]));
  } else {
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
  }

  // verify that all keys now have the new value that
  // was set by the compaction process.
  for (int i = 0; i < 100001; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    std::string newvalue = Get(1, key);
    ASSERT_EQ(newvalue.compare(NEW_VALUE), 0);
  }
}

TEST_F(DBTestCompactionFilter, CompactionFilterWithMergeOperator) {
  std::string one, two, three, four;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  PutFixed64(&four, 4);

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  options.num_levels = 3;
  // Filter out keys with value is 2.
  options.compaction_filter_factory =
      std::make_shared<ConditionalFilterFactory>(two);
  DestroyAndReopen(options);

  // In the same compaction, a value type needs to be deleted based on
  // compaction filter, and there is a merge type for the key. compaction
  // filter result is ignored.
  ASSERT_OK(db_->Put(WriteOptions(), "foo", two));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "foo", one));
  ASSERT_OK(Flush());
  std::string newvalue = Get("foo");
  ASSERT_EQ(newvalue, three);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  newvalue = Get("foo");
  ASSERT_EQ(newvalue, three);

  // value key can be deleted based on compaction filter, leaving only
  // merge keys.
  ASSERT_OK(db_->Put(WriteOptions(), "bar", two));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  newvalue = Get("bar");
  ASSERT_EQ("NOT_FOUND", newvalue);
  ASSERT_OK(db_->Merge(WriteOptions(), "bar", two));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  newvalue = Get("bar");
  ASSERT_EQ(two, two);

  // Compaction filter never applies to merge keys.
  ASSERT_OK(db_->Put(WriteOptions(), "foobar", one));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "foobar", two));
  ASSERT_OK(Flush());
  newvalue = Get("foobar");
  ASSERT_EQ(newvalue, three);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  newvalue = Get("foobar");
  ASSERT_EQ(newvalue, three);

  // In the same compaction, both of value type and merge type keys need to be
  // deleted based on compaction filter, and there is a merge type for the key.
  // For both keys, compaction filter results are ignored.
  ASSERT_OK(db_->Put(WriteOptions(), "barfoo", two));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "barfoo", two));
  ASSERT_OK(Flush());
  newvalue = Get("barfoo");
  ASSERT_EQ(newvalue, four);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  newvalue = Get("barfoo");
  ASSERT_EQ(newvalue, four);
}

TEST_F(DBTestCompactionFilter, CompactionFilterContextManual) {
  KeepFilterFactory* filter = new KeepFilterFactory(
      true /* check_context */, true /* check_context_cf_id */,
      true /* check_context_input_table_properties */);

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_filter_factory.reset(filter);
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 8;
  Reopen(options);
  const int kNumFiles = 3;
  int num_keys_per_file = 400;
  for (int j = 0; j < kNumFiles; j++) {
    // Write several keys.
    const std::string value(10, 'x');
    for (int i = 0; i < num_keys_per_file; i++) {
      char key[100];
      snprintf(key, sizeof(key), "B%08d%02d", i, j);
      ASSERT_OK(Put(key, value));
    }
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    // Make sure next file is much smaller so automatic compaction will not
    // be triggered.
    num_keys_per_file /= 2;
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Force a manual compaction
  cfilter_count = 0;
  filter->expect_manual_compaction_.store(true);
  filter->expect_full_compaction_.store(true);
  filter->expect_cf_id_.store(0);
  filter->expect_input_start_level_.store(0);
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(
      &filter->expect_input_table_properties_));
  ASSERT_TRUE(filter->expect_input_table_properties_.size() == kNumFiles);

  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(cfilter_count, 700);
  ASSERT_EQ(NumSortedRuns(0), 1);
  ASSERT_TRUE(filter->compaction_filter_created());

  // Verify total number of keys is correct after manual compaction.
  {
    int count = 0;
    int total = 0;
    Arena arena;
    InternalKeyComparator icmp(options.comparator);
    ReadOptions read_options;
    ScopedArenaPtr<InternalIterator> iter(dbfull()->NewInternalIterator(
        read_options, &arena, kMaxSequenceNumber));
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    while (iter->Valid()) {
      ParsedInternalKey ikey(Slice(), 0, kTypeValue);
      ASSERT_OK(ParseInternalKey(iter->key(), &ikey, true /* log_err_key */));
      total++;
      if (ikey.sequence != 0) {
        count++;
      }
      iter->Next();
    }
    ASSERT_EQ(total, 700);
    ASSERT_EQ(count, 0);
  }
}

TEST_F(DBTestCompactionFilter, CompactionFilterContextCfId) {
  KeepFilterFactory* filter = new KeepFilterFactory(false, true);
  filter->expect_cf_id_.store(1);

  Options options = CurrentOptions();
  options.compaction_filter_factory.reset(filter);
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 2;
  CreateAndReopenWithCF({"pikachu"}, options);

  int num_keys_per_file = 400;
  for (int j = 0; j < 3; j++) {
    // Write several keys.
    const std::string value(10, 'x');
    for (int i = 0; i < num_keys_per_file; i++) {
      char key[100];
      snprintf(key, sizeof(key), "B%08d%02d", i, j);
      ASSERT_OK(Put(1, key, value));
    }
    ASSERT_OK(Flush(1));
    // Make sure next file is much smaller so automatic compaction will not
    // be triggered.
    num_keys_per_file /= 2;
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_TRUE(filter->compaction_filter_created());
}

// Compaction filters applies to all records, regardless snapshots.
TEST_F(DBTestCompactionFilter, CompactionFilterIgnoreSnapshot) {
  std::string five = std::to_string(5);
  Options options = CurrentOptions();
  options.compaction_filter_factory = std::make_shared<DeleteISFilterFactory>();
  options.disable_auto_compactions = true;
  options.create_if_missing = true;
  DestroyAndReopen(options);

  // Put some data.
  const Snapshot* snapshot = nullptr;
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 10; ++i) {
      ASSERT_OK(Put(std::to_string(table * 100 + i), "val"));
    }
    ASSERT_OK(Flush());

    if (table == 0) {
      snapshot = db_->GetSnapshot();
    }
  }
  assert(snapshot != nullptr);

  cfilter_count = 0;
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // The filter should delete 40 records.
  ASSERT_EQ(40, cfilter_count);

  {
    // Scan the entire database as of the snapshot to ensure
    // that nothing is left
    ReadOptions read_options;
    read_options.snapshot = snapshot;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    int count = 0;
    while (iter->Valid()) {
      count++;
      iter->Next();
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 6);
    read_options.snapshot = nullptr;
    std::unique_ptr<Iterator> iter1(db_->NewIterator(read_options));
    ASSERT_OK(iter1->status());
    iter1->SeekToFirst();
    count = 0;
    while (iter1->Valid()) {
      count++;
      iter1->Next();
    }
    ASSERT_OK(iter1->status());
    // We have deleted 10 keys from 40 using the compaction filter
    //  Keys 6-9 before the snapshot and 100-105 after the snapshot
    ASSERT_EQ(count, 30);
  }

  // Release the snapshot and compact again -> now all records should be
  // removed.
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBTestCompactionFilter, SkipUntil) {
  Options options = CurrentOptions();
  options.compaction_filter_factory = std::make_shared<SkipEvenFilterFactory>();
  options.disable_auto_compactions = true;
  options.create_if_missing = true;
  DestroyAndReopen(options);

  // Write 100K keys, these are written to a few files in L0.
  for (int table = 0; table < 4; ++table) {
    // Key ranges in tables are [0, 38], [106, 149], [212, 260], [318, 371].
    for (int i = table * 6; i < 39 + table * 11; ++i) {
      char key[100];
      snprintf(key, sizeof(key), "%010d", table * 100 + i);
      ASSERT_OK(Put(key, std::to_string(table * 1000 + i)));
    }
    ASSERT_OK(Flush());
  }

  cfilter_skips = 0;
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Number of skips in tables: 2, 3, 3, 3.
  ASSERT_EQ(11, cfilter_skips);

  for (int table = 0; table < 4; ++table) {
    for (int i = table * 6; i < 39 + table * 11; ++i) {
      int k = table * 100 + i;
      char key[100];
      snprintf(key, sizeof(key), "%010d", table * 100 + i);
      auto expected = std::to_string(table * 1000 + i);
      std::string val;
      Status s = db_->Get(ReadOptions(), key, &val);
      if (k / 10 % 2 == 0) {
        ASSERT_TRUE(s.IsNotFound());
      } else {
        ASSERT_OK(s);
        ASSERT_EQ(expected, val);
      }
    }
  }
}

TEST_F(DBTestCompactionFilter, SkipUntilWithBloomFilter) {
  BlockBasedTableOptions table_options;
  table_options.whole_key_filtering = false;
  table_options.filter_policy.reset(NewBloomFilterPolicy(100, false));

  Options options = CurrentOptions();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewCappedPrefixTransform(9));
  options.compaction_filter_factory = std::make_shared<SkipEvenFilterFactory>();
  options.disable_auto_compactions = true;
  options.create_if_missing = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("0000000010", "v10"));
  ASSERT_OK(Put("0000000020", "v20"));  // skipped
  ASSERT_OK(Put("0000000050", "v50"));
  ASSERT_OK(Flush());

  cfilter_skips = 0;
  EXPECT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ(1, cfilter_skips);

  Status s;
  std::string val;

  s = db_->Get(ReadOptions(), "0000000010", &val);
  ASSERT_OK(s);
  EXPECT_EQ("v10", val);

  s = db_->Get(ReadOptions(), "0000000020", &val);
  EXPECT_TRUE(s.IsNotFound());

  s = db_->Get(ReadOptions(), "0000000050", &val);
  ASSERT_OK(s);
  EXPECT_EQ("v50", val);
}

class TestNotSupportedFilter : public CompactionFilter {
 public:
  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    return true;
  }

  const char* Name() const override { return "NotSupported"; }
  bool IgnoreSnapshots() const override { return false; }
};

TEST_F(DBTestCompactionFilter, IgnoreSnapshotsFalse) {
  Options options = CurrentOptions();
  options.compaction_filter = new TestNotSupportedFilter();
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "v10"));
  ASSERT_OK(Put("z", "v20"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("a", "v10"));
  ASSERT_OK(Put("z", "v20"));
  ASSERT_OK(Flush());

  // Comapction should fail because IgnoreSnapshots() = false
  EXPECT_TRUE(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr)
                  .IsNotSupported());

  delete options.compaction_filter;
}

class TestNotSupportedFilterFactory : public CompactionFilterFactory {
 public:
  explicit TestNotSupportedFilterFactory(TableFileCreationReason reason)
      : reason_(reason) {}

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    return reason_ == reason;
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /* context */) override {
    return std::unique_ptr<CompactionFilter>(new TestNotSupportedFilter());
  }

  const char* Name() const override { return "TestNotSupportedFilterFactory"; }

 private:
  const TableFileCreationReason reason_;
};

TEST_F(DBTestCompactionFilter, IgnoreSnapshotsFalseDuringFlush) {
  Options options = CurrentOptions();
  options.compaction_filter_factory =
      std::make_shared<TestNotSupportedFilterFactory>(
          TableFileCreationReason::kFlush);
  Reopen(options);

  ASSERT_OK(Put("a", "v10"));
  ASSERT_TRUE(Flush().IsNotSupported());
}

TEST_F(DBTestCompactionFilter, IgnoreSnapshotsFalseRecovery) {
  Options options = CurrentOptions();
  options.compaction_filter_factory =
      std::make_shared<TestNotSupportedFilterFactory>(
          TableFileCreationReason::kRecovery);
  Reopen(options);

  ASSERT_OK(Put("a", "v10"));
  ASSERT_TRUE(TryReopen(options).IsNotSupported());
}

TEST_F(DBTestCompactionFilter, DropKeyWithSingleDelete) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;

  Reopen(options);

  ASSERT_OK(Put("a", "v0"));
  ASSERT_OK(Put("b", "v0"));
  const Snapshot* snapshot = db_->GetSnapshot();

  ASSERT_OK(SingleDelete("b"));
  ASSERT_OK(Flush());

  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = options.num_levels - 1;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  }

  db_->ReleaseSnapshot(snapshot);
  Close();

  class DeleteFilterV2 : public CompactionFilter {
   public:
    Decision FilterV2(int /*level*/, const Slice& key, ValueType /*value_type*/,
                      const Slice& /*existing_value*/,
                      std::string* /*new_value*/,
                      std::string* /*skip_until*/) const override {
      if (key.starts_with("b")) {
        return Decision::kPurge;
      }
      return Decision::kRemove;
    }

    const char* Name() const override { return "DeleteFilterV2"; }
  } delete_filter_v2;

  options.compaction_filter = &delete_filter_v2;
  options.level0_file_num_compaction_trigger = 2;
  Reopen(options);

  ASSERT_OK(Put("b", "v1"));
  ASSERT_OK(Put("x", "v1"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("r", "v1"));
  ASSERT_OK(Put("z", "v1"));
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  Close();

  options.compaction_filter = nullptr;
  Reopen(options);
  ASSERT_OK(SingleDelete("b"));
  ASSERT_OK(Flush());
  {
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
