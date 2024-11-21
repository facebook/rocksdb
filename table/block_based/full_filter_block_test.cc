//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/full_filter_block.h"

#include <set>

#include "rocksdb/filter_policy.h"
#include "rocksdb/status.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/mock_block_based_table.h"
#include "table/format.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class TestFilterBitsBuilder : public FilterBitsBuilder {
 public:
  explicit TestFilterBitsBuilder() = default;

  // Add Key to filter
  void AddKey(const Slice& key) override {
    hash_entries_.push_back(Hash(key.data(), key.size(), 1));
  }
  void AddKeyAndAlt(const Slice& key, const Slice& alt) override {
    AddKey(key);
    AddKey(alt);
  }

  using FilterBitsBuilder::Finish;

  // Generate the filter using the keys that are added
  Slice Finish(std::unique_ptr<const char[]>* buf) override {
    uint32_t len = static_cast<uint32_t>(hash_entries_.size()) * 4;
    char* data = new char[len];
    for (size_t i = 0; i < hash_entries_.size(); i++) {
      EncodeFixed32(data + i * 4, hash_entries_[i]);
    }
    const char* const_data = data;
    buf->reset(const_data);
    return Slice(data, len);
  }

  size_t EstimateEntriesAdded() override { return hash_entries_.size(); }

  size_t ApproximateNumEntries(size_t bytes) override { return bytes / 4; }

 private:
  std::vector<uint32_t> hash_entries_;
};

class MockBlockBasedTable : public BlockBasedTable {
 public:
  explicit MockBlockBasedTable(Rep* rep)
      : BlockBasedTable(rep, nullptr /* block_cache_tracer */) {}
};

class TestFilterBitsReader : public FilterBitsReader {
 public:
  explicit TestFilterBitsReader(const Slice& contents)
      : data_(contents.data()), len_(static_cast<uint32_t>(contents.size())) {}

  // Silence compiler warning about overloaded virtual
  using FilterBitsReader::MayMatch;
  bool MayMatch(const Slice& entry) override {
    uint32_t h = Hash(entry.data(), entry.size(), 1);
    for (size_t i = 0; i + 4 <= len_; i += 4) {
      if (h == DecodeFixed32(data_ + i)) {
        return true;
      }
    }
    return false;
  }

 private:
  const char* data_;
  uint32_t len_;
};

class TestHashFilter : public FilterPolicy {
 public:
  const char* Name() const override { return "TestHashFilter"; }
  const char* CompatibilityName() const override { return Name(); }

  FilterBitsBuilder* GetBuilderWithContext(
      const FilterBuildingContext&) const override {
    return new TestFilterBitsBuilder();
  }

  FilterBitsReader* GetFilterBitsReader(const Slice& contents) const override {
    return new TestFilterBitsReader(contents);
  }
};

class PluginFullFilterBlockTest : public mock::MockBlockBasedTableTester,
                                  public testing::Test {
 public:
  PluginFullFilterBlockTest()
      : mock::MockBlockBasedTableTester(new TestHashFilter) {}
};

TEST_F(PluginFullFilterBlockTest, PluginEmptyBuilder) {
  FullFilterBlockBuilder builder(nullptr, true, GetBuilder());
  Slice slice = builder.TEST_Finish();
  ASSERT_EQ("", EscapeString(slice));

  CachableEntry<ParsedFullFilterBlock> block(
      new ParsedFullFilterBlock(table_options_.filter_policy.get(),
                                BlockContents(slice)),
      nullptr /* cache */, nullptr /* cache_handle */, true /* own_value */);

  FullFilterBlockReader reader(table_.get(), std::move(block));
  // Remain same symantic with blockbased filter
  ASSERT_TRUE(reader.KeyMayMatch("foo",
                                 /*const_ikey_ptr=*/nullptr,
                                 /*get_context=*/nullptr,
                                 /*lookup_context=*/nullptr, ReadOptions()));
}

TEST_F(PluginFullFilterBlockTest, PluginSingleChunk) {
  FullFilterBlockBuilder builder(nullptr, true, GetBuilder());
  builder.Add("foo");
  builder.Add("bar");
  builder.Add("box");
  builder.Add("box");
  builder.Add("hello");
  Slice slice = builder.TEST_Finish();

  CachableEntry<ParsedFullFilterBlock> block(
      new ParsedFullFilterBlock(table_options_.filter_policy.get(),
                                BlockContents(slice)),
      nullptr /* cache */, nullptr /* cache_handle */, true /* own_value */);

  FullFilterBlockReader reader(table_.get(), std::move(block));
  ASSERT_TRUE(reader.KeyMayMatch("foo",
                                 /*const_ikey_ptr=*/nullptr,
                                 /*get_context=*/nullptr,
                                 /*lookup_context=*/nullptr, ReadOptions()));
  ASSERT_TRUE(reader.KeyMayMatch("bar",
                                 /*const_ikey_ptr=*/nullptr,
                                 /*get_context=*/nullptr,
                                 /*lookup_context=*/nullptr, ReadOptions()));
  ASSERT_TRUE(reader.KeyMayMatch("box",
                                 /*const_ikey_ptr=*/nullptr,
                                 /*get_context=*/nullptr,
                                 /*lookup_context=*/nullptr, ReadOptions()));
  ASSERT_TRUE(reader.KeyMayMatch("hello",
                                 /*const_ikey_ptr=*/nullptr,
                                 /*get_context=*/nullptr,
                                 /*lookup_context=*/nullptr, ReadOptions()));
  ASSERT_TRUE(reader.KeyMayMatch("foo",
                                 /*const_ikey_ptr=*/nullptr,
                                 /*get_context=*/nullptr,
                                 /*lookup_context=*/nullptr, ReadOptions()));
  ASSERT_TRUE(!reader.KeyMayMatch("missing",
                                  /*const_ikey_ptr=*/nullptr,
                                  /*get_context=*/nullptr,
                                  /*lookup_context=*/nullptr, ReadOptions()));
  ASSERT_TRUE(!reader.KeyMayMatch("other",
                                  /*const_ikey_ptr=*/nullptr,
                                  /*get_context=*/nullptr,
                                  /*lookup_context=*/nullptr, ReadOptions()));
}

class FullFilterBlockTest : public mock::MockBlockBasedTableTester,
                            public testing::Test {
 public:
  FullFilterBlockTest()
      : mock::MockBlockBasedTableTester(NewBloomFilterPolicy(10, false)) {}
};

TEST_F(FullFilterBlockTest, EmptyBuilder) {
  FullFilterBlockBuilder builder(nullptr, true, GetBuilder());
  Slice slice = builder.TEST_Finish();
  ASSERT_EQ("", EscapeString(slice));

  CachableEntry<ParsedFullFilterBlock> block(
      new ParsedFullFilterBlock(table_options_.filter_policy.get(),
                                BlockContents(slice)),
      nullptr /* cache */, nullptr /* cache_handle */, true /* own_value */);

  FullFilterBlockReader reader(table_.get(), std::move(block));
  // Remain same symantic with blockbased filter
  ASSERT_TRUE(reader.KeyMayMatch("foo",
                                 /*const_ikey_ptr=*/nullptr,
                                 /*get_context=*/nullptr,
                                 /*lookup_context=*/nullptr, ReadOptions()));
}

class CountUniqueFilterBitsBuilderWrapper : public FilterBitsBuilder {
  std::unique_ptr<FilterBitsBuilder> b_;
  std::set<std::string> uniq_;

 public:
  explicit CountUniqueFilterBitsBuilderWrapper(FilterBitsBuilder* b) : b_(b) {}

  ~CountUniqueFilterBitsBuilderWrapper() override = default;

  void AddKey(const Slice& key) override {
    b_->AddKey(key);
    uniq_.insert(key.ToString());
  }
  void AddKeyAndAlt(const Slice& key, const Slice& alt) override {
    b_->AddKeyAndAlt(key, alt);
    uniq_.insert(key.ToString());
    uniq_.insert(alt.ToString());
  }

  using FilterBitsBuilder::Finish;

  Slice Finish(std::unique_ptr<const char[]>* buf) override {
    Slice rv = b_->Finish(buf);
    Status s_dont_care = b_->MaybePostVerify(rv);
    s_dont_care.PermitUncheckedError();
    uniq_.clear();
    return rv;
  }

  size_t EstimateEntriesAdded() override { return b_->EstimateEntriesAdded(); }

  size_t ApproximateNumEntries(size_t bytes) override {
    return b_->ApproximateNumEntries(bytes);
  }

  size_t CountUnique() { return uniq_.size(); }
};

TEST_F(FullFilterBlockTest, DuplicateEntries) {
  {  // empty prefixes
    std::unique_ptr<const SliceTransform> prefix_extractor(
        NewFixedPrefixTransform(0));
    auto bits_builder = new CountUniqueFilterBitsBuilderWrapper(GetBuilder());
    const bool WHOLE_KEY = true;
    FullFilterBlockBuilder builder(prefix_extractor.get(), WHOLE_KEY,
                                   bits_builder);
    ASSERT_EQ(0, bits_builder->CountUnique());
    // adds key and empty prefix; both abstractions count them
    builder.Add("key1");
    ASSERT_EQ(2, bits_builder->CountUnique());
    // Add different key (unique) and also empty prefix (not unique).
    // From here in this test, it's immaterial whether the block builder
    // can count unique keys.
    builder.Add("key2");
    ASSERT_EQ(3, bits_builder->CountUnique());
    // Empty key -> nothing unique
    builder.Add("");
    ASSERT_EQ(3, bits_builder->CountUnique());
  }

  // mix of empty and non-empty
  std::unique_ptr<const SliceTransform> prefix_extractor(
      NewFixedPrefixTransform(7));
  auto bits_builder = new CountUniqueFilterBitsBuilderWrapper(GetBuilder());
  const bool WHOLE_KEY = true;
  FullFilterBlockBuilder builder(prefix_extractor.get(), WHOLE_KEY,
                                 bits_builder);
  builder.Add("");  // test with empty key too
  builder.Add("prefix1key1");
  builder.Add("prefix1key1");
  builder.Add("prefix1key2");
  builder.Add("prefix1key3");
  builder.Add("prefix2key4");
  // 1 empty, 2 non-empty prefixes, and 4 non-empty keys
  ASSERT_EQ(1 + 2 + 4, bits_builder->CountUnique());
}

TEST_F(FullFilterBlockTest, SingleChunk) {
  FullFilterBlockBuilder builder(nullptr, true, GetBuilder());
  ASSERT_TRUE(builder.IsEmpty());
  builder.Add("foo");
  ASSERT_FALSE(builder.IsEmpty());
  builder.Add("bar");
  builder.Add("box");
  builder.Add("box");
  builder.Add("hello");
  // "box" only counts once
  ASSERT_EQ(4, builder.EstimateEntriesAdded());
  ASSERT_FALSE(builder.IsEmpty());
  Slice slice;
  Status s = builder.Finish(BlockHandle(), &slice);
  ASSERT_OK(s);

  CachableEntry<ParsedFullFilterBlock> block(
      new ParsedFullFilterBlock(table_options_.filter_policy.get(),
                                BlockContents(slice)),
      nullptr /* cache */, nullptr /* cache_handle */, true /* own_value */);

  FullFilterBlockReader reader(table_.get(), std::move(block));
  ASSERT_TRUE(reader.KeyMayMatch("foo",
                                 /*const_ikey_ptr=*/nullptr,
                                 /*get_context=*/nullptr,
                                 /*lookup_context=*/nullptr, ReadOptions()));
  ASSERT_TRUE(reader.KeyMayMatch("bar",
                                 /*const_ikey_ptr=*/nullptr,
                                 /*get_context=*/nullptr,
                                 /*lookup_context=*/nullptr, ReadOptions()));
  ASSERT_TRUE(reader.KeyMayMatch("box",
                                 /*const_ikey_ptr=*/nullptr,
                                 /*get_context=*/nullptr,
                                 /*lookup_context=*/nullptr, ReadOptions()));
  ASSERT_TRUE(reader.KeyMayMatch("hello",
                                 /*const_ikey_ptr=*/nullptr,
                                 /*get_context=*/nullptr,
                                 /*lookup_context=*/nullptr, ReadOptions()));
  ASSERT_TRUE(reader.KeyMayMatch("foo",
                                 /*const_ikey_ptr=*/nullptr,
                                 /*get_context=*/nullptr,
                                 /*lookup_context=*/nullptr, ReadOptions()));
  ASSERT_TRUE(!reader.KeyMayMatch("missing",
                                  /*const_ikey_ptr=*/nullptr,
                                  /*get_context=*/nullptr,
                                  /*lookup_context=*/nullptr, ReadOptions()));
  ASSERT_TRUE(!reader.KeyMayMatch("other",
                                  /*const_ikey_ptr=*/nullptr,
                                  /*get_context=*/nullptr,
                                  /*lookup_context=*/nullptr, ReadOptions()));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
