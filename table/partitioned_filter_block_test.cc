//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <map>

#include "rocksdb/filter_policy.h"

#include "table/index_builder.h"
#include "table/partitioned_filter_block.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

std::map<uint64_t, Slice> slices;

class MockedBlockBasedTable : public BlockBasedTable {
 public:
  explicit MockedBlockBasedTable(Rep* rep) : BlockBasedTable(rep) {}

  virtual CachableEntry<FilterBlockReader> GetFilter(
      const BlockHandle& filter_blk_handle, const bool is_a_filter_partition,
      bool no_io) const override {
    Slice slice = slices[filter_blk_handle.offset()];
    auto obj = new FullFilterBlockReader(
        nullptr, true, BlockContents(slice, false, kNoCompression),
        rep_->table_options.filter_policy->GetFilterBitsReader(slice), nullptr);
    return {obj, nullptr};
  }
};

class PartitionedFilterBlockTest : public testing::Test {
 public:
  BlockBasedTableOptions table_options_;
  InternalKeyComparator icomp = InternalKeyComparator(BytewiseComparator());

  PartitionedFilterBlockTest() {
    table_options_.filter_policy.reset(NewBloomFilterPolicy(10, false));
    table_options_.no_block_cache = true;  // Otherwise BlockBasedTable::Close
                                           // will access variable that are not
                                           // initialized in our mocked version
  }

  std::shared_ptr<Cache> cache_;
  ~PartitionedFilterBlockTest() {}

  const std::string keys[4] = {"afoo", "bar", "box", "hello"};
  const std::string missing_keys[2] = {"missing", "other"};

  uint64_t MaxIndexSize() {
    int num_keys = sizeof(keys) / sizeof(*keys);
    uint64_t max_key_size = 0;
    for (int i = 1; i < num_keys; i++) {
      max_key_size = std::max(max_key_size, static_cast<uint64_t>(keys[i].size()));
    }
    uint64_t max_index_size = num_keys * (max_key_size + 8 /*handle*/);
    return max_index_size;
  }

  int last_offset = 10;
  BlockHandle Write(const Slice& slice) {
    BlockHandle bh(last_offset + 1, slice.size());
    slices[bh.offset()] = slice;
    last_offset += bh.size();
    return bh;
  }

  PartitionedIndexBuilder* NewIndexBuilder() {
    return PartitionedIndexBuilder::CreateIndexBuilder(&icomp, table_options_);
  }

  PartitionedFilterBlockBuilder* NewBuilder(
      PartitionedIndexBuilder* const p_index_builder) {
    return new PartitionedFilterBlockBuilder(
        nullptr, table_options_.whole_key_filtering,
        table_options_.filter_policy->GetFilterBitsBuilder(),
        table_options_.index_block_restart_interval, p_index_builder);
  }

  std::unique_ptr<MockedBlockBasedTable> table;

  PartitionedFilterBlockReader* NewReader(
      PartitionedFilterBlockBuilder* builder) {
    BlockHandle bh;
    Status status;
    Slice slice;
    do {
      slice = builder->Finish(bh, &status);
      bh = Write(slice);
    } while (status.IsIncomplete());
    const Options options;
    const ImmutableCFOptions ioptions(options);
    const EnvOptions env_options;
    table.reset(new MockedBlockBasedTable(new BlockBasedTable::Rep(
        ioptions, env_options, table_options_, icomp, false)));
    auto reader = new PartitionedFilterBlockReader(
        nullptr, true, BlockContents(slice, false, kNoCompression), nullptr,
        nullptr, *icomp.user_comparator(), table.get());
    return reader;
  }

  void VerifyReader(PartitionedFilterBlockBuilder* builder,
                    bool empty = false) {
    std::unique_ptr<PartitionedFilterBlockReader> reader(NewReader(builder));
    // Querying added keys
    const bool no_io = true;
    for (auto key : keys) {
      auto ikey = InternalKey(key, 0, ValueType::kTypeValue);
      const Slice ikey_slice = Slice(*ikey.rep());
      ASSERT_TRUE(reader->KeyMayMatch(key, kNotValid, !no_io, &ikey_slice));
    }
    {
      // querying a key twice
      auto ikey = InternalKey(keys[0], 0, ValueType::kTypeValue);
      const Slice ikey_slice = Slice(*ikey.rep());
      ASSERT_TRUE(reader->KeyMayMatch(keys[0], kNotValid, !no_io, &ikey_slice));
    }
    // querying missing keys
    for (auto key : missing_keys) {
      auto ikey = InternalKey(key, 0, ValueType::kTypeValue);
      const Slice ikey_slice = Slice(*ikey.rep());
      if (empty) {
        ASSERT_TRUE(reader->KeyMayMatch(key, kNotValid, !no_io, &ikey_slice));
      } else {
        // assuming a good hash function
        ASSERT_FALSE(reader->KeyMayMatch(key, kNotValid, !no_io, &ikey_slice));
      }
    }
  }

  int TestBlockPerKey() {
    std::unique_ptr<PartitionedIndexBuilder> pib(NewIndexBuilder());
    std::unique_ptr<PartitionedFilterBlockBuilder> builder(
        NewBuilder(pib.get()));
    int i = 0;
    builder->Add(keys[i]);
    CutABlock(pib.get(), keys[i], keys[i + 1]);
    i++;
    builder->Add(keys[i]);
    CutABlock(pib.get(), keys[i], keys[i + 1]);
    i++;
    builder->Add(keys[i]);
    builder->Add(keys[i]);
    CutABlock(pib.get(), keys[i], keys[i + 1]);
    i++;
    builder->Add(keys[i]);
    CutABlock(pib.get(), keys[i]);

    VerifyReader(builder.get());
    return CountNumOfIndexPartitions(pib.get());
  }

  void TestBlockPerTwoKeys() {
    std::unique_ptr<PartitionedIndexBuilder> pib(NewIndexBuilder());
    std::unique_ptr<PartitionedFilterBlockBuilder> builder(
        NewBuilder(pib.get()));
    int i = 0;
    builder->Add(keys[i]);
    i++;
    builder->Add(keys[i]);
    CutABlock(pib.get(), keys[i], keys[i + 1]);
    i++;
    builder->Add(keys[i]);
    builder->Add(keys[i]);
    i++;
    builder->Add(keys[i]);
    CutABlock(pib.get(), keys[i]);

    VerifyReader(builder.get());
  }

  void TestBlockPerAllKeys() {
    std::unique_ptr<PartitionedIndexBuilder> pib(NewIndexBuilder());
    std::unique_ptr<PartitionedFilterBlockBuilder> builder(
        NewBuilder(pib.get()));
    int i = 0;
    builder->Add(keys[i]);
    i++;
    builder->Add(keys[i]);
    i++;
    builder->Add(keys[i]);
    builder->Add(keys[i]);
    i++;
    builder->Add(keys[i]);
    CutABlock(pib.get(), keys[i]);

    VerifyReader(builder.get());
  }

  void CutABlock(PartitionedIndexBuilder* builder,
                 const std::string& user_key) {
    // Assuming a block is cut, add an entry to the index
    std::string key =
        std::string(*InternalKey(user_key, 0, ValueType::kTypeValue).rep());
    BlockHandle dont_care_block_handle(1, 1);
    builder->AddIndexEntry(&key, nullptr, dont_care_block_handle);
  }

  void CutABlock(PartitionedIndexBuilder* builder, const std::string& user_key,
                 const std::string& next_user_key) {
    // Assuming a block is cut, add an entry to the index
    std::string key =
        std::string(*InternalKey(user_key, 0, ValueType::kTypeValue).rep());
    std::string next_key = std::string(
        *InternalKey(next_user_key, 0, ValueType::kTypeValue).rep());
    BlockHandle dont_care_block_handle(1, 1);
    Slice slice = Slice(next_key.data(), next_key.size());
    builder->AddIndexEntry(&key, &slice, dont_care_block_handle);
  }

  int CountNumOfIndexPartitions(PartitionedIndexBuilder* builder) {
    IndexBuilder::IndexBlocks dont_care_ib;
    BlockHandle dont_care_bh(10, 10);
    Status s;
    int cnt = 0;
    do {
      s = builder->Finish(&dont_care_ib, dont_care_bh);
      cnt++;
    } while (s.IsIncomplete());
    return cnt - 1;  // 1 is 2nd level index
  }
};

TEST_F(PartitionedFilterBlockTest, EmptyBuilder) {
  std::unique_ptr<PartitionedIndexBuilder> pib(NewIndexBuilder());
  std::unique_ptr<PartitionedFilterBlockBuilder> builder(NewBuilder(pib.get()));
  const bool empty = true;
  VerifyReader(builder.get(), empty);
}

TEST_F(PartitionedFilterBlockTest, OneBlock) {
  uint64_t max_index_size = MaxIndexSize();
  for (uint64_t i = 1; i < max_index_size + 1; i++) {
    table_options_.metadata_block_size = i;
    TestBlockPerAllKeys();
  }
}

TEST_F(PartitionedFilterBlockTest, TwoBlocksPerKey) {
  uint64_t max_index_size = MaxIndexSize();
  for (uint64_t i = 1; i < max_index_size + 1; i++) {
    table_options_.metadata_block_size = i;
    TestBlockPerTwoKeys();
  }
}

TEST_F(PartitionedFilterBlockTest, OneBlockPerKey) {
  uint64_t max_index_size = MaxIndexSize();
  for (uint64_t i = 1; i < max_index_size + 1; i++) {
    table_options_.metadata_block_size = i;
    TestBlockPerKey();
  }
}

TEST_F(PartitionedFilterBlockTest, PartitionCount) {
  int num_keys = sizeof(keys) / sizeof(*keys);
  table_options_.metadata_block_size = MaxIndexSize();
  int partitions = TestBlockPerKey();
  ASSERT_EQ(partitions, 1);
  // A low number ensures cutting a block after each key
  table_options_.metadata_block_size = 1;
  partitions = TestBlockPerKey();
  ASSERT_EQ(partitions, num_keys - 1 /* last two keys make one flush */);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
