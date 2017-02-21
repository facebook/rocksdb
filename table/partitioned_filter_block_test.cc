//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/partitioned_filter_block.h"

#include "rocksdb/filter_policy.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

typedef PartitionIndexBuilder PartitionedFilterBlockBuilder;

std::map<uint64_t, Slice> slices;

class PartitionedFilterBlockTest : public testing::Test {
 public:
  BlockBasedTableOptions table_options_;
  InternalKeyComparator icomp = InternalKeyComparator(BytewiseComparator());

  PartitionedFilterBlockTest() {
    cache_ = NewLRUCache(1, 1, false);
    table_options_.block_cache = cache_;
    table_options_.filter_policy.reset(NewBloomFilterPolicy(10, false));
  }

  std::shared_ptr<Cache> cache_;
  ~PartitionedFilterBlockTest() {}

  const std::string keys[4] = {"afoo", "bar", "box", "hello"};
  const std::string missing_keys[2] = {"missing", "other"};

  int last_offset = 10;
  BlockHandle Write(Slice& slice) {
    BlockHandle bh(last_offset + 1, slice.size());
    slices[bh.offset()] = slice;
    last_offset += bh.size();
    return bh;
  }

  PartitionedFilterBlockBuilder* NewBuilder() {
    return new PartitionedFilterBlockBuilder(
        &icomp, nullptr, table_options_.index_per_partition,
        table_options_.index_block_restart_interval,
        table_options_.whole_key_filtering,
        table_options_.filter_policy->GetFilterBitsBuilder(), table_options_);
  }

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
    // BlockBasedTable::Open(ioptions, env_options, table_options, icomp,
    // std::unique_ptr<rocksdb::RandomAccessFileReader>(), 0L, table, false,
    // false, 0);
    std::unique_ptr<BlockBasedTable> table(
        new BlockBasedTable(new BlockBasedTable::Rep(
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
      const Slice ikey =
          Slice(*InternalKey(key, 0, ValueType::kTypeValue).rep());
      ASSERT_TRUE(reader->KeyMayMatch(key, kNotValid, !no_io, &ikey));
    }
    {
      // querying a key twice
      const Slice ikey =
          Slice(*InternalKey(keys[0], 0, ValueType::kTypeValue).rep());
      ASSERT_TRUE(reader->KeyMayMatch(keys[0], kNotValid, !no_io, &ikey));
    }
    // querying missing keys
    for (auto key : missing_keys) {
      const Slice ikey =
          Slice(*InternalKey(key, 0, ValueType::kTypeValue).rep());
      if (empty) {
        ASSERT_TRUE(reader->KeyMayMatch(key, kNotValid, !no_io, &ikey));
      } else {
        // assuming a good hash function
        ASSERT_FALSE(reader->KeyMayMatch(key, kNotValid, !no_io, &ikey));
      }
    }
  }

  void TestBlockPerKey() {
    table_options_.index_per_partition = 1;
    std::unique_ptr<PartitionedFilterBlockBuilder> builder(NewBuilder());
    int i = 0;
    builder->Add(keys[i]);
    CutABlock(builder.get(), keys[i], keys[i + 1]);
    i++;
    builder->Add(keys[i]);
    CutABlock(builder.get(), keys[i], keys[i + 1]);
    i++;
    builder->Add(keys[i]);
    builder->Add(keys[i]);
    CutABlock(builder.get(), keys[i], keys[i + 1]);
    i++;
    builder->Add(keys[i]);
    CutABlock(builder.get(), keys[i]);

    VerifyReader(builder.get());
  }

  void TestBlockPerTwoKeys() {
    std::unique_ptr<PartitionedFilterBlockBuilder> builder(NewBuilder());
    int i = 0;
    builder->Add(keys[i]);
    i++;
    builder->Add(keys[i]);
    CutABlock(builder.get(), keys[i], keys[i + 1]);
    i++;
    builder->Add(keys[i]);
    builder->Add(keys[i]);
    i++;
    builder->Add(keys[i]);
    CutABlock(builder.get(), keys[i]);

    VerifyReader(builder.get());
  }

  void TestBlockPerAllKeys() {
    std::unique_ptr<PartitionedFilterBlockBuilder> builder(NewBuilder());
    int i = 0;
    builder->Add(keys[i]);
    i++;
    builder->Add(keys[i]);
    i++;
    builder->Add(keys[i]);
    builder->Add(keys[i]);
    i++;
    builder->Add(keys[i]);
    CutABlock(builder.get(), keys[i]);

    VerifyReader(builder.get());
  }

  void CutABlock(PartitionedFilterBlockBuilder* builder,
                 const std::string& user_key) {
    // Assuming a block is cut, add an entry to the index
    std::string key =
        std::string(*InternalKey(user_key, 0, ValueType::kTypeValue).rep());
    BlockHandle dont_care_block_handle(1, 1);
    builder->AddIndexEntry(&key, nullptr, dont_care_block_handle);
  }

  void CutABlock(PartitionedFilterBlockBuilder* builder,
                 const std::string& user_key,
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
};

TEST_F(PartitionedFilterBlockTest, EmptyBuilder) {
  std::unique_ptr<PartitionedFilterBlockBuilder> builder(NewBuilder());
  const bool empty = true;
  VerifyReader(builder.get(), empty);
}

TEST_F(PartitionedFilterBlockTest, OneBlock) {
  int num_keys = sizeof(keys) / sizeof(*keys);
  for (int i = 1; i < num_keys + 1; i++) {
    table_options_.index_per_partition = i;
    TestBlockPerAllKeys();
  }
}

TEST_F(PartitionedFilterBlockTest, TwoBlocksPerKey) {
  int num_keys = sizeof(keys) / sizeof(*keys);
  for (int i = 1; i < num_keys + 1; i++) {
    table_options_.index_per_partition = i;
    TestBlockPerTwoKeys();
  }
}

TEST_F(PartitionedFilterBlockTest, OneBlockPerKey) {
  int num_keys = sizeof(keys) / sizeof(*keys);
  for (int i = 1; i < num_keys + 1; i++) {
    table_options_.index_per_partition = i;
    TestBlockPerKey();
  }
}

// A Mock of BlockBasedTable
// The only three engaged methods are GetFilter, ReadFilter, and Open. The rest
// are noop.
BlockBasedTable::CachableEntry<FilterBlockReader> BlockBasedTable::GetFilter(
    const BlockHandle& bh, const bool is_a_filter_partition, bool no_io) const {
  Slice slice = slices[bh.offset()];
  auto obj = new FullFilterBlockReader(
      nullptr, true, BlockContents(slice, false, kNoCompression),
      rep_->table_options.filter_policy->GetFilterBitsReader(slice), nullptr);
  return {obj, nullptr};
}

FilterBlockReader* BlockBasedTable::ReadFilter(
    const BlockHandle& bh, const bool is_a_filter_partition) const {
  assert(0);
  return nullptr;
  // return slices[bh.offset()].get();
}

Status BlockBasedTable::Open(const ImmutableCFOptions& ioptions,
                             const EnvOptions& env_options,
                             const BlockBasedTableOptions& table_options,
                             const InternalKeyComparator& internal_comparator,
                             unique_ptr<RandomAccessFileReader>&& file,
                             uint64_t file_size,
                             unique_ptr<TableReader>* table_reader,
                             const bool prefetch_index_and_filter_in_cache,
                             const bool skip_filters, const int level) {
  Rep* rep = new BlockBasedTable::Rep(ioptions, env_options, table_options,
                                      internal_comparator, skip_filters);
  *table_reader = unique_ptr<BlockBasedTable>(new BlockBasedTable(rep));
  return Status::OK();
}

void BlockBasedTable::GenerateCachePrefix(Cache* cc, WritableFile* file,
                                          char* buffer, size_t* size) {}
void BlockBasedTable::GenerateCachePrefix(Cache* cc, RandomAccessFile* file,
                                          char* buffer, size_t* size) {}
Slice BlockBasedTable::GetCacheKey(const char* cache_key_prefix,
                                   size_t cache_key_prefix_size,
                                   const BlockHandle& handle, char* cache_key) {
  return Slice();
}
BlockBasedTable::~BlockBasedTable() {}
InternalIterator* BlockBasedTable::NewIterator(const ReadOptions& read_options,
                                               Arena* arena,
                                               bool skip_filters) {
  return nullptr;
}
uint64_t BlockBasedTable::ApproximateOffsetOf(const Slice& key) { return 0; }
InternalIterator* BlockBasedTable::NewRangeTombstoneIterator(
    const ReadOptions& read_options) {
  return nullptr;
}
void BlockBasedTable::SetupForCompaction() {}
std::shared_ptr<const TableProperties> BlockBasedTable::GetTableProperties()
    const {
  return rep_->table_properties;
}
size_t BlockBasedTable::ApproximateMemoryUsage() const { return 0; }
Status BlockBasedTable::Get(const ReadOptions& read_options, const Slice& key,
                            GetContext* get_context, bool skip_filters) {
  return Status::OK();
}
Status BlockBasedTable::Prefetch(const Slice* const begin,
                                 const Slice* const end) {
  return Status::OK();
}
Status BlockBasedTable::DumpTable(WritableFile* out_file) {
  return Status::OK();
}
void BlockBasedTable::Close() {}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
