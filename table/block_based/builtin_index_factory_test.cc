// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/builtin_index_factory.h"

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/index_factory.h"
#include "rocksdb/slice.h"
#include "rocksdb/user_defined_index.h"
#include "table/block_based/index_builder.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

// ============================================================================
// Backward compatibility aliases test
// ============================================================================

TEST(BackwardCompatTest, UserDefinedIndexAliasesCompile) {
  // Verify backward-compatible type aliases in user_defined_index.h
  static_assert(std::is_same_v<UserDefinedIndexBuilder, IndexFactoryBuilder>);
  static_assert(std::is_same_v<UserDefinedIndexIterator, IndexFactoryIterator>);
  static_assert(std::is_same_v<UserDefinedIndexReader, IndexFactoryReader>);
  static_assert(std::is_same_v<UserDefinedIndexFactory, IndexFactory>);
  static_assert(std::is_same_v<UserDefinedIndexOption, IndexFactoryOptions>);
  ASSERT_STREQ(kUserDefinedIndexPrefix, kIndexFactoryMetaPrefix);
}

// Helper to build IndexFactoryOptions with BytewiseComparator.
static IndexFactoryOptions MakeOptions() {
  IndexFactoryOptions opts;
  opts.comparator = BytewiseComparator();
  return opts;
}

// The internal IndexBuilder uses delta encoding for block handles and expects
// consecutive blocks laid out as: offset_i = offset_{i-1} + size_{i-1} +
// kBlockTrailerSize (5 bytes: 1-byte compression type + 32-bit checksum).
static constexpr uint64_t kBlockTrailerSize = 5;
static constexpr uint64_t kBlockSize = 100;

// Helper to add a few index entries to a builder. Simulates block boundaries
// for keys "aaa", "bbb", "ccc" at consecutive offsets.
static void AddSampleEntries(IndexFactoryBuilder* builder) {
  std::string scratch;
  IndexFactoryBuilder::IndexEntryContext ctx;
  ctx.last_key_tag = 0;
  ctx.first_key_tag = 0;

  Slice key_a("aaa");
  Slice key_b("bbb");
  Slice key_c("ccc");
  IndexFactoryBuilder::BlockHandle bh{0, 0};

  // Block 0: [0, 100)
  bh = {0, kBlockSize};
  builder->AddIndexEntry(key_a, &key_b, bh, &scratch, ctx);

  // Block 1: starts at 0 + 100 + 5 = 105
  bh = {kBlockSize + kBlockTrailerSize, kBlockSize};
  builder->AddIndexEntry(key_b, &key_c, bh, &scratch, ctx);

  // Block 2 (last): starts at 105 + 100 + 5 = 210
  bh = {2 * (kBlockSize + kBlockTrailerSize), kBlockSize};
  builder->AddIndexEntry(key_c, nullptr, bh, &scratch, ctx);
}

// ============================================================================
// BinarySearchIndexFactory tests
// ============================================================================

class BinarySearchIndexFactoryTest : public ::testing::Test {};

TEST_F(BinarySearchIndexFactoryTest, Name) {
  BinarySearchIndexFactory factory(/*with_first_key=*/false);
  ASSERT_STREQ(factory.Name(), "rocksdb.builtin.BinarySearchIndex");
}

TEST_F(BinarySearchIndexFactoryTest, NameWithFirstKey) {
  BinarySearchIndexFactory factory(/*with_first_key=*/true);
  ASSERT_STREQ(factory.Name(), "rocksdb.builtin.BinarySearchWithFirstKeyIndex");
}

TEST_F(BinarySearchIndexFactoryTest, KClassName) {
  ASSERT_STREQ(BinarySearchIndexFactory::kClassName(),
               "rocksdb.builtin.BinarySearchIndex");
}

TEST_F(BinarySearchIndexFactoryTest, KClassNameWithFirstKey) {
  ASSERT_STREQ(BinarySearchIndexFactory::kClassNameWithFirstKey(),
               "rocksdb.builtin.BinarySearchWithFirstKeyIndex");
}

TEST_F(BinarySearchIndexFactoryTest, NewBuilderSucceeds) {
  BinarySearchIndexFactory factory(/*with_first_key=*/false);
  auto opts = MakeOptions();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory.NewBuilder(opts, builder));
  ASSERT_NE(builder, nullptr);
}

TEST_F(BinarySearchIndexFactoryTest, NewBuilderRequiresComparator) {
  BinarySearchIndexFactory factory(/*with_first_key=*/false);
  IndexFactoryOptions opts;  // comparator is nullptr
  std::unique_ptr<IndexFactoryBuilder> builder;
  Status s = factory.NewBuilder(opts, builder);
  ASSERT_TRUE(s.IsInvalidArgument());
}

TEST_F(BinarySearchIndexFactoryTest, BuilderAddAndFinish) {
  BinarySearchIndexFactory factory(/*with_first_key=*/false);
  auto opts = MakeOptions();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory.NewBuilder(opts, builder));

  AddSampleEntries(builder.get());

  ASSERT_GT(builder->EstimatedSize(), static_cast<uint64_t>(0));

  Slice contents;
  ASSERT_OK(builder->Finish(&contents));
  ASSERT_GT(contents.size(), static_cast<size_t>(0));
}

TEST_F(BinarySearchIndexFactoryTest, NewBuilderWithFirstKeySucceeds) {
  BinarySearchIndexFactory factory(/*with_first_key=*/true);
  auto opts = MakeOptions();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory.NewBuilder(opts, builder));
  ASSERT_NE(builder, nullptr);
}

TEST_F(BinarySearchIndexFactoryTest, NewReaderReturnsNotSupported) {
  BinarySearchIndexFactory factory(/*with_first_key=*/false);
  auto opts = MakeOptions();
  Slice dummy_contents("dummy");
  std::unique_ptr<IndexFactoryReader> reader;
  Status s = factory.NewReader(opts, dummy_contents, reader);
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_EQ(reader, nullptr);
}

// ============================================================================
// HashIndexFactory tests
// ============================================================================

class HashIndexFactoryTest : public ::testing::Test {};

TEST_F(HashIndexFactoryTest, Name) {
  HashIndexFactory factory;
  ASSERT_STREQ(factory.Name(), "rocksdb.builtin.HashIndex");
}

TEST_F(HashIndexFactoryTest, KClassName) {
  ASSERT_STREQ(HashIndexFactory::kClassName(), "rocksdb.builtin.HashIndex");
}

TEST_F(HashIndexFactoryTest, NewBuilderSucceeds) {
  HashIndexFactory factory;
  auto opts = MakeOptions();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory.NewBuilder(opts, builder));
  ASSERT_NE(builder, nullptr);
}

TEST_F(HashIndexFactoryTest, NewBuilderRequiresComparator) {
  HashIndexFactory factory;
  IndexFactoryOptions opts;  // comparator is nullptr
  std::unique_ptr<IndexFactoryBuilder> builder;
  Status s = factory.NewBuilder(opts, builder);
  ASSERT_TRUE(s.IsInvalidArgument());
}

TEST_F(HashIndexFactoryTest, BuilderAddAndFinish) {
  HashIndexFactory factory;
  auto opts = MakeOptions();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory.NewBuilder(opts, builder));

  AddSampleEntries(builder.get());

  // HashIndexBuilder::CurrentIndexSizeEstimate() always returns 0 by design.
  // The hash builder tracks size differently from the binary search builder.

  Slice contents;
  ASSERT_OK(builder->Finish(&contents));
  ASSERT_GT(contents.size(), static_cast<size_t>(0));
}

TEST_F(HashIndexFactoryTest, NewReaderReturnsNotSupported) {
  HashIndexFactory factory;
  auto opts = MakeOptions();
  Slice dummy_contents("dummy");
  std::unique_ptr<IndexFactoryReader> reader;
  Status s = factory.NewReader(opts, dummy_contents, reader);
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_EQ(reader, nullptr);
}

// ============================================================================
// PartitionedIndexFactory tests
// ============================================================================

class PartitionedIndexFactoryTest : public ::testing::Test {};

TEST_F(PartitionedIndexFactoryTest, Name) {
  PartitionedIndexFactory factory;
  ASSERT_STREQ(factory.Name(), "rocksdb.builtin.PartitionedIndex");
}

TEST_F(PartitionedIndexFactoryTest, KClassName) {
  ASSERT_STREQ(PartitionedIndexFactory::kClassName(),
               "rocksdb.builtin.PartitionedIndex");
}

TEST_F(PartitionedIndexFactoryTest, NewBuilderSucceeds) {
  PartitionedIndexFactory factory;
  auto opts = MakeOptions();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory.NewBuilder(opts, builder));
  ASSERT_NE(builder, nullptr);
}

TEST_F(PartitionedIndexFactoryTest, NewBuilderRequiresComparator) {
  PartitionedIndexFactory factory;
  IndexFactoryOptions opts;  // comparator is nullptr
  std::unique_ptr<IndexFactoryBuilder> builder;
  Status s = factory.NewBuilder(opts, builder);
  ASSERT_TRUE(s.IsInvalidArgument());
}

TEST_F(PartitionedIndexFactoryTest, BuilderAddEntries) {
  PartitionedIndexFactory factory;
  auto opts = MakeOptions();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory.NewBuilder(opts, builder));

  // AddIndexEntry should succeed without crashing.
  AddSampleEntries(builder.get());

  // EstimatedSize should be non-zero after adding entries.
  ASSERT_GT(builder->EstimatedSize(), static_cast<uint64_t>(0));

  // Note: PartitionedIndexBuilder::Finish() requires a multi-step protocol
  // with partition block handles provided by the table builder. Testing the
  // full Finish flow requires integration with BlockBasedTableBuilder and is
  // covered by higher-level tests (e.g., table_test).
}

TEST_F(PartitionedIndexFactoryTest, NewReaderReturnsNotSupported) {
  PartitionedIndexFactory factory;
  auto opts = MakeOptions();
  Slice dummy_contents("dummy");
  std::unique_ptr<IndexFactoryReader> reader;
  Status s = factory.NewReader(opts, dummy_contents, reader);
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_EQ(reader, nullptr);
}

// ============================================================================
// InternalKeyReconstruction test: verify that BuiltinIndexFactoryBuilder's
// AddIndexEntry (which reconstructs internal keys from user keys + tags)
// produces the same output as calling the raw ShortenedIndexBuilder directly
// with pre-built internal keys.
// ============================================================================

TEST_F(BinarySearchIndexFactoryTest, InternalKeyReconstruction) {
  // Create factory with full config so we exercise the has_config_ path.
  BuiltinIndexFactoryConfig config;
  InternalKeyComparator icmp(BytewiseComparator());
  config.internal_comparator = &icmp;
  config.use_delta_encoding_for_index_values = true;
  BlockBasedTableOptions table_opts;
  config.table_options = &table_opts;

  BinarySearchIndexFactory factory(/*with_first_key=*/false, config);
  IndexFactoryOptions opts;
  opts.comparator = BytewiseComparator();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory.NewBuilder(opts, builder));

  // Add entries with known user keys and tags via the public interface.
  IndexFactoryBuilder::BlockHandle h1{0, kBlockSize};
  IndexFactoryBuilder::BlockHandle h2{kBlockSize + kBlockTrailerSize,
                                      kBlockSize};
  IndexFactoryBuilder::IndexEntryContext ctx1;
  ctx1.last_key_tag = PackSequenceAndType(100, kTypeValue);
  ctx1.first_key_tag = PackSequenceAndType(50, kTypeValue);
  IndexFactoryBuilder::IndexEntryContext ctx2;
  ctx2.last_key_tag = PackSequenceAndType(50, kTypeValue);
  ctx2.first_key_tag = 0;

  std::string scratch;
  Slice next1("bbb");
  builder->AddIndexEntry(Slice("aaa"), &next1, h1, &scratch, ctx1);
  builder->AddIndexEntry(Slice("bbb"), nullptr, h2, &scratch, ctx2);

  // Verify Finish produces valid output.
  Slice contents;
  ASSERT_OK(builder->Finish(&contents));
  ASSERT_GT(contents.size(), static_cast<size_t>(0));

  // Also create a raw ShortenedIndexBuilder with the same parameters and
  // pass pre-built internal keys. The outputs should be identical.
  std::unique_ptr<IndexBuilder> raw_builder(IndexBuilder::CreateIndexBuilder(
      BlockBasedTableOptions::kBinarySearch, &icmp,
      /*int_key_slice_transform=*/nullptr,
      /*use_value_delta_encoding=*/true, table_opts,
      /*ts_sz=*/0, /*persist_user_defined_timestamps=*/true));

  // Construct internal keys: user_key + PutFixed64(PackSequenceAndType(...))
  std::string ik1;
  ik1.append("aaa");
  PutFixed64(&ik1, PackSequenceAndType(100, kTypeValue));
  std::string ik2;
  ik2.append("bbb");
  PutFixed64(&ik2, PackSequenceAndType(50, kTypeValue));
  Slice ik1_slice(ik1);
  Slice ik2_slice(ik2);

  BlockHandle bh1(0, kBlockSize);
  BlockHandle bh2(kBlockSize + kBlockTrailerSize, kBlockSize);
  raw_builder->AddIndexEntry(ik1_slice, &ik2_slice, bh1, &scratch,
                             /*skip_delta_encoding=*/false);
  raw_builder->AddIndexEntry(ik2_slice, nullptr, bh2, &scratch,
                             /*skip_delta_encoding=*/false);

  IndexBuilder::IndexBlocks raw_blocks;
  ASSERT_OK(raw_builder->Finish(&raw_blocks));

  // Both should produce the same index block content.
  ASSERT_EQ(contents.ToString(), raw_blocks.index_block_contents.ToString());
}

// ============================================================================
// GetPartitionCoordinator test: partitioned builders return a non-null
// coordinator; non-partitioned builders return null.
// ============================================================================

TEST_F(PartitionedIndexFactoryTest, GetPartitionCoordinator) {
  // Create factory with full config (needed for partitioned builder).
  BuiltinIndexFactoryConfig config;
  InternalKeyComparator icmp(BytewiseComparator());
  config.internal_comparator = &icmp;
  config.use_delta_encoding_for_index_values = true;
  BlockBasedTableOptions table_opts;
  table_opts.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
  table_opts.metadata_block_size = 4096;
  config.table_options = &table_opts;

  PartitionedIndexFactory factory(config);
  IndexFactoryOptions opts;
  opts.comparator = BytewiseComparator();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory.NewBuilder(opts, builder));

  // Partitioned builder should return a non-null coordinator.
  auto* coord = builder->GetPartitionCoordinator();
  ASSERT_NE(coord, nullptr);

  // Non-partitioned builders should return null.
  BinarySearchIndexFactory bs_factory(/*with_first_key=*/false);
  std::unique_ptr<IndexFactoryBuilder> bs_builder;
  ASSERT_OK(bs_factory.NewBuilder(opts, bs_builder));
  ASSERT_EQ(bs_builder->GetPartitionCoordinator(), nullptr);

  HashIndexFactory hash_factory;
  std::unique_ptr<IndexFactoryBuilder> hash_builder;
  ASSERT_OK(hash_factory.NewBuilder(opts, hash_builder));
  ASSERT_EQ(hash_builder->GetPartitionCoordinator(), nullptr);
}

// ============================================================================
// FinishAndWrite default implementation test: the default FinishAndWrite
// calls Finish() then WriteBlock() once. We verify with a mock writer.
// ============================================================================

// Mock IndexBlockWriter for testing the FinishAndWrite protocol.
class MockIndexBlockWriter : public IndexFactoryBuilder::IndexBlockWriter {
 public:
  Status WriteBlock(const Slice& contents,
                    IndexFactoryBuilder::BlockHandle* handle,
                    bool /*compress*/) override {
    blocks_written.push_back(contents.ToString());
    handle->offset = next_offset;
    handle->size = contents.size();
    next_offset += contents.size();
    return Status::OK();
  }
  void AddMetaBlock(const std::string& name,
                    const IndexFactoryBuilder::BlockHandle& handle) override {
    meta_blocks.emplace_back(name, handle);
  }

  std::vector<std::string> blocks_written;
  std::vector<std::pair<std::string, IndexFactoryBuilder::BlockHandle>>
      meta_blocks;
  uint64_t next_offset = 0;
};

TEST_F(BinarySearchIndexFactoryTest, FinishAndWriteDefaultImpl) {
  // The default FinishAndWrite calls Finish() then WriteBlock() once.
  auto factory = BinarySearchIndexFactory(/*with_first_key=*/false);
  IndexFactoryOptions opts;
  opts.comparator = BytewiseComparator();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory.NewBuilder(opts, builder));

  // Add some entries.
  AddSampleEntries(builder.get());

  // FinishAndWrite through the default impl.
  MockIndexBlockWriter writer;
  IndexFactoryBuilder::BlockHandle final_handle{0, 0};
  ASSERT_OK(builder->FinishAndWrite(&writer, &final_handle, /*compress=*/true));

  // Should have written exactly one block.
  ASSERT_EQ(writer.blocks_written.size(), static_cast<size_t>(1));
  ASSERT_GT(writer.blocks_written[0].size(), static_cast<size_t>(0));
  ASSERT_EQ(final_handle.offset, static_cast<uint64_t>(0));
  ASSERT_EQ(final_handle.size, writer.blocks_written[0].size());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
