// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/builtin_index_factory.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/index_factory.h"
#include "rocksdb/slice.h"
#include "rocksdb/user_defined_index.h"
#include "table/block_based/block_based_table_reader.h"
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

// CRITICAL: This test pins the exact string value of the meta block prefix
// used by user-defined indexes on disk. Changing this value breaks all SSTs
// written by previous RocksDB versions that use the UserDefinedIndex feature.
//
// Note that this test deliberately uses a hardcoded string literal rather
// than the constants -- that way an accidental change to either constant is
// caught here. If you find yourself updating this assertion, stop: you are
// introducing a backward-incompatible on-disk change that requires a
// deliberate format-version bump and a migration path.
TEST(OnDiskFormatTest, UserDefinedIndexMetaPrefix) {
  ASSERT_STREQ(kIndexFactoryMetaPrefix, "rocksdb.user_defined_index.");
  ASSERT_STREQ(kUserDefinedIndexPrefix, "rocksdb.user_defined_index.");
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
static constexpr uint64_t kBlockTrailerSize =
    BlockBasedTable::kBlockTrailerSize;
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

static std::string GeneratedKey(size_t index) {
  return "key" + std::to_string(100000 + index);
}

static void AddGeneratedEntries(IndexFactoryBuilder* builder, size_t count) {
  std::string scratch;
  for (size_t i = 0; i < count; ++i) {
    const std::string key = GeneratedKey(i);
    const std::string next_key = GeneratedKey(i + 1);
    const Slice key_slice(key);
    const Slice next_key_slice(next_key);
    const Slice* next_key_ptr = i + 1 == count ? nullptr : &next_key_slice;

    IndexFactoryBuilder::IndexEntryContext ctx;
    ctx.last_key_tag =
        PackSequenceAndType(static_cast<SequenceNumber>(count - i), kTypeValue);
    ctx.first_key_tag =
        next_key_ptr == nullptr
            ? 0
            : PackSequenceAndType(static_cast<SequenceNumber>(count - i - 1),
                                  kTypeValue);
    const IndexFactoryBuilder::BlockHandle handle{
        i * (kBlockSize + kBlockTrailerSize), kBlockSize};
    builder->AddIndexEntry(key_slice, next_key_ptr, handle, &scratch, ctx);
  }
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
  IndexFactoryOptions opts;
  opts.comparator = nullptr;
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
  IndexFactoryOptions opts;
  opts.comparator = nullptr;
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
  IndexFactoryOptions opts;
  opts.comparator = nullptr;
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
  ASSERT_EQ(
      static_cast<BuiltinIndexFactoryBuilder*>(builder.get())->GetComparator(),
      &icmp);

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

  auto* builtin_builder =
      static_cast<BuiltinIndexFactoryBuilder*>(builder.get());
  // Partitioned builder should return a non-null coordinator.
  auto* coord = builtin_builder->GetPartitionCoordinator();
  ASSERT_NE(coord, nullptr);

  // Non-partitioned builders should return null.
  BinarySearchIndexFactory bs_factory(/*with_first_key=*/false);
  std::unique_ptr<IndexFactoryBuilder> bs_builder;
  ASSERT_OK(bs_factory.NewBuilder(opts, bs_builder));
  auto* bs_builtin = static_cast<BuiltinIndexFactoryBuilder*>(bs_builder.get());
  ASSERT_EQ(bs_builtin->GetPartitionCoordinator(), nullptr);

  HashIndexFactory hash_factory;
  std::unique_ptr<IndexFactoryBuilder> hash_builder;
  ASSERT_OK(hash_factory.NewBuilder(opts, hash_builder));
  auto* hash_builtin =
      static_cast<BuiltinIndexFactoryBuilder*>(hash_builder.get());
  ASSERT_EQ(hash_builtin->GetPartitionCoordinator(), nullptr);
}

// ============================================================================
// Built-in FinishAndWrite writes a single binary-search index block through
// the internal writer callback.
// ============================================================================

class MockIndexBlockWriter : public BuiltinIndexBlockWriter {
 public:
  Status WriteBlock(const Slice& contents,
                    IndexFactoryBuilder::BlockHandle* handle,
                    bool /*compress*/) override {
    if (write_count == fail_on_write) {
      ++write_count;
      return Status::IOError("mock WriteBlock failed");
    }
    blocks_written.push_back(contents.ToString());
    handle->offset = next_offset;
    handle->size = contents.size();
    next_offset += contents.size() + kBlockTrailerSize;
    ++write_count;
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
  int write_count = 0;
  int fail_on_write = -1;
};

TEST_F(BinarySearchIndexFactoryTest, FinishAndWriteSingleBlock) {
  auto factory = BinarySearchIndexFactory(/*with_first_key=*/false);
  IndexFactoryOptions opts;
  opts.comparator = BytewiseComparator();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory.NewBuilder(opts, builder));

  // Add some entries.
  AddSampleEntries(builder.get());

  MockIndexBlockWriter writer;
  IndexFactoryBuilder::BlockHandle final_handle{0, 0};
  ASSERT_OK(static_cast<BuiltinIndexFactoryBuilder*>(builder.get())
                ->FinishAndWrite(&writer, &final_handle, /*compress=*/true));

  // Should have written exactly one block.
  ASSERT_EQ(writer.blocks_written.size(), static_cast<size_t>(1));
  ASSERT_GT(writer.blocks_written[0].size(), static_cast<size_t>(0));
  ASSERT_EQ(final_handle.offset, static_cast<uint64_t>(0));
  ASSERT_EQ(final_handle.size, writer.blocks_written[0].size());
}

TEST_F(BinarySearchIndexFactoryTest, FinishAndWritePropagatesWriteBlockError) {
  auto factory = BinarySearchIndexFactory(/*with_first_key=*/false);
  IndexFactoryOptions opts;
  opts.comparator = BytewiseComparator();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory.NewBuilder(opts, builder));

  AddSampleEntries(builder.get());

  MockIndexBlockWriter writer;
  writer.fail_on_write = 0;
  IndexFactoryBuilder::BlockHandle final_handle{0, 0};
  Status s = static_cast<BuiltinIndexFactoryBuilder*>(builder.get())
                 ->FinishAndWrite(&writer, &final_handle, /*compress=*/true);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_TRUE(writer.blocks_written.empty());
}

// ============================================================================
// Helpers for parallel-compression API surface tests.
// ============================================================================

// Build a configured BuiltinIndexFactoryBuilder for a given index_type.
// The internal_comparator and table_options are owned by the caller and
// must outlive the returned builder.
static std::unique_ptr<IndexFactoryBuilder> MakeConfiguredBuiltinBuilder(
    BlockBasedTableOptions::IndexType index_type,
    const InternalKeyComparator& icmp, const BlockBasedTableOptions& topts) {
  BuiltinIndexFactoryConfig config;
  config.internal_comparator = &icmp;
  config.use_delta_encoding_for_index_values = true;
  config.table_options = &topts;
  IndexFactoryOptions opts;
  opts.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  Status s = NewBuiltinIndexFactoryBuilder(index_type, config, opts, builder);
  EXPECT_OK(s);
  return builder;
}

// Build an internal-key string of the form `user_key | PackSequenceAndType`.
static std::string MakeInternalKey(const std::string& user_key,
                                   SequenceNumber seq, ValueType vt) {
  std::string out(user_key);
  PutFixed64(&out, PackSequenceAndType(seq, vt));
  return out;
}

// ============================================================================
// NewBuiltinIndexFactoryBuilder helper: dispatches on IndexType to the
// appropriate built-in factory and returns a usable IndexFactoryBuilder.
// ============================================================================

class NewBuiltinIndexFactoryBuilderTest : public ::testing::Test {};

TEST_F(NewBuiltinIndexFactoryBuilderTest, DispatchesAllIndexTypes) {
  InternalKeyComparator icmp(BytewiseComparator());
  BlockBasedTableOptions topts;

  for (auto t : {BlockBasedTableOptions::kBinarySearch,
                 BlockBasedTableOptions::kBinarySearchWithFirstKey,
                 BlockBasedTableOptions::kHashSearch,
                 BlockBasedTableOptions::kTwoLevelIndexSearch}) {
    SCOPED_TRACE("index_type=" + std::to_string(static_cast<int>(t)));
    BlockBasedTableOptions per_type = topts;
    per_type.index_type = t;
    if (t == BlockBasedTableOptions::kTwoLevelIndexSearch) {
      per_type.metadata_block_size = 4096;
    }
    auto builder = MakeConfiguredBuiltinBuilder(t, icmp, per_type);
    ASSERT_NE(builder, nullptr);
  }
}

TEST_F(NewBuiltinIndexFactoryBuilderTest, BinarySearchProducesUsableBuilder) {
  InternalKeyComparator icmp(BytewiseComparator());
  BlockBasedTableOptions topts;
  topts.index_type = BlockBasedTableOptions::kBinarySearch;

  auto builder = MakeConfiguredBuiltinBuilder(
      BlockBasedTableOptions::kBinarySearch, icmp, topts);
  ASSERT_NE(builder, nullptr);

  AddSampleEntries(builder.get());

  Slice contents;
  ASSERT_OK(builder->Finish(&contents));
  ASSERT_GT(contents.size(), static_cast<size_t>(0));
}

TEST_F(NewBuiltinIndexFactoryBuilderTest,
       BinarySearchWithFirstKeyProducesUsableBuilder) {
  InternalKeyComparator icmp(BytewiseComparator());
  BlockBasedTableOptions topts;
  topts.index_type = BlockBasedTableOptions::kBinarySearchWithFirstKey;

  auto builder = MakeConfiguredBuiltinBuilder(
      BlockBasedTableOptions::kBinarySearchWithFirstKey, icmp, topts);
  auto* builtin = static_cast<BuiltinIndexFactoryBuilder*>(builder.get());

  std::string scratch;
  const std::string ik_a = MakeInternalKey("aaa", 100, kTypeValue);
  const std::string ik_b = MakeInternalKey("bbb", 50, kTypeValue);
  const std::string ik_c = MakeInternalKey("ccc", 25, kTypeValue);
  const Slice ik_a_s(ik_a);
  const Slice ik_b_s(ik_b);
  const Slice ik_c_s(ik_c);
  const BlockHandle bh1(0, kBlockSize);
  const BlockHandle bh2(kBlockSize + kBlockTrailerSize, kBlockSize);
  const BlockHandle bh3(2 * (kBlockSize + kBlockTrailerSize), kBlockSize);

  builtin->OnKeyAddedInternal(ik_a_s, std::nullopt);
  builtin->AddIndexEntryDirect(ik_a_s, &ik_b_s, bh1, &scratch,
                               /*skip_delta_encoding=*/false);
  builtin->OnKeyAddedInternal(ik_b_s, std::nullopt);
  builtin->AddIndexEntryDirect(ik_b_s, &ik_c_s, bh2, &scratch,
                               /*skip_delta_encoding=*/false);
  builtin->OnKeyAddedInternal(ik_c_s, std::nullopt);
  builtin->AddIndexEntryDirect(ik_c_s, nullptr, bh3, &scratch,
                               /*skip_delta_encoding=*/false);

  Slice contents;
  ASSERT_OK(builder->Finish(&contents));
  ASSERT_GT(contents.size(), static_cast<size_t>(0));
}

TEST_F(NewBuiltinIndexFactoryBuilderTest, PartitionedFinishAndWrite) {
  InternalKeyComparator icmp(BytewiseComparator());
  BlockBasedTableOptions topts;
  topts.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
  topts.metadata_block_size = 128;

  auto builder = MakeConfiguredBuiltinBuilder(
      BlockBasedTableOptions::kTwoLevelIndexSearch, icmp, topts);
  auto* builtin = static_cast<BuiltinIndexFactoryBuilder*>(builder.get());

  AddGeneratedEntries(builder.get(), 64);

  MockIndexBlockWriter writer;
  IndexFactoryBuilder::BlockHandle final_handle{0, 0};
  ASSERT_OK(builtin->FinishAndWrite(&writer, &final_handle,
                                    /*compress=*/false));
  ASSERT_GT(writer.blocks_written.size(), static_cast<size_t>(1));
  ASSERT_GT(final_handle.size, static_cast<uint64_t>(0));
  ASSERT_GT(builtin->NumPartitions(), static_cast<size_t>(0));
}

// ============================================================================
// Parallel compression API surface on BuiltinIndexFactoryBuilder.
//
// Verifies that the SupportsParallelAddEntry / CreatePreparedAddEntry /
// PrepareAddEntry / FinishAddEntry contract works end-to-end and produces
// the same index block as the synchronous AddIndexEntry path. This is the
// path used by EmitBlockForParallel + BGWorker; if it ever drifts from the
// synchronous path, parallel compression with the built-in index produces
// silently corrupt output.
// ============================================================================

class BuiltinParallelCompressionApiTest : public ::testing::Test {};

TEST_F(BuiltinParallelCompressionApiTest, SupportsParallelAddEntry) {
  // All built-in IndexBuilder subclasses implement PrepareIndexEntry /
  // FinishIndexEntry, so BuiltinIndexFactoryBuilder reports support for
  // the parallel protocol regardless of index_type. Whether the table
  // builder *uses* parallel compression is a separate decision driven by
  // partition_filters / decouple_partitioned_filters settings.
  InternalKeyComparator icmp(BytewiseComparator());
  BlockBasedTableOptions topts;

  for (auto t : {BlockBasedTableOptions::kBinarySearch,
                 BlockBasedTableOptions::kBinarySearchWithFirstKey,
                 BlockBasedTableOptions::kHashSearch,
                 BlockBasedTableOptions::kTwoLevelIndexSearch}) {
    SCOPED_TRACE("index_type=" + std::to_string(static_cast<int>(t)));
    BlockBasedTableOptions per_type = topts;
    per_type.index_type = t;
    if (t == BlockBasedTableOptions::kTwoLevelIndexSearch) {
      per_type.metadata_block_size = 4096;
    }
    auto builder = MakeConfiguredBuiltinBuilder(t, icmp, per_type);
    EXPECT_TRUE(builder->SupportsParallelAddEntry());
  }
}

TEST_F(BuiltinParallelCompressionApiTest, PrepareFinishMatchesAddIndexEntry) {
  // Drive both the synchronous and parallel paths with identical input and
  // verify they produce byte-identical index blocks.
  InternalKeyComparator icmp(BytewiseComparator());
  BlockBasedTableOptions topts;
  topts.index_type = BlockBasedTableOptions::kBinarySearch;

  // --- Synchronous path ---
  auto sync_builder = MakeConfiguredBuiltinBuilder(
      BlockBasedTableOptions::kBinarySearch, icmp, topts);
  std::string scratch_a;
  IndexFactoryBuilder::IndexEntryContext ctx_ab;
  ctx_ab.last_key_tag = PackSequenceAndType(100, kTypeValue);
  ctx_ab.first_key_tag = PackSequenceAndType(50, kTypeValue);
  Slice key_a("aaa");
  Slice key_b("bbb");
  Slice key_c("ccc");
  IndexFactoryBuilder::BlockHandle h1{0, kBlockSize};
  IndexFactoryBuilder::BlockHandle h2{kBlockSize + kBlockTrailerSize,
                                      kBlockSize};
  IndexFactoryBuilder::BlockHandle h3{2 * (kBlockSize + kBlockTrailerSize),
                                      kBlockSize};
  IndexFactoryBuilder::IndexEntryContext ctx_bc;
  ctx_bc.last_key_tag = PackSequenceAndType(50, kTypeValue);
  ctx_bc.first_key_tag = PackSequenceAndType(25, kTypeValue);
  IndexFactoryBuilder::IndexEntryContext ctx_c;
  ctx_c.last_key_tag = PackSequenceAndType(25, kTypeValue);
  ctx_c.first_key_tag = 0;
  sync_builder->AddIndexEntry(key_a, &key_b, h1, &scratch_a, ctx_ab);
  sync_builder->AddIndexEntry(key_b, &key_c, h2, &scratch_a, ctx_bc);
  sync_builder->AddIndexEntry(key_c, nullptr, h3, &scratch_a, ctx_c);
  Slice sync_contents;
  ASSERT_OK(sync_builder->Finish(&sync_contents));

  // --- Parallel path: stage prepares first, then commit in order ---
  auto par_builder = MakeConfiguredBuiltinBuilder(
      BlockBasedTableOptions::kBinarySearch, icmp, topts);
  ASSERT_TRUE(par_builder->SupportsParallelAddEntry());
  auto p1 = par_builder->CreatePreparedAddEntry();
  auto p2 = par_builder->CreatePreparedAddEntry();
  auto p3 = par_builder->CreatePreparedAddEntry();
  par_builder->PrepareAddEntry(key_a, &key_b, ctx_ab, p1.get());
  par_builder->PrepareAddEntry(key_b, &key_c, ctx_bc, p2.get());
  par_builder->PrepareAddEntry(key_c, nullptr, ctx_c, p3.get());
  std::string scratch_b;
  par_builder->FinishAddEntry(h1, p1.get(), &scratch_b,
                              /*skip_delta_encoding=*/false);
  par_builder->FinishAddEntry(h2, p2.get(), &scratch_b,
                              /*skip_delta_encoding=*/false);
  par_builder->FinishAddEntry(h3, p3.get(), &scratch_b,
                              /*skip_delta_encoding=*/false);
  Slice par_contents;
  ASSERT_OK(par_builder->Finish(&par_contents));

  EXPECT_EQ(sync_contents.ToString(), par_contents.ToString());
}

TEST_F(BuiltinParallelCompressionApiTest,
       PrepareAddEntryDirectMatchesPrepareAddEntry) {
  // Compare PrepareAddEntryDirect (fast path used by EmitBlockForParallel,
  // takes internal keys) to PrepareAddEntry (the public path that takes
  // user keys + tags). Both must produce identical staged entries and,
  // after FinishAddEntry, identical index blocks.
  InternalKeyComparator icmp(BytewiseComparator());
  BlockBasedTableOptions topts;
  topts.index_type = BlockBasedTableOptions::kBinarySearch;

  // Sequence/type tags chosen distinct so any leakage shows up.
  const SequenceNumber seq_a = 1000;
  const SequenceNumber seq_b = 500;
  const SequenceNumber seq_c = 250;

  std::string ik_a = MakeInternalKey("aaa", seq_a, kTypeValue);
  std::string ik_b = MakeInternalKey("bbb", seq_b, kTypeValue);
  std::string ik_c = MakeInternalKey("ccc", seq_c, kTypeValue);
  Slice ik_a_s(ik_a);
  Slice ik_b_s(ik_b);
  Slice ik_c_s(ik_c);

  IndexFactoryBuilder::BlockHandle h1{0, kBlockSize};
  IndexFactoryBuilder::BlockHandle h2{kBlockSize + kBlockTrailerSize,
                                      kBlockSize};
  IndexFactoryBuilder::BlockHandle h3{2 * (kBlockSize + kBlockTrailerSize),
                                      kBlockSize};

  // --- Public path: PrepareAddEntry with user keys + context tags ---
  auto pub_builder = MakeConfiguredBuiltinBuilder(
      BlockBasedTableOptions::kBinarySearch, icmp, topts);
  IndexFactoryBuilder::IndexEntryContext ctx_ab;
  ctx_ab.last_key_tag = PackSequenceAndType(seq_a, kTypeValue);
  ctx_ab.first_key_tag = PackSequenceAndType(seq_b, kTypeValue);
  IndexFactoryBuilder::IndexEntryContext ctx_bc;
  ctx_bc.last_key_tag = PackSequenceAndType(seq_b, kTypeValue);
  ctx_bc.first_key_tag = PackSequenceAndType(seq_c, kTypeValue);
  IndexFactoryBuilder::IndexEntryContext ctx_c;
  ctx_c.last_key_tag = PackSequenceAndType(seq_c, kTypeValue);
  ctx_c.first_key_tag = 0;
  Slice key_a("aaa");
  Slice key_b("bbb");
  Slice key_c("ccc");
  auto p1 = pub_builder->CreatePreparedAddEntry();
  auto p2 = pub_builder->CreatePreparedAddEntry();
  auto p3 = pub_builder->CreatePreparedAddEntry();
  pub_builder->PrepareAddEntry(key_a, &key_b, ctx_ab, p1.get());
  pub_builder->PrepareAddEntry(key_b, &key_c, ctx_bc, p2.get());
  pub_builder->PrepareAddEntry(key_c, nullptr, ctx_c, p3.get());
  std::string scratch_pub;
  pub_builder->FinishAddEntry(h1, p1.get(), &scratch_pub, false);
  pub_builder->FinishAddEntry(h2, p2.get(), &scratch_pub, false);
  pub_builder->FinishAddEntry(h3, p3.get(), &scratch_pub, false);
  Slice pub_contents;
  ASSERT_OK(pub_builder->Finish(&pub_contents));

  // --- Direct path: PrepareAddEntryDirect with internal keys ---
  auto dir_builder = MakeConfiguredBuiltinBuilder(
      BlockBasedTableOptions::kBinarySearch, icmp, topts);
  // The direct path is on BuiltinIndexFactoryBuilder, not the base interface.
  auto* dir = static_cast<BuiltinIndexFactoryBuilder*>(dir_builder.get());
  auto d1 = dir->CreatePreparedAddEntry();
  auto d2 = dir->CreatePreparedAddEntry();
  auto d3 = dir->CreatePreparedAddEntry();
  dir->PrepareAddEntryDirect(ik_a_s, &ik_b_s, d1.get());
  dir->PrepareAddEntryDirect(ik_b_s, &ik_c_s, d2.get());
  dir->PrepareAddEntryDirect(ik_c_s, nullptr, d3.get());
  std::string scratch_dir;
  dir->FinishAddEntry(h1, d1.get(), &scratch_dir, false);
  dir->FinishAddEntry(h2, d2.get(), &scratch_dir, false);
  dir->FinishAddEntry(h3, d3.get(), &scratch_dir, false);
  Slice dir_contents;
  ASSERT_OK(dir_builder->Finish(&dir_contents));

  EXPECT_EQ(pub_contents.ToString(), dir_contents.ToString());
}

TEST_F(BuiltinParallelCompressionApiTest, AddIndexEntryDirectMatchesPublic) {
  // Same equivalence guarantee for the synchronous fast path used by
  // ForwardAddIndexEntryToAll.
  InternalKeyComparator icmp(BytewiseComparator());
  BlockBasedTableOptions topts;
  topts.index_type = BlockBasedTableOptions::kBinarySearch;

  const SequenceNumber seq_a = 100;
  const SequenceNumber seq_b = 50;
  const SequenceNumber seq_c = 25;
  std::string ik_a = MakeInternalKey("aaa", seq_a, kTypeValue);
  std::string ik_b = MakeInternalKey("bbb", seq_b, kTypeValue);
  std::string ik_c = MakeInternalKey("ccc", seq_c, kTypeValue);
  Slice ik_a_s(ik_a);
  Slice ik_b_s(ik_b);
  Slice ik_c_s(ik_c);

  BlockHandle bh1(0, kBlockSize);
  BlockHandle bh2(kBlockSize + kBlockTrailerSize, kBlockSize);
  BlockHandle bh3(2 * (kBlockSize + kBlockTrailerSize), kBlockSize);

  // --- Public path: AddIndexEntry (user keys + context tags) ---
  auto pub_builder = MakeConfiguredBuiltinBuilder(
      BlockBasedTableOptions::kBinarySearch, icmp, topts);
  std::string scratch_pub;
  IndexFactoryBuilder::IndexEntryContext ctx_ab;
  ctx_ab.last_key_tag = PackSequenceAndType(seq_a, kTypeValue);
  ctx_ab.first_key_tag = PackSequenceAndType(seq_b, kTypeValue);
  IndexFactoryBuilder::IndexEntryContext ctx_bc;
  ctx_bc.last_key_tag = PackSequenceAndType(seq_b, kTypeValue);
  ctx_bc.first_key_tag = PackSequenceAndType(seq_c, kTypeValue);
  IndexFactoryBuilder::IndexEntryContext ctx_c;
  ctx_c.last_key_tag = PackSequenceAndType(seq_c, kTypeValue);
  ctx_c.first_key_tag = 0;
  Slice key_a("aaa");
  Slice key_b("bbb");
  Slice key_c("ccc");
  IndexFactoryBuilder::BlockHandle ph1{bh1.offset(), bh1.size()};
  IndexFactoryBuilder::BlockHandle ph2{bh2.offset(), bh2.size()};
  IndexFactoryBuilder::BlockHandle ph3{bh3.offset(), bh3.size()};
  pub_builder->AddIndexEntry(key_a, &key_b, ph1, &scratch_pub, ctx_ab);
  pub_builder->AddIndexEntry(key_b, &key_c, ph2, &scratch_pub, ctx_bc);
  pub_builder->AddIndexEntry(key_c, nullptr, ph3, &scratch_pub, ctx_c);
  Slice pub_contents;
  ASSERT_OK(pub_builder->Finish(&pub_contents));

  // --- Direct path: AddIndexEntryDirect (internal keys) ---
  auto dir_builder = MakeConfiguredBuiltinBuilder(
      BlockBasedTableOptions::kBinarySearch, icmp, topts);
  auto* dir = static_cast<BuiltinIndexFactoryBuilder*>(dir_builder.get());
  std::string scratch_dir;
  dir->AddIndexEntryDirect(ik_a_s, &ik_b_s, bh1, &scratch_dir, false);
  dir->AddIndexEntryDirect(ik_b_s, &ik_c_s, bh2, &scratch_dir, false);
  dir->AddIndexEntryDirect(ik_c_s, nullptr, bh3, &scratch_dir, false);
  Slice dir_contents;
  ASSERT_OK(dir_builder->Finish(&dir_contents));

  EXPECT_EQ(pub_contents.ToString(), dir_contents.ToString());
}

TEST_F(BuiltinParallelCompressionApiTest, SetSkipDeltaEncodingAppliesNextAdd) {
  InternalKeyComparator icmp(BytewiseComparator());
  BlockBasedTableOptions topts;
  topts.index_type = BlockBasedTableOptions::kBinarySearch;

  const SequenceNumber seq_a = 100;
  const SequenceNumber seq_b = 50;
  const SequenceNumber seq_c = 25;
  std::string ik_a = MakeInternalKey("aaa", seq_a, kTypeValue);
  std::string ik_b = MakeInternalKey("bbb", seq_b, kTypeValue);
  std::string ik_c = MakeInternalKey("ccc", seq_c, kTypeValue);
  Slice ik_a_s(ik_a);
  Slice ik_b_s(ik_b);
  Slice ik_c_s(ik_c);

  BlockHandle bh1(0, kBlockSize);
  BlockHandle bh2(kBlockSize + kBlockTrailerSize + 17, kBlockSize);
  BlockHandle bh3(bh2.offset() + kBlockSize + kBlockTrailerSize, kBlockSize);

  auto public_builder = MakeConfiguredBuiltinBuilder(
      BlockBasedTableOptions::kBinarySearch, icmp, topts);
  auto* public_builtin =
      static_cast<BuiltinIndexFactoryBuilder*>(public_builder.get());
  std::string scratch_public;
  IndexFactoryBuilder::IndexEntryContext ctx_ab;
  ctx_ab.last_key_tag = PackSequenceAndType(seq_a, kTypeValue);
  ctx_ab.first_key_tag = PackSequenceAndType(seq_b, kTypeValue);
  IndexFactoryBuilder::IndexEntryContext ctx_bc;
  ctx_bc.last_key_tag = PackSequenceAndType(seq_b, kTypeValue);
  ctx_bc.first_key_tag = PackSequenceAndType(seq_c, kTypeValue);
  IndexFactoryBuilder::IndexEntryContext ctx_c;
  ctx_c.last_key_tag = PackSequenceAndType(seq_c, kTypeValue);
  ctx_c.first_key_tag = 0;
  Slice key_a("aaa");
  Slice key_b("bbb");
  Slice key_c("ccc");
  IndexFactoryBuilder::BlockHandle ph1{bh1.offset(), bh1.size()};
  IndexFactoryBuilder::BlockHandle ph2{bh2.offset(), bh2.size()};
  IndexFactoryBuilder::BlockHandle ph3{bh3.offset(), bh3.size()};
  public_builder->AddIndexEntry(key_a, &key_b, ph1, &scratch_public, ctx_ab);
  public_builtin->SetSkipDeltaEncoding(true);
  public_builder->AddIndexEntry(key_b, &key_c, ph2, &scratch_public, ctx_bc);
  public_builder->AddIndexEntry(key_c, nullptr, ph3, &scratch_public, ctx_c);
  Slice public_contents;
  ASSERT_OK(public_builder->Finish(&public_contents));

  auto direct_builder = MakeConfiguredBuiltinBuilder(
      BlockBasedTableOptions::kBinarySearch, icmp, topts);
  auto* direct = static_cast<BuiltinIndexFactoryBuilder*>(direct_builder.get());
  std::string scratch_direct;
  direct->AddIndexEntryDirect(ik_a_s, &ik_b_s, bh1, &scratch_direct,
                              /*skip_delta_encoding=*/false);
  direct->AddIndexEntryDirect(ik_b_s, &ik_c_s, bh2, &scratch_direct,
                              /*skip_delta_encoding=*/true);
  direct->AddIndexEntryDirect(ik_c_s, nullptr, bh3, &scratch_direct,
                              /*skip_delta_encoding=*/false);
  Slice direct_contents;
  ASSERT_OK(direct_builder->Finish(&direct_contents));

  EXPECT_EQ(public_contents.ToString(), direct_contents.ToString());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
