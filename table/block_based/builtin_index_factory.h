// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "rocksdb/index_factory.h"
#include "rocksdb/table.h"
#include "table/block_based/index_builder.h"

namespace ROCKSDB_NAMESPACE {

class InternalKeyComparator;
class InternalKeySliceTransform;
class Statistics;

// Built-in index factories wrap the internal IndexBuilder / IndexReader
// behind the public IndexFactory interface. They translate between the
// public form (user keys, simple BlockHandles) and the internal form
// (internal keys, IndexValue with first_internal_key).

// Construction parameters used by the built-in factories' NewBuilder()
// to configure the internal IndexBuilder. Custom factories use only
// IndexFactoryOptions::comparator and do not need this.
struct BuiltinIndexFactoryConfig {
  const InternalKeyComparator* internal_comparator = nullptr;
  const InternalKeySliceTransform* internal_prefix_transform = nullptr;
  bool use_delta_encoding_for_index_values = true;
  // Pointer to the Rep's table_options (which outlives the builder).
  // Avoids copying the large BlockBasedTableOptions struct per-SST.
  const BlockBasedTableOptions* table_options = nullptr;
  size_t ts_sz = 0;
  bool persist_user_defined_timestamps = true;
  Statistics* stats = nullptr;
};

// BinarySearchIndexFactory: the default BlockBasedTable index. Wraps
// ShortenedIndexBuilder and BinarySearchIndexReader. Handles both
// kBinarySearch and kBinarySearchWithFirstKey.
class BinarySearchIndexFactory : public IndexFactory {
 public:
  // @param with_first_key  If true, creates kBinarySearchWithFirstKey
  //                        indexes that store the first internal key per
  //                        block for optimized point lookups.
  BinarySearchIndexFactory(bool with_first_key,
                           const BuiltinIndexFactoryConfig& config);

  ~BinarySearchIndexFactory() override = default;

  const char* Name() const override;
  static const char* kClassName();
  static const char* kClassNameWithFirstKey();
  using IndexFactory::NewBuilder;
  using IndexFactory::NewReader;

  Status NewBuilder(
      const IndexFactoryOptions& options,
      std::unique_ptr<IndexFactoryBuilder>& builder) const override;

  Status NewReader(const IndexFactoryOptions& options, Slice& index_contents,
                   std::unique_ptr<IndexFactoryReader>& reader) const override;

 private:
  bool with_first_key_;
  BuiltinIndexFactoryConfig config_;
};

// HashIndexFactory: prefix-hash index. Wraps HashIndexBuilder and
// HashIndexReader. Requires a configured prefix_extractor.
class HashIndexFactory : public IndexFactory {
 public:
  explicit HashIndexFactory(const BuiltinIndexFactoryConfig& config);

  ~HashIndexFactory() override = default;

  const char* Name() const override;
  static const char* kClassName();
  using IndexFactory::NewBuilder;
  using IndexFactory::NewReader;

  Status NewBuilder(
      const IndexFactoryOptions& options,
      std::unique_ptr<IndexFactoryBuilder>& builder) const override;

  Status NewReader(const IndexFactoryOptions& options, Slice& index_contents,
                   std::unique_ptr<IndexFactoryReader>& reader) const override;

 private:
  BuiltinIndexFactoryConfig config_;
};

// PartitionedIndexFactory: two-level partitioned index. Wraps
// PartitionedIndexBuilder and PartitionIndexReader. Implements the
// multi-block FinishAndWrite protocol and exposes the underlying
// PartitionedIndexBuilder for filter <-> index partition alignment.
class PartitionedIndexFactory : public IndexFactory {
 public:
  explicit PartitionedIndexFactory(const BuiltinIndexFactoryConfig& config);

  ~PartitionedIndexFactory() override = default;

  const char* Name() const override;
  static const char* kClassName();
  using IndexFactory::NewBuilder;
  using IndexFactory::NewReader;

  Status NewBuilder(
      const IndexFactoryOptions& options,
      std::unique_ptr<IndexFactoryBuilder>& builder) const override;

  Status NewReader(const IndexFactoryOptions& options, Slice& index_contents,
                   std::unique_ptr<IndexFactoryReader>& reader) const override;

 private:
  BuiltinIndexFactoryConfig config_;
};

// Dispatch on BlockBasedTableOptions::IndexType and construct the
// matching built-in factory's builder.
Status NewBuiltinIndexFactoryBuilder(
    BlockBasedTableOptions::IndexType index_type,
    const BuiltinIndexFactoryConfig& config, const IndexFactoryOptions& options,
    std::unique_ptr<IndexFactoryBuilder>& out);

// BuiltinIndexFactoryBuilder adapts the internal IndexBuilder to the common
// IndexFactoryBuilder ownership and parallel-entry protocols. Built-in
// indexes still require the *Direct key methods and FinishAndWrite because
// their internal-key and multi-block contracts are not part of the custom
// index SPI.
//
// Unqualified BlockHandle inside this class means the public
// IndexFactoryBuilder::BlockHandle. The internal table/format.h handle is
// always spelled ::ROCKSDB_NAMESPACE::BlockHandle.

class BuiltinIndexBlockWriter {
 public:
  virtual ~BuiltinIndexBlockWriter() = default;

  virtual Status WriteBlock(const Slice& contents,
                            IndexFactoryBuilder::BlockHandle* handle,
                            bool compress) = 0;
  virtual void AddMetaBlock(const std::string& name,
                            const IndexFactoryBuilder::BlockHandle& handle) = 0;
};

class BuiltinIndexFactoryBuilder : public IndexFactoryBuilder {
 public:
  BuiltinIndexFactoryBuilder(BlockBasedTableOptions::IndexType index_type,
                             std::unique_ptr<IndexBuilder> internal_builder,
                             PartitionedIndexBuilder* partitioned_builder);
  ~BuiltinIndexFactoryBuilder() override;

  // Forward to the internal builder with the full internal key.
  // Needed by kBinarySearchWithFirstKey to track first_internal_key.
  // Inlined because this is called per key from the table builder.
  inline void OnKeyAddedInternal(const Slice& internal_key,
                                 const std::optional<Slice>& value) {
    internal_builder_->OnKeyAdded(internal_key, value);
  }

  // --- IndexFactoryBuilder overrides ---
  Slice AddIndexEntry(const Slice& last_key_in_current_block,
                      const Slice* first_key_in_next_block,
                      const BlockHandle& block_handle,
                      std::string* separator_scratch,
                      const IndexEntryContext& context) override;

  void OnKeyAdded(const Slice& key, ValueType type,
                  const Slice& value) override;

  Status Finish(Slice* index_contents) override;
  uint64_t EstimatedSize() const override;
  uint64_t CurrentIndexSizeEstimate() const;

  Status FinishAndWrite(BuiltinIndexBlockWriter* writer,
                        BlockHandle* final_handle, bool compress);

  bool SupportsParallelAddEntry() const override;
  std::unique_ptr<PreparedAddEntry> CreatePreparedAddEntry() override;
  void PrepareAddEntry(const Slice& last_key, const Slice* next_key,
                       const IndexEntryContext& ctx,
                       PreparedAddEntry* out) override;
  void FinishAddEntry(const BlockHandle& handle, PreparedAddEntry* entry,
                      std::string* separator_scratch,
                      bool skip_delta_encoding) override;

  // Metadata the table builder reads back to populate SST properties. These
  // describe the standard index layout, so they are only meaningful for the
  // built-in builder and are not part of the public builder interface.
  bool separator_is_key_plus_seq() const;
  uint64_t NumUniformIndexBlocks() const;
  size_t IndexSize() const;
  size_t NumPartitions() const;
  size_t TopLevelIndexSize(uint64_t offset) const;

  // Non-null only when the underlying builder is partitioned. The
  // partitioned filter builder uses it to align filter partitions with
  // index partitions.
  PartitionedIndexBuilder* GetPartitionedIndexBuilder();

  IndexBuilder* GetInternalBuilder();

  // Synchronous fast path: pass internal keys straight through to the
  // underlying IndexBuilder. Avoids the decompose/recompose overhead of
  // the public AddIndexEntry (which works in user-key form).
  Slice AddIndexEntryDirect(const Slice& last_internal_key,
                            const Slice* first_internal_key_next,
                            const ::ROCKSDB_NAMESPACE::BlockHandle& handle,
                            std::string* separator_scratch,
                            bool skip_delta_encoding);

  // Parallel fast path: stages a prepared entry from internal keys
  // directly, skipping the parse-and-repack the public PrepareAddEntry
  // performs from user keys + context tags. Caller must later invoke
  // FinishAddEntry() with the resolved BlockHandle to commit the entry.
  void PrepareAddEntryDirect(const Slice& last_internal_key,
                             const Slice* first_internal_key_next,
                             PreparedAddEntry* out);

 private:
  BlockBasedTableOptions::IndexType index_type_;
  std::unique_ptr<IndexBuilder> internal_builder_;
  // Non-owning alias of internal_builder_ when it is a
  // PartitionedIndexBuilder, else nullptr.
  PartitionedIndexBuilder* partitioned_builder_;
  // ReconstructInternalKeys() and PrepareAddEntry() use these buffers only
  // on the public user-key path. A table builder uses either the
  // synchronous add path or the parallel prepare/finish path for one SST,
  // not both concurrently.
  std::string last_internal_key_;
  std::string next_internal_key_;

  // Reconstruct full internal keys from user keys and packed tags.
  // Writes into last_internal_key_ and (if next_user_key != nullptr)
  // next_internal_key_ member buffers.
  void ReconstructInternalKeys(const Slice& last_user_key,
                               const Slice* next_user_key,
                               const IndexEntryContext& ctx);
};

}  // namespace ROCKSDB_NAMESPACE
