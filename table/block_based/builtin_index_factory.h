// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

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
  // Lightweight constructor for standalone / test usage.
  // NewBuilder will use minimal default configuration.
  // @param with_first_key  If true, creates kBinarySearchWithFirstKey
  //                        indexes that store the first internal key per
  //                        block for optimized point lookups.
  explicit BinarySearchIndexFactory(bool with_first_key = false);

  // Full constructor for use by the table builder.
  // The factory stores all internal params needed by NewBuilder().
  BinarySearchIndexFactory(bool with_first_key,
                           const BuiltinIndexFactoryConfig& config);

  ~BinarySearchIndexFactory() override = default;

  const char* Name() const override;
  static const char* kClassName();
  static const char* kClassNameWithFirstKey();

  Status NewBuilder(
      const IndexFactoryOptions& options,
      std::unique_ptr<IndexFactoryBuilder>& builder) const override;

  IndexFactoryBuilder* NewBuilder() const override {
    std::unique_ptr<IndexFactoryBuilder> builder;
    Status s = NewBuilder(IndexFactoryOptions(), builder);
    return s.ok() ? builder.release() : nullptr;
  }

  Status NewReader(const IndexFactoryOptions& options, Slice& index_contents,
                   std::unique_ptr<IndexFactoryReader>& reader) const override;

  std::unique_ptr<IndexFactoryReader> NewReader(
      Slice& index_contents) const override {
    std::unique_ptr<IndexFactoryReader> reader;
    Status s = NewReader(IndexFactoryOptions(), index_contents, reader);
    return s.ok() ? std::move(reader) : nullptr;
  }

 private:
  bool with_first_key_;
  bool has_config_ = false;
  BuiltinIndexFactoryConfig config_;
};

// HashIndexFactory: prefix-hash index. Wraps HashIndexBuilder and
// HashIndexReader. Requires a configured prefix_extractor.
class HashIndexFactory : public IndexFactory {
 public:
  // Lightweight constructor for standalone / test usage.
  HashIndexFactory() = default;

  // Full constructor for use by the table builder.
  explicit HashIndexFactory(const BuiltinIndexFactoryConfig& config);

  ~HashIndexFactory() override = default;

  const char* Name() const override;
  static const char* kClassName();

  Status NewBuilder(
      const IndexFactoryOptions& options,
      std::unique_ptr<IndexFactoryBuilder>& builder) const override;

  IndexFactoryBuilder* NewBuilder() const override {
    std::unique_ptr<IndexFactoryBuilder> builder;
    Status s = NewBuilder(IndexFactoryOptions(), builder);
    return s.ok() ? builder.release() : nullptr;
  }

  Status NewReader(const IndexFactoryOptions& options, Slice& index_contents,
                   std::unique_ptr<IndexFactoryReader>& reader) const override;

  std::unique_ptr<IndexFactoryReader> NewReader(
      Slice& index_contents) const override {
    std::unique_ptr<IndexFactoryReader> reader;
    Status s = NewReader(IndexFactoryOptions(), index_contents, reader);
    return s.ok() ? std::move(reader) : nullptr;
  }

 private:
  bool has_config_ = false;
  BuiltinIndexFactoryConfig config_;
};

// PartitionedIndexFactory: two-level partitioned index. Wraps
// PartitionedIndexBuilder and PartitionIndexReader. Implements the
// multi-block FinishAndWrite protocol and provides the internal
// PartitionCoordinator used for filter <-> index partition alignment.
class PartitionedIndexFactory : public IndexFactory {
 public:
  // Lightweight constructor for standalone / test usage.
  PartitionedIndexFactory() = default;

  // Full constructor for use by the table builder.
  explicit PartitionedIndexFactory(const BuiltinIndexFactoryConfig& config);

  ~PartitionedIndexFactory() override = default;

  const char* Name() const override;
  static const char* kClassName();

  Status NewBuilder(
      const IndexFactoryOptions& options,
      std::unique_ptr<IndexFactoryBuilder>& builder) const override;

  IndexFactoryBuilder* NewBuilder() const override {
    std::unique_ptr<IndexFactoryBuilder> builder;
    Status s = NewBuilder(IndexFactoryOptions(), builder);
    return s.ok() ? builder.release() : nullptr;
  }

  Status NewReader(const IndexFactoryOptions& options, Slice& index_contents,
                   std::unique_ptr<IndexFactoryReader>& reader) const override;

  std::unique_ptr<IndexFactoryReader> NewReader(
      Slice& index_contents) const override {
    std::unique_ptr<IndexFactoryReader> reader;
    Status s = NewReader(IndexFactoryOptions(), index_contents, reader);
    return s.ok() ? std::move(reader) : nullptr;
  }

 private:
  bool has_config_ = false;
  BuiltinIndexFactoryConfig config_;
};

// Dispatch on BlockBasedTableOptions::IndexType and construct the
// matching built-in factory's builder.
Status NewBuiltinIndexFactoryBuilder(
    BlockBasedTableOptions::IndexType index_type,
    const BuiltinIndexFactoryConfig& config, const IndexFactoryOptions& options,
    std::unique_ptr<IndexFactoryBuilder>& out);

// BuiltinIndexFactoryBuilder: adapts the internal IndexBuilder to the
// public IndexFactoryBuilder interface. The table builder uses the
// *Direct methods to bypass the user-key parse-and-repack on the
// per-block-boundary hot path.
class BlockHandle;  // Internal BlockHandle from table/format.h

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
  BuiltinIndexFactoryBuilder(const InternalKeyComparator* icmp,
                             const BlockBasedTableOptions* table_opts);
  BuiltinIndexFactoryBuilder(std::unique_ptr<InternalKeyComparator> icmp,
                             const BlockBasedTableOptions* table_opts);
  ~BuiltinIndexFactoryBuilder() override;

  void SetInternalBuilder(std::unique_ptr<IndexBuilder> builder);

  const InternalKeyComparator* GetComparator() const;
  const BlockBasedTableOptions& GetTableOptions() const;

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

  bool separator_is_key_plus_seq() const override;
  uint64_t NumUniformIndexBlocks() const override;
  size_t IndexSize() const override;
  size_t NumPartitions() const override;
  size_t TopLevelIndexSize(uint64_t offset) const override;
  PartitionCoordinator* GetPartitionCoordinator();

  IndexBuilder* GetInternalBuilder();

  // Set whether the next AddIndexEntry should skip delta encoding.
  // This is called by the table builder when block alignment padding
  // causes non-contiguous block offsets, which breaks the delta
  // encoding assumption. Must be called before AddIndexEntry.
  void SetSkipDeltaEncoding(bool skip) { skip_delta_encoding_ = skip; }

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
  std::unique_ptr<InternalKeyComparator> owned_icmp_;
  const InternalKeyComparator* icmp_;
  const BlockBasedTableOptions* table_opts_;
  std::unique_ptr<IndexBuilder> internal_builder_;
  // ReconstructInternalKeys() uses these buffers only on the public
  // user-key path. A table builder uses either the synchronous add path or the
  // parallel prepare/finish path for one SST, not both concurrently.
  std::string last_internal_key_;
  std::string next_internal_key_;
  bool skip_delta_encoding_ = false;

  // Reconstruct full internal keys from user keys and packed tags.
  // Writes into last_internal_key_ and (if next_user_key != nullptr)
  // next_internal_key_ member buffers.
  void ReconstructInternalKeys(const Slice& last_user_key,
                               const Slice* next_user_key,
                               const IndexEntryContext& ctx);

  // Returns true when the internal builder is a PartitionedIndexBuilder,
  // determined by checking table_opts_->index_type. Used to safely
  // static_cast for partition-specific methods without requiring RTTI.
  bool IsPartitioned() const;
};

}  // namespace ROCKSDB_NAMESPACE
