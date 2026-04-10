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

namespace ROCKSDB_NAMESPACE {

class InternalKeyComparator;
class InternalKeySliceTransform;
class Statistics;

// ============================================================================
// Built-in index factories.
//
// These wrap RocksDB's internal index builder/reader infrastructure behind
// the public IndexFactory interface. They are proper IndexFactory subclasses
// — at the same abstraction level as any custom IndexFactory implementation.
//
// The internal IndexBuilder/IndexReader classes remain as implementation
// details. The built-in factories delegate to them, translating between
// the public interface (user keys, simple BlockHandles) and the internal
// interface (internal keys, IndexValue with first_internal_key).
//
// Unlike custom IndexFactory implementations, these built-in factories store
// internal construction parameters (comparator, prefix transform, table
// options, etc.) that are set at factory creation time and used by
// NewBuilder(). This allows the table builder to create the built-in index
// through the same factory interface used for custom indexes.
// ============================================================================

// ---------------------------------------------------------------------------
// Internal construction parameters for built-in index factories.
//
// These are set at factory creation time (per-SST in the Rep constructor)
// and used by NewBuilder() to construct the internal IndexBuilder with
// the correct configuration. Custom IndexFactory implementations do NOT
// need these — they use only IndexFactoryOptions::comparator.
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// BinarySearchIndexFactory: the default index for BlockBasedTable.
//
// Wraps ShortenedIndexBuilder (for building) and BinarySearchIndexReader
// (for reading). Supports kBinarySearch and kBinarySearchWithFirstKey
// index types.
//
// This factory is implicitly used when no custom IndexFactory is configured.
// It can also be explicitly set as a secondary index alongside a custom
// primary index.
// ---------------------------------------------------------------------------
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

  Status NewReader(const IndexFactoryOptions& options, Slice& index_contents,
                   std::unique_ptr<IndexFactoryReader>& reader) const override;

 private:
  bool with_first_key_;
  bool has_config_ = false;
  BuiltinIndexFactoryConfig config_;
};

// ---------------------------------------------------------------------------
// HashIndexFactory: hash-based prefix index for BlockBasedTable.
//
// Wraps HashIndexBuilder (for building) and HashIndexReader (for reading).
// Requires a prefix_extractor to be configured.
// ---------------------------------------------------------------------------
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

  Status NewReader(const IndexFactoryOptions& options, Slice& index_contents,
                   std::unique_ptr<IndexFactoryReader>& reader) const override;

 private:
  bool has_config_ = false;
  BuiltinIndexFactoryConfig config_;
};

// ---------------------------------------------------------------------------
// PartitionedIndexFactory: two-level partitioned index for BlockBasedTable.
//
// Wraps PartitionedIndexBuilder (for building) and PartitionIndexReader
// (for reading). Supports partitioned filters via the PartitionCoordinator
// interface. The builder implements the full FinishAndWrite protocol for
// multi-partition writes and exposes GetPartitionCoordinator() for
// filter↔index partition alignment.
// ---------------------------------------------------------------------------
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

  Status NewReader(const IndexFactoryOptions& options, Slice& index_contents,
                   std::unique_ptr<IndexFactoryReader>& reader) const override;

 private:
  bool has_config_ = false;
  BuiltinIndexFactoryConfig config_;
};

// ---------------------------------------------------------------------------
// BuiltinIndexFactoryBuilder: adapts the internal IndexBuilder behind the
// public IndexFactoryBuilder interface. Declared here so the table builder
// can access methods like OnKeyAddedInternal() and AddIndexEntryDirect()
// for the fast path. Implementation is in builtin_index_factory.cc.
// ---------------------------------------------------------------------------
class BlockHandle;  // Internal BlockHandle from table/format.h
class IndexBuilder;

class BuiltinIndexFactoryBuilder : public IndexFactoryBuilder {
 public:
  BuiltinIndexFactoryBuilder(std::unique_ptr<InternalKeyComparator> icmp,
                             const BlockBasedTableOptions* table_opts);
  ~BuiltinIndexFactoryBuilder() override;

  void SetInternalBuilder(std::unique_ptr<IndexBuilder> builder);

  const InternalKeyComparator* GetComparator() const;
  const BlockBasedTableOptions& GetTableOptions() const;

  // Forward OnKeyAdded to the internal builder with the full internal key.
  // Called by the table builder which has the internal key available.
  // Needed for kBinarySearchWithFirstKey to track first_internal_key.
  void OnKeyAddedInternal(const Slice& internal_key,
                          const std::optional<Slice>& value);

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

  Status FinishAndWrite(IndexBlockWriter* writer, BlockHandle* final_handle,
                        bool compress) override;

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
  uint64_t NumPartitions() const override;
  uint64_t TopLevelIndexSize(uint64_t offset) const override;
  PartitionCoordinator* GetPartitionCoordinator() override;

  IndexBuilder* GetInternalBuilder();

  // Set whether the next AddIndexEntry should skip delta encoding.
  // This is called by the table builder when block alignment padding
  // causes non-contiguous block offsets, which breaks the delta
  // encoding assumption. Must be called before AddIndexEntry.
  void SetSkipDeltaEncoding(bool skip) { skip_delta_encoding_ = skip; }

  // Fast path for when no user-key translation is needed. Passes internal
  // keys directly to the underlying IndexBuilder, avoiding the decompose-
  // recompose overhead of the public AddIndexEntry (which converts user
  // keys back to internal keys). Used by ForwardAddIndexEntryToAll when
  // there are no custom indexes.
  Slice AddIndexEntryDirect(const Slice& last_internal_key,
                            const Slice* first_internal_key_next,
                            const ::ROCKSDB_NAMESPACE::BlockHandle& handle,
                            std::string* separator_scratch,
                            bool skip_delta_encoding);

 private:
  std::unique_ptr<InternalKeyComparator> icmp_;
  const BlockBasedTableOptions* table_opts_;
  std::unique_ptr<IndexBuilder> internal_builder_;
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
