// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  *  EXPERIMENTAL: This interface is part of the RocksDB User     *
//  *  Defined Index (UDI) framework and may change at any time     *
//  *  without notice. It is not yet considered part of the stable   *
//  *  public API.                                                   *
//  *****************************************************************

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "rocksdb/advanced_iterator.h"
#include "rocksdb/customizable.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct ReadOptions;
class Comparator;
class PartitionCoordinator;

// Prefix for meta block keys used by custom index implementations.
inline constexpr const char* kIndexFactoryMetaPrefix = "rocksdb.index_factory.";

// ============================================================================
// IndexFactory: pluggable index for BlockBasedTable SST files.
//
// The IndexFactory interface allows custom index implementations (e.g., trie,
// learned index, etc.) to coexist alongside the built-in binary search index.
// In most modes, both indexes are built and stored in each SST file:
//   - The built-in binary search index (present in kSecondary/kPrimary)
//   - The custom index (present when an IndexFactory is configured)
// In kPrimaryOnly mode, only the custom index is built.
//
// Read routing:
//   - By default (index_mode=kBuiltinOnly), reads use the built-in binary
//     search index.
//   - When index_mode is kPrimary or kPrimaryOnly in BlockBasedTableOptions,
//     all reads (including internal operations) route through the custom index.
//   - Per-read override: set ReadOptions::read_index to select the
//     custom index for a specific read (relevant in kSecondary mode).
//
// This follows the FilterPolicy model: the built-in binary search index is
// analogous to the default data block format, while custom IndexFactory
// implementations are analogous to custom FilterPolicy implementations.
//
// Single-index mode (kPrimaryOnly): the built-in binary search index is
// not built. Only the custom IndexFactory produces an index, stored as a
// meta block in the SST. A minimal empty index block is written to
// satisfy the SST footer format.
//
// Fault injection note: the custom index meta block is vulnerable to
// metadata write fault injection (metadata_write_fault_one_in). If the
// meta block is corrupted, kPrimaryOnly has no fallback index and the
// compaction iterator reads zero keys from the affected SST. This is
// expected behavior — the standard binary search index (in kPrimary and
// below) is part of the SST's main block layout and is not affected by
// metadata write faults, providing a natural fallback. The stress tool
// disables compaction_verify_record_count for kPrimary/kPrimaryOnly
// when write fault injection is active. Without fault injection, all
// modes pass the compaction record count check correctly.
// ============================================================================

// ---------------------------------------------------------------------------
// IndexFactoryBuilder: builds a custom index during SST construction.
//
// Called by BlockBasedTableBuilder for every key and every data block
// boundary. The builder accumulates index entries and serializes them
// into a meta block stored in the SST.
//
// Thread safety: all methods except EstimatedSize() are called from a
// single thread (the emit thread in BlockBasedTableBuilder). Parallel
// compression is not supported for custom IndexFactory implementations.
// ---------------------------------------------------------------------------
class IndexFactoryBuilder {
 public:
  // Simple block handle used by the public interface.
  // Equivalent to the internal BlockHandle but without encoding/decoding.
  struct BlockHandle {
    uint64_t offset;
    uint64_t size;
  };

  // Context passed to AddIndexEntry describing the internal key tags
  // (packed sequence number + value type) for the last key in the
  // current block and the first key in the next block. These enable
  // custom indexes that need sequence-number-aware separator selection
  // (e.g., for correct Seek when the same user key spans multiple
  // blocks with different sequence numbers).
  struct IndexEntryContext {
    uint64_t last_key_tag = 0;
    uint64_t first_key_tag = 0;
  };

  // Value type categories for OnKeyAdded.
  enum ValueType : uint8_t {
    kValue = 0,
    kDelete = 1,
    kMerge = 2,
    kOther = 3,
  };

  virtual ~IndexFactoryBuilder() = default;

  // Called once for each data block boundary. The implementation should
  // record the association between the separator key and the block handle.
  //
  // @param last_key_in_current_block  User key of the last entry in the
  //                                   current data block.
  // @param first_key_in_next_block    User key of the first entry in the
  //                                   next data block. nullptr for the
  //                                   last block in the SST.
  // @param block_handle               Location and size of the data block.
  // @param separator_scratch          Scratch space for computing the
  //                                   separator. The returned Slice may
  //                                   reference this string.
  // @param context                    Packed sequence+type tags for the
  //                                   boundary keys.
  // @return                           The separator key actually stored.
  virtual Slice AddIndexEntry(const Slice& last_key_in_current_block,
                              const Slice* first_key_in_next_block,
                              const BlockHandle& block_handle,
                              std::string* separator_scratch,
                              const IndexEntryContext& context) = 0;

  // Called for every key added to the SST. This provides the custom
  // index with per-key visibility (e.g., for building a filter or
  // maintaining statistics). The default implementation is a no-op.
  //
  // @param key    User key (no internal key trailer).
  // @param type   Value type category.
  // @param value  The user value associated with this key.
  virtual void OnKeyAdded(const Slice& /*key*/, ValueType /*type*/,
                          const Slice& /*value*/) {}

  // Serialize the index into a byte buffer. The memory backing the
  // returned Slice must remain valid until this builder is destroyed.
  virtual Status Finish(Slice* index_contents) = 0;

  // Returns the estimated size in bytes of the index built so far.
  // Used by BlockBasedTableBuilder for SST file size estimation.
  // Thread safety: for built-in indexes, may be called concurrently from
  // the emit thread. Custom IndexFactory implementations are single-
  // threaded (parallel compression disabled), so concurrency is not a
  // concern.
  virtual uint64_t EstimatedSize() const = 0;

  // =========================================================================
  // Optional protocols. Default implementations are provided.
  // Built-in index factories override these for full functionality.
  // Custom index implementations typically do NOT need to override these.
  // =========================================================================

  // --- Write protocol: IndexBlockWriter callback for Finish ---
  //
  // Some indexes (e.g., partitioned) need to write multiple blocks during
  // Finish. The IndexBlockWriter callback allows the builder to drive the
  // write loop internally.
  class IndexBlockWriter {
   public:
    virtual ~IndexBlockWriter() = default;
    // Write a block to the SST file. The handle is populated on success.
    // @param compress  If true, the block may be compressed per SST options.
    virtual Status WriteBlock(const Slice& contents, BlockHandle* handle,
                              bool compress) = 0;
    // Register a meta block by name and handle with the meta index builder.
    // Called during FinishAndWrite for indexes that produce auxiliary meta
    // blocks (e.g., hash index prefix blocks).
    virtual void AddMetaBlock(const std::string& name,
                              const BlockHandle& handle) = 0;
  };

  // Finish the index and write all blocks via the writer callback.
  // The final_handle receives the handle of the top-level index block.
  // Default: calls Finish(Slice*) and writes a single block.
  virtual Status FinishAndWrite(IndexBlockWriter* writer,
                                BlockHandle* final_handle, bool compress) {
    Slice contents;
    Status s = Finish(&contents);
    if (!s.ok()) return s;
    return writer->WriteBlock(contents, final_handle, compress);
  }

  // --- Parallel compression protocol ---
  //
  // Splits AddIndexEntry into two phases for concurrent block compression:
  //   Phase 1 (emit thread): PrepareAddEntry — records keys + separator
  //   Phase 2 (write thread): FinishAddEntry — records the block handle
  //
  // Custom implementations that don't support parallel compression leave
  // SupportsParallelAddEntry() returning false. The table builder will
  // use single-threaded AddIndexEntry instead.

  struct PreparedAddEntry {
    virtual ~PreparedAddEntry() = default;
  };

  virtual bool SupportsParallelAddEntry() const { return false; }

  virtual std::unique_ptr<PreparedAddEntry> CreatePreparedAddEntry() {
    return nullptr;
  }

  // Phase 1: called on the emit thread. Records the separator keys.
  // The block handle is not yet known.
  virtual void PrepareAddEntry(const Slice& /*last_key_in_current_block*/,
                               const Slice* /*first_key_in_next_block*/,
                               const IndexEntryContext& /*context*/,
                               PreparedAddEntry* /*out*/) {}

  // Phase 2: called on the write thread. Records the block handle.
  // skip_delta_encoding is true when block alignment padding causes
  // non-sequential offsets.
  virtual void FinishAddEntry(const BlockHandle& /*block_handle*/,
                              PreparedAddEntry* /*entry*/,
                              std::string* /*separator_scratch*/,
                              bool /*skip_delta_encoding*/) {}

  // --- Metadata queries for table properties ---
  //
  // These are queried after Finish() to populate SST table properties.
  // Default values are appropriate for simple (non-partitioned) indexes.

  // Whether index separators include sequence numbers (internal key format).
  // true = separators are full internal keys (user_key + seq + type).
  // false = separators are user keys only.
  virtual bool separator_is_key_plus_seq() const { return true; }

  // Number of uniform-sized index blocks (0 if not applicable).
  virtual uint64_t NumUniformIndexBlocks() const { return 0; }

  // Total serialized index size (after Finish).
  virtual size_t IndexSize() const { return 0; }

  // --- Partitioned index metadata (0 for non-partitioned) ---

  virtual uint64_t NumPartitions() const { return 0; }

  virtual uint64_t TopLevelIndexSize(uint64_t /*offset*/) const { return 0; }

  // --- Filter coordination ---
  //
  // Returns a PartitionCoordinator for filter↔index partition alignment.
  // nullptr if this builder doesn't support partitioned coordination.
  // The returned pointer is valid for the lifetime of this builder.
  virtual PartitionCoordinator* GetPartitionCoordinator() { return nullptr; }
};

// ---------------------------------------------------------------------------
// IndexFactoryIterator: iterates over index entries in a custom index.
//
// Returned by IndexFactoryReader::NewIterator. Each position in the
// iterator corresponds to a data block in the SST file.
//
// The iterator returns user keys (not internal keys) as separator keys,
// and simple BlockHandle values (offset + size). The BlockBasedTable
// reader adapts these to the internal InternalIteratorBase<IndexValue>
// interface automatically.
// ---------------------------------------------------------------------------
class IndexFactoryIterator {
 public:
  virtual ~IndexFactoryIterator() = default;

  // Hint for upcoming scan ranges. Implementations may use this for
  // prefetching or bounding.
  // @param scan_opts  Array of scan range descriptors.
  // @param num_opts   Number of elements in scan_opts.
  virtual void Prepare(const ScanOptions scan_opts[], size_t num_opts) = 0;

  // Context for Seek, carrying the packed sequence+type tag of the
  // target key. Used by indexes that need sequence-number-aware block
  // selection (e.g., when the same user key spans multiple blocks).
  struct SeekContext {
    uint64_t target_tag = 0;
  };

  // Position at the first entry >= target and populate result.
  virtual Status SeekAndGetResult(const Slice& target, IterateResult* result,
                                  const SeekContext& context) = 0;

  // Advance to the next entry and populate result.
  virtual Status NextAndGetResult(IterateResult* result) = 0;

  // Position at the first entry.
  // Default: seeks with an empty key (works for bytewise comparator).
  virtual Status SeekToFirstAndGetResult(IterateResult* result) {
    return SeekAndGetResult(Slice(), result, SeekContext{});
  }

  // Position at the last entry. Optional — reverse iteration support.
  // Default: returns NotSupported.
  virtual Status SeekToLastAndGetResult(IterateResult* result) {
    (void)result;
    return Status::NotSupported("SeekToLast not supported by this index");
  }

  // Move to the previous entry. Optional — reverse iteration support.
  // Default: returns NotSupported.
  virtual Status PrevAndGetResult(IterateResult* result) {
    (void)result;
    return Status::NotSupported("Prev not supported by this index");
  }

  // Returns the block handle for the current position.
  virtual IndexFactoryBuilder::BlockHandle value() = 0;
};

// ---------------------------------------------------------------------------
// IndexFactoryReader: reads a custom index from a serialized SST block.
// ---------------------------------------------------------------------------
class IndexFactoryReader {
 public:
  virtual ~IndexFactoryReader() = default;

  // Create an iterator over the index.
  virtual std::unique_ptr<IndexFactoryIterator> NewIterator(
      const ReadOptions& read_options) = 0;

  // Approximate heap memory used by this reader (excluding the raw
  // index block contents, which are tracked separately by the block
  // cache or table reader).
  virtual size_t ApproximateMemoryUsage() const = 0;
};

// ---------------------------------------------------------------------------
// IndexFactoryOptions: configuration passed to NewBuilder / NewReader.
// ---------------------------------------------------------------------------
struct IndexFactoryOptions {
  // The user comparator for this column family.
  const Comparator* comparator = nullptr;
};

// ---------------------------------------------------------------------------
// IndexFactory: the top-level factory that creates builders and readers.
//
// Extends Customizable for string-based construction (CreateFromString),
// options serialization, and Name()-based identification.
// ---------------------------------------------------------------------------
class IndexFactory : public Customizable {
 public:
  ~IndexFactory() override = default;

  static const char* Type() { return "IndexFactory"; }

  // Create an IndexFactory from a string identifier (e.g., "trie").
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value,
                                 std::shared_ptr<IndexFactory>* factory);

  // Create a builder for constructing the index during SST creation.
  virtual Status NewBuilder(
      const IndexFactoryOptions& options,
      std::unique_ptr<IndexFactoryBuilder>& builder) const = 0;

  // Create a reader for an existing serialized index block.
  // @param options         Configuration (comparator, etc.)
  // @param index_contents  Raw bytes of the serialized index. The Slice
  //                        must remain valid for the lifetime of the reader.
  virtual Status NewReader(
      const IndexFactoryOptions& options, Slice& index_contents,
      std::unique_ptr<IndexFactoryReader>& reader) const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
