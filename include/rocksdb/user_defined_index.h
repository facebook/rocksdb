//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************

#pragma once

#include <string>

#include "rocksdb/advanced_iterator.h"
#include "rocksdb/customizable.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// Prefix for user-defined index block names
inline constexpr const char* kUserDefinedIndexPrefix =
    "rocksdb.user_defined_index.";

// This is a public API for user-defined index builders.
// It allows users to define their own index format and build custom
// indexes during table building. Currently, only a monolithic index
// block is supported (no partitioned index).

// The interface for building user-defined index.
class UserDefinedIndexBuilder {
 public:
  // Indicates the type of key-value entry being added via OnKeyAdded().
  // UDI builders that only use AddIndexEntry() (e.g., trie-based indexes)
  // can safely ignore this.
  enum ValueType : uint8_t {
    kValue = 0,   // Put: the value is the full user value.
    kDelete = 1,  // Deletion (Delete, SingleDelete, or DeleteWithTimestamp):
                  // the value is typically empty.
    kMerge = 2,   // Merge operand: the value is a partial update.
    kOther = 3,   // Other types (e.g., blob reference, wide-column entity).
                  // The value format is type-specific and may not be the
                  // actual user data.
    kTypeMax,     // Sentinel — must be last. Value may change across releases.
  };

  // File offset and size of the data block
  struct BlockHandle {
    uint64_t offset;
    uint64_t size;
  };

  // Optional context for AddIndexEntry providing sequence numbers at block
  // boundaries. Passed as a struct for forward-compatible extensibility
  // (new fields can be added without breaking existing implementations).
  struct IndexEntryContext {
    // Tag (packed sequence number and type) of last_key_in_current_block:
    //   (sequence_number << 8) | value_type
    // This is the same format used by InternalKeyComparator for ordering.
    // UDI implementations that encode sequence numbers should store this
    // tag (not just the sequence number) to ensure correct block
    // selection when the same user key spans multiple blocks.
    uint64_t last_key_tag = 0;
    // Tag (packed sequence number and type) of first_key_in_next_block (valid
    // only when first_key_in_next_block != nullptr).
    uint64_t first_key_tag = 0;
  };

  virtual ~UserDefinedIndexBuilder() = default;

  // Add a new index entry for a data block boundary.
  //
  // The keys are user keys (without the 8-byte tag).
  //
  // The UDI is free to compute a separator between the two user keys and
  // store it along with the block handle. The separator must satisfy:
  //   last_key_in_current_block <= separator < first_key_in_next_block
  // in user-key order (ignoring sequence numbers).
  //
  // Called before the OnKeyAdded() call for first_key_in_next_block.
  // @last_key_in_current_block: The last user key in the current data block
  // @first_key_in_next_block: First user key in the next data block, or
  //                           nullptr if this is the last block
  // @block_handle: offset/size of the data block
  // @separator_scratch: scratch buffer for a computed separator
  // @context: sequence number context for block boundaries. The sequence
  //   numbers are needed when the same user key spans a data block boundary
  //   (e.g., when snapshots keep multiple versions of a key). Without
  //   sequence numbers, the UDI cannot produce a separator that distinguishes
  //   the two blocks. This mirrors the internal index's behavior of switching
  //   to full internal-key separators (see
  //   ShortenedIndexBuilder::must_use_separator_with_seq_).
  //   Implementations that don't need sequence numbers can ignore the context.
  // @return: the separator stored in the index
  virtual Slice AddIndexEntry(const Slice& last_key_in_current_block,
                              const Slice* first_key_in_next_block,
                              const BlockHandle& block_handle,
                              std::string* separator_scratch,
                              const IndexEntryContext& context) = 0;

  // Called for every key-value pair added to the SST file. UDI builders may
  // override this to collect per-key information (e.g., for secondary
  // indexes). Builders that only use separator keys from AddIndexEntry()
  // (e.g., trie-based indexes) can leave this as a no-op.
  //
  // @key:   The user key (without sequence number or type suffix).
  // @type:  The entry type — kValue (Put), kDelete, kMerge, or kOther.
  //         For kDelete entries, the value may be empty. For kOther, the
  //         value format is type-specific and may not be actual user data.
  // @value: The associated value (may be empty for deletions).
  //
  // NOTE: In SST files produced by flush or compaction, there may be multiple
  // entries for the same user key with different sequence numbers (e.g., when
  // snapshots are active). UDI builders that use OnKeyAdded() should be
  // prepared for this.
  //
  // Thread safety: For a given builder instance, OnKeyAdded() and
  // AddIndexEntry() are always called from a single thread. Builders do
  // not need internal synchronization.
  virtual void OnKeyAdded(const Slice& /*key*/, ValueType /*type*/,
                          const Slice& /*value*/) {}

  // Finish building the index.
  // Returns a Status and the serialized index contents.
  // The memory backing the contents should not be freed until this builder
  // object is destructed.
  virtual Status Finish(Slice* index_contents) = 0;

  // Returns an estimate of the current serialized index size in bytes.
  virtual uint64_t EstimatedSize() const = 0;
};

// The interface for iterating the user defined index. This will be
// instantiated and used by a scan to iterate through the index entries
// covered by the scan.
class UserDefinedIndexIterator {
 public:
  virtual ~UserDefinedIndexIterator() = default;

  // Prepare the iterator for a series of scans. The iterator should use
  // this as an opportunity to do any prefetching and buffering of results.
  virtual void Prepare(const ScanOptions scan_opts[], size_t num_opts) = 0;

  // Optional context for SeekAndGetResult providing the target sequence
  // number. Passed as a struct for forward-compatible extensibility.
  struct SeekContext {
    // Tag (packed sequence number and type) of the target key:
    //   (sequence_number << 8) | value_type
    // Used by UDI implementations that encode sequence numbers (when the
    // same user key spans multiple data blocks) to locate the correct block.
    // Must match the format stored in
    // IndexEntryContext::last_key_tag.
    uint64_t target_tag = 0;
  };

  // Position the index iterator at the very first index entry. The result
  // must be populated the same way as SeekAndGetResult.
  //
  // The default implementation calls SeekAndGetResult with an empty key,
  // which works for BytewiseComparator (empty string is the smallest key).
  // Implementations should override this if they can reach the first entry
  // more efficiently or if they use a comparator where empty is not smallest.
  virtual Status SeekToFirstAndGetResult(IterateResult* result) {
    return SeekAndGetResult(Slice(), result, SeekContext{});
  }

  // Position the index iterator at the very last index entry. The result
  // must be populated the same way as SeekAndGetResult.
  //
  // The default implementation returns NotSupported. Concrete UDI
  // implementations must override this to support reverse iteration
  // (SeekToLast, Prev), which is required for full iterator functionality.
  virtual Status SeekToLastAndGetResult(IterateResult* result) {
    (void)result;
    return Status::NotSupported("SeekToLast not supported by this UDI");
  }

  // Move to the previous index entry. The result must be populated the
  // same way as SeekAndGetResult.
  //
  // The default implementation returns NotSupported. Concrete UDI
  // implementations must override this to support reverse iteration
  // (SeekToLast, Prev), which is required for full iterator functionality.
  virtual Status PrevAndGetResult(IterateResult* result) {
    (void)result;
    return Status::NotSupported("Prev not supported by this UDI");
  }

  // Given the target key, position the index iterator at the index entry
  // for the data block that may contain the target.
  //
  // The target is a user key.
  //
  // The result must be updated with the index key and bound_check_result.
  // bound_check_result should be kOutOfBound if no block satisfies the
  // target, kInbound if the data block is definitely within bounds, or
  // kUnknown if partially within bounds.
  //
  // The UDI implementation needs to be careful about returning kOutOfBound.
  // If a limit key is specified in ScanOptions, an implementation that
  // does not store the first key in the block for the corresponding index
  // entry cannot reliably determine if the block is out of bounds. It must
  // compare against the previous index key to determine if the current block
  // is out of bounds w.r.t the limit. Other termination criteria (specified
  // in property_bag) may cause the scan to terminate earlier, in which case
  // kOutOfBound can be returned earlier.
  //
  // @context: sequence number context for the seek. The sequence number is
  //   needed when the same user key spans multiple data blocks with different
  //   sequence numbers. Without it, the UDI cannot distinguish which block to
  //   return for a given (user_key, seqno) target. Implementations that don't
  //   need sequence numbers can ignore the context.
  virtual Status SeekAndGetResult(const Slice& target, IterateResult* result,
                                  const SeekContext& context) = 0;

  // Advance to the next index entry. The result must be populated similar
  // to SeekAndGetResult.
  virtual Status NextAndGetResult(IterateResult* result) = 0;

  // Return the BlockHandle in the current index entry
  virtual UserDefinedIndexBuilder::BlockHandle value() = 0;
};

// A reader interface for the user defined index
class UserDefinedIndexReader {
 public:
  virtual ~UserDefinedIndexReader() = default;

  // Allocate an iterator that will be used by RocksDB to perform scans
  virtual std::unique_ptr<UserDefinedIndexIterator> NewIterator(
      const ReadOptions& read_options) = 0;

  // The memory usage of the index, including the size of the raw contents and
  // any other heap data structures allocated by the reader
  virtual size_t ApproximateMemoryUsage() const = 0;
};

// Options for user defined index
struct UserDefinedIndexOption {
  const Comparator* comparator = BytewiseComparator();
};

// Factory for creating user-defined index builders.
class UserDefinedIndexFactory : public Customizable {
 public:
  ~UserDefinedIndexFactory() override = default;

  static const char* Type() { return "UserDefinedIndexFactory"; }

  static Status CreateFromString(
      const ConfigOptions& config_options, const std::string& value,
      std::shared_ptr<UserDefinedIndexFactory>* factory);

  // Create a new builder for user-defined index.
  virtual UserDefinedIndexBuilder* NewBuilder() const = 0;

  // Create a new user defined index reader given the contents of the index
  // block
  virtual std::unique_ptr<UserDefinedIndexReader> NewReader(
      Slice& index_block) const = 0;

  // New API for allowing customized comparator
  virtual Status NewBuilder(
      const UserDefinedIndexOption& /*option*/,
      std::unique_ptr<UserDefinedIndexBuilder>& builder) const {
    builder.reset(NewBuilder());
    return Status::OK();
  }

  virtual Status NewReader(
      const UserDefinedIndexOption& /*option*/, Slice& index_block,
      std::unique_ptr<UserDefinedIndexReader>& reader) const {
    reader = NewReader(index_block);
    return Status::OK();
  }
};

}  // namespace ROCKSDB_NAMESPACE
