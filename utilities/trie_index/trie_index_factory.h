//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************
//
//  Trie-based User Defined Index (UDI) for RocksDB's block-based tables.
//
//  This provides a TrieIndexFactory that implements the UserDefinedIndexFactory
//  interface, building a Fast Succinct Trie (FST) index from the separator keys
//  generated during SST file construction. Based on the SuRF paper results, the
//  trie is expected to achieve significant space reduction compared to the
//  default binary search index while providing comparable Seek() performance.
//
//  Usage:
//    auto trie_factory = std::make_shared<TrieIndexFactory>();
//    BlockBasedTableOptions table_options;
//    table_options.user_defined_index_factory = trie_factory;
//
//  At read time, set ReadOptions::table_index_factory to the same factory
//  to use the trie for iteration:
//    ReadOptions ro;
//    ro.table_index_factory = trie_factory.get();
//    auto iter = db->NewIterator(ro);

#pragma once

#include <cstdlib>
#include <memory>
#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/user_defined_index.h"
#include "utilities/trie_index/louds_trie.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// ============================================================================
// TrieIndexBuilder: Implements UserDefinedIndexBuilder using LoudsTrieBuilder.
//
// During SST file construction, RocksDB calls:
//   1. OnKeyAdded() for each key-value pair.
//   2. AddIndexEntry() at each data block boundary.
//   3. Finish() to serialize the index.
//
// The trie builder collects the separator keys from AddIndexEntry() and
// builds a LOUDS-encoded trie during Finish().
// ============================================================================
class TrieIndexBuilder : public UserDefinedIndexBuilder {
 public:
  explicit TrieIndexBuilder(const Comparator* comparator);
  ~TrieIndexBuilder() override = default;

  // Called at each data block boundary. We compute the shortest separator
  // between last_key_in_current_block and first_key_in_next_block, insert
  // it into the trie, and return it.
  Slice AddIndexEntry(const Slice& last_key_in_current_block,
                      const Slice* first_key_in_next_block,
                      const BlockHandle& block_handle,
                      std::string* separator_scratch) override;

  // Called for each key added to the SST. Currently a no-op — the trie is
  // built entirely from separator keys provided via AddIndexEntry().
  void OnKeyAdded(const Slice& key, ValueType type,
                  const Slice& value) override;

  // Finalize the trie and return the serialized index data.
  Status Finish(Slice* index_contents) override;

 private:
  const Comparator* comparator_;
  LoudsTrieBuilder trie_builder_;
  bool finished_;
};

// ============================================================================
// TrieIndexIterator: Implements UserDefinedIndexIterator using
// LoudsTrieIterator.
//
// Wraps LoudsTrieIterator and adapts it to the UDI iterator interface,
// handling bounds checking against ScanOptions.
// ============================================================================
class TrieIndexIterator : public UserDefinedIndexIterator {
 public:
  TrieIndexIterator(const LoudsTrie* trie, const Comparator* comparator);
  ~TrieIndexIterator() override = default;

  // Prepare for a batch of scans. Stores scan bounds for later use.
  void Prepare(const ScanOptions scan_opts[], size_t num_opts) override;

  // Seek to the first index entry >= target. Checks bounds and sets
  // result->bound_check_result accordingly.
  Status SeekAndGetResult(const Slice& target, IterateResult* result) override;

  // Advance to the next index entry. Checks bounds and sets
  // result->bound_check_result accordingly.
  Status NextAndGetResult(IterateResult* result) override;

  // Return the BlockHandle of the current leaf.
  UserDefinedIndexBuilder::BlockHandle value() override;

 private:
  // Check if the current block is within the active scan bounds.
  // reference_key is the key to compare against the limit: for Seek this
  // is the seek target, for Next this is the previous separator key.
  // The trie stores separator keys (upper bounds on block contents), not
  // first-in-block keys, so we cannot compare the current separator against
  // the limit directly — see the UDI API contract in user_defined_index.h.
  IterBoundCheck CheckBounds(const Slice& reference_key) const;

  const Comparator* comparator_;
  LoudsTrieIterator iter_;
  // Scratch space for the current separator key (reconstructed from trie).
  std::string current_key_scratch_;
  // Previous separator key, used as reference for Next() bounds checking.
  std::string prev_key_scratch_;
  bool has_prev_key_;

  // Active scan options (from Prepare()).
  std::vector<ScanOptions> scan_opts_;
  size_t current_scan_idx_;
  bool prepared_;
};

// ============================================================================
// TrieIndexReader: Implements UserDefinedIndexReader.
//
// Owns (or references) the deserialized LoudsTrie and creates iterators
// for read operations.
// ============================================================================
class TrieIndexReader : public UserDefinedIndexReader {
 public:
  explicit TrieIndexReader(const Comparator* comparator);
  ~TrieIndexReader() override = default;

  // Initialize from serialized index data. The data must remain valid for
  // the lifetime of this reader (it's typically a block cache entry).
  Status InitFromSlice(const Slice& data);

  // Create a new iterator for scanning.
  std::unique_ptr<UserDefinedIndexIterator> NewIterator(
      const ReadOptions& read_options) override;

  // Approximate memory usage of the deserialized trie.
  size_t ApproximateMemoryUsage() const override;

 private:
  const Comparator* comparator_;
  LoudsTrie trie_;
  size_t data_size_;  // Size of the raw serialized data.
};

// ============================================================================
// TrieIndexFactory: Implements UserDefinedIndexFactory.
//
// Factory for creating TrieIndexBuilder (during SST file writes) and
// TrieIndexReader (during SST file reads). Registered as a Customizable
// with name "trie_index".
// ============================================================================
class TrieIndexFactory : public UserDefinedIndexFactory {
 public:
  TrieIndexFactory() = default;
  ~TrieIndexFactory() override = default;

  static const char* kClassName() { return "trie_index"; }
  const char* Name() const override { return kClassName(); }

  // Deprecated API (required by base class). Use the overloads that accept
  // UserDefinedIndexOption instead. These must never be called; the new
  // overloads with UserDefinedIndexOption are always used by the block-based
  // table builder/reader. Abort unconditionally (in both debug and release
  // builds) to surface programming errors immediately.
  UserDefinedIndexBuilder* NewBuilder() const override {
    abort();
    return nullptr;
  }
  std::unique_ptr<UserDefinedIndexReader> NewReader(
      Slice& /*index_block*/) const override {
    abort();
    return nullptr;
  }

  // New API with comparator.
  Status NewBuilder(
      const UserDefinedIndexOption& option,
      std::unique_ptr<UserDefinedIndexBuilder>& builder) const override;

  Status NewReader(
      const UserDefinedIndexOption& option, Slice& index_block,
      std::unique_ptr<UserDefinedIndexReader>& reader) const override;
};

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
