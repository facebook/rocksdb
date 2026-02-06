//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/trie_index/trie_index_factory.h"

#include <algorithm>
#include <cassert>

#include "rocksdb/comparator.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// ============================================================================
// TrieIndexBuilder
// ============================================================================

TrieIndexBuilder::TrieIndexBuilder(const Comparator* comparator)
    : comparator_(comparator), finished_(false) {}

Slice TrieIndexBuilder::AddIndexEntry(const Slice& last_key_in_current_block,
                                      const Slice* first_key_in_next_block,
                                      const BlockHandle& block_handle,
                                      std::string* separator_scratch) {
  // Compute the shortest separator between the two keys using the comparator.
  // FindShortestSeparator takes `*start` as both input and output:
  //   input:  *start == last_key_in_current_block
  //   output: *start modified to shortest string in [start, limit)
  // If first_key_in_next_block is nullptr, this is the last block — use a
  // short successor of the last key.
  Slice separator;
  if (first_key_in_next_block != nullptr) {
    *separator_scratch = last_key_in_current_block.ToString();
    comparator_->FindShortestSeparator(separator_scratch,
                                       *first_key_in_next_block);
    separator = Slice(*separator_scratch);
  } else {
    // Last block: use a short successor of the last key.
    *separator_scratch = last_key_in_current_block.ToString();
    comparator_->FindShortSuccessor(separator_scratch);
    separator = Slice(*separator_scratch);
  }

  // Add the separator key and block handle to the trie.
  TrieBlockHandle handle;
  handle.offset = block_handle.offset;
  handle.size = block_handle.size;
  trie_builder_.AddKey(separator, handle);

  return separator;
}

void TrieIndexBuilder::OnKeyAdded(const Slice& /*key*/, ValueType /*type*/,
                                  const Slice& /*value*/) {
  // No-op: the trie is built from separator keys in AddIndexEntry(), not
  // from individual key-value pairs.
}

Status TrieIndexBuilder::Finish(Slice* index_contents) {
  if (finished_) {
    return Status::InvalidArgument("TrieIndexBuilder::Finish called twice");
  }
  finished_ = true;

  if (trie_builder_.NumKeys() == 0) {
    // No data blocks were added — return empty contents.
    *index_contents = Slice();
    return Status::OK();
  }

  trie_builder_.Finish();
  *index_contents = trie_builder_.GetSerializedData();
  return Status::OK();
}

// ============================================================================
// TrieIndexIterator
// ============================================================================

TrieIndexIterator::TrieIndexIterator(const LoudsTrie* trie,
                                     const Comparator* comparator)
    : comparator_(comparator),
      iter_(trie),
      current_scan_idx_(0),
      prepared_(false) {}

void TrieIndexIterator::Prepare(const ScanOptions scan_opts[],
                                size_t num_opts) {
  scan_opts_.clear();
  scan_opts_.reserve(num_opts);
  for (size_t i = 0; i < num_opts; i++) {
    scan_opts_.push_back(scan_opts[i]);
  }
  current_scan_idx_ = 0;
  prepared_ = true;
}

Status TrieIndexIterator::SeekAndGetResult(const Slice& target,
                                           IterateResult* result) {
  if (!iter_.Seek(target)) {
    result->bound_check_result = IterBoundCheck::kOutOfBound;
    result->key = Slice();
    return Status::OK();
  }

  // The iterator is positioned on a valid leaf. Set the result key.
  result->key = iter_.Key();
  current_key_scratch_ = result->key.ToString();
  result->key = Slice(current_key_scratch_);

  // Check bounds.
  result->bound_check_result = CheckBounds();
  return Status::OK();
}

Status TrieIndexIterator::NextAndGetResult(IterateResult* result) {
  if (!iter_.Next()) {
    result->bound_check_result = IterBoundCheck::kOutOfBound;
    result->key = Slice();
    return Status::OK();
  }

  result->key = iter_.Key();
  current_key_scratch_ = result->key.ToString();
  result->key = Slice(current_key_scratch_);

  result->bound_check_result = CheckBounds();
  return Status::OK();
}

UserDefinedIndexBuilder::BlockHandle TrieIndexIterator::value() {
  auto handle = iter_.Value();
  return UserDefinedIndexBuilder::BlockHandle{handle.offset, handle.size};
}

IterBoundCheck TrieIndexIterator::CheckBounds() const {
  if (!prepared_ || scan_opts_.empty()) {
    // No bounds to check — always in-bound.
    return IterBoundCheck::kInbound;
  }

  if (current_scan_idx_ >= scan_opts_.size()) {
    return IterBoundCheck::kOutOfBound;
  }

  const auto& opts = scan_opts_[current_scan_idx_];

  // Check upper bound (limit).
  if (opts.range.limit.has_value()) {
    const Slice& limit = opts.range.limit.value();
    if (comparator_->Compare(Slice(current_key_scratch_), limit) >= 0) {
      return IterBoundCheck::kOutOfBound;
    }
  }

  return IterBoundCheck::kInbound;
}

// ============================================================================
// TrieIndexReader
// ============================================================================

TrieIndexReader::TrieIndexReader(const Comparator* comparator)
    : comparator_(comparator), data_size_(0) {}

Status TrieIndexReader::InitFromSlice(const Slice& data) {
  data_size_ = data.size();
  return trie_.InitFromData(data);
}

std::unique_ptr<UserDefinedIndexIterator> TrieIndexReader::NewIterator(
    const ReadOptions& /*read_options*/) {
  return std::make_unique<TrieIndexIterator>(&trie_, comparator_);
}

size_t TrieIndexReader::ApproximateMemoryUsage() const {
  // The deserialized trie holds the handles vector plus the bitvector data.
  // For a rough estimate, use the serialized data size plus the decoded
  // handles vector.
  return data_size_ + trie_.NumKeys() * sizeof(TrieBlockHandle);
}

// ============================================================================
// TrieIndexFactory
// ============================================================================

Status TrieIndexFactory::NewBuilder(
    const UserDefinedIndexOption& option,
    std::unique_ptr<UserDefinedIndexBuilder>& builder) const {
  builder = std::make_unique<TrieIndexBuilder>(option.comparator);
  return Status::OK();
}

Status TrieIndexFactory::NewReader(
    const UserDefinedIndexOption& option, Slice& index_block,
    std::unique_ptr<UserDefinedIndexReader>& reader) const {
  auto trie_reader = std::make_unique<TrieIndexReader>(option.comparator);
  Status s = trie_reader->InitFromSlice(index_block);
  if (!s.ok()) {
    return s;
  }
  reader = std::move(trie_reader);
  return Status::OK();
}

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
