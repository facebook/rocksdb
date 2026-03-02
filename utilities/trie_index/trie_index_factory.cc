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

  // Always finish the trie builder, even with 0 keys — this produces a valid
  // serialized trie that can be parsed by NewReader. Without this, an empty
  // Slice would be returned, causing InitFromData to fail with "data too short
  // for header".
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
      has_prev_key_(false),
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
  has_prev_key_ = false;
  prepared_ = true;
}

Status TrieIndexIterator::SeekAndGetResult(const Slice& target,
                                           IterateResult* result) {
  // Advance current_scan_idx_ past any scans whose limit <= target.
  // This handles the multi-scan case where the caller seeks into a later
  // scan range after the previous scan returned kOutOfBound.
  if (prepared_) {
    while (current_scan_idx_ < scan_opts_.size()) {
      const auto& opts = scan_opts_[current_scan_idx_];
      if (opts.range.limit.has_value() &&
          comparator_->Compare(target, opts.range.limit.value()) >= 0) {
        current_scan_idx_++;
      } else {
        break;
      }
    }
  }

  has_prev_key_ = false;

  if (!iter_.Seek(target)) {
    // Use kUnknown rather than kOutOfBound: we cannot be certain that all
    // keys in this file exceed the upper bound. The next SST file in the
    // level may still contain keys within the scan range.
    result->bound_check_result = IterBoundCheck::kUnknown;
    result->key = Slice();
    return Status::OK();
  }

  // The iterator is positioned on a valid leaf. Set the result key.
  result->key = iter_.Key();
  current_key_scratch_ = result->key.ToString();
  result->key = Slice(current_key_scratch_);

  // Use the seek target as the reference key for bounds checking.
  // The trie stores separator keys (upper bounds on block contents), not
  // first-in-block keys. If target < limit, the block may contain keys
  // within bounds even if the separator >= limit.
  result->bound_check_result = CheckBounds(target);
  return Status::OK();
}

Status TrieIndexIterator::NextAndGetResult(IterateResult* result) {
  // Save the current separator as "previous" before advancing.
  // Used as the reference key for bounds checking: if prev_sep >= limit,
  // all keys in the next block are >= prev_sep >= limit → out of bounds.
  prev_key_scratch_ = current_key_scratch_;
  has_prev_key_ = true;

  if (!iter_.Next()) {
    // Use kUnknown rather than kOutOfBound: we cannot be certain that all
    // keys in this file exceed the upper bound. The next SST file in the
    // level may still contain keys within the scan range.
    result->bound_check_result = IterBoundCheck::kUnknown;
    result->key = Slice();
    return Status::OK();
  }

  result->key = iter_.Key();
  current_key_scratch_ = result->key.ToString();
  result->key = Slice(current_key_scratch_);

  // Use the previous separator as the reference key. If prev_sep < limit,
  // the current block may contain keys within bounds (conservative).
  result->bound_check_result = CheckBounds(Slice(prev_key_scratch_));
  return Status::OK();
}

UserDefinedIndexBuilder::BlockHandle TrieIndexIterator::value() {
  auto handle = iter_.Value();
  return UserDefinedIndexBuilder::BlockHandle{handle.offset, handle.size};
}

IterBoundCheck TrieIndexIterator::CheckBounds(
    const Slice& reference_key) const {
  if (!prepared_ || scan_opts_.empty()) {
    // No bounds to check — always in-bound.
    return IterBoundCheck::kInbound;
  }

  if (current_scan_idx_ >= scan_opts_.size()) {
    return IterBoundCheck::kOutOfBound;
  }

  const auto& opts = scan_opts_[current_scan_idx_];

  // Check upper bound (limit) against the reference key, NOT the current
  // separator. The trie stores separator keys (upper bounds on block
  // contents), so comparing the separator against the limit would
  // prematurely reject blocks that contain keys < limit.
  //
  // For Seek: reference_key = seek target. If target < limit, the found
  //   block may contain keys within bounds.
  // For Next: reference_key = previous separator. If prev_sep < limit,
  //   the current block may contain keys within bounds.
  //
  // This is conservative: it may return kInbound for a block that is fully
  // out of bounds. The data-level iterator handles per-key filtering.
  if (opts.range.limit.has_value()) {
    const Slice& limit = opts.range.limit.value();
    if (comparator_->Compare(reference_key, limit) >= 0) {
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
  // The trie uses zero-copy pointers into the serialized data for bitvectors
  // and handle arrays, so the base cost is the serialized data size. On top
  // of that, InitFromData() heap-allocates child position lookup tables
  // (s_child_start_pos_ and s_child_end_pos_) for Select-free sparse
  // traversal — 8 bytes per sparse internal node.
  return data_size_ + trie_.ApproximateAuxMemoryUsage();
}

// ============================================================================
// TrieIndexFactory
// ============================================================================

Status TrieIndexFactory::NewBuilder(
    const UserDefinedIndexOption& option,
    std::unique_ptr<UserDefinedIndexBuilder>& builder) const {
  // The trie traverses keys byte-by-byte in lexicographic order, so it
  // requires a bytewise comparator. Non-bytewise comparators (e.g.,
  // ReverseBytewiseComparator or custom comparators) would produce separator
  // keys in a different order than the trie's byte-level traversal, causing
  // incorrect Seek results.
  if (option.comparator != nullptr &&
      option.comparator != BytewiseComparator()) {
    return Status::NotSupported(
        "TrieIndexFactory requires BytewiseComparator; got: ",
        option.comparator->Name());
  }
  builder = std::make_unique<TrieIndexBuilder>(option.comparator);
  return Status::OK();
}

Status TrieIndexFactory::NewReader(
    const UserDefinedIndexOption& option, Slice& index_block,
    std::unique_ptr<UserDefinedIndexReader>& reader) const {
  if (option.comparator != nullptr &&
      option.comparator != BytewiseComparator()) {
    return Status::NotSupported(
        "TrieIndexFactory requires BytewiseComparator; got: ",
        option.comparator->Name());
  }
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
