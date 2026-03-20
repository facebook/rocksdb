//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/trie_index/trie_index_factory.h"

#include <algorithm>
#include <cassert>
#include <cstring>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// ============================================================================
// TrieIndexBuilder
// ============================================================================

TrieIndexBuilder::TrieIndexBuilder(const Comparator* comparator)
    : comparator_(comparator),
      finished_(false),
      must_use_separator_with_seq_(false) {}

Slice TrieIndexBuilder::AddIndexEntry(const Slice& last_key_in_current_block,
                                      const Slice* first_key_in_next_block,
                                      const BlockHandle& block_handle,
                                      std::string* separator_scratch,
                                      const IndexEntryContext& context) {
  SequenceNumber last_key_seq = context.last_key_seq;

  // Compute a short separator between the two user keys using the
  // comparator. FindShortestSeparator takes `*start` as both input and output:
  //   input:  *start == last_key_in_current_block
  //   output: *start modified to shortest string in [start, limit)
  // If first_key_in_next_block is nullptr, this is the last block — use a
  // short successor of the last key.
  Slice separator;
  // True when last_key and first_key_in_next_block are the same user key
  // (same-user-key block boundary). Computed once and reused below for
  // both the sticky flag and the per-entry seqno decision.
  bool same_user_key = false;
  if (first_key_in_next_block != nullptr) {
    same_user_key = comparator_->Compare(last_key_in_current_block,
                                         *first_key_in_next_block) == 0;

    *separator_scratch = last_key_in_current_block.ToString();
    comparator_->FindShortestSeparator(separator_scratch,
                                       *first_key_in_next_block);
    separator = Slice(*separator_scratch);

    // Detect same-user-key block boundary: if the two user keys are identical,
    // FindShortestSeparator returns the same key for both sides, making it
    // impossible to distinguish the two blocks. Set the sticky flag so that
    // at Finish() time, ALL separators will include encoded seqnos.
    // This mirrors ShortenedIndexBuilder::must_use_separator_with_seq_.
    if (!must_use_separator_with_seq_ && same_user_key) {
      must_use_separator_with_seq_ = true;
    }

    // Edge case: FindShortestSeparator may fail to shorten the key even when
    // the user keys are different. Example: FindShortestSeparator("abc","abd")
    // returns "abc" unchanged because incrementing 'c' would yield "abd" which
    // is not < limit. When the resulting separator matches the previous entry's
    // separator, the blocks will be grouped into the same run in Finish().
    // We must mark this as a same-user-key boundary so it gets a real seqno
    // rather than kMaxSequenceNumber (which would trigger the overflow block
    // assertion in Finish()).
    if (!same_user_key && !buffered_entries_.empty() &&
        buffered_entries_.back().separator_key == *separator_scratch) {
      same_user_key = true;
      if (!must_use_separator_with_seq_) {
        must_use_separator_with_seq_ = true;
      }
    }
  } else {
    // Last block: use the last key itself as the separator, NOT a shortened
    // successor. This matches the standard ShortenedIndexBuilder behavior
    // (see index_builder.h GetSeparatorWithSeq lines 278-286): it only calls
    // FindShortInternalKeySuccessor when shortening_mode is
    // kShortenSeparatorsAndSuccessor, which is not the default. With the
    // default kShortenSeparators, the last block's separator is simply
    // last_key_in_current_block.
    //
    // Why this matters: FindShortSuccessor can widen the key range. For
    // example, if the actual last key is "9\xff\xff", FindShortSuccessor
    // produces ":" (0x3A). The trie would then claim to cover keys up to
    // ":", but the data block only contains keys up to "9\xff\xff". A seek
    // targeting a key in that gap (e.g., "9\xff\xff\x01") would find a
    // block via the trie that contains no matching data, causing iterator
    // desynchronization — the trie index returns a valid block while the
    // standard index correctly reports no match.
    separator = last_key_in_current_block;

    // Edge case: if this last block's separator matches the previous entry's
    // separator, they share the same user key (same-user-key run boundary).
    if (!buffered_entries_.empty() &&
        comparator_->Compare(buffered_entries_.back().separator_key,
                             separator) == 0) {
      same_user_key = true;
      if (!must_use_separator_with_seq_) {
        must_use_separator_with_seq_ = true;
      }
    }
  }

  // Buffer the entry for deferred trie construction in Finish().
  // We buffer rather than adding to the trie immediately because the
  // all-or-nothing seqno encoding decision is made at Finish() time.
  TrieBlockHandle handle;
  handle.offset = block_handle.offset;
  handle.size = block_handle.size;

  BufferedEntry entry;
  entry.separator_key = separator.ToString();
  // For same-user-key boundaries, use the actual seqno of the last key.
  // For different-user-key boundaries, use kMaxSequenceNumber (sentinel
  // meaning "this is not a same-key boundary, never advance past it").
  if (same_user_key) {
    entry.seqno = last_key_seq;
  } else {
    entry.seqno = kMaxSequenceNumber;
  }
  entry.handle = handle;
  buffered_entries_.push_back(std::move(entry));

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

  // Use seqno side-table when any same-user-key block boundary was detected.
  // The must_use_separator_with_seq_ flag is set in AddIndexEntry() whenever
  // the comparator finds two identical user keys at a block boundary. This
  // always implies duplicate separators exist (since
  // FindShortestSeparator("foo", "foo") = "foo"), so no separate scan is
  // needed.
  bool use_seqno = must_use_separator_with_seq_;
  trie_builder_.SetHasSeqnoEncoding(use_seqno);

  if (use_seqno) {
    // Feed de-duplicated separators to the trie with seqno side-table metadata.
    // Consecutive identical separators form a "run" — only the first occurrence
    // goes into the trie (as the primary block). The remaining blocks in the
    // run are stored as overflow blocks in the side-table.
    //
    // For non-boundary separators (different user keys), seqno is set to 0
    // (sentinel = "never advance past this leaf"). kMaxSequenceNumber from
    // AddIndexEntry is mapped to 0 here.
    size_t i = 0;
    while (i < buffered_entries_.size()) {
      const auto& entry = buffered_entries_[i];

      // Count how many consecutive entries share this separator key.
      size_t run_start = i;
      size_t run_end = i + 1;
      while (run_end < buffered_entries_.size() &&
             buffered_entries_[run_end].separator_key == entry.separator_key) {
        run_end++;
      }
      uint32_t block_count = static_cast<uint32_t>(run_end - run_start);

      // Map kMaxSequenceNumber (non-same-key boundary) to 0 (sentinel).
      uint64_t seqno = (entry.seqno == kMaxSequenceNumber) ? 0 : entry.seqno;

      // Add the primary (first) block for this separator.
      trie_builder_.AddKeyWithSeqno(Slice(entry.separator_key), entry.handle,
                                    seqno, block_count);

      // Add overflow blocks (2nd, 3rd, ... in the run).
      // Overflow blocks only exist within same-key runs, so their seqnos
      // come from last_key_seq in AddIndexEntry (never kMaxSequenceNumber).
      // The seqno may be 0 when bottommost compaction zeroes all sequence
      // numbers — this is valid; see AddOverflowBlock comment.
      for (size_t j = run_start + 1; j < run_end; j++) {
        assert(buffered_entries_[j].seqno != kMaxSequenceNumber);
        trie_builder_.AddOverflowBlock(buffered_entries_[j].handle,
                                       buffered_entries_[j].seqno);
      }

      i = run_end;
    }
  } else {
    // Common case: no same-user-key boundaries, add separators directly.
    // Zero overhead — no seqno data stored.
    for (const auto& entry : buffered_entries_) {
      trie_builder_.AddKey(Slice(entry.separator_key), entry.handle);
    }
  }

  // Release buffered entries — no longer needed after feeding to the trie.
  buffered_entries_.clear();
  buffered_entries_.shrink_to_fit();

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
                                     const Comparator* comparator,
                                     bool has_seqno_encoding)
    : comparator_(comparator),
      iter_(trie),
      trie_(trie),
      current_scan_idx_(0),
      prepared_(false),
      has_seqno_encoding_(has_seqno_encoding),
      overflow_run_index_(0),
      overflow_run_size_(1),
      overflow_base_idx_(0) {}

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

Status TrieIndexIterator::SeekToFirstAndGetResult(IterateResult* result) {
  // Reset overflow state — SeekToFirst always lands on the primary block
  // of the first trie leaf.
  overflow_run_index_ = 0;
  overflow_run_size_ = 1;
  overflow_base_idx_ = 0;

  if (!iter_.SeekToFirst()) {
    result->bound_check_result = IterBoundCheck::kUnknown;
    result->key = Slice();
    return Status::OK();
  }

  result->key = iter_.Key();
  current_key_scratch_ = result->key.ToString();
  result->key = Slice(current_key_scratch_);

  // Set up overflow state for the first leaf if seqno encoding is active.
  if (has_seqno_encoding_) {
    uint64_t leaf_idx = iter_.LeafIndex();
    uint32_t block_count = trie_->GetLeafBlockCount(leaf_idx);
    overflow_run_size_ = block_count;
    overflow_base_idx_ = trie_->GetOverflowBase(leaf_idx);
  }

  // The very first entry is always in bounds (no target to compare against
  // the limit, and the first block cannot precede any scan range).
  result->bound_check_result = IterBoundCheck::kInbound;
  return Status::OK();
}

Status TrieIndexIterator::SeekAndGetResult(const Slice& target,
                                           IterateResult* result,
                                           const SeekContext& context) {
  SequenceNumber target_seq = context.target_seq;

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

  // Reset overflow state.
  overflow_run_index_ = 0;
  overflow_run_size_ = 1;
  overflow_base_idx_ = 0;

  // Always seek with user key only — the trie stores user-key separators.
  // When seqno encoding is active, post-seek correction handles the seqno.
  if (!iter_.Seek(target)) {
    // No leaf has a key >= target: the target is past all blocks in this SST.
    // Return kUnknown (not kOutOfBound) because exhausting this SST's trie
    // says nothing about the upper bound — the next SST on the level may
    // still contain in-bound keys. kOutOfBound would cause LevelIterator to
    // stop scanning the level prematurely.
    result->bound_check_result = IterBoundCheck::kUnknown;
    result->key = Slice();
    return Status::OK();
  }

  // Set the result key (always a user key, no suffix stripping needed).
  result->key = iter_.Key();
  current_key_scratch_ = result->key.ToString();
  result->key = Slice(current_key_scratch_);

  // ---- Post-seek correction for seqno side-table ----
  //
  // When has_seqno_encoding_ is true, the leaf we landed on may have a
  // seqno side-table entry. We use it to determine if this is the right
  // block for the given (target, target_seq).
  //
  // The trie stores separators that are upper bounds on block contents:
  //   separator_key >= all keys in the block
  //   separator_seqno = seqno of the last key written to the block
  //
  // For same-user-key boundaries, the separator IS the user key. The seqno
  // determines which block within a run of same-key blocks is correct:
  //   - If target_seq >= leaf_seqno: this is the right block (target's
  //     internal key <= separator's internal key, because higher seqno means
  //     "smaller" internal key for the same user key)
  //   - If target_seq < leaf_seqno: target's internal key > separator,
  //     so we need to advance to the next block in the run
  //
  // For non-boundary leaves (leaf_seqno == 0), the `leaf_seqno != 0` guard
  // short-circuits before the comparison, so we never advance. This is the
  // zero-overhead common path.
  if (has_seqno_encoding_ && iter_.Valid()) {
    uint64_t leaf_idx = iter_.LeafIndex();
    uint64_t leaf_seqno = trie_->GetLeafSeqno(leaf_idx);

    if (leaf_seqno != 0 && target_seq < leaf_seqno) {
      // Target's internal key is AFTER the separator (lower seqno = later
      // in internal key order for same user key). Advance through overflow
      // blocks.
      uint32_t block_count = trie_->GetLeafBlockCount(leaf_idx);
      uint32_t base = trie_->GetOverflowBase(leaf_idx);

      bool found = false;
      for (uint32_t oi = 0; oi < block_count - 1; oi++) {
        uint64_t ov_seqno = trie_->GetOverflowSeqno(base + oi);
        if (ov_seqno == 0 || target_seq >= ov_seqno) {
          // This overflow block is the right one.
          overflow_run_index_ = oi + 1;  // 1-based (0 = primary)
          overflow_run_size_ = block_count;
          overflow_base_idx_ = base;
          found = true;
          break;
        }
      }

      if (!found) {
        // target_seq is below all seqnos in this run. Advance to the next
        // trie leaf (the block after the run).
        if (!iter_.Next()) {
          // Exhausted all blocks: target is past the end of this SST.
          // Return kUnknown — see comment in Seek path above.
          result->bound_check_result = IterBoundCheck::kUnknown;
          result->key = Slice();
          return Status::OK();
        }
        // Update key and overflow state for the new leaf.
        result->key = iter_.Key();
        current_key_scratch_ = result->key.ToString();
        result->key = Slice(current_key_scratch_);
        overflow_run_index_ = 0;
        overflow_run_size_ = 1;
        overflow_base_idx_ = 0;
        // Check if the new leaf also has overflow (unlikely but possible
        // with adjacent same-key runs for different user keys).
        // iter_.Valid() is guaranteed here — Next() returned true above.
        if (has_seqno_encoding_) {
          uint64_t new_leaf = iter_.LeafIndex();
          overflow_run_size_ = trie_->GetLeafBlockCount(new_leaf);
          overflow_base_idx_ = trie_->GetOverflowBase(new_leaf);
        }
      }
    } else {
      // Right block (common path). Set overflow state in case this leaf
      // has a run (for subsequent Next() calls).
      uint32_t block_count = trie_->GetLeafBlockCount(leaf_idx);
      overflow_run_index_ = 0;
      overflow_run_size_ = block_count;
      overflow_base_idx_ = trie_->GetOverflowBase(leaf_idx);
    }
  }

  result->bound_check_result = CheckBounds(target);
  return Status::OK();
}

Status TrieIndexIterator::NextAndGetResult(IterateResult* result) {
  // Save the current separator (user key) as "previous" before advancing.
  prev_key_scratch_ = current_key_scratch_;

  // If we're in an overflow run and haven't exhausted it, advance within
  // the run (no trie traversal needed — just increment the overflow index).
  if (overflow_run_index_ + 1 < overflow_run_size_) {
    overflow_run_index_++;
    // The key doesn't change (same separator for all blocks in the run).
    result->key = Slice(current_key_scratch_);
    result->bound_check_result = CheckBounds(Slice(prev_key_scratch_));
    return Status::OK();
  }

  // Advance to the next trie leaf.
  overflow_run_index_ = 0;
  overflow_run_size_ = 1;
  overflow_base_idx_ = 0;

  if (!iter_.Next()) {
    // No more blocks: past the end of this SST.
    // Return kUnknown — see comment in Seek path above.
    result->bound_check_result = IterBoundCheck::kUnknown;
    result->key = Slice();
    return Status::OK();
  }

  result->key = iter_.Key();
  current_key_scratch_ = result->key.ToString();
  result->key = Slice(current_key_scratch_);

  // Set overflow state for the new leaf.
  if (has_seqno_encoding_ && iter_.Valid()) {
    uint64_t leaf_idx = iter_.LeafIndex();
    overflow_run_size_ = trie_->GetLeafBlockCount(leaf_idx);
    overflow_base_idx_ = trie_->GetOverflowBase(leaf_idx);
  }

  result->bound_check_result = CheckBounds(Slice(prev_key_scratch_));
  return Status::OK();
}

UserDefinedIndexBuilder::BlockHandle TrieIndexIterator::value() {
  if (overflow_run_index_ == 0) {
    // Primary block — use the trie leaf's handle.
    auto handle = iter_.Value();
    return UserDefinedIndexBuilder::BlockHandle{handle.offset, handle.size};
  }
  // Overflow block — use the side-table handle.
  // overflow_run_index_ is 1-based, overflow array is 0-based.
  uint32_t overflow_idx = overflow_base_idx_ + overflow_run_index_ - 1;
  auto handle = trie_->GetOverflowHandle(overflow_idx);
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
  return std::make_unique<TrieIndexIterator>(&trie_, comparator_,
                                             trie_.HasSeqnoEncoding());
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
  // Default to BytewiseComparator when null. The trie requires a bytewise
  // comparator for separator key ordering; null would cause a dereference
  // crash in AddIndexEntry when comparing keys.
  const Comparator* cmp =
      option.comparator ? option.comparator : BytewiseComparator();
  builder = std::make_unique<TrieIndexBuilder>(cmp);
  return Status::OK();
}

Status TrieIndexFactory::NewReader(
    const UserDefinedIndexOption& option, Slice& index_block,
    std::unique_ptr<UserDefinedIndexReader>& reader) const {
  const Comparator* cmp =
      option.comparator ? option.comparator : BytewiseComparator();
  if (cmp != BytewiseComparator()) {
    return Status::NotSupported(
        "TrieIndexFactory requires BytewiseComparator; got: ", cmp->Name());
  }
  auto trie_reader = std::make_unique<TrieIndexReader>(cmp);
  Status s = trie_reader->InitFromSlice(index_block);
  if (!s.ok()) {
    return s;
  }
  reader = std::move(trie_reader);
  return Status::OK();
}

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
