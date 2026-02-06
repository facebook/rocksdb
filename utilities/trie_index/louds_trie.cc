//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/trie_index/louds_trie.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <limits>
#include <utility>

#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

static constexpr uint32_t kTrieFormatVersion = 1;
static constexpr uint32_t kTrieMagic = 0x54524945;  // "TRIE" in ASCII

// ============================================================================
// LoudsTrieBuilder implementation
// ============================================================================

LoudsTrieBuilder::LoudsTrieBuilder()
    : cutoff_level_(0),
      max_depth_(0),
      dense_leaf_count_(0),
      sparse_leaf_count_(0),
      dense_node_count_(0),
      dense_child_count_(0) {}

void LoudsTrieBuilder::AddKey(const Slice& key, const TrieBlockHandle& handle) {
  keys_.emplace_back(key.data(), key.size());
  handles_.push_back(handle);
}

// Compute the optimal cutoff level between dense and sparse encoding.
// Merges the node-per-level and label-per-level computations into a single
// pass over the key set, avoiding the redundant LCP computation that would
// occur if they were computed separately.
uint32_t LoudsTrieBuilder::ComputeCutoffLevel() const {
  if (keys_.empty()) return 0;

  std::vector<uint64_t> nodes_per_level(max_depth_ + 1, 0);
  std::vector<uint64_t> labels_per_level(max_depth_ + 1, 0);
  nodes_per_level[0] = 1;

  for (size_t i = 0; i < keys_.size(); i++) {
    // Compute LCP with previous key (once per key pair).
    uint32_t lcp = 0;
    if (i > 0) {
      const auto& prev = keys_[i - 1];
      const auto& curr = keys_[i];
      uint32_t min_len =
          static_cast<uint32_t>(std::min(prev.size(), curr.size()));
      while (lcp < min_len && prev[lcp] == curr[lcp]) {
        lcp++;
      }
    }

    uint32_t key_len = static_cast<uint32_t>(keys_[i].size());

    // New nodes: levels (lcp+1) through key_len (same as before).
    for (uint32_t l = lcp + 1; l <= key_len && l <= max_depth_; l++) {
      nodes_per_level[l]++;
    }

    // Labels: a label exists at level l if the key has a character at l
    // and this label is distinct from the previous key's label (lcp <= l).
    // For the first key (i==0), all levels contribute a label.
    // Labels: count distinct labels per level. For sorted keys, a label at
    // level l is "new" only if l >= lcp (the shared prefix with the previous
    // key). For the first key, all levels contribute a new label.
    uint32_t label_start = (i == 0) ? 0 : lcp;
    for (uint32_t l = label_start; l < key_len && l <= max_depth_; l++) {
      labels_per_level[l]++;
    }
  }

  // Find first level where sparse encoding is cheaper than dense.
  for (uint32_t l = 0; l <= max_depth_; l++) {
    uint64_t n = nodes_per_level[l];
    uint64_t labels = labels_per_level[l];
    if (n == 0) return l;

    // Dense: 256 bits for d_labels_ + 1 bit for d_is_prefix_key_ per node,
    //        plus 1 bit for d_has_child_ per label.
    uint64_t dense_cost = n * 257 + labels;
    // Sparse: 8 bits (label byte) + 1 bit (s_has_child_) + 1 bit (s_louds_)
    //         per label, plus 1 bit for s_is_prefix_key_ per node.
    uint64_t sparse_cost = labels * 10 + n;

    if (sparse_cost < dense_cost) {
      return l;
    }
  }
  return max_depth_ + 1;
}

void LoudsTrieBuilder::Finish() {
  if (keys_.empty()) {
    cutoff_level_ = 0;
    max_depth_ = 0;
    dense_leaf_count_ = 0;
    sparse_leaf_count_ = 0;
    dense_node_count_ = 0;
    dense_child_count_ = 0;
    SerializeAll();
    return;
  }

  // Compute max depth.
  max_depth_ = 0;
  for (const auto& key : keys_) {
    max_depth_ = std::max(max_depth_, static_cast<uint32_t>(key.size()));
  }

  // Determine cutoff level.
  cutoff_level_ = ComputeCutoffLevel();
  cutoff_level_ = std::min(cutoff_level_, max_depth_ + 1);

  // =========================================================================
  // Build per-level trie data directly from sorted keys (streaming approach).
  //
  // Instead of constructing an explicit trie with per-node heap-allocated
  // children vectors and then BFS-encoding it, we infer the trie structure
  // directly from the sorted key sequence using LCP (longest common prefix)
  // analysis and build flat per-level arrays. This is the approach used by
  // the SuRF reference implementation (Zhang et al., SIGMOD 2018).
  //
  // Memory advantage: flat per-level vectors avoid the O(total_nodes) per-node
  // allocation overhead. For N keys with average depth D:
  //   - Explicit tree: ~48 bytes per node x N*D nodes = ~48*N*D bytes
  //   - Per-level: ~10 bytes per label + ~17 bytes per node, all in flat
  //     vectors with a single allocation per level.
  //
  // Algorithm: For each key, compute LCP with the previous key. The LCP
  // determines which levels share the trie path (existing nodes) and where
  // new branches start. At each level l >= lcp, the key contributes a new
  // label to the current (last) node at that level.
  //
  // Three key mechanisms handle deferred information:
  //   1. Deferred internal marking: when a label is first added, we mark it
  //      as a leaf (has_child=false). If a subsequent key continues the path
  //      through this label, we retroactively mark it as internal.
  //   2. Handle migration: when a leaf becomes internal (i.e., a prefix key
  //      emerges), the handle moves from the label's leaf_handle to the
  //      child node's prefix_handle.
  //   3. Lazy node creation: terminal nodes at depth K (where a key of
  //      length K ends) are created on-demand when retroactive marking
  //      needs them.
  // =========================================================================

  // Per-level data: flat arrays for labels, has_child, and handle tracking.
  // All labels within a level are concatenated; node boundaries are tracked
  // by node_label_start (the index where each node's labels begin).
  struct PerLevelData {
    // ---- Per-label data (all nodes concatenated) ----
    std::vector<uint8_t> labels;
    // Using uint8_t instead of bool to avoid std::vector<bool> proxy issues.
    std::vector<uint8_t> has_child;
    // Handle index for leaf labels (-1 if internal). When has_child[i] is
    // false, leaf_handle[i] is the key index whose handle should be emitted
    // for this leaf child.
    std::vector<int64_t> leaf_handle;

    // ---- Per-node data ----
    std::vector<uint8_t> is_prefix;
    // Handle index for prefix key nodes (-1 if not a prefix key).
    std::vector<int64_t> prefix_handle;
    // Index in labels[] where each node's labels start. Used to derive
    // louds bits and to iterate over nodes during LOUDS encoding.
    std::vector<uint64_t> node_label_start;

    uint64_t node_count() const {
      return static_cast<uint64_t>(is_prefix.size());
    }

    // Start a new node. Call this before adding labels to the node.
    void StartNode() {
      node_label_start.push_back(static_cast<uint64_t>(labels.size()));
      is_prefix.push_back(false);
      prefix_handle.push_back(-1);
    }

    // Add a label to the current (last) node.
    void AddLabel(uint8_t label, bool is_internal, int64_t handle) {
      labels.push_back(label);
      has_child.push_back(is_internal);
      leaf_handle.push_back(is_internal ? -1 : handle);
    }
  };

  std::vector<PerLevelData> levels(max_depth_ + 1);

  // Initialize root node at level 0.
  levels[0].StartNode();

  for (size_t ki = 0; ki < keys_.size(); ki++) {
    const auto& key = keys_[ki];
    uint32_t K = static_cast<uint32_t>(key.size());

    // Compute LCP with previous key.
    uint32_t lcp = 0;
    if (ki > 0) {
      const auto& prev = keys_[ki - 1];
      uint32_t min_len =
          static_cast<uint32_t>(std::min(prev.size(), key.size()));
      while (lcp < min_len && prev[lcp] == key[lcp]) {
        lcp++;
      }
    }

    // Step 1: Retroactive internal marking.
    //
    // If lcp > 0, the label at level lcp-1 (the deepest shared label) may
    // need to transition from leaf to internal. This happens when:
    //   - The previous key's path ended at or before level lcp (the label
    //     was originally a leaf), AND
    //   - The current key continues deeper through the same child.
    //
    // Since keys are sorted and lcp is the shared prefix length, the label
    // at level lcp-1 is always the LAST label in the current (last) node at
    // that level. We only need to mark this one label.
    if (lcp > 0) {
      auto& pl = levels[lcp - 1];
      if (!pl.has_child.empty() && !pl.has_child.back()) {
        // Leaf → internal transition.
        pl.has_child.back() = true;

        // Handle migration: move the leaf's handle to a NEW child node at
        // level lcp as a prefix key. The child node is always new because
        // this label just transitioned from leaf to internal — no child
        // node existed before for this branch.
        //
        // The leaf_handle must be valid (>= 0) because the label was marked
        // as a leaf in Step 2, which always sets the handle for leaf labels.
        assert(pl.leaf_handle.back() >= 0);
        int64_t h = pl.leaf_handle.back();
        pl.leaf_handle.back() = -1;

        if (lcp <= max_depth_) {
          levels[lcp].StartNode();
          levels[lcp].is_prefix.back() = true;
          levels[lcp].prefix_handle.back() = h;
        }
      }
    }

    // Step 2: Add labels for levels lcp..K-1.
    //
    // At level lcp: add a label to the existing (last) node.
    // At levels > lcp: create a new node and add its first label.
    for (uint32_t l = lcp; l < K; l++) {
      uint8_t label = static_cast<uint8_t>(key[l]);
      bool is_internal = (l + 1 < K);  // Key continues to next level.

      if (l == lcp) {
        // Adding to existing node. Verify it exists.
        assert(levels[l].node_count() > 0);
      } else {
        // New child node at this level.
        levels[l].StartNode();
      }

      // Associate handle with the last label if this is a leaf (key ends).
      int64_t handle = is_internal ? -1 : static_cast<int64_t>(ki);
      levels[l].AddLabel(label, is_internal, handle);
    }
  }

  // =========================================================================
  // Phase 2: Build LOUDS bitvectors and reorder handles from per-level data.
  //
  // The per-level data is already in BFS order (nodes at each level are
  // created left-to-right as sorted keys are processed). We iterate by
  // level and by node within each level:
  //   - For each node: emit prefix key handle (if any), then for each label:
  //     if leaf → emit handle; if internal → skip (child visited at next
  //     level).
  //   - Build dense/sparse bitvectors from labels and has_child flags.
  // =========================================================================

  d_labels_ = BitvectorBuilder();
  d_has_child_ = BitvectorBuilder();
  d_is_prefix_key_ = BitvectorBuilder();
  dense_node_count_ = 0;
  dense_child_count_ = 0;

  s_labels_.clear();
  s_has_child_ = BitvectorBuilder();
  s_louds_ = BitvectorBuilder();
  s_is_prefix_key_ = BitvectorBuilder();

  dense_leaf_count_ = 0;
  sparse_leaf_count_ = 0;

  std::vector<TrieBlockHandle> bfs_ordered_handles;
  bfs_ordered_handles.reserve(keys_.size());

  for (uint32_t level = 0; level <= max_depth_; level++) {
    const auto& ld = levels[level];
    if (ld.node_count() == 0) continue;

    bool is_dense = (level < cutoff_level_);

    for (uint64_t ni = 0; ni < ld.node_count(); ni++) {
      // Determine label range for this node.
      uint64_t label_start = ld.node_label_start[ni];
      uint64_t label_end = (ni + 1 < ld.node_count())
                               ? ld.node_label_start[ni + 1]
                               : static_cast<uint64_t>(ld.labels.size());

      // ---- Handle reordering: emit prefix key handle ----
      if (ld.is_prefix[ni] && ld.prefix_handle[ni] >= 0) {
        bfs_ordered_handles.push_back(
            handles_[static_cast<size_t>(ld.prefix_handle[ni])]);
      }

      // Skip pure leaf nodes (no labels) — they are accounted for by
      // has_child=0 in their parent. They don't produce LOUDS entries.
      if (label_start == label_end) {
        if (ld.is_prefix[ni]) {
          // Prefix key node with no children at THIS level (but it was
          // already counted above). This can happen for nodes created by
          // lazy creation that never gained children.
          if (is_dense) {
            dense_leaf_count_++;
          } else {
            sparse_leaf_count_++;
          }
        }
        continue;
      }

      if (is_dense) {
        // ---- Dense encoding ----
        dense_node_count_++;
        d_is_prefix_key_.Append(ld.is_prefix[ni]);

        // Build 256-bit label bitmap as 4 x 64-bit words.
        uint64_t bitmap[4] = {};
        for (uint64_t li = label_start; li < label_end; li++) {
          uint8_t label = ld.labels[li];
          bitmap[label / 64] |= uint64_t(1) << (label % 64);
        }
        for (int w = 0; w < 4; w++) {
          d_labels_.AppendWord(bitmap[w], 64);
        }

        // Emit has_child bits, count leaves, emit leaf handles.
        for (uint64_t li = label_start; li < label_end; li++) {
          bool is_internal = ld.has_child[li];
          d_has_child_.Append(is_internal);

          if (!is_internal) {
            if (ld.leaf_handle[li] >= 0) {
              bfs_ordered_handles.push_back(
                  handles_[static_cast<size_t>(ld.leaf_handle[li])]);
            }
            dense_leaf_count_++;
          } else if (level == cutoff_level_ - 1) {
            dense_child_count_++;
          }
        }

        if (ld.is_prefix[ni]) {
          dense_leaf_count_++;
        }
      } else {
        // ---- Sparse encoding ----
        s_is_prefix_key_.Append(ld.is_prefix[ni]);
        bool first_label = true;
        for (uint64_t li = label_start; li < label_end; li++) {
          bool is_internal = ld.has_child[li];

          s_labels_.push_back(ld.labels[li]);
          s_has_child_.Append(is_internal);
          s_louds_.Append(first_label);
          first_label = false;

          if (!is_internal) {
            if (ld.leaf_handle[li] >= 0) {
              bfs_ordered_handles.push_back(
                  handles_[static_cast<size_t>(ld.leaf_handle[li])]);
            }
            sparse_leaf_count_++;
          }
        }

        if (ld.is_prefix[ni]) {
          sparse_leaf_count_++;
        }
      }
    }
  }

  // When cutoff_level_ = 0, all nodes are sparse and the root is the sole
  // "root sparse node" (not a child of any dense level).
  if (cutoff_level_ == 0) {
    dense_child_count_ = 1;
  }

  assert(bfs_ordered_handles.size() == keys_.size());
  handles_ = std::move(bfs_ordered_handles);

  SerializeAll();
}

void LoudsTrieBuilder::SerializeAll() {
  serialized_data_.clear();

  PutFixed32(&serialized_data_, kTrieMagic);
  PutFixed32(&serialized_data_, kTrieFormatVersion);
  PutFixed64(&serialized_data_, keys_.size());
  PutFixed32(&serialized_data_, cutoff_level_);
  PutFixed32(&serialized_data_, max_depth_);
  PutFixed64(&serialized_data_, dense_leaf_count_);
  PutFixed64(&serialized_data_, sparse_leaf_count_);
  PutFixed64(&serialized_data_, dense_node_count_);
  PutFixed64(&serialized_data_, dense_child_count_);

  // Dense section.
  {
    Bitvector bv;
    bv.BuildFrom(d_labels_);
    bv.Serialize(&serialized_data_);
  }
  {
    Bitvector bv;
    bv.BuildFrom(d_has_child_);
    bv.Serialize(&serialized_data_);
  }
  {
    Bitvector bv;
    bv.BuildFrom(d_is_prefix_key_);
    bv.Serialize(&serialized_data_);
  }

  // Sparse section.
  uint64_t sl_size = s_labels_.size();
  PutFixed64(&serialized_data_, sl_size);
  if (sl_size > 0) {
    serialized_data_.append(reinterpret_cast<const char*>(s_labels_.data()),
                            sl_size);
    size_t padding = (8 - (sl_size % 8)) % 8;
    serialized_data_.append(padding, '\0');
  }
  {
    Bitvector bv;
    bv.BuildFrom(s_has_child_);
    bv.Serialize(&serialized_data_);
  }
  {
    Bitvector bv;
    bv.BuildFrom(s_louds_);
    bv.Serialize(&serialized_data_);
  }
  {
    Bitvector bv;
    bv.BuildFrom(s_is_prefix_key_);
    bv.Serialize(&serialized_data_);
  }

  // Block handles: delta-varint encoded.
  // Offsets are delta-encoded (each value = offset[i] - offset[i-1]),
  // sizes are stored directly. Both use varint64 encoding.
  // This exploits the fact that SST data blocks are sequential, so offset
  // deltas are small (~4KB), and sizes are typically similar across blocks.
  {
    uint64_t prev_offset = 0;
    for (const auto& h : handles_) {
      PutVarint64(&serialized_data_, h.offset - prev_offset);
      PutVarint64(&serialized_data_, h.size);
      prev_offset = h.offset;
    }
  }
}

// ============================================================================
// LoudsTrie implementation
// ============================================================================

LoudsTrie::LoudsTrie()
    : num_keys_(0),
      cutoff_level_(0),
      max_depth_(0),
      dense_leaf_count_(0),
      dense_node_count_(0),
      dense_child_count_(0),
      s_labels_data_(nullptr),
      s_labels_size_(0) {}

Status LoudsTrie::InitFromData(const Slice& data) {
  const char* p = data.data();
  size_t remaining = data.size();

  if (remaining < 56) {
    return Status::Corruption("Trie index: data too short for header");
  }

  uint32_t magic;
  memcpy(&magic, p, 4);
  p += 4;
  remaining -= 4;
  if (magic != kTrieMagic) {
    return Status::Corruption("Trie index: bad magic number");
  }

  uint32_t version;
  memcpy(&version, p, 4);
  p += 4;
  remaining -= 4;
  if (version != kTrieFormatVersion) {
    return Status::Corruption("Trie index: unsupported format version");
  }

  memcpy(&num_keys_, p, 8);
  p += 8;
  remaining -= 8;
  memcpy(&cutoff_level_, p, 4);
  p += 4;
  remaining -= 4;
  memcpy(&max_depth_, p, 4);
  p += 4;
  remaining -= 4;
  memcpy(&dense_leaf_count_, p, 8);
  p += 8;
  remaining -= 8;

  uint64_t sparse_leaf_count;
  memcpy(&sparse_leaf_count, p, 8);
  p += 8;
  remaining -= 8;
  memcpy(&dense_node_count_, p, 8);
  p += 8;
  remaining -= 8;
  memcpy(&dense_child_count_, p, 8);
  p += 8;
  remaining -= 8;

  // Dense bitvectors.
  size_t consumed;
  Status s;

  s = d_labels_.InitFromData(p, remaining, &consumed);
  if (!s.ok()) return s;
  p += consumed;
  remaining -= consumed;

  s = d_has_child_.InitFromData(p, remaining, &consumed);
  if (!s.ok()) return s;
  p += consumed;
  remaining -= consumed;

  s = d_is_prefix_key_.InitFromData(p, remaining, &consumed);
  if (!s.ok()) return s;
  p += consumed;
  remaining -= consumed;

  // Sparse section.
  if (remaining < 8) {
    return Status::Corruption("Trie index: truncated sparse labels size");
  }
  memcpy(&s_labels_size_, p, 8);
  p += 8;
  remaining -= 8;

  if (s_labels_size_ > 0) {
    if (remaining < s_labels_size_) {
      return Status::Corruption("Trie index: truncated sparse labels");
    }
    s_labels_data_ = reinterpret_cast<const uint8_t*>(p);
    p += s_labels_size_;
    remaining -= s_labels_size_;
    size_t padding = (8 - (s_labels_size_ % 8)) % 8;
    if (remaining < padding) {
      return Status::Corruption("Trie index: truncated sparse label padding");
    }
    p += padding;
    remaining -= padding;
  }

  s = s_has_child_.InitFromData(p, remaining, &consumed);
  if (!s.ok()) return s;
  p += consumed;
  remaining -= consumed;

  s = s_louds_.InitFromData(p, remaining, &consumed);
  if (!s.ok()) return s;
  p += consumed;
  remaining -= consumed;

  s = s_is_prefix_key_.InitFromData(p, remaining, &consumed);
  if (!s.ok()) return s;
  p += consumed;
  remaining -= consumed;

  // Block handles: delta-varint encoded.
  // Sanity check: each handle needs at least 2 bytes (two varint64 minimums).
  if (num_keys_ > 0 && remaining / 2 < num_keys_) {
    return Status::Corruption(
        "Trie index: num_keys too large for remaining data");
  }
  handles_.resize(num_keys_);
  Slice handles_slice(p, remaining);
  uint64_t prev_offset = 0;
  for (uint64_t i = 0; i < num_keys_; i++) {
    uint64_t delta, size;
    if (!GetVarint64(&handles_slice, &delta) ||
        !GetVarint64(&handles_slice, &size)) {
      return Status::Corruption("Trie index: truncated block handle");
    }
    handles_[i].offset = prev_offset + delta;
    handles_[i].size = size;
    prev_offset = handles_[i].offset;
  }

  return Status::OK();
}

TrieBlockHandle LoudsTrie::GetHandle(uint64_t leaf_index) const {
  assert(leaf_index < num_keys_);
  return handles_[leaf_index];
}

// ============================================================================
// LoudsTrieIterator implementation
//
// The iterator maintains a stack of positions (path_) from the root to the
// current leaf. Each entry records the position in the bitvector/label array
// at that depth, enabling backtracking for Next() and key reconstruction.
//
// Dense positions: pos = node_num * 256 + label_byte. The node_num is the
// global node number across all dense levels (concatenated). The label_byte
// is pos % 256.
//
// Sparse positions: pos = index into s_labels_ array.
//
// Leaf ordering: BFS leaf order matches sorted key order. For each node in
// BFS order, first its prefix key leaf (if any), then its child leaves
// (has_child = 0) in label order.
// ============================================================================

LoudsTrieIterator::LoudsTrieIterator(const LoudsTrie* trie)
    : trie_(trie), valid_(false), leaf_index_(0), is_at_prefix_key_(false) {
  // Pre-reserve capacity for current_key_ to avoid heap reallocations during
  // Seek/Next traversal. MaxDepth() is the length of the longest key, so
  // no trie path will exceed this depth.
  current_key_.reserve(trie_->MaxDepth());
}

// --- Dense helpers ---

bool LoudsTrieIterator::DenseSeekLabel(uint64_t node_num, uint8_t target_byte,
                                       uint64_t* out_pos) {
  uint64_t pos = node_num * 256 + target_byte;
  if (pos < trie_->d_labels_.NumBits() && trie_->d_labels_.GetBit(pos)) {
    *out_pos = pos;
    return true;
  }
  uint64_t node_end = (node_num + 1) * 256;
  uint64_t next = trie_->d_labels_.NextSetBit(pos + 1);
  if (next < node_end && next < trie_->d_labels_.NumBits()) {
    *out_pos = next;
    return false;
  }
  *out_pos = node_end;
  return false;
}

uint64_t LoudsTrieIterator::DenseChildNodeNum(uint64_t pos) const {
  uint64_t label_rank = trie_->d_labels_.Rank1(pos + 1) - 1;
  return DenseChildNodeNumFromRank(label_rank);
}

uint64_t LoudsTrieIterator::DenseChildNodeNumFromRank(
    uint64_t label_rank) const {
  // In the concatenated dense model, node 0 is the root (no parent).
  // Each internal child (d_has_child_ bit = 1) creates a new node numbered
  // 1, 2, 3, ... in BFS order. The child's global node number =
  // rank1(d_has_child_, label_rank + 1).
  return trie_->d_has_child_.Rank1(label_rank + 1);
}

uint64_t LoudsTrieIterator::DenseLeafIndex(uint64_t pos) const {
  uint64_t label_rank = trie_->d_labels_.Rank1(pos + 1) - 1;
  return DenseLeafIndexFromRank(pos, label_rank);
}

uint64_t LoudsTrieIterator::DenseLeafIndexFromRank(uint64_t pos,
                                                   uint64_t label_rank) const {
  // Leaf ordinal for a dense leaf at position `pos` in d_labels_.
  //
  // BFS leaf order: for each node 0..N, prefix key first, then non-internal
  // children. So leaf index =
  //   (prefix keys at nodes [0, node_num]) +
  //   (non-internal labels at positions [0, label_rank]) - 1 (0-indexed).
  uint64_t node_num = pos / 256;
  uint64_t leaf_labels =
      (label_rank + 1) - trie_->d_has_child_.Rank1(label_rank + 1);
  uint64_t prefix_keys = trie_->d_is_prefix_key_.Rank1(node_num + 1);
  return prefix_keys + leaf_labels - 1;
}

uint64_t LoudsTrieIterator::DensePrefixKeyLeafIndex(uint64_t node_num) const {
  // Leaf ordinal for a prefix key at dense node N. The prefix key comes
  // after all leaves at nodes [0, N-1] and before leaf labels at node N.
  // Index = (prefix keys before N) + (non-internal labels before N).
  uint64_t labels_before_node = trie_->d_labels_.Rank1(node_num * 256);
  uint64_t internal_before = trie_->d_has_child_.Rank1(labels_before_node);
  uint64_t leaf_labels_before = labels_before_node - internal_before;
  uint64_t prefix_keys_before = trie_->d_is_prefix_key_.Rank1(node_num);
  return prefix_keys_before + leaf_labels_before;
}

// --- Sparse helpers ---

bool LoudsTrieIterator::SparseSeekLabel(uint64_t node_start_pos,
                                        uint64_t node_end_pos,
                                        uint8_t target_byte,
                                        uint64_t* out_pos) {
  // Labels within a sparse node are stored in sorted order.
  // Use std::lower_bound (binary search) for O(log k) lookup where k is
  // the node fanout. This matches the SuRF reference implementation's
  // strategy for medium-sized nodes and is significantly faster than linear
  // scan for nodes at the dense/sparse boundary (which often have 20-50+
  // children).
  const uint8_t* base = trie_->s_labels_data_;
  const uint8_t* begin = base + node_start_pos;
  const uint8_t* end = base + node_end_pos;
  const uint8_t* it = std::lower_bound(begin, end, target_byte);

  if (it == end) {
    *out_pos = node_end_pos;
    return false;
  }
  *out_pos = static_cast<uint64_t>(it - base);
  return (*it == target_byte);
}

uint64_t LoudsTrieIterator::SparseChildNodeNum(uint64_t pos) const {
  // The first dense_child_count_ sparse nodes are children of the last
  // dense level. Additional sparse internal children add nodes after that.
  // child_node = dense_child_count_ + rank1(s_has_child_, pos+1) - 1
  return trie_->dense_child_count_ + trie_->s_has_child_.Rank1(pos + 1) - 1;
}

uint64_t LoudsTrieIterator::SparseLeafIndex(uint64_t pos) const {
  // Leaf ordinal for a sparse leaf at position pos.
  // = dense_leaf_count + prefix_keys_at_nodes[0..N] +
  // non-internal_labels[0..pos] - 1
  uint64_t sparse_node = SparseNodeNum(pos);
  uint64_t prefix_keys = trie_->s_is_prefix_key_.Rank1(sparse_node + 1);
  uint64_t internal = trie_->s_has_child_.Rank1(pos + 1);
  uint64_t leaf_labels = (pos + 1) - internal;
  return trie_->dense_leaf_count_ + prefix_keys + leaf_labels - 1;
}

uint64_t LoudsTrieIterator::SparsePrefixKeyLeafIndex(
    uint64_t sparse_node_num) const {
  // Leaf ordinal for a sparse prefix key. Same logic as DensePrefixKeyLeafIndex
  // but offset by dense_leaf_count_.
  uint64_t start_pos = SparseNodeStartPos(sparse_node_num);
  uint64_t prefix_keys_before = trie_->s_is_prefix_key_.Rank1(sparse_node_num);
  uint64_t internal_before = trie_->s_has_child_.Rank1(start_pos);
  uint64_t leaf_labels_before = start_pos - internal_before;
  return trie_->dense_leaf_count_ + prefix_keys_before + leaf_labels_before;
}

uint64_t LoudsTrieIterator::SparseNodeNum(uint64_t pos) const {
  return trie_->s_louds_.Rank1(pos + 1) - 1;
}

uint64_t LoudsTrieIterator::SparseNodeStartPos(uint64_t sparse_node_num) const {
  if (sparse_node_num == 0) return 0;
  return trie_->s_louds_.Select1(sparse_node_num);
}

uint64_t LoudsTrieIterator::SparseNodeEndPos(uint64_t start_pos) const {
  uint64_t next = trie_->s_louds_.NextSetBit(start_pos + 1);
  if (next >= trie_->s_louds_.NumBits()) {
    return trie_->s_labels_size_;
  }
  return next;
}

// Helper: descend from (in_dense, node_num) to leftmost leaf, pushing
// path entries and building current_key_. Returns true if a leaf was found.
bool LoudsTrieIterator::DescendToLeftmostLeaf(bool in_dense,
                                              uint64_t node_num) {
  while (true) {
    if (in_dense) {
      // Check prefix key first.
      if (trie_->d_is_prefix_key_.NumBits() > 0 &&
          node_num < trie_->d_is_prefix_key_.NumBits() &&
          trie_->d_is_prefix_key_.GetBit(node_num)) {
        is_at_prefix_key_ = true;
        leaf_index_ = DensePrefixKeyLeafIndex(node_num);
        valid_ = true;
        return true;
      }

      uint64_t base = node_num * 256;
      if (base >= trie_->d_labels_.NumBits()) {
        valid_ = false;
        return false;
      }
      uint64_t first = trie_->d_labels_.NextSetBit(base);
      if (first >= base + 256 || first >= trie_->d_labels_.NumBits()) {
        valid_ = false;
        return false;
      }

      path_.push_back(LevelPos::MakeDense(first));
      current_key_.push_back(static_cast<char>(first % 256));

      uint64_t label_rank = trie_->d_labels_.Rank1(first + 1) - 1;
      if (!trie_->d_has_child_.GetBit(label_rank)) {
        leaf_index_ = DenseLeafIndexFromRank(first, label_rank);
        valid_ = true;
        return true;
      }

      uint64_t child = DenseChildNodeNumFromRank(label_rank);
      if (child < trie_->dense_node_count_) {
        node_num = child;
        in_dense = true;
      } else {
        node_num = child - trie_->dense_node_count_;
        in_dense = false;
      }
    } else {
      // Check prefix key first.
      if (trie_->s_is_prefix_key_.NumBits() > 0 &&
          node_num < trie_->s_is_prefix_key_.NumBits() &&
          trie_->s_is_prefix_key_.GetBit(node_num)) {
        is_at_prefix_key_ = true;
        leaf_index_ = SparsePrefixKeyLeafIndex(node_num);
        valid_ = true;
        return true;
      }

      uint64_t start = SparseNodeStartPos(node_num);
      if (start >= trie_->s_labels_size_) {
        valid_ = false;
        return false;
      }

      path_.push_back(LevelPos::MakeSparse(start));
      current_key_.push_back(static_cast<char>(trie_->s_labels_data_[start]));

      if (!trie_->s_has_child_.GetBit(start)) {
        leaf_index_ = SparseLeafIndex(start);
        valid_ = true;
        return true;
      }

      node_num = SparseChildNodeNum(start);
      in_dense = false;
    }
  }
}

// Main Seek implementation.
bool LoudsTrieIterator::Seek(const Slice& target) {
  valid_ = false;
  leaf_index_ = 0;
  current_key_.clear();
  path_.clear();
  is_at_prefix_key_ = false;

  if (trie_->NumKeys() == 0) return false;

  bool in_dense = (trie_->cutoff_level_ > 0);
  uint64_t node_num = 0;

  assert(target.size() <= UINT32_MAX);
  for (uint32_t depth = 0; depth < static_cast<uint32_t>(target.size());
       depth++) {
    uint8_t target_byte = static_cast<uint8_t>(target[depth]);

    if (in_dense) {
      uint64_t pos;
      bool exact = DenseSeekLabel(node_num, target_byte, &pos);
      uint64_t node_end = (node_num + 1) * 256;

      if (pos >= node_end) {
        // No label >= target_byte. Backtrack.
        return Advance();
      }

      path_.push_back(LevelPos::MakeDense(pos));
      current_key_.push_back(static_cast<char>(pos % 256));

      // Cache label_rank: avoids redundant Rank1(d_labels_) in both
      // has_child check and DenseChildNodeNum/DenseLeafIndex.
      uint64_t label_rank = trie_->d_labels_.Rank1(pos + 1) - 1;
      bool is_internal = trie_->d_has_child_.GetBit(label_rank);

      if (!exact) {
        // Landed on label > target_byte. Go to leftmost leaf in subtree.
        if (!is_internal) {
          leaf_index_ = DenseLeafIndexFromRank(pos, label_rank);
          valid_ = true;
          return true;
        }
        uint64_t child = DenseChildNodeNumFromRank(label_rank);
        bool child_dense = (child < trie_->dense_node_count_);
        uint64_t cn = child_dense ? child : child - trie_->dense_node_count_;
        return DescendToLeftmostLeaf(child_dense, cn);
      }

      if (!is_internal) {
        // This label is a leaf. Check if target is fully consumed.
        if (depth == static_cast<uint32_t>(target.size()) - 1) {
          // Exact match: trie key == target.
          leaf_index_ = DenseLeafIndexFromRank(pos, label_rank);
          valid_ = true;
          return true;
        }
        // Target has more bytes, so trie key is a proper prefix of target.
        // Trie key < target. Advance to the NEXT leaf.
        leaf_index_ = DenseLeafIndexFromRank(pos, label_rank);
        valid_ = true;
        return Advance();
      }

      uint64_t child = DenseChildNodeNumFromRank(label_rank);
      if (child < trie_->dense_node_count_) {
        node_num = child;
        in_dense = true;
      } else {
        node_num = child - trie_->dense_node_count_;
        in_dense = false;
      }
    } else {
      uint64_t start = SparseNodeStartPos(node_num);
      uint64_t end = SparseNodeEndPos(start);

      uint64_t pos;
      bool exact = SparseSeekLabel(start, end, target_byte, &pos);

      if (pos >= end) {
        return Advance();
      }

      path_.push_back(LevelPos::MakeSparse(pos));
      current_key_.push_back(static_cast<char>(trie_->s_labels_data_[pos]));

      bool is_internal = trie_->s_has_child_.GetBit(pos);

      if (!exact) {
        if (!is_internal) {
          leaf_index_ = SparseLeafIndex(pos);
          valid_ = true;
          return true;
        }
        return DescendToLeftmostLeaf(false, SparseChildNodeNum(pos));
      }

      if (!is_internal) {
        // Check if target is fully consumed.
        if (depth == static_cast<uint32_t>(target.size()) - 1) {
          leaf_index_ = SparseLeafIndex(pos);
          valid_ = true;
          return true;
        }
        // Target has more bytes. Trie key < target. Advance.
        leaf_index_ = SparseLeafIndex(pos);
        valid_ = true;
        return Advance();
      }

      node_num = SparseChildNodeNum(pos);
      in_dense = false;
    }
  }

  // Target key fully consumed. Check if current node is a prefix key.
  if (in_dense) {
    if (trie_->d_is_prefix_key_.NumBits() > 0 &&
        node_num < trie_->d_is_prefix_key_.NumBits() &&
        trie_->d_is_prefix_key_.GetBit(node_num)) {
      is_at_prefix_key_ = true;
      leaf_index_ = DensePrefixKeyLeafIndex(node_num);
      valid_ = true;
      return true;
    }
  } else {
    if (trie_->s_is_prefix_key_.NumBits() > 0 &&
        node_num < trie_->s_is_prefix_key_.NumBits() &&
        trie_->s_is_prefix_key_.GetBit(node_num)) {
      is_at_prefix_key_ = true;
      leaf_index_ = SparsePrefixKeyLeafIndex(node_num);
      valid_ = true;
      return true;
    }
  }

  // Descend to leftmost leaf.
  return DescendToLeftmostLeaf(in_dense, node_num);
}

bool LoudsTrieIterator::Next() {
  if (!valid_) return false;

  if (is_at_prefix_key_) {
    is_at_prefix_key_ = false;
    // The prefix key is at a node that also has children. The next leaf
    // is the leftmost child leaf.
    bool in_dense;
    uint64_t node_num;
    if (path_.empty()) {
      // Root is the prefix key.
      in_dense = (trie_->cutoff_level_ > 0);
      node_num = 0;
    } else {
      // The last path entry is the label that leads TO this prefix key node.
      // The prefix key node IS the child of that label.
      auto last = path_.back();
      if (last.is_dense()) {
        // Compute label_rank once and reuse for child node lookup.
        uint64_t lr = trie_->d_labels_.Rank1(last.pos() + 1) - 1;
        uint64_t child = DenseChildNodeNumFromRank(lr);
        in_dense = (child < trie_->dense_node_count_);
        node_num = in_dense ? child : child - trie_->dense_node_count_;
      } else {
        node_num = SparseChildNodeNum(last.pos());
        in_dense = false;
      }
    }

    // Find leftmost child leaf (NOT checking prefix key again, since we
    // just came from it).
    if (in_dense) {
      uint64_t base = node_num * 256;
      if (base >= trie_->d_labels_.NumBits()) {
        return Advance();
      }
      uint64_t first = trie_->d_labels_.NextSetBit(base);
      if (first >= base + 256 || first >= trie_->d_labels_.NumBits()) {
        return Advance();
      }
      path_.push_back(LevelPos::MakeDense(first));
      current_key_.push_back(static_cast<char>(first % 256));

      uint64_t label_rank = trie_->d_labels_.Rank1(first + 1) - 1;
      if (!trie_->d_has_child_.GetBit(label_rank)) {
        leaf_index_ = DenseLeafIndexFromRank(first, label_rank);
        valid_ = true;
        return true;
      }
      uint64_t child = DenseChildNodeNumFromRank(label_rank);
      bool cd = (child < trie_->dense_node_count_);
      return DescendToLeftmostLeaf(
          cd, cd ? child : child - trie_->dense_node_count_);
    } else {
      uint64_t start = SparseNodeStartPos(node_num);
      if (start >= trie_->s_labels_size_) {
        return Advance();
      }
      path_.push_back(LevelPos::MakeSparse(start));
      current_key_.push_back(static_cast<char>(trie_->s_labels_data_[start]));

      if (!trie_->s_has_child_.GetBit(start)) {
        leaf_index_ = SparseLeafIndex(start);
        valid_ = true;
        return true;
      }
      return DescendToLeftmostLeaf(false, SparseChildNodeNum(start));
    }
  }

  return Advance();
}

bool LoudsTrieIterator::Advance() {
  // Backtrack up the path to find the next sibling, then descend to the
  // leftmost leaf in that subtree.
  while (!path_.empty()) {
    auto cur = path_.back();
    path_.pop_back();
    if (!current_key_.empty()) {
      current_key_.pop_back();
    }

    if (cur.is_dense()) {
      uint64_t cur_pos = cur.pos();
      uint64_t node_num = cur_pos / 256;
      uint64_t node_end = (node_num + 1) * 256;
      uint64_t next = trie_->d_labels_.NextSetBit(cur_pos + 1);

      if (next < node_end && next < trie_->d_labels_.NumBits()) {
        path_.push_back(LevelPos::MakeDense(next));
        current_key_.push_back(static_cast<char>(next % 256));

        uint64_t label_rank = trie_->d_labels_.Rank1(next + 1) - 1;
        if (!trie_->d_has_child_.GetBit(label_rank)) {
          leaf_index_ = DenseLeafIndexFromRank(next, label_rank);
          valid_ = true;
          return true;
        }

        uint64_t child = DenseChildNodeNumFromRank(label_rank);
        bool cd = (child < trie_->dense_node_count_);
        return DescendToLeftmostLeaf(
            cd, cd ? child : child - trie_->dense_node_count_);
      }
    } else {
      uint64_t next_pos = cur.pos() + 1;
      if (next_pos < trie_->s_labels_size_ &&
          !trie_->s_louds_.GetBit(next_pos)) {
        path_.push_back(LevelPos::MakeSparse(next_pos));
        current_key_.push_back(
            static_cast<char>(trie_->s_labels_data_[next_pos]));

        if (!trie_->s_has_child_.GetBit(next_pos)) {
          leaf_index_ = SparseLeafIndex(next_pos);
          valid_ = true;
          return true;
        }

        return DescendToLeftmostLeaf(false, SparseChildNodeNum(next_pos));
      }
    }
  }

  valid_ = false;
  return false;
}

TrieBlockHandle LoudsTrieIterator::Value() const {
  assert(valid_);
  return trie_->GetHandle(leaf_index_);
}

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
