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
  if (keys_.empty()) {
    return 0;
  }

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
    if (n == 0) {
      return l;
    }

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
    if (ld.node_count() == 0) {
      continue;
    }

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
    bv.EncodeTo(&serialized_data_);
  }
  {
    Bitvector bv;
    bv.BuildFrom(d_has_child_);
    bv.EncodeTo(&serialized_data_);
  }
  {
    Bitvector bv;
    bv.BuildFrom(d_is_prefix_key_);
    bv.EncodeTo(&serialized_data_);
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
  Bitvector bv_s_has_child;
  bv_s_has_child.BuildFrom(s_has_child_);
  bv_s_has_child.EncodeTo(&serialized_data_);

  Bitvector bv_s_louds;
  bv_s_louds.BuildFrom(s_louds_);
  bv_s_louds.EncodeTo(&serialized_data_);

  {
    Bitvector bv;
    bv.BuildFrom(s_is_prefix_key_);
    bv.EncodeTo(&serialized_data_);
  }

  // Child position lookup tables for Select-free sparse traversal.
  // Compute and serialize: s_child_start_pos_[k] and s_child_end_pos_[k]
  // for each internal label (has_child=1).
  uint64_t num_internal = bv_s_has_child.NumOnes();
  {
    PutFixed64(&serialized_data_, num_internal);

    if (num_internal > 0 && sl_size <= UINT32_MAX) {
      for (uint64_t k = 0; k < num_internal; k++) {
        uint64_t child_node = dense_child_count_ + k;
        uint64_t child_start = bv_s_louds.FindNthOneBit(child_node);
        PutFixed32(&serialized_data_, static_cast<uint32_t>(child_start));
      }
      // Pad to 8-byte alignment
      size_t bytes_written = num_internal * sizeof(uint32_t);
      size_t padding = (8 - (bytes_written % 8)) % 8;
      serialized_data_.append(padding, '\0');

      for (uint64_t k = 0; k < num_internal; k++) {
        uint64_t child_node = dense_child_count_ + k;
        uint64_t child_start = bv_s_louds.FindNthOneBit(child_node);
        uint64_t child_end = bv_s_louds.NextSetBit(child_start + 1);
        if (child_end > sl_size) {
          child_end = sl_size;
        }
        PutFixed32(&serialized_data_, static_cast<uint32_t>(child_end));
      }
      // Pad to 8-byte alignment
      bytes_written = num_internal * sizeof(uint32_t);
      padding = (8 - (bytes_written % 8)) % 8;
      serialized_data_.append(padding, '\0');
    }
  }

  // Path compression: detect and serialize fanout-1 chains.
  //
  // A chain starts at the child of internal label k and consists of >= 2
  // consecutive fanout-1 nodes (all internal except possibly the last,
  // which may be a leaf). For each chain, we store:
  //   - suffix bytes (the label at each chain node)
  //   - chain length
  //   - the child_idx at the end of the chain (UINT32_MAX if ends at leaf)
  //
  // This lets Seek() skip entire chains with a single memcmp instead of
  // traversing level by level with Rank1 at each step.
  {
    // Build child start/end arrays in memory for chain detection.
    // These are the same values we just serialized above.
    std::vector<uint32_t> child_starts(num_internal);
    std::vector<uint32_t> child_ends(num_internal);
    if (num_internal > 0 && sl_size <= UINT32_MAX) {
      for (uint64_t k = 0; k < num_internal; k++) {
        uint64_t child_node = dense_child_count_ + k;
        uint64_t cs = bv_s_louds.FindNthOneBit(child_node);
        child_starts[k] = static_cast<uint32_t>(cs);
        uint64_t ce = bv_s_louds.NextSetBit(cs + 1);
        if (ce > sl_size) {
          ce = sl_size;
        }
        child_ends[k] = static_cast<uint32_t>(ce);
      }
    }

    // For each internal label k, detect if its child starts a chain.
    std::string chain_suffix_data;
    // Offsets into chain_suffix_data for each internal label.
    // UINT32_MAX means no chain.
    std::vector<uint32_t> chain_offsets(num_internal, UINT32_MAX);
    std::vector<uint16_t> chain_lens(num_internal, 0);
    // child_idx at chain end (UINT32_MAX = leaf).
    std::vector<uint32_t> chain_end_child_idx(num_internal, UINT32_MAX);

    // Build s_is_prefix_key bitvector for prefix key checks during chain
    // detection. Chains must not contain prefix key nodes because the chain
    // skip logic bypasses prefix key checks.
    Bitvector bv_s_is_prefix_key;
    bv_s_is_prefix_key.BuildFrom(s_is_prefix_key_);

    for (uint64_t k = 0; k < num_internal; k++) {
      uint32_t cs = child_starts[k];
      uint32_t ce = child_ends[k];

      // Child must be fanout-1 (single label).
      if (ce - cs != 1) {
        continue;
      }

      // Check if the child's label is internal (has_child = 1).
      if (!bv_s_has_child.GetBit(cs)) {
        continue;
      }

      // Check if the child node is a prefix key — if so, cannot skip it.
      {
        uint64_t child_sparse_node = bv_s_louds.Rank1(cs + 1) - 1;
        if (child_sparse_node < bv_s_is_prefix_key.NumBits() &&
            bv_s_is_prefix_key.GetBit(child_sparse_node)) {
          continue;
        }
      }

      // Found a fanout-1 internal node. Follow the chain.
      std::vector<uint8_t> suffix;
      suffix.push_back(s_labels_[cs]);

      // Get the child_idx of this chain node.
      uint64_t cur_child_idx = bv_s_has_child.Rank1(cs + 1) - 1;
      uint32_t last_child_idx =
          UINT32_MAX;  // Will be set to chain end's child.

      while (true) {
        // cur_child_idx is the index of the current chain node's internal
        // label.
        if (cur_child_idx >= num_internal) {
          break;
        }

        uint32_t next_cs = child_starts[cur_child_idx];
        uint32_t next_ce = child_ends[cur_child_idx];

        if (next_ce - next_cs != 1) {
          // Child has fanout > 1. Chain ends here at an internal node.
          last_child_idx = static_cast<uint32_t>(cur_child_idx);
          break;
        }

        // Check if next node is a prefix key — stop chain here.
        {
          uint64_t next_sparse_node = bv_s_louds.Rank1(next_cs + 1) - 1;
          if (next_sparse_node < bv_s_is_prefix_key.NumBits() &&
              bv_s_is_prefix_key.GetBit(next_sparse_node)) {
            // End chain before this prefix key node.
            last_child_idx = static_cast<uint32_t>(cur_child_idx);
            break;
          }
        }

        // Next child is also fanout-1. Check if internal or leaf.
        suffix.push_back(s_labels_[next_cs]);

        if (!bv_s_has_child.GetBit(next_cs)) {
          // Chain ends at a leaf.
          last_child_idx = UINT32_MAX;
          break;
        }

        // Continue chaining.
        cur_child_idx = bv_s_has_child.Rank1(next_cs + 1) - 1;
      }

      // Only store chains of meaningful length. Short chains don't save
      // enough Rank1 calls to justify the metadata overhead (10 bytes per
      // chain + suffix bytes).
      static constexpr size_t kMinChainLength = 8;
      if (suffix.size() >= kMinChainLength && suffix.size() <= UINT16_MAX) {
        chain_offsets[k] = static_cast<uint32_t>(chain_suffix_data.size());
        chain_lens[k] = static_cast<uint16_t>(suffix.size());
        chain_end_child_idx[k] = last_child_idx;
        chain_suffix_data.append(reinterpret_cast<const char*>(suffix.data()),
                                 suffix.size());
      }
    }

    // Chain cost/benefit filter: only emit chains when they provide a net
    // speed benefit.
    //
    // Key insight: chains help when a Seek is *likely* to hit a chain.
    // For tries with few chains relative to keys (e.g., numeric keys with
    // a single long shared-prefix chain), every Seek benefits. For tries
    // with many chains relative to keys (e.g., random hex with thousands
    // of short chains), each chain is rarely hit and the per-level bitmap
    // check overhead outweighs the occasional hit.
    //
    // Metric: emit chains only when num_chains <= num_keys. When there are
    // more chains than keys, most chains won't be hit by any Seek, making
    // the bitmap overhead a net loss. Additionally, apply a space budget
    // (10% of base trie size) to prevent excessive metadata.
    {
      static constexpr double kChainBudgetPct = 0.10;
      static constexpr size_t kPerChainMetaBytes = 10;  // 4 + 2 + 4

      uint64_t candidate_count = 0;
      for (uint64_t k = 0; k < num_internal; k++) {
        if (chain_lens[k] > 0) {
          candidate_count++;
        }
      }

      uint64_t num_keys = handles_.size();
      bool too_many_chains = (candidate_count > num_keys);

      if (candidate_count == 0 || too_many_chains) {
        for (uint64_t k = 0; k < num_internal; k++) {
          chain_lens[k] = 0;
        }
      } else {
        // Apply space budget to prevent excessive metadata even when
        // the chain count is reasonable.
        size_t base_trie_size = serialized_data_.size();
        size_t budget = static_cast<size_t>(base_trie_size * kChainBudgetPct);
        size_t bitmap_fixed_cost =
            (num_internal + 7) / 8 + (num_internal / 512 + 1) * 8;

        if (budget <= bitmap_fixed_cost) {
          for (uint64_t k = 0; k < num_internal; k++) {
            chain_lens[k] = 0;
          }
        } else {
          size_t available = budget - bitmap_fixed_cost;
          size_t total_cost = 0;
          for (uint64_t k = 0; k < num_internal; k++) {
            if (chain_lens[k] > 0) {
              total_cost += kPerChainMetaBytes + chain_lens[k];
            }
          }

          if (total_cost > available) {
            // Over budget. Keep longest chains first.
            struct ChainCandidate {
              uint64_t idx;
              size_t cost;
              uint16_t length;
            };
            std::vector<ChainCandidate> candidates;
            candidates.reserve(candidate_count);
            for (uint64_t k = 0; k < num_internal; k++) {
              if (chain_lens[k] > 0) {
                candidates.push_back(
                    {k, kPerChainMetaBytes + chain_lens[k], chain_lens[k]});
              }
            }
            std::sort(candidates.begin(), candidates.end(),
                      [](const ChainCandidate& a, const ChainCandidate& b) {
                        return a.length > b.length;
                      });

            std::vector<bool> keep(num_internal, false);
            size_t used = 0;
            for (const auto& c : candidates) {
              if (used + c.cost <= available) {
                keep[c.idx] = true;
                used += c.cost;
              }
            }

            std::string new_suffix_data;
            for (uint64_t k = 0; k < num_internal; k++) {
              if (chain_lens[k] > 0 && !keep[k]) {
                chain_lens[k] = 0;
                chain_offsets[k] = UINT32_MAX;
                chain_end_child_idx[k] = UINT32_MAX;
              } else if (chain_lens[k] > 0) {
                uint32_t old_off = chain_offsets[k];
                chain_offsets[k] =
                    static_cast<uint32_t>(new_suffix_data.size());
                new_suffix_data.append(chain_suffix_data.data() + old_off,
                                       chain_lens[k]);
              }
            }
            chain_suffix_data = std::move(new_suffix_data);
          }
        }
      }
    }

    // Count actual chains and build bitmap + compact arrays.
    uint64_t num_chains = 0;
    BitvectorBuilder chain_bitmap_builder;
    for (uint64_t k = 0; k < num_internal; k++) {
      bool has_chain = (chain_lens[k] > 0);
      chain_bitmap_builder.Append(has_chain);
      if (has_chain) {
        num_chains++;
      }
    }

    // Serialize: num_chains, then bitmap + compact arrays if any.
    PutFixed64(&serialized_data_, num_chains);

    if (num_chains > 0) {
      // Write chain bitmap (1 bit per internal label).
      Bitvector chain_bitmap_bv;
      chain_bitmap_bv.BuildFrom(chain_bitmap_builder);
      chain_bitmap_bv.EncodeTo(&serialized_data_);

      // Write compact chain_offsets (uint32_t per chain).
      for (uint64_t k = 0; k < num_internal; k++) {
        if (chain_lens[k] > 0) {
          PutFixed32(&serialized_data_, chain_offsets[k]);
        }
      }
      size_t bytes_written = num_chains * sizeof(uint32_t);
      size_t padding = (8 - (bytes_written % 8)) % 8;
      serialized_data_.append(padding, '\0');

      // Write compact chain_lens (uint16_t per chain).
      for (uint64_t k = 0; k < num_internal; k++) {
        if (chain_lens[k] > 0) {
          serialized_data_.append(reinterpret_cast<const char*>(&chain_lens[k]),
                                  sizeof(uint16_t));
        }
      }
      bytes_written = num_chains * sizeof(uint16_t);
      padding = (8 - (bytes_written % 8)) % 8;
      serialized_data_.append(padding, '\0');

      // Write compact chain_end_child_idx (uint32_t per chain).
      for (uint64_t k = 0; k < num_internal; k++) {
        if (chain_lens[k] > 0) {
          PutFixed32(&serialized_data_, chain_end_child_idx[k]);
        }
      }
      bytes_written = num_chains * sizeof(uint32_t);
      padding = (8 - (bytes_written % 8)) % 8;
      serialized_data_.append(padding, '\0');

      // Write suffix data blob.
      uint64_t chain_data_size = chain_suffix_data.size();
      PutFixed64(&serialized_data_, chain_data_size);
      serialized_data_.append(chain_suffix_data);
      padding = (8 - (chain_data_size % 8)) % 8;
      serialized_data_.append(padding, '\0');
    }
  }

  // Block handles: packed uint32_t arrays for offsets and sizes.
  // BFS leaf order does not match key-sorted order for keys of different
  // lengths, so offsets are NOT monotone and cannot use Elias-Fano.
  {
    if (!handles_.empty()) {
      size_t n = handles_.size();
      // Write offsets array (uint32_t, padded to 8-byte alignment).
      for (size_t i = 0; i < n; i++) {
        assert(handles_[i].offset <= UINT32_MAX);
        PutFixed32(&serialized_data_,
                   static_cast<uint32_t>(handles_[i].offset));
      }
      size_t bytes_written = n * sizeof(uint32_t);
      size_t padding = (8 - (bytes_written % 8)) % 8;
      serialized_data_.append(padding, '\0');

      // Write sizes array (uint32_t, padded to 8-byte alignment).
      for (size_t i = 0; i < n; i++) {
        assert(handles_[i].size <= UINT32_MAX);
        PutFixed32(&serialized_data_, static_cast<uint32_t>(handles_[i].size));
      }
      bytes_written = n * sizeof(uint32_t);
      padding = (8 - (bytes_written % 8)) % 8;
      serialized_data_.append(padding, '\0');
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
      s_labels_size_(0),
      s_chain_suffix_data_(nullptr),
      s_chain_suffix_size_(0),
      handle_offsets_(nullptr),
      handle_sizes_(nullptr) {}

Status LoudsTrie::InitFromData(const Slice& data) {
  const char* p = data.data();
  size_t remaining = data.size();

  // The trie data contains bitvectors with uint64_t arrays and handle arrays
  // with uint32_t entries, all accessed via reinterpret_cast pointers that
  // require proper alignment. Block buffers from heap/cache allocations are
  // typically aligned, but mmap'd data or other sources may not be. If the
  // data is not 8-byte aligned, copy it into an owned aligned buffer.
  // std::string::data() returns memory from new[]/malloc, which is aligned
  // to at least alignof(max_align_t) (>= 8 on all supported platforms).
  if (reinterpret_cast<uintptr_t>(p) % alignof(uint64_t) != 0) {
    aligned_copy_.assign(p, remaining);
    p = aligned_copy_.data();
  }

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

  // Validate max_depth_ from untrusted data. The iterator allocates a
  // key_buf_ of size MaxDepth()+1; if max_depth_ == UINT32_MAX, the +1
  // overflows uint32_t to 0, causing a zero-length allocation and subsequent
  // buffer overflow. A key longer than 64 KB is unrealistic for a block
  // index separator (RocksDB keys are typically < 1 KB).
  static constexpr uint32_t kMaxReasonableDepth = 65536;
  if (max_depth_ > kMaxReasonableDepth) {
    return Status::Corruption("Trie index: max_depth exceeds reasonable limit");
  }

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
  if (!s.ok()) {
    return s;
  }
  p += consumed;
  remaining -= consumed;

  s = d_has_child_.InitFromData(p, remaining, &consumed);
  if (!s.ok()) {
    return s;
  }
  p += consumed;
  remaining -= consumed;

  s = d_is_prefix_key_.InitFromData(p, remaining, &consumed);
  if (!s.ok()) {
    return s;
  }
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
  if (!s.ok()) {
    return s;
  }
  p += consumed;
  remaining -= consumed;

  s = s_louds_.InitFromData(p, remaining, &consumed);
  if (!s.ok()) {
    return s;
  }
  p += consumed;
  remaining -= consumed;

  s = s_is_prefix_key_.InitFromData(p, remaining, &consumed);
  if (!s.ok()) {
    return s;
  }
  p += consumed;
  remaining -= consumed;

  // Child position lookup tables for Select-free sparse traversal.
  uint64_t num_internal = 0;
  {
    if (remaining < 8) {
      return Status::Corruption(
          "Trie index: truncated child position table count");
    }
    memcpy(&num_internal, p, 8);
    p += 8;
    remaining -= 8;

    if (num_internal > 0) {
      // Read s_child_start_pos_
      size_t start_bytes = num_internal * sizeof(uint32_t);
      size_t start_padded = (start_bytes + 7) & ~size_t(7);
      if (remaining < start_padded) {
        return Status::Corruption(
            "Trie index: truncated child start position table");
      }
      s_child_start_pos_.resize(num_internal);
      memcpy(s_child_start_pos_.data(), p, start_bytes);
      p += start_padded;
      remaining -= start_padded;

      // Read s_child_end_pos_
      size_t end_bytes = num_internal * sizeof(uint32_t);
      size_t end_padded = (end_bytes + 7) & ~size_t(7);
      if (remaining < end_padded) {
        return Status::Corruption(
            "Trie index: truncated child end position table");
      }
      s_child_end_pos_.resize(num_internal);
      memcpy(s_child_end_pos_.data(), p, end_bytes);
      p += end_padded;
      remaining -= end_padded;
    }
  }

  // Path compression: chain metadata for fanout-1 chains.
  // Format: num_chains (uint64), then if > 0: bitmap + compact arrays.
  {
    if (remaining < 8) {
      return Status::Corruption("Trie index: truncated chain count");
    }
    uint64_t num_chains;
    memcpy(&num_chains, p, 8);
    p += 8;
    remaining -= 8;

    if (num_chains > 0) {
      // Read chain bitmap.
      s = s_chain_bitmap_.InitFromData(p, remaining, &consumed);
      if (!s.ok()) {
        return s;
      }
      p += consumed;
      remaining -= consumed;

      // Read compact chain_offsets (uint32_t per chain).
      size_t offsets_bytes = num_chains * sizeof(uint32_t);
      size_t offsets_padded = (offsets_bytes + 7) & ~size_t(7);
      if (remaining < offsets_padded) {
        return Status::Corruption("Trie index: truncated chain offsets");
      }
      s_chain_suffix_offsets_.resize(num_chains);
      memcpy(s_chain_suffix_offsets_.data(), p, offsets_bytes);
      p += offsets_padded;
      remaining -= offsets_padded;

      // Read compact chain_lens (uint16_t per chain).
      size_t lens_bytes = num_chains * sizeof(uint16_t);
      size_t lens_padded = (lens_bytes + 7) & ~size_t(7);
      if (remaining < lens_padded) {
        return Status::Corruption("Trie index: truncated chain lengths");
      }
      s_chain_lens_.resize(num_chains);
      memcpy(s_chain_lens_.data(), p, lens_bytes);
      p += lens_padded;
      remaining -= lens_padded;

      // Read compact chain_end_child_idx (uint32_t per chain).
      size_t end_bytes = num_chains * sizeof(uint32_t);
      size_t end_padded = (end_bytes + 7) & ~size_t(7);
      if (remaining < end_padded) {
        return Status::Corruption("Trie index: truncated chain end indices");
      }
      s_chain_end_child_idx_.resize(num_chains);
      memcpy(s_chain_end_child_idx_.data(), p, end_bytes);
      p += end_padded;
      remaining -= end_padded;

      // Read suffix data blob.
      if (remaining < 8) {
        return Status::Corruption("Trie index: truncated chain suffix size");
      }
      memcpy(&s_chain_suffix_size_, p, 8);
      p += 8;
      remaining -= 8;

      size_t suffix_padded = (s_chain_suffix_size_ + 7) & ~size_t(7);
      if (remaining < suffix_padded) {
        return Status::Corruption("Trie index: truncated chain suffix data");
      }
      s_chain_suffix_data_ = reinterpret_cast<const uint8_t*>(p);
      p += suffix_padded;
      remaining -= suffix_padded;
    }
  }

  // Block handles: packed uint32_t arrays for offsets and sizes.
  if (num_keys_ > 0) {
    size_t arr_bytes = num_keys_ * sizeof(uint32_t);
    size_t arr_padded = (arr_bytes + 7) & ~size_t(7);

    // Read offsets array.
    if (remaining < arr_padded) {
      return Status::Corruption("Trie index: truncated handle offsets");
    }
    if (reinterpret_cast<uintptr_t>(p) % alignof(uint32_t) != 0) {
      return Status::Corruption("Trie index: handle offsets not aligned");
    }
    handle_offsets_ = reinterpret_cast<const uint32_t*>(p);
    p += arr_padded;
    remaining -= arr_padded;

    // Read sizes array.
    if (remaining < arr_padded) {
      return Status::Corruption("Trie index: truncated handle sizes");
    }
    if (reinterpret_cast<uintptr_t>(p) % alignof(uint32_t) != 0) {
      return Status::Corruption("Trie index: handle sizes not aligned");
    }
    handle_sizes_ = reinterpret_cast<const uint32_t*>(p);
    // p and remaining not advanced — this is the last section.
  }

  return Status::OK();
}
TrieBlockHandle LoudsTrie::GetHandle(uint64_t leaf_index) const {
  assert(leaf_index < num_keys_);
  return TrieBlockHandle{handle_offsets_[leaf_index],
                         handle_sizes_[leaf_index]};
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
    : has_chains_(trie->HasChains()),
      trie_(trie),
      valid_(false),
      leaf_index_(0),
      key_len_(0),
      key_cap_(0),
      is_at_prefix_key_(false) {
  // Allocate key buffer once, sized to the longest possible trie path.
  // MaxDepth() is the length of the longest key, so no traversal will
  // exceed this depth.
  key_cap_ = trie_->MaxDepth() + 1;
  key_buf_ = std::make_unique<char[]>(key_cap_);
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
  uint64_t has_child_rank = trie_->d_has_child_.Rank1(label_rank + 1);
  return DenseLeafIndexFromRankAndHasChildRank(pos, label_rank, has_child_rank);
}

uint64_t LoudsTrieIterator::DenseLeafIndexFromRankAndHasChildRank(
    uint64_t pos, uint64_t label_rank, uint64_t has_child_rank) const {
  // Leaf ordinal for a dense leaf at position `pos` in d_labels_.
  //
  // BFS leaf order: for each node 0..N, prefix key first, then non-internal
  // children. So leaf index =
  //   (prefix keys at nodes [0, node_num]) +
  //   (non-internal labels at positions [0, label_rank]) - 1 (0-indexed).
  //
  // When there are no prefix keys, the prefix_keys term is zero and we
  // can skip the d_is_prefix_key_.Rank1 call entirely.
  uint64_t leaf_labels = (label_rank + 1) - has_child_rank;
  if (trie_->d_is_prefix_key_.NumOnes() == 0) {
    return leaf_labels - 1;
  }
  uint64_t node_num = pos / 256;
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
  // Labels within a sparse node are stored in sorted order. Find the first
  // label >= target_byte.
  //
  // Strategy: use linear scan for small nodes (<=16 labels) and binary
  // search for larger nodes. Profiling shows that the vast majority of
  // sparse nodes have small fanout (often 1-10 children), where linear
  // scan is faster than std::lower_bound due to sequential memory access
  // and predictable branches. The threshold of 16 was chosen because
  // 16 bytes fits in a single cache line and binary search gains an
  // advantage only around log2(16)=4 comparisons with ~50% branch
  // misprediction rate.
  static constexpr uint64_t kLinearScanThreshold = 16;

  const uint8_t* base = trie_->s_labels_data_;
  const uint8_t* begin = base + node_start_pos;
  const uint8_t* end = base + node_end_pos;
  uint64_t size = node_end_pos - node_start_pos;

  const uint8_t* it;
  if (size <= kLinearScanThreshold) {
    // Linear scan: fastest for small nodes.
    it = begin;
    while (it < end && *it < target_byte) {
      ++it;
    }
  } else {
    it = std::lower_bound(begin, end, target_byte);
  }

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
  uint64_t has_child_rank = trie_->s_has_child_.Rank1(pos + 1);
  return SparseLeafIndexFromHasChildRank(pos, has_child_rank);
}

uint64_t LoudsTrieIterator::SparseLeafIndexFromHasChildRank(
    uint64_t pos, uint64_t has_child_rank) const {
  // Leaf ordinal for a sparse leaf at position pos.
  // = dense_leaf_count + prefix_keys_at_nodes[0..N] +
  // non-internal_labels[0..pos] - 1
  //
  // When there are no prefix keys (the common case), the prefix_keys term
  // is zero and we can skip both SparseNodeNum (1 Rank1 on s_louds_) and
  // s_is_prefix_key_.Rank1, leaving only the precomputed has_child rank.
  uint64_t leaf_labels = (pos + 1) - has_child_rank;

  if (trie_->s_is_prefix_key_.NumOnes() == 0) {
    return trie_->dense_leaf_count_ + leaf_labels - 1;
  }
  uint64_t sparse_node = SparseNodeNum(pos);
  uint64_t prefix_keys = trie_->s_is_prefix_key_.Rank1(sparse_node + 1);
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
  if (sparse_node_num == 0) {
    return 0;
  }

  // Use the precomputed lookup table for nodes that are children of sparse
  // internal labels. These are the most common case during traversal.
  if (sparse_node_num >= trie_->dense_child_count_ &&
      !trie_->s_child_start_pos_.empty()) {
    uint64_t internal_idx = sparse_node_num - trie_->dense_child_count_;
    if (internal_idx < trie_->s_child_start_pos_.size()) {
      return trie_->s_child_start_pos_[internal_idx];
    }
  }

  // Fallback to FindNthOneBit for:
  // - Root sparse nodes (children of dense nodes)
  // - Very large tries where lookup table wasn't built
  return trie_->s_louds_.FindNthOneBit(sparse_node_num);
}

uint64_t LoudsTrieIterator::SparseNodeEndPos(uint64_t start_pos) const {
  uint64_t next = trie_->s_louds_.NextSetBit(start_pos + 1);
  if (next >= trie_->s_louds_.NumBits()) {
    return trie_->s_labels_size_;
  }
  return next;
}

// Helper: descend from (in_dense, node_num) to leftmost leaf, pushing
// path entries and building key_buf_. Returns true if a leaf was found.
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
      key_buf_[key_len_++] = static_cast<char>(first % 256);

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
      key_buf_[key_len_++] = static_cast<char>(trie_->s_labels_data_[start]);

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
// Uses SuRF-style Select-free traversal for sparse regions: instead of
// tracking node_num and calling FindNthOneBit to find node boundaries, we track
// (start_pos, end_pos) directly and use only Rank1 + array lookup.
template <bool kHasChains>
bool LoudsTrieIterator::SeekImpl(const Slice& target) {
  valid_ = false;
  leaf_index_ = 0;
  key_len_ = 0;
  path_.clear();
  is_at_prefix_key_ = false;

  if (trie_->NumKeys() == 0) {
    return false;
  }

  bool in_dense = (trie_->cutoff_level_ > 0);
  uint64_t node_num = 0;

  // SuRF-style: For sparse traversal, track (start_pos, end_pos) directly
  // instead of node_num. This eliminates FindNthOneBit calls entirely.
  uint64_t sparse_start = 0;
  uint64_t sparse_end = 0;
  bool have_sparse_bounds = false;

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
      key_buf_[key_len_++] = static_cast<char>(pos % 256);

      // Cache label_rank: avoids redundant Rank1(d_labels_) in both
      // has_child check and DenseChildNodeNum/DenseLeafIndex.
      uint64_t label_rank = trie_->d_labels_.Rank1(pos + 1) - 1;
      bool is_internal = trie_->d_has_child_.GetBit(label_rank);
      // Compute has_child_rank once, reuse for both leaf index and child
      // node number. DenseChildNodeNumFromRank(lr) = Rank1(lr + 1) and
      // DenseLeafIndexFromRankAndHasChildRank also needs Rank1(lr + 1).
      uint64_t has_child_rank = trie_->d_has_child_.Rank1(label_rank + 1);

      if (!exact) {
        // Landed on label > target_byte. Go to leftmost leaf in subtree.
        if (!is_internal) {
          leaf_index_ = DenseLeafIndexFromRankAndHasChildRank(pos, label_rank,
                                                              has_child_rank);
          valid_ = true;
          return true;
        }
        uint64_t child = has_child_rank;  // = DenseChildNodeNumFromRank(lr)
        bool child_dense = (child < trie_->dense_node_count_);
        uint64_t cn = child_dense ? child : child - trie_->dense_node_count_;
        return DescendToLeftmostLeaf(child_dense, cn);
      }

      if (!is_internal) {
        // This label is a leaf. Check if target is fully consumed.
        if (depth == static_cast<uint32_t>(target.size()) - 1) {
          // Exact match: trie key == target.
          leaf_index_ = DenseLeafIndexFromRankAndHasChildRank(pos, label_rank,
                                                              has_child_rank);
          valid_ = true;
          return true;
        }
        // Target has more bytes, so trie key is a proper prefix of target.
        // Trie key < target. Advance to the NEXT leaf.
        leaf_index_ = DenseLeafIndexFromRankAndHasChildRank(pos, label_rank,
                                                            has_child_rank);
        valid_ = true;
        return Advance();
      }

      uint64_t child = has_child_rank;  // = DenseChildNodeNumFromRank(lr)
      if (child < trie_->dense_node_count_) {
        node_num = child;
        in_dense = true;
      } else {
        node_num = child - trie_->dense_node_count_;
        in_dense = false;
        have_sparse_bounds = false;  // Will compute on first sparse access
      }
    } else {
      // SuRF-style sparse traversal: Use (start, end) positions directly.
      // No FindNthOneBit calls - only Rank1 + array lookup.
      uint64_t start;
      uint64_t end;
      if (have_sparse_bounds) {
        start = sparse_start;
        end = sparse_end;
      } else {
        // First entry into sparse region from dense, or from root.
        // Need to compute bounds using FindNthOneBit (only once per
        // dense->sparse).
        start = SparseNodeStartPos(node_num);
        end = SparseNodeEndPos(start);
      }

      // Fast path for fanout-1 nodes: the most common case for tries built
      // from keys with long common prefixes (e.g., zero-padded numeric keys).
      // Avoids the SparseSeekLabel function call and reduces branch logic to
      // a single byte comparison.
      if (start + 1 == end) {
        uint8_t label = trie_->s_labels_data_[start];

        if (label == target_byte) {
          // Exact match (the overwhelmingly common case for fanout-1).
          path_.push_back(LevelPos::MakeSparse(start));
          key_buf_[key_len_++] = static_cast<char>(label);

          bool is_internal = trie_->s_has_child_.GetBit(start);
          uint64_t has_child_rank = trie_->s_has_child_.Rank1(start + 1);
          if (is_internal) {
            // Internal node: use rank for child lookup.
            if (!trie_->s_child_start_pos_.empty()) {
              uint64_t child_idx = has_child_rank - 1;
              if (child_idx < trie_->s_child_start_pos_.size()) {
                // ---- Path compression: chain skip ----
                // Check if this child starts a fanout-1 chain. If so, compare
                // the remaining target bytes against the chain suffix with a
                // single memcmp instead of traversing level by level.
                // Guarded by if constexpr: when kHasChains=false, the compiler
                // eliminates this entire block from the generated code.
                if constexpr (kHasChains) {
                  if (child_idx < trie_->s_chain_bitmap_.NumBits() &&
                      trie_->s_chain_bitmap_.GetBit(child_idx)) {
                    uint64_t chain_idx =
                        trie_->s_chain_bitmap_.Rank1(child_idx + 1) - 1;
                    uint16_t chain_len = trie_->s_chain_lens_[chain_idx];
                    const uint8_t* suffix =
                        trie_->s_chain_suffix_data_ +
                        trie_->s_chain_suffix_offsets_[chain_idx];
                    uint32_t target_remaining =
                        static_cast<uint32_t>(target.size()) - depth - 1;

                    if (target_remaining >= chain_len) {
                      // Target has enough bytes to cover the full chain.
                      const uint8_t* target_bytes =
                          reinterpret_cast<const uint8_t*>(target.data()) +
                          depth + 1;
                      int cmp = memcmp(target_bytes, suffix, chain_len);
                      if (cmp == 0) {
                        // Full chain match! Push all chain nodes onto path_.
                        // Walk the chain using child position tables to get
                        // each node's label position for path reconstruction.
                        uint64_t cur_idx = child_idx;
                        for (uint16_t ci = 0; ci < chain_len; ci++) {
                          uint32_t cs = trie_->s_child_start_pos_[cur_idx];
                          path_.push_back(LevelPos::MakeSparse(cs));
                          key_buf_[key_len_++] =
                              static_cast<char>(trie_->s_labels_data_[cs]);
                          // Move to next chain node (last node handled below).
                          if (ci + 1 < chain_len) {
                            cur_idx = trie_->s_has_child_.Rank1(cs + 1) - 1;
                          }
                        }
                        // Chain fully matched. Advance depth past the chain.
                        depth += chain_len;

                        // Set up for the node after the chain.
                        uint32_t end_child_idx =
                            trie_->s_chain_end_child_idx_[chain_idx];
                        if (end_child_idx == UINT32_MAX) {
                          // Chain ends at a leaf. Check if target is consumed.
                          uint32_t last_cs = trie_->s_child_start_pos_[cur_idx];
                          uint64_t last_hcr =
                              trie_->s_has_child_.Rank1(last_cs + 1);
                          if (depth ==
                              static_cast<uint32_t>(target.size()) - 1) {
                            leaf_index_ = SparseLeafIndexFromHasChildRank(
                                last_cs, last_hcr);
                            valid_ = true;
                            return true;
                          }
                          // Target has more bytes, trie key < target. Advance.
                          leaf_index_ = SparseLeafIndexFromHasChildRank(
                              last_cs, last_hcr);
                          valid_ = true;
                          return Advance();
                        }
                        // Chain ends at an internal node with fanout > 1.
                        sparse_start = trie_->s_child_start_pos_[end_child_idx];
                        sparse_end = trie_->s_child_end_pos_[end_child_idx];
                        have_sparse_bounds = true;
                        in_dense = false;
                        continue;
                      }
                      // Chain mismatch. Find the divergence point.
                      uint16_t mismatch_pos = 0;
                      while (mismatch_pos < chain_len &&
                             target_bytes[mismatch_pos] ==
                                 suffix[mismatch_pos]) {
                        mismatch_pos++;
                      }
                      // Push path entries up to the mismatch point.
                      uint64_t cur_idx = child_idx;
                      for (uint16_t ci = 0; ci < mismatch_pos; ci++) {
                        uint32_t cs = trie_->s_child_start_pos_[cur_idx];
                        path_.push_back(LevelPos::MakeSparse(cs));
                        key_buf_[key_len_++] =
                            static_cast<char>(trie_->s_labels_data_[cs]);
                        cur_idx = trie_->s_has_child_.Rank1(cs + 1) - 1;
                      }
                      // At the mismatch node: push the node's label and handle.
                      uint32_t mis_cs = trie_->s_child_start_pos_[cur_idx];
                      path_.push_back(LevelPos::MakeSparse(mis_cs));
                      key_buf_[key_len_++] =
                          static_cast<char>(trie_->s_labels_data_[mis_cs]);

                      if (target_bytes[mismatch_pos] < suffix[mismatch_pos]) {
                        // Target < chain. Chain's path leads to the first key
                        // >= target. Descend to leftmost leaf from here.
                        if (!trie_->s_has_child_.GetBit(mis_cs)) {
                          leaf_index_ = SparseLeafIndex(mis_cs);
                          valid_ = true;
                          return true;
                        }
                        return DescendToLeftmostLeaf(
                            false, SparseChildNodeNum(mis_cs));
                      }
                      // target_bytes[mismatch_pos] > suffix[mismatch_pos]:
                      // All keys through this chain node are < target. Advance.
                      return Advance();
                    }
                    // Target runs out before chain ends.
                    // Check if target's remaining bytes match the chain prefix.
                    if (target_remaining > 0) {
                      const uint8_t* target_bytes =
                          reinterpret_cast<const uint8_t*>(target.data()) +
                          depth + 1;
                      int cmp = memcmp(target_bytes, suffix, target_remaining);
                      if (cmp < 0) {
                        // Target < chain prefix. Push matched portion.
                        uint64_t cur_idx = child_idx;
                        uint32_t cs = trie_->s_child_start_pos_[cur_idx];
                        path_.push_back(LevelPos::MakeSparse(cs));
                        key_buf_[key_len_++] =
                            static_cast<char>(trie_->s_labels_data_[cs]);
                        // Descend to leftmost leaf from this first chain node.
                        if (!trie_->s_has_child_.GetBit(cs)) {
                          leaf_index_ = SparseLeafIndex(cs);
                          valid_ = true;
                          return true;
                        }
                        return DescendToLeftmostLeaf(false,
                                                     SparseChildNodeNum(cs));
                      }
                      if (cmp > 0) {
                        // Target > chain prefix at some point. We need to find
                        // the exact divergence point.
                        uint16_t mismatch_pos = 0;
                        while (mismatch_pos < target_remaining &&
                               target_bytes[mismatch_pos] ==
                                   suffix[mismatch_pos]) {
                          mismatch_pos++;
                        }
                        // Push path entries up to the mismatch.
                        uint64_t cur_idx = child_idx;
                        for (uint16_t ci = 0; ci < mismatch_pos; ci++) {
                          uint32_t cs2 = trie_->s_child_start_pos_[cur_idx];
                          path_.push_back(LevelPos::MakeSparse(cs2));
                          key_buf_[key_len_++] =
                              static_cast<char>(trie_->s_labels_data_[cs2]);
                          cur_idx = trie_->s_has_child_.Rank1(cs2 + 1) - 1;
                        }
                        uint32_t mis_cs2 = trie_->s_child_start_pos_[cur_idx];
                        path_.push_back(LevelPos::MakeSparse(mis_cs2));
                        key_buf_[key_len_++] =
                            static_cast<char>(trie_->s_labels_data_[mis_cs2]);
                        return Advance();
                      }
                      // cmp == 0: target matches chain prefix exactly. Target
                      // is fully consumed. Push matched nodes and check prefix
                      // key / descend to leftmost leaf.
                      uint64_t cur_idx = child_idx;
                      for (uint32_t ci = 0; ci < target_remaining; ci++) {
                        uint32_t cs = trie_->s_child_start_pos_[cur_idx];
                        path_.push_back(LevelPos::MakeSparse(cs));
                        key_buf_[key_len_++] =
                            static_cast<char>(trie_->s_labels_data_[cs]);
                        if (ci + 1 < target_remaining) {
                          cur_idx = trie_->s_has_child_.Rank1(cs + 1) - 1;
                        }
                      }
                      // Target consumed mid-chain. The remaining chain nodes
                      // form keys > target. Descend to leftmost leaf from the
                      // next chain node.
                      uint32_t last_cs = trie_->s_child_start_pos_[cur_idx];
                      // This node is always internal (it's mid-chain).
                      return DescendToLeftmostLeaf(false,
                                                   SparseChildNodeNum(last_cs));
                    }
                    // target_remaining == 0: target fully consumed.
                    // This means the target ended exactly at the parent node.
                    // The chain nodes are all > target. Push first chain node
                    // and descend to leftmost leaf.
                    uint32_t cs = trie_->s_child_start_pos_[child_idx];
                    path_.push_back(LevelPos::MakeSparse(cs));
                    key_buf_[key_len_++] =
                        static_cast<char>(trie_->s_labels_data_[cs]);
                    if (!trie_->s_has_child_.GetBit(cs)) {
                      leaf_index_ = SparseLeafIndex(cs);
                      valid_ = true;
                      return true;
                    }
                    return DescendToLeftmostLeaf(false, SparseChildNodeNum(cs));
                  }
                }  // if constexpr (kHasChains)
                // No chain — normal child lookup.
                sparse_start = trie_->s_child_start_pos_[child_idx];
                sparse_end = trie_->s_child_end_pos_[child_idx];
                have_sparse_bounds = true;
              } else {
                node_num = SparseChildNodeNum(start);
                have_sparse_bounds = false;
              }
            } else {
              node_num = SparseChildNodeNum(start);
              have_sparse_bounds = false;
            }
            in_dense = false;
            continue;
          }
          // Leaf node.
          if (depth == static_cast<uint32_t>(target.size()) - 1) {
            leaf_index_ =
                SparseLeafIndexFromHasChildRank(start, has_child_rank);
            valid_ = true;
            return true;
          }
          leaf_index_ = SparseLeafIndexFromHasChildRank(start, has_child_rank);
          valid_ = true;
          return Advance();
        }

        // label != target_byte. Still need to push path for backtracking.
        path_.push_back(LevelPos::MakeSparse(start));
        key_buf_[key_len_++] = static_cast<char>(label);

        if (label > target_byte) {
          // Label is greater: go to leftmost leaf in subtree.
          bool is_internal = trie_->s_has_child_.GetBit(start);
          uint64_t has_child_rank = trie_->s_has_child_.Rank1(start + 1);
          if (!is_internal) {
            leaf_index_ =
                SparseLeafIndexFromHasChildRank(start, has_child_rank);
            valid_ = true;
            return true;
          }
          return DescendToLeftmostLeaf(false, SparseChildNodeNum(start));
        }
        // label < target_byte: no label >= target in this node. Backtrack.
        return Advance();
      }

      // General path for nodes with fanout > 1.
      uint64_t pos;
      bool exact = SparseSeekLabel(start, end, target_byte, &pos);

      if (pos >= end) {
        return Advance();
      }

      path_.push_back(LevelPos::MakeSparse(pos));
      key_buf_[key_len_++] = static_cast<char>(trie_->s_labels_data_[pos]);

      bool is_internal = trie_->s_has_child_.GetBit(pos);
      uint64_t has_child_rank = trie_->s_has_child_.Rank1(pos + 1);

      if (!exact) {
        if (!is_internal) {
          leaf_index_ = SparseLeafIndexFromHasChildRank(pos, has_child_rank);
          valid_ = true;
          return true;
        }
        return DescendToLeftmostLeaf(false, SparseChildNodeNum(pos));
      }

      if (!is_internal) {
        // Check if target is fully consumed.
        if (depth == static_cast<uint32_t>(target.size()) - 1) {
          leaf_index_ = SparseLeafIndexFromHasChildRank(pos, has_child_rank);
          valid_ = true;
          return true;
        }
        // Target has more bytes. Trie key < target. Advance.
        leaf_index_ = SparseLeafIndexFromHasChildRank(pos, has_child_rank);
        valid_ = true;
        return Advance();
      }

      // Descend to child: Get child bounds using Rank1 + array lookup.
      // This is the key SuRF optimization - NO FindNthOneBit here!
      // Reuse the already-computed has_child_rank.
      {
        uint64_t child_idx = has_child_rank - 1;
        if (!trie_->s_child_start_pos_.empty() &&
            child_idx < trie_->s_child_start_pos_.size()) {
          // ---- Path compression: chain skip (general path) ----
          if constexpr (kHasChains) {
            if (child_idx < trie_->s_chain_bitmap_.NumBits() &&
                trie_->s_chain_bitmap_.GetBit(child_idx)) {
              uint64_t chain_idx =
                  trie_->s_chain_bitmap_.Rank1(child_idx + 1) - 1;
              uint16_t chain_len = trie_->s_chain_lens_[chain_idx];
              const uint8_t* suffix = trie_->s_chain_suffix_data_ +
                                      trie_->s_chain_suffix_offsets_[chain_idx];
              uint32_t target_remaining =
                  static_cast<uint32_t>(target.size()) - depth - 1;

              if (target_remaining >= chain_len) {
                const uint8_t* target_bytes =
                    reinterpret_cast<const uint8_t*>(target.data()) + depth + 1;
                int cmp = memcmp(target_bytes, suffix, chain_len);
                if (cmp == 0) {
                  // Full chain match! Push all chain nodes.
                  uint64_t cur_idx = child_idx;
                  for (uint16_t ci = 0; ci < chain_len; ci++) {
                    uint32_t cs = trie_->s_child_start_pos_[cur_idx];
                    path_.push_back(LevelPos::MakeSparse(cs));
                    key_buf_[key_len_++] =
                        static_cast<char>(trie_->s_labels_data_[cs]);
                    if (ci + 1 < chain_len) {
                      cur_idx = trie_->s_has_child_.Rank1(cs + 1) - 1;
                    }
                  }
                  depth += chain_len;

                  uint32_t end_child_idx =
                      trie_->s_chain_end_child_idx_[chain_idx];
                  if (end_child_idx == UINT32_MAX) {
                    uint32_t last_cs = trie_->s_child_start_pos_[cur_idx];
                    uint64_t last_hcr = trie_->s_has_child_.Rank1(last_cs + 1);
                    if (depth == static_cast<uint32_t>(target.size()) - 1) {
                      leaf_index_ =
                          SparseLeafIndexFromHasChildRank(last_cs, last_hcr);
                      valid_ = true;
                      return true;
                    }
                    leaf_index_ =
                        SparseLeafIndexFromHasChildRank(last_cs, last_hcr);
                    valid_ = true;
                    return Advance();
                  }
                  sparse_start = trie_->s_child_start_pos_[end_child_idx];
                  sparse_end = trie_->s_child_end_pos_[end_child_idx];
                  have_sparse_bounds = true;
                  in_dense = false;
                  continue;
                }
                // Mismatch: find divergence point.
                uint16_t mismatch_pos = 0;
                while (mismatch_pos < chain_len &&
                       target_bytes[mismatch_pos] == suffix[mismatch_pos]) {
                  mismatch_pos++;
                }
                uint64_t cur_idx = child_idx;
                for (uint16_t ci = 0; ci < mismatch_pos; ci++) {
                  uint32_t cs = trie_->s_child_start_pos_[cur_idx];
                  path_.push_back(LevelPos::MakeSparse(cs));
                  key_buf_[key_len_++] =
                      static_cast<char>(trie_->s_labels_data_[cs]);
                  cur_idx = trie_->s_has_child_.Rank1(cs + 1) - 1;
                }
                uint32_t mis_cs = trie_->s_child_start_pos_[cur_idx];
                path_.push_back(LevelPos::MakeSparse(mis_cs));
                key_buf_[key_len_++] =
                    static_cast<char>(trie_->s_labels_data_[mis_cs]);

                if (target_bytes[mismatch_pos] < suffix[mismatch_pos]) {
                  if (!trie_->s_has_child_.GetBit(mis_cs)) {
                    leaf_index_ = SparseLeafIndex(mis_cs);
                    valid_ = true;
                    return true;
                  }
                  return DescendToLeftmostLeaf(false,
                                               SparseChildNodeNum(mis_cs));
                }
                return Advance();
              }
              // Target runs out before chain ends.
              if (target_remaining > 0) {
                const uint8_t* target_bytes =
                    reinterpret_cast<const uint8_t*>(target.data()) + depth + 1;
                int cmp = memcmp(target_bytes, suffix, target_remaining);
                if (cmp < 0) {
                  uint32_t cs = trie_->s_child_start_pos_[child_idx];
                  path_.push_back(LevelPos::MakeSparse(cs));
                  key_buf_[key_len_++] =
                      static_cast<char>(trie_->s_labels_data_[cs]);
                  if (!trie_->s_has_child_.GetBit(cs)) {
                    leaf_index_ = SparseLeafIndex(cs);
                    valid_ = true;
                    return true;
                  }
                  return DescendToLeftmostLeaf(false, SparseChildNodeNum(cs));
                }
                if (cmp > 0) {
                  uint16_t mp = 0;
                  while (mp < target_remaining &&
                         target_bytes[mp] == suffix[mp]) {
                    mp++;
                  }
                  uint64_t cur_idx = child_idx;
                  for (uint16_t ci = 0; ci < mp; ci++) {
                    uint32_t cs2 = trie_->s_child_start_pos_[cur_idx];
                    path_.push_back(LevelPos::MakeSparse(cs2));
                    key_buf_[key_len_++] =
                        static_cast<char>(trie_->s_labels_data_[cs2]);
                    cur_idx = trie_->s_has_child_.Rank1(cs2 + 1) - 1;
                  }
                  uint32_t mis_cs2 = trie_->s_child_start_pos_[cur_idx];
                  path_.push_back(LevelPos::MakeSparse(mis_cs2));
                  key_buf_[key_len_++] =
                      static_cast<char>(trie_->s_labels_data_[mis_cs2]);
                  return Advance();
                }
                // Prefix match: target consumed mid-chain.
                uint64_t cur_idx = child_idx;
                for (uint32_t ci = 0; ci < target_remaining; ci++) {
                  uint32_t cs = trie_->s_child_start_pos_[cur_idx];
                  path_.push_back(LevelPos::MakeSparse(cs));
                  key_buf_[key_len_++] =
                      static_cast<char>(trie_->s_labels_data_[cs]);
                  if (ci + 1 < target_remaining) {
                    cur_idx = trie_->s_has_child_.Rank1(cs + 1) - 1;
                  }
                }
                uint32_t last_cs = trie_->s_child_start_pos_[cur_idx];
                return DescendToLeftmostLeaf(false,
                                             SparseChildNodeNum(last_cs));
              }
              // target_remaining == 0: first chain node is > target.
              uint32_t cs = trie_->s_child_start_pos_[child_idx];
              path_.push_back(LevelPos::MakeSparse(cs));
              key_buf_[key_len_++] =
                  static_cast<char>(trie_->s_labels_data_[cs]);
              if (!trie_->s_has_child_.GetBit(cs)) {
                leaf_index_ = SparseLeafIndex(cs);
                valid_ = true;
                return true;
              }
              return DescendToLeftmostLeaf(false, SparseChildNodeNum(cs));
            }
          }  // if constexpr (kHasChains)
          // No chain — normal child lookup.
          sparse_start = trie_->s_child_start_pos_[child_idx];
          sparse_end = trie_->s_child_end_pos_[child_idx];
          have_sparse_bounds = true;
        } else {
          node_num = SparseChildNodeNum(pos);
          have_sparse_bounds = false;
        }
      }
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
    // For prefix key check, we need node_num. Compute it if we were tracking
    // positions directly.
    if (have_sparse_bounds) {
      // Compute node_num from sparse_start position.
      // node_num = number of nodes before this position = Rank1(s_louds, pos)
      // But sparse_start IS the start of the node, so:
      // node_num = Rank1(s_louds_, sparse_start + 1) - 1
      node_num = trie_->s_louds_.Rank1(sparse_start + 1) - 1;
    }
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
  if (!valid_) {
    return false;
  }

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
      key_buf_[key_len_++] = static_cast<char>(first % 256);

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
      key_buf_[key_len_++] = static_cast<char>(trie_->s_labels_data_[start]);

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
    if (key_len_ > 0) {
      key_len_--;
    }

    if (cur.is_dense()) {
      uint64_t cur_pos = cur.pos();
      uint64_t node_num = cur_pos / 256;
      uint64_t node_end = (node_num + 1) * 256;
      uint64_t next = trie_->d_labels_.NextSetBit(cur_pos + 1);

      if (next < node_end && next < trie_->d_labels_.NumBits()) {
        path_.push_back(LevelPos::MakeDense(next));
        key_buf_[key_len_++] = static_cast<char>(next % 256);

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
        key_buf_[key_len_++] =
            static_cast<char>(trie_->s_labels_data_[next_pos]);

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

// Explicit template instantiations for SeekImpl.
template bool LoudsTrieIterator::SeekImpl<true>(const Slice&);
template bool LoudsTrieIterator::SeekImpl<false>(const Slice&);

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
