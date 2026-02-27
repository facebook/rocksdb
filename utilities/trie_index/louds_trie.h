//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************
//
//  Fast Succinct Trie (FST) implementation based on the LOUDS (Level-Order
//  Unary Degree Sequence) encoding, inspired by the SuRF paper (Zhang et al.,
//  SIGMOD 2018). The trie uses a hybrid encoding:
//
//  - LOUDS-Dense: For the upper levels of the trie (levels close to the root)
//    where the fanout tends to be high. Uses 256-bit bitmaps per node (one bit
//    per possible byte label), achieving excellent cache locality and O(1)
//    child lookup via popcount.
//
//  - LOUDS-Sparse: For the lower levels of the trie where fanout is typically
//    low. Uses compact label arrays and bitvectors, achieving better space
//    efficiency than the dense encoding for sparse regions.
//
//  The boundary between dense and sparse levels (the "cutoff level") is chosen
//  to minimize total space: dense levels use 256 bits per node regardless of
//  fanout, while sparse levels use ~10 bits per edge. When the average fanout
//  drops below ~25 children per node, sparse becomes more efficient.
//
//  Key design decisions:
//  - Immutable: Built once from sorted keys during SST file construction.
//  - Flat-array layout: The entire trie is stored as a sequence of bitvectors,
//    making serialization trivial and enabling zero-copy reads from disk.
//  - Leaf-indexed: Each trie leaf maps to a data block handle via packed
//    uint32_t offset/size arrays, indexed by the leaf's BFS ordinal.
//  - Key reconstruction: The separator key is reconstructed by tracing
//    the path from root to the current leaf, collecting byte labels at
//    each level. Dense levels encode the label in the bit position
//    (pos % 256), sparse levels store it in the label array.
//
//  Leaf ordinal computation (SuRF formulas):
//  - Dense leaf: rank1(d_labels, pos+1) - rank1(d_has_child, rank1(d_labels,
//    pos+1)) + rank1(d_is_prefix_key, node_num+1) - 1
//    where pos = node_num * 256 + label_byte
//  - Sparse leaf: (label_pos - rank1(s_has_child, label_pos)) +
//    rank1(s_is_prefix_key, node_num+1) + dense_leaf_count - 1

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/autovector.h"
#include "utilities/trie_index/bitvector.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// ============================================================================
// Forward declarations
// ============================================================================
class LoudsTrie;

// ============================================================================
// BlockHandle: offset and size of a data block in the SST file.
// Matches UserDefinedIndexBuilder::BlockHandle but defined locally to avoid
// header dependencies in the core trie implementation.
// ============================================================================
struct TrieBlockHandle {
  uint64_t offset = 0;
  uint64_t size = 0;
};

// ============================================================================
// LoudsTrieBuilder: Constructs a LOUDS-encoded trie from sorted keys.
//
// Usage:
//   LoudsTrieBuilder builder;
//   for each data block in sorted order:
//     builder.AddKey(separator_key, block_handle);
//   builder.Finish();
//   Slice serialized = builder.GetSerializedData();
//
// The builder collects all separator keys, then in Finish() it:
//  1. Determines the optimal cutoff level between dense and sparse encoding.
//  2. Constructs LOUDS-Dense bitvectors for levels [0, cutoff).
//  3. Constructs LOUDS-Sparse bitvectors for levels [cutoff, max_depth).
//  4. Serializes everything into a flat buffer.
//
// All keys must be added in sorted order (according to the comparator used
// by the SST file).
// ============================================================================
class LoudsTrieBuilder {
 public:
  LoudsTrieBuilder();

  // Add a separator key and its associated data block handle.
  // Keys must be added in sorted (ascending) order.
  void AddKey(const Slice& key, const TrieBlockHandle& handle);

  // Finalize the trie construction. After this call, GetSerializedData()
  // returns the serialized trie.
  void Finish();

  // Get the serialized trie data. Valid only after Finish().
  Slice GetSerializedData() const { return Slice(serialized_data_); }

  // Number of keys (leaves) in the trie.
  uint64_t NumKeys() const { return static_cast<uint64_t>(keys_.size()); }

 private:
  // Determine the optimal cutoff level between dense and sparse encoding.
  // Returns the first level where sparse encoding is more space-efficient.
  uint32_t ComputeCutoffLevel() const;

  // Serialize all trie data structures into serialized_data_.
  void SerializeAll();

  // ---- Input data ----
  std::vector<std::string> keys_;
  std::vector<TrieBlockHandle> handles_;

  // ---- Trie structure (built during Finish()) ----

  // Cutoff level: levels [0, cutoff_level_) use dense, rest use sparse.
  uint32_t cutoff_level_;

  // Max key depth (length of longest key).
  uint32_t max_depth_;

  // LOUDS-Dense bitvectors (all levels concatenated into single bitvectors):
  //   d_labels_: 256-bit bitmaps concatenated for all nodes across all dense
  //     levels. Bit (node_cumulative_offset + label) is set if node has
  //     child with that label.
  //   d_has_child_: One bit per set bit in d_labels_. Set if the child
  //     is an internal node (has further children), clear if it's a leaf.
  //   d_is_prefix_key_: One bit per node across all dense levels. Set if
  //     the path from root to this node forms a valid key (prefix match).
  BitvectorBuilder d_labels_;
  BitvectorBuilder d_has_child_;
  BitvectorBuilder d_is_prefix_key_;

  // LOUDS-Sparse arrays (all sparse levels concatenated):
  //   s_labels_: Byte labels of all edges, in level-order.
  //   s_has_child_: One bit per label. Set if the child is internal.
  //   s_louds_: One bit per label. Set at the first label of each node
  //     (marks node boundaries in the label array).
  //   s_is_prefix_key_: One bit per node in the sparse region. Set if the
  //     path to this node forms a valid key.
  std::vector<uint8_t> s_labels_;
  BitvectorBuilder s_has_child_;
  BitvectorBuilder s_louds_;
  BitvectorBuilder s_is_prefix_key_;

  // Total number of leaves in dense and sparse sections.
  uint64_t dense_leaf_count_;
  uint64_t sparse_leaf_count_;

  // Total number of nodes in all dense levels combined.
  uint64_t dense_node_count_;

  // Number of sparse root nodes: internal children at the last dense level
  // that cross into the sparse region. See LoudsTrie::dense_child_count_.
  uint64_t dense_child_count_;

  // ---- Serialized output ----
  std::string serialized_data_;
};

// ============================================================================
// LoudsTrie: Immutable LOUDS-encoded trie for reading.
//
// Deserialized from a flat buffer (e.g., read from an SST meta-block).
// Supports Seek (find the first leaf >= target key) and Next (advance to
// the next leaf in sorted order).
//
// The trie does NOT own the underlying data when initialized from external
// memory (e.g., a block cache entry). The caller must ensure the data remains
// valid for the lifetime of this object.
//
// The trie is organized as:
// - Dense levels [0, cutoff_level_): bitvectors d_labels_, d_has_child_,
//   d_is_prefix_key_ with 256-bit bitmaps per node.
// - Sparse levels [cutoff_level_, max_depth_]: arrays s_labels_,
//   s_has_child_, s_louds_, s_is_prefix_key_.
// ============================================================================
class LoudsTrie {
 public:
  LoudsTrie();

  // LoudsTrie contains Bitvector members (which hold raw pointers into
  // owned or external memory) and a raw pointer to sparse labels data.
  // Copying would create dangling pointers or aliased external references.
  // Move relies on std::string move preserving the buffer address for
  // strings exceeding the SSO threshold, which is always true for trie
  // data (hundreds to thousands of bytes). All major implementations
  // (libc++, libstdc++, MSVC STL) guarantee this for large strings.
  LoudsTrie(const LoudsTrie&) = delete;
  LoudsTrie& operator=(const LoudsTrie&) = delete;
  LoudsTrie(LoudsTrie&&) = default;
  LoudsTrie& operator=(LoudsTrie&&) = default;

  // Initialize from serialized data. Returns Status::OK() on success,
  // or Status::Corruption() if the data is malformed.
  Status InitFromData(const Slice& data);

  // ---- Accessors ----
  uint64_t NumKeys() const { return num_keys_; }
  uint32_t CutoffLevel() const { return cutoff_level_; }
  uint32_t MaxDepth() const { return max_depth_; }

  // Get the block handle for the i-th leaf (0-indexed).
  TrieBlockHandle GetHandle(uint64_t leaf_index) const;

  // Whether this trie has path-compression chains. Used by the iterator
  // to select a specialized Seek implementation at construction time,
  // avoiding any per-level overhead when chains are absent.
  bool HasChains() const { return !s_chain_lens_.empty(); }

  // Approximate heap memory used by auxiliary data structures (child position
  // lookup tables). Does not include the serialized data itself (which is
  // typically owned by the block cache).
  size_t ApproximateAuxMemoryUsage() const {
    return (s_child_start_pos_.capacity() + s_child_end_pos_.capacity()) *
           sizeof(uint32_t);
  }

  // Allow the iterator to access internal bitvectors directly for
  // performance-critical rank/select operations during traversal.
  friend class LoudsTrieIterator;

 private:
  // Number of keys (leaves).
  uint64_t num_keys_;

  // Cutoff level between dense and sparse.
  uint32_t cutoff_level_;

  // Maximum key depth.
  uint32_t max_depth_;

  // Dense leaf count (leaves in levels [0, cutoff_level_)).
  uint64_t dense_leaf_count_;

  // Total number of nodes across all dense levels.
  uint64_t dense_node_count_;

  // Number of sparse root nodes: internal children at the last dense level
  // (cutoff_level_ - 1) that cross into the sparse region. Used to offset
  // sparse node numbering so that sparse nodes are numbered after all dense
  // nodes. When cutoff_level_ == 0, this is set to 1 (the root itself).
  uint64_t dense_child_count_;

  // LOUDS-Dense bitvectors (all dense levels concatenated).
  Bitvector d_labels_;
  Bitvector d_has_child_;
  Bitvector d_is_prefix_key_;

  // LOUDS-Sparse (all sparse levels concatenated).
  const uint8_t* s_labels_data_;
  uint64_t s_labels_size_;
  Bitvector s_has_child_;
  Bitvector s_louds_;
  Bitvector s_is_prefix_key_;

  // SuRF-style child position lookup tables for Select-free traversal.
  // Instead of computing FindNthOneBit(node_num) during traversal, we
  // precompute child start/end positions indexed by internal label rank. This
  // allows traversal using only Rank1 (O(1)) and array lookup (O(1)).
  //
  // For the k-th internal label (has_child[pos]=1, where k = Rank1(pos+1)-1):
  //   s_child_start_pos_[k] = start position of child node
  //   s_child_end_pos_[k] = end position (exclusive) of child node
  //
  // Memory overhead: 8 bytes per internal node (2 x uint32_t).
  std::vector<uint32_t> s_child_start_pos_;
  std::vector<uint32_t> s_child_end_pos_;

  // Path compression: chain metadata for fanout-1 chains in the sparse region.
  //
  // A "chain" is a sequence of >= 2 consecutive fanout-1 nodes (nodes with
  // exactly one label that is internal) starting from the child of an internal
  // label. Chains are common in tries with long shared prefixes (e.g.,
  // zero-padded numeric keys, URL paths).
  //
  // For the k-th internal label (same indexing as s_child_start_pos_):
  // Storage uses a bitmap (1 bit per internal label) for O(1) chain detection,
  // plus compact arrays indexed by chain ordinal (Rank1 on the bitmap).
  //
  // Lookup during Seek:
  //   1. s_chain_bitmap_.GetBit(child_idx) — has chain?
  //   2. chain_idx = s_chain_bitmap_.Rank1(child_idx) - 1
  //   3. s_chain_lens_[chain_idx], s_chain_suffix_offsets_[chain_idx], etc.
  //
  // Space overhead: 1 bit per internal label (bitmap) + 10 bytes per chain
  // (offset + len + end_child_idx) + suffix bytes. For key sets with few
  // chains (e.g., random hex), overhead is < 1 byte per internal label.
  Bitvector s_chain_bitmap_;
  std::vector<uint32_t> s_chain_suffix_offsets_;
  std::vector<uint16_t> s_chain_lens_;
  std::vector<uint32_t> s_chain_end_child_idx_;
  const uint8_t* s_chain_suffix_data_;
  uint64_t s_chain_suffix_size_;

  // Block handles: packed uint32_t arrays for data block offsets and sizes.
  // BFS leaf order does not necessarily match key-sorted order (deeper leaves
  // appear later in BFS even if they precede shallower leaves
  // lexicographically), so offsets are NOT monotonically non-decreasing and
  // cannot use Elias-Fano encoding. Instead, we store offsets and sizes as
  // packed uint32_t arrays for O(1) random access.
  //
  // uint32_t limits individual values to ~4 GB, which is sufficient since
  // RocksDB SST files are typically 64 MB to 1 GB and never exceed 4 GB.
  const uint32_t* handle_offsets_;
  const uint32_t* handle_sizes_;

  // Aligned copy of the serialized trie data, used when the input data from
  // the block reader is not 8-byte aligned (e.g., mmap at an unaligned file
  // offset). All Bitvector and raw pointer members reference this buffer
  // when non-empty. std::string::data() returns memory from new[]/malloc,
  // which is aligned to at least alignof(max_align_t) >= 8.
  std::string aligned_copy_;
};

// ============================================================================
// LoudsTrieIterator: Iterates over the leaves of a LoudsTrie.
//
// Supports forward-only iteration: Seek(key) + Next().
// Reconstructs the separator key from the trie path at each position.
//
// The iterator maintains a stack of positions through the trie (one per
// level from root to current leaf). This enables:
// - Key reconstruction: collecting the label byte at each level.
// - Backtracking for Next(): pop to parent, advance to next sibling.
// - Leaf ordinal computation: using rank formulas on bitvectors.
//
// Design follows the SuRF reference implementation for correctness.
// ============================================================================
class LoudsTrieIterator {
 public:
  explicit LoudsTrieIterator(const LoudsTrie* trie);

  // Seek to the first leaf whose key is >= `target`.
  // Returns true if positioned on a valid leaf.
  //
  // Dispatches to a specialized implementation selected at construction time
  // based on whether the trie has path-compression chains. This eliminates
  // all chain-related code from the instruction cache when chains are absent,
  // following the same pattern as RocksDB's BlockIter::ParseNextKey template.
  bool Seek(const Slice& target) {
    if (has_chains_) {
      return SeekImpl<true>(target);
    }
    return SeekImpl<false>(target);
  }

  // Advance to the next leaf in sorted order.
  // Returns true if positioned on a valid leaf.
  bool Next();

  // Check if the iterator is positioned on a valid leaf.
  bool Valid() const { return valid_; }

  // Get the current separator key. Valid only when Valid() is true.
  // The returned Slice is valid until the next Seek/Next call.
  Slice Key() const { return Slice(key_buf_.get(), key_len_); }

  // Get the current leaf index (for mapping to block handles).
  uint64_t LeafIndex() const { return leaf_index_; }

  // Get the block handle for the current leaf.
  TrieBlockHandle Value() const;

 private:
  // Position within a single trie level. The iterator maintains a stack
  // of these from root to the current position.
  //
  // Packed into 8 bytes by encoding the is_dense flag in bit 63 of the
  // position value. Since bitvector positions and label array indices are
  // well under 2^63, this is safe. This halves the per-level memory from
  // 16 bytes (with alignment padding) to 8 bytes, improving cache
  // utilization for the path_ stack.
  struct LevelPos {
    // Position in the bitvector/label array at this level, with the
    // is_dense flag encoded in the high bit (bit 63).
    // - Dense: bit position in d_labels_ (= cumulative_node_offset * 256 +
    //   label_byte). The label byte is pos % 256.
    // - Sparse: index into s_labels_ array.
    uint64_t pos_and_flag;

    static constexpr uint64_t kDenseFlag = uint64_t(1) << 63;

    uint64_t pos() const { return pos_and_flag & ~kDenseFlag; }
    bool is_dense() const { return (pos_and_flag & kDenseFlag) != 0; }

    static LevelPos MakeDense(uint64_t p) { return {p | kDenseFlag}; }
    static LevelPos MakeSparse(uint64_t p) { return {p}; }
  };

  // --- Dense level helpers ---

  // Seek within a dense node for a target label byte.
  // Sets result to the position of the label if found, or the position of
  // the next label >= target_byte if not found.
  // Returns true if the exact label was found, false if we landed on a
  // label > target_byte (or no label exists).
  bool DenseSeekLabel(uint64_t node_num, uint8_t target_byte,
                      uint64_t* out_pos);

  // Compute the child node number for a dense internal child at `pos`.
  // child_node_num = rank1(d_has_child_, rank1(d_labels_, pos+1) - 1 + 1)
  // but simplified: rank1(d_has_child_, label_rank) where label_rank is the
  // rank of the label among set bits.
  uint64_t DenseChildNodeNum(uint64_t pos) const;

  // Same as DenseChildNodeNum but takes a pre-computed label_rank to avoid
  // redundant Rank1(d_labels_) calls in hot paths where label_rank was
  // already computed for has_child checking.
  uint64_t DenseChildNodeNumFromRank(uint64_t label_rank) const;

  // Compute the leaf ordinal for a dense leaf at `pos`.
  // leaf_idx = rank1(d_labels_, pos+1) - rank1(d_has_child_,
  //   rank1(d_labels_, pos+1)) + rank1(d_is_prefix_key_, node_num+1) - 1
  uint64_t DenseLeafIndex(uint64_t pos) const;

  // Same as DenseLeafIndex but takes a pre-computed label_rank.
  uint64_t DenseLeafIndexFromRank(uint64_t pos, uint64_t label_rank) const;

  // Same as DenseLeafIndexFromRank but also takes a pre-computed
  // d_has_child_.Rank1(label_rank + 1) to avoid redundant rank call.
  uint64_t DenseLeafIndexFromRankAndHasChildRank(uint64_t pos,
                                                 uint64_t label_rank,
                                                 uint64_t has_child_rank) const;

  // Compute the leaf ordinal for a dense prefix key at node `node_num`.
  // A prefix key leaf comes before any child leaves of that node.
  uint64_t DensePrefixKeyLeafIndex(uint64_t node_num) const;

  // Get the node number for a dense position.
  // node_num = pos / 256
  uint64_t DenseNodeNum(uint64_t pos) const { return pos / 256; }

  // --- Sparse level helpers ---

  // Seek within a sparse node starting at `node_start_pos` for label byte.
  // Returns the position in s_labels_ if found, or the position of the next
  // label >= target_byte. Sets `found_exact` accordingly.
  // `node_end_pos` is one past the last label position of this node.
  bool SparseSeekLabel(uint64_t node_start_pos, uint64_t node_end_pos,
                       uint8_t target_byte, uint64_t* out_pos);

  // Compute the child node number for a sparse internal child at `pos`.
  // child_node_num = rank1(s_has_child_, pos+1) - 1 + dense_child_count_
  // (offset by dense_child_count because sparse nodes are numbered after
  // all dense nodes).
  uint64_t SparseChildNodeNum(uint64_t pos) const;

  // Compute the leaf ordinal for a sparse leaf at `pos`.
  uint64_t SparseLeafIndex(uint64_t pos) const;

  // Same as SparseLeafIndex but takes a pre-computed
  // s_has_child_.Rank1(pos + 1) to avoid redundant rank call.
  uint64_t SparseLeafIndexFromHasChildRank(uint64_t pos,
                                           uint64_t has_child_rank) const;

  // Compute the leaf ordinal for a sparse prefix key at sparse node
  // `sparse_node_num`. The sparse_node_num is 0-indexed among sparse nodes
  // only (not including dense nodes).
  uint64_t SparsePrefixKeyLeafIndex(uint64_t sparse_node_num) const;

  // Get the sparse node number (0-indexed among sparse nodes) from a
  // position in the s_labels_ array.
  uint64_t SparseNodeNum(uint64_t pos) const;

  // Get the start position (in s_labels_) for sparse node `sparse_node_num`.
  uint64_t SparseNodeStartPos(uint64_t sparse_node_num) const;

  // Get the end position (one past last label) for a sparse node starting
  // at `start_pos`.
  uint64_t SparseNodeEndPos(uint64_t start_pos) const;

  // --- Traversal helpers ---

  // Descend from the given node to the leftmost leaf in its subtree,
  // pushing entries onto path_ and building key_buf_. Sets
  // leaf_index_ and valid_. Returns true if a leaf was found.
  bool DescendToLeftmostLeaf(bool in_dense, uint64_t node_num);

  // Advance to the next valid leaf by backtracking up the trie path
  // and finding the next sibling label, then descending to the leftmost
  // leaf in that subtree. Used by Next().
  // Returns true if a next leaf was found.
  bool Advance();

  // Seek implementation, templated on whether path-compression chains exist.
  // When kHasChains=false, the compiler eliminates ALL chain-related code,
  // keeping the i-cache footprint minimal for tries without chains.
  // This follows the same pattern as RocksDB's BlockIter::ParseNextKey.
  template <bool kHasChains>
  bool SeekImpl(const Slice& target);

  // True if the trie has path-compression chains. Set once in the constructor
  // and used by Seek() to dispatch to the correct specialization.
  bool has_chains_;

  const LoudsTrie* trie_;
  bool valid_;
  uint64_t leaf_index_;

  // The reconstructed key at the current position. Each byte corresponds
  // to a level in path_. For dense levels, the byte is pos % 256; for
  // sparse levels, the byte is s_labels_[pos].
  //
  // Key reconstruction appends one byte per trie level in the Seek/Next
  // hot loop, so the append operation must be as cheap as possible — a
  // single inlined store + increment with no function call overhead. The
  // buffer is heap-allocated once in the constructor to MaxDepth()+1 bytes.
  std::unique_ptr<char[]> key_buf_;
  uint32_t key_len_;
  uint32_t key_cap_;

  // Stack of positions from root to current leaf. path_[i] holds the
  // position at depth i in the trie. The depth equals key_len_ when
  // positioned on a child leaf, or key_len_ when positioned on a prefix
  // key (in which case the last path_ entry's label is not appended to
  // key_buf_ since the node itself is the key).
  //
  // For a prefix key, we mark it by setting is_at_prefix_key_ = true and
  // the path_ only goes up to the prefix key node level (no label entry
  // for the prefix key itself since the key terminates at the node, not
  // at a child edge).
  //
  // Uses autovector with 24 inline slots to avoid heap allocation for
  // tries up to 24 levels deep. Most real-world key sets have depth < 24.
  autovector<LevelPos, 24> path_;

  // True if the current leaf is a prefix key (the key terminates at a
  // node that also has children). In this case, path_.size() == depth
  // of the node and key_buf_[0..key_len_) holds the prefix key value.
  bool is_at_prefix_key_;
};

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
