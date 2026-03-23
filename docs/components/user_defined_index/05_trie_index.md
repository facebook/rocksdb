# Trie Index Implementation

**Files:** `utilities/trie_index/trie_index_factory.h`, `utilities/trie_index/trie_index_factory.cc`, `utilities/trie_index/louds_trie.h`, `utilities/trie_index/louds_trie.cc`, `utilities/trie_index/bitvector.h`, `utilities/trie_index/bitvector.cc`

## Overview

The trie index is the bundled UDI implementation. It builds a Fast Succinct Trie (FST) from separator keys during SST construction, using a LOUDS (Level-Order Unary Degree Sequence) encoding inspired by the SuRF paper (Zhang et al., SIGMOD 2018). The trie exploits common key prefixes to achieve significant space reduction compared to the default binary search index.

## LOUDS Encoding

The trie uses a hybrid encoding with two regions:

### LOUDS-Dense (Upper Levels)

For levels close to the root where fanout tends to be high. Each node uses a 256-bit bitmap — one bit per possible byte label.

Key data structures (all levels concatenated into single bitvectors):

| Bitvector | Description |
|-----------|-------------|
| `d_labels_` | 256-bit bitmaps per node; bit `(node_num * 256 + label)` set if edge exists |
| `d_has_child_` | One bit per set bit in `d_labels_`; set if child is internal (has further children) |
| `d_is_prefix_key_` | One bit per node; set if path from root to this node is a valid key |

Child lookup is O(1) via popcount (`Rank1`) operations.

### LOUDS-Sparse (Lower Levels)

For deeper levels where fanout is typically low. Uses compact label arrays instead of bitmaps.

| Structure | Description |
|-----------|-------------|
| `s_labels_` | Byte labels of all edges, in level-order |
| `s_has_child_` | One bit per label; set if child is internal |
| `s_louds_` | One bit per label; set at first label of each node (marks boundaries) |
| `s_is_prefix_key_` | One bit per node; set if path is a valid key |

### Cutoff Level Selection

The boundary between dense and sparse levels is chosen to minimize total space. Dense levels use 256 bits per node regardless of fanout; sparse levels use approximately 10 bits per edge. When average fanout drops below approximately 25 children per node, sparse becomes more efficient. The `ComputeCutoffLevel()` method finds this crossover point.

## Path Compression

Chains of fanout-1 nodes (nodes with exactly one internal child) are compressed in the sparse region. These are common with long shared prefixes (e.g., zero-padded numeric keys, URL paths).

For each chain:

| Data | Description |
|------|-------------|
| `s_chain_bitmap_` | 1 bit per internal label; set if a chain starts here |
| `s_chain_lens_[]` | Length of each chain |
| `s_chain_suffix_offsets_[]` | Offset into suffix data for the chain's byte sequence |
| `s_chain_end_child_idx_[]` | Internal label index at chain end |
| `s_chain_suffix_data_` | Concatenated chain byte sequences |

During Seek, chains are matched against the target key in a single comparison, avoiding per-level traversal for the chain's length. Overhead is approximately 1 bit per internal label (bitmap) plus 10 bytes per chain.

The iterator uses template specialization (`SeekImpl<kHasChains>`) to eliminate all chain-related code from the instruction cache when chains are absent.

## Block Handle Storage

Leaf-indexed: each trie leaf maps to a data block handle via packed `uint32_t` arrays:

- `handle_offsets_[i]`: File offset of the i-th leaf's data block
- `handle_sizes_[i]`: Size of the i-th leaf's data block

`uint32_t` limits individual values to approximately 4 GB, which is sufficient since RocksDB SST files never exceed 4 GB.

Note: BFS leaf order does not necessarily match key-sorted order (deeper leaves appear later in BFS even if they precede shallower leaves lexicographically). Offsets are NOT monotonically non-decreasing.

## Serialization Format

The trie is serialized into a flat buffer for zero-copy reads:

**Fixed Header (56 bytes):**
- Magic number (4 bytes)
- Format version (4 bytes)
- `num_keys` (8 bytes)
- `cutoff_level` (4 bytes)
- `max_depth` (4 bytes)
- `dense_leaf_count` (8 bytes)
- `dense_node_count` (8 bytes)
- `dense_child_count` (8 bytes)
- Flags (4 bytes): `has_seqno_encoding` flag
- Reserved padding (4 bytes, for 8-byte alignment)

**LOUDS Trie Body:** Dense bitvectors followed by sparse arrays, each with its own length prefix.

**Block Handles:** `uint32_t` offset and size arrays, padded to 8-byte alignment.

**Seqno Side-Table (if `has_seqno_encoding`):** See Chapter 4.

### Alignment

When the input data from the block reader is not 8-byte aligned (e.g., mmap at an unaligned file offset), `LoudsTrie::InitFromData()` creates an aligned copy in `aligned_copy_`. All bitvector and raw pointer members reference this buffer.

## Comparator Requirement

The `TrieIndexFactory` requires `BytewiseComparator` and rejects any other comparator with `Status::NotSupported`. This is because the trie traverses keys byte-by-byte in lexicographic order; non-bytewise comparators would produce separator keys in a different order than the trie's byte-level traversal.

The UDI framework itself supports arbitrary comparators via `UserDefinedIndexOption::comparator`. Custom UDI implementations can use any comparator.

## Auxiliary Data Structures

During `InitFromData()`, the trie precomputes Select-free child position lookup tables:

- `s_child_start_pos_[k]`: Start position of the k-th internal child's node
- `s_child_end_pos_[k]`: End position (exclusive)

These allow traversal using only `Rank1` (O(1)) and array lookup (O(1)), avoiding the slower `FindNthOneBit` (Select) operation. Memory overhead: 8 bytes per internal node.

Approximate auxiliary memory usage is reported by `LoudsTrie::ApproximateAuxMemoryUsage()`.
