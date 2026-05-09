# MemTable Lookup

**Files:** `db/memtable.cc`, `db/memtable.h`, `db/memtable_list.cc`, `db/memtable_list.h`

## MemTable::Get() Flow

The memtable lookup follows a multi-stage pipeline for each key:

Step 1: **Empty check** -- If `IsEmpty()` returns true (no entries in the memtable, indicated by `first_seqno_ == 0`), `Get()` returns false immediately (key not found).

Step 2: **Range deletion check** -- Create a range tombstone iterator and check if the key is covered by any range tombstone. If covered, update `max_covering_tombstone_seq` with the tombstone's sequence number. This does not immediately delete the key -- it records the highest covering tombstone sequence for later comparison in `SaveValue()`.

Step 3: **Bloom filter check** -- If memtable bloom filters are enabled:
- `memtable_whole_key_filtering` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`): Check the full user key against the bloom filter
- Prefix mode: Extract the prefix using `prefix_extractor` and check the prefix bloom
- If the bloom filter says "definitely not present", skip the skiplist search entirely and return false (key not found in this memtable)

Step 4: **Skiplist search** -- Call `GetFromTable()` which invokes `MemTableRep::Get()`. The skiplist positions itself at the first entry matching the user key and invokes the `SaveValue()` callback for each matching entry.

## SaveValue() Callback

`SaveValue()` processes each skiplist entry found during the memtable search:

Step 1: **Parse entry** -- Extract `user_key`, `sequence`, `type`, and `value` from the encoded memtable entry.

Step 2: **User key match** -- Compare the entry's user key with the lookup key. If they differ, return true (continue iteration to next entry).

Step 3: **Snapshot visibility** -- Call `CheckCallback(seq)` to verify the entry is visible to the reader's snapshot. If not visible, skip to the next entry.

Step 4: **Range tombstone integration** -- If `max_covering_tombstone_seq > seq`, the entry is covered by a range tombstone with a higher sequence number. Treat it as a deletion.

Step 5: **Type dispatch** -- Handle the entry based on its value type:

| Type | Action |
|------|--------|
| `kTypeValue` | Return the value, search complete |
| `kTypeValuePreferredSeqno` | Return the value with preferred seqno handling, search complete |
| `kTypeDeletion` | Return NotFound, search complete |
| `kTypeDeletionWithTimestamp` | Return NotFound, search complete |
| `kTypeSingleDeletion` | Return NotFound, search complete |
| `kTypeMerge` | Push operand to `MergeContext`, continue if merge not finalized |
| `kTypeBlobIndex` | Return blob reference for later retrieval, search complete |
| `kTypeWideColumnEntity` | Return wide-column entity, search complete |

## Merge Handling in MemTable

When `kTypeMerge` is encountered, the merge operand is pushed onto `MergeContext`. The search continues to find either:
- A base value (`kTypeValue`) -- triggers `TimedFullMerge()` with base value + operands
- A deletion -- triggers `TimedFullMerge()` without base value
- End of memtable -- merge state (`merge_in_progress`) is passed to the next layer (immutable memtable or SST files)

## Immutable MemTable Search

`MemTableListVersion::Get()` in `db/memtable_list.cc` searches immutable memtables in newest-to-oldest order. The `memlist_` vector maintains this ordering.

For each immutable memtable:

1. Call `memtable->Get()` with the same `SaveValue()` logic
2. If a final value is found (`done == true`), return immediately
3. If merge is in progress, continue to the next memtable to accumulate more operands
4. Track the first sequence number found across all memtables

**Important:** The newest-to-oldest ordering ensures that the most recent version of a key is found first, enabling early termination when a definitive result (Put or Delete) is encountered.

## MemTable Bloom Filter Details

Memtable bloom filters are distinct from SST file bloom filters:

- Configured via `memtable_prefix_bloom_size_ratio` and `memtable_whole_key_filtering` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`)
- Built incrementally as entries are inserted
- Not persisted -- rebuilt for each memtable
- Prefix bloom uses the `prefix_extractor` to extract key prefixes

When `memtable_whole_key_filtering` is true and the bloom filter indicates the key is not present, the entire skiplist search is skipped. This is a significant optimization for workloads with many point lookups that miss in the memtable.

When both `memtable_whole_key_filtering` and `prefix_extractor` are set, only whole-key filtering is used for `Get()` to save CPU. The prefix bloom check is skipped entirely in this case.

Note: Bloom filter false positives cause unnecessary skiplist searches but never incorrect results. False negatives are not possible with a correctly implemented bloom filter.
