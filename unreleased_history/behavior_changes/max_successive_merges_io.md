* `DB::Merge()` will only keep merge operand count within `ColumnFamilyOptions::max_successive_merges` when the key's merge operands are all found in memory. It no longer issues filesystem reads.
