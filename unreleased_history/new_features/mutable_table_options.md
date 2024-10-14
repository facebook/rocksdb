* All "simple" options in `BlockBasedTableOptions` are now mutable with `DB::SetOptions()`. For now, "simple" only includes non-pointer options that are 64 bits or less.
