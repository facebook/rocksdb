Fixed `DB::ClipColumnFamily()` deleting a non-L0 SST file whose largest key equals `begin_key`, which lost keys at the start of the kept range.
