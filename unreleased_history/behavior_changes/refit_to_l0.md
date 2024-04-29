`CompactRange()` with `CompactRangeOptions::change_level = true` and `CompactRangeOptions::target_level = 0` that ends up moving more than 1 file from non-L0 to L0 will return `Status::Aborted()`.
