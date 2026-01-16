Fixed an infinite compaction loop bug with User-Defined Timestamps (UDT) where bottommost files were repeatedly marked for compaction even though their timestamp could not be collapsed.
