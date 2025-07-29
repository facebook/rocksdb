Fixed a bug in remote compaction that may mistakenly delete live SST file(s) during the cleanup phase when no keys survive the compaction (all expired)
