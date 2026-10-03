Preserve legacy UDI routing when loading old OPTIONS files, avoid double charging custom index blocks for legacy readers, and reject invalid index modes in standalone SST readers and writers.

Keep cached custom-index Slice views intact when a reader consumes its input. Register custom-index cache helpers without requiring a later change. Trie index size estimates now account for shared prefixes, preventing premature compaction file cuts, and reject data-block offsets or sizes exceeding the 32-bit on-disk limit instead of truncating them.
