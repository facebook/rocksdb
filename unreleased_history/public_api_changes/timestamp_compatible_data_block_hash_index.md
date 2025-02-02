Add `Comparator::KeysAreBytewiseComparableOtherThanTimestamp` to allow column
families that use a timestamp-aware comparator with otherwise
bytewise-comparable keys to enable
`BlockBasedTableOptions::kDataBlockBinaryAndHash`
