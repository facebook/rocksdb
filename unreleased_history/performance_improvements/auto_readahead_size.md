Added additional improvements in tuning readahead_size during Scans when auto_readahead_size is enabled. However it's not supported with Iterator::Prev operation and will return NotSupported error.
