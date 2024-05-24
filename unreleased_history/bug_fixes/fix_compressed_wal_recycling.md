* Fixed a false positive `Status::Corruption` reported when reopening a DB that used `DBOptions::recycle_log_file_num > 0` and `DBOptions::wal_compression != kNoCompression`.
