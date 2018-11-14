//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <string>

#include "rocksdb/iterator.h"
#include "rocksdb/table.h"

namespace rocksdb {

class SstKVIterator : public Iterator {
 public:
  SstKVIterator() {}
  virtual ~SstKVIterator() {}
  // If `sequence` is not nullptr, it will be set as the SequenceNumber of
  // current entry. If `type` is not nullptr, it will be set as the ValueType of
  // current entry.
  virtual Slice key(SequenceNumber* sequence, int* type) const = 0;
  // key() will be implemented by key(nullptr, nullptr).
  virtual Slice key() const = 0;
};

// SstFileReader is used to read sst files.
// SstFileReader may be safely accessed from multiple threads
// without external synchronization.
class SstFileReader {
 public:
  // `file_name` specifies path of the read-only sst file to be accessed.
  // `options` to control the behavior of TableReader.
  // `comparator` provides a total order across slices that are used as keys in
  // sstable.
  SstFileReader(const std::string& file_name, Options options = Options(),
                const Comparator* comparator = BytewiseComparator());

  ~SstFileReader();

  // Returns an iterator over this sst file.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  //
  // Caller should delete the iterator when it is no longer needed.
  // The returned iterator should be deleted before this SstFileReader is
  // deleted.
  SstKVIterator* NewIterator(const ReadOptions& read_options,
                             const SliceTransform* prefix_extractor,
                             Arena* arena = nullptr, bool skip_filters = false,
                             bool for_compaction = false);
  // If `verify_checksum` is true, all data read from underlying storage will be
  // verified against corresponding checksums.
  // If `cache` is true, the "data block"/"index block"" read for this iteration
  // be placed in block cache. Both parameters will be used to create
  // ReadOptions.
  SstKVIterator* NewIterator(bool cksum, bool cache,
                             const SliceTransform* prefix_extractor);

  // If `prefix_extractor` is not specified, then will use prefix_extractor of
  // `options` in constructor.
  SstKVIterator* NewIterator(bool cksum, bool cache = false);

  // Get table properties.
  Status ReadTableProperties(
      std::shared_ptr<const TableProperties>* table_properties);

  // Returns init result of SstFileReader.
  // It can be invoked after constructor to tell whether is ready to use.
  Status getStatus();

  // Check whether there is corruption in this file.
  Status VerifyChecksum();

 private:
  struct Rep;
  std::unique_ptr<Rep> rep_;
};
}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
