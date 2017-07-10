//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef ROCKSDB_LITE

#include "rocksdb/convenience.h"

#include "db/db_impl.h"

namespace rocksdb {

void CancelAllBackgroundWork(DB* db, bool wait) {
  (dynamic_cast<DBImpl*>(db->GetRootDB()))->CancelAllBackgroundWork(wait);
}

Status DeleteFilesInRange(DB* db, ColumnFamilyHandle* column_family,
                          const Slice* begin, const Slice* end) {
  return (dynamic_cast<DBImpl*>(db->GetRootDB()))
      ->DeleteFilesInRange(column_family, begin, end);
}

Status VerifyChecksum(const std::string& file_path) {
  unique_ptr<RandomAccessFile> file;
  uint64_t file_size;

  Options options;
  ImmutableCFOptions ioptions(options);
  EnvOptions env_options;
  InternalKeyComparator internal_comparator(BytewiseComparator());

  Status s = ioptions.env->NewRandomAccessFile(file_path, &file, env_options);
  if (s.ok()) {
    s = ioptions.env->GetFileSize(file_path, &file_size);
  } else {
    return s;
  }
  unique_ptr<TableReader> table_reader;
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(file), file_path));
  s = ioptions.table_factory->NewTableReader(
      TableReaderOptions(ioptions, env_options, internal_comparator,
                         false /* skip_filters */, -1 /* level */),
      std::move(file_reader), file_size, &table_reader,
      false /* prefetch_index_and_filter_in_cache */);
  if (!s.ok()) {
    return s;
  }
  s = table_reader->VerifyChecksum();
  return s;
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
