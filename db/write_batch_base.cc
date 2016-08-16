//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "rocksdb/write_batch_base.h"

#include <string>

#include "rocksdb/slice.h"

namespace rocksdb {

// Simple implementation of SlicePart variants of Put().  Child classes
// can override these method with more performant solutions if they choose.
void WriteBatchBase::Put(ColumnFamilyHandle* column_family,
                         const SliceParts& key, const SliceParts& value) {
  std::string key_buf, value_buf;
  Slice key_slice(key, &key_buf);
  Slice value_slice(value, &value_buf);

  Put(column_family, key_slice, value_slice);
}

void WriteBatchBase::Put(const SliceParts& key, const SliceParts& value) {
  std::string key_buf, value_buf;
  Slice key_slice(key, &key_buf);
  Slice value_slice(value, &value_buf);

  Put(key_slice, value_slice);
}

void WriteBatchBase::Delete(ColumnFamilyHandle* column_family,
                            const SliceParts& key) {
  std::string key_buf;
  Slice key_slice(key, &key_buf);
  Delete(column_family, key_slice);
}

void WriteBatchBase::Delete(const SliceParts& key) {
  std::string key_buf;
  Slice key_slice(key, &key_buf);
  Delete(key_slice);
}

void WriteBatchBase::SingleDelete(ColumnFamilyHandle* column_family,
                                  const SliceParts& key) {
  std::string key_buf;
  Slice key_slice(key, &key_buf);
  SingleDelete(column_family, key_slice);
}

void WriteBatchBase::SingleDelete(const SliceParts& key) {
  std::string key_buf;
  Slice key_slice(key, &key_buf);
  SingleDelete(key_slice);
}

void WriteBatchBase::DeleteRange(ColumnFamilyHandle* column_family,
                                 const SliceParts& begin_key,
                                 const SliceParts& end_key) {
  std::string begin_key_buf, end_key_buf;
  Slice begin_key_slice(begin_key, &begin_key_buf);
  Slice end_key_slice(end_key, &end_key_buf);
  DeleteRange(column_family, begin_key_slice, end_key_slice);
}

void WriteBatchBase::DeleteRange(const SliceParts& begin_key,
                                 const SliceParts& end_key) {
  std::string begin_key_buf, end_key_buf;
  Slice begin_key_slice(begin_key, &begin_key_buf);
  Slice end_key_slice(end_key, &end_key_buf);
  DeleteRange(begin_key_slice, end_key_slice);
}

void WriteBatchBase::Merge(ColumnFamilyHandle* column_family,
                           const SliceParts& key, const SliceParts& value) {
  std::string key_buf, value_buf;
  Slice key_slice(key, &key_buf);
  Slice value_slice(value, &value_buf);

  Merge(column_family, key_slice, value_slice);
}

void WriteBatchBase::Merge(const SliceParts& key, const SliceParts& value) {
  std::string key_buf, value_buf;
  Slice key_slice(key, &key_buf);
  Slice value_slice(value, &value_buf);

  Merge(key_slice, value_slice);
}

}  // namespace rocksdb
