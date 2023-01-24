// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::DB methods from Java side.

#include "rocksffi/rocksffi.h"

#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/status.h"

/**
 * @brief get the value at the key, returning a (possibly pinned) slice
 *
 */
extern "C" int rocksdb_ffi_get_pinnable(
    ROCKSDB_NAMESPACE::DB* db, ROCKSDB_NAMESPACE::ReadOptions* read_options,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf, rocksdb_input_slice_t* key,
    rocksdb_pinnable_slice_t* value) {
  ROCKSDB_NAMESPACE::Slice key_slice(key->data, key->size);
  auto value_slice = value->pinnable_slice;
  new ROCKSDB_NAMESPACE::PinnableSlice();
  auto status = db->Get(*read_options, cf, key_slice, value_slice);
  if (status.ok()) {
    value->data = value_slice->data();
    value->size = value_slice->size();
    value->is_pinned = value_slice->IsPinned();
  }
  return status.code();
}

extern "C" int rocksdb_ffi_new_pinnable(rocksdb_pinnable_slice_t* value) {
  value->pinnable_slice = new ROCKSDB_NAMESPACE::PinnableSlice();
  return ROCKSDB_NAMESPACE::Status::Code::kOk;
}

extern "C" int rocksdb_ffi_reset_pinnable(rocksdb_pinnable_slice_t* value) {
  if (value->is_pinned) {
    value->pinnable_slice->Reset();
    value->is_pinned = false;
  }
  return ROCKSDB_NAMESPACE::Status::Code::kOk;
}

extern "C" int rocksdb_ffi_delete_pinnable(rocksdb_pinnable_slice_t* value) {
  auto* value_slice = value->pinnable_slice;
  delete value_slice;
  return ROCKSDB_NAMESPACE::Status::Code::kOk;
}

/**
 * @brief get, returning the value copied into the supplied value buffer
 *
 * Because there is only ever 1 transition of the Java/C++ boundary,
 * this may be more efficient than the pinnable slice version for small value
 * sizes.
 */
extern "C" int rocksdb_ffi_get_output(
    ROCKSDB_NAMESPACE::DB* db, ROCKSDB_NAMESPACE::ReadOptions* read_options,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf, rocksdb_input_slice_t* key,
    rocksdb_output_slice_t* value) {
  ROCKSDB_NAMESPACE::Slice key_slice(key->data, key->size);
  ROCKSDB_NAMESPACE::PinnableSlice value_slice;
  auto status = db->Get(*read_options, cf, key_slice, &value_slice);
  if (status.ok()) {
    auto copy_size = std::min(value_slice.size(), value->capacity);
    std::memcpy(value->data, value_slice.data(), copy_size);
    value->size = value_slice.size();
  }
  return status.code();
}

extern "C" int rocksdb_ffi_identity(int input) { return input + 1; }
