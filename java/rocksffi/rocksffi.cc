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

extern "C" int rocksdb_ffi_get(ROCKSDB_NAMESPACE::DB* db,
                               ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf,
                               rocksdb_input_slice_t* key,
                               rocksdb_output_slice_t* value) {
  ROCKSDB_NAMESPACE::ReadOptions read_options;
  ROCKSDB_NAMESPACE::Slice key_slice(key->data, key->size);
  ROCKSDB_NAMESPACE::PinnableSlice* value_slice =
      new ROCKSDB_NAMESPACE::PinnableSlice();
  std::cout << "key size: " << key->size << ", key data: " << key->data
            << std::endl;
  auto status = db->Get(read_options, cf, key_slice, value_slice);
  if (status.ok()) {
    std::cout << "value size: " << value_slice->size()
              << ", value data: " << value_slice->data() << std::endl;
    value->data = value_slice->data();
    value->size = value_slice->size();
    value->pinnable_slice = value_slice;
  } else {
    delete value_slice;
  }
  return status.code();
}

extern "C" int rocksdb_ffi_reset_output(rocksdb_output_slice_t* /*value*/) {
  return ROCKSDB_NAMESPACE::Status::Code::kOk;
}
