// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <string>

#include "utilities/merge_operators/bytesxor.h"

namespace rocksdb {

std::shared_ptr<MergeOperator> MergeOperators::CreateBytesXOROperator() {
  return std::make_shared<BytesXOROperator>();
}

bool BytesXOROperator::Merge(const Slice& key,
                            const Slice* existing_value,
                            const Slice& value,
                            std::string* new_value,
                            Logger* logger) const {
  XOR(existing_value, value, new_value);
  return true;
}

void BytesXOROperator::XOR(const Slice* existing_value,
          const Slice& value, std::string* new_value) const {
  if (!existing_value) {
    new_value->clear();
    new_value->assign(value.data(), value.size());
    return;
  }

  size_t min_size = std::min(existing_value->size(), value.size());
  size_t max_size = std::max(existing_value->size(), value.size());

  new_value->clear();
  new_value->reserve(max_size);

  const char* existing_value_data = existing_value->data();
  const char* value_data = value.data();

  for (size_t i = 0; i < min_size; i++) {
    new_value->push_back(existing_value_data[i] ^ value_data[i]);
  }

  if (existing_value->size() == max_size) {
    for (size_t i = min_size; i < max_size; i++) {
      new_value->push_back(existing_value_data[i]);
    }
  } else {
	  assert(value.size() == max_size);
    for (size_t i = min_size; i < max_size; i++) {
      new_value->push_back(value_data[i]);
    }
  }
}

}  // namespace rocksdb
