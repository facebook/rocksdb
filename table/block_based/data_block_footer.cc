//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based/data_block_footer.h"

#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// Hash index bit (bit 31)
constexpr uint32_t kHashIndexBit = 1u << 31;

void DataBlockFooter::EncodeTo(std::string* dst) const {
  assert(num_restarts <= kMaxNumRestarts);

  uint32_t packed = num_restarts;
  if (index_type == BlockBasedTableOptions::kDataBlockBinaryAndHash) {
    packed |= kHashIndexBit;
  } else {
    assert(index_type == BlockBasedTableOptions::kDataBlockBinarySearch);
  }

  PutFixed32(dst, packed);
}

Status DataBlockFooter::DecodeFrom(Slice* input) {
  if (input->size() < kMinEncodedLength) {
    return Status::Corruption("Block too small for footer");
  }

  // Decode from the end of the input
  const char* footer_ptr = input->data() + input->size() - kMinEncodedLength;
  uint32_t packed = DecodeFixed32(footer_ptr);

  if (packed & kHashIndexBit) {
    index_type = BlockBasedTableOptions::kDataBlockBinaryAndHash;
    packed &= ~kHashIndexBit;
  } else {
    index_type = BlockBasedTableOptions::kDataBlockBinarySearch;
  }

  // Check for reserved/unrecognized feature bits (anything beyond
  // kMaxNumRestarts)
  if (packed > kMaxNumRestarts) {
    return Status::Corruption(
        "Unrecognized feature in block footer (reserved bits set)");
  }

  num_restarts = packed;

  // Remove the footer from the input slice
  input->remove_suffix(kMinEncodedLength);

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
