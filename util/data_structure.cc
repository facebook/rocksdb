//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/data_structure.h"

#include "util/math.h"

namespace ROCKSDB_NAMESPACE::detail {

int CountTrailingZeroBitsForSmallEnumSet(uint64_t v) {
  return CountTrailingZeroBits(v);
}

}  // namespace ROCKSDB_NAMESPACE::detail
