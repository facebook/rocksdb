//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <limits>
#include <cassert>
#include "rocksdb/options.h"
#include "rocksdb/immutable_options.h"
#include "util/mutable_cf_options.h"

namespace rocksdb {

namespace {
// Multiple two operands. If they overflow, return op1.
uint64_t MultiplyCheckOverflow(uint64_t op1, int op2) {
  if (op1 == 0) {
    return 0;
  }
  if (op2 <= 0) {
    return op1;
  }
  uint64_t casted_op2 = (uint64_t) op2;
  if (std::numeric_limits<uint64_t>::max() / op1 < casted_op2) {
    return op1;
  }
  return op1 * casted_op2;
}
}  // anonymous namespace

void MutableCFOptions::RefreshDerivedOptions(
    const ImmutableCFOptions& ioptions) {
  max_file_size.resize(ioptions.num_levels);
  level_max_bytes.resize(ioptions.num_levels);
  for (int i = 0; i < ioptions.num_levels; ++i) {
    if (i == 0 && ioptions.compaction_style == kCompactionStyleUniversal) {
      max_file_size[i] = ULLONG_MAX;
      level_max_bytes[i] = max_bytes_for_level_base;
    } else if (i > 1) {
      max_file_size[i] = MultiplyCheckOverflow(max_file_size[i - 1],
                                               target_file_size_multiplier);
      level_max_bytes[i] = MultiplyCheckOverflow(
          MultiplyCheckOverflow(level_max_bytes[i - 1],
                                max_bytes_for_level_multiplier),
          max_bytes_for_level_multiplier_additional[i - 1]);
    } else {
      max_file_size[i] = target_file_size_base;
      level_max_bytes[i] = max_bytes_for_level_base;
    }
  }
}

uint64_t MutableCFOptions::MaxFileSizeForLevel(int level) const {
  assert(level >= 0);
  assert(level < (int)max_file_size.size());
  return max_file_size[level];
}
uint64_t MutableCFOptions::MaxBytesForLevel(int level) const {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  assert(level >= 0);
  assert(level < (int)level_max_bytes.size());
  return level_max_bytes[level];
}
uint64_t MutableCFOptions::MaxGrandParentOverlapBytes(int level) const {
  return MaxFileSizeForLevel(level) * max_grandparent_overlap_factor;
}
uint64_t MutableCFOptions::ExpandedCompactionByteSizeLimit(int level) const {
  return MaxFileSizeForLevel(level) * expanded_compaction_factor;
}

}  // namespace rocksdb
