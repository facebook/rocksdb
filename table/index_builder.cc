//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/index_builder.h"
#include <assert.h>
#include <inttypes.h>
#include <list>
#include <string>

#include "rocksdb/comparator.h"
#include "table/format.h"
#include "table/partitioned_filter_block.h"

// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace rocksdb {
// using namespace rocksdb;
// Create a index builder based on its type.
IndexBuilder* IndexBuilder::CreateIndexBuilder(
    BlockBasedTableOptions::IndexType index_type,
    const InternalKeyComparator* comparator,
    const SliceTransform* prefix_extractor, int index_block_restart_interval,
    uint64_t index_per_partition) {
  switch (index_type) {
    case BlockBasedTableOptions::kBinarySearch: {
      return new ShortenedIndexBuilder(comparator,
                                       index_block_restart_interval);
    }
    case BlockBasedTableOptions::kHashSearch: {
      return new HashIndexBuilder(comparator, prefix_extractor,
                                  index_block_restart_interval);
    }
    case BlockBasedTableOptions::kTwoLevelIndexSearch: {
      return new PartitionIndexBuilder(comparator, prefix_extractor,
                                       index_per_partition,
                                       index_block_restart_interval);
    }
    default: {
      assert(!"Do not recognize the index type ");
      return nullptr;
    }
  }
  // impossible.
  assert(false);
  return nullptr;
}
}  // namespace rocksdb
