//  Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include "table/block_based/block_based_filter_block.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/block_based_table_reader.h"

namespace rocksdb {
namespace mock {

class MockBlockBasedTable : public BlockBasedTable {
 public:
  explicit MockBlockBasedTable(Rep* rep)
      : BlockBasedTable(rep, nullptr /* block_cache_tracer */) {}
};

class MockBlockBasedTableTester {
 public:
  Options options_;
  ImmutableCFOptions ioptions_;
  EnvOptions env_options_;
  BlockBasedTableOptions table_options_;
  InternalKeyComparator icomp_;
  std::unique_ptr<BlockBasedTable> table_;

  MockBlockBasedTableTester(const FilterPolicy *filter_policy)
      : ioptions_(options_),
        env_options_(options_),
        icomp_(options_.comparator) {
    table_options_.filter_policy.reset(filter_policy);

    constexpr bool skip_filters = false;
    constexpr int level = 0;
    constexpr bool immortal_table = false;
    table_.reset(new MockBlockBasedTable(
        new BlockBasedTable::Rep(ioptions_, env_options_, table_options_,
                                 icomp_, skip_filters, level, immortal_table)));
  }
};

}  // namespace mock
}  // namespace rocksdb
