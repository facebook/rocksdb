//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "table/block_based/parsed_full_filter_block.h"
#include "rocksdb/filter_policy.h"

namespace ROCKSDB_NAMESPACE {

ParsedFullFilterBlock::ParsedFullFilterBlock(const FilterPolicy* filter_policy,
                                             BlockContents&& contents,
                                             Statistics* statistics)
    : block_contents_(std::move(contents)),
      filter_bits_reader_(
          !block_contents_.data.empty()
              ? filter_policy->GetFilterBitsReader(block_contents_.data)
              : nullptr) {
  if (statistics) {
    uint64_t generation = statistics->GetGeneration();
    if (generation) {
      statistics_for_evict_ = statistics;
      statistics_generation_ = generation;
    }
  }
}

ParsedFullFilterBlock::~ParsedFullFilterBlock() {
  if (statistics_for_evict_ &&
      statistics_generation_ == statistics_for_evict_->GetGeneration()) {
    RecordTick(statistics_for_evict_, BLOCK_CACHE_FILTER_BYTES_EVICT,
               ApproximateMemoryUsage());
  }
}

}  // namespace ROCKSDB_NAMESPACE
