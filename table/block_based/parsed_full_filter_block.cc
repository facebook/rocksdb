//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "table/block_based/parsed_full_filter_block.h"
#include "rocksdb/filter_policy.h"

namespace ROCKSDB_NAMESPACE {

ParsedFullFilterBlock::ParsedFullFilterBlock(
    const FilterPolicy* filter_policy, BlockContents&& contents,
    const std::shared_ptr<Statistics>& statistics)
    : block_contents_(std::move(contents)),
      filter_bits_reader_(
          !block_contents_.data.empty()
              ? filter_policy->GetFilterBitsReader(block_contents_.data)
              : nullptr),
      statistics_(statistics) {}

ParsedFullFilterBlock::~ParsedFullFilterBlock() {
  RecordTick(statistics_.get(), BLOCK_CACHE_FILTER_BYTES_EVICT,
             ApproximateMemoryUsage());
}

size_t ParsedFullFilterBlock::ApproximateMemoryUsage() const {
  size_t usage =
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      malloc_usable_size((void*)this);
#else
      sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  if (filter_bits_reader_) {
    usage += filter_bits_reader_->ApproximateMemoryUsage();
  }
  usage += block_contents_.ApproximateMemoryUsage();
  return usage;
}

}  // namespace ROCKSDB_NAMESPACE
