//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "table/block_based/parsed_full_filter_block.h"

#include "table/block_based/filter_policy_internal.h"

namespace ROCKSDB_NAMESPACE {

ParsedFullFilterBlock::ParsedFullFilterBlock(const FilterPolicy* filter_policy,
                                             BlockContents&& contents)
    : block_contents_(std::move(contents)),
      filter_bits_reader_(
          !block_contents_.data.empty()
              ? filter_policy->GetFilterBitsReader(block_contents_.data)
              : nullptr) {}

ParsedFullFilterBlock::~ParsedFullFilterBlock() = default;

}  // namespace ROCKSDB_NAMESPACE
