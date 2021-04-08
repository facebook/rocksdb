//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

class FilterBitsReader;
class FilterPolicy;
class Statistics;

// The sharable/cachable part of the full filter.
class ParsedFullFilterBlock {
 public:
  ParsedFullFilterBlock(const FilterPolicy* filter_policy,
                        BlockContents&& contents,
                        const std::shared_ptr<Statistics>& statistics);
  ~ParsedFullFilterBlock();

  FilterBitsReader* filter_bits_reader() const {
    return filter_bits_reader_.get();
  }

  size_t ApproximateMemoryUsage() const;

  bool own_bytes() const { return block_contents_.own_bytes(); }

 private:
  BlockContents block_contents_;
  std::unique_ptr<FilterBitsReader> filter_bits_reader_;
  // For filter eviction statistics, optional
  std::shared_ptr<Statistics> statistics_;
};

}  // namespace ROCKSDB_NAMESPACE
