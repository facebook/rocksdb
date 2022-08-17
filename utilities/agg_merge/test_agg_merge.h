//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <algorithm>
#include <cstddef>
#include <memory>
#include <unordered_map>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/agg_merge.h"
#include "utilities/cassandra/cassandra_options.h"

namespace ROCKSDB_NAMESPACE {
class SumAggregator : public Aggregator {
 public:
  ~SumAggregator() override {}
  bool Aggregate(const std::vector<Slice>&, std::string& result) const override;
  bool DoPartialAggregate() const override { return true; }
};

class MultipleAggregator : public Aggregator {
 public:
  ~MultipleAggregator() override {}
  bool Aggregate(const std::vector<Slice>&, std::string& result) const override;
  bool DoPartialAggregate() const override { return true; }
};

class Last3Aggregator : public Aggregator {
 public:
  ~Last3Aggregator() override {}
  bool Aggregate(const std::vector<Slice>&, std::string& result) const override;
};

class EncodeHelper {
 public:
  static std::string EncodeFuncAndInt(const Slice& function_name,
                                      int64_t value);
  static std::string EncodeInt(int64_t value);
  static std::string EncodeList(const std::vector<Slice>& list);
  static std::string EncodeFuncAndList(const Slice& function_name,
                                       const std::vector<Slice>& list);
};
}  // namespace ROCKSDB_NAMESPACE
