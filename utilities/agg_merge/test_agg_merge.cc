//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "test_agg_merge.h"

#include <assert.h>

#include <deque>
#include <vector>

#include "util/coding.h"
#include "utilities/agg_merge/agg_merge.h"

namespace ROCKSDB_NAMESPACE {

std::string EncodeHelper::EncodeFuncAndInt(const Slice& function_name,
                                           int64_t value) {
  std::string encoded_value;
  PutVarsignedint64(&encoded_value, value);
  return EncodeAggFuncAndValue(function_name, encoded_value);
}

std::string EncodeHelper::EncodeInt(int64_t value) {
  std::string encoded_value;
  PutVarsignedint64(&encoded_value, value);
  return encoded_value;
}

std::string EncodeHelper::EncodeFuncAndList(const Slice& function_name,
                                            const std::vector<Slice>& list) {
  return EncodeAggFuncAndValue(function_name, EncodeList(list));
}

std::string EncodeHelper::EncodeList(const std::vector<Slice>& list) {
  std::string result;
  for (const Slice& entity : list) {
    PutLengthPrefixedSlice(&result, entity);
  }
  return result;
}

bool SumAggregator::Aggregate(const std::vector<Slice>& item_list,
                              std::string* result) const {
  int64_t sum = 0;
  for (const Slice& item : item_list) {
    int64_t ivalue;
    Slice v = item;
    if (!GetVarsignedint64(&v, &ivalue) || !v.empty()) {
      return false;
    }
    sum += ivalue;
  }
  *result = EncodeHelper::EncodeInt(sum);
  return true;
}

bool Last3Aggregator::Aggregate(const std::vector<Slice>& item_list,
                                std::string* result) const {
  std::vector<Slice> last3;
  last3.reserve(3);
  for (Slice item : item_list) {
    Slice entity;
    bool ret;
    while ((ret = GetLengthPrefixedSlice(&item, &entity)) == true) {
      last3.push_back(entity);
      if (last3.size() >= 3) {
        break;
      }
    }
    if (last3.size() >= 3) {
      break;
    }
    if (!ret) {
      continue;
    }
  }
  *result = EncodeHelper::EncodeList(last3);
  return true;
}
}  // namespace ROCKSDB_NAMESPACE
