// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <deque>
#include <future>
#include <string>
#include <vector>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
// A class used to aggregate data per key/value. The plug-in function is
// implemented and registered using AddAggregator(). And then use it
// with merge operator created using CreateAggMergeOperator().
class Aggregator {
 public:
  virtual ~Aggregator() {}
  // The input list is in reverse insertion order, with values[0] to be
  // the one inserted last and values.back() to be the one inserted first.
  // The oldest one might be from Get().
  // Return whether aggregation succeeded. False for aggregation error.
  virtual bool Aggregate(const std::vector<Slice>& values,
                         std::string* result) const = 0;

  // True if a partial aggregation should be invoked. Some aggregators
  // might opt to skip partial aggregation if possible.
  virtual bool DoPartialAggregate() const { return true; }
};

// The function adds aggregation plugin by function name. It is used
// by all the aggregation operator created using CreateAggMergeOperator().
// It's currently not thread safe to run concurrently with the aggregation
// merge operator. It is recommended that all the aggregation function
// is added before calling CreateAggMergeOperator().
Status AddAggregator(const std::string& function_name,
                     std::unique_ptr<Aggregator>&& agg);

// Get the singleton instance of merge operator for aggregation.
// Always the same one is returned with a shared_ptr is hold as a
// static variable by the function.
// This is done so because options.merge_operator is shared_ptr.
// Users can push values to be updated with a merge operand encoded with
// registered function name and payload using EncodeAggFuncAndValue(),
// and the merge operator will invoke the aggregation function.
// An example:
//
//    // Assume class ExampleSumAggregator is implemented to do simple sum.
//    AddAggregator("sum", std::make_unique<ExampleSumAggregator>());
//    std::shared_ptr<MergeOperator> mp_guard = CreateAggMergeOperator();
//    options.merge_operator = mp_guard.get();
//    ...... // Creating DB
//    db->Put(WriteOptions(), "foo", "100");
//    db->Merge(WriteOptions(), "foo", EncodeAggFuncAndValue("sum", "200"));
//    db->Merge(WriteOptions(), "foo", EncodeAggFuncAndValue("sum", "300"));
//    std::string value;
//    Status s = db->Get(ReadOptions, "foo", &value);
//    assert(s.ok());
//    Slice func, aggregated_value;
//    assert(ExtractAggFuncAndValue(value, func, aggregated_value));
//    assert(func == "sum");
//    assert(aggregated_value == "600");
//
// If the aggregation function is not registered or there is an error
// returned by aggregation function, the result will be encoded with a fake
// aggregation function kErrorFuncName, with each merge operands to be encoded
// into a list that can be extracted using ExtractList();
//
// If users add a merge operand using a different aggregation function from
// the previous one, the previous ones will be ignored.
std::shared_ptr<MergeOperator> GetAggMergeOperator();

// Encode aggregation function and payload that can be consumed by aggregation
// merge operator.
std::string EncodeAggFuncAndValue(const Slice& function_name,
                                  const Slice& payload);
// Helper function to extract aggregation function name and payload.
// Return false if it fails to decode.
bool ExtractAggFuncAndValue(const Slice& op, Slice& func, Slice& value);

// Extract encoded list. This can be used to extract error merge operands when
// the returned function name is kErrorFuncName.
bool ExtractList(const Slice& encoded_list, std::vector<Slice>& decoded_list);

// Special error function name reserved for merging or aggregation error.
extern const std::string kErrorFuncName;

}  // namespace ROCKSDB_NAMESPACE
