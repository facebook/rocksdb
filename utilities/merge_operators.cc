// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "utilities/merge_operators.h"

#include <memory>

#include "options/customizable_helper.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {
static bool CreateMergeOperator(const std::string& id,
                                std::shared_ptr<MergeOperator>* op) {
  bool success = true;
  if (id == "put" || id == "PutOperator") {
    *op = MergeOperators::CreatePutOperator();
  } else if (id == "put_v1") {
    *op = MergeOperators::CreateDeprecatedPutOperator();
  } else if (id == "uint64add" || id == "UInt64AddOperator") {
    *op = MergeOperators::CreateUInt64AddOperator();
  } else if (id == "stringappend" || id == "StringAppendOperator") {
    *op = MergeOperators::CreateStringAppendOperator();
  } else if (id == "stringappendtest" || id == "StringAppendTESTOperator") {
    *op = MergeOperators::CreateStringAppendTESTOperator();
  } else if (id == "max" || id == "MaxOperator") {
    *op = MergeOperators::CreateMaxOperator();
  } else if (id == "bytesxor" || id == "BytesXOR") {
    *op = MergeOperators::CreateBytesXOROperator();
  } else if (id == "sortlist" || id == "MergeSortOperator") {
    *op = MergeOperators::CreateSortOperator();
  } else {
    success = false;
  }
  return success;
}

std::shared_ptr<MergeOperator> MergeOperators::CreateFromStringId(
    const std::string& id) {
  std::shared_ptr<MergeOperator> op;
  ConfigOptions config_options;
  Status s = MergeOperator::CreateFromString(config_options, id, &op);
  if (s.ok()) {
    return op;
  } else {
    return nullptr;
  }
}

Status MergeOperator::CreateFromString(const ConfigOptions& config_options,
                                       const std::string& value,
                                       std::shared_ptr<MergeOperator>* result) {
  Status status = LoadSharedObject<MergeOperator>(config_options, value,
                                                  CreateMergeOperator, result);
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
