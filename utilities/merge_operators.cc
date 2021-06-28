// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "utilities/merge_operators.h"

#include <memory>

#include "options/customizable_helper.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/object_registry.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/bytesxor.h"
#include "utilities/merge_operators/sortlist.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

namespace ROCKSDB_NAMESPACE {
std::shared_ptr<MergeOperator> MergeOperators::CreateFromStringId(
    const std::string& id) {
  if (id == "put" || id == "PutOperator") {
    return CreatePutOperator();
  } else if (id == "put_v1") {
    return CreateDeprecatedPutOperator();
  } else if (id == "uint64add" || id == "UInt64AddOperator") {
    return CreateUInt64AddOperator();
  } else if (id == "stringappend" || id == StringAppendOperator::kClassName()) {
    return CreateStringAppendOperator();
  } else if (id == "stringappendtest" ||
             id == StringAppendTESTOperator::kClassName()) {
    return CreateStringAppendTESTOperator();
  } else if (id == "max" || id == "MaxOperator") {
    return CreateMaxOperator();
  } else if (id == "bytesxor" || id == BytesXOROperator::kClassName()) {
    return CreateBytesXOROperator();
  } else if (id == "sortlist" || id == SortList::kClassName()) {
    return CreateSortOperator();
  } else {
    // Empty or unknown, just return nullptr
    return nullptr;
  }
}

#ifndef ROCKSDB_LITE
static int RegisterBuiltinMergeOperators(ObjectLibrary& library,
                                         const std::string& /*arg*/) {
  library.Register<MergeOperator>(
      StringAppendOperator::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new StringAppendOperator(","));
        return guard->get();
      });
  library.Register<MergeOperator>(
      StringAppendTESTOperator::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new StringAppendTESTOperator(","));
        return guard->get();
      });
  return 2;
}
#endif  // ROCKSDB_LITE

Status MergeOperator::CreateFromString(const ConfigOptions& config_options,
                                       const std::string& value,
                                       std::shared_ptr<MergeOperator>* result) {
#ifndef ROCKSDB_LITE
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterBuiltinMergeOperators(*(ObjectLibrary::Default().get()), "");
  });
#endif  // ROCKSDB_LITE
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  std::shared_ptr<MergeOperator> merge_op;
  Status status =
      ConfigurableHelper::GetOptionsMap(value, result->get(), &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  }
  if (opt_map.empty()) {
    merge_op = MergeOperators::CreateFromStringId(id);
    if (merge_op) {
      *result = merge_op;
      return Status::OK();
    }
  }
  if (value.empty()) {
    // No Id and no options.  Clear the object
    result->reset();
    return Status::OK();
  } else if (id.empty()) {  // We have no Id but have options.  Not good
    return Status::NotSupported("Cannot reset object ", id);
  } else {
    std::string curr_opts;
#ifndef ROCKSDB_LITE
    if (*result != nullptr && (*result)->GetId() == id) {
      // Try to get the existing options, ignoring any errors
      ConfigOptions embedded = config_options;
      embedded.delimiter = ";";
      (*result)->GetOptionString(embedded, &curr_opts).PermitUncheckedError();
    }
    status = config_options.registry->NewSharedObject(id, &merge_op);
#else
    status = Status::NotSupported("Cannot load object in LITE mode ", id);
#endif
    if (!status.ok()) {
      if (config_options.ignore_unsupported_options &&
          status.IsNotSupported()) {
        result->reset();
        return Status::OK();
      }
    } else if (!curr_opts.empty() || !opt_map.empty()) {
      status = ConfigurableHelper::ConfigureNewObject(
          config_options, merge_op.get(), id, curr_opts, opt_map);
    }
    if (status.ok()) {
      *result = merge_op;
    }
    return status;
  }
}
}  // namespace ROCKSDB_NAMESPACE
