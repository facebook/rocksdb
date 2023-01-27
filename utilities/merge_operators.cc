// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "utilities/merge_operators.h"

#include <memory>

#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "utilities/merge_operators/bytesxor.h"
#include "utilities/merge_operators/sortlist.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

namespace ROCKSDB_NAMESPACE {
static bool LoadMergeOperator(const std::string& id,
                              std::shared_ptr<MergeOperator>* result) {
  bool success = true;
  // TODO: Hook the "name" up to the actual Name() of the MergeOperators?
  // Requires these classes be moved into a header file...
  if (id == "put" || id == "PutOperator") {
    *result = MergeOperators::CreatePutOperator();
  } else if (id == "put_v1") {
    *result = MergeOperators::CreateDeprecatedPutOperator();
  } else if (id == "uint64add" || id == "UInt64AddOperator") {
    *result = MergeOperators::CreateUInt64AddOperator();
  } else if (id == "max" || id == "MaxOperator") {
    *result = MergeOperators::CreateMaxOperator();
  } else {
    success = false;
  }
  return success;
}

static int RegisterBuiltinMergeOperators(ObjectLibrary& library,
                                         const std::string& /*arg*/) {
  size_t num_types;
  library.AddFactory<MergeOperator>(
      ObjectLibrary::PatternEntry(StringAppendOperator::kClassName())
          .AnotherName(StringAppendOperator::kNickName()),
      [](const std::string& /*uri*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new StringAppendOperator(","));
        return guard->get();
      });
  library.AddFactory<MergeOperator>(
      ObjectLibrary::PatternEntry(StringAppendTESTOperator::kClassName())
          .AnotherName(StringAppendTESTOperator::kNickName()),
      [](const std::string& /*uri*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new StringAppendTESTOperator(","));
        return guard->get();
      });
  library.AddFactory<MergeOperator>(
      ObjectLibrary::PatternEntry(SortList::kClassName())
          .AnotherName(SortList::kNickName()),
      [](const std::string& /*uri*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new SortList());
        return guard->get();
      });
  library.AddFactory<MergeOperator>(
      ObjectLibrary::PatternEntry(BytesXOROperator::kClassName())
          .AnotherName(BytesXOROperator::kNickName()),
      [](const std::string& /*uri*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new BytesXOROperator());
        return guard->get();
      });

  return static_cast<int>(library.GetFactoryCount(&num_types));
}

Status MergeOperator::CreateFromString(const ConfigOptions& config_options,
                                       const std::string& value,
                                       std::shared_ptr<MergeOperator>* result) {
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterBuiltinMergeOperators(*(ObjectLibrary::Default().get()), "");
  });
  return LoadSharedObject<MergeOperator>(config_options, value,
                                         LoadMergeOperator, result);
}

std::shared_ptr<MergeOperator> MergeOperators::CreateFromStringId(
    const std::string& id) {
  std::shared_ptr<MergeOperator> result;
  Status s = MergeOperator::CreateFromString(ConfigOptions(), id, &result);
  if (s.ok()) {
    return result;
  } else {
    // Empty or unknown, just return nullptr
    return nullptr;
  }
}

}  // namespace ROCKSDB_NAMESPACE
