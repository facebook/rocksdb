//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/periodic_compaction_policy.h"

#include <mutex>

#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE {
namespace {
const std::unordered_map<std::string, OptionTypeInfo>&
FixedLookaheadPeriodicCompactionPolicyTypeInfo() {
  static const std::unordered_map<std::string, OptionTypeInfo> type_info = {
      {"lookahead_seconds",
       {0, OptionType::kUInt64T, OptionVerificationType::kNormal,
        OptionTypeFlags::kNone}},
  };
  return type_info;
}
}  // namespace

FixedLookaheadPeriodicCompactionPolicy::FixedLookaheadPeriodicCompactionPolicy(
    uint64_t lookahead_seconds)
    : lookahead_seconds_(lookahead_seconds) {
  RegisterOptions("LookaheadSeconds", &lookahead_seconds_,
                  &FixedLookaheadPeriodicCompactionPolicyTypeInfo());
}

PeriodicCompactionPolicy::Decision
FixedLookaheadPeriodicCompactionPolicy::GetDecision(
    const Context& /* context */) const {
  Decision decision;
  decision.deadline_lookahead_seconds = lookahead_seconds_;
  return decision;
}

namespace {
static int RegisterPeriodicCompactionPolicies(ObjectLibrary& library,
                                              const std::string& /*arg*/) {
  library.AddFactory<PeriodicCompactionPolicy>(
      FixedLookaheadPeriodicCompactionPolicy::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<PeriodicCompactionPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(new FixedLookaheadPeriodicCompactionPolicy(0));
        return guard->get();
      });
  return 1;
}
}  // namespace

Status PeriodicCompactionPolicy::CreateFromString(
    const ConfigOptions& options, const std::string& value,
    std::shared_ptr<PeriodicCompactionPolicy>* result) {
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterPeriodicCompactionPolicies(*(ObjectLibrary::Default().get()), "");
  });
  return LoadSharedObject<PeriodicCompactionPolicy>(options, value, result);
}

}  // namespace ROCKSDB_NAMESPACE
