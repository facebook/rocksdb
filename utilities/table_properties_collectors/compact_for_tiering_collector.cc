//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/table_properties_collectors/compact_for_tiering_collector.h"

#include <sstream>

#include "db/seqno_to_time_mapping.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
const std::string
    CompactForTieringCollector::kNumEligibleLastLevelEntriesPropertyName =
        "rocksdb.eligible.last.level.entries";

CompactForTieringCollector::CompactForTieringCollector(
    int level_at_creation, int num_levels,
    SequenceNumber last_level_inclusive_max_seqno_threshold,
    size_t compaction_trigger, bool enabled)
    : level_at_creation_(level_at_creation),
      num_levels_(num_levels),
      last_level_inclusive_max_seqno_threshold_(
          last_level_inclusive_max_seqno_threshold),
      compaction_trigger_(compaction_trigger),
      enabled_(enabled) {}

Status CompactForTieringCollector::AddUserKey(const Slice& /*key*/,
                                              const Slice& value,
                                              EntryType type,
                                              SequenceNumber seq,
                                              uint64_t /*file_size*/) {
  if (!enabled_ || level_at_creation_ == num_levels_ - 1 ||
      last_level_inclusive_max_seqno_threshold_ == kMaxSequenceNumber) {
    return Status::OK();
  }
  SequenceNumber seq_for_check = seq;
  if (type == kEntryTimedPut) {
    seq_for_check = ParsePackedValueForSeqno(value);
  }
  if (seq_for_check < last_level_inclusive_max_seqno_threshold_) {
    last_level_eligible_entries_counter_++;
  }
  return Status::OK();
}

Status CompactForTieringCollector::Finish(UserCollectedProperties* properties) {
  assert(!finish_called_);
  if (compaction_trigger_ != 0 &&
      last_level_eligible_entries_counter_ >= compaction_trigger_) {
    need_compaction_ = true;
  }
  finish_called_ = true;
  if (last_level_eligible_entries_counter_ > 0) {
    *properties = UserCollectedProperties{
        {kNumEligibleLastLevelEntriesPropertyName,
         std::to_string(last_level_eligible_entries_counter_)},
    };
  }
  return Status::OK();
}

UserCollectedProperties CompactForTieringCollector::GetReadableProperties()
    const {
  return UserCollectedProperties{
      {kNumEligibleLastLevelEntriesPropertyName,
       std::to_string(last_level_eligible_entries_counter_)},
  };
}

bool CompactForTieringCollector::NeedCompact() const {
  return need_compaction_;
}

void CompactForTieringCollector::Reset() {
  last_level_eligible_entries_counter_ = 0;
  finish_called_ = false;
  need_compaction_ = false;
}

TablePropertiesCollector*
CompactForTieringCollectorFactory::CreateTablePropertiesCollector(
    TablePropertiesCollectorFactory::Context context) {
  uint64_t compaction_trigger = GetCompactionTrigger(context.column_family_id);
  return new CompactForTieringCollector(
      context.level_at_creation, context.num_levels,
      context.last_level_inclusive_max_seqno_threshold, compaction_trigger,
      enabled_);
}

static std::unordered_map<std::string, OptionTypeInfo>
    on_compact_for_tiering_type_info = {
        {"compaction_triggers",
         {0, OptionType::kUnknown, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareNever | OptionTypeFlags::kMutable,
          [](const ConfigOptions&, const std::string&, const std::string& value,
             void* addr) {
            auto* factory =
                static_cast<CompactForTieringCollectorFactory*>(addr);
            factory->SetCompactionTriggers(ParseUint32MapToSizeT(value));
            return Status::OK();
          },
          [](const ConfigOptions&, const std::string&, const void* addr,
             std::string* value) {
            const auto* factory =
                static_cast<const CompactForTieringCollectorFactory*>(addr);
            SerializeUint32MapToSizeT(
                const_cast<CompactForTieringCollectorFactory*>(factory)
                    ->GetCompactionTriggers(),
                value);
            return Status::OK();
          },
          nullptr}},
        {"enabled",
         {0, OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareNever | OptionTypeFlags::kMutable,
          [](const ConfigOptions&, const std::string&, const std::string& value,
             void* addr) {
            auto* factory =
                static_cast<CompactForTieringCollectorFactory*>(addr);
            factory->SetEnabled(ParseBoolean("", value));
            return Status::OK();
          },
          [](const ConfigOptions&, const std::string&, const void* addr,
             std::string* value) {
            const auto* factory =
                static_cast<const CompactForTieringCollectorFactory*>(addr);
            *value = std::to_string(
                const_cast<CompactForTieringCollectorFactory*>(factory)
                    ->GetEnabled());
            return Status::OK();
          },
          nullptr}},

};

CompactForTieringCollectorFactory::CompactForTieringCollectorFactory(
    const std::map<uint32_t, size_t>& compaction_triggers, bool enabled)
    : compaction_triggers_(compaction_triggers), enabled_(enabled) {
  RegisterOptions("", this, &on_compact_for_tiering_type_info);
}

std::string CompactForTieringCollectorFactory::ToString() const {
  std::ostringstream cfg;
  cfg << Name() << ", compaction triggers: {";
  bool first_entry = true;
  for (const auto& [cf_id, compaction_trigger] : compaction_triggers_) {
    if (first_entry) {
      first_entry = false;
    } else {
      cfg << ", ";
    }
    cfg << "(column family id: " << cf_id
        << ", compaction trigger: " << compaction_trigger << ")";
  }
  cfg << "}, enabled: " << (enabled_ ? "true" : "false") << std::endl;
  return cfg.str();
}

std::shared_ptr<CompactForTieringCollectorFactory>
NewCompactForTieringCollectorFactory(
    const std::map<uint32_t, size_t>& compaction_triggers, bool enabled) {
  return std::make_shared<CompactForTieringCollectorFactory>(
      compaction_triggers, enabled);
  return std::shared_ptr<CompactForTieringCollectorFactory>(
      new CompactForTieringCollectorFactory(compaction_triggers, enabled));
}

}  // namespace ROCKSDB_NAMESPACE
