// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef ROCKSDB_LITE

#include "utilities/ttl/db_ttl_impl.h"

#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
static std::unordered_map<std::string, OptionTypeInfo> ttl_merge_op_type_info =
    {{"user_operator",
      OptionTypeInfo::AsCustomSharedPtr<MergeOperator>(
          0, OptionVerificationType::kByName, OptionTypeFlags::kNone)}};

TtlMergeOperator::TtlMergeOperator(
    const std::shared_ptr<MergeOperator>& merge_op, SystemClock* clock)
    : user_merge_op_(merge_op), clock_(clock) {
  RegisterOptions("TtlMergeOptions", &user_merge_op_, &ttl_merge_op_type_info);
}

bool TtlMergeOperator::FullMergeV2(const MergeOperationInput& merge_in,
                                   MergeOperationOutput* merge_out) const {
  const uint32_t ts_len = DBWithTTLImpl::kTSLength;
  if (merge_in.existing_value && merge_in.existing_value->size() < ts_len) {
    ROCKS_LOG_ERROR(merge_in.logger,
                    "Error: Could not remove timestamp from existing value.");
    return false;
  }

  // Extract time-stamp from each operand to be passed to user_merge_op_
  std::vector<Slice> operands_without_ts;
  for (const auto& operand : merge_in.operand_list) {
    if (operand.size() < ts_len) {
      ROCKS_LOG_ERROR(merge_in.logger,
                      "Error: Could not remove timestamp from operand value.");
      return false;
    }
    operands_without_ts.push_back(operand);
    operands_without_ts.back().remove_suffix(ts_len);
  }

  // Apply the user merge operator (store result in *new_value)
  bool good = true;
  MergeOperationOutput user_merge_out(merge_out->new_value,
                                      merge_out->existing_operand);
  if (merge_in.existing_value) {
    Slice existing_value_without_ts(merge_in.existing_value->data(),
                                    merge_in.existing_value->size() - ts_len);
    good = user_merge_op_->FullMergeV2(
        MergeOperationInput(merge_in.key, &existing_value_without_ts,
                            operands_without_ts, merge_in.logger),
        &user_merge_out);
  } else {
    good = user_merge_op_->FullMergeV2(
        MergeOperationInput(merge_in.key, nullptr, operands_without_ts,
                            merge_in.logger),
        &user_merge_out);
  }

  // Return false if the user merge operator returned false
  if (!good) {
    return false;
  }

  if (merge_out->existing_operand.data()) {
    merge_out->new_value.assign(merge_out->existing_operand.data(),
                                merge_out->existing_operand.size());
    merge_out->existing_operand = Slice(nullptr, 0);
  }

  // Augment the *new_value with the ttl time-stamp
  int64_t curtime;
  if (!clock_->GetCurrentTime(&curtime).ok()) {
    ROCKS_LOG_ERROR(
        merge_in.logger,
        "Error: Could not get current time to be attached internally "
        "to the new value.");
    return false;
  } else {
    char ts_string[ts_len];
    EncodeFixed32(ts_string, (int32_t)curtime);
    merge_out->new_value.append(ts_string, ts_len);
    return true;
  }
}

bool TtlMergeOperator::PartialMergeMulti(const Slice& key,
                                         const std::deque<Slice>& operand_list,
                                         std::string* new_value,
                                         Logger* logger) const {
  const uint32_t ts_len = DBWithTTLImpl::kTSLength;
  std::deque<Slice> operands_without_ts;

  for (const auto& operand : operand_list) {
    if (operand.size() < ts_len) {
      ROCKS_LOG_ERROR(logger, "Error: Could not remove timestamp from value.");
      return false;
    }

    operands_without_ts.push_back(
        Slice(operand.data(), operand.size() - ts_len));
  }

  // Apply the user partial-merge operator (store result in *new_value)
  assert(new_value);
  if (!user_merge_op_->PartialMergeMulti(key, operands_without_ts, new_value,
                                         logger)) {
    return false;
  }

  // Augment the *new_value with the ttl time-stamp
  int64_t curtime;
  if (!clock_->GetCurrentTime(&curtime).ok()) {
    ROCKS_LOG_ERROR(
        logger,
        "Error: Could not get current time to be attached internally "
        "to the new value.");
    return false;
  } else {
    char ts_string[ts_len];
    EncodeFixed32(ts_string, (int32_t)curtime);
    new_value->append(ts_string, ts_len);
    return true;
  }
}

Status TtlMergeOperator::PrepareOptions(const ConfigOptions& config_options) {
  if (clock_ == nullptr) {
    clock_ = config_options.env->GetSystemClock().get();
  }
  return MergeOperator::PrepareOptions(config_options);
}

Status TtlMergeOperator::ValidateOptions(
    const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts) const {
  if (user_merge_op_ == nullptr) {
    return Status::InvalidArgument(
        "UserMergeOperator required by TtlMergeOperator");
  } else if (clock_ == nullptr) {
    return Status::InvalidArgument("SystemClock required by TtlMergeOperator");
  } else {
    return MergeOperator::ValidateOptions(db_opts, cf_opts);
  }
}

void DBWithTTLImpl::SanitizeOptions(int32_t ttl, ColumnFamilyOptions* options,
                                    SystemClock* clock) {
  if (options->compaction_filter) {
    options->compaction_filter =
        new TtlCompactionFilter(ttl, clock, options->compaction_filter);
  } else {
    options->compaction_filter_factory =
        std::shared_ptr<CompactionFilterFactory>(new TtlCompactionFilterFactory(
            ttl, clock, options->compaction_filter_factory));
  }

  if (options->merge_operator) {
    options->merge_operator.reset(
        new TtlMergeOperator(options->merge_operator, clock));
  }
}

static std::unordered_map<std::string, OptionTypeInfo> ttl_type_info = {
    {"ttl", {0, OptionType::kInt32T}},
};

static std::unordered_map<std::string, OptionTypeInfo> ttl_cff_type_info = {
    {"user_filter_factory",
     OptionTypeInfo::AsCustomSharedPtr<CompactionFilterFactory>(
         0, OptionVerificationType::kByNameAllowFromNull,
         OptionTypeFlags::kNone)}};
static std::unordered_map<std::string, OptionTypeInfo> user_cf_type_info = {
    {"user_filter",
     OptionTypeInfo::AsCustomRawPtr<const CompactionFilter>(
         0, OptionVerificationType::kByName, OptionTypeFlags::kAllowNull)}};

TtlCompactionFilter::TtlCompactionFilter(
    int32_t ttl, SystemClock* clock, const CompactionFilter* _user_comp_filter,
    std::unique_ptr<const CompactionFilter> _user_comp_filter_from_factory)
    : LayeredCompactionFilterBase(_user_comp_filter,
                                  std::move(_user_comp_filter_from_factory)),
      ttl_(ttl),
      clock_(clock) {
  RegisterOptions("TTL", &ttl_, &ttl_type_info);
  RegisterOptions("UserFilter", &user_comp_filter_, &user_cf_type_info);
}

bool TtlCompactionFilter::Filter(int level, const Slice& key,
                                 const Slice& old_val, std::string* new_val,
                                 bool* value_changed) const {
  if (DBWithTTLImpl::IsStale(old_val, ttl_, clock_)) {
    return true;
  }
  if (user_comp_filter() == nullptr) {
    return false;
  }
  assert(old_val.size() >= DBWithTTLImpl::kTSLength);
  Slice old_val_without_ts(old_val.data(),
                           old_val.size() - DBWithTTLImpl::kTSLength);
  if (user_comp_filter()->Filter(level, key, old_val_without_ts, new_val,
                                 value_changed)) {
    return true;
  }
  if (*value_changed) {
    new_val->append(old_val.data() + old_val.size() - DBWithTTLImpl::kTSLength,
                    DBWithTTLImpl::kTSLength);
  }
  return false;
}

Status TtlCompactionFilter::PrepareOptions(
    const ConfigOptions& config_options) {
  if (clock_ == nullptr) {
    clock_ = config_options.env->GetSystemClock().get();
  }
  return LayeredCompactionFilterBase::PrepareOptions(config_options);
}

Status TtlCompactionFilter::ValidateOptions(
    const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts) const {
  if (clock_ == nullptr) {
    return Status::InvalidArgument(
        "SystemClock required by TtlCompactionFilter");
  } else {
    return LayeredCompactionFilterBase::ValidateOptions(db_opts, cf_opts);
  }
}

TtlCompactionFilterFactory::TtlCompactionFilterFactory(
    int32_t ttl, SystemClock* clock,
    std::shared_ptr<CompactionFilterFactory> comp_filter_factory)
    : ttl_(ttl), clock_(clock), user_comp_filter_factory_(comp_filter_factory) {
  RegisterOptions("UserOptions", &user_comp_filter_factory_,
                  &ttl_cff_type_info);
  RegisterOptions("TTL", &ttl_, &ttl_type_info);
}

std::unique_ptr<CompactionFilter>
TtlCompactionFilterFactory::CreateCompactionFilter(
    const CompactionFilter::Context& context) {
  std::unique_ptr<const CompactionFilter> user_comp_filter_from_factory =
      nullptr;
  if (user_comp_filter_factory_) {
    user_comp_filter_from_factory =
        user_comp_filter_factory_->CreateCompactionFilter(context);
  }

  return std::unique_ptr<TtlCompactionFilter>(new TtlCompactionFilter(
      ttl_, clock_, nullptr, std::move(user_comp_filter_from_factory)));
}

Status TtlCompactionFilterFactory::PrepareOptions(
    const ConfigOptions& config_options) {
  if (clock_ == nullptr) {
    clock_ = config_options.env->GetSystemClock().get();
  }
  return CompactionFilterFactory::PrepareOptions(config_options);
}

Status TtlCompactionFilterFactory::ValidateOptions(
    const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts) const {
  if (clock_ == nullptr) {
    return Status::InvalidArgument(
        "SystemClock required by TtlCompactionFilterFactory");
  } else {
    return CompactionFilterFactory::ValidateOptions(db_opts, cf_opts);
  }
}

int RegisterTtlObjects(ObjectLibrary& library, const std::string& /*arg*/) {
  library.AddFactory<MergeOperator>(
      TtlMergeOperator::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /* errmsg */) {
        guard->reset(new TtlMergeOperator(nullptr, nullptr));
        return guard->get();
      });
  library.AddFactory<CompactionFilterFactory>(
      TtlCompactionFilterFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<CompactionFilterFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new TtlCompactionFilterFactory(0, nullptr, nullptr));
        return guard->get();
      });
  library.AddFactory<CompactionFilter>(
      TtlCompactionFilter::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<CompactionFilter>* /*guard*/,
         std::string* /* errmsg */) {
        return new TtlCompactionFilter(0, nullptr, nullptr);
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}
// Open the db inside DBWithTTLImpl because options needs pointer to its ttl
DBWithTTLImpl::DBWithTTLImpl(DB* db) : DBWithTTL(db), closed_(false) {}

DBWithTTLImpl::~DBWithTTLImpl() {
  if (!closed_) {
    Close().PermitUncheckedError();
  }
}

Status DBWithTTLImpl::Close() {
  Status ret = Status::OK();
  if (!closed_) {
    Options default_options = GetOptions();
    // Need to stop background compaction before getting rid of the filter
    CancelAllBackgroundWork(db_, /* wait = */ true);
    ret = db_->Close();
    delete default_options.compaction_filter;
    closed_ = true;
  }
  return ret;
}

void DBWithTTLImpl::RegisterTtlClasses() {
  static std::once_flag once;
  std::call_once(once, [&]() {
    ObjectRegistry::Default()->AddLibrary("TTL", RegisterTtlObjects, "");
  });
}

Status DBWithTTL::Open(const Options& options, const std::string& dbname,
                       DBWithTTL** dbptr, int32_t ttl, bool read_only) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DBWithTTL::Open(db_options, dbname, column_families, &handles,
                             dbptr, {ttl}, read_only);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }
  return s;
}

Status DBWithTTL::Open(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DBWithTTL** dbptr,
    const std::vector<int32_t>& ttls, bool read_only) {
  DBWithTTLImpl::RegisterTtlClasses();
  if (ttls.size() != column_families.size()) {
    return Status::InvalidArgument(
        "ttls size has to be the same as number of column families");
  }

  SystemClock* clock = (db_options.env == nullptr)
                           ? SystemClock::Default().get()
                           : db_options.env->GetSystemClock().get();

  std::vector<ColumnFamilyDescriptor> column_families_sanitized =
      column_families;
  for (size_t i = 0; i < column_families_sanitized.size(); ++i) {
    DBWithTTLImpl::SanitizeOptions(
        ttls[i], &column_families_sanitized[i].options, clock);
  }
  DB* db;

  Status st;
  if (read_only) {
    st = DB::OpenForReadOnly(db_options, dbname, column_families_sanitized,
                             handles, &db);
  } else {
    st = DB::Open(db_options, dbname, column_families_sanitized, handles, &db);
  }
  if (st.ok()) {
    *dbptr = new DBWithTTLImpl(db);
  } else {
    *dbptr = nullptr;
  }
  return st;
}

Status DBWithTTLImpl::CreateColumnFamilyWithTtl(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    ColumnFamilyHandle** handle, int ttl) {
  RegisterTtlClasses();
  ColumnFamilyOptions sanitized_options = options;
  DBWithTTLImpl::SanitizeOptions(ttl, &sanitized_options,
                                 GetEnv()->GetSystemClock().get());

  return DBWithTTL::CreateColumnFamily(sanitized_options, column_family_name,
                                       handle);
}

Status DBWithTTLImpl::CreateColumnFamily(const ColumnFamilyOptions& options,
                                         const std::string& column_family_name,
                                         ColumnFamilyHandle** handle) {
  return CreateColumnFamilyWithTtl(options, column_family_name, handle, 0);
}

// Appends the current timestamp to the string.
// Returns false if could not get the current_time, true if append succeeds
Status DBWithTTLImpl::AppendTS(const Slice& val, std::string* val_with_ts,
                               SystemClock* clock) {
  val_with_ts->reserve(kTSLength + val.size());
  char ts_string[kTSLength];
  int64_t curtime;
  Status st = clock->GetCurrentTime(&curtime);
  if (!st.ok()) {
    return st;
  }
  EncodeFixed32(ts_string, (int32_t)curtime);
  val_with_ts->append(val.data(), val.size());
  val_with_ts->append(ts_string, kTSLength);
  return st;
}

// Returns corruption if the length of the string is lesser than timestamp, or
// timestamp refers to a time lesser than ttl-feature release time
Status DBWithTTLImpl::SanityCheckTimestamp(const Slice& str) {
  if (str.size() < kTSLength) {
    return Status::Corruption("Error: value's length less than timestamp's\n");
  }
  // Checks that TS is not lesser than kMinTimestamp
  // Gaurds against corruption & normal database opened incorrectly in ttl mode
  int32_t timestamp_value = DecodeFixed32(str.data() + str.size() - kTSLength);
  if (timestamp_value < kMinTimestamp) {
    return Status::Corruption("Error: Timestamp < ttl feature release time!\n");
  }
  return Status::OK();
}

// Checks if the string is stale or not according to TTl provided
bool DBWithTTLImpl::IsStale(const Slice& value, int32_t ttl,
                            SystemClock* clock) {
  if (ttl <= 0) {  // Data is fresh if TTL is non-positive
    return false;
  }
  int64_t curtime;
  if (!clock->GetCurrentTime(&curtime).ok()) {
    return false;  // Treat the data as fresh if could not get current time
  }
  int32_t timestamp_value =
      DecodeFixed32(value.data() + value.size() - kTSLength);
  return (timestamp_value + ttl) < curtime;
}

// Strips the TS from the end of the slice
Status DBWithTTLImpl::StripTS(PinnableSlice* pinnable_val) {
  if (pinnable_val->size() < kTSLength) {
    return Status::Corruption("Bad timestamp in key-value");
  }
  // Erasing characters which hold the TS
  pinnable_val->remove_suffix(kTSLength);
  return Status::OK();
}

// Strips the TS from the end of the string
Status DBWithTTLImpl::StripTS(std::string* str) {
  if (str->length() < kTSLength) {
    return Status::Corruption("Bad timestamp in key-value");
  }
  // Erasing characters which hold the TS
  str->erase(str->length() - kTSLength, kTSLength);
  return Status::OK();
}

Status DBWithTTLImpl::Put(const WriteOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          const Slice& val) {
  WriteBatch batch;
  Status st = batch.Put(column_family, key, val);
  if (st.ok()) {
    st = Write(options, &batch);
  }
  return st;
}

Status DBWithTTLImpl::Get(const ReadOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          PinnableSlice* value) {
  Status st = db_->Get(options, column_family, key, value);
  if (!st.ok()) {
    return st;
  }
  st = SanityCheckTimestamp(*value);
  if (!st.ok()) {
    return st;
  }
  return StripTS(value);
}

std::vector<Status> DBWithTTLImpl::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  auto statuses = db_->MultiGet(options, column_family, keys, values);
  for (size_t i = 0; i < keys.size(); ++i) {
    if (!statuses[i].ok()) {
      continue;
    }
    statuses[i] = SanityCheckTimestamp((*values)[i]);
    if (!statuses[i].ok()) {
      continue;
    }
    statuses[i] = StripTS(&(*values)[i]);
  }
  return statuses;
}

bool DBWithTTLImpl::KeyMayExist(const ReadOptions& options,
                                ColumnFamilyHandle* column_family,
                                const Slice& key, std::string* value,
                                bool* value_found) {
  bool ret = db_->KeyMayExist(options, column_family, key, value, value_found);
  if (ret && value != nullptr && value_found != nullptr && *value_found) {
    if (!SanityCheckTimestamp(*value).ok() || !StripTS(value).ok()) {
      return false;
    }
  }
  return ret;
}

Status DBWithTTLImpl::Merge(const WriteOptions& options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            const Slice& value) {
  WriteBatch batch;
  Status st = batch.Merge(column_family, key, value);
  if (st.ok()) {
    st = Write(options, &batch);
  }
  return st;
}

Status DBWithTTLImpl::Write(const WriteOptions& opts, WriteBatch* updates) {
  class Handler : public WriteBatch::Handler {
   public:
    explicit Handler(SystemClock* clock) : clock_(clock) {}
    WriteBatch updates_ttl;
    Status PutCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override {
      std::string value_with_ts;
      Status st = AppendTS(value, &value_with_ts, clock_);
      if (!st.ok()) {
        return st;
      }
      return WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                     value_with_ts);
    }
    Status MergeCF(uint32_t column_family_id, const Slice& key,
                   const Slice& value) override {
      std::string value_with_ts;
      Status st = AppendTS(value, &value_with_ts, clock_);
      if (!st.ok()) {
        return st;
      }
      return WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                       value_with_ts);
    }
    Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
      return WriteBatchInternal::Delete(&updates_ttl, column_family_id, key);
    }
    Status DeleteRangeCF(uint32_t column_family_id, const Slice& begin_key,
                         const Slice& end_key) override {
      return WriteBatchInternal::DeleteRange(&updates_ttl, column_family_id,
                                             begin_key, end_key);
    }
    void LogData(const Slice& blob) override { updates_ttl.PutLogData(blob); }

   private:
    SystemClock* clock_;
  };
  Handler handler(GetEnv()->GetSystemClock().get());
  Status st = updates->Iterate(&handler);
  if (!st.ok()) {
    return st;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Iterator* DBWithTTLImpl::NewIterator(const ReadOptions& opts,
                                     ColumnFamilyHandle* column_family) {
  return new TtlIterator(db_->NewIterator(opts, column_family));
}

void DBWithTTLImpl::SetTtl(ColumnFamilyHandle *h, int32_t ttl) {
  std::shared_ptr<TtlCompactionFilterFactory> filter;
  Options opts;
  opts = GetOptions(h);
  filter = std::static_pointer_cast<TtlCompactionFilterFactory>(
                                       opts.compaction_filter_factory);
  if (!filter)
    return;
  filter->SetTtl(ttl);
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
