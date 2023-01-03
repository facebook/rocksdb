//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_job.h"
#include "db/compaction/compaction_state.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/utilities/options_type.h"

#ifndef ROCKSDB_LITE
namespace ROCKSDB_NAMESPACE {
class SubcompactionState;

CompactionServiceJobStatus
CompactionJob::ProcessKeyValueCompactionWithCompactionService(
    SubcompactionState* sub_compact) {
  assert(sub_compact);
  assert(sub_compact->compaction);
  assert(db_options_.compaction_service);

  const Compaction* compaction = sub_compact->compaction;
  CompactionServiceInput compaction_input;
  compaction_input.output_level = compaction->output_level();
  compaction_input.db_id = db_id_;

  const std::vector<CompactionInputFiles>& inputs =
      *(compact_->compaction->inputs());
  for (const auto& files_per_level : inputs) {
    for (const auto& file : files_per_level.files) {
      compaction_input.input_files.emplace_back(
          MakeTableFileName(file->fd.GetNumber()));
    }
  }
  compaction_input.column_family.name =
      compaction->column_family_data()->GetName();
  compaction_input.column_family.options =
      compaction->column_family_data()->GetLatestCFOptions();
  compaction_input.db_options =
      BuildDBOptions(db_options_, mutable_db_options_copy_);
  compaction_input.snapshots = existing_snapshots_;
  compaction_input.has_begin = sub_compact->start.has_value();
  compaction_input.begin =
      compaction_input.has_begin ? sub_compact->start->ToString() : "";
  compaction_input.has_end = sub_compact->end.has_value();
  compaction_input.end =
      compaction_input.has_end ? sub_compact->end->ToString() : "";

  std::string compaction_input_binary;
  Status s = compaction_input.Write(&compaction_input_binary);
  if (!s.ok()) {
    sub_compact->status = s;
    return CompactionServiceJobStatus::kFailure;
  }

  std::ostringstream input_files_oss;
  bool is_first_one = true;
  for (const auto& file : compaction_input.input_files) {
    input_files_oss << (is_first_one ? "" : ", ") << file;
    is_first_one = false;
  }

  ROCKS_LOG_INFO(
      db_options_.info_log,
      "[%s] [JOB %d] Starting remote compaction (output level: %d): %s",
      compaction_input.column_family.name.c_str(), job_id_,
      compaction_input.output_level, input_files_oss.str().c_str());
  CompactionServiceJobInfo info(dbname_, db_id_, db_session_id_,
                                GetCompactionId(sub_compact), thread_pri_);
  CompactionServiceJobStatus compaction_status =
      db_options_.compaction_service->StartV2(info, compaction_input_binary);
  switch (compaction_status) {
    case CompactionServiceJobStatus::kSuccess:
      break;
    case CompactionServiceJobStatus::kFailure:
      sub_compact->status = Status::Incomplete(
          "CompactionService failed to start compaction job.");
      ROCKS_LOG_WARN(db_options_.info_log,
                     "[%s] [JOB %d] Remote compaction failed to start.",
                     compaction_input.column_family.name.c_str(), job_id_);
      return compaction_status;
    case CompactionServiceJobStatus::kUseLocal:
      ROCKS_LOG_INFO(
          db_options_.info_log,
          "[%s] [JOB %d] Remote compaction fallback to local by API Start.",
          compaction_input.column_family.name.c_str(), job_id_);
      return compaction_status;
    default:
      assert(false);  // unknown status
      break;
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Waiting for remote compaction...",
                 compaction_input.column_family.name.c_str(), job_id_);
  std::string compaction_result_binary;
  compaction_status = db_options_.compaction_service->WaitForCompleteV2(
      info, &compaction_result_binary);

  if (compaction_status == CompactionServiceJobStatus::kUseLocal) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Remote compaction fallback to local by API "
                   "WaitForComplete.",
                   compaction_input.column_family.name.c_str(), job_id_);
    return compaction_status;
  }

  CompactionServiceResult compaction_result;
  s = CompactionServiceResult::Read(compaction_result_binary,
                                    &compaction_result);

  if (compaction_status == CompactionServiceJobStatus::kFailure) {
    if (s.ok()) {
      if (compaction_result.status.ok()) {
        sub_compact->status = Status::Incomplete(
            "CompactionService failed to run the compaction job (even though "
            "the internal status is okay).");
      } else {
        // set the current sub compaction status with the status returned from
        // remote
        sub_compact->status = compaction_result.status;
      }
    } else {
      sub_compact->status = Status::Incomplete(
          "CompactionService failed to run the compaction job (and no valid "
          "result is returned).");
      compaction_result.status.PermitUncheckedError();
    }
    ROCKS_LOG_WARN(db_options_.info_log,
                   "[%s] [JOB %d] Remote compaction failed.",
                   compaction_input.column_family.name.c_str(), job_id_);
    return compaction_status;
  }

  if (!s.ok()) {
    sub_compact->status = s;
    compaction_result.status.PermitUncheckedError();
    return CompactionServiceJobStatus::kFailure;
  }
  sub_compact->status = compaction_result.status;

  std::ostringstream output_files_oss;
  is_first_one = true;
  for (const auto& file : compaction_result.output_files) {
    output_files_oss << (is_first_one ? "" : ", ") << file.file_name;
    is_first_one = false;
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Receive remote compaction result, output path: "
                 "%s, files: %s",
                 compaction_input.column_family.name.c_str(), job_id_,
                 compaction_result.output_path.c_str(),
                 output_files_oss.str().c_str());

  if (!s.ok()) {
    sub_compact->status = s;
    return CompactionServiceJobStatus::kFailure;
  }

  for (const auto& file : compaction_result.output_files) {
    uint64_t file_num = versions_->NewFileNumber();
    auto src_file = compaction_result.output_path + "/" + file.file_name;
    auto tgt_file = TableFileName(compaction->immutable_options()->cf_paths,
                                  file_num, compaction->output_path_id());
    s = fs_->RenameFile(src_file, tgt_file, IOOptions(), nullptr);
    if (!s.ok()) {
      sub_compact->status = s;
      return CompactionServiceJobStatus::kFailure;
    }

    FileMetaData meta;
    uint64_t file_size;
    s = fs_->GetFileSize(tgt_file, IOOptions(), &file_size, nullptr);
    if (!s.ok()) {
      sub_compact->status = s;
      return CompactionServiceJobStatus::kFailure;
    }
    meta.fd = FileDescriptor(file_num, compaction->output_path_id(), file_size,
                             file.smallest_seqno, file.largest_seqno);
    meta.smallest.DecodeFrom(file.smallest_internal_key);
    meta.largest.DecodeFrom(file.largest_internal_key);
    meta.oldest_ancester_time = file.oldest_ancester_time;
    meta.file_creation_time = file.file_creation_time;
    meta.epoch_number = file.epoch_number;
    meta.marked_for_compaction = file.marked_for_compaction;
    meta.unique_id = file.unique_id;

    auto cfd = compaction->column_family_data();
    sub_compact->Current().AddOutput(std::move(meta),
                                     cfd->internal_comparator(), false, false,
                                     true, file.paranoid_hash);
  }
  sub_compact->compaction_job_stats = compaction_result.stats;
  sub_compact->Current().SetNumOutputRecords(
      compaction_result.num_output_records);
  sub_compact->Current().SetTotalBytes(compaction_result.total_bytes);
  RecordTick(stats_, REMOTE_COMPACT_READ_BYTES, compaction_result.bytes_read);
  RecordTick(stats_, REMOTE_COMPACT_WRITE_BYTES,
             compaction_result.bytes_written);
  return CompactionServiceJobStatus::kSuccess;
}

std::string CompactionServiceCompactionJob::GetTableFileName(
    uint64_t file_number) {
  return MakeTableFileName(output_path_, file_number);
}

void CompactionServiceCompactionJob::RecordCompactionIOStats() {
  compaction_result_->bytes_read += IOSTATS(bytes_read);
  compaction_result_->bytes_written += IOSTATS(bytes_written);
  CompactionJob::RecordCompactionIOStats();
}

CompactionServiceCompactionJob::CompactionServiceCompactionJob(
    int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
    const MutableDBOptions& mutable_db_options, const FileOptions& file_options,
    VersionSet* versions, const std::atomic<bool>* shutting_down,
    LogBuffer* log_buffer, FSDirectory* output_directory, Statistics* stats,
    InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
    std::vector<SequenceNumber> existing_snapshots,
    std::shared_ptr<Cache> table_cache, EventLogger* event_logger,
    const std::string& dbname, const std::shared_ptr<IOTracer>& io_tracer,
    const std::atomic<bool>& manual_compaction_canceled,
    const std::string& db_id, const std::string& db_session_id,
    std::string output_path,
    const CompactionServiceInput& compaction_service_input,
    CompactionServiceResult* compaction_service_result)
    : CompactionJob(
          job_id, compaction, db_options, mutable_db_options, file_options,
          versions, shutting_down, log_buffer, nullptr, output_directory,
          nullptr, stats, db_mutex, db_error_handler,
          std::move(existing_snapshots), kMaxSequenceNumber, nullptr, nullptr,
          std::move(table_cache), event_logger,
          compaction->mutable_cf_options()->paranoid_file_checks,
          compaction->mutable_cf_options()->report_bg_io_stats, dbname,
          &(compaction_service_result->stats), Env::Priority::USER, io_tracer,
          manual_compaction_canceled, db_id, db_session_id,
          compaction->column_family_data()->GetFullHistoryTsLow()),
      output_path_(std::move(output_path)),
      compaction_input_(compaction_service_input),
      compaction_result_(compaction_service_result) {}

Status CompactionServiceCompactionJob::Run() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_RUN);

  auto* c = compact_->compaction;
  assert(c->column_family_data() != nullptr);
  assert(c->column_family_data()->current()->storage_info()->NumLevelFiles(
             compact_->compaction->level()) > 0);

  write_hint_ =
      c->column_family_data()->CalculateSSTWriteHint(c->output_level());
  bottommost_level_ = c->bottommost_level();

  Slice begin = compaction_input_.begin;
  Slice end = compaction_input_.end;
  compact_->sub_compact_states.emplace_back(
      c,
      compaction_input_.has_begin ? std::optional<Slice>(begin)
                                  : std::optional<Slice>(),
      compaction_input_.has_end ? std::optional<Slice>(end)
                                : std::optional<Slice>(),
      /*sub_job_id*/ 0);

  log_buffer_->FlushBufferToLog();
  LogCompaction();
  const uint64_t start_micros = db_options_.clock->NowMicros();
  // Pick the only sub-compaction we should have
  assert(compact_->sub_compact_states.size() == 1);
  SubcompactionState* sub_compact = compact_->sub_compact_states.data();

  ProcessKeyValueCompaction(sub_compact);

  compaction_stats_.stats.micros =
      db_options_.clock->NowMicros() - start_micros;
  compaction_stats_.stats.cpu_micros =
      sub_compact->compaction_job_stats.cpu_micros;

  RecordTimeToHistogram(stats_, COMPACTION_TIME,
                        compaction_stats_.stats.micros);
  RecordTimeToHistogram(stats_, COMPACTION_CPU_TIME,
                        compaction_stats_.stats.cpu_micros);

  Status status = sub_compact->status;
  IOStatus io_s = sub_compact->io_status;

  if (io_status_.ok()) {
    io_status_ = io_s;
  }

  if (status.ok()) {
    constexpr IODebugContext* dbg = nullptr;

    if (output_directory_) {
      io_s = output_directory_->FsyncWithDirOptions(IOOptions(), dbg,
                                                    DirFsyncOptions());
    }
  }
  if (io_status_.ok()) {
    io_status_ = io_s;
  }
  if (status.ok()) {
    status = io_s;
  }
  if (status.ok()) {
    // TODO: Add verify_table()
  }

  // Finish up all book-keeping to unify the subcompaction results
  compact_->AggregateCompactionStats(compaction_stats_, *compaction_job_stats_);
  UpdateCompactionStats();
  RecordCompactionIOStats();

  LogFlush(db_options_.info_log);
  compact_->status = status;
  compact_->status.PermitUncheckedError();

  // Build compaction result
  compaction_result_->output_level = compact_->compaction->output_level();
  compaction_result_->output_path = output_path_;
  for (const auto& output_file : sub_compact->GetOutputs()) {
    auto& meta = output_file.meta;
    compaction_result_->output_files.emplace_back(
        MakeTableFileName(meta.fd.GetNumber()), meta.fd.smallest_seqno,
        meta.fd.largest_seqno, meta.smallest.Encode().ToString(),
        meta.largest.Encode().ToString(), meta.oldest_ancester_time,
        meta.file_creation_time, meta.epoch_number,
        output_file.validator.GetHash(), meta.marked_for_compaction,
        meta.unique_id);
  }
  InternalStats::CompactionStatsFull compaction_stats;
  sub_compact->AggregateCompactionStats(compaction_stats);
  compaction_result_->num_output_records =
      compaction_stats.stats.num_output_records;
  compaction_result_->total_bytes = compaction_stats.TotalBytesWritten();

  return status;
}

void CompactionServiceCompactionJob::CleanupCompaction() {
  CompactionJob::CleanupCompaction();
}

// Internal binary format for the input and result data
enum BinaryFormatVersion : uint32_t {
  kOptionsString = 1,  // Use string format similar to Option string format
};

static std::unordered_map<std::string, OptionTypeInfo> cfd_type_info = {
    {"name",
     {offsetof(struct ColumnFamilyDescriptor, name), OptionType::kEncodedString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"options",
     {offsetof(struct ColumnFamilyDescriptor, options),
      OptionType::kConfigurable, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto cf_options = static_cast<ColumnFamilyOptions*>(addr);
        return GetColumnFamilyOptionsFromString(opts, ColumnFamilyOptions(),
                                                value, cf_options);
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto cf_options = static_cast<const ColumnFamilyOptions*>(addr);
        std::string result;
        auto status =
            GetStringFromColumnFamilyOptions(opts, *cf_options, &result);
        *value = "{" + result + "}";
        return status;
      },
      [](const ConfigOptions& opts, const std::string& name, const void* addr1,
         const void* addr2, std::string* mismatch) {
        const auto this_one = static_cast<const ColumnFamilyOptions*>(addr1);
        const auto that_one = static_cast<const ColumnFamilyOptions*>(addr2);
        auto this_conf = CFOptionsAsConfigurable(*this_one);
        auto that_conf = CFOptionsAsConfigurable(*that_one);
        std::string mismatch_opt;
        bool result =
            this_conf->AreEquivalent(opts, that_conf.get(), &mismatch_opt);
        if (!result) {
          *mismatch = name + "." + mismatch_opt;
        }
        return result;
      }}},
};

static std::unordered_map<std::string, OptionTypeInfo> cs_input_type_info = {
    {"column_family",
     OptionTypeInfo::Struct(
         "column_family", &cfd_type_info,
         offsetof(struct CompactionServiceInput, column_family),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
    {"db_options",
     {offsetof(struct CompactionServiceInput, db_options),
      OptionType::kConfigurable, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto options = static_cast<DBOptions*>(addr);
        return GetDBOptionsFromString(opts, DBOptions(), value, options);
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto options = static_cast<const DBOptions*>(addr);
        std::string result;
        auto status = GetStringFromDBOptions(opts, *options, &result);
        *value = "{" + result + "}";
        return status;
      },
      [](const ConfigOptions& opts, const std::string& name, const void* addr1,
         const void* addr2, std::string* mismatch) {
        const auto this_one = static_cast<const DBOptions*>(addr1);
        const auto that_one = static_cast<const DBOptions*>(addr2);
        auto this_conf = DBOptionsAsConfigurable(*this_one);
        auto that_conf = DBOptionsAsConfigurable(*that_one);
        std::string mismatch_opt;
        bool result =
            this_conf->AreEquivalent(opts, that_conf.get(), &mismatch_opt);
        if (!result) {
          *mismatch = name + "." + mismatch_opt;
        }
        return result;
      }}},
    {"snapshots", OptionTypeInfo::Vector<uint64_t>(
                      offsetof(struct CompactionServiceInput, snapshots),
                      OptionVerificationType::kNormal, OptionTypeFlags::kNone,
                      {0, OptionType::kUInt64T})},
    {"input_files", OptionTypeInfo::Vector<std::string>(
                        offsetof(struct CompactionServiceInput, input_files),
                        OptionVerificationType::kNormal, OptionTypeFlags::kNone,
                        {0, OptionType::kEncodedString})},
    {"output_level",
     {offsetof(struct CompactionServiceInput, output_level), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"db_id",
     {offsetof(struct CompactionServiceInput, db_id),
      OptionType::kEncodedString}},
    {"has_begin",
     {offsetof(struct CompactionServiceInput, has_begin), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"begin",
     {offsetof(struct CompactionServiceInput, begin),
      OptionType::kEncodedString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"has_end",
     {offsetof(struct CompactionServiceInput, has_end), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"end",
     {offsetof(struct CompactionServiceInput, end), OptionType::kEncodedString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    cs_output_file_type_info = {
        {"file_name",
         {offsetof(struct CompactionServiceOutputFile, file_name),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_seqno",
         {offsetof(struct CompactionServiceOutputFile, smallest_seqno),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_seqno",
         {offsetof(struct CompactionServiceOutputFile, largest_seqno),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_internal_key",
         {offsetof(struct CompactionServiceOutputFile, smallest_internal_key),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_internal_key",
         {offsetof(struct CompactionServiceOutputFile, largest_internal_key),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"oldest_ancester_time",
         {offsetof(struct CompactionServiceOutputFile, oldest_ancester_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_creation_time",
         {offsetof(struct CompactionServiceOutputFile, file_creation_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"epoch_number",
         {offsetof(struct CompactionServiceOutputFile, epoch_number),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"paranoid_hash",
         {offsetof(struct CompactionServiceOutputFile, paranoid_hash),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"marked_for_compaction",
         {offsetof(struct CompactionServiceOutputFile, marked_for_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"unique_id",
         OptionTypeInfo::Array<uint64_t, 2>(
             offsetof(struct CompactionServiceOutputFile, unique_id),
             OptionVerificationType::kNormal, OptionTypeFlags::kNone,
             {0, OptionType::kUInt64T})},
};

static std::unordered_map<std::string, OptionTypeInfo>
    compaction_job_stats_type_info = {
        {"elapsed_micros",
         {offsetof(struct CompactionJobStats, elapsed_micros),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"cpu_micros",
         {offsetof(struct CompactionJobStats, cpu_micros), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"num_input_records",
         {offsetof(struct CompactionJobStats, num_input_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_blobs_read",
         {offsetof(struct CompactionJobStats, num_blobs_read),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_files",
         {offsetof(struct CompactionJobStats, num_input_files),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_files_at_output_level",
         {offsetof(struct CompactionJobStats, num_input_files_at_output_level),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_records",
         {offsetof(struct CompactionJobStats, num_output_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_files",
         {offsetof(struct CompactionJobStats, num_output_files),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_files_blob",
         {offsetof(struct CompactionJobStats, num_output_files_blob),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"is_full_compaction",
         {offsetof(struct CompactionJobStats, is_full_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"is_manual_compaction",
         {offsetof(struct CompactionJobStats, is_manual_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_bytes",
         {offsetof(struct CompactionJobStats, total_input_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_blob_bytes_read",
         {offsetof(struct CompactionJobStats, total_blob_bytes_read),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_output_bytes",
         {offsetof(struct CompactionJobStats, total_output_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_output_bytes_blob",
         {offsetof(struct CompactionJobStats, total_output_bytes_blob),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_records_replaced",
         {offsetof(struct CompactionJobStats, num_records_replaced),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_raw_key_bytes",
         {offsetof(struct CompactionJobStats, total_input_raw_key_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_raw_value_bytes",
         {offsetof(struct CompactionJobStats, total_input_raw_value_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_deletion_records",
         {offsetof(struct CompactionJobStats, num_input_deletion_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_expired_deletion_records",
         {offsetof(struct CompactionJobStats, num_expired_deletion_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_corrupt_keys",
         {offsetof(struct CompactionJobStats, num_corrupt_keys),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_write_nanos",
         {offsetof(struct CompactionJobStats, file_write_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_range_sync_nanos",
         {offsetof(struct CompactionJobStats, file_range_sync_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_fsync_nanos",
         {offsetof(struct CompactionJobStats, file_fsync_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_prepare_write_nanos",
         {offsetof(struct CompactionJobStats, file_prepare_write_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_output_key_prefix",
         {offsetof(struct CompactionJobStats, smallest_output_key_prefix),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_output_key_prefix",
         {offsetof(struct CompactionJobStats, largest_output_key_prefix),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_single_del_fallthru",
         {offsetof(struct CompactionJobStats, num_single_del_fallthru),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_single_del_mismatch",
         {offsetof(struct CompactionJobStats, num_single_del_mismatch),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

namespace {
// this is a helper struct to serialize and deserialize class Status, because
// Status's members are not public.
struct StatusSerializationAdapter {
  uint8_t code;
  uint8_t subcode;
  uint8_t severity;
  std::string message;

  StatusSerializationAdapter() = default;
  explicit StatusSerializationAdapter(const Status& s) {
    code = s.code();
    subcode = s.subcode();
    severity = s.severity();
    auto msg = s.getState();
    message = msg ? msg : "";
  }

  Status GetStatus() const {
    return Status{static_cast<Status::Code>(code),
                  static_cast<Status::SubCode>(subcode),
                  static_cast<Status::Severity>(severity), message};
  }
};
}  // namespace

static std::unordered_map<std::string, OptionTypeInfo>
    status_adapter_type_info = {
        {"code",
         {offsetof(struct StatusSerializationAdapter, code),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"subcode",
         {offsetof(struct StatusSerializationAdapter, subcode),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"severity",
         {offsetof(struct StatusSerializationAdapter, severity),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"message",
         {offsetof(struct StatusSerializationAdapter, message),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo> cs_result_type_info = {
    {"status",
     {offsetof(struct CompactionServiceResult, status),
      OptionType::kCustomizable, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto status_obj = static_cast<Status*>(addr);
        StatusSerializationAdapter adapter;
        Status s = OptionTypeInfo::ParseType(
            opts, value, status_adapter_type_info, &adapter);
        *status_obj = adapter.GetStatus();
        return s;
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto status_obj = static_cast<const Status*>(addr);
        StatusSerializationAdapter adapter(*status_obj);
        std::string result;
        Status s = OptionTypeInfo::SerializeType(opts, status_adapter_type_info,
                                                 &adapter, &result);
        *value = "{" + result + "}";
        return s;
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr1, const void* addr2, std::string* mismatch) {
        const auto status1 = static_cast<const Status*>(addr1);
        const auto status2 = static_cast<const Status*>(addr2);

        StatusSerializationAdapter adatper1(*status1);
        StatusSerializationAdapter adapter2(*status2);
        return OptionTypeInfo::TypesAreEqual(opts, status_adapter_type_info,
                                             &adatper1, &adapter2, mismatch);
      }}},
    {"output_files",
     OptionTypeInfo::Vector<CompactionServiceOutputFile>(
         offsetof(struct CompactionServiceResult, output_files),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone,
         OptionTypeInfo::Struct("output_files", &cs_output_file_type_info, 0,
                                OptionVerificationType::kNormal,
                                OptionTypeFlags::kNone))},
    {"output_level",
     {offsetof(struct CompactionServiceResult, output_level), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"output_path",
     {offsetof(struct CompactionServiceResult, output_path),
      OptionType::kEncodedString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"num_output_records",
     {offsetof(struct CompactionServiceResult, num_output_records),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"total_bytes",
     {offsetof(struct CompactionServiceResult, total_bytes),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"bytes_read",
     {offsetof(struct CompactionServiceResult, bytes_read),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"bytes_written",
     {offsetof(struct CompactionServiceResult, bytes_written),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"stats", OptionTypeInfo::Struct(
                  "stats", &compaction_job_stats_type_info,
                  offsetof(struct CompactionServiceResult, stats),
                  OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
};

Status CompactionServiceInput::Read(const std::string& data_str,
                                    CompactionServiceInput* obj) {
  if (data_str.size() <= sizeof(BinaryFormatVersion)) {
    return Status::InvalidArgument("Invalid CompactionServiceInput string");
  }
  auto format_version = DecodeFixed32(data_str.data());
  if (format_version == kOptionsString) {
    ConfigOptions cf;
    cf.invoke_prepare_options = false;
    cf.ignore_unknown_options = true;
    return OptionTypeInfo::ParseType(
        cf, data_str.substr(sizeof(BinaryFormatVersion)), cs_input_type_info,
        obj);
  } else {
    return Status::NotSupported(
        "Compaction Service Input data version not supported: " +
        std::to_string(format_version));
  }
}

Status CompactionServiceInput::Write(std::string* output) {
  char buf[sizeof(BinaryFormatVersion)];
  EncodeFixed32(buf, kOptionsString);
  output->append(buf, sizeof(BinaryFormatVersion));
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::SerializeType(cf, cs_input_type_info, this, output);
}

Status CompactionServiceResult::Read(const std::string& data_str,
                                     CompactionServiceResult* obj) {
  if (data_str.size() <= sizeof(BinaryFormatVersion)) {
    return Status::InvalidArgument("Invalid CompactionServiceResult string");
  }
  auto format_version = DecodeFixed32(data_str.data());
  if (format_version == kOptionsString) {
    ConfigOptions cf;
    cf.invoke_prepare_options = false;
    cf.ignore_unknown_options = true;
    return OptionTypeInfo::ParseType(
        cf, data_str.substr(sizeof(BinaryFormatVersion)), cs_result_type_info,
        obj);
  } else {
    return Status::NotSupported(
        "Compaction Service Result data version not supported: " +
        std::to_string(format_version));
  }
}

Status CompactionServiceResult::Write(std::string* output) {
  char buf[sizeof(BinaryFormatVersion)];
  EncodeFixed32(buf, kOptionsString);
  output->append(buf, sizeof(BinaryFormatVersion));
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::SerializeType(cf, cs_result_type_info, this, output);
}

#ifndef NDEBUG
bool CompactionServiceResult::TEST_Equals(CompactionServiceResult* other) {
  std::string mismatch;
  return TEST_Equals(other, &mismatch);
}

bool CompactionServiceResult::TEST_Equals(CompactionServiceResult* other,
                                          std::string* mismatch) {
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::TypesAreEqual(cf, cs_result_type_info, this, other,
                                       mismatch);
}

bool CompactionServiceInput::TEST_Equals(CompactionServiceInput* other) {
  std::string mismatch;
  return TEST_Equals(other, &mismatch);
}

bool CompactionServiceInput::TEST_Equals(CompactionServiceInput* other,
                                         std::string* mismatch) {
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::TypesAreEqual(cf, cs_input_type_info, this, other,
                                       mismatch);
}
#endif  // NDEBUG
}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
