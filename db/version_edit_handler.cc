//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit_handler.h"

#include <cinttypes>
#include <sstream>

#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_source.h"
#include "db/version_edit.h"
#include "logging/logging.h"
#include "monitoring/persistent_stats_history.h"

namespace ROCKSDB_NAMESPACE {

void VersionEditHandlerBase::Iterate(log::Reader& reader,
                                     Status* log_read_status) {
  Slice record;
  std::string scratch;
  assert(log_read_status);
  assert(log_read_status->ok());

  size_t recovered_edits = 0;
  Status s = Initialize();
  while (reader.LastRecordEnd() < max_manifest_read_size_ && s.ok() &&
         reader.ReadRecord(&record, &scratch) && log_read_status->ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      break;
    }

    s = read_buffer_.AddEdit(&edit);
    if (!s.ok()) {
      break;
    }
    ColumnFamilyData* cfd = nullptr;
    if (edit.is_in_atomic_group_) {
      if (read_buffer_.IsFull()) {
        for (auto& e : read_buffer_.replay_buffer()) {
          s = ApplyVersionEdit(e, &cfd);
          if (!s.ok()) {
            break;
          }
          ++recovered_edits;
        }
        if (!s.ok()) {
          break;
        }
        read_buffer_.Clear();
      }
    } else {
      s = ApplyVersionEdit(edit, &cfd);
      if (s.ok()) {
        ++recovered_edits;
      }
    }
  }
  if (!log_read_status->ok()) {
    s = *log_read_status;
  }

  CheckIterationResult(reader, &s);

  if (!s.ok()) {
    if (s.IsCorruption()) {
      // when we find a Corruption error, something is
      // wrong with the underlying file. in this case we
      // want to report the filename, so in here we append
      // the filename to the Corruption message
      assert(reader.file());

      // build a new error message
      std::stringstream message;
      // append previous dynamic state message
      const char* state = s.getState();
      if (state != nullptr) {
        message << state;
        message << ' ';
      }
      // append the filename to the corruption message
      message << "in file " << reader.file()->file_name();
      // overwrite the status with the extended status
      s = Status(s.code(), s.subcode(), s.severity(), message.str());
    }
    status_ = s;
  }
  TEST_SYNC_POINT_CALLBACK("VersionEditHandlerBase::Iterate:Finish",
                           &recovered_edits);
}

Status ListColumnFamiliesHandler::ApplyVersionEdit(
    VersionEdit& edit, ColumnFamilyData** /*unused*/) {
  Status s;
  if (edit.is_column_family_add_) {
    if (column_family_names_.find(edit.column_family_) !=
        column_family_names_.end()) {
      s = Status::Corruption("Manifest adding the same column family twice");
    } else {
      column_family_names_.insert(
          {edit.column_family_, edit.column_family_name_});
    }
  } else if (edit.is_column_family_drop_) {
    if (column_family_names_.find(edit.column_family_) ==
        column_family_names_.end()) {
      s = Status::Corruption("Manifest - dropping non-existing column family");
    } else {
      column_family_names_.erase(edit.column_family_);
    }
  }
  return s;
}

Status FileChecksumRetriever::ApplyVersionEdit(VersionEdit& edit,
                                               ColumnFamilyData** /*unused*/) {
  for (const auto& deleted_file : edit.GetDeletedFiles()) {
    Status s = file_checksum_list_.RemoveOneFileChecksum(deleted_file.second);
    if (!s.ok()) {
      return s;
    }
  }
  for (const auto& new_file : edit.GetNewFiles()) {
    Status s = file_checksum_list_.InsertOneFileChecksum(
        new_file.second.fd.GetNumber(), new_file.second.file_checksum,
        new_file.second.file_checksum_func_name);
    if (!s.ok()) {
      return s;
    }
  }
  for (const auto& new_blob_file : edit.GetBlobFileAdditions()) {
    std::string checksum_value = new_blob_file.GetChecksumValue();
    std::string checksum_method = new_blob_file.GetChecksumMethod();
    assert(checksum_value.empty() == checksum_method.empty());
    if (checksum_method.empty()) {
      checksum_value = kUnknownFileChecksum;
      checksum_method = kUnknownFileChecksumFuncName;
    }
    Status s = file_checksum_list_.InsertOneFileChecksum(
        new_blob_file.GetBlobFileNumber(), checksum_value, checksum_method);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

VersionEditHandler::VersionEditHandler(
    bool read_only, std::vector<ColumnFamilyDescriptor> column_families,
    VersionSet* version_set, bool track_missing_files,
    bool no_error_if_files_missing, const std::shared_ptr<IOTracer>& io_tracer,
    bool skip_load_table_files, EpochNumberRequirement epoch_number_requirement)
    : VersionEditHandlerBase(),
      read_only_(read_only),
      column_families_(std::move(column_families)),
      version_set_(version_set),
      track_missing_files_(track_missing_files),
      no_error_if_files_missing_(no_error_if_files_missing),
      io_tracer_(io_tracer),
      skip_load_table_files_(skip_load_table_files),
      initialized_(false),
      epoch_number_requirement_(epoch_number_requirement) {
  assert(version_set_ != nullptr);
}

Status VersionEditHandler::Initialize() {
  Status s;
  if (!initialized_) {
    for (const auto& cf_desc : column_families_) {
      name_to_options_.emplace(cf_desc.name, cf_desc.options);
    }
    auto default_cf_iter = name_to_options_.find(kDefaultColumnFamilyName);
    if (default_cf_iter == name_to_options_.end()) {
      s = Status::InvalidArgument("Default column family not specified");
    }
    if (s.ok()) {
      VersionEdit default_cf_edit;
      default_cf_edit.AddColumnFamily(kDefaultColumnFamilyName);
      default_cf_edit.SetColumnFamily(0);
      ColumnFamilyData* cfd =
          CreateCfAndInit(default_cf_iter->second, default_cf_edit);
      assert(cfd != nullptr);
#ifdef NDEBUG
      (void)cfd;
#endif
      initialized_ = true;
    }
  }
  return s;
}

Status VersionEditHandler::ApplyVersionEdit(VersionEdit& edit,
                                            ColumnFamilyData** cfd) {
  Status s;
  if (edit.is_column_family_add_) {
    s = OnColumnFamilyAdd(edit, cfd);
  } else if (edit.is_column_family_drop_) {
    s = OnColumnFamilyDrop(edit, cfd);
  } else if (edit.IsWalAddition()) {
    s = OnWalAddition(edit);
  } else if (edit.IsWalDeletion()) {
    s = OnWalDeletion(edit);
  } else {
    s = OnNonCfOperation(edit, cfd);
  }
  if (s.ok()) {
    assert(cfd != nullptr);
    s = ExtractInfoFromVersionEdit(*cfd, edit);
  }
  return s;
}

Status VersionEditHandler::OnColumnFamilyAdd(VersionEdit& edit,
                                             ColumnFamilyData** cfd) {
  bool cf_in_not_found = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &cf_in_not_found, &cf_in_builders);

  assert(cfd != nullptr);
  *cfd = nullptr;
  Status s;
  if (cf_in_builders || cf_in_not_found) {
    s = Status::Corruption("MANIFEST adding the same column family twice: " +
                           edit.column_family_name_);
  }
  if (s.ok()) {
    auto cf_options = name_to_options_.find(edit.column_family_name_);
    // implicitly add persistent_stats column family without requiring user
    // to specify
    ColumnFamilyData* tmp_cfd = nullptr;
    bool is_persistent_stats_column_family =
        edit.column_family_name_.compare(kPersistentStatsColumnFamilyName) == 0;
    if (cf_options == name_to_options_.end() &&
        !is_persistent_stats_column_family) {
      column_families_not_found_.emplace(edit.column_family_,
                                         edit.column_family_name_);
    } else {
      if (is_persistent_stats_column_family) {
        ColumnFamilyOptions cfo;
        OptimizeForPersistentStats(&cfo);
        tmp_cfd = CreateCfAndInit(cfo, edit);
      } else {
        tmp_cfd = CreateCfAndInit(cf_options->second, edit);
      }
      *cfd = tmp_cfd;
    }
  }
  return s;
}

Status VersionEditHandler::OnColumnFamilyDrop(VersionEdit& edit,
                                              ColumnFamilyData** cfd) {
  bool cf_in_not_found = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &cf_in_not_found, &cf_in_builders);

  assert(cfd != nullptr);
  *cfd = nullptr;
  ColumnFamilyData* tmp_cfd = nullptr;
  Status s;
  if (cf_in_builders) {
    tmp_cfd = DestroyCfAndCleanup(edit);
  } else if (cf_in_not_found) {
    column_families_not_found_.erase(edit.column_family_);
  } else {
    s = Status::Corruption("MANIFEST - dropping non-existing column family");
  }
  *cfd = tmp_cfd;
  return s;
}

Status VersionEditHandler::OnWalAddition(VersionEdit& edit) {
  assert(edit.IsWalAddition());
  return version_set_->wals_.AddWals(edit.GetWalAdditions());
}

Status VersionEditHandler::OnWalDeletion(VersionEdit& edit) {
  assert(edit.IsWalDeletion());
  return version_set_->wals_.DeleteWalsBefore(
      edit.GetWalDeletion().GetLogNumber());
}

Status VersionEditHandler::OnNonCfOperation(VersionEdit& edit,
                                            ColumnFamilyData** cfd) {
  bool cf_in_not_found = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &cf_in_not_found, &cf_in_builders);

  assert(cfd != nullptr);
  *cfd = nullptr;
  Status s;
  if (!cf_in_not_found) {
    if (!cf_in_builders) {
      s = Status::Corruption(
          "MANIFEST record referencing unknown column family");
    }
    ColumnFamilyData* tmp_cfd = nullptr;
    if (s.ok()) {
      auto builder_iter = builders_.find(edit.column_family_);
      assert(builder_iter != builders_.end());
      tmp_cfd = version_set_->GetColumnFamilySet()->GetColumnFamily(
          edit.column_family_);
      assert(tmp_cfd != nullptr);
      s = MaybeCreateVersion(edit, tmp_cfd, /*force_create_version=*/false);
      if (s.ok()) {
        s = builder_iter->second->version_builder()->Apply(&edit);
      }
    }
    *cfd = tmp_cfd;
  }
  return s;
}

// TODO maybe cache the computation result
bool VersionEditHandler::HasMissingFiles() const {
  bool ret = false;
  for (const auto& elem : cf_to_missing_files_) {
    const auto& missing_files = elem.second;
    if (!missing_files.empty()) {
      ret = true;
      break;
    }
  }
  if (!ret) {
    for (const auto& elem : cf_to_missing_blob_files_high_) {
      if (elem.second != kInvalidBlobFileNumber) {
        ret = true;
        break;
      }
    }
  }
  return ret;
}

void VersionEditHandler::CheckColumnFamilyId(const VersionEdit& edit,
                                             bool* cf_in_not_found,
                                             bool* cf_in_builders) const {
  assert(cf_in_not_found != nullptr);
  assert(cf_in_builders != nullptr);
  // Not found means that user didn't supply that column
  // family option AND we encountered column family add
  // record. Once we encounter column family drop record,
  // we will delete the column family from
  // column_families_not_found.
  bool in_not_found = column_families_not_found_.find(edit.column_family_) !=
                      column_families_not_found_.end();
  // in builders means that user supplied that column family
  // option AND that we encountered column family add record
  bool in_builders = builders_.find(edit.column_family_) != builders_.end();
  // They cannot both be true
  assert(!(in_not_found && in_builders));
  *cf_in_not_found = in_not_found;
  *cf_in_builders = in_builders;
}

void VersionEditHandler::CheckIterationResult(const log::Reader& reader,
                                              Status* s) {
  assert(s != nullptr);
  if (!s->ok()) {
    // Do nothing here.
  } else if (!version_edit_params_.has_log_number_ ||
             !version_edit_params_.has_next_file_number_ ||
             !version_edit_params_.has_last_sequence_) {
    std::string msg("no ");
    if (!version_edit_params_.has_log_number_) {
      msg.append("log_file_number, ");
    }
    if (!version_edit_params_.has_next_file_number_) {
      msg.append("next_file_number, ");
    }
    if (!version_edit_params_.has_last_sequence_) {
      msg.append("last_sequence, ");
    }
    msg = msg.substr(0, msg.size() - 2);
    msg.append(" entry in MANIFEST");
    *s = Status::Corruption(msg);
  }
  // There were some column families in the MANIFEST that weren't specified
  // in the argument. This is OK in read_only mode
  if (s->ok() && MustOpenAllColumnFamilies() &&
      !column_families_not_found_.empty()) {
    std::string msg;
    for (const auto& cf : column_families_not_found_) {
      msg.append(", ");
      msg.append(cf.second);
    }
    msg = msg.substr(2);
    *s = Status::InvalidArgument("Column families not opened: " + msg);
  }
  if (s->ok()) {
    version_set_->GetColumnFamilySet()->UpdateMaxColumnFamily(
        version_edit_params_.max_column_family_);
    version_set_->MarkMinLogNumberToKeep(
        version_edit_params_.min_log_number_to_keep_);
    version_set_->MarkFileNumberUsed(version_edit_params_.prev_log_number_);
    version_set_->MarkFileNumberUsed(version_edit_params_.log_number_);
    for (auto* cfd : *(version_set_->GetColumnFamilySet())) {
      if (cfd->IsDropped()) {
        continue;
      }
      auto builder_iter = builders_.find(cfd->GetID());
      assert(builder_iter != builders_.end());
      auto* builder = builder_iter->second->version_builder();
      if (!builder->CheckConsistencyForNumLevels()) {
        *s = Status::InvalidArgument(
            "db has more levels than options.num_levels");
        break;
      }
    }
  }
  if (s->ok()) {
    for (auto* cfd : *(version_set_->GetColumnFamilySet())) {
      if (cfd->IsDropped()) {
        continue;
      }
      if (read_only_) {
        cfd->table_cache()->SetTablesAreImmortal();
      }
      *s = LoadTables(cfd, /*prefetch_index_and_filter_in_cache=*/false,
                      /*is_initial_load=*/true);
      if (!s->ok()) {
        // If s is IOError::PathNotFound, then we mark the db as corrupted.
        if (s->IsPathNotFound()) {
          *s = Status::Corruption("Corruption: " + s->ToString());
        }
        break;
      }
    }
  }

  if (s->ok()) {
    for (auto* cfd : *(version_set_->column_family_set_)) {
      if (cfd->IsDropped()) {
        continue;
      }
      assert(cfd->initialized());
      VersionEdit edit;
      *s = MaybeCreateVersion(edit, cfd, /*force_create_version=*/true);
      if (!s->ok()) {
        break;
      }
    }
  }
  if (s->ok()) {
    version_set_->manifest_file_size_ = reader.GetReadOffset();
    assert(version_set_->manifest_file_size_ > 0);
    version_set_->next_file_number_.store(
        version_edit_params_.next_file_number_ + 1);
    SequenceNumber last_seq = version_edit_params_.last_sequence_;
    assert(last_seq != kMaxSequenceNumber);
    if (last_seq != kMaxSequenceNumber &&
        last_seq > version_set_->last_allocated_sequence_.load()) {
      version_set_->last_allocated_sequence_.store(last_seq);
    }
    if (last_seq != kMaxSequenceNumber &&
        last_seq > version_set_->last_published_sequence_.load()) {
      version_set_->last_published_sequence_.store(last_seq);
    }
    if (last_seq != kMaxSequenceNumber &&
        last_seq > version_set_->last_sequence_.load()) {
      version_set_->last_sequence_.store(last_seq);
    }
    if (last_seq != kMaxSequenceNumber &&
        last_seq > version_set_->descriptor_last_sequence_) {
      // This is the maximum last sequence of all `VersionEdit`s iterated. It
      // may be greater than the maximum `largest_seqno` of all files in case
      // the newest data referred to by the MANIFEST has been dropped or had its
      // sequence number zeroed through compaction.
      version_set_->descriptor_last_sequence_ = last_seq;
    }
    version_set_->prev_log_number_ = version_edit_params_.prev_log_number_;
  }
}

ColumnFamilyData* VersionEditHandler::CreateCfAndInit(
    const ColumnFamilyOptions& cf_options, const VersionEdit& edit) {
  ColumnFamilyData* cfd = version_set_->CreateColumnFamily(cf_options, &edit);
  assert(cfd != nullptr);
  cfd->set_initialized();
  assert(builders_.find(edit.column_family_) == builders_.end());
  builders_.emplace(edit.column_family_,
                    VersionBuilderUPtr(new BaseReferencedVersionBuilder(cfd)));
  if (track_missing_files_) {
    cf_to_missing_files_.emplace(edit.column_family_,
                                 std::unordered_set<uint64_t>());
    cf_to_missing_blob_files_high_.emplace(edit.column_family_,
                                           kInvalidBlobFileNumber);
  }
  return cfd;
}

ColumnFamilyData* VersionEditHandler::DestroyCfAndCleanup(
    const VersionEdit& edit) {
  auto builder_iter = builders_.find(edit.column_family_);
  assert(builder_iter != builders_.end());
  builders_.erase(builder_iter);
  if (track_missing_files_) {
    auto missing_files_iter = cf_to_missing_files_.find(edit.column_family_);
    assert(missing_files_iter != cf_to_missing_files_.end());
    cf_to_missing_files_.erase(missing_files_iter);

    auto missing_blob_files_high_iter =
        cf_to_missing_blob_files_high_.find(edit.column_family_);
    assert(missing_blob_files_high_iter !=
           cf_to_missing_blob_files_high_.end());
    cf_to_missing_blob_files_high_.erase(missing_blob_files_high_iter);
  }
  ColumnFamilyData* ret =
      version_set_->GetColumnFamilySet()->GetColumnFamily(edit.column_family_);
  assert(ret != nullptr);
  ret->SetDropped();
  ret->UnrefAndTryDelete();
  ret = nullptr;
  return ret;
}

Status VersionEditHandler::MaybeCreateVersion(const VersionEdit& /*edit*/,
                                              ColumnFamilyData* cfd,
                                              bool force_create_version) {
  assert(cfd->initialized());
  Status s;
  if (force_create_version) {
    auto builder_iter = builders_.find(cfd->GetID());
    assert(builder_iter != builders_.end());
    auto* builder = builder_iter->second->version_builder();
    auto* v = new Version(cfd, version_set_, version_set_->file_options_,
                          *cfd->GetLatestMutableCFOptions(), io_tracer_,
                          version_set_->current_version_number_++,
                          epoch_number_requirement_);
    s = builder->SaveTo(v->storage_info());
    if (s.ok()) {
      // Install new version
      v->PrepareAppend(
          *cfd->GetLatestMutableCFOptions(),
          !(version_set_->db_options_->skip_stats_update_on_db_open));
      version_set_->AppendVersion(cfd, v);
    } else {
      delete v;
    }
  }
  return s;
}

Status VersionEditHandler::LoadTables(ColumnFamilyData* cfd,
                                      bool prefetch_index_and_filter_in_cache,
                                      bool is_initial_load) {
  bool skip_load_table_files = skip_load_table_files_;
  TEST_SYNC_POINT_CALLBACK(
      "VersionEditHandler::LoadTables:skip_load_table_files",
      &skip_load_table_files);
  if (skip_load_table_files) {
    return Status::OK();
  }
  assert(cfd != nullptr);
  assert(!cfd->IsDropped());
  auto builder_iter = builders_.find(cfd->GetID());
  assert(builder_iter != builders_.end());
  assert(builder_iter->second != nullptr);
  VersionBuilder* builder = builder_iter->second->version_builder();
  assert(builder);
  Status s = builder->LoadTableHandlers(
      cfd->internal_stats(),
      version_set_->db_options_->max_file_opening_threads,
      prefetch_index_and_filter_in_cache, is_initial_load,
      cfd->GetLatestMutableCFOptions()->prefix_extractor,
      MaxFileSizeForL0MetaPin(*cfd->GetLatestMutableCFOptions()));
  if ((s.IsPathNotFound() || s.IsCorruption()) && no_error_if_files_missing_) {
    s = Status::OK();
  }
  if (!s.ok() && !version_set_->db_options_->paranoid_checks) {
    s = Status::OK();
  }
  return s;
}

Status VersionEditHandler::ExtractInfoFromVersionEdit(ColumnFamilyData* cfd,
                                                      const VersionEdit& edit) {
  Status s;
  if (edit.has_db_id_) {
    version_set_->db_id_ = edit.GetDbId();
    version_edit_params_.SetDBId(edit.db_id_);
  }
  if (cfd != nullptr) {
    if (edit.has_log_number_) {
      if (cfd->GetLogNumber() > edit.log_number_) {
        ROCKS_LOG_WARN(
            version_set_->db_options()->info_log,
            "MANIFEST corruption detected, but ignored - Log numbers in "
            "records NOT monotonically increasing");
      } else {
        cfd->SetLogNumber(edit.log_number_);
        version_edit_params_.SetLogNumber(edit.log_number_);
      }
    }
    if (edit.has_comparator_ &&
        edit.comparator_ != cfd->user_comparator()->Name()) {
      if (!cf_to_cmp_names_) {
        s = Status::InvalidArgument(
            cfd->user_comparator()->Name(),
            "does not match existing comparator " + edit.comparator_);
      } else {
        cf_to_cmp_names_->emplace(cfd->GetID(), edit.comparator_);
      }
    }
    if (edit.HasFullHistoryTsLow()) {
      const std::string& new_ts = edit.GetFullHistoryTsLow();
      cfd->SetFullHistoryTsLow(new_ts);
    }
  }

  if (s.ok()) {
    if (edit.has_prev_log_number_) {
      version_edit_params_.SetPrevLogNumber(edit.prev_log_number_);
    }
    if (edit.has_next_file_number_) {
      version_edit_params_.SetNextFile(edit.next_file_number_);
    }
    if (edit.has_max_column_family_) {
      version_edit_params_.SetMaxColumnFamily(edit.max_column_family_);
    }
    if (edit.has_min_log_number_to_keep_) {
      version_edit_params_.min_log_number_to_keep_ =
          std::max(version_edit_params_.min_log_number_to_keep_,
                   edit.min_log_number_to_keep_);
    }
    if (edit.has_last_sequence_) {
      // `VersionEdit::last_sequence_`s are assumed to be non-decreasing. This
      // is legacy behavior that cannot change without breaking downgrade
      // compatibility.
      assert(!version_edit_params_.has_last_sequence_ ||
             version_edit_params_.last_sequence_ <= edit.last_sequence_);
      version_edit_params_.SetLastSequence(edit.last_sequence_);
    }
    if (!version_edit_params_.has_prev_log_number_) {
      version_edit_params_.SetPrevLogNumber(0);
    }
  }
  return s;
}

VersionEditHandlerPointInTime::VersionEditHandlerPointInTime(
    bool read_only, std::vector<ColumnFamilyDescriptor> column_families,
    VersionSet* version_set, const std::shared_ptr<IOTracer>& io_tracer,
    EpochNumberRequirement epoch_number_requirement)
    : VersionEditHandler(read_only, column_families, version_set,
                         /*track_missing_files=*/true,
                         /*no_error_if_files_missing=*/true, io_tracer,
                         epoch_number_requirement) {}

VersionEditHandlerPointInTime::~VersionEditHandlerPointInTime() {
  for (const auto& elem : versions_) {
    delete elem.second;
  }
  versions_.clear();
}

void VersionEditHandlerPointInTime::CheckIterationResult(
    const log::Reader& reader, Status* s) {
  VersionEditHandler::CheckIterationResult(reader, s);
  assert(s != nullptr);
  if (s->ok()) {
    for (auto* cfd : *(version_set_->column_family_set_)) {
      if (cfd->IsDropped()) {
        continue;
      }
      assert(cfd->initialized());
      auto v_iter = versions_.find(cfd->GetID());
      if (v_iter != versions_.end()) {
        assert(v_iter->second != nullptr);

        version_set_->AppendVersion(cfd, v_iter->second);
        versions_.erase(v_iter);
      }
    }
  } else {
    for (const auto& elem : versions_) {
      delete elem.second;
    }
    versions_.clear();
  }
}

ColumnFamilyData* VersionEditHandlerPointInTime::DestroyCfAndCleanup(
    const VersionEdit& edit) {
  ColumnFamilyData* cfd = VersionEditHandler::DestroyCfAndCleanup(edit);
  auto v_iter = versions_.find(edit.column_family_);
  if (v_iter != versions_.end()) {
    delete v_iter->second;
    versions_.erase(v_iter);
  }
  return cfd;
}

Status VersionEditHandlerPointInTime::MaybeCreateVersion(
    const VersionEdit& edit, ColumnFamilyData* cfd, bool force_create_version) {
  assert(cfd != nullptr);
  if (!force_create_version) {
    assert(edit.column_family_ == cfd->GetID());
  }
  auto missing_files_iter = cf_to_missing_files_.find(cfd->GetID());
  assert(missing_files_iter != cf_to_missing_files_.end());
  std::unordered_set<uint64_t>& missing_files = missing_files_iter->second;

  auto missing_blob_files_high_iter =
      cf_to_missing_blob_files_high_.find(cfd->GetID());
  assert(missing_blob_files_high_iter != cf_to_missing_blob_files_high_.end());
  const uint64_t prev_missing_blob_file_high =
      missing_blob_files_high_iter->second;

  VersionBuilder* builder = nullptr;

  if (prev_missing_blob_file_high != kInvalidBlobFileNumber) {
    auto builder_iter = builders_.find(cfd->GetID());
    assert(builder_iter != builders_.end());
    builder = builder_iter->second->version_builder();
    assert(builder != nullptr);
  }

  // At this point, we have not yet applied the new version edits read from the
  // MANIFEST. We check whether we have any missing table and blob files.
  const bool prev_has_missing_files =
      !missing_files.empty() ||
      (prev_missing_blob_file_high != kInvalidBlobFileNumber &&
       prev_missing_blob_file_high >= builder->GetMinOldestBlobFileNumber());

  for (const auto& file : edit.GetDeletedFiles()) {
    uint64_t file_num = file.second;
    auto fiter = missing_files.find(file_num);
    if (fiter != missing_files.end()) {
      missing_files.erase(fiter);
    }
  }

  assert(!cfd->ioptions()->cf_paths.empty());
  Status s;
  for (const auto& elem : edit.GetNewFiles()) {
    int level = elem.first;
    const FileMetaData& meta = elem.second;
    const FileDescriptor& fd = meta.fd;
    uint64_t file_num = fd.GetNumber();
    const std::string fpath =
        MakeTableFileName(cfd->ioptions()->cf_paths[0].path, file_num);
    s = VerifyFile(cfd, fpath, level, meta);
    if (s.IsPathNotFound() || s.IsNotFound() || s.IsCorruption()) {
      missing_files.insert(file_num);
      s = Status::OK();
    } else if (!s.ok()) {
      break;
    }
  }

  uint64_t missing_blob_file_num = prev_missing_blob_file_high;
  for (const auto& elem : edit.GetBlobFileAdditions()) {
    uint64_t file_num = elem.GetBlobFileNumber();
    s = VerifyBlobFile(cfd, file_num, elem);
    if (s.IsPathNotFound() || s.IsNotFound() || s.IsCorruption()) {
      missing_blob_file_num = std::max(missing_blob_file_num, file_num);
      s = Status::OK();
    } else if (!s.ok()) {
      break;
    }
  }

  bool has_missing_blob_files = false;
  if (missing_blob_file_num != kInvalidBlobFileNumber &&
      missing_blob_file_num >= prev_missing_blob_file_high) {
    missing_blob_files_high_iter->second = missing_blob_file_num;
    has_missing_blob_files = true;
  } else if (missing_blob_file_num < prev_missing_blob_file_high) {
    assert(false);
  }

  // We still have not applied the new version edit, but have tried to add new
  // table and blob files after verifying their presence and consistency.
  // Therefore, we know whether we will see new missing table and blob files
  // later after actually applying the version edit. We perform the check here
  // and record the result.
  const bool has_missing_files =
      !missing_files.empty() || has_missing_blob_files;

  bool missing_info = !version_edit_params_.has_log_number_ ||
                      !version_edit_params_.has_next_file_number_ ||
                      !version_edit_params_.has_last_sequence_;

  // Create version before apply edit. The version will represent the state
  // before applying the version edit.
  // A new version will created if:
  // 1) no error has occurred so far, and
  // 2) log_number_, next_file_number_ and last_sequence_ are known, and
  // 3) any of the following:
  //   a) no missing file before, but will have missing file(s) after applying
  //      this version edit.
  //   b) no missing file after applying the version edit, and the caller
  //      explicitly request that a new version be created.
  if (s.ok() && !missing_info &&
      ((has_missing_files && !prev_has_missing_files) ||
       (!has_missing_files && force_create_version))) {
    if (!builder) {
      auto builder_iter = builders_.find(cfd->GetID());
      assert(builder_iter != builders_.end());
      builder = builder_iter->second->version_builder();
      assert(builder);
    }

    auto* version = new Version(cfd, version_set_, version_set_->file_options_,
                                *cfd->GetLatestMutableCFOptions(), io_tracer_,
                                version_set_->current_version_number_++,
                                epoch_number_requirement_);
    s = builder->LoadTableHandlers(
        cfd->internal_stats(),
        version_set_->db_options_->max_file_opening_threads, false, true,
        cfd->GetLatestMutableCFOptions()->prefix_extractor,
        MaxFileSizeForL0MetaPin(*cfd->GetLatestMutableCFOptions()));
    if (!s.ok()) {
      delete version;
      if (s.IsCorruption()) {
        s = Status::OK();
      }
      return s;
    }
    s = builder->SaveTo(version->storage_info());
    if (s.ok()) {
      version->PrepareAppend(
          *cfd->GetLatestMutableCFOptions(),
          !version_set_->db_options_->skip_stats_update_on_db_open);
      auto v_iter = versions_.find(cfd->GetID());
      if (v_iter != versions_.end()) {
        delete v_iter->second;
        v_iter->second = version;
      } else {
        versions_.emplace(cfd->GetID(), version);
      }
    } else {
      delete version;
    }
  }
  return s;
}

Status VersionEditHandlerPointInTime::VerifyFile(ColumnFamilyData* cfd,
                                                 const std::string& fpath,
                                                 int level,
                                                 const FileMetaData& fmeta) {
  return version_set_->VerifyFileMetadata(cfd, fpath, level, fmeta);
}

Status VersionEditHandlerPointInTime::VerifyBlobFile(
    ColumnFamilyData* cfd, uint64_t blob_file_num,
    const BlobFileAddition& blob_addition) {
  BlobSource* blob_source = cfd->blob_source();
  assert(blob_source);
  CacheHandleGuard<BlobFileReader> blob_file_reader;
  Status s = blob_source->GetBlobFileReader(blob_file_num, &blob_file_reader);
  if (!s.ok()) {
    return s;
  }
  // TODO: verify checksum
  (void)blob_addition;
  return s;
}

Status VersionEditHandlerPointInTime::LoadTables(
    ColumnFamilyData* /*cfd*/, bool /*prefetch_index_and_filter_in_cache*/,
    bool /*is_initial_load*/) {
  return Status::OK();
}

Status ManifestTailer::Initialize() {
  if (Mode::kRecovery == mode_) {
    return VersionEditHandler::Initialize();
  }
  assert(Mode::kCatchUp == mode_);
  Status s;
  if (!initialized_) {
    ColumnFamilySet* cfd_set = version_set_->GetColumnFamilySet();
    assert(cfd_set);
    ColumnFamilyData* default_cfd = cfd_set->GetDefault();
    assert(default_cfd);
    auto builder_iter = builders_.find(default_cfd->GetID());
    assert(builder_iter != builders_.end());

    Version* dummy_version = default_cfd->dummy_versions();
    assert(dummy_version);
    Version* base_version = dummy_version->Next();
    assert(base_version);
    base_version->Ref();
    VersionBuilderUPtr new_builder(
        new BaseReferencedVersionBuilder(default_cfd, base_version));
    builder_iter->second = std::move(new_builder);

    initialized_ = true;
  }
  return s;
}

Status ManifestTailer::ApplyVersionEdit(VersionEdit& edit,
                                        ColumnFamilyData** cfd) {
  Status s = VersionEditHandler::ApplyVersionEdit(edit, cfd);
  if (s.ok()) {
    assert(cfd);
    if (*cfd) {
      cfds_changed_.insert(*cfd);
    }
  }
  return s;
}

Status ManifestTailer::OnColumnFamilyAdd(VersionEdit& edit,
                                         ColumnFamilyData** cfd) {
  if (Mode::kRecovery == mode_) {
    return VersionEditHandler::OnColumnFamilyAdd(edit, cfd);
  }
  assert(Mode::kCatchUp == mode_);
  ColumnFamilySet* cfd_set = version_set_->GetColumnFamilySet();
  assert(cfd_set);
  ColumnFamilyData* tmp_cfd = cfd_set->GetColumnFamily(edit.GetColumnFamily());
  assert(cfd);
  *cfd = tmp_cfd;
  if (!tmp_cfd) {
    // For now, ignore new column families created after Recover() succeeds.
    return Status::OK();
  }
  auto builder_iter = builders_.find(edit.GetColumnFamily());
  assert(builder_iter != builders_.end());

  Version* dummy_version = tmp_cfd->dummy_versions();
  assert(dummy_version);
  Version* base_version = dummy_version->Next();
  assert(base_version);
  base_version->Ref();
  VersionBuilderUPtr new_builder(
      new BaseReferencedVersionBuilder(tmp_cfd, base_version));
  builder_iter->second = std::move(new_builder);

#ifndef NDEBUG
  auto version_iter = versions_.find(edit.GetColumnFamily());
  assert(version_iter == versions_.end());
#endif  // !NDEBUG
  return Status::OK();
}

void ManifestTailer::CheckIterationResult(const log::Reader& reader,
                                          Status* s) {
  VersionEditHandlerPointInTime::CheckIterationResult(reader, s);
  assert(s);
  if (s->ok()) {
    if (Mode::kRecovery == mode_) {
      mode_ = Mode::kCatchUp;
    } else {
      assert(Mode::kCatchUp == mode_);
    }
  }
}

Status ManifestTailer::VerifyFile(ColumnFamilyData* cfd,
                                  const std::string& fpath, int level,
                                  const FileMetaData& fmeta) {
  Status s =
      VersionEditHandlerPointInTime::VerifyFile(cfd, fpath, level, fmeta);
  // TODO: Open file or create hard link to prevent the file from being
  // deleted.
  return s;
}

void DumpManifestHandler::CheckIterationResult(const log::Reader& reader,
                                               Status* s) {
  VersionEditHandler::CheckIterationResult(reader, s);
  if (!s->ok()) {
    fprintf(stdout, "%s\n", s->ToString().c_str());
    return;
  }
  assert(cf_to_cmp_names_);
  for (auto* cfd : *(version_set_->column_family_set_)) {
    fprintf(stdout,
            "--------------- Column family \"%s\"  (ID %" PRIu32
            ") --------------\n",
            cfd->GetName().c_str(), cfd->GetID());
    fprintf(stdout, "log number: %" PRIu64 "\n", cfd->GetLogNumber());
    auto it = cf_to_cmp_names_->find(cfd->GetID());
    if (it != cf_to_cmp_names_->end()) {
      fprintf(stdout,
              "comparator: <%s>, but the comparator object is not available.\n",
              it->second.c_str());
    } else {
      fprintf(stdout, "comparator: %s\n", cfd->user_comparator()->Name());
    }
    assert(cfd->current());

    // Print out DebugStrings. Can include non-terminating null characters.
    fwrite(cfd->current()->DebugString(hex_).data(), sizeof(char),
           cfd->current()->DebugString(hex_).size(), stdout);
  }
  fprintf(stdout,
          "next_file_number %" PRIu64 " last_sequence %" PRIu64
          "  prev_log_number %" PRIu64 " max_column_family %" PRIu32
          " min_log_number_to_keep %" PRIu64 "\n",
          version_set_->current_next_file_number(),
          version_set_->LastSequence(), version_set_->prev_log_number(),
          version_set_->column_family_set_->GetMaxColumnFamily(),
          version_set_->min_log_number_to_keep());
}

}  // namespace ROCKSDB_NAMESPACE
