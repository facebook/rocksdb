//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit_handler.h"

#include "monitoring/persistent_stats_history.h"

namespace ROCKSDB_NAMESPACE {

VersionEditHandler::VersionEditHandler(
    bool read_only, const std::vector<ColumnFamilyDescriptor>& column_families,
    VersionSet* version_set, bool track_missing_files,
    bool no_error_if_table_files_missing)
    : read_only_(read_only),
      column_families_(column_families),
      status_(),
      version_set_(version_set),
      track_missing_files_(track_missing_files),
      no_error_if_table_files_missing_(no_error_if_table_files_missing),
      initialized_(false) {
  assert(version_set_ != nullptr);
}

Status VersionEditHandler::Iterate(log::Reader& reader, std::string* db_id) {
  Slice record;
  std::string scratch;
  size_t recovered_edits = 0;
  Status s = Initialize();
  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      break;
    }
    if (edit.has_db_id_) {
      version_set_->db_id_ = edit.GetDbId();
      if (db_id != nullptr) {
        *db_id = version_set_->db_id_;
      }
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

  CheckIterationResult(reader, &s);

  if (!s.ok()) {
    status_ = s;
  }
  return s;
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
    read_buffer_.Clear();
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
  if (s->ok() && !read_only_ && !column_families_not_found_.empty()) {
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
    version_set_->MarkMinLogNumberToKeep2PC(
        version_edit_params_.min_log_number_to_keep_);
    version_set_->MarkFileNumberUsed(version_edit_params_.prev_log_number_);
    version_set_->MarkFileNumberUsed(version_edit_params_.log_number_);
    for (auto* cfd : *(version_set_->GetColumnFamilySet())) {
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
    version_set_->last_allocated_sequence_ =
        version_edit_params_.last_sequence_;
    version_set_->last_published_sequence_ =
        version_edit_params_.last_sequence_;
    version_set_->last_sequence_ = version_edit_params_.last_sequence_;
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
  }
  ColumnFamilyData* ret =
      version_set_->GetColumnFamilySet()->GetColumnFamily(edit.column_family_);
  assert(ret != nullptr);
  if (ret->UnrefAndTryDelete()) {
    ret = nullptr;
  } else {
    assert(false);
  }
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
                          *cfd->GetLatestMutableCFOptions(),
                          version_set_->current_version_number_++);
    s = builder->SaveTo(v->storage_info());
    if (s.ok()) {
      // Install new version
      v->PrepareApply(
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
  assert(cfd != nullptr);
  assert(!cfd->IsDropped());
  Status s;
  auto builder_iter = builders_.find(cfd->GetID());
  assert(builder_iter != builders_.end());
  assert(builder_iter->second != nullptr);
  VersionBuilder* builder = builder_iter->second->version_builder();
  assert(builder);
  s = builder->LoadTableHandlers(
      cfd->internal_stats(),
      version_set_->db_options_->max_file_opening_threads,
      prefetch_index_and_filter_in_cache, is_initial_load,
      cfd->GetLatestMutableCFOptions()->prefix_extractor.get());
  if ((s.IsPathNotFound() || s.IsCorruption()) &&
      no_error_if_table_files_missing_) {
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
  if (cfd != nullptr) {
    if (edit.has_db_id_) {
      version_edit_params_.SetDBId(edit.db_id_);
    }
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
      s = Status::InvalidArgument(
          cfd->user_comparator()->Name(),
          "does not match existing comparator " + edit.comparator_);
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
      version_edit_params_.SetLastSequence(edit.last_sequence_);
    }
    if (!version_edit_params_.has_prev_log_number_) {
      version_edit_params_.SetPrevLogNumber(0);
    }
  }
  return s;
}

VersionEditHandlerPointInTime::VersionEditHandlerPointInTime(
    bool read_only, const std::vector<ColumnFamilyDescriptor>& column_families,
    VersionSet* version_set)
    : VersionEditHandler(read_only, column_families, version_set,
                         /*track_missing_files=*/true,
                         /*no_error_if_table_files_missing=*/true) {}

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
  const bool prev_has_missing_files = !missing_files.empty();
  for (const auto& file : edit.GetDeletedFiles()) {
    uint64_t file_num = file.second;
    auto fiter = missing_files.find(file_num);
    if (fiter != missing_files.end()) {
      missing_files.erase(fiter);
    }
  }
  Status s;
  for (const auto& elem : edit.GetNewFiles()) {
    const FileMetaData& meta = elem.second;
    const FileDescriptor& fd = meta.fd;
    uint64_t file_num = fd.GetNumber();
    const std::string fpath =
        MakeTableFileName(cfd->ioptions()->cf_paths[0].path, file_num);
    s = version_set_->VerifyFileMetadata(fpath, meta);
    if (s.IsPathNotFound() || s.IsNotFound() || s.IsCorruption()) {
      missing_files.insert(file_num);
      s = Status::OK();
    }
  }
  bool missing_info = !version_edit_params_.has_log_number_ ||
                      !version_edit_params_.has_next_file_number_ ||
                      !version_edit_params_.has_last_sequence_;

  // Create version before apply edit
  if (!missing_info && ((!missing_files.empty() && !prev_has_missing_files) ||
                        (missing_files.empty() && force_create_version))) {
    auto builder_iter = builders_.find(cfd->GetID());
    assert(builder_iter != builders_.end());
    auto* builder = builder_iter->second->version_builder();
    auto* version = new Version(cfd, version_set_, version_set_->file_options_,
                                *cfd->GetLatestMutableCFOptions(),
                                version_set_->current_version_number_++);
    s = builder->SaveTo(version->storage_info());
    if (s.ok()) {
      version->PrepareApply(
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

}  // namespace ROCKSDB_NAMESPACE
