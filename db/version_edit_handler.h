//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

struct FileMetaData;

class VersionEditHandlerBase {
 public:
  explicit VersionEditHandlerBase(const ReadOptions& read_options)
      : read_options_(read_options),
        max_manifest_read_size_(std::numeric_limits<uint64_t>::max()) {}

  virtual ~VersionEditHandlerBase() {}

  void Iterate(log::Reader& reader, Status* log_read_status);

  const Status& status() const { return status_; }

  AtomicGroupReadBuffer& GetReadBuffer() { return read_buffer_; }

 protected:
  explicit VersionEditHandlerBase(const ReadOptions& read_options,
                                  uint64_t max_read_size)
      : read_options_(read_options), max_manifest_read_size_(max_read_size) {}
  virtual Status Initialize() { return Status::OK(); }

  virtual Status ApplyVersionEdit(VersionEdit& edit,
                                  ColumnFamilyData** cfd) = 0;

  virtual Status OnAtomicGroupReplayBegin() { return Status::OK(); }
  virtual Status OnAtomicGroupReplayEnd() { return Status::OK(); }

  virtual void CheckIterationResult(const log::Reader& /*reader*/,
                                    Status* /*s*/) {}

  void ClearReadBuffer() { read_buffer_.Clear(); }

  Status status_;

  const ReadOptions& read_options_;

 private:
  AtomicGroupReadBuffer read_buffer_;
  const uint64_t max_manifest_read_size_;
};

class ListColumnFamiliesHandler : public VersionEditHandlerBase {
 public:
  explicit ListColumnFamiliesHandler(const ReadOptions& read_options)
      : VersionEditHandlerBase(read_options) {}

  ~ListColumnFamiliesHandler() override {}

  const std::map<uint32_t, std::string> GetColumnFamilyNames() const {
    return column_family_names_;
  }

 protected:
  Status ApplyVersionEdit(VersionEdit& edit,
                          ColumnFamilyData** /*unused*/) override;

 private:
  // default column family is always implicitly there
  std::map<uint32_t, std::string> column_family_names_{
      {0, kDefaultColumnFamilyName}};
};

class FileChecksumRetriever : public VersionEditHandlerBase {
 public:
  FileChecksumRetriever(const ReadOptions& read_options, uint64_t max_read_size,
                        FileChecksumList& file_checksum_list)
      : VersionEditHandlerBase(read_options, max_read_size),
        file_checksum_list_(file_checksum_list) {}

  ~FileChecksumRetriever() override {}

 protected:
  Status ApplyVersionEdit(VersionEdit& edit,
                          ColumnFamilyData** /*unused*/) override;

 private:
  FileChecksumList& file_checksum_list_;
};

using VersionBuilderUPtr = std::unique_ptr<BaseReferencedVersionBuilder>;

// A class used for scanning MANIFEST file.
// VersionEditHandler reads a MANIFEST file, parses the version edits, and
// builds the version set's in-memory state, e.g. the version storage info for
// the versions of column families. It replays all the version edits in one
// MANIFEST file to build the end version.
//
// To use this class and its subclasses,
// 1. Create an object of VersionEditHandler or its subclasses.
//    VersionEditHandler handler(read_only, column_families, version_set,
//                               track_found_and_missing_files,
//                               no_error_if_files_missing);
// 2. Status s = handler.Iterate(reader, &db_id);
// 3. Check s and handle possible errors.
//
// Not thread-safe, external synchronization is necessary if an object of
// VersionEditHandler is shared by multiple threads.
class VersionEditHandler : public VersionEditHandlerBase {
 public:
  explicit VersionEditHandler(
      bool read_only,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      VersionSet* version_set, bool track_found_and_missing_files,
      bool no_error_if_files_missing,
      const std::shared_ptr<IOTracer>& io_tracer,
      const ReadOptions& read_options, bool allow_incomplete_valid_version,
      EpochNumberRequirement epoch_number_requirement =
          EpochNumberRequirement::kMustPresent)
      : VersionEditHandler(read_only, column_families, version_set,
                           track_found_and_missing_files,
                           no_error_if_files_missing, io_tracer, read_options,
                           /*skip_load_table_files=*/false,
                           allow_incomplete_valid_version,
                           epoch_number_requirement) {}

  ~VersionEditHandler() override {}

  const VersionEditParams& GetVersionEditParams() const {
    return version_edit_params_;
  }

  void GetDbId(std::string* db_id) const {
    if (db_id && version_edit_params_.HasDbId()) {
      *db_id = version_edit_params_.GetDbId();
    }
  }

  virtual Status VerifyFile(ColumnFamilyData* /*cfd*/,
                            const std::string& /*fpath*/, int /*level*/,
                            const FileMetaData& /*fmeta*/) {
    return Status::OK();
  }

  virtual Status VerifyBlobFile(ColumnFamilyData* /*cfd*/,
                                uint64_t /*blob_file_num*/,
                                const BlobFileAddition& /*blob_addition*/) {
    return Status::OK();
  }

 protected:
  explicit VersionEditHandler(
      bool read_only, std::vector<ColumnFamilyDescriptor> column_families,
      VersionSet* version_set, bool track_found_and_missing_files,
      bool no_error_if_files_missing,
      const std::shared_ptr<IOTracer>& io_tracer,
      const ReadOptions& read_options, bool skip_load_table_files,
      bool allow_incomplete_valid_version,
      EpochNumberRequirement epoch_number_requirement =
          EpochNumberRequirement::kMustPresent);

  Status ApplyVersionEdit(VersionEdit& edit, ColumnFamilyData** cfd) override;

  virtual Status OnColumnFamilyAdd(VersionEdit& edit, ColumnFamilyData** cfd);

  Status OnColumnFamilyDrop(VersionEdit& edit, ColumnFamilyData** cfd);

  Status OnNonCfOperation(VersionEdit& edit, ColumnFamilyData** cfd);

  Status OnWalAddition(VersionEdit& edit);

  Status OnWalDeletion(VersionEdit& edit);

  Status Initialize() override;

  void CheckColumnFamilyId(const VersionEdit& edit, bool* do_not_open_cf,
                           bool* cf_in_builders) const;

  void CheckIterationResult(const log::Reader& reader, Status* s) override;

  ColumnFamilyData* CreateCfAndInit(const ColumnFamilyOptions& cf_options,
                                    const VersionEdit& edit);

  virtual ColumnFamilyData* DestroyCfAndCleanup(const VersionEdit& edit);

  virtual Status MaybeCreateVersionBeforeApplyEdit(const VersionEdit& edit,
                                                   ColumnFamilyData* cfd,
                                                   bool force_create_version);

  virtual Status LoadTables(ColumnFamilyData* cfd,
                            bool prefetch_index_and_filter_in_cache,
                            bool is_initial_load);

  virtual bool MustOpenAllColumnFamilies() const {
    return !version_set_->unchanging();
  }

  const bool read_only_;
  std::vector<ColumnFamilyDescriptor> column_families_;
  VersionSet* version_set_;
  std::unordered_map<uint32_t, VersionBuilderUPtr> builders_;
  std::unordered_map<std::string, ColumnFamilyOptions> name_to_options_;
  const bool track_found_and_missing_files_;
  // Keeps track of column families in manifest that were not found in
  // column families parameters. Namely, the user asks to not open these column
  // families. In non read only mode, if those column families are not dropped
  // by subsequent manifest records, Recover() will return failure status.
  std::unordered_map<uint32_t, std::string> do_not_open_column_families_;
  VersionEditParams version_edit_params_;
  bool no_error_if_files_missing_;
  std::shared_ptr<IOTracer> io_tracer_;
  bool skip_load_table_files_;
  bool initialized_;
  std::unique_ptr<std::unordered_map<uint32_t, std::string>> cf_to_cmp_names_;
  // If false, only a complete Version for which all files consisting it can be
  // found is considered a valid Version. If true, besides complete Version, an
  // incomplete Version with only a suffix of L0 files missing is also
  // considered valid if the Version is never edited in an atomic group.
  const bool allow_incomplete_valid_version_;
  EpochNumberRequirement epoch_number_requirement_;
  std::unordered_set<uint32_t> cfds_to_mark_no_udt_;

 private:
  Status ExtractInfoFromVersionEdit(ColumnFamilyData* cfd,
                                    const VersionEdit& edit);

  // When `FileMetaData.user_defined_timestamps_persisted` is false and
  // user-defined timestamp size is non-zero. User-defined timestamps are
  // stripped from file boundaries: `smallest`, `largest` in
  // `VersionEdit.DecodeFrom` before they were written to Manifest.
  // This is the mirroring change to handle file boundaries on the Manifest read
  // path for this scenario: to pad a minimum timestamp to the user key in
  // `smallest` and `largest` so their format are consistent with the running
  // user comparator.
  Status MaybeHandleFileBoundariesForNewFiles(VersionEdit& edit,
                                              const ColumnFamilyData* cfd);
};

// A class similar to its base class, i.e. VersionEditHandler.
// Unlike VersionEditHandler that only aims to build the end version, this class
// supports building the most recent point in time version. A point in time
// version is a version for which no files are missing, or if
// `allow_incomplete_valid_version` is true, only a suffix of L0 files (and
// their associated blob files) are missing.
//
// Building a point in time version when end version is not available can
// be useful for best efforts recovery (options.best_efforts_recovery), which
// uses this class and sets `allow_incomplete_valid_version` to true.
// It's also useful for secondary instances/follower instances for which end
// version could be transiently unavailable. These two cases use subclass
// `ManifestTailer` and sets `allow_incomplete_valid_version` to false.
//
// Not thread-safe, external synchronization is necessary if an object of
// VersionEditHandlerPointInTime is shared by multiple threads.
class VersionEditHandlerPointInTime : public VersionEditHandler {
 public:
  VersionEditHandlerPointInTime(
      bool read_only, std::vector<ColumnFamilyDescriptor> column_families,
      VersionSet* version_set, const std::shared_ptr<IOTracer>& io_tracer,
      const ReadOptions& read_options, bool allow_incomplete_valid_version,
      EpochNumberRequirement epoch_number_requirement =
          EpochNumberRequirement::kMustPresent);
  ~VersionEditHandlerPointInTime() override;

  bool HasMissingFiles() const;

  virtual Status VerifyFile(ColumnFamilyData* cfd, const std::string& fpath,
                            int level, const FileMetaData& fmeta) override;
  virtual Status VerifyBlobFile(ColumnFamilyData* cfd, uint64_t blob_file_num,
                                const BlobFileAddition& blob_addition) override;

 protected:
  Status OnAtomicGroupReplayBegin() override;
  Status OnAtomicGroupReplayEnd() override;
  void CheckIterationResult(const log::Reader& reader, Status* s) override;

  ColumnFamilyData* DestroyCfAndCleanup(const VersionEdit& edit) override;
  // `MaybeCreateVersionBeforeApplyEdit(..., false)` creates a version upon a
  // negative edge trigger (transition from valid to invalid).
  //
  // `MaybeCreateVersionBeforeApplyEdit(..., true)` creates a version on a
  // positive level trigger (state is valid).
  Status MaybeCreateVersionBeforeApplyEdit(const VersionEdit& edit,
                                           ColumnFamilyData* cfd,
                                           bool force_create_version) override;

  Status LoadTables(ColumnFamilyData* cfd,
                    bool prefetch_index_and_filter_in_cache,
                    bool is_initial_load) override;

  std::unordered_map<uint32_t, Version*> versions_;

  // `atomic_update_versions_` is for ensuring all-or-nothing AtomicGroup
  // recoveries.  When `atomic_update_versions_` is nonempty, it serves as a
  // barrier to updating `versions_` until all its values are populated.
  std::unordered_map<uint32_t, Version*> atomic_update_versions_;
  // `atomic_update_versions_missing_` counts the nullptr values in
  // `atomic_update_versions_`.
  size_t atomic_update_versions_missing_;

  bool in_atomic_group_ = false;

 private:
  bool AtomicUpdateVersionsCompleted();
  bool AtomicUpdateVersionsContains(uint32_t cfid);
  void AtomicUpdateVersionsDropCf(uint32_t cfid);

  // This function is called for `Version*` updates for column families in an
  // incomplete atomic update. It buffers `Version*` updates in
  // `atomic_update_versions_`.
  void AtomicUpdateVersionsPut(Version* version);

  // This function is called upon completion of an atomic update. It applies
  // `Version*` updates in `atomic_update_versions_` to `versions_`.
  void AtomicUpdateVersionsApply();
};

// A class similar to `VersionEditHandlerPointInTime` that parse MANIFEST and
// builds point in time version.
// `ManifestTailer` supports reading one MANIFEST file in multiple tailing
// attempts and supports switching to a different MANIFEST after
// `PrepareToReadNewManifest` is called. This class is used by secondary and
// follower instance.
class ManifestTailer : public VersionEditHandlerPointInTime {
 public:
  explicit ManifestTailer(std::vector<ColumnFamilyDescriptor> column_families,
                          VersionSet* version_set,
                          const std::shared_ptr<IOTracer>& io_tracer,
                          const ReadOptions& read_options,
                          EpochNumberRequirement epoch_number_requirement =
                              EpochNumberRequirement::kMustPresent)
      : VersionEditHandlerPointInTime(
            /*read_only=*/true, column_families, version_set, io_tracer,
            read_options,
            /*allow_incomplete_valid_version=*/false, epoch_number_requirement),
        mode_(Mode::kRecovery) {}

  Status VerifyFile(ColumnFamilyData* cfd, const std::string& fpath, int level,
                    const FileMetaData& fmeta) override;

  void PrepareToReadNewManifest() {
    initialized_ = false;
    ClearReadBuffer();
  }

  std::unordered_set<ColumnFamilyData*>& GetUpdatedColumnFamilies() {
    return cfds_changed_;
  }

  std::vector<std::string> GetAndClearIntermediateFiles();

 protected:
  Status Initialize() override;

  bool MustOpenAllColumnFamilies() const override { return false; }

  Status ApplyVersionEdit(VersionEdit& edit, ColumnFamilyData** cfd) override;

  Status OnColumnFamilyAdd(VersionEdit& edit, ColumnFamilyData** cfd) override;

  void CheckIterationResult(const log::Reader& reader, Status* s) override;

  enum Mode : uint8_t {
    kRecovery = 0,
    kCatchUp = 1,
  };

  Mode mode_;
  std::unordered_set<ColumnFamilyData*> cfds_changed_;
};

class DumpManifestHandler : public VersionEditHandler {
 public:
  DumpManifestHandler(std::vector<ColumnFamilyDescriptor> column_families,
                      VersionSet* version_set,
                      const std::shared_ptr<IOTracer>& io_tracer,
                      const ReadOptions& read_options, bool verbose, bool hex,
                      bool json)
      : VersionEditHandler(
            /*read_only=*/true, column_families, version_set,
            /*track_found_and_missing_files=*/false,
            /*no_error_if_files_missing=*/false, io_tracer, read_options,
            /*skip_load_table_files=*/true,
            /*allow_incomplete_valid_version=*/false,
            /*epoch_number_requirement=*/EpochNumberRequirement::kMustPresent),
        verbose_(verbose),
        hex_(hex),
        json_(json),
        count_(0) {
    cf_to_cmp_names_.reset(new std::unordered_map<uint32_t, std::string>());
  }

  ~DumpManifestHandler() override {}

  Status ApplyVersionEdit(VersionEdit& edit, ColumnFamilyData** cfd) override {
    // Write out each individual edit
    if (json_) {
      // Print out DebugStrings. Can include non-terminating null characters.
      std::string edit_dump_str = edit.DebugJSON(count_, hex_);
      fwrite(edit_dump_str.data(), sizeof(char), edit_dump_str.size(), stdout);
      fwrite("\n", sizeof(char), 1, stdout);
    } else if (verbose_) {
      // Print out DebugStrings. Can include non-terminating null characters.
      std::string edit_dump_str = edit.DebugString(hex_);
      fwrite(edit_dump_str.data(), sizeof(char), edit_dump_str.size(), stdout);
    }
    ++count_;
    return VersionEditHandler::ApplyVersionEdit(edit, cfd);
  }

  void CheckIterationResult(const log::Reader& reader, Status* s) override;

 private:
  const bool verbose_;
  const bool hex_;
  const bool json_;
  int count_;
};

}  // namespace ROCKSDB_NAMESPACE
