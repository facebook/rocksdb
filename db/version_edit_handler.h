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
  explicit VersionEditHandlerBase()
      : max_manifest_read_size_(std::numeric_limits<uint64_t>::max()) {}

  virtual ~VersionEditHandlerBase() {}

  void Iterate(log::Reader& reader, Status* log_read_status);

  const Status& status() const { return status_; }

  AtomicGroupReadBuffer& GetReadBuffer() { return read_buffer_; }

 protected:
  explicit VersionEditHandlerBase(uint64_t max_read_size)
      : max_manifest_read_size_(max_read_size) {}
  virtual Status Initialize() { return Status::OK(); }

  virtual Status ApplyVersionEdit(VersionEdit& edit,
                                  ColumnFamilyData** cfd) = 0;

  virtual void CheckIterationResult(const log::Reader& /*reader*/,
                                    Status* /*s*/) {}

  void ClearReadBuffer() { read_buffer_.Clear(); }

  Status status_;

 private:
  AtomicGroupReadBuffer read_buffer_;
  const uint64_t max_manifest_read_size_;
};

class ListColumnFamiliesHandler : public VersionEditHandlerBase {
 public:
  ListColumnFamiliesHandler() : VersionEditHandlerBase() {}

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
  FileChecksumRetriever(uint64_t max_read_size,
                        FileChecksumList& file_checksum_list)
      : VersionEditHandlerBase(max_read_size),
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
// the versions of column families.
// To use this class and its subclasses,
// 1. Create an object of VersionEditHandler or its subclasses.
//    VersionEditHandler handler(read_only, column_families, version_set,
//                               track_missing_files,
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
      VersionSet* version_set, bool track_missing_files,
      bool no_error_if_files_missing,
      const std::shared_ptr<IOTracer>& io_tracer)
      : VersionEditHandler(read_only, column_families, version_set,
                           track_missing_files, no_error_if_files_missing,
                           io_tracer, /*skip_load_table_files=*/false) {}

  ~VersionEditHandler() override {}

  const VersionEditParams& GetVersionEditParams() const {
    return version_edit_params_;
  }

  bool HasMissingFiles() const;

  void GetDbId(std::string* db_id) const {
    if (db_id && version_edit_params_.has_db_id_) {
      *db_id = version_edit_params_.db_id_;
    }
  }

 protected:
  explicit VersionEditHandler(
      bool read_only, std::vector<ColumnFamilyDescriptor> column_families,
      VersionSet* version_set, bool track_missing_files,
      bool no_error_if_files_missing,
      const std::shared_ptr<IOTracer>& io_tracer, bool skip_load_table_files);

  Status ApplyVersionEdit(VersionEdit& edit, ColumnFamilyData** cfd) override;

  virtual Status OnColumnFamilyAdd(VersionEdit& edit, ColumnFamilyData** cfd);

  Status OnColumnFamilyDrop(VersionEdit& edit, ColumnFamilyData** cfd);

  Status OnNonCfOperation(VersionEdit& edit, ColumnFamilyData** cfd);

  Status OnWalAddition(VersionEdit& edit);

  Status OnWalDeletion(VersionEdit& edit);

  Status Initialize() override;

  void CheckColumnFamilyId(const VersionEdit& edit, bool* cf_in_not_found,
                           bool* cf_in_builders) const;

  void CheckIterationResult(const log::Reader& reader, Status* s) override;

  ColumnFamilyData* CreateCfAndInit(const ColumnFamilyOptions& cf_options,
                                    const VersionEdit& edit);

  virtual ColumnFamilyData* DestroyCfAndCleanup(const VersionEdit& edit);

  virtual Status MaybeCreateVersion(const VersionEdit& edit,
                                    ColumnFamilyData* cfd,
                                    bool force_create_version);

  Status LoadTables(ColumnFamilyData* cfd,
                    bool prefetch_index_and_filter_in_cache,
                    bool is_initial_load);

  virtual bool MustOpenAllColumnFamilies() const { return !read_only_; }

  const bool read_only_;
  std::vector<ColumnFamilyDescriptor> column_families_;
  VersionSet* version_set_;
  std::unordered_map<uint32_t, VersionBuilderUPtr> builders_;
  std::unordered_map<std::string, ColumnFamilyOptions> name_to_options_;
  // Keeps track of column families in manifest that were not found in
  // column families parameters. if those column families are not dropped
  // by subsequent manifest records, Recover() will return failure status.
  std::unordered_map<uint32_t, std::string> column_families_not_found_;
  VersionEditParams version_edit_params_;
  const bool track_missing_files_;
  std::unordered_map<uint32_t, std::unordered_set<uint64_t>>
      cf_to_missing_files_;
  std::unordered_map<uint32_t, uint64_t> cf_to_missing_blob_files_high_;
  bool no_error_if_files_missing_;
  std::shared_ptr<IOTracer> io_tracer_;
  bool skip_load_table_files_;
  bool initialized_;
  std::unique_ptr<std::unordered_map<uint32_t, std::string>> cf_to_cmp_names_;

 private:
  Status ExtractInfoFromVersionEdit(ColumnFamilyData* cfd,
                                    const VersionEdit& edit);
};

// A class similar to its base class, i.e. VersionEditHandler.
// VersionEditHandlerPointInTime restores the versions to the most recent point
// in time such that at this point, the version does not have missing files.
//
// Not thread-safe, external synchronization is necessary if an object of
// VersionEditHandlerPointInTime is shared by multiple threads.
class VersionEditHandlerPointInTime : public VersionEditHandler {
 public:
  VersionEditHandlerPointInTime(
      bool read_only, std::vector<ColumnFamilyDescriptor> column_families,
      VersionSet* version_set, const std::shared_ptr<IOTracer>& io_tracer);
  ~VersionEditHandlerPointInTime() override;

 protected:
  void CheckIterationResult(const log::Reader& reader, Status* s) override;
  ColumnFamilyData* DestroyCfAndCleanup(const VersionEdit& edit) override;
  Status MaybeCreateVersion(const VersionEdit& edit, ColumnFamilyData* cfd,
                            bool force_create_version) override;
  virtual Status VerifyFile(const std::string& fpath,
                            const FileMetaData& fmeta);
  virtual Status VerifyBlobFile(ColumnFamilyData* cfd, uint64_t blob_file_num,
                                const BlobFileAddition& blob_addition);

  std::unordered_map<uint32_t, Version*> versions_;
};

class ManifestTailer : public VersionEditHandlerPointInTime {
 public:
  explicit ManifestTailer(std::vector<ColumnFamilyDescriptor> column_families,
                          VersionSet* version_set,
                          const std::shared_ptr<IOTracer>& io_tracer)
      : VersionEditHandlerPointInTime(/*read_only=*/false, column_families,
                                      version_set, io_tracer),
        mode_(Mode::kRecovery) {}

  void PrepareToReadNewManifest() {
    initialized_ = false;
    ClearReadBuffer();
  }

  std::unordered_set<ColumnFamilyData*>& GetUpdatedColumnFamilies() {
    return cfds_changed_;
  }

 protected:
  Status Initialize() override;

  bool MustOpenAllColumnFamilies() const override { return false; }

  Status ApplyVersionEdit(VersionEdit& edit, ColumnFamilyData** cfd) override;

  Status OnColumnFamilyAdd(VersionEdit& edit, ColumnFamilyData** cfd) override;

  void CheckIterationResult(const log::Reader& reader, Status* s) override;

  Status VerifyFile(const std::string& fpath,
                    const FileMetaData& fmeta) override;

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
                      const std::shared_ptr<IOTracer>& io_tracer, bool verbose,
                      bool hex, bool json)
      : VersionEditHandler(
            /*read_only=*/true, column_families, version_set,
            /*track_missing_files=*/false,
            /*no_error_if_files_missing=*/false, io_tracer,
            /*skip_load_table_files=*/true),
        verbose_(verbose),
        hex_(hex),
        json_(json),
        count_(0) {
    cf_to_cmp_names_.reset(new std::unordered_map<uint32_t, std::string>());
  }

  ~DumpManifestHandler() override {}

  Status ApplyVersionEdit(VersionEdit& edit, ColumnFamilyData** cfd) override {
    // Write out each individual edit
    if (verbose_ && !json_) {
      // Print out DebugStrings. Can include non-terminating null characters.
      fwrite(edit.DebugString(hex_).data(), sizeof(char),
             edit.DebugString(hex_).size(), stdout);
    } else if (json_) {
      // Print out DebugStrings. Can include non-terminating null characters.
      fwrite(edit.DebugString(hex_).data(), sizeof(char),
             edit.DebugString(hex_).size(), stdout);
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
