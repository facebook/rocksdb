// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "cloud/manifest_reader.h"

#include <unordered_map>

#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "cloud/cloud_manifest.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "db/version_set.h"
#include "env/composite_env_wrapper.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

LocalManifestReader::LocalManifestReader(std::shared_ptr<Logger> info_log,
                                         CloudFileSystem* cfs)
    : info_log_(std::move(info_log)), cfs_(cfs) {}

IOStatus LocalManifestReader::GetLiveFilesLocally(
    const std::string& local_dbname, std::set<uint64_t>* list) const {
  auto* cfs_impl = dynamic_cast<CloudFileSystemImpl*>(cfs_);
  assert(cfs_impl);
  // cloud manifest should be set in CloudFileSystem, and it should map to local
  // CloudManifest
  assert(cfs_impl->GetCloudManifest());
  auto cloud_manifest = cfs_impl->GetCloudManifest();
  auto current_epoch = cloud_manifest->GetCurrentEpoch();

  std::unique_ptr<SequentialFileReader> manifest_file_reader;
  IOStatus s;
  {
    // file name here doesn't matter, it will always be mapped to the correct
    // Manifest file. use empty epoch here so that it will be recognized as
    // manifest file type
    auto local_manifest_file = cfs_impl->RemapFilename(
        ManifestFileWithEpoch(local_dbname, "" /* epoch */));

    std::unique_ptr<FSSequentialFile> file;
    s = cfs_impl->NewSequentialFile(local_manifest_file, FileOptions(), &file,
                                    nullptr /*dbg*/);
    if (!s.ok()) {
      return s;
    }
    manifest_file_reader.reset(
        new SequentialFileReader(std::move(file), local_manifest_file));
  }

  return GetLiveFilesFromFileReader(std::move(manifest_file_reader), list);
}

IOStatus LocalManifestReader::GetManifestLiveFiles(
    const std::string& manifest_file, std::set<uint64_t>* list) const {
  auto* cfs_impl = dynamic_cast<CloudFileSystemImpl*>(cfs_);
  assert(cfs_impl);

  std::unique_ptr<SequentialFileReader> manifest_file_reader;
  IOStatus s;
  {
    std::unique_ptr<FSSequentialFile> file;
    s = cfs_impl->NewSequentialFile(manifest_file, FileOptions(), &file,
                                    nullptr /*dbg*/);
    if (!s.ok()) {
      return s;
    }
    manifest_file_reader.reset(
        new SequentialFileReader(std::move(file), manifest_file));
  }

  return GetLiveFilesFromFileReader(std::move(manifest_file_reader), list);
}

IOStatus LocalManifestReader::GetLiveFilesFromFileReader(
    std::unique_ptr<SequentialFileReader> file_reader,
    std::set<uint64_t>* list) const {
  Status s;
  // create a callback that gets invoked whil looping through the log records
  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(nullptr, std::move(file_reader), &reporter,
                     true /*checksum*/, 0);

  Slice record;
  std::string scratch;

  // keep track of each CF's live files on each level
  std::unordered_map<uint32_t,                // CF id
                     std::unordered_map<int,  // level
                                        std::unordered_set<uint64_t>>>
      cf_live_files;

  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      break;
    }

    // add the files that are added by this transaction
    std::vector<std::pair<int, FileMetaData>> new_files = edit.GetNewFiles();
    for (auto& one : new_files) {
      uint64_t num = one.second.fd.GetNumber();
      cf_live_files[edit.GetColumnFamily()][one.first].insert(num);
    }
    // delete the files that are removed by this transaction
    std::set<std::pair<int, uint64_t>> deleted_files = edit.GetDeletedFiles();
    for (auto& one : deleted_files) {
      int level = one.first;
      uint64_t num = one.second;
      // Deleted files should belong to some CF
      auto it = cf_live_files.find(edit.GetColumnFamily());
      if ((it == cf_live_files.end()) || (it->second.count(level) == 0) ||
          (it->second[level].count(num) == 0)) {
        return IOStatus::Corruption(
            "Corrupted Manifest file with unrecognized deleted file: " +
            std::to_string(level) + "," + std::to_string(num));
      }
      it->second[level].erase(num);
    }

    // Removing the files from dropped CF, since we don't mark the files as
    // deleted in Manifest when a CF is dropped,
    if (edit.IsColumnFamilyDrop()) {
      cf_live_files.erase(edit.GetColumnFamily());
    }
  }

  for (auto& [cf_id, live_files] : cf_live_files) {
    for (auto& [level, level_live_files] : live_files) {
      (void)cf_id;
      (void)level;
      list->insert(level_live_files.begin(), level_live_files.end());
    }
  }

  file_reader.reset();
  return status_to_io_status(std::move(s));
}

ManifestReader::ManifestReader(std::shared_ptr<Logger> info_log,
                               CloudFileSystem* cfs,
                               const std::string& bucket_prefix)
    : LocalManifestReader(std::move(info_log), cfs),
      bucket_prefix_(bucket_prefix) {}

//
// Extract all the live files needed by this MANIFEST file and corresponding
// cloud_manifest object
//
IOStatus ManifestReader::GetLiveFiles(const std::string& bucket_path,
                                      std::set<uint64_t>* list) const {
  IOStatus s;
  std::unique_ptr<CloudManifest> cloud_manifest;
  const FileOptions file_opts;
  IODebugContext* dbg = nullptr;
  {
    std::unique_ptr<FSSequentialFile> file;
    auto cfs_impl = dynamic_cast<CloudFileSystemImpl*>(cfs_);
    assert(cfs_impl);
    auto cloudManifestFile = MakeCloudManifestFile(
        bucket_path, cfs_impl->GetCloudFileSystemOptions().cookie_on_open);
    s = cfs_->NewSequentialFileCloud(bucket_prefix_, cloudManifestFile,
                                     file_opts, &file, dbg);
    if (!s.ok()) {
      return s;
    }
    s = CloudManifest::LoadFromLog(
        std::unique_ptr<SequentialFileReader>(
            new SequentialFileReader(std::move(file), cloudManifestFile)),
        &cloud_manifest);
    if (!s.ok()) {
      return s;
    }
  }
  std::unique_ptr<SequentialFileReader> file_reader;
  {
    auto manifestFile =
        ManifestFileWithEpoch(bucket_path, cloud_manifest->GetCurrentEpoch());
    std::unique_ptr<FSSequentialFile> file;
    s = cfs_->NewSequentialFileCloud(bucket_prefix_, manifestFile, file_opts,
                                     &file, dbg);
    if (!s.ok()) {
      return s;
    }
    file_reader.reset(new SequentialFileReader(std::move(file), manifestFile));
  }

  return GetLiveFilesFromFileReader(std::move(file_reader), list);
}

IOStatus ManifestReader::GetMaxFileNumberFromManifest(FileSystem* fs,
                                                      const std::string& fname,
                                                      uint64_t* maxFileNumber) {
  // We check if the file exists to return IsNotFound() error status if it does
  // (NewSequentialFile) doesn't have the same behavior on file not existing --
  // it returns IOError instead.
  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;
  auto s = fs->FileExists(fname, io_opts, dbg);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<FSSequentialFile> file;
  s = fs->NewSequentialFile(fname, FileOptions(), &file, dbg);
  if (!s.ok()) {
    return s;
  }

  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(NULL,
                     std::unique_ptr<SequentialFileReader>(
                         new SequentialFileReader(std::move(file), fname)),
                     &reporter, true /*checksum*/, 0);

  Slice record;
  std::string scratch;

  *maxFileNumber = 0;
  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = status_to_io_status(edit.DecodeFrom(record));
    if (!s.ok()) {
      break;
    }
    uint64_t f;
    if (edit.GetNextFileNumber(&f)) {
      assert(*maxFileNumber <= f);
      *maxFileNumber = f;
    }
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
#endif /* ROCKSDB_LITE */
