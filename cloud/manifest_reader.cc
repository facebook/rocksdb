// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "cloud/manifest_reader.h"

#include "cloud/cloud_manifest.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "db/version_set.h"
#include "env/composite_env_wrapper.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

ManifestReader::ManifestReader(std::shared_ptr<Logger> info_log, CloudEnv* cenv,
                               const std::string& bucket_prefix)
    : info_log_(info_log), cenv_(cenv), bucket_prefix_(bucket_prefix) {}

ManifestReader::~ManifestReader() {}

//
// Extract all the live files needed by this MANIFEST file
//
Status ManifestReader::GetLiveFiles(const std::string bucket_path,
                                    std::set<uint64_t>* list) {
  Status s;
  std::unique_ptr<CloudManifest> cloud_manifest;
  {
    std::unique_ptr<SequentialFile> file;
    auto cloudManifestFile = CloudManifestFile(bucket_path);
    s = cenv_->NewSequentialFileCloud(bucket_prefix_, cloudManifestFile, &file,
                                      EnvOptions());
    if (!s.ok()) {
      return s;
    }
    s = CloudManifest::LoadFromLog(
        std::unique_ptr<SequentialFileReader>(new SequentialFileReader(
            NewLegacySequentialFileWrapper(file), cloudManifestFile)),
        &cloud_manifest);
    if (!s.ok()) {
      return s;
    }
  }
  std::unique_ptr<SequentialFileReader> file_reader;
  {
    auto manifestFile = ManifestFileWithEpoch(
        bucket_path, cloud_manifest->GetCurrentEpoch().ToString());
    std::unique_ptr<SequentialFile> file;
    s = cenv_->NewSequentialFileCloud(bucket_prefix_, manifestFile, &file,
                                      EnvOptions());
    if (!s.ok()) {
      return s;
    }
    file_reader.reset(new SequentialFileReader(
        NewLegacySequentialFileWrapper(file), manifestFile));
  }

  // create a callback that gets invoked whil looping through the log records
  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(nullptr, std::move(file_reader), &reporter,
                     true /*checksum*/, 0);

  Slice record;
  std::string scratch;
  int count = 0;

  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      break;
    }
    count++;

    // add the files that are added by this transaction
    std::vector<std::pair<int, FileMetaData>> new_files = edit.GetNewFiles();
    for (auto& one : new_files) {
      uint64_t num = one.second.fd.GetNumber();
      list->insert(num);
    }
    // delete the files that are removed by this transaction
    std::set<std::pair<int, uint64_t>> deleted_files = edit.GetDeletedFiles();
    for (auto& one : deleted_files) {
      uint64_t num = one.second;
      list->erase(num);
    }
  }
  file_reader.reset();
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[mn] manifest for db %s has %d entries %s", bucket_path.c_str(), count,
      s.ToString().c_str());
  return s;
}

Status ManifestReader::GetMaxFileNumberFromManifest(Env* env,
                                                    const std::string& fname,
                                                    uint64_t* maxFileNumber) {
  // We check if the file exists to return IsNotFound() error status if it does
  // (NewSequentialFile) doesn't have the same behavior on file not existing --
  // it returns IOError instead.
  auto s = env->FileExists(fname);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<SequentialFile> file;
  s = env->NewSequentialFile(fname, &file, EnvOptions());
  if (!s.ok()) {
    return s;
  }

  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(
      NULL,
      std::unique_ptr<SequentialFileReader>(new SequentialFileReader(
          NewLegacySequentialFileWrapper(file), fname)),
      &reporter, true /*checksum*/, 0);

  Slice record;
  std::string scratch;

  *maxFileNumber = 0;
  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
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
