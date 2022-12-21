// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <set>
#include <string>
#include <vector>

#include "cloud/cloud_env_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

// Operates on MANIFEST files stored locally
class LocalManifestReader {
 public:
  LocalManifestReader(std::shared_ptr<Logger> info_log, CloudEnv* cenv);

  // Retrive all live files by reading manifest files locally.
  // If Manifest file doesn't exist, it will be pulled from s3
  //
  // If manifest_file_version is not null, it will fetch latest Manifest file
  // from s3 first before reading it(be careful with this since it will override
  // the manfiest file in local_dbname). manifest_file_version will be set as
  // the version we read from s3.
  //
  // REQUIRES: cenv_ should have cloud_manifest_ set, and it should have the
  // same content as the CLOUDMANIFEST file stored locally. cloud_manifest_ is
  // not updated when calling the function
  // REQUIRES: cloud storage has versioning enabled if manifest_file_version !=
  // nullptr
  IOStatus GetLiveFilesLocally(
      const std::string& local_dbname, std::set<uint64_t>* list,
      std::string* manifest_file_version = nullptr) const;

 protected:
  // Get all the live sst file number by reading version_edit records from file_reader
  IOStatus GetLiveFilesFromFileReader(
      std::unique_ptr<SequentialFileReader> file_reader,
      std::set<uint64_t>* list) const;

  std::shared_ptr<Logger> info_log_;
  CloudEnv* cenv_;
};

//
// Operates on MANIFEST files stored in the cloud bucket directly
//
class ManifestReader: public LocalManifestReader {
 public:
  ManifestReader(std::shared_ptr<Logger> info_log, CloudEnv* cenv,
                 const std::string& bucket_prefix);

  // Retrieve all live files referred to by this bucket path
  // It will read from CLOUDMANIFEST and MANIFEST file in s3 directly
  // TODO(wei): remove this function. Reading from s3 directly is very slow for
  // large MANIFEST file
  IOStatus GetLiveFiles(const std::string& bucket_path,
                        std::set<uint64_t>* list) const;

  static IOStatus GetMaxFileNumberFromManifest(FileSystem* fs,
                                               const std::string& fname,
                                               uint64_t* maxFileNumber);

 private:
  std::string bucket_prefix_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
