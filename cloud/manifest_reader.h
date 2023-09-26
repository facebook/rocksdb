// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <set>
#include <string>

#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

class CloudFileSystem;
class FileSystem;
class Logger;
class SequentialFileReader;

// Operates on MANIFEST files stored locally
class LocalManifestReader {
 public:
  LocalManifestReader(std::shared_ptr<Logger> info_log, CloudFileSystem* cfs);

  // Retrive all live files by reading manifest files locally.
  // If Manifest file doesn't exist, it will be pulled from s3
  //
  // REQUIRES: cfs_ should have cloud_manifest_ set, and it should have the
  // same content as the CLOUDMANIFEST file stored locally. cloud_manifest_ is
  // not updated when calling the function
  IOStatus GetLiveFilesLocally(const std::string& local_dbname,
                               std::set<uint64_t>* list) const;

  // Read given local manifest file and return all live files that it
  // references. This doesn't rely on CLOUDMANIFEST and just accepts (any valid)
  // manifest file.
  //
  // Provided manifest file is not updated or pulled from cloud when calling the
  // function.
  IOStatus GetManifestLiveFiles(const std::string& manifest_file,
                                std::set<uint64_t>* list) const;

 protected:
  // Get all the live SST file numbers by reading version_edit records from
  // file_reader
  IOStatus GetLiveFilesFromFileReader(
      std::unique_ptr<SequentialFileReader> file_reader,
      std::set<uint64_t>* list) const;

  std::shared_ptr<Logger> info_log_;
  CloudFileSystem* cfs_;
};

//
// Operates on MANIFEST files stored in the cloud bucket directly
//
class ManifestReader : public LocalManifestReader {
 public:
  ManifestReader(std::shared_ptr<Logger> info_log, CloudFileSystem* cfs,
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
