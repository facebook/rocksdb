// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <memory>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class CloudFileSystemImpl;
class Logger;

//
// Purges all unneeded files in a storage bucket
//
class Purge {
 public:
  // The bucket name is specified when the CloudFileSystem was created.
  Purge(CloudFileSystemImpl* fs, std::shared_ptr<Logger> info_log);

  virtual ~Purge();

  // Remove any un-needed files from the storage bucket
  virtual Status PurgeObsoleteFiles();

  // Remove any dbids that do not have backing files in S3
  virtual Status PurgeObsoleteDbid();

 private:
  CloudFileSystemImpl* cfs_;
  std::shared_ptr<Logger> info_log_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
