//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cinttypes>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/compression_type.h"
#include "rocksdb/env.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class VersionSet;
class FileSystem;
struct ImmutableCFOptions;
struct MutableCFOptions;
struct FileOptions;
class BlobFileAddition;
class Status;
class Slice;
class BlobLogWriter;

class BlobFileBuilder {
 public:
  BlobFileBuilder(VersionSet* versions, Env* env, FileSystem* fs,
                  const ImmutableCFOptions* immutable_cf_options,
                  const MutableCFOptions* mutable_cf_options,
                  const FileOptions* file_options, int job_id,
                  uint32_t column_family_id,
                  const std::string& column_family_name,
                  Env::IOPriority io_priority,
                  Env::WriteLifeTimeHint write_hint,
                  std::vector<BlobFileAddition>* blob_file_additions);

  BlobFileBuilder(std::function<uint64_t()> file_number_generator, Env* env,
                  FileSystem* fs,
                  const ImmutableCFOptions* immutable_cf_options,
                  const MutableCFOptions* mutable_cf_options,
                  const FileOptions* file_options, int job_id,
                  uint32_t column_family_id,
                  const std::string& column_family_name,
                  Env::IOPriority io_priority,
                  Env::WriteLifeTimeHint write_hint,
                  std::vector<BlobFileAddition>* blob_file_additions);

  BlobFileBuilder(const BlobFileBuilder&) = delete;
  BlobFileBuilder& operator=(const BlobFileBuilder&) = delete;

  ~BlobFileBuilder();

  Status Add(const Slice& key, const Slice& value, std::string* blob_index);
  Status Finish();

 private:
  bool IsBlobFileOpen() const;
  Status OpenBlobFileIfNeeded();
  Status CompressBlobIfNeeded(Slice* blob, std::string* compressed_blob) const;
  Status WriteBlobToFile(const Slice& key, const Slice& blob,
                         uint64_t* blob_file_number, uint64_t* blob_offset);
  Status CloseBlobFile();
  Status CloseBlobFileIfNeeded();

  std::function<uint64_t()> file_number_generator_;
  Env* env_;
  FileSystem* fs_;
  const ImmutableCFOptions* immutable_cf_options_;
  uint64_t min_blob_size_;
  uint64_t blob_file_size_;
  CompressionType blob_compression_type_;
  const FileOptions* file_options_;
  int job_id_;
  uint32_t column_family_id_;
  std::string column_family_name_;
  Env::IOPriority io_priority_;
  Env::WriteLifeTimeHint write_hint_;
  std::vector<BlobFileAddition>* blob_file_additions_;
  std::unique_ptr<BlobLogWriter> writer_;
  uint64_t blob_count_;
  uint64_t blob_bytes_;
};

}  // namespace ROCKSDB_NAMESPACE
