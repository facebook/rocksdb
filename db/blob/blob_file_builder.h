//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cassert>
#include <cinttypes>
#include <memory>
#include <string>
#include <vector>

#include "db/blob/blob_log_writer.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class VersionSet;
class Env;
class FileSystem;
struct ImmutableCFOptions;
struct MutableCFOptions;
struct FileOptions;
class BlobFileAddition;
class Status;
class Slice;

class BlobFileBuilder {
 public:
  BlobFileBuilder(VersionSet* versions, Env* env, FileSystem* fs,
                  const ImmutableCFOptions* immutable_cf_options,
                  const MutableCFOptions* mutable_cf_options,
                  const FileOptions* file_options, uint32_t column_family_id,
                  std::vector<BlobFileAddition>* blob_file_additions)
      : versions_(versions),
        env_(env),
        fs_(fs),
        immutable_cf_options_(immutable_cf_options),
        mutable_cf_options_(mutable_cf_options),
        file_options_(file_options),
        column_family_id_(column_family_id),
        blob_file_additions_(blob_file_additions),
        blob_count_(0),
        blob_bytes_(0) {
    assert(versions_);
    assert(env_);
    assert(fs_);
    assert(immutable_cf_options_);
    assert(mutable_cf_options_);
    assert(file_options_);
    assert(blob_file_additions_);
  }

  BlobFileBuilder(const BlobFileBuilder&) = delete;
  BlobFileBuilder& operator=(const BlobFileBuilder&) = delete;

  Status Add(const Slice& key, const Slice& value, Slice* blob_index);
  Status Finish();

 private:
  bool IsBlobFileOpen() const;
  Status OpenBlobFileIfNeeded();
  Status CompressBlobIfNeeded(Slice* blob, CompressionType compression_type,
                              std::string* compressed_blob) const;
  Status WriteBlobToFile(const Slice& key, const Slice& blob,
                         uint64_t* blob_file_number, uint64_t* blob_offset);
  Status CloseBlobFile();
  Status CloseBlobFileIfNeeded();

  VersionSet* versions_;
  Env* env_;
  FileSystem* fs_;
  const ImmutableCFOptions* immutable_cf_options_;
  const MutableCFOptions* mutable_cf_options_;
  const FileOptions* file_options_;
  uint32_t column_family_id_;
  std::vector<BlobFileAddition>* blob_file_additions_;
  std::unique_ptr<BlobLogWriter> writer_;
  uint64_t blob_count_;
  uint64_t blob_bytes_;
  std::string blob_index_;
};

}  // namespace ROCKSDB_NAMESPACE
