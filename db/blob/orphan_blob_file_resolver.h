//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rocksdb/compression_type.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class FileSystem;
class Logger;
class RandomAccessFileReader;
class Statistics;
class SystemClock;
class VersionSet;

// Resolves BlobIndex entries during WAL replay that point to orphan blob files
// (files on disk but not registered in any CF's VersionStorageInfo).
//
// During recovery, instead of registering orphan blob files directly into the
// MANIFEST, this resolver reads blob values on demand and converts them back
// to raw kTypeValue entries. The existing flush infrastructure then creates
// new properly-tracked blob files.
//
// Lifecycle:
//   - Created after versions_->Recover(), before WAL replay
//   - Used during WAL replay by MemTableInserter::PutBlobIndexCF
//   - Destroyed after WAL replay completes
class OrphanBlobFileResolver {
 public:
  // Scan the DB directory, identify orphan blob files not registered in any
  // CF's VersionStorageInfo, open file handles, and read/validate headers.
  // Files with invalid headers or belonging to dropped CFs are skipped.
  static Status Create(FileSystem* fs, const std::string& dbname,
                       SystemClock* clock, Statistics* statistics,
                       Logger* info_log, VersionSet* versions,
                       std::unique_ptr<OrphanBlobFileResolver>* resolver);

  ~OrphanBlobFileResolver();

  // Returns true if file_number belongs to an orphan blob file.
  bool IsOrphan(uint64_t file_number) const;

  // Returns true if file_number is registered in any CF's VersionStorageInfo.
  // Used to detect BlobIndex entries pointing to files that are neither
  // registered nor resolvable (e.g., truncated by crash before header flush).
  bool IsRegistered(uint64_t file_number) const;

  // Read blob value from an orphan file. The caller provides the BlobIndex
  // fields (file_number, offset, value_size, compression) and the user key
  // for verification.
  //
  // On success: returns OK and fills *value with the decompressed raw value.
  // On failure: returns NotFound (file not orphan) or Corruption (read/CRC
  //             error), increments discarded counter.
  Status TryResolveBlob(uint64_t file_number, uint64_t offset,
                        uint64_t value_size, CompressionType compression,
                        const Slice& user_key, std::string* value);

  uint64_t resolved_count() const { return resolved_count_; }
  uint64_t discarded_count() const { return discarded_count_; }
  size_t orphan_file_count() const { return orphan_files_.size(); }

  // Information about an orphan file needed for MANIFEST registration.
  struct OrphanFileInfo {
    uint64_t file_number;
    uint32_t column_family_id;
    uint64_t file_size;
    uint64_t blob_count;
    uint64_t total_blob_bytes;
    bool has_footer;  // true if the file already has a valid footer
    // Position after the last fully validated record. For files without a
    // footer, the file should be truncated to this size before sealing.
    // Equals BlobLogHeader::kSize + total_blob_bytes.
    uint64_t valid_data_size;
  };

  // Returns metadata for all orphan files, used after WAL replay to
  // register them in MANIFEST.
  std::vector<OrphanFileInfo> GetOrphanFileInfo() const;

 private:
  struct OrphanFile {
    std::unique_ptr<RandomAccessFileReader> reader;
    uint64_t file_size;
    CompressionType compression;
    uint32_t column_family_id;
    uint64_t blob_count;
    uint64_t total_blob_bytes;
    bool has_footer;
  };

  OrphanBlobFileResolver(SystemClock* clock, Statistics* statistics,
                         Logger* info_log);

  FileSystem* fs_;
  SystemClock* clock_;
  Statistics* statistics_;
  Logger* info_log_;

  // Map from file_number to open file handle + metadata.
  std::unordered_map<uint64_t, OrphanFile> orphan_files_;

  // Set of blob file numbers registered in any CF's VersionStorageInfo.
  // Used to distinguish "registered" (safe to keep as kTypeBlobIndex) from
  // "unregistered and unresolvable" (must discard during WAL replay).
  std::unordered_set<uint64_t> registered_files_;

  uint64_t resolved_count_ = 0;
  uint64_t discarded_count_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
