//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <cassert>
#include <unordered_map>

#include "port/port.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/status.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

// This is the class to generate the file checksum based on Crc32. It
// will be used as the default checksum method for SST file checksum
class FileChecksumGenCrc32c : public FileChecksumGenerator {
 public:
  FileChecksumGenCrc32c(const FileChecksumGenContext& /*context*/) {
    checksum_ = 0;
  }

  void Update(const char* data, size_t n) override {
    checksum_ = crc32c::Extend(checksum_, data, n);
  }

  void Finalize() override {
    assert(checksum_str_.empty());
    // Store as big endian raw bytes
    PutFixed32(&checksum_str_, EndianSwapValue(checksum_));
  }

  std::string GetChecksum() const override {
    assert(!checksum_str_.empty());
    return checksum_str_;
  }

  const char* Name() const override { return "FileChecksumCrc32c"; }

 private:
  uint32_t checksum_;
  std::string checksum_str_;
};

class FileChecksumGenCrc32cFactory : public FileChecksumGenFactory {
 public:
  std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
      const FileChecksumGenContext& context) override {
    if (context.requested_checksum_func_name.empty() ||
        context.requested_checksum_func_name == "FileChecksumCrc32c") {
      return std::unique_ptr<FileChecksumGenerator>(
          new FileChecksumGenCrc32c(context));
    } else {
      return nullptr;
    }
  }

  const char* Name() const override { return "FileChecksumGenCrc32cFactory"; }
};

// The default implementaion of FileChecksumList
class FileChecksumListImpl : public FileChecksumList {
 public:
  FileChecksumListImpl() {}
  void reset() override;

  size_t size() const override;

  Status GetAllFileChecksums(
      std::vector<uint64_t>* file_numbers, std::vector<std::string>* checksums,
      std::vector<std::string>* checksum_func_names) override;

  Status SearchOneFileChecksum(uint64_t file_number, std::string* checksum,
                               std::string* checksum_func_name) override;

  Status InsertOneFileChecksum(uint64_t file_number,
                               const std::string& checksum,
                               const std::string& checksum_func_name) override;

  Status RemoveOneFileChecksum(uint64_t file_number) override;

 private:
  // Key is the file number, the first portion of the value is checksum, the
  // second portion of the value is checksum function name.
  std::unordered_map<uint64_t, std::pair<std::string, std::string>>
      checksum_map_;
};

// If manifest_file_size < std::numeric_limits<uint64_t>::max(), only use
// that length prefix of the manifest file.
Status GetFileChecksumsFromManifest(Env* src_env, const std::string& abs_path,
                                    uint64_t manifest_file_size,
                                    FileChecksumList* checksum_list);

}  // namespace ROCKSDB_NAMESPACE
