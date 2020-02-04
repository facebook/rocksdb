//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <cassert>
#include <unordered_map>
#include "rocksdb/file_checksum.h"
#include "rocksdb/status.h"
#include "util/crc32c.h"
#include "util/string_util.h"

namespace rocksdb {

// This is the class to generate the file checksum based on Crc32. It
// will be used as the default checksum method for SST file checksum
class FileChecksumFuncCrc32c : public FileChecksumFunc {
 public:
  std::string Extend(const std::string& init_checksum, const char* data,
                     size_t n) override {
    assert(data != nullptr);
    uint32_t checksum_value = ParseUint32(init_checksum);
    return ToString(crc32c::Extend(checksum_value, data, n));
  }

  std::string Value(const char* data, size_t n) override {
    assert(data != nullptr);
    return ToString(crc32c::Value(data, n));
  }

  std::string ProcessChecksum(const std::string& checksum) override {
    uint32_t checksum_value = ParseUint32(checksum);
    return ToString(crc32c::Mask(checksum_value));
  }

  const char* Name() const override { return "FileChecksumCrc32c"; }
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

}  // namespace rocksdb
