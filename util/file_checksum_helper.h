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

namespace rocksdb {

// This is the class to generate the file checksum based on Crc32. It
// will be used as the default checksum method for SST file checksum
class FileChecksumFuncCrc32c : public FileChecksumFunc {
 public:
  uint32_t Extend(uint32_t init_checksum, const char* data, size_t n) override {
    assert(data != nullptr);
    return crc32c::Extend(init_checksum, data, n);
  }

  uint32_t Value(const char* data, size_t n) override {
    assert(data != nullptr);
    return crc32c::Value(data, n);
  }

  uint32_t ProcessChecksum(const uint32_t checksum) override {
    return crc32c::Mask(checksum);
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
      std::vector<uint64_t>* file_numbers, std::vector<uint32_t>* checksums,
      std::vector<std::string>* checksum_func_names) override;

  Status SearchOneFileChecksum(uint64_t file_number, uint32_t* checksum,
                               std::string* checksum_func_name) override;

  Status InsertOneFileChecksum(uint64_t file_number, uint32_t checksum,
                               std::string checksum_func_name) override;

  Status RemoveOneFileChecksum(uint64_t file_number) override;

 private:
  std::unordered_map<uint64_t, std::pair<uint32_t, std::string>> checksum_map_;
};

}  // namespace rocksdb
