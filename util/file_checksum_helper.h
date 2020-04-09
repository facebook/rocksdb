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
#include "util/crc32c.h"
#include "util/string_util.h"

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

  void Finalize() override { checksum_str_ = Uint32ToString(checksum_); }

  std::string GetChecksum() const override { return checksum_str_; }

  const char* Name() const override { return "FileChecksumCrc32c"; }

  // Convert a uint32_t type data into a 4 bytes string.
  static std::string Uint32ToString(uint32_t v) {
    std::string s;
    if (port::kLittleEndian) {
      s.append(reinterpret_cast<char*>(&v), sizeof(v));
    } else {
      char buf[sizeof(v)];
      buf[0] = v & 0xff;
      buf[1] = (v >> 8) & 0xff;
      buf[2] = (v >> 16) & 0xff;
      buf[3] = (v >> 24) & 0xff;
      s.append(buf, sizeof(v));
    }
    size_t i = 0, j = s.size() - 1;
    while (i < j) {
      char tmp = s[i];
      s[i] = s[j];
      s[j] = tmp;
      ++i;
      --j;
    }
    return s;
  }

  // Convert a 4 bytes size string into a uint32_t type data.
  static uint32_t StringToUint32(std::string s) {
    assert(s.size() == sizeof(uint32_t));
    size_t i = 0, j = s.size() - 1;
    while (i < j) {
      char tmp = s[i];
      s[i] = s[j];
      s[j] = tmp;
      ++i;
      --j;
    }
    uint32_t v = 0;
    if (port::kLittleEndian) {
      memcpy(&v, s.c_str(), sizeof(uint32_t));
    } else {
      const char* buf = s.c_str();
      v |= static_cast<uint32_t>(buf[0]);
      v |= (static_cast<uint32_t>(buf[1]) << 8);
      v |= (static_cast<uint32_t>(buf[2]) << 16);
      v |= (static_cast<uint32_t>(buf[3]) << 24);
    }
    return v;
  }

 private:
  uint32_t checksum_;
  std::string checksum_str_;
};

class FileChecksumGenCrc32cFactory : public FileChecksumGenFactory {
 public:
  std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
      const FileChecksumGenContext& context) override {
    return std::unique_ptr<FileChecksumGenerator>(
        new FileChecksumGenCrc32c(context));
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

}  // namespace ROCKSDB_NAMESPACE
