//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <cassert>
#include "rocksdb/sst_file_checksum.h"
#include "util/crc32c.h"

namespace rocksdb {

// This is the class to generate the SST file checksum based on Crc32. It
// will be used as the default checksum method for SST file checksum
class SstFileChecksumCrc32c : public SstFileChecksumFunc {
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

  const char* Name() const override { return "SstFileChecksumCrc32c"; }
};
}  // namespace rocksdb
