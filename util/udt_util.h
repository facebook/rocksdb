//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <sstream>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// Dummy record in WAL logs signaling user-defined timestamp sizes for
// subsequent records.
class UserDefinedTimestampSizeRecord {
 public:
  UserDefinedTimestampSizeRecord() {}
  explicit UserDefinedTimestampSizeRecord(
      std::vector<std::pair<uint32_t, size_t>>&& cf_to_ts_sz)
      : cf_to_ts_sz_(std::move(cf_to_ts_sz)) {}

  const std::vector<std::pair<uint32_t, size_t>>& GetUserDefinedTimestampSize()
      const {
    return cf_to_ts_sz_;
  }

  inline void EncodeTo(std::string* dst) const {
    assert(dst != nullptr);
    for (const auto& [cf_id, ts_sz] : cf_to_ts_sz_) {
      assert(ts_sz != 0);
      PutFixed32(dst, cf_id);
      PutFixed16(dst, static_cast<uint16_t>(ts_sz));
    }
  }

  inline Status DecodeFrom(Slice* src) {
    const size_t total_size = src->size();
    if ((total_size % kSizePerColumnFamily) != 0) {
      std::ostringstream oss;
      oss << "User-defined timestamp size record length: " << total_size
          << " is not a multiple of " << kSizePerColumnFamily << std::endl;
      return Status::Corruption(oss.str());
    }
    int num_of_entries = static_cast<int>(total_size / kSizePerColumnFamily);
    for (int i = 0; i < num_of_entries; i++) {
      uint32_t cf_id = 0;
      uint16_t ts_sz = 0;
      if (!GetFixed32(src, &cf_id) || !GetFixed16(src, &ts_sz)) {
        return Status::Corruption(
            "Error decoding user-defined timestamp size record entry");
      }
      cf_to_ts_sz_.emplace_back(cf_id, static_cast<size_t>(ts_sz));
    }
    return Status::OK();
  }

  inline std::string DebugString() const {
    std::ostringstream oss;

    for (const auto& [cf_id, ts_sz] : cf_to_ts_sz_) {
      oss << "Column family: " << cf_id
          << ", user-defined timestamp size: " << ts_sz << std::endl;
    }
    return oss.str();
  }

 private:
  // 4 bytes for column family id, 2 bytes for user-defined timestamp size.
  static constexpr size_t kSizePerColumnFamily = 4 + 2;

  std::vector<std::pair<uint32_t, size_t>> cf_to_ts_sz_;
};

}  // namespace ROCKSDB_NAMESPACE
