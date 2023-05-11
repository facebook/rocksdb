//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <functional>
#include <sstream>
#include <unordered_set>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// Dummy record in WAL logs signaling user-defined timestamp sizes for
// subsequent records.
class UserDefinedTimestampSizeRecord {
 public:
  struct RecordPairHash {
    std::size_t operator()(const std::pair<uint32_t, size_t>& p) const {
      auto hash1 = std::hash<uint32_t>{}(p.first);
      auto hash2 = std::hash<size_t>{}(p.second);
      return hash1 ^ (hash2 + 0x9e3779b9 + (hash1 << 6) + (hash1 >> 2));
    }
  };

  UserDefinedTimestampSizeRecord() {}
  explicit UserDefinedTimestampSizeRecord(
      std::unordered_set<std::pair<uint32_t, size_t>, RecordPairHash>&&
          cf_to_ts_sz)
      : cf_to_ts_sz_(std::move(cf_to_ts_sz)) {}

  const std::unordered_set<std::pair<uint32_t, size_t>, RecordPairHash>&
  GetUserDefinedTimestampSize() const {
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
      cf_to_ts_sz_.insert(std::make_pair(cf_id, static_cast<size_t>(ts_sz)));
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

  std::unordered_set<std::pair<uint32_t, size_t>, RecordPairHash> cf_to_ts_sz_;
};

}  // namespace ROCKSDB_NAMESPACE
