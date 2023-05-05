//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <map>
#include <sstream>

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
      std::map<uint32_t, size_t> cf_to_ts_sz)
      : cf_to_ts_sz_(cf_to_ts_sz) {}

  const std::map<uint32_t, size_t>& GetUserDefinedTimestampSize() const {
    return cf_to_ts_sz_;
  }

  inline void EncodeTo(std::string* dst) const {
    assert(dst != nullptr);
    for (const auto [cf_id, ts_sz] : cf_to_ts_sz_) {
      assert(ts_sz != 0);
      PutFixed32(dst, cf_id);
      PutFixed16(dst, static_cast<uint16_t>(ts_sz));
    }
  }

  inline Status DecodeFrom(Slice* src) {
    int num_of_entries = static_cast<int>(src->size() / (4 + 2));
    for (int i = 0; i < num_of_entries; i++) {
      uint32_t cf_id;
      uint16_t ts_sz;
      if (!GetFixed32(src, &cf_id) || !GetFixed16(src, &ts_sz)) {
        return Status::Corruption(
            "Error decoding user-defined timestamp size record entry");
      }
      cf_to_ts_sz_[cf_id] = static_cast<size_t>(ts_sz);
    }
    return Status::OK();
  }

  inline std::string DebugString() const {
    std::ostringstream oss;

    for (const auto [cf_id, ts_sz] : cf_to_ts_sz_) {
      oss << "Column family: " << cf_id
          << ", user-defined timestamp size: " << ts_sz << std::endl;
    }
    return oss.str();
  }

 private:
  std::map<uint32_t, size_t> cf_to_ts_sz_;
};

}  // namespace ROCKSDB_NAMESPACE
