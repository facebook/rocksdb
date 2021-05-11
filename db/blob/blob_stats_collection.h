//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <unordered_map>

#include "db/blob/blob_stats_record.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

template <typename T>
class BlobStatsCollection {
 public:
  struct I {
    const T& operator()(const T& t) { return t; }
    T& operator()(T& t) { return t; }
  };

  template <typename F = I>
  void AddBlob(uint64_t blob_file_number, uint64_t bytes, F f = F()) {
    f(blob_stats_[blob_file_number]).AddBlob(bytes);
  }

  template <typename F = I>
  void AddBlobs(uint64_t blob_file_number, uint64_t count, uint64_t bytes,
                F f = F()) {
    f(blob_stats_[blob_file_number]).AddBlobs(count, bytes);
  }

  template <typename F = I>
  void EncodeTo(std::string* output, F f = F()) {
    if (blob_stats_.empty()) {
      return;
    }

    PutVarint64(output, blob_stats_.size());

    for (const auto& pair : blob_stats_) {
      const uint64_t blob_file_number = pair.first;
      const auto& stats = f(pair.second);

      BlobStatsRecord record(blob_file_number, stats.GetCount(),
                             stats.GetBytes());
      record.EncodeTo(output);
    }
  }

  template <typename F = I>
  Status DecodeFrom(Slice* input, F f = F()) {
    constexpr char class_name[] = "BlobStatsCollection";

    uint64_t size = 0;
    if (!GetVarint64(input, &size)) {
      return Status::Corruption(class_name, "Error decoding size");
    }

    for (uint64_t i = 0; i < size; ++i) {
      BlobStatsRecord record;

      const Status s = record.DecodeFrom(input);
      if (!s.ok()) {
        return s;
      }

      f(blob_stats_[record.GetBlobFileNumber()])
          .AddBlobs(record.GetCount(), record.GetBytes());
    }

    return Status::OK();
  }

 private:
  std::unordered_map<uint64_t, T> blob_stats_;
};

}  // namespace ROCKSDB_NAMESPACE
