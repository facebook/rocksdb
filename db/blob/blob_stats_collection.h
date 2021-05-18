//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>

#include "db/blob/blob_stats.h"
#include "db/blob/blob_stats_record.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class BlobStatsCollection {
 public:
  template <typename Container>
  static void EncodeTo(const Container& c, std::string* output) {
    PutVarint64(output, c.size());

    for (auto it = c.begin(); it != c.end(); ++it) {
      const uint64_t blob_file_number = it->first;
      const BlobStats& stats = it->second;

      const BlobStatsRecord record(blob_file_number, stats.GetCount(),
                                   stats.GetBytes());
      record.EncodeTo(output);
    }
  }

  template <typename F>
  static Status DecodeFrom(Slice* input, F f) {
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

      f(record.GetBlobFileNumber(), record.GetCount(), record.GetBytes());
    }

    return Status::OK();
  }
};

}  // namespace ROCKSDB_NAMESPACE
