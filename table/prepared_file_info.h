//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>

#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {

// Definition of the type that is opaque to public API users (declared only as a
// forward declaration in rocksdb/options.h and rocksdb/sst_file_writer.h).
// Produced by SstFileWriter::Finish and consumed by ExternalSstFileIngestionJob
// so ingestion can skip re-opening and scanning the file to recompute its
// metadata.
struct PreparedFileInfo {
  uint64_t file_size = 0;
  // Encoded internal keys (user key + 8-byte footer), with the user-defined
  // timestamp stripped when timestamps are not persisted.
  std::string smallest_internal_key;
  std::string largest_internal_key;
  // Range-deletion boundary user keys (timestamp-stripped when applicable);
  // empty when the file has no range deletions.
  std::string smallest_range_del_key;
  std::string largest_range_del_key;
  TableProperties table_properties;
};

}  // namespace ROCKSDB_NAMESPACE
