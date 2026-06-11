//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/stackable_db.h"

namespace ROCKSDB_NAMESPACE {

struct ColumnDBBlobRange {
  // Index into the column vector passed to ColumnDB::Get().
  size_t column_index = 0;

  // Offset and size inside the large blob column value.
  uint64_t offset = 0;
  uint64_t size = 0;
};

struct ColumnDBOptions {
  // Name of the inline wide column storing the schema / directory payload.
  std::string schema_column_name = "schema";

  // Name of the wide column storing the blob reference for the encoded feature
  // payload. ColumnDB expects this column to contain an encoded BlobIndex.
  std::string blob_column_name = "blob";

  // Translates the schema payload and requested column names into byte ranges
  // inside the blob column value.
  std::function<Status(const Slice& schema,
                       const std::vector<Slice>& columns,
                       std::vector<ColumnDBBlobRange>* ranges)>
      translate;
};

class ColumnDB : public StackableDB {
 public:
  using StackableDB::Get;

  static Status Open(const ColumnDBOptions& options, std::unique_ptr<DB>&& db,
                     ColumnDB** column_db);

  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const std::vector<Slice>& columns,
                     std::vector<PinnableSlice>* values) = 0;

  virtual Status Get(const ReadOptions& options, const Slice& key,
                     const std::vector<Slice>& columns,
                     std::vector<PinnableSlice>* values) {
    return Get(options, DefaultColumnFamily(), key, columns, values);
  }

 protected:
  explicit ColumnDB(std::unique_ptr<DB>&& db) : StackableDB(std::move(db)) {}
};

}  // namespace ROCKSDB_NAMESPACE
