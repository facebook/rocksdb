//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Status;
struct ReadOptions;
struct ImmutableCFOptions;
struct FileOptions;
class Slice;
class GetContext;
class RandomAccessFileReader;

class BlobFileReader {
 public:
  static Status Create(const ReadOptions& read_options,
                       const ImmutableCFOptions& immutable_cf_options,
                       const FileOptions& file_options,
                       uint64_t blob_file_number,
                       std::unique_ptr<BlobFileReader>* reader);

  BlobFileReader(const BlobFileReader&) = delete;
  BlobFileReader& operator=(const BlobFileReader&) = delete;

  ~BlobFileReader();

  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 uint64_t offset, uint64_t size, GetContext* get_context) const;

 private:
  explicit BlobFileReader(
      std::unique_ptr<RandomAccessFileReader>&& file_reader);

  std::unique_ptr<RandomAccessFileReader> file_reader_;
};

}  // namespace ROCKSDB_NAMESPACE
