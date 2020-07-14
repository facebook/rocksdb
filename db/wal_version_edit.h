// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

// WAL related classes used in VersionEdit.

#pragma once

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter;
class Slice;
class Status;

using WalNumber = uint64_t;

// The size of WAL is unknown, used when the WAL is not closed yet.

// Metadata of a WAL.
class WalMetadata {
 public:
  WalMetadata() = default;

  WalMetadata(uint64_t size_bytes) : size_bytes_(size_bytes) {}

  bool HasUnknownSize() const { return size_bytes_ == kUnknownWalSize; }

  void SetSizeInBytes(uint64_t bytes) { size_bytes_ = bytes; }

  uint64_t GetSizeInBytes() const { return size_bytes_; }

 private:
  constexpr static uint64_t kUnknownWalSize = 0;

  // Size of a closed WAL in bytes.
  uint64_t size_bytes_ = kUnknownWalSize;
};

// Records the event of adding a WAL in VersionEdit.
class WalAddition {
 public:
  WalAddition() : number_(0), metadata_() {}

  WalAddition(WalNumber number, const WalMetadata& metadata)
      : number_(number), metadata_(metadata) {}

  WalNumber GetLogNumber() const { return number_; }

  const WalMetadata& GetMetadata() const { return metadata_; }

  void EncodeTo(std::string* dst) const;

  Status DecodeFrom(Slice* src);

  std::string DebugString() const;

 private:
  WalNumber number_;
  WalMetadata metadata_;
};

std::ostream& operator<<(std::ostream& os, const WalAddition& wal);
JSONWriter& operator<<(JSONWriter& jw, const WalAddition& wal);

using WalAdditions = std::vector<WalAddition>;

}  // namespace ROCKSDB_NAMESPACE
