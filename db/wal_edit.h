// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

// WAL related classes used in VersionEdit and VersionSet.

#pragma once

#include <map>
#include <ostream>
#include <string>
#include <vector>

#include "logging/event_logger.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter;
class Slice;
class Status;

using WalNumber = uint64_t;

// Metadata of a WAL.
class WalMetadata {
 public:
  WalMetadata() = default;

  explicit WalMetadata(uint64_t size_bytes) : size_bytes_(size_bytes) {}

  bool HasSize() const { return size_bytes_ != kUnknownWalSize; }

  void SetSizeInBytes(uint64_t bytes) { size_bytes_ = bytes; }

  uint64_t GetSizeInBytes() const { return size_bytes_; }

 private:
  // The size of WAL is unknown, used when the WAL is not closed yet.
  constexpr static uint64_t kUnknownWalSize = 0;

  // Size of a closed WAL in bytes.
  uint64_t size_bytes_ = kUnknownWalSize;
};

// These tags are persisted to MANIFEST, so it's part of the user API.
enum class WalAdditionTag : uint32_t {
  // Indicates that there are no more tags.
  kTerminate = 1,
  // Size in bytes.
  kSize = 2,
  // Add tags in the future, such as checksum?
};

// Records the event of adding a WAL in VersionEdit.
class WalAddition {
 public:
  WalAddition() : number_(0), metadata_() {}

  explicit WalAddition(WalNumber number) : number_(number), metadata_() {}

  WalAddition(WalNumber number, WalMetadata meta)
      : number_(number), metadata_(std::move(meta)) {}

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

// Records the event of deleting/archiving a WAL in VersionEdit.
class WalDeletion {
 public:
  WalDeletion() : number_(0) {}

  explicit WalDeletion(WalNumber number) : number_(number) {}

  WalNumber GetLogNumber() const { return number_; }

  void EncodeTo(std::string* dst) const;

  Status DecodeFrom(Slice* src);

  std::string DebugString() const;

 private:
  WalNumber number_;
};

std::ostream& operator<<(std::ostream& os, const WalDeletion& wal);
JSONWriter& operator<<(JSONWriter& jw, const WalDeletion& wal);

using WalDeletions = std::vector<WalDeletion>;

// Used in VersionSet to keep the current set of WALs.
//
// When a WAL is created, closed, deleted, or archived,
// a VersionEdit is logged to MANIFEST and
// the WAL is added to or deleted from WalSet.
//
// Not thread safe, needs external synchronization such as holding DB mutex.
class WalSet {
 public:
  // Add WAL(s).
  // If the WAL has size, it means the WAL is closed,
  // then there must be an existing WAL without size that is added
  // when creating the WAL, otherwise, return Status::Corruption.
  // Can happen when applying a VersionEdit or recovering from MANIFEST.
  Status AddWal(const WalAddition& wal);
  Status AddWals(const WalAdditions& wals);

  // Delete WAL(s).
  // The WAL to be deleted must exist, otherwise,
  // return Status::Corruption.
  // Can happen when applying a VersionEdit or recovering from MANIFEST.
  Status DeleteWal(const WalDeletion& wal);
  Status DeleteWals(const WalDeletions& wals);

  // Resets the internal state.
  void Reset();

  const std::map<WalNumber, WalMetadata>& GetWals() const { return wals_; }

 private:
  std::map<WalNumber, WalMetadata> wals_;
};

}  // namespace ROCKSDB_NAMESPACE
