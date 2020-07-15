// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/wal_version.h"

#include <algorithm>

#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

namespace {

enum WalAdditionTag : uint32_t {
  // There are no more tags after this tag.
  kTerminate = 1,
  // Add tags in the future, such as checksum?
};

}  // anonymous namespace

void WalAddition::EncodeTo(std::string* dst) const {
  PutVarint64(dst, number_);
  PutVarint64(dst, metadata_.GetSizeInBytes());
  PutVarint32(dst, WalAdditionTag::kTerminate);
}

Status WalAddition::DecodeFrom(Slice* src) {
  constexpr char class_name[] = "WalAddition";

  if (!GetVarint64(src, &number_)) {
    return Status::Corruption(class_name, "Error decoding WAL log number");
  }

  uint64_t size = 0;
  if (!GetVarint64(src, &size)) {
    return Status::Corruption(class_name, "Error decoding WAL file size");
  }
  metadata_.SetSizeInBytes(size);

  while (true) {
    uint32_t tag = 0;
    if (!GetVarint32(src, &tag)) {
      return Status::Corruption(class_name, "Error decoding tag");
    }
    if (tag == kTerminate) {
      break;
    }
    // TODO: process future tags such as checksum.
  }
  return Status::OK();
}

JSONWriter& operator<<(JSONWriter& jw, const WalAddition& wal) {
  jw << "LogNumber" << wal.GetLogNumber() << "SizeInBytes"
     << wal.GetMetadata().GetSizeInBytes();
  return jw;
}

std::ostream& operator<<(std::ostream& os, const WalAddition& wal) {
  os << "log_number: " << wal.GetLogNumber()
     << " size_in_bytes: " << wal.GetMetadata().GetSizeInBytes();
  return os;
}

std::string WalAddition::DebugString() const {
  std::ostringstream oss;
  oss << *this;
  return oss.str();
}

void WalSet::PurgeObsoleteWals(WalNumber min_log_number_to_keep) {
  auto it = wals_.lower_bound(min_log_number_to_keep);
  wals_.erase(wals_.begin(), it);
}

void WalSet::Reset() {
  wals_.clear();
}

Status WalSet::CheckWals(Env* env, WalNumber min_log_number_to_keep,
                         const std::vector<uint64_t>& log_numbers,
                         const std::vector<std::string>& log_fnames) const {
  assert(log_numbers.size() == log_fnames.size());
  assert(env != nullptr);

  // Skip the ignorable logs.
  size_t log_idx = 0;
  {
    auto it = std::lower_bound(log_numbers.begin(), log_numbers.end(),
                              min_log_number_to_keep);
    if (it == log_numbers.end()) {
      log_idx = log_numbers.size();
    } else {
      log_idx = it - log_numbers.begin();
    }
  }

  if (log_idx >= log_numbers.size()) {
    // Not log to recover from.
    return Status::OK();
  }

  auto it = wals_.find(log_numbers[log_idx]);
  if (it == wals_.end()) {
    // The last synced batch of logs might not be persisted into MANIFEST.
    return Status::OK();
  }

  Status s;
  for (; it != wals_.end(); ++log_idx, ++it) {
    const uint64_t log_number = it->first;
    const WalMetadata& wal_meta = it->second;

    if (log_idx >= log_numbers.size() || log_number != log_numbers[log_idx]) {
      std::stringstream ss;
      ss << "Missing WAL with log number: " << log_number << ".";
      s = Status::Corruption(ss.str());
      break;
    }

    if (wal_meta.HasSize()) {
      uint64_t bytes = 0;
      s = env->GetFileSize(log_fnames[log_idx], &bytes);
      if (!s.ok()) {
        break;
      }
      if (wal_meta.GetSizeInBytes() != bytes) {
        std::stringstream ss;
        ss << "Size mismatch: WAL (log number: " << log_number
           << ") in MANIFEST is " << wal_meta.GetSizeInBytes()
           << " bytes , but actually is " << bytes << " bytes on disk.";
        s = Status::Corruption(ss.str());
        break;
      }
    }  // else since the size is unknown, skip the size check.
  }

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
