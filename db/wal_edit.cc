// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/wal_edit.h"

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

void WalAddition::EncodeTo(std::string* dst) const {
  PutVarint64(dst, number_);

  if (metadata_.HasSyncedSize()) {
    PutVarint32(dst, static_cast<uint32_t>(WalAdditionTag::kSyncedSize));
    PutVarint64(dst, metadata_.GetSyncedSizeInBytes());
  }

  PutVarint32(dst, static_cast<uint32_t>(WalAdditionTag::kTerminate));
}

Status WalAddition::DecodeFrom(Slice* src) {
  constexpr char class_name[] = "WalAddition";

  if (!GetVarint64(src, &number_)) {
    return Status::Corruption(class_name, "Error decoding WAL log number");
  }

  while (true) {
    uint32_t tag_value = 0;
    if (!GetVarint32(src, &tag_value)) {
      return Status::Corruption(class_name, "Error decoding tag");
    }
    WalAdditionTag tag = static_cast<WalAdditionTag>(tag_value);
    switch (tag) {
      case WalAdditionTag::kSyncedSize: {
        uint64_t size = 0;
        if (!GetVarint64(src, &size)) {
          return Status::Corruption(class_name, "Error decoding WAL file size");
        }
        metadata_.SetSyncedSizeInBytes(size);
        break;
      }
      // TODO: process future tags such as checksum.
      case WalAdditionTag::kTerminate:
        return Status::OK();
      default: {
        std::stringstream ss;
        ss << "Unknown tag " << tag_value;
        return Status::Corruption(class_name, ss.str());
      }
    }
  }
}

JSONWriter& operator<<(JSONWriter& jw, const WalAddition& wal) {
  jw << "LogNumber" << wal.GetLogNumber() << "SyncedSizeInBytes"
     << wal.GetMetadata().GetSyncedSizeInBytes();
  return jw;
}

std::ostream& operator<<(std::ostream& os, const WalAddition& wal) {
  os << "log_number: " << wal.GetLogNumber()
     << " synced_size_in_bytes: " << wal.GetMetadata().GetSyncedSizeInBytes();
  return os;
}

std::string WalAddition::DebugString() const {
  std::ostringstream oss;
  oss << *this;
  return oss.str();
}

void WalDeletion::EncodeTo(std::string* dst) const {
  PutVarint64(dst, number_);
}

Status WalDeletion::DecodeFrom(Slice* src) {
  constexpr char class_name[] = "WalDeletion";

  if (!GetVarint64(src, &number_)) {
    return Status::Corruption(class_name, "Error decoding WAL log number");
  }

  return Status::OK();
}

JSONWriter& operator<<(JSONWriter& jw, const WalDeletion& wal) {
  jw << "LogNumber" << wal.GetLogNumber();
  return jw;
}

std::ostream& operator<<(std::ostream& os, const WalDeletion& wal) {
  os << "log_number: " << wal.GetLogNumber();
  return os;
}

std::string WalDeletion::DebugString() const {
  std::ostringstream oss;
  oss << *this;
  return oss.str();
}

Status WalSet::AddWal(const WalAddition& wal) {
  if (wal.GetLogNumber() < min_wal_number_to_keep_) {
    // The WAL has been obsolete, ignore it.
    return Status::OK();
  }

  auto it = wals_.lower_bound(wal.GetLogNumber());
  bool existing = it != wals_.end() && it->first == wal.GetLogNumber();

  if (!existing) {
    wals_.insert(it, {wal.GetLogNumber(), wal.GetMetadata()});
    return Status::OK();
  }

  assert(existing);
  if (!wal.GetMetadata().HasSyncedSize()) {
    std::stringstream ss;
    ss << "WAL " << wal.GetLogNumber() << " is created more than once";
    return Status::Corruption("WalSet::AddWal", ss.str());
  }

  assert(wal.GetMetadata().HasSyncedSize());
  if (it->second.HasSyncedSize() && wal.GetMetadata().GetSyncedSizeInBytes() <=
                                        it->second.GetSyncedSizeInBytes()) {
    // This is possible because version edits with different synced WAL sizes
    // for the same WAL can be committed out-of-order. For example, thread
    // 1 synces the first 10 bytes of 1.log, while thread 2 synces the first 20
    // bytes of 1.log. It's possible that thread 1 calls LogAndApply() after
    // thread 2.
    // In this case, just return ok.
    return Status::OK();
  }

  // Update synced size for the given WAL.
  it->second.SetSyncedSizeInBytes(wal.GetMetadata().GetSyncedSizeInBytes());
  return Status::OK();
}

Status WalSet::AddWals(const WalAdditions& wals) {
  Status s;
  for (const WalAddition& wal : wals) {
    s = AddWal(wal);
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

Status WalSet::DeleteWalsBefore(WalNumber wal) {
  if (wal > min_wal_number_to_keep_) {
    min_wal_number_to_keep_ = wal;
    wals_.erase(wals_.begin(), wals_.lower_bound(wal));
  }
  return Status::OK();
}

void WalSet::Reset() {
  wals_.clear();
  min_wal_number_to_keep_ = 0;
}

Status WalSet::CheckWals(
    Env* env,
    const std::unordered_map<WalNumber, std::string>& logs_on_disk) const {
  assert(env != nullptr);

  Status s;
  for (const auto& wal : wals_) {
    const uint64_t log_number = wal.first;
    const WalMetadata& wal_meta = wal.second;

    if (!wal_meta.HasSyncedSize()) {
      // The WAL and WAL directory is not even synced,
      // so the WAL's inode may not be persisted,
      // then the WAL might not show up when listing WAL directory.
      continue;
    }

    if (logs_on_disk.find(log_number) == logs_on_disk.end()) {
      std::stringstream ss;
      ss << "Missing WAL with log number: " << log_number << ".";
      s = Status::Corruption(ss.str());
      break;
    }

    uint64_t log_file_size = 0;
    s = env->GetFileSize(logs_on_disk.at(log_number), &log_file_size);
    if (!s.ok()) {
      break;
    }
    if (log_file_size < wal_meta.GetSyncedSizeInBytes()) {
      std::stringstream ss;
      ss << "Size mismatch: WAL (log number: " << log_number
         << ") in MANIFEST is " << wal_meta.GetSyncedSizeInBytes()
         << " bytes , but actually is " << log_file_size << " bytes on disk.";
      s = Status::Corruption(ss.str());
      break;
    }
  }

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
