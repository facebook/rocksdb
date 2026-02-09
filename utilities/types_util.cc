//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/types_util.h"

#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {

Status GetInternalKeyForSeek(const Slice& user_key,
                             const Comparator* comparator, std::string* buf) {
  if (!comparator) {
    return Status::InvalidArgument(
        "Constructing an internal key requires user key comparator.");
  }
  size_t ts_sz = comparator->timestamp_size();
  Slice max_ts = comparator->GetMaxTimestamp();
  if (ts_sz != max_ts.size()) {
    return Status::InvalidArgument(
        "The maximum timestamp returned by Comparator::GetMaxTimestamp is "
        "invalid.");
  }
  buf->reserve(user_key.size() + ts_sz + kNumInternalBytes);
  buf->assign(user_key.data(), user_key.size());
  if (ts_sz) {
    buf->append(max_ts.data(), max_ts.size());
  }
  PutFixed64(buf, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
  return Status::OK();
}

Status GetInternalKeyForSeekForPrev(const Slice& user_key,
                                    const Comparator* comparator,
                                    std::string* buf) {
  if (!comparator) {
    return Status::InvalidArgument(
        "Constructing an internal key requires user key comparator.");
  }
  size_t ts_sz = comparator->timestamp_size();
  Slice min_ts = comparator->GetMinTimestamp();
  if (ts_sz != min_ts.size()) {
    return Status::InvalidArgument(
        "The minimum timestamp returned by Comparator::GetMinTimestamp is "
        "invalid.");
  }
  buf->reserve(user_key.size() + ts_sz + kNumInternalBytes);
  buf->assign(user_key.data(), user_key.size());
  if (ts_sz) {
    buf->append(min_ts.data(), min_ts.size());
  }
  PutFixed64(buf, PackSequenceAndType(0, kValueTypeForSeekForPrev));
  return Status::OK();
}

Status ParseEntry(const Slice& internal_key, const Comparator* comparator,
                  ParsedEntryInfo* parsed_entry) {
  if (internal_key.size() < kNumInternalBytes) {
    return Status::InvalidArgument("Internal key size invalid.");
  }
  if (!comparator) {
    return Status::InvalidArgument(
        "Parsing an internal key requires user key comparator.");
  }
  ParsedInternalKey pikey;
  Status status = ParseInternalKey(internal_key, &pikey, /*log_err_key=*/false);
  if (!status.ok()) {
    return status;
  }

  size_t ts_sz = comparator->timestamp_size();
  if (pikey.user_key.size() < ts_sz) {
    return Status::InvalidArgument("User key(with timestamp) size invalid.");
  }
  if (ts_sz == 0) {
    parsed_entry->user_key = pikey.user_key;
  } else {
    parsed_entry->user_key = StripTimestampFromUserKey(pikey.user_key, ts_sz);
    parsed_entry->timestamp =
        ExtractTimestampFromUserKey(pikey.user_key, ts_sz);
  }
  parsed_entry->sequence = pikey.sequence;
  parsed_entry->type = ROCKSDB_NAMESPACE::GetEntryType(pikey.type);
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
