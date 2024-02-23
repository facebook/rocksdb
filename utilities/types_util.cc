//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/types_util.h"

#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {

Status ParseEntry(const Slice& internal_key, ParsedEntryInfo* parsed_entry) {
  if (internal_key.size() < kNumInternalBytes) {
    return Status::InvalidArgument("Internal key size invalid.");
  }
  ParsedInternalKey pikey;
  Status status = ParseInternalKey(internal_key, &pikey, /*log_err_key=*/false);
  if (!status.ok()) {
    return status;
  }

  parsed_entry->user_key = pikey.user_key;
  parsed_entry->sequence = pikey.sequence;
  parsed_entry->type = ROCKSDB_NAMESPACE::GetEntryType(pikey.type);
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
