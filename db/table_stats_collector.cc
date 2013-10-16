//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/table_stats_collector.h"

#include "db/dbformat.h"
#include "util/coding.h"

namespace rocksdb {

Status InternalKeyStatsCollector::Add(const Slice& key, const Slice& value) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    return Status::InvalidArgument("Invalid internal key");
  }

  if (ikey.type == ValueType::kTypeDeletion) {
    ++deleted_keys_;
  }

  return Status::OK();
}

Status InternalKeyStatsCollector::Finish(
    TableStats::UserCollectedStats* stats) {
  assert(stats);
  assert(stats->find(InternalKeyTableStatsNames::kDeletedKeys) == stats->end());
  std::string val;

  PutVarint64(&val, deleted_keys_);
  stats->insert(std::make_pair(InternalKeyTableStatsNames::kDeletedKeys, val));

  return Status::OK();
}

Status UserKeyTableStatsCollector::Add(const Slice& key, const Slice& value) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    return Status::InvalidArgument("Invalid internal key");
  }

  return collector_->Add(ikey.user_key, value);
}

Status UserKeyTableStatsCollector::Finish(
    TableStats::UserCollectedStats* stats) {
  return collector_->Finish(stats);
}

const std::string InternalKeyTableStatsNames::kDeletedKeys
  = "rocksdb.deleted.keys";

}  // namespace rocksdb
