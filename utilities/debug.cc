//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/debug.h"

#include "db/db_impl.h"

namespace rocksdb {

Status GetAllKeyVersions(DB* db, Slice begin_key, Slice end_key,
                         std::vector<KeyVersion>* key_versions) {
  assert(key_versions != nullptr);
  key_versions->clear();

  DBImpl* idb = dynamic_cast<DBImpl*>(db->GetRootDB());
  auto icmp = InternalKeyComparator(idb->GetOptions().comparator);
  RangeDelAggregator range_del_agg(icmp, {} /* snapshots */);
  Arena arena;
  ScopedArenaIterator iter(idb->NewInternalIterator(&arena, &range_del_agg));

  if (!begin_key.empty()) {
    InternalKey ikey;
    ikey.SetMaxPossibleForUserKey(begin_key);
    iter->Seek(ikey.Encode());
  } else {
    iter->SeekToFirst();
  }

  std::vector<KeyVersion> res;
  for (; iter->Valid(); iter->Next()) {
    ParsedInternalKey ikey;
    if (!ParseInternalKey(iter->key(), &ikey)) {
      return Status::Corruption("Internal Key [" + iter->key().ToString() +
                                "] parse error!");
    }

    if (!end_key.empty() &&
        icmp.user_comparator()->Compare(ikey.user_key, end_key) >= 0) {
      break;
    }

    key_versions->emplace_back();
    key_versions->back().user_key = ikey.user_key.ToString();
    key_versions->back().value = iter->value().ToString();
    key_versions->back().sequence = ikey.sequence;
    key_versions->back().type = static_cast<int>(ikey.type);
  }
  return Status::OK();
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
