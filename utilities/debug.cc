//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).


#include "rocksdb/utilities/debug.h"

#include "db/db_impl/db_impl.h"
#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE {

static std::unordered_map<std::string, ValueType> value_type_string_map = {
    {"TypeDeletion", ValueType::kTypeDeletion},
    {"TypeValue", ValueType::kTypeValue},
    {"TypeMerge", ValueType::kTypeMerge},
    {"TypeLogData", ValueType::kTypeLogData},
    {"TypeColumnFamilyDeletion", ValueType::kTypeColumnFamilyDeletion},
    {"TypeColumnFamilyValue", ValueType::kTypeColumnFamilyValue},
    {"TypeColumnFamilyMerge", ValueType::kTypeColumnFamilyMerge},
    {"TypeSingleDeletion", ValueType::kTypeSingleDeletion},
    {"TypeColumnFamilySingleDeletion",
     ValueType::kTypeColumnFamilySingleDeletion},
    {"TypeBeginPrepareXID", ValueType::kTypeBeginPrepareXID},
    {"TypeEndPrepareXID", ValueType::kTypeEndPrepareXID},
    {"TypeCommitXID", ValueType::kTypeCommitXID},
    {"TypeRollbackXID", ValueType::kTypeRollbackXID},
    {"TypeNoop", ValueType::kTypeNoop},
    {"TypeColumnFamilyRangeDeletion",
     ValueType::kTypeColumnFamilyRangeDeletion},
    {"TypeRangeDeletion", ValueType::kTypeRangeDeletion},
    {"TypeColumnFamilyBlobIndex", ValueType::kTypeColumnFamilyBlobIndex},
    {"TypeBlobIndex", ValueType::kTypeBlobIndex},
    {"TypeBeginPersistedPrepareXID", ValueType::kTypeBeginPersistedPrepareXID},
    {"TypeBeginUnprepareXID", ValueType::kTypeBeginUnprepareXID},
    {"TypeDeletionWithTimestamp", ValueType::kTypeDeletionWithTimestamp},
    {"TypeCommitXIDAndTimestamp", ValueType::kTypeCommitXIDAndTimestamp},
    {"TypeWideColumnEntity", ValueType::kTypeWideColumnEntity},
    {"TypeColumnFamilyWideColumnEntity",
     ValueType::kTypeColumnFamilyWideColumnEntity},
    {"TypeValuePreferredSeqno", ValueType::kTypeValuePreferredSeqno},
    {"TypeColumnFamilyValuePreferredSeqno",
     ValueType::kTypeColumnFamilyValuePreferredSeqno},
};

std::string KeyVersion::GetTypeName() const {
  std::string type_name;
  if (SerializeEnum<ValueType>(value_type_string_map,
                               static_cast<ValueType>(type), &type_name)) {
    return type_name;
  } else {
    return "Invalid";
  }
}

Status GetAllKeyVersions(DB* db, Slice begin_key, Slice end_key,
                         size_t max_num_ikeys,
                         std::vector<KeyVersion>* key_versions) {
  if (nullptr == db) {
    return Status::InvalidArgument("db cannot be null.");
  }
  return GetAllKeyVersions(db, db->DefaultColumnFamily(), begin_key, end_key,
                           max_num_ikeys, key_versions);
}

Status GetAllKeyVersions(DB* db, ColumnFamilyHandle* cfh, Slice begin_key,
                         Slice end_key, size_t max_num_ikeys,
                         std::vector<KeyVersion>* key_versions) {
  if (nullptr == db) {
    return Status::InvalidArgument("db cannot be null.");
  }
  if (nullptr == cfh) {
    return Status::InvalidArgument("Column family handle cannot be null.");
  }
  if (nullptr == key_versions) {
    return Status::InvalidArgument("key_versions cannot be null.");
  }
  key_versions->clear();

  DBImpl* idb = static_cast<DBImpl*>(db->GetRootDB());
  auto icmp = InternalKeyComparator(idb->GetOptions(cfh).comparator);
  ReadOptions read_options;
  Arena arena;
  ScopedArenaPtr<InternalIterator> iter(
      idb->NewInternalIterator(read_options, &arena, kMaxSequenceNumber, cfh));

  const Comparator* ucmp = icmp.user_comparator();
  size_t ts_sz = ucmp->timestamp_size();

  Slice from_slice = begin_key;
  bool has_begin = !begin_key.empty();
  Slice end_slice = end_key;
  bool has_end = !end_key.empty();
  std::string begin_key_buf, end_key_buf;
  auto [from, end] = MaybeAddTimestampsToRange(
      has_begin ? &from_slice : nullptr, has_end ? &end_slice : nullptr, ts_sz,
      &begin_key_buf, &end_key_buf);
  if (has_begin) {
    assert(from.has_value());
    InternalKey ikey;
    ikey.SetMinPossibleForUserKey(from.value());
    iter->Seek(ikey.Encode());
  } else {
    iter->SeekToFirst();
  }

  size_t num_keys = 0;
  for (; iter->Valid(); iter->Next()) {
    ParsedInternalKey ikey;
    Status pik_status =
        ParseInternalKey(iter->key(), &ikey, true /* log_err_key */);  // TODO
    if (!pik_status.ok()) {
      return pik_status;
    }

    if (has_end && end.has_value() &&
        icmp.user_comparator()->Compare(ikey.user_key, end.value()) > 0) {
      break;
    }

    key_versions->emplace_back(ikey.user_key.ToString() /* _user_key */,
                               iter->value().ToString() /* _value */,
                               ikey.sequence /* _sequence */,
                               static_cast<int>(ikey.type) /* _type */);
    if (++num_keys >= max_num_ikeys) {
      break;
    }
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE

