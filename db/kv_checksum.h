//  Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file contains classes containing fields to protect individual entries.
// The classes are named "ProtectionInfo<suffix>", where <suffix> indicates the
// combination of fields that are covered. Each field has a single letter
// abbreviation as follows.
//
// K = key
// V = value
// O = optype aka value type
// T = timestamp
// S = seqno
// C = CF ID
//
// Then, for example, a class that protects an entry consisting of key, value,
// optype, timestamp, and CF ID (i.e., a `WriteBatch` entry) would be named
// `ProtectionInfoKVOTC`.

#pragma once

#include <type_traits>

#include "db/dbformat.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

template <typename T>
struct ProtectionInfo;
template <typename T>
struct ProtectionInfoKVOT;
template <typename T>
struct ProtectionInfoKVOTC;
template <typename T>
struct ProtectionInfoKVOTS;

// Aliases for eight-byte ("quadword") protection infos.
typedef ProtectionInfo<uint64_t> QwordProtectionInfo;
typedef ProtectionInfoKVOT<uint64_t> QwordProtectionInfoKVOT;
typedef ProtectionInfoKVOTC<uint64_t> QwordProtectionInfoKVOTC;
typedef ProtectionInfoKVOTS<uint64_t> QwordProtectionInfoKVOTS;

// T is the type of the unsigned integer where protection info will be stored.
template <typename T>
struct ProtectionInfo {
  ProtectionInfo() {
    // Standard-layout of the `ProtectionInfo.*` classes guarantees pointer-
    // interconveritibility. The first member of each `ProtectionInfo.+` class
    // is a stripped-down object. The first member of `ProtectionInfo` is the
    // integrity protection array. Given all this, we can use `reinterpret_cast`
    // to safely reinterpret the type of any `ProtectionInfo.*` pointer, or even
    // convert any such pointers to a pointer to the integrity protection array.
    static_assert(std::is_standard_layout<ProtectionInfo<T>>::value, "");
  }

  Status GetStatus() const;
  ProtectionInfoKVOT<T> ProtectKVOT(uint64_t key_checksum,
                                    uint64_t value_checksum, ValueType op_type,
                                    uint64_t ts_checksum) const;

  T val_ = 0;
};

template <typename T>
struct ProtectionInfoKVOT {
  ProtectionInfoKVOT() {
    static_assert(std::is_standard_layout<ProtectionInfoKVOT<T>>::value, "");
  }

  ProtectionInfo<T> StripKVOT(uint64_t key_checksum, uint64_t value_checksum,
                              ValueType op_type, uint64_t ts_checksum) const;

  ProtectionInfoKVOTC<T> ProtectC(ColumnFamilyId column_family_id) const;
  ProtectionInfoKVOTS<T> ProtectS(SequenceNumber sequence_number) const;

  void UpdateK(uint64_t old_key_checksum, uint64_t new_key_checksum);
  void UpdateV(uint64_t old_value_checksum, uint64_t new_value_checksum);
  void UpdateO(ValueType old_op_type, ValueType new_op_type);
  void UpdateT(uint64_t old_ts_checksum, uint64_t new_ts_checksum);

  ProtectionInfo<T> info_;
};

template <typename T>
struct ProtectionInfoKVOTC {
  ProtectionInfoKVOTC() {
    static_assert(std::is_standard_layout<ProtectionInfoKVOTC<T>>::value, "");
  }

  ProtectionInfoKVOT<T> StripC(ColumnFamilyId column_family_id) const;

  void UpdateK(uint64_t old_key_checksum, uint64_t new_key_checksum) {
    kvot_.UpdateK(old_key_checksum, new_key_checksum);
  }
  void UpdateV(uint64_t old_value_checksum, uint64_t new_value_checksum) {
    kvot_.UpdateV(old_value_checksum, new_value_checksum);
  }
  void UpdateO(ValueType old_op_type, ValueType new_op_type) {
    kvot_.UpdateO(old_op_type, new_op_type);
  }
  void UpdateT(uint64_t old_ts_checksum, uint64_t new_ts_checksum) {
    kvot_.UpdateT(old_ts_checksum, new_ts_checksum);
  }
  void UpdateC(ColumnFamilyId old_column_family_id,
               ColumnFamilyId new_column_family_id);

  ProtectionInfoKVOT<T> kvot_;
};

template <typename T>
struct ProtectionInfoKVOTS {
  ProtectionInfoKVOTS() {
    static_assert(std::is_standard_layout<ProtectionInfoKVOTS<T>>::value, "");
  }

  ProtectionInfoKVOT<T> StripS(SequenceNumber sequence_number) const;

  void UpdateK(uint64_t old_key_checksum, uint64_t new_key_checksum) {
    kvot_.UpdateK(old_key_checksum, new_key_checksum);
  }
  void UpdateV(uint64_t old_value_checksum, uint64_t new_value_checksum) {
    kvot_.UpdateV(old_value_checksum, new_value_checksum);
  }
  void UpdateO(ValueType old_op_type, ValueType new_op_type) {
    kvot_.UpdateO(old_op_type, new_op_type);
  }
  void UpdateT(uint64_t old_ts_checksum, uint64_t new_ts_checksum) {
    kvot_.UpdateT(old_ts_checksum, new_ts_checksum);
  }
  void UpdateS(SequenceNumber old_sequence_number,
               SequenceNumber new_sequence_number);

  ProtectionInfoKVOT<T> kvot_;
};

template <typename T>
Status ProtectionInfo<T>::GetStatus() const {
  if (val_ != 0) {
    return Status::Corruption("ProtectionInfo mismatch");
  }
  return Status::OK();
}

template <typename T>
ProtectionInfoKVOT<T> ProtectionInfo<T>::ProtectKVOT(
    uint64_t key_checksum, uint64_t value_checksum, ValueType op_type,
    uint64_t ts_checksum) const {
  ProtectionInfoKVOT<T> res(
      *reinterpret_cast<const ProtectionInfoKVOT<T>*>(this));
  T* val_ptr = reinterpret_cast<T*>(&res);
  *val_ptr = *val_ptr ^ static_cast<T>(key_checksum);
  *val_ptr = *val_ptr ^ static_cast<T>(value_checksum);
  *val_ptr = *val_ptr ^ static_cast<T>(op_type);
  *val_ptr = *val_ptr ^ static_cast<T>(ts_checksum);
  return res;
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateK(uint64_t old_key_checksum,
                                    uint64_t new_key_checksum) {
  T* val_ptr = reinterpret_cast<T*>(this);
  *val_ptr = *val_ptr ^ static_cast<T>(old_key_checksum);
  *val_ptr = *val_ptr ^ static_cast<T>(new_key_checksum);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateV(uint64_t old_value_checksum,
                                    uint64_t new_value_checksum) {
  T* val_ptr = reinterpret_cast<T*>(this);
  *val_ptr = *val_ptr ^ static_cast<T>(old_value_checksum);
  *val_ptr = *val_ptr ^ static_cast<T>(new_value_checksum);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateO(ValueType old_op_type,
                                    ValueType new_op_type) {
  T* val_ptr = reinterpret_cast<T*>(this);
  *val_ptr = *val_ptr ^ static_cast<T>(old_op_type);
  *val_ptr = *val_ptr ^ static_cast<T>(new_op_type);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateT(uint64_t old_ts_checksum,
                                    uint64_t new_ts_checksum) {
  T* val_ptr = reinterpret_cast<T*>(this);
  *val_ptr = *val_ptr ^ static_cast<T>(old_ts_checksum);
  *val_ptr = *val_ptr ^ static_cast<T>(new_ts_checksum);
}

template <typename T>
ProtectionInfo<T> ProtectionInfoKVOT<T>::StripKVOT(uint64_t key_checksum,
                                                   uint64_t value_checksum,
                                                   ValueType op_type,
                                                   uint64_t ts_checksum) const {
  ProtectionInfo<T> res(*reinterpret_cast<const ProtectionInfo<T>*>(this));
  T* val_ptr = reinterpret_cast<T*>(&res);
  *val_ptr = *val_ptr ^ static_cast<T>(key_checksum);
  *val_ptr = *val_ptr ^ static_cast<T>(value_checksum);
  *val_ptr = *val_ptr ^ static_cast<T>(op_type);
  *val_ptr = *val_ptr ^ static_cast<T>(ts_checksum);
  return res;
}

template <typename T>
ProtectionInfoKVOTC<T> ProtectionInfoKVOT<T>::ProtectC(
    ColumnFamilyId column_family_id) const {
  ProtectionInfoKVOTC<T> res(
      *reinterpret_cast<const ProtectionInfoKVOTC<T>*>(this));
  T* val_ptr = reinterpret_cast<T*>(&res);
  *val_ptr = *val_ptr ^ static_cast<T>(column_family_id);
  return res;
}

template <typename T>
ProtectionInfoKVOT<T> ProtectionInfoKVOTC<T>::StripC(
    ColumnFamilyId column_family_id) const {
  ProtectionInfoKVOT<T> res(
      *reinterpret_cast<const ProtectionInfoKVOT<T>*>(this));
  T* val_ptr = reinterpret_cast<T*>(&res);
  *val_ptr = *val_ptr ^ static_cast<T>(column_family_id);
  return res;
}

template <typename T>
void ProtectionInfoKVOTC<T>::UpdateC(ColumnFamilyId old_column_family_id,
                                     ColumnFamilyId new_column_family_id) {
  T* val_ptr = reinterpret_cast<T*>(this);
  *val_ptr = *val_ptr ^ static_cast<T>(old_column_family_id);
  *val_ptr = *val_ptr ^ static_cast<T>(new_column_family_id);
}

template <typename T>
ProtectionInfoKVOTS<T> ProtectionInfoKVOT<T>::ProtectS(
    SequenceNumber sequence_number) const {
  ProtectionInfoKVOTS<T> res(
      *reinterpret_cast<const ProtectionInfoKVOTS<T>*>(this));
  T* val_ptr = reinterpret_cast<T*>(&res);
  *val_ptr = *val_ptr ^ static_cast<T>(sequence_number);
  return res;
}

template <typename T>
ProtectionInfoKVOT<T> ProtectionInfoKVOTS<T>::StripS(
    SequenceNumber sequence_number) const {
  ProtectionInfoKVOT<T> res(
      *reinterpret_cast<const ProtectionInfoKVOT<T>*>(this));
  T* val_ptr = reinterpret_cast<T*>(&res);
  *val_ptr = *val_ptr ^ static_cast<T>(sequence_number);
  return res;
}

template <typename T>
void ProtectionInfoKVOTS<T>::UpdateS(SequenceNumber old_sequence_number,
                                     SequenceNumber new_sequence_number) {
  T* val_ptr = reinterpret_cast<T*>(this);
  *val_ptr = *val_ptr ^ static_cast<T>(old_sequence_number);
  *val_ptr = *val_ptr ^ static_cast<T>(new_sequence_number);
}

}  // namespace ROCKSDB_NAMESPACE
