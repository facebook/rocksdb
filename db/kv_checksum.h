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
//
// The `ProtectionInfo.*` classes are templated on the integer type used to hold
// the XOR of hashes for each field. When the integer type is narrower than the
// hash values, we lop off the most significant bits to make it fit.

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

template <typename T>
class ProtectionInfo {
 public:
  ProtectionInfo<T>() = default;

  Status GetStatus() const;
  ProtectionInfoKVOT<T> ProtectKVOT(uint64_t key_checksum,
                                    uint64_t value_checksum, ValueType op_type,
                                    uint64_t ts_checksum) const;

 private:
  friend class ProtectionInfoKVOT<T>;
  friend class ProtectionInfoKVOTS<T>;
  friend class ProtectionInfoKVOTC<T>;

  ProtectionInfo<T>(T val) : val_(val) {
    static_assert(sizeof(ProtectionInfo<T>) == sizeof(T));
  }

  T GetVal() const {
    return val_;
  }
  void SetVal(T val) {
    val_ = val;
  }

  T val_ = 0;
};

template <typename T>
class ProtectionInfoKVOT {
 public:
  ProtectionInfoKVOT<T>() = default;

  ProtectionInfo<T> StripKVOT(uint64_t key_checksum, uint64_t value_checksum,
                              ValueType op_type, uint64_t ts_checksum) const;

  ProtectionInfoKVOTC<T> ProtectC(ColumnFamilyId column_family_id) const;
  ProtectionInfoKVOTS<T> ProtectS(SequenceNumber sequence_number) const;

  void UpdateK(uint64_t old_key_checksum, uint64_t new_key_checksum);
  void UpdateV(uint64_t old_value_checksum, uint64_t new_value_checksum);
  void UpdateO(ValueType old_op_type, ValueType new_op_type);
  void UpdateT(uint64_t old_ts_checksum, uint64_t new_ts_checksum);

 private:
  friend class ProtectionInfo<T>;
  friend class ProtectionInfoKVOTS<T>;
  friend class ProtectionInfoKVOTC<T>;

  ProtectionInfoKVOT<T>(T val) : info_(val) {
    static_assert(sizeof(ProtectionInfoKVOT<T>) == sizeof(T));
  }

  T GetVal() const {
    return info_.GetVal();
  }
  void SetVal(T val) {
    info_.SetVal(val);
  }

  ProtectionInfo<T> info_;
};

template <typename T>
class ProtectionInfoKVOTC {
 public:
  ProtectionInfoKVOTC<T>() = default;

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

 private:
  friend class ProtectionInfoKVOT<T>;

  ProtectionInfoKVOTC<T>(T val) : kvot_(val) {
    static_assert(sizeof(ProtectionInfoKVOTC<T>) == sizeof(T));
  }

  T GetVal() const {
    return kvot_.GetVal();
  }
  void SetVal(T val) {
    kvot_.SetVal(val);
  }

  ProtectionInfoKVOT<T> kvot_;
};

template <typename T>
class ProtectionInfoKVOTS {
 public:
  ProtectionInfoKVOTS<T>() = default;

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

 private:
  friend class ProtectionInfoKVOT<T>;

  ProtectionInfoKVOTS<T>(T val) : kvot_(val) {
    static_assert(sizeof(ProtectionInfoKVOTS<T>) == sizeof(T));
  }

  T GetVal() const {
    return kvot_.GetVal();
  }
  void SetVal(T val) {
    kvot_.SetVal(val);
  }

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
  T val = GetVal();
  val = val ^ static_cast<T>(key_checksum);
  val = val ^ static_cast<T>(value_checksum);
  val = val ^ static_cast<T>(op_type);
  val = val ^ static_cast<T>(ts_checksum);
  return ProtectionInfoKVOT<T>(val);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateK(uint64_t old_key_checksum,
                                    uint64_t new_key_checksum) {
  T val = GetVal();
  val = val ^ static_cast<T>(old_key_checksum);
  val = val ^ static_cast<T>(new_key_checksum);
  SetVal(val);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateV(uint64_t old_value_checksum,
                                    uint64_t new_value_checksum) {
  T val = GetVal();
  val = val ^ static_cast<T>(old_value_checksum);
  val = val ^ static_cast<T>(new_value_checksum);
  SetVal(val);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateO(ValueType old_op_type,
                                    ValueType new_op_type) {
  T val = GetVal();
  val = val ^ static_cast<T>(old_op_type);
  val = val ^ static_cast<T>(new_op_type);
  SetVal(val);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateT(uint64_t old_ts_checksum,
                                    uint64_t new_ts_checksum) {
  T val = GetVal();
  val = val ^ static_cast<T>(old_ts_checksum);
  val = val ^ static_cast<T>(new_ts_checksum);
  SetVal(val);
}

template <typename T>
ProtectionInfo<T> ProtectionInfoKVOT<T>::StripKVOT(uint64_t key_checksum,
                                                   uint64_t value_checksum,
                                                   ValueType op_type,
                                                   uint64_t ts_checksum) const {
  T val = GetVal();
  val = val ^ static_cast<T>(key_checksum);
  val = val ^ static_cast<T>(value_checksum);
  val = val ^ static_cast<T>(op_type);
  val = val ^ static_cast<T>(ts_checksum);
  return ProtectionInfo<T>(val);
}

template <typename T>
ProtectionInfoKVOTC<T> ProtectionInfoKVOT<T>::ProtectC(
    ColumnFamilyId column_family_id) const {
  T val = GetVal();
  val = val ^ static_cast<T>(column_family_id);
  return ProtectionInfoKVOTC<T>(val);
}

template <typename T>
ProtectionInfoKVOT<T> ProtectionInfoKVOTC<T>::StripC(
    ColumnFamilyId column_family_id) const {
  T val = GetVal();
  val = val ^ static_cast<T>(column_family_id);
  return ProtectionInfoKVOT<T>(val);
}

template <typename T>
void ProtectionInfoKVOTC<T>::UpdateC(ColumnFamilyId old_column_family_id,
                                     ColumnFamilyId new_column_family_id) {
  T val = GetVal();
  val = val ^ static_cast<T>(old_column_family_id);
  val = val ^ static_cast<T>(new_column_family_id);
  SetVal(val);
}

template <typename T>
ProtectionInfoKVOTS<T> ProtectionInfoKVOT<T>::ProtectS(
    SequenceNumber sequence_number) const {
  T val = GetVal();
  val = val ^ static_cast<T>(sequence_number);
  return ProtectionInfoKVOTS<T>(val);
}

template <typename T>
ProtectionInfoKVOT<T> ProtectionInfoKVOTS<T>::StripS(
    SequenceNumber sequence_number) const {
  T val = GetVal();
  val = val ^ static_cast<T>(sequence_number);
  return ProtectionInfoKVOT<T>(val);
}

template <typename T>
void ProtectionInfoKVOTS<T>::UpdateS(SequenceNumber old_sequence_number,
                                     SequenceNumber new_sequence_number) {
  T val = GetVal();
  val = val ^ static_cast<T>(old_sequence_number);
  val = val ^ static_cast<T>(new_sequence_number);
  SetVal(val);
}

}  // namespace ROCKSDB_NAMESPACE
