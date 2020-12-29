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
// the XOR of hashes for each field. Only unsigned integer types are supported,
// and the maximum supported integer width is 64 bits. When the integer type is
// narrower than the hash values, we lop off the most significant bits to make
// them fit.
//
// The `ProtectionInfo.*` classes are all intended to be non-persistent. We do
// not currently make the byte order consistent for integer fields before
// hashing them, so the resulting values are endianness-dependent.

#pragma once

#include <type_traits>

#include "db/dbformat.h"
#include "rocksdb/types.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

template <typename T>
class ProtectionInfo;
template <typename T>
class ProtectionInfoKVOT;
template <typename T>
class ProtectionInfoKVOTC;
template <typename T>
class ProtectionInfoKVOTS;

// Aliases for 64-bit protection infos.
typedef ProtectionInfo<uint64_t> ProtectionInfo64;
typedef ProtectionInfoKVOT<uint64_t> ProtectionInfoKVOT64;
typedef ProtectionInfoKVOTC<uint64_t> ProtectionInfoKVOTC64;
typedef ProtectionInfoKVOTS<uint64_t> ProtectionInfoKVOTS64;

template <typename T>
class ProtectionInfo {
 public:
  ProtectionInfo<T>() = default;

  Status GetStatus() const;
  ProtectionInfoKVOT<T> ProtectKVOT(const Slice& key, const Slice& value,
                                    ValueType op_type,
                                    const Slice& timestamp) const;
  ProtectionInfoKVOT<T> ProtectKVOT(const SliceParts& key,
                                    const SliceParts& value, ValueType op_type,
                                    const Slice& timestamp) const;

 private:
  friend class ProtectionInfoKVOT<T>;
  friend class ProtectionInfoKVOTS<T>;
  friend class ProtectionInfoKVOTC<T>;

  // Each field is hashed with an independent value so we can catch fields being
  // swapped. Per the `NPHash64()` docs, using consecutive seeds is a pitfall,
  // and we should instead vary our seeds by a large odd number. This value by
  // which we increment (0xD28AAD72F49BD50B) was taken from
  // `head -c8 /dev/urandom | hexdump`, run repeatedly until it yielded an odd
  // number. The values are computed manually since the Windows C++ compiler
  // complains about the overflow when adding constants.
  static const uint64_t kSeedK = 0;
  static const uint64_t kSeedV = 0xD28AAD72F49BD50B;
  static const uint64_t kSeedO = 0xA5155AE5E937AA16;
  static const uint64_t kSeedT = 0x77A00858DDD37F21;
  static const uint64_t kSeedS = 0x4A2AB5CBD26F542C;
  static const uint64_t kSeedC = 0x1CB5633EC70B2937;

  ProtectionInfo<T>(T val) : val_(val) {
    static_assert(sizeof(ProtectionInfo<T>) == sizeof(T), "");
  }

  T GetVal() const { return val_; }
  void SetVal(T val) { val_ = val; }

  T val_ = 0;
};

template <typename T>
class ProtectionInfoKVOT {
 public:
  ProtectionInfoKVOT<T>() = default;

  ProtectionInfo<T> StripKVOT(const Slice& key, const Slice& value,
                              ValueType op_type, const Slice& timestamp) const;
  ProtectionInfo<T> StripKVOT(const SliceParts& key, const SliceParts& value,
                              ValueType op_type, const Slice& timestamp) const;

  ProtectionInfoKVOTC<T> ProtectC(ColumnFamilyId column_family_id) const;
  ProtectionInfoKVOTS<T> ProtectS(SequenceNumber sequence_number) const;

  void UpdateK(const Slice& old_key, const Slice& new_key);
  void UpdateK(const SliceParts& old_key, const SliceParts& new_key);
  void UpdateV(const Slice& old_value, const Slice& new_value);
  void UpdateV(const SliceParts& old_value, const SliceParts& new_value);
  void UpdateO(ValueType old_op_type, ValueType new_op_type);
  void UpdateT(const Slice& old_timestamp, const Slice& new_timestamp);

 private:
  friend class ProtectionInfo<T>;
  friend class ProtectionInfoKVOTS<T>;
  friend class ProtectionInfoKVOTC<T>;

  ProtectionInfoKVOT<T>(T val) : info_(val) {
    static_assert(sizeof(ProtectionInfoKVOT<T>) == sizeof(T), "");
  }

  T GetVal() const { return info_.GetVal(); }
  void SetVal(T val) { info_.SetVal(val); }

  ProtectionInfo<T> info_;
};

template <typename T>
class ProtectionInfoKVOTC {
 public:
  ProtectionInfoKVOTC<T>() = default;

  ProtectionInfoKVOT<T> StripC(ColumnFamilyId column_family_id) const;

  void UpdateK(const Slice& old_key, const Slice& new_key) {
    kvot_.UpdateK(old_key, new_key);
  }
  void UpdateK(const SliceParts& old_key, const SliceParts& new_key) {
    kvot_.UpdateK(old_key, new_key);
  }
  void UpdateV(const Slice& old_value, const Slice& new_value) {
    kvot_.UpdateV(old_value, new_value);
  }
  void UpdateV(const SliceParts& old_value, const SliceParts& new_value) {
    kvot_.UpdateV(old_value, new_value);
  }
  void UpdateO(ValueType old_op_type, ValueType new_op_type) {
    kvot_.UpdateO(old_op_type, new_op_type);
  }
  void UpdateT(const Slice& old_timestamp, const Slice& new_timestamp) {
    kvot_.UpdateT(old_timestamp, new_timestamp);
  }
  void UpdateC(ColumnFamilyId old_column_family_id,
               ColumnFamilyId new_column_family_id);

 private:
  friend class ProtectionInfoKVOT<T>;

  ProtectionInfoKVOTC<T>(T val) : kvot_(val) {
    static_assert(sizeof(ProtectionInfoKVOTC<T>) == sizeof(T), "");
  }

  T GetVal() const { return kvot_.GetVal(); }
  void SetVal(T val) { kvot_.SetVal(val); }

  ProtectionInfoKVOT<T> kvot_;
};

template <typename T>
class ProtectionInfoKVOTS {
 public:
  ProtectionInfoKVOTS<T>() = default;

  ProtectionInfoKVOT<T> StripS(SequenceNumber sequence_number) const;

  void UpdateK(const Slice& old_key, const Slice& new_key) {
    kvot_.UpdateK(old_key, new_key);
  }
  void UpdateK(const SliceParts& old_key, const SliceParts& new_key) {
    kvot_.UpdateK(old_key, new_key);
  }
  void UpdateV(const Slice& old_value, const Slice& new_value) {
    kvot_.UpdateV(old_value, new_value);
  }
  void UpdateV(const SliceParts& old_value, const SliceParts& new_value) {
    kvot_.UpdateV(old_value, new_value);
  }
  void UpdateO(ValueType old_op_type, ValueType new_op_type) {
    kvot_.UpdateO(old_op_type, new_op_type);
  }
  void UpdateT(const Slice& old_timestamp, const Slice& new_timestamp) {
    kvot_.UpdateT(old_timestamp, new_timestamp);
  }
  void UpdateS(SequenceNumber old_sequence_number,
               SequenceNumber new_sequence_number);

 private:
  friend class ProtectionInfoKVOT<T>;

  ProtectionInfoKVOTS<T>(T val) : kvot_(val) {
    static_assert(sizeof(ProtectionInfoKVOTS<T>) == sizeof(T), "");
  }

  T GetVal() const { return kvot_.GetVal(); }
  void SetVal(T val) { kvot_.SetVal(val); }

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
    const Slice& key, const Slice& value, ValueType op_type,
    const Slice& timestamp) const {
  T val = GetVal();
  val = val ^ static_cast<T>(GetSliceNPHash64(key, ProtectionInfo<T>::kSeedK));
  val =
      val ^ static_cast<T>(GetSliceNPHash64(value, ProtectionInfo<T>::kSeedV));
  val = val ^
        static_cast<T>(NPHash64(reinterpret_cast<char*>(&op_type),
                                sizeof(op_type), ProtectionInfo<T>::kSeedO));
  val = val ^
        static_cast<T>(GetSliceNPHash64(timestamp, ProtectionInfo<T>::kSeedT));
  return ProtectionInfoKVOT<T>(val);
}

template <typename T>
ProtectionInfoKVOT<T> ProtectionInfo<T>::ProtectKVOT(
    const SliceParts& key, const SliceParts& value, ValueType op_type,
    const Slice& timestamp) const {
  T val = GetVal();
  val = val ^
        static_cast<T>(GetSlicePartsNPHash64(key, ProtectionInfo<T>::kSeedK));
  val = val ^
        static_cast<T>(GetSlicePartsNPHash64(value, ProtectionInfo<T>::kSeedV));
  val = val ^
        static_cast<T>(NPHash64(reinterpret_cast<char*>(&op_type),
                                sizeof(op_type), ProtectionInfo<T>::kSeedO));
  val = val ^
        static_cast<T>(GetSliceNPHash64(timestamp, ProtectionInfo<T>::kSeedT));
  return ProtectionInfoKVOT<T>(val);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateK(const Slice& old_key,
                                    const Slice& new_key) {
  T val = GetVal();
  val = val ^
        static_cast<T>(GetSliceNPHash64(old_key, ProtectionInfo<T>::kSeedK));
  val = val ^
        static_cast<T>(GetSliceNPHash64(new_key, ProtectionInfo<T>::kSeedK));
  SetVal(val);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateK(const SliceParts& old_key,
                                    const SliceParts& new_key) {
  T val = GetVal();
  val = val ^ static_cast<T>(
                  GetSlicePartsNPHash64(old_key, ProtectionInfo<T>::kSeedK));
  val = val ^ static_cast<T>(
                  GetSlicePartsNPHash64(new_key, ProtectionInfo<T>::kSeedK));
  SetVal(val);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateV(const Slice& old_value,
                                    const Slice& new_value) {
  T val = GetVal();
  val = val ^
        static_cast<T>(GetSliceNPHash64(old_value, ProtectionInfo<T>::kSeedV));
  val = val ^
        static_cast<T>(GetSliceNPHash64(new_value, ProtectionInfo<T>::kSeedV));
  SetVal(val);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateV(const SliceParts& old_value,
                                    const SliceParts& new_value) {
  T val = GetVal();
  val = val ^ static_cast<T>(
                  GetSlicePartsNPHash64(old_value, ProtectionInfo<T>::kSeedV));
  val = val ^ static_cast<T>(
                  GetSlicePartsNPHash64(new_value, ProtectionInfo<T>::kSeedV));
  SetVal(val);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateO(ValueType old_op_type,
                                    ValueType new_op_type) {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(reinterpret_cast<char*>(&old_op_type),
                                      sizeof(old_op_type),
                                      ProtectionInfo<T>::kSeedO));
  val = val ^ static_cast<T>(NPHash64(reinterpret_cast<char*>(&new_op_type),
                                      sizeof(new_op_type),
                                      ProtectionInfo<T>::kSeedO));
  SetVal(val);
}

template <typename T>
void ProtectionInfoKVOT<T>::UpdateT(const Slice& old_timestamp,
                                    const Slice& new_timestamp) {
  T val = GetVal();
  val = val ^ static_cast<T>(
                  GetSliceNPHash64(old_timestamp, ProtectionInfo<T>::kSeedT));
  val = val ^ static_cast<T>(
                  GetSliceNPHash64(new_timestamp, ProtectionInfo<T>::kSeedT));
  SetVal(val);
}

template <typename T>
ProtectionInfo<T> ProtectionInfoKVOT<T>::StripKVOT(
    const Slice& key, const Slice& value, ValueType op_type,
    const Slice& timestamp) const {
  T val = GetVal();
  val = val ^ static_cast<T>(GetSliceNPHash64(key, ProtectionInfo<T>::kSeedK));
  val =
      val ^ static_cast<T>(GetSliceNPHash64(value, ProtectionInfo<T>::kSeedV));
  val = val ^
        static_cast<T>(NPHash64(reinterpret_cast<char*>(&op_type),
                                sizeof(op_type), ProtectionInfo<T>::kSeedO));
  val = val ^
        static_cast<T>(GetSliceNPHash64(timestamp, ProtectionInfo<T>::kSeedT));
  return ProtectionInfo<T>(val);
}

template <typename T>
ProtectionInfo<T> ProtectionInfoKVOT<T>::StripKVOT(
    const SliceParts& key, const SliceParts& value, ValueType op_type,
    const Slice& timestamp) const {
  T val = GetVal();
  val = val ^
        static_cast<T>(GetSlicePartsNPHash64(key, ProtectionInfo<T>::kSeedK));
  val = val ^
        static_cast<T>(GetSlicePartsNPHash64(value, ProtectionInfo<T>::kSeedV));
  val = val ^
        static_cast<T>(NPHash64(reinterpret_cast<char*>(&op_type),
                                sizeof(op_type), ProtectionInfo<T>::kSeedO));
  val = val ^
        static_cast<T>(GetSliceNPHash64(timestamp, ProtectionInfo<T>::kSeedT));
  return ProtectionInfo<T>(val);
}

template <typename T>
ProtectionInfoKVOTC<T> ProtectionInfoKVOT<T>::ProtectC(
    ColumnFamilyId column_family_id) const {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(
                  reinterpret_cast<char*>(&column_family_id),
                  sizeof(column_family_id), ProtectionInfo<T>::kSeedC));
  return ProtectionInfoKVOTC<T>(val);
}

template <typename T>
ProtectionInfoKVOT<T> ProtectionInfoKVOTC<T>::StripC(
    ColumnFamilyId column_family_id) const {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(
                  reinterpret_cast<char*>(&column_family_id),
                  sizeof(column_family_id), ProtectionInfo<T>::kSeedC));
  return ProtectionInfoKVOT<T>(val);
}

template <typename T>
void ProtectionInfoKVOTC<T>::UpdateC(ColumnFamilyId old_column_family_id,
                                     ColumnFamilyId new_column_family_id) {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(
                  reinterpret_cast<char*>(&old_column_family_id),
                  sizeof(old_column_family_id), ProtectionInfo<T>::kSeedC));
  val = val ^ static_cast<T>(NPHash64(
                  reinterpret_cast<char*>(&new_column_family_id),
                  sizeof(new_column_family_id), ProtectionInfo<T>::kSeedC));
  SetVal(val);
}

template <typename T>
ProtectionInfoKVOTS<T> ProtectionInfoKVOT<T>::ProtectS(
    SequenceNumber sequence_number) const {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(reinterpret_cast<char*>(&sequence_number),
                                      sizeof(sequence_number),
                                      ProtectionInfo<T>::kSeedS));
  return ProtectionInfoKVOTS<T>(val);
}

template <typename T>
ProtectionInfoKVOT<T> ProtectionInfoKVOTS<T>::StripS(
    SequenceNumber sequence_number) const {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(reinterpret_cast<char*>(&sequence_number),
                                      sizeof(sequence_number),
                                      ProtectionInfo<T>::kSeedS));
  return ProtectionInfoKVOT<T>(val);
}

template <typename T>
void ProtectionInfoKVOTS<T>::UpdateS(SequenceNumber old_sequence_number,
                                     SequenceNumber new_sequence_number) {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(
                  reinterpret_cast<char*>(&old_sequence_number),
                  sizeof(old_sequence_number), ProtectionInfo<T>::kSeedS));
  val = val ^ static_cast<T>(NPHash64(
                  reinterpret_cast<char*>(&new_sequence_number),
                  sizeof(new_sequence_number), ProtectionInfo<T>::kSeedS));
  SetVal(val);
}

}  // namespace ROCKSDB_NAMESPACE
