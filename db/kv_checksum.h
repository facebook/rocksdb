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

#include "db/dbformat.h"
#include "rocksdb/types.h"

#include <type_traits>

namespace ROCKSDB_NAMESPACE {

template <size_t N>
struct ProtectionInfo;
template <size_t N>
struct ProtectionInfoKVOT;
template <size_t N>
struct ProtectionInfoKVOTC;
template <size_t N>
struct ProtectionInfoKVOTS;

// Aliases for eight-byte ("quadword") protection infos.
typedef ProtectionInfo<8> QwordProtectionInfo;
typedef ProtectionInfoKVOT<8> QwordProtectionInfoKVOT;
typedef ProtectionInfoKVOTC<8> QwordProtectionInfoKVOTC;
typedef ProtectionInfoKVOTS<8> QwordProtectionInfoKVOTS;

// N is the number of bytes into which protection info is folded.
// Currently supported values are 1, 2, 4, and 8.
template <size_t N>
struct ProtectionInfo {
  ProtectionInfo() {
    // Standard-layout of the `ProtectionInfo.*` classes guarantees pointer-
    // interconveritibility. The first member of each `ProtectionInfo.+` class
    // is a stripped-down object. The first member of `ProtectionInfo` is the
    // integrity protection array. Given all this, we can use `reinterpret_cast`
    // to safely reinterpret the type of any `ProtectionInfo.*` pointer, or even
    // convert any such pointers to a pointer to the integrity protection array.
    static_assert(std::is_standard_layout<ProtectionInfo<N>>::value);
  }

  Status GetStatus() const;
  ProtectionInfoKVOT<N> ProtectKVOT(uint64_t key_checksum,
                                    uint64_t value_checksum, ValueType op_type,
                                    uint64_t ts_checksum) const;

  unsigned char buf_[N] __attribute__((aligned(N))) = {0};
  static_assert(N > 0 && N <= 8);
  static_assert((N & (N - 1)) == 0);
};

template <size_t N>
struct ProtectionInfoKVOT {
  ProtectionInfoKVOT() {
    static_assert(std::is_standard_layout<ProtectionInfoKVOT<N>>::value);
  }

  ProtectionInfo<N> StripKVOT(uint64_t key_checksum, uint64_t value_checksum,
                              ValueType op_type, uint64_t ts_checksum) const;

  ProtectionInfoKVOTC<N> ProtectC(ColumnFamilyId column_family_id) const;
  ProtectionInfoKVOTS<N> ProtectS(SequenceNumber sequence_number) const;

  void UpdateK(uint64_t old_key_checksum, uint64_t new_key_checksum);
  void UpdateV(uint64_t old_value_checksum, uint64_t new_value_checksum);
  void UpdateO(ValueType old_op_type, ValueType new_op_type);
  void UpdateT(uint64_t old_ts_checksum, uint64_t new_ts_checksum);

  ProtectionInfo<N> info_;
};

template <size_t N>
struct ProtectionInfoKVOTC {
  ProtectionInfoKVOTC() {
    static_assert(std::is_standard_layout<ProtectionInfoKVOTC<N>>::value);
  }

  ProtectionInfoKVOT<N> StripC(ColumnFamilyId column_family_id) const;

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

  ProtectionInfoKVOT<N> kvot_;
};

template <size_t N>
struct ProtectionInfoKVOTS {
  ProtectionInfoKVOTS() {
    static_assert(std::is_standard_layout<ProtectionInfoKVOTS<N>>::value);
  }

  ProtectionInfoKVOT<N> StripS(SequenceNumber sequence_number) const;

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

  ProtectionInfoKVOT<N> kvot_;
};

// Non-persistent: this XOR-folding function produces different outcomes for
// different endiannesses so its output must not be persisted.
//
// Only supported for `N` and `sizeof(T)` that are powers of two and that
// cannot be larger than eight.
template <size_t N, class T>
void NPFoldXor(unsigned char* dst, T info) {
  static_assert(sizeof(T) > 0 && sizeof(T) <= 8);
  static_assert((sizeof(T) & (sizeof(T) - 1)) == 0);
  static_assert(N > 0 && N <= 8);
  static_assert((N & (N - 1)) == 0);

  unsigned char* info_bytes = reinterpret_cast<unsigned char*>(&info);
  uint64_t dst_val = 0;
  memcpy(&dst_val, dst, N);
  // In C++17 the below can be a static-if.
  if (N < sizeof(T)) {
    assert(sizeof(T) % N == 0);
    for (size_t i = 0; i < sizeof(T); i += N) {
      uint64_t info_val = 0;
      memcpy(&info_val, info_bytes, N);
      dst_val = dst_val ^ info_val;
      info_bytes += N;
    }
  } else {
    // TODO(ajkr): For calling `NPFoldXor()` multiple times with `info`s smaller
    // than `dst`'s width, it'd be better to "spray" those `info`s over `dst`.
    // Currently they always start at the low byte address. It could be done by
    // mapping each type `T` to a starting byte offsets to ensure the same start
    // offset is used for both protecting and stripping.
    uint64_t info_val = 0;
    memcpy(&info_val, info_bytes, sizeof(T));
    dst_val = dst_val ^ info_val;
  }
  memcpy(dst, &dst_val, N);
}

template <size_t N>
Status ProtectionInfo<N>::GetStatus() const {
  unsigned char expected[N] = {0};
  if (memcmp(buf_, expected, N) != 0) {
    return Status::Corruption("ProtectionInfo mismatch");
  }
  return Status::OK();
}

template <size_t N>
ProtectionInfoKVOT<N> ProtectionInfo<N>::ProtectKVOT(
    uint64_t key_checksum, uint64_t value_checksum, ValueType op_type,
    uint64_t ts_checksum) const {
  ProtectionInfoKVOT<N> res(
      *reinterpret_cast<const ProtectionInfoKVOT<N>*>(this));
  // There is no pointer-interconvertibility between a pointer to an array and a
  // pointer to the array's first element, so we need to take that step
  // explicitly.
  auto res_buf_ptr = reinterpret_cast<unsigned char(*)[N]>(&res);
  NPFoldXor<N>(&(*res_buf_ptr)[0], key_checksum);
  NPFoldXor<N>(&(*res_buf_ptr)[0], value_checksum);
  NPFoldXor<N>(&(*res_buf_ptr)[0], op_type);
  NPFoldXor<N>(&(*res_buf_ptr)[0], ts_checksum);
  return res;
}

template <size_t N>
void ProtectionInfoKVOT<N>::UpdateK(uint64_t old_key_checksum,
                                    uint64_t new_key_checksum) {
  auto buf_ptr = reinterpret_cast<unsigned char(*)[N]>(this);
  NPFoldXor<N>(&(*buf_ptr)[0], old_key_checksum);
  NPFoldXor<N>(&(*buf_ptr)[0], new_key_checksum);
}

template <size_t N>
void ProtectionInfoKVOT<N>::UpdateV(uint64_t old_value_checksum,
                                    uint64_t new_value_checksum) {
  auto buf_ptr = reinterpret_cast<unsigned char(*)[N]>(this);
  NPFoldXor<N>(&(*buf_ptr)[0], old_value_checksum);
  NPFoldXor<N>(&(*buf_ptr)[0], new_value_checksum);
}

template <size_t N>
void ProtectionInfoKVOT<N>::UpdateO(ValueType old_op_type,
                                    ValueType new_op_type) {
  auto buf_ptr = reinterpret_cast<unsigned char(*)[N]>(this);
  NPFoldXor<N>(&(*buf_ptr)[0], old_op_type);
  NPFoldXor<N>(&(*buf_ptr)[0], new_op_type);
}

template <size_t N>
void ProtectionInfoKVOT<N>::UpdateT(uint64_t old_ts_checksum,
                                    uint64_t new_ts_checksum) {
  auto buf_ptr = reinterpret_cast<unsigned char(*)[N]>(this);
  NPFoldXor<N>(&(*buf_ptr)[0], old_ts_checksum);
  NPFoldXor<N>(&(*buf_ptr)[0], new_ts_checksum);
}

template <size_t N>
ProtectionInfo<N> ProtectionInfoKVOT<N>::StripKVOT(uint64_t key_checksum,
                                                   uint64_t value_checksum,
                                                   ValueType op_type,
                                                   uint64_t ts_checksum) const {
  ProtectionInfo<N> res(*reinterpret_cast<const ProtectionInfo<N>*>(this));
  auto res_buf_ptr = reinterpret_cast<unsigned char(*)[N]>(&res);
  NPFoldXor<N>(&(*res_buf_ptr)[0], key_checksum);
  NPFoldXor<N>(&(*res_buf_ptr)[0], value_checksum);
  NPFoldXor<N>(&(*res_buf_ptr)[0], op_type);
  NPFoldXor<N>(&(*res_buf_ptr)[0], ts_checksum);
  return res;
}

template <size_t N>
ProtectionInfoKVOTC<N> ProtectionInfoKVOT<N>::ProtectC(
    ColumnFamilyId column_family_id) const {
  ProtectionInfoKVOTC<N> res(
      *reinterpret_cast<const ProtectionInfoKVOTC<N>*>(this));
  auto res_buf_ptr = reinterpret_cast<unsigned char(*)[N]>(&res);
  NPFoldXor<N>(&(*res_buf_ptr)[0], column_family_id);
  return res;
}

template <size_t N>
ProtectionInfoKVOT<N> ProtectionInfoKVOTC<N>::StripC(
    ColumnFamilyId column_family_id) const {
  ProtectionInfoKVOT<N> res(
      *reinterpret_cast<const ProtectionInfoKVOT<N>*>(this));
  auto res_buf_ptr = reinterpret_cast<unsigned char(*)[N]>(&res);
  NPFoldXor<N>(&(*res_buf_ptr)[0], column_family_id);
  return res;
}

template <size_t N>
void ProtectionInfoKVOTC<N>::UpdateC(ColumnFamilyId old_column_family_id,
                                     ColumnFamilyId new_column_family_id) {
  auto buf_ptr = reinterpret_cast<unsigned char(*)[N]>(this);
  NPFoldXor<N>(&(*buf_ptr)[0], old_column_family_id);
  NPFoldXor<N>(&(*buf_ptr)[0], new_column_family_id);
}

template <size_t N>
ProtectionInfoKVOTS<N> ProtectionInfoKVOT<N>::ProtectS(
    SequenceNumber sequence_number) const {
  ProtectionInfoKVOTS<N> res(
      *reinterpret_cast<const ProtectionInfoKVOTS<N>*>(this));
  auto res_buf_ptr = reinterpret_cast<unsigned char(*)[N]>(&res);
  NPFoldXor<N>(&(*res_buf_ptr)[0], sequence_number);
  return res;
}

template <size_t N>
ProtectionInfoKVOT<N> ProtectionInfoKVOTS<N>::StripS(
    SequenceNumber sequence_number) const {
  ProtectionInfoKVOT<N> res(
      *reinterpret_cast<const ProtectionInfoKVOT<N>*>(this));
  auto res_buf_ptr = reinterpret_cast<unsigned char(*)[N]>(&res);
  NPFoldXor<N>(&(*res_buf_ptr)[0], sequence_number);
  return res;
}

template <size_t N>
void ProtectionInfoKVOTS<N>::UpdateS(SequenceNumber old_sequence_number,
                                     SequenceNumber new_sequence_number) {
  auto buf_ptr = reinterpret_cast<unsigned char(*)[N]>(this);
  NPFoldXor<N>(&(*buf_ptr)[0], old_sequence_number);
  NPFoldXor<N>(&(*buf_ptr)[0], new_sequence_number);
}

}  // namespace ROCKSDB_NAMESPACE
