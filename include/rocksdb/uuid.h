//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// A base struct / holder for an identifier of up to 128 bits. All zeros is
// considered empty, so implementations should avoid generating / saving the
// empty value when a value is actually generated.
struct RawUuid {
  //== Data
  uint64_t lower64 = 0;
  uint64_t upper64 = 0;

  //== Construction
  RawUuid() { assert(IsEmpty()); }

  static RawUuid FromRawBytes(const char *buf_of_16);

  //== Functions
  inline bool IsEmpty() const { return lower64 == 0 && upper64 == 0; }

  // For saving into a machine-independent 16-byte string. The written
  // characters can contain nul bytes.
  std::string ToRawBytes() const;
  // For saving into a machine-independent 16-byte string, into an
  // existing buffer. No trailing nul is written or expected, but the
  // written characters can contain nul bytes. Returns a Slice for the
  // buffer and 16 written characters.
  Slice PutRawBytes(char *buf_of_16) const;

  // Data access for low-level copying between dervied structs
  inline RawUuid *Data() { return this; }
  inline const RawUuid *Data() const { return this; }
};

inline bool operator==(const RawUuid &lhs, const RawUuid &rhs) {
  return lhs.lower64 == rhs.lower64 && lhs.upper64 == rhs.upper64;
}
inline bool operator!=(const RawUuid &lhs, const RawUuid &rhs) {
  return !(lhs == rhs);
}
bool operator<(const RawUuid &lhs, const RawUuid &rhs);

// An RFC 4122 Universally Unique IDentifier. Variant 1 version 4 is the
// most common, which offers 122 bits of entropy. The text form is 32
// hexadecimal digits and four hyphens, for a total of 36 characters.
// https://en.wikipedia.org/wiki/Universally_unique_identifier
//
// This is considered suitable for identifying medium-high frequency events.
// For example, if 1 million computers each generate 10 random RfcUuids every
// second for a year, the chance of one being repeated in that time is 1 in
// millions. See Env::GenerateRfcUuid().
//
// Implementation detail: RocksDB uses a byte ordering that puts the
// 4-bit version in the upper-most bits of upper64 and the 1-4 bit variant
// in the upper-most bits of lower64.
struct RfcUuid : public RawUuid {
  RfcUuid() { assert(IsEmpty()); }

  // Parses from the 36-character form with hexadecimal digits and four
  // hyphens. Returns Corruption if malformed.
  static Status Parse(const Slice &input, RfcUuid *out);

  // Assuming the input is random with 128 bits of entropy, this converts to
  // an RfcUuid by setting variant 1 version 4, losing 6 bits of entropy.
  static RfcUuid FromRawLoseData(const RawUuid &raw);

  // Generates the 36-character text form
  std::string ToString() const;
  // Generates the 36-character text form directly into a buffer,
  // returning a Slice into the buffer for the 36 characters written.
  Slice PutString(char *buf_of_36) const;

  // From RFC 4122, except reserved/unspecified variants are unchecked and
  // might be returned. Variant is >= 0 and <= 4
  int GetVariant() const;
  // Sets variant while leaving other data bits in place. Note that higher
  // variants overwrite more data bits, so the variant should generally only
  // be set once.
  void SetVariant(int variant);

  // From RFC 4122, Version is >= 0 and <= 15
  int GetVersion() const;
  // Sets version while leaving other data bits in place.
  void SetVersion(int version);
};

// Specialized for same ordering as the string representation
bool operator<(const RfcUuid &lhs, const RfcUuid &rhs);

}  // namespace ROCKSDB_NAMESPACE

namespace std {
template <>
struct hash<ROCKSDB_NAMESPACE::RawUuid> {
  std::size_t operator()(ROCKSDB_NAMESPACE::RawUuid const &u) const noexcept {
    // Assume suitable distribution already
    return static_cast<size_t>(u.lower64 ^ u.upper64);
  }
};

template <>
struct hash<ROCKSDB_NAMESPACE::RfcUuid> {
  std::size_t operator()(ROCKSDB_NAMESPACE::RfcUuid const &u) const noexcept {
    return std::hash<ROCKSDB_NAMESPACE::RawUuid>{}(*u.Data());
  }
};

}  // namespace std
