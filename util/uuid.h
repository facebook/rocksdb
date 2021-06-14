//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/status.h"
#include "util/math128.h"

namespace ROCKSDB_NAMESPACE {

// A holder for an identifier of up to 128 bits.
using RawUuid = Unsigned128;

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
struct RfcUuid {
  RawUuid data = 0;

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

  // Should only be true when unset
  inline bool IsEmpty() { return data == 0; }

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

inline bool operator<(const RfcUuid &lhs, const RfcUuid &rhs) {
  return lhs.data < rhs.data;
}

inline bool operator==(const RfcUuid &lhs, const RfcUuid &rhs) {
  return lhs.data == rhs.data;
}

// A "mostly unique ID" encodable into 20 characters in base 36 ([0-9][A-Z])
// This provides a compact, portable text representation with 103.4 bits of
// entropy. This was originally developed for DB Session IDs (see
// DB::GetDbSessionId(). All zeros means unset or empty.
//
// This is considered suitable for identifying medium-frequency events or
// narrow scope events. For example, if 1 million computers each generate a
// random RocksMuid every minute for a year, or one computer generates a
// million every second for a week, the chance of a RocksMuid being repeated
// in that time is 1 in millions. See Env::GenerateRawUuid() and
// FromRawLoseData().
//
// In text form, the digits are in standard written order: lowest-order last,
// except each piece (lower64 and upper64) only holds 10 base-36 digits.
// Thus, each uint64_t piece is < kPieceMod. FIXME
struct RocksMuid {
  RawUuid scaled_data = 0;

  // Parses from the 20-digit base-36 text form. Returns Corruption if
  // malformed.
  static Status Parse(const Slice &input, RocksMuid *out);

  // This converts a RawUuid (also OK: RfcUuid) to a RocksMuid by
  // FIXME on both 64-bit pieces. Assuming the input is random with
  // 128 bits of entropy, about 24.6 bits of entropy are lost in the
  // conversion.
  static RocksMuid FromRawLoseData(const RawUuid &raw);

  // For generating a human readable string, base-36 with 20 digits
  std::string ToString() const;
  // For generating a human readable string (base-36 with 20 digits)
  // into an existing buffer. No trailing nul is written or expected.
  // Returns a Slice for the buffer and 20 written digits.
  Slice PutString(char *buf_of_20) const;

 private:
  static Unsigned128 ReducedToScaled(const Unsigned128 &reduced);
  static void ScaledToPieces(const Unsigned128 &scaled, uint64_t *upper_piece,
                             uint64_t *lower_piece);
  static Unsigned128 PiecesToReduced(uint64_t upper_piece,
                                     uint64_t lower_piece);
  static Unsigned128 ScaledToReduced(const Unsigned128 &scaled);

  // 36 to the 10th power
  static constexpr uint64_t kPieceMod = 3656158440062976;
};

inline bool operator<(const RocksMuid &lhs, const RocksMuid &rhs) {
  return lhs.scaled_data < rhs.scaled_data;
}

inline bool operator==(const RocksMuid &lhs, const RocksMuid &rhs) {
  return lhs.scaled_data == rhs.scaled_data;
}

RawUuid AddInCounterA(const RawUuid &u, uint64_t counter);
RawUuid AddInCounterB(const RawUuid &u, uint64_t counter);
RawUuid AddInCounterC(const RawUuid &u, uint64_t counter);

RawUuid SubtractOffCounterA(const RawUuid &u, uint64_t counter);
RawUuid SubtractOffCounterB(const RawUuid &u, uint64_t counter);
RawUuid SubtractOffCounterC(const RawUuid &u, uint64_t counter);

}  // namespace ROCKSDB_NAMESPACE

namespace std {
template <>
struct hash<ROCKSDB_NAMESPACE::RfcUuid> {
  std::size_t operator()(ROCKSDB_NAMESPACE::RfcUuid const &u) const noexcept {
    return std::hash<ROCKSDB_NAMESPACE::Unsigned128>()(u.data);
  }
};

template <>
struct hash<ROCKSDB_NAMESPACE::RocksMuid> {
  std::size_t operator()(ROCKSDB_NAMESPACE::RocksMuid const &u) const noexcept {
    return std::hash<ROCKSDB_NAMESPACE::Unsigned128>()(u.scaled_data);
  }
};
}  // namespace std
