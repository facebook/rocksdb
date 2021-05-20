//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/uuid.h"

namespace ROCKSDB_NAMESPACE {

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
// Thus, each uint64_t piece is < kPieceMod.
struct RocksMuid : public RawUuid {
  RocksMuid() { assert(IsEmpty()); }

  // Parses from the 20-digit base-36 text form. Returns Corruption if
  // malformed.
  static Status Parse(const Slice &input, RocksMuid *out);

  // This converts a RawUuid (also OK: RfcUuid) to a RocksMuid by
  // % kPieceMod on both 64-bit pieces. Assuming the input is random with
  // 128 bits of entropy, about 24.6 bits of entropy are lost in the
  // conversion.
  static RocksMuid FromRawLoseData(const RawUuid &raw);

  // For generating a human readable string, base-36 with 20 digits
  std::string ToString() const;
  // For generating a human readable string (base-36 with 20 digits)
  // into an existing buffer. No trailing nul is written or expected.
  // Returns a Slice for the buffer and 20 written digits.
  Slice PutString(char *buf_of_20) const;

  // Convert to an RFC 4122 Universally Unique IDentifier by setting
  // variant 1 version 4. (The conversion is simple because the bits
  // used by variant and version in RfcUuid are kept 0 in RocksMuid.)
  RfcUuid ToRfcUuid() const;

  // 36 to the 10th power
  static constexpr uint64_t kPieceMod = 3656158440062976;
};

}  // namespace ROCKSDB_NAMESPACE
