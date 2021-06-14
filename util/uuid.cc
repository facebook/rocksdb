//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/uuid.h"

#include "rocksdb/status.h"
#include "util/hash.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

const char kDigitsUpperCase[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const char kDigitsLowerCase[] = "0123456789abcdefghijklmnopqrstuvwxyz";

template <size_t kBase>
inline void PutBaseString(char **buf, size_t n, uint64_t v,
                          const char *digitChars) {
  for (size_t i = n; i > 0; --i) {
    (*buf)[i - 1] = digitChars[v % kBase];
    v /= kBase;
  }
  *buf += n;
}

template <size_t kBase>
inline Status ParseBaseString(const char **buf, size_t n, uint64_t *v) {
  const char *const end = *buf + n;
  while (*buf < end) {
    char c = *((*buf)++);
    unsigned digit = 0;
    if (c >= '0' && c <= '9') {
      digit = static_cast<unsigned>(c - '0');
    } else if (c >= 'A' && c <= 'Z') {
      digit = static_cast<unsigned>(c - 'A' + 10);
    } else if (c >= 'a' && c <= 'z') {
      digit = static_cast<unsigned>(c - 'a' + 10);
    } else {
      return Status::Corruption(std::string("Not a digit: ") + c);
    }
    if (digit >= kBase) {
      return Status::Corruption("Not a base " + ToString(kBase) +
                                " digit: " + c);
    }
    *v = *v * kBase + digit;
  }
  return Status::OK();
}

inline Status ParseHyphen(const char **buf) {
  char c = *((*buf)++);
  if (c != '-') {
    return Status::Corruption("Expected or misplaced '-'");
  }
  return Status::OK();
}

}  // namespace

int RfcUuid::GetVersion() const {
  return static_cast<uint8_t>(data >> 76) & 0xf;
}

int RfcUuid::GetVariant() const {
  // Inefficient count leading 1's
  if ((data & (uint64_t{1} << 63)) == 0) {
    return 0;
  } else if ((data & (uint64_t{1} << 62)) == 0) {
    return 1;
  } else if ((data & (uint64_t{1} << 61)) == 0) {
    return 2;
  } else {
    // Unspecified/reserved
    return 3;
  }
}

void RfcUuid::SetVariant(int variant) {
  if (variant < 0 || variant >= 4) {
    // Out of range.
    assert(false);
    variant = 3;
  }
  // Unary encode with leading ones
  data |= uint64_t{0xf000000000000000} << (4 - variant);
  // and then a zero
  data &= ~(Unsigned128{1} << (63 - variant));
}

void RfcUuid::SetVersion(int version) {
  // clear four bits and set version
  data &= ~(Unsigned128{0xf} << 76);
  data |= Unsigned128{static_cast<unsigned>(version) & 0xfU} << 76;
}

std::string RfcUuid::ToString() const {
  std::string rv(36, '\0');
  PutString(&rv[0]);
  return rv;
}

Slice RfcUuid::PutString(char *buf_of_36) const {
  char *buf = buf_of_36;
  PutBaseString<16>(&buf, 8, Upper64of128(data) >> 32, kDigitsLowerCase);
  *(buf++) = '-';
  PutBaseString<16>(&buf, 4, Upper64of128(data) >> 16, kDigitsLowerCase);
  *(buf++) = '-';
  PutBaseString<16>(&buf, 4, Upper64of128(data), kDigitsLowerCase);
  *(buf++) = '-';
  PutBaseString<16>(&buf, 4, Lower64of128(data) >> 48, kDigitsLowerCase);
  *(buf++) = '-';
  PutBaseString<16>(&buf, 12, Lower64of128(data), kDigitsLowerCase);
  assert(buf == buf_of_36 + 36);
  return Slice(buf_of_36, 36);
}

Status RfcUuid::Parse(const Slice &input, RfcUuid *out) {
  if (input.size() != 36) {
    return Status::Corruption(std::string("Expected 36 bytes but got ") +
                              ROCKSDB_NAMESPACE::ToString(input.size()));
  }
  const char *buf = input.data();
  Status s;
  uint64_t upper64 = 0;
  uint64_t lower64 = 0;
  if (s.ok()) {
    s = ParseBaseString<16>(&buf, 8, &upper64);
  }
  if (s.ok()) {
    s = ParseHyphen(&buf);
  }
  if (s.ok()) {
    s = ParseBaseString<16>(&buf, 4, &upper64);
  }
  if (s.ok()) {
    s = ParseHyphen(&buf);
  }
  if (s.ok()) {
    s = ParseBaseString<16>(&buf, 4, &upper64);
  }
  if (s.ok()) {
    s = ParseHyphen(&buf);
  }
  if (s.ok()) {
    s = ParseBaseString<16>(&buf, 4, &lower64);
  }
  if (s.ok()) {
    s = ParseHyphen(&buf);
  }
  if (s.ok()) {
    s = ParseBaseString<16>(&buf, 12, &lower64);
  }
  if (s.ok()) {
    out->data = (Unsigned128{upper64} << 64) | lower64;
    assert(buf == input.data() + input.size());
  }
  return s;
}

RfcUuid RfcUuid::FromRawLoseData(const RawUuid &raw) {
  RfcUuid u;
  // To drop lowest bits (preserving middle and high bits), shift down bits
  // where version and variant will be set
  u.data = (raw & (Unsigned128{0xffffffffffff} << 80)) |
           ((raw & (Unsigned128{0xfff} << 68)) >> 4) |
           ((raw & (Unsigned128{0xfffffffffffffff} << 6)) >> 6);
  // Variant 1 version 4 indicates randomly generated
  u.SetVariant(1);
  u.SetVersion(4);
  assert(!u.IsEmpty());
  return u;
}

Unsigned128 RocksMuid::ReducedToScaled(const Unsigned128 &reduced) {
  // This scales the "reduced" value in lowest bits to the entire range of
  // 128-bit values, essentially occupying the "upper" bits but not exactly
  // because we don't fit into bit boundaries. Bascially, we are going to
  // multiply by 2**128 / 36**20 ~= 25455957.05553etc. and round down. We
  // use large unsigned arithmetic to simulate fixed point real arithmetic.
  //
  // Sufficient precision in < 128 bits:
  // python> hex((2**208 / (36**20) + 1))
  // '0x1846d550e37b5063df43be64e08L'
  static const Unsigned128 kReciprocal =
      (Unsigned128{0x1846d550e37} << 64) + 0xb5063df43be64e08;

  Unsigned128 upper128;
  Unsigned128 lower128;

  // Multiply
  Multiply128to256(reduced, kReciprocal, &upper128, &lower128);

  // Find the right place for our decimal point and round down.
  // (208 + 48 == 256)
  return (upper128 << 48) | (lower128 >> 80);
}

void RocksMuid::ScaledToPieces(const Unsigned128 &scaled, uint64_t *upper_piece,
                               uint64_t *lower_piece) {
  Unsigned128 upper128;
  Unsigned128 lower128;

  // A conversion similar to FastRange (two rounds)
  Multiply128to256(scaled, kPieceMod, &upper128, &lower128);
  *upper_piece = Lower64of128(upper128);

  Multiply128to256(lower128, kPieceMod, &upper128, &lower128);
  *lower_piece = Lower64of128(upper128);
}

Unsigned128 RocksMuid::PiecesToReduced(uint64_t upper_piece,
                                       uint64_t lower_piece) {
  assert(upper_piece < kPieceMod);
  assert(lower_piece < kPieceMod);
  return Multiply64to128(upper_piece, kPieceMod) + lower_piece;
}

Unsigned128 RocksMuid::ScaledToReduced(const Unsigned128 &scaled) {
  uint64_t upper_piece;
  uint64_t lower_piece;
  ScaledToPieces(scaled, &upper_piece, &lower_piece);
  return PiecesToReduced(upper_piece, lower_piece);
}

Status RocksMuid::Parse(const Slice &input, RocksMuid *out) {
  if (input.size() != 20) {
    return Status::Corruption(std::string("Expected 20 bytes but got ") +
                              ROCKSDB_NAMESPACE::ToString(input.size()));
  }
  const char *buf = input.data();
  uint64_t upper_piece = 0;
  uint64_t lower_piece = 0;
  Status s;
  if (s.ok()) {
    s = ParseBaseString<36>(&buf, 10, &upper_piece);
  }
  if (s.ok()) {
    s = ParseBaseString<36>(&buf, 10, &lower_piece);
  }
  if (s.ok()) {
    out->scaled_data =
        ReducedToScaled(PiecesToReduced(upper_piece, lower_piece));

#ifndef NDEBUG
    uint64_t check_upper;
    uint64_t check_lower;
    ScaledToPieces(out->scaled_data, &check_upper, &check_lower);
    assert(check_upper == upper_piece);
    assert(check_lower == lower_piece);
#endif
  }
  return s;
}

RocksMuid RocksMuid::FromRawLoseData(const RawUuid &raw) {
  RocksMuid m;
  // Raw is already "scaled" but canonicalize it
  m.scaled_data = ReducedToScaled(ScaledToReduced(raw));
  return m;
}

std::string RocksMuid::ToString() const {
  std::string rv(20U, '\0');
  PutString(&rv[0]);
  return rv;
}

Slice RocksMuid::PutString(char *buf_of_20) const {
  uint64_t upper_piece;
  uint64_t lower_piece;
  ScaledToPieces(scaled_data, &upper_piece, &lower_piece);

  char *buf = buf_of_20;
  PutBaseString<36>(&buf, 10, upper_piece, kDigitsUpperCase);
  PutBaseString<36>(&buf, 10, lower_piece, kDigitsUpperCase);

  assert(buf == buf_of_20 + 20);
  return Slice(buf_of_20, 20);
}

namespace {

// TODO: move to math.h, resolve EndianSwapValue dependency
template <typename T>
inline T ReverseBits(T v) {
  static_assert(std::is_integral<T>::value, "non-integral type");

  T r = EndianSwapValue(v);
  constexpr T kHighestByte = T{1} << ((sizeof(T) - 1) * 8);
  constexpr T kEveryByte = kHighestByte | (kHighestByte / 255);

  r = ((r & (kEveryByte * 0x0f)) << 4) | ((r & (kEveryByte * 0xf0)) >> 4);
  r = ((r & (kEveryByte * 0x33)) << 2) | ((r & (kEveryByte * 0xcc)) >> 2);
  r = ((r & (kEveryByte * 0x55)) << 1) | ((r & (kEveryByte * 0xaa)) >> 1);

  return r;
}

// Random primes occupying most of the value range
constexpr uint64_t kPrime64_A = 0xe6143049bc367e43;
constexpr uint64_t kPrime64_B = 0xd1bd70385a9c3035;
constexpr uint64_t kPrime64_C = 0xc44c84ce3dbee1e9;

}  // namespace

RawUuid AddInCounterA(const RawUuid &u, uint64_t counter) {
  // Reverse counter bits to favor middle to high bits for small differences
  // in counter
  return u + Multiply64to128(ReverseBits(counter), kPrime64_A);
}
RawUuid SubtractOffCounterA(const RawUuid &u, uint64_t counter) {
  return u - Multiply64to128(ReverseBits(counter), kPrime64_A);
}

RawUuid AddInCounterB(const RawUuid &u, uint64_t counter) {
  // Favor low to middle bits for small differences in counter
  return u + Multiply64to128(counter, kPrime64_B);
}
RawUuid SubtractOffCounterB(const RawUuid &u, uint64_t counter) {
  return u - Multiply64to128(counter, kPrime64_B);
}

RawUuid AddInCounterC(const RawUuid &u, uint64_t counter) {
  // Favor low to middle bits for small differences in counter
  return u + Multiply64to128(counter, kPrime64_C);
}
RawUuid SubtractOffCounterC(const RawUuid &u, uint64_t counter) {
  return u - Multiply64to128(counter, kPrime64_C);
}

}  // namespace ROCKSDB_NAMESPACE
