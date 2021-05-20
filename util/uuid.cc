//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/uuid.h"

#include "rocksdb/status.h"
#include "util/string_util.h"
#include "util/uuid_internal.h"
//#include "util/xxhash.h"

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

bool operator<(const RawUuid &lhs, const RawUuid &rhs) {
  return std::tie(lhs.upper64, lhs.lower64) <
         std::tie(rhs.upper64, rhs.lower64);
}

bool operator<(const RfcUuid &lhs, const RfcUuid &rhs) {
  uint64_t left_alt_upper = (lhs.upper64 << 16) | (lhs.upper64 >> 48);
  uint64_t right_alt_upper = (rhs.upper64 << 16) | (rhs.upper64 >> 48);
  return std::tie(left_alt_upper, lhs.lower64) <
         std::tie(right_alt_upper, rhs.lower64);
}

int RfcUuid::GetVersion() const { return static_cast<int>(upper64 >> 60); }

int RfcUuid::GetVariant() const {
  // Inefficient count leading 1's
  if ((lower64 & (uint64_t{1} << 63)) == 0) {
    return 0;
  } else if ((lower64 & (uint64_t{1} << 62)) == 0) {
    return 1;
  } else if ((lower64 & (uint64_t{1} << 61)) == 0) {
    return 2;
  } else if ((lower64 & (uint64_t{1} << 60)) == 0) {
    // Unspecified/reserved
    return 3;
  } else {
    // Unspecified/reserved
    return 4;
  }
}

void RfcUuid::SetVariant(int variant) {
  if (variant < 0 || variant >= 4) {
    // clear top four
    lower64 &= 0x0fffffffffffffff;
  } else {
    // Unary encode with leading ones
    lower64 |= uint64_t{0xf000000000000000} << (4 - variant);
    // and then a zero
    lower64 &= ~(uint64_t{1} << (63 - variant));
  }
}

void RfcUuid::SetVersion(int version) {
  // clear top four and set version
  upper64 &= 0x0fffffffffffffff;
  upper64 |= static_cast<uint64_t>(version) << 60;
}

std::string RfcUuid::ToString() const {
  std::string rv(36, '\0');
  PutString(&rv[0]);
  return rv;
}

Slice RfcUuid::PutString(char *buf_of_36) const {
  char *buf = buf_of_36;
  PutBaseString<16>(&buf, 8, upper64 >> 16, kDigitsLowerCase);
  *(buf++) = '-';
  PutBaseString<16>(&buf, 4, upper64, kDigitsLowerCase);
  *(buf++) = '-';
  PutBaseString<16>(&buf, 4, upper64 >> 48, kDigitsLowerCase);
  *(buf++) = '-';
  PutBaseString<16>(&buf, 4, lower64 >> 48, kDigitsLowerCase);
  *(buf++) = '-';
  PutBaseString<16>(&buf, 12, lower64, kDigitsLowerCase);
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
    uint64_t upper_upper = 0;
    s = ParseBaseString<16>(&buf, 4, &upper_upper);
    upper64 |= upper_upper << 48;
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
    out->upper64 = upper64;
    out->lower64 = lower64;
    assert(buf == input.data() + input.size());
  }
  return s;
}

RfcUuid RfcUuid::FromRawLoseData(const RawUuid &raw) {
  RfcUuid u;
  *u.Data() = *raw.Data();
  // Variant 1 version 4 indicates randomly generated
  u.SetVariant(1);
  u.SetVersion(4);
  assert(!u.IsEmpty());
  return u;
}

Status RocksMuid::Parse(const Slice &input, RocksMuid *out) {
  if (input.size() != 20) {
    return Status::Corruption(std::string("Expected 20 bytes but got ") +
                              ROCKSDB_NAMESPACE::ToString(input.size()));
  }
  const char *buf = input.data();
  Status s;
  if (s.ok()) {
    s = ParseBaseString<36>(&buf, 10, &out->upper64);
  }
  if (s.ok()) {
    s = ParseBaseString<36>(&buf, 10, &out->lower64);
  }
  return s;
}

std::string RocksMuid::ToString() const {
  std::string rv(20U, '\0');
  PutString(&rv[0]);
  return rv;
}

Slice RocksMuid::PutString(char *buf_of_20) const {
  char *buf = buf_of_20;
  PutBaseString<36>(&buf, 10, upper64, kDigitsUpperCase);
  PutBaseString<36>(&buf, 10, lower64, kDigitsUpperCase);
  assert(buf == buf_of_20 + 20);
  return Slice(buf_of_20, 20);
}

RocksMuid RocksMuid::FromRawLoseData(const RawUuid &raw) {
  uint64_t a = raw.lower64 % RocksMuid::kPieceMod;
  uint64_t b = raw.upper64 % RocksMuid::kPieceMod;
  // Ensure non-empty
  a += ((a | b) == 0);

  RocksMuid m;
  m.lower64 = a;
  m.upper64 = b;
  assert(!m.IsEmpty());
  return m;
}

}  // namespace ROCKSDB_NAMESPACE
