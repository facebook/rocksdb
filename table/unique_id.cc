//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdint>

#include "table/unique_id_impl.h"
#include "util/coding_lean.h"
#include "util/hash.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

std::string EncodeSessionId(uint64_t upper, uint64_t lower) {
  std::string db_session_id(20U, '\0');
  char *buf = &db_session_id[0];
  // Preserving `lower` is slightly tricky. 36^12 is slightly more than
  // 62 bits, so we use 12 chars plus the bottom two bits of one more.
  // (A tiny fraction of 20 digit strings go unused.)
  uint64_t a = (upper << 2) | (lower >> 62);
  uint64_t b = lower & (UINT64_MAX >> 2);
  PutBaseChars<36>(&buf, 8, a, /*uppercase*/ true);
  PutBaseChars<36>(&buf, 12, b, /*uppercase*/ true);
  assert(buf == &*db_session_id.end());
  return db_session_id;
}

Status DecodeSessionId(const std::string &db_session_id, uint64_t *upper,
                       uint64_t *lower) {
  const size_t len = db_session_id.size();
  if (len == 0) {
    return Status::NotSupported("Missing db_session_id");
  }
  // Anything from 13 to 24 chars is reasonable. We don't have to limit to
  // exactly 20.
  if (len < 13) {
    return Status::NotSupported("Too short db_session_id");
  }
  if (len > 24) {
    return Status::NotSupported("Too long db_session_id");
  }
  uint64_t a = 0, b = 0;
  const char *buf = &*db_session_id.begin();
  bool success = ParseBaseChars<36>(&buf, len - 12U, &a);
  if (!success) {
    return Status::NotSupported("Bad digit in db_session_id");
  }
  success = ParseBaseChars<36>(&buf, 12U, &b);
  if (!success) {
    return Status::NotSupported("Bad digit in db_session_id");
  }
  assert(buf == &*db_session_id.end());
  *upper = a >> 2;
  *lower = (b & (UINT64_MAX >> 2)) | (a << 62);
  return Status::OK();
}

Status GetSstInternalUniqueId(const std::string &db_id,
                              const std::string &db_session_id,
                              uint64_t file_number,
                              std::array<uint64_t, 3> *out) {
  if (db_id.empty()) {
    return Status::NotSupported("Missing db_id");
  }
  if (file_number == 0) {
    return Status::NotSupported("Missing or bad file number");
  }
  if (db_session_id.empty()) {
    return Status::NotSupported("Missing db_session_id");
  }
  uint64_t session_upper = 0;  // Assignment to appease clang-analyze
  uint64_t session_lower = 0;  // Assignment to appease clang-analyze
  {
    Status s = DecodeSessionId(db_session_id, &session_upper, &session_lower);
    if (!s.ok()) {
      return s;
    }
  }

  // Exactly preserve session lower to ensure that session ids generated
  // during the same process lifetime are guaranteed unique.
  // We put this first in anticipation of matching a small-ish set of cache
  // key prefixes to cover entries relevant to any DB.
  (*out)[0] = session_lower;

  // Hash the session upper (~39 bits entropy) and DB id (120+ bits entropy)
  // for very high global uniqueness entropy.
  // (It is possible that many DBs descended from one common DB id are copied
  // around and proliferate, in which case session id is critical, but it is
  // more common for different DBs to have different DB ids.)
  uint64_t db_a, db_b;
  Hash2x64(db_id.data(), db_id.size(), session_upper, &db_a, &db_b);

  // Xor in file number for guaranteed uniqueness by file number for a given
  // session and DB id. (Xor slightly better than + here. See
  // https://github.com/pdillinger/unique_id )
  (*out)[1] = db_a ^ file_number;

  // Extra (optional) global uniqueness
  (*out)[2] = db_b;

  return Status::OK();
}

namespace {
// Taken from XXH3
inline uint64_t Avalanche(uint64_t h64) {
  h64 ^= h64 >> 37;
  h64 *= 0x165667919E3779F9ULL;
  h64 ^= h64 >> 32;
  return h64;
}

constexpr uint64_t kPrime64_1 = 0x9E3779B185EBCA87U;
constexpr uint64_t kPrime64_2 = 0xC2B2AE3D27D4EB4FU;
// Multiplicative inverses mod 2^64
constexpr uint64_t kPrime64Inv_1 = 0x887493432BADB37U;
constexpr uint64_t kPrime64Inv_2 = 0xBA79078168D4BAFU;

}  // namespace

void InternalUniqueIdToExternal(std::array<uint64_t, 3> *in_out) {
  uint64_t a = (*in_out)[0] * kPrime64_1;
  uint64_t b = (*in_out)[1] * kPrime64_2;
  b ^= Avalanche(a);
  a ^= Avalanche(b);
  (*in_out)[2] += a + b;
  b ^= Avalanche(a);
  (*in_out)[0] = a;
  (*in_out)[1] = b;
}

void ExternalUniqueIdToInternal(std::array<uint64_t, 3> *in_out) {
  uint64_t a = (*in_out)[0];
  uint64_t b = (*in_out)[1];
  b ^= Avalanche(a);
  (*in_out)[2] -= a + b;
  a ^= Avalanche(b);
  b ^= Avalanche(a);
  (*in_out)[0] = a * kPrime64Inv_1;
  (*in_out)[1] = b * kPrime64Inv_2;
}

template <>
Status GetUniqueIdFromTableProperties(const TableProperties &props,
                                      std::array<uint64_t, 3> *out_array) {
  Status s = GetSstInternalUniqueId(props.db_id, props.db_session_id,
                                    props.orig_file_number, out_array);
  if (s.ok()) {
    InternalUniqueIdToExternal(out_array);
  }
  return s;
}

Status GetUniqueIdFromTableProperties(const TableProperties &props,
                                      uint64_t *out_array, size_t out_len) {
  if (out_len > 3) {
    return Status::InvalidArgument("Cannot get unique id > 192 bits");
  }
  std::array<uint64_t, 3> tmp;
  Status s = GetUniqueIdFromTableProperties(props, &tmp);
  if (s.ok()) {
    for (size_t i = 0; i < out_len; ++i) {
      out_array[i] = tmp[i];
    }
  }
  return s;
}

Status GetUniqueIdFromTableProperties(const TableProperties &props,
                                      char *out_array, size_t out_len) {
  if (out_len > 24) {
    return Status::InvalidArgument("Cannot get unique id > 192 bits");
  }
  std::array<uint64_t, 3> tmp;
  Status s = GetUniqueIdFromTableProperties(props, &tmp);
  if (s.ok()) {
    // Always encode to little endian
    size_t i = 0;
    for (; i + 8 <= out_len; i += 8) {
      EncodeFixed64(out_array + i, tmp[i / 8]);
    }
    for (; i < out_len; ++i) {
      out_array[i] = static_cast<char>(tmp[i / 8] >> ((i % 8) * 8));
    }
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
