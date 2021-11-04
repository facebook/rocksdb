//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/cache_key.h"

#include <algorithm>
#include <atomic>

#include "rocksdb/cache.h"
#include "table/unique_id_impl.h"
#include "util/hash.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {

CacheKey CacheKey::CreateUniqueForCacheLifetime(Cache *cache) {
  // +1 so that we can reserve all zeros for "unset" cache key
  uint64_t id = cache->NewId() + 1;
  // Ensure we don't collide with CreateUniqueForProcessLifetime
  assert(id >> 63 == 0);
  return CacheKey(0, id);
}

CacheKey CacheKey::CreateUniqueForProcessLifetime() {
  // To avoid colliding with CreateUniqueForCacheLifetime, assuming
  // Cache::NewId counts up from zero, here we count down from UINT64_MAX.
  // If this ever becomes a point of contention, we could use CoreLocalArray.
  static std::atomic<uint64_t> counter{UINT64_MAX};
  uint64_t id = counter.fetch_sub(1, std::memory_order_relaxed);
  // Ensure we don't collide with CreateUniqueForCacheLifetime
  assert(id >> 63 == 1);
  return CacheKey(0, id);
}

OffsetableCacheKey::OffsetableCacheKey(const std::string &db_id,
                                       const std::string &db_session_id,
                                       uint64_t file_number,
                                       uint64_t max_offset) {
#ifndef NDEBUG
  max_offset_ = max_offset;
#endif
  // Closely related to GetSstInternalUniqueId, but only need 128 bits.
  // See also https://github.com/pdillinger/unique_id for background.

  uint64_t session_upper = 0;  // Assignment to appease clang-analyze
  uint64_t session_lower = 0;  // Assignment to appease clang-analyze
  {
    Status s = DecodeSessionId(db_session_id, &session_upper, &session_lower);
    if (!s.ok()) {
      // A reasonable fallback in case malformed
      Hash2x64(db_session_id.data(), db_session_id.size(), &session_upper,
               &session_lower);
    }
  }

  // Hash the session upper (~39 bits entropy) and DB id (120+ bits entropy)
  // for more global uniqueness entropy.
  // (It is possible that many DBs descended from one common DB id are copied
  // around and proliferate, in which case session id is critical, but it is
  // more common for different DBs to have different DB ids.)
  uint64_t db_hash = Hash64(db_id.data(), db_id.size(), session_upper);

  // This establishes the db+session id part of the cache key.
  //
  // Exactly preserve (in common cases; see modifiers below) session lower to
  // ensure that session ids generated during the same process lifetime are
  // guaranteed unique.
  //
  // We put this first in anticipation of matching a small-ish set of cache
  // key prefixes to cover entries relevant to any DB.
  session_etc64_ = session_lower;
  // This provides extra entopy in case of different DB id or process
  // generating a session id, but is also partly/variably obscured by
  // file_number and offset (see below).
  offset_etc64_ = db_hash;

  // Into offset_etc64_ we are (eventually) going to pack & xor in an offset and
  // a file_number, but we might need the file_number to overflow into
  // session_etc64_. (We only want one possible value for session_etc64_ per
  // file.)
  //
  // Figure out how many bytes of file_number we are going to be able to
  // pack in with max_offset, though our encoding will only support packing
  // in up to 3 bytes of file_number. (16M file numbers is enough for a new
  // file number every second for half a year.)
  int file_number_bytes_in_offset_etc =
      (63 - FloorLog2(max_offset | 0x100000000U)) / 8;
  int file_number_bits_in_offset_etc = file_number_bytes_in_offset_etc * 8;

  // Assert two bits of metadata
  assert(file_number_bytes_in_offset_etc >= 0 &&
         file_number_bytes_in_offset_etc <= 3);
  // Assert we couldn't have used a larger allowed number of bytes (shift
  // would chop off bytes).
  assert(file_number_bytes_in_offset_etc == 3 ||
         (max_offset << (file_number_bits_in_offset_etc + 8) >>
          (file_number_bits_in_offset_etc + 8)) != max_offset);

  uint64_t mask = (uint64_t{1} << (file_number_bits_in_offset_etc)) - 1;
  // Pack into high bits of etc so that offset can go in low bits of etc
  uint64_t offset_etc_modifier = ReverseBits(file_number & mask);
  assert(offset_etc_modifier << file_number_bits_in_offset_etc == 0U);

  // Overflow and 3 - byte count (likely both zero) go into session_id part
  uint64_t session_etc_modifier =
      (file_number >> file_number_bits_in_offset_etc << 2) |
      static_cast<uint64_t>(3 - file_number_bytes_in_offset_etc);
  // Packed into high bits to minimize interference with session id counter.
  session_etc_modifier = ReverseBits(session_etc_modifier);

  // Assert session_id part is only modified in extreme cases
  assert(session_etc_modifier == 0 || file_number > /*3 bytes*/ 0xffffffU ||
         max_offset > /*5 bytes*/ 0xffffffffffU);

  // Xor in the modifiers
  session_etc64_ ^= session_etc_modifier;
  offset_etc64_ ^= offset_etc_modifier;

  // Although DBImpl guarantees (in recent versions) that session_lower is not
  // zero, that's not entirely sufficient to guarantee that session_etc64_ is
  // not zero (so that the 0 case can be used by CacheKey::CreateUnique*)
  if (session_etc64_ == 0U) {
    session_etc64_ = session_upper | 1U;
  }
  assert(session_etc64_ != 0);
}

}  // namespace ROCKSDB_NAMESPACE
