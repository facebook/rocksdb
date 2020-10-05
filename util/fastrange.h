//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// fastrange/FastRange: A faster alternative to % for mapping a hash value
// to an arbitrary range. See https://github.com/lemire/fastrange
//
// Generally recommended are FastRange32 for mapping results of 32-bit
// hash functions and FastRange64 for mapping results of 64-bit hash
// functions. FastRange is less forgiving than % if the input hashes are
// not well distributed over the full range of the type (32 or 64 bits).
//
// Also included is a templated implementation FastRangeGeneric for use
// in generic algorithms, but not otherwise recommended because of
// potential ambiguity. Unlike with %, it is critical to use the right
// FastRange variant for the output size of your hash function.

#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

#ifdef TEST_UINT128_COMPAT
#undef HAVE_UINT128_EXTENSION
#endif

namespace ROCKSDB_NAMESPACE {

namespace detail {

// Using a class template to support partial specialization
template <typename Hash, typename Range>
struct FastRangeGenericImpl {
  // only reach this on no supported specialization
};

template <typename Range>
struct FastRangeGenericImpl<uint32_t, Range> {
  static inline Range Fn(uint32_t hash, Range range) {
    static_assert(std::is_unsigned<Range>::value, "must be unsigned");
    static_assert(sizeof(Range) <= sizeof(uint32_t),
                  "cannot be larger than hash (32 bits)");

    uint64_t product = uint64_t{range} * hash;
    return static_cast<Range>(product >> 32);
  }
};

template <typename Range>
struct FastRangeGenericImpl<uint64_t, Range> {
  static inline Range Fn(uint64_t hash, Range range) {
    static_assert(std::is_unsigned<Range>::value, "must be unsigned");
    static_assert(sizeof(Range) <= sizeof(uint64_t),
                  "cannot be larger than hash (64 bits)");

#ifdef HAVE_UINT128_EXTENSION
    // Can use compiler's 128-bit type. Trust it to do the right thing.
    __uint128_t wide = __uint128_t{range} * hash;
    return static_cast<Range>(wide >> 64);
#else
    // Fall back: full decomposition.
    // NOTE: GCC seems to fully understand this code as 64-bit x 64-bit
    // -> 128-bit multiplication and optimize it appropriately
    uint64_t range64 = range;  // ok to shift by 32, even if Range is 32-bit
    uint64_t tmp = uint64_t{range64 & 0xffffFFFF} * uint64_t{hash & 0xffffFFFF};
    tmp >>= 32;
    tmp += uint64_t{range64 & 0xffffFFFF} * uint64_t{hash >> 32};
    // Avoid overflow: first add lower 32 of tmp2, and later upper 32
    uint64_t tmp2 = uint64_t{range64 >> 32} * uint64_t{hash & 0xffffFFFF};
    tmp += static_cast<uint32_t>(tmp2);
    tmp >>= 32;
    tmp += (tmp2 >> 32);
    tmp += uint64_t{range64 >> 32} * uint64_t{hash >> 32};
    return static_cast<Range>(tmp);
#endif
  }
};

}  // namespace detail

// Now an omnibus templated function (yay parameter inference).
//
// NOTICE:
// This templated version is not recommended for typical use because
// of the potential to mix a 64-bit FastRange with a 32-bit bit hash,
// most likely because you put your 32-bit hash in an "unsigned long"
// which is 64 bits on some platforms. That doesn't really matter for
// an operation like %, but 64-bit FastRange gives extremely bad results,
// mostly zero, on 32-bit hash values. And because good hashing is not
// generally required for correctness, this kind of mistake could go
// unnoticed with just unit tests. Plus it could vary by platform.
template <typename Hash, typename Range>
inline Range FastRangeGeneric(Hash hash, Range range) {
  return detail::FastRangeGenericImpl<Hash, Range>::Fn(hash, range);
}

// The most popular / convenient / recommended variants:

// Map a quality 64-bit hash value down to an arbitrary size_t range.
// (size_t is standard for mapping to things in memory.)
inline size_t FastRange64(uint64_t hash, size_t range) {
  return FastRangeGeneric(hash, range);
}

// Map a quality 32-bit hash value down to an arbitrary uint32_t range.
inline uint32_t FastRange32(uint32_t hash, uint32_t range) {
  return FastRangeGeneric(hash, range);
}

}  // namespace ROCKSDB_NAMESPACE
