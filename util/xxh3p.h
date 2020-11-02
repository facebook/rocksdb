//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
/*
   xxHash - Extremely Fast Hash algorithm
   Development source file for `xxh3`
   Copyright (C) 2019-present, Yann Collet.

   BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

       * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following disclaimer
   in the documentation and/or other materials provided with the
   distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   You can contact the author at :
   - xxHash source repository : https://github.com/Cyan4973/xxHash
*/

/* RocksDB Note: This file contains a preview release (xxhash repository
   version 0.7.2) of XXH3 that is unlikely to be compatible with the final
   version of XXH3. We have therefore renamed this XXH3p ("preview"), for
   clarity so that we can continue to use this version even after
   integrating a newer incompatible version.
*/

/* Note :
   This file is separated for development purposes.
   It will be integrated into `xxhash.c` when development phase is complete.
*/

#ifndef XXH3p_H
#define XXH3p_H


/* ===   Dependencies   === */

#undef XXH_INLINE_ALL   /* in case it's already defined */
#define XXH_INLINE_ALL
#include "xxhash.h"


/* ===   Compiler specifics   === */

#if defined (__STDC_VERSION__) && __STDC_VERSION__ >= 199901L   /* >= C99 */
#  define XXH_RESTRICT   restrict
#else
/* note : it might be useful to define __restrict or __restrict__ for some C++ compilers */
#  define XXH_RESTRICT   /* disable */
#endif

#if defined(__GNUC__)
#  if defined(__AVX2__)
#    include <immintrin.h>
#  elif defined(__SSE2__)
#    include <emmintrin.h>
#  elif defined(__ARM_NEON__) || defined(__ARM_NEON)
#    define inline __inline__  /* clang bug */
#    include <arm_neon.h>
#    undef inline
#  endif
#elif defined(_MSC_VER)
#  include <intrin.h>
#endif

/*
 * Sanity check.
 *
 * XXH3 only requires these features to be efficient:
 *
 *  - Usable unaligned access
 *  - A 32-bit or 64-bit ALU
 *      - If 32-bit, a decent ADC instruction
 *  - A 32 or 64-bit multiply with a 64-bit result
 *
 * Almost all 32-bit and 64-bit targets meet this, except for Thumb-1, the
 * classic 16-bit only subset of ARM's instruction set.
 *
 * First of all, Thumb-1 lacks support for the UMULL instruction which
 * performs the important long multiply. This means numerous __aeabi_lmul
 * calls.
 *
 * Second of all, the 8 functional registers are just not enough.
 * Setup for __aeabi_lmul, byteshift loads, pointers, and all arithmetic need
 * Lo registers, and this shuffling results in thousands more MOVs than A32.
 *
 * A32 and T32 don't have this limitation. They can access all 14 registers,
 * do a 32->64 multiply with UMULL, and the flexible operand is helpful too.
 *
 * If compiling Thumb-1 for a target which supports ARM instructions, we
 * will give a warning.
 *
 * Usually, if this happens, it is because of an accident and you probably
 * need to specify -march, as you probably meant to compileh for a newer
 * architecture.
 */
#if defined(__thumb__) && !defined(__thumb2__) && defined(__ARM_ARCH_ISA_ARM)
#   warning "XXH3 is highly inefficient without ARM or Thumb-2."
#endif

/* ==========================================
 * Vectorization detection
 * ========================================== */
#define XXH_SCALAR 0
#define XXH_SSE2   1
#define XXH_AVX2   2
#define XXH_NEON   3
#define XXH_VSX    4

#ifndef XXH_VECTOR    /* can be defined on command line */
#  if defined(__AVX2__)
#    define XXH_VECTOR XXH_AVX2
#  elif defined(__SSE2__) || defined(_M_AMD64) || defined(_M_X64) || (defined(_M_IX86_FP) && (_M_IX86_FP == 2))
#    define XXH_VECTOR XXH_SSE2
#  elif defined(__GNUC__) /* msvc support maybe later */ \
  && (defined(__ARM_NEON__) || defined(__ARM_NEON)) \
  && (defined(__LITTLE_ENDIAN__) /* We only support little endian NEON */ \
    || (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__))
#    define XXH_VECTOR XXH_NEON
#  elif defined(__PPC64__) && defined(__POWER8_VECTOR__) && defined(__GNUC__)
#    define XXH_VECTOR XXH_VSX
#  else
#    define XXH_VECTOR XXH_SCALAR
#  endif
#endif

/* control alignment of accumulator,
 * for compatibility with fast vector loads */
#ifndef XXH_ACC_ALIGN
#  if XXH_VECTOR == 0   /* scalar */
#     define XXH_ACC_ALIGN 8
#  elif XXH_VECTOR == 1  /* sse2 */
#     define XXH_ACC_ALIGN 16
#  elif XXH_VECTOR == 2  /* avx2 */
#     define XXH_ACC_ALIGN 32
#  elif XXH_VECTOR == 3  /* neon */
#     define XXH_ACC_ALIGN 16
#  elif XXH_VECTOR == 4  /* vsx */
#     define XXH_ACC_ALIGN 16
#  endif
#endif

/* xxh_u64 XXH_mult32to64(xxh_u32 a, xxh_u64 b) { return (xxh_u64)a * (xxh_u64)b; } */
#if defined(_MSC_VER) && defined(_M_IX86)
#    include <intrin.h>
#    define XXH_mult32to64(x, y) __emulu(x, y)
#else
#    define XXH_mult32to64(x, y) ((xxh_u64)((x) & 0xFFFFFFFF) * (xxh_u64)((y) & 0xFFFFFFFF))
#endif

/* VSX stuff. It's a lot because VSX support is mediocre across compilers and
 * there is a lot of mischief with endianness. */
#if XXH_VECTOR == XXH_VSX
#  include <altivec.h>
#  undef vector
typedef __vector unsigned long long U64x2;
typedef __vector unsigned char U8x16;
typedef __vector unsigned U32x4;

#ifndef XXH_VSX_BE
#  if defined(__BIG_ENDIAN__) \
  || (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#    define XXH_VSX_BE 1
#  elif defined(__VEC_ELEMENT_REG_ORDER__) && __VEC_ELEMENT_REG_ORDER__ == __ORDER_BIG_ENDIAN__
#    warning "-maltivec=be is not recommended. Please use native endianness."
#    define XXH_VSX_BE 1
#  else
#    define XXH_VSX_BE 0
#  endif
#endif

/* We need some helpers for big endian mode. */
#if XXH_VSX_BE
/* A wrapper for POWER9's vec_revb. */
#  ifdef __POWER9_VECTOR__
#    define XXH_vec_revb vec_revb
#  else
XXH_FORCE_INLINE U64x2 XXH_vec_revb(U64x2 val)
{
    U8x16 const vByteSwap = { 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00,
                              0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0x09, 0x08 };
    return vec_perm(val, val, vByteSwap);
}
#  endif

/* Power8 Crypto gives us vpermxor which is very handy for
 * PPC64EB.
 *
 * U8x16 vpermxor(U8x16 a, U8x16 b, U8x16 mask)
 * {
 *     U8x16 ret;
 *     for (int i = 0; i < 16; i++) {
 *         ret[i] = a[mask[i] & 0xF] ^ b[mask[i] >> 4];
 *     }
 *     return ret;
 * }
 *
 * Because both of the main loops load the key, swap, and xor it with input,
 * we can combine the key swap into this instruction.
 */
#  ifdef vec_permxor
#    define XXH_vec_permxor vec_permxor
#  else
#    define XXH_vec_permxor __builtin_crypto_vpermxor
#  endif
#endif  /* XXH_VSX_BE */
/*
 * Because we reinterpret the multiply, there are endian memes: vec_mulo actually becomes
 * vec_mule.
 *
 * Additionally, the intrinsic wasn't added until GCC 8, despite existing for a while.
 * Clang has an easy way to control this, we can just use the builtin which doesn't swap.
 * GCC needs inline assembly. */
#if __has_builtin(__builtin_altivec_vmuleuw)
#  define XXH_vec_mulo __builtin_altivec_vmulouw
#  define XXH_vec_mule __builtin_altivec_vmuleuw
#else
/* Adapted from https://github.com/google/highwayhash/blob/master/highwayhash/hh_vsx.h. */
XXH_FORCE_INLINE U64x2 XXH_vec_mulo(U32x4 a, U32x4 b) {
    U64x2 result;
    __asm__("vmulouw %0, %1, %2" : "=v" (result) : "v" (a), "v" (b));
    return result;
}
XXH_FORCE_INLINE U64x2 XXH_vec_mule(U32x4 a, U32x4 b) {
    U64x2 result;
    __asm__("vmuleuw %0, %1, %2" : "=v" (result) : "v" (a), "v" (b));
    return result;
}
#endif /* __has_builtin(__builtin_altivec_vmuleuw) */
#endif /* XXH_VECTOR == XXH_VSX */

/* prefetch
 * can be disabled, by declaring XXH_NO_PREFETCH build macro */
#if defined(XXH_NO_PREFETCH)
#  define XXH_PREFETCH(ptr)  (void)(ptr)  /* disabled */
#else
#  if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_I86))  /* _mm_prefetch() is not defined outside of x86/x64 */
#    include <mmintrin.h>   /* https://msdn.microsoft.com/fr-fr/library/84szxsww(v=vs.90).aspx */
#    define XXH_PREFETCH(ptr)  _mm_prefetch((const char*)(ptr), _MM_HINT_T0)
#  elif defined(__GNUC__) && ( (__GNUC__ >= 4) || ( (__GNUC__ == 3) && (__GNUC_MINOR__ >= 1) ) )
#    define XXH_PREFETCH(ptr)  __builtin_prefetch((ptr), 0 /* rw==read */, 3 /* locality */)
#  else
#    define XXH_PREFETCH(ptr) (void)(ptr)  /* disabled */
#  endif
#endif  /* XXH_NO_PREFETCH */


/* ==========================================
 * XXH3 default settings
 * ========================================== */

#define XXH_SECRET_DEFAULT_SIZE 192   /* minimum XXH3p_SECRET_SIZE_MIN */

#if (XXH_SECRET_DEFAULT_SIZE < XXH3p_SECRET_SIZE_MIN)
#  error "default keyset is not large enough"
#endif

XXH_ALIGN(64) static const xxh_u8 kSecret[XXH_SECRET_DEFAULT_SIZE] = {
    0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c,
    0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f,
    0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21,
    0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c,
    0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3,
    0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8,
    0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d,
    0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64,

    0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
    0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e,
    0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce,
    0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e,
};

/*
 * GCC for x86 has a tendency to use SSE in this loop. While it
 * successfully avoids swapping (as MUL overwrites EAX and EDX), it
 * slows it down because instead of free register swap shifts, it
 * must use pshufd and punpckl/hd.
 *
 * To prevent this, we use this attribute to shut off SSE.
 */
#if defined(__GNUC__) && !defined(__clang__) && defined(__i386__)
__attribute__((__target__("no-sse")))
#endif
static XXH128_hash_t
XXH_mult64to128(xxh_u64 lhs, xxh_u64 rhs)
{
    /*
     * GCC/Clang __uint128_t method.
     *
     * On most 64-bit targets, GCC and Clang define a __uint128_t type.
     * This is usually the best way as it usually uses a native long 64-bit
     * multiply, such as MULQ on x86_64 or MUL + UMULH on aarch64.
     *
     * Usually.
     *
     * Despite being a 32-bit platform, Clang (and emscripten) define this
     * type despite not having the arithmetic for it. This results in a
     * laggy compiler builtin call which calculates a full 128-bit multiply.
     * In that case it is best to use the portable one.
     * https://github.com/Cyan4973/xxHash/issues/211#issuecomment-515575677
     */
#if defined(__GNUC__) && !defined(__wasm__) \
    && defined(__SIZEOF_INT128__) \
    || (defined(_INTEGRAL_MAX_BITS) && _INTEGRAL_MAX_BITS >= 128)

    __uint128_t product = (__uint128_t)lhs * (__uint128_t)rhs;
    XXH128_hash_t const r128 = { (xxh_u64)(product), (xxh_u64)(product >> 64) };
    return r128;

    /*
     * MSVC for x64's _umul128 method.
     *
     * xxh_u64 _umul128(xxh_u64 Multiplier, xxh_u64 Multiplicand, xxh_u64 *HighProduct);
     *
     * This compiles to single operand MUL on x64.
     */
#elif defined(_M_X64) || defined(_M_IA64)

#ifndef _MSC_VER
#   pragma intrinsic(_umul128)
#endif
    xxh_u64 product_high;
    xxh_u64 const product_low = _umul128(lhs, rhs, &product_high);
    XXH128_hash_t const r128 = { product_low, product_high };
    return r128;

#else
    /*
     * Portable scalar method. Optimized for 32-bit and 64-bit ALUs.
     *
     * This is a fast and simple grade school multiply, which is shown
     * below with base 10 arithmetic instead of base 0x100000000.
     *
     *           9 3 // D2 lhs = 93
     *         x 7 5 // D2 rhs = 75
     *     ----------
     *           1 5 // D2 lo_lo = (93 % 10) * (75 % 10)
     *         4 5 | // D2 hi_lo = (93 / 10) * (75 % 10)
     *         2 1 | // D2 lo_hi = (93 % 10) * (75 / 10)
     *     + 6 3 | | // D2 hi_hi = (93 / 10) * (75 / 10)
     *     ---------
     *         2 7 | // D2 cross  = (15 / 10) + (45 % 10) + 21
     *     + 6 7 | | // D2 upper  = (27 / 10) + (45 / 10) + 63
     *     ---------
     *       6 9 7 5
     *
     * The reasons for adding the products like this are:
     *  1. It avoids manual carry tracking. Just like how
     *     (9 * 9) + 9 + 9 = 99, the same applies with this for
     *     UINT64_MAX. This avoids a lot of complexity.
     *
     *  2. It hints for, and on Clang, compiles to, the powerful UMAAL
     *     instruction available in ARMv6+ A32/T32, which is shown below:
     *
     *         void UMAAL(xxh_u32 *RdLo, xxh_u32 *RdHi, xxh_u32 Rn, xxh_u32 Rm)
     *         {
     *             xxh_u64 product = (xxh_u64)*RdLo * (xxh_u64)*RdHi + Rn + Rm;
     *             *RdLo = (xxh_u32)(product & 0xFFFFFFFF);
     *             *RdHi = (xxh_u32)(product >> 32);
     *         }
     *
     *     This instruction was designed for efficient long multiplication,
     *     and allows this to be calculated in only 4 instructions which
     *     is comparable to some 64-bit ALUs.
     *
     *  3. It isn't terrible on other platforms. Usually this will be
     *     a couple of 32-bit ADD/ADCs.
     */

    /* First calculate all of the cross products. */
    xxh_u64 const lo_lo = XXH_mult32to64(lhs & 0xFFFFFFFF, rhs & 0xFFFFFFFF);
    xxh_u64 const hi_lo = XXH_mult32to64(lhs >> 32,        rhs & 0xFFFFFFFF);
    xxh_u64 const lo_hi = XXH_mult32to64(lhs & 0xFFFFFFFF, rhs >> 32);
    xxh_u64 const hi_hi = XXH_mult32to64(lhs >> 32,        rhs >> 32);

    /* Now add the products together. These will never overflow. */
    xxh_u64 const cross = (lo_lo >> 32) + (hi_lo & 0xFFFFFFFF) + lo_hi;
    xxh_u64 const upper = (hi_lo >> 32) + (cross >> 32)        + hi_hi;
    xxh_u64 const lower = (cross << 32) | (lo_lo & 0xFFFFFFFF);

    XXH128_hash_t r128 = { lower, upper };
    return r128;
#endif
}

/*
 * We want to keep the attribute here because a target switch
 * disables inlining.
 *
 * Does a 64-bit to 128-bit multiply, then XOR folds it.
 * The reason for the separate function is to prevent passing
 * too many structs around by value. This will hopefully inline
 * the multiply, but we don't force it.
 */
#if defined(__GNUC__) && !defined(__clang__) && defined(__i386__)
__attribute__((__target__("no-sse")))
#endif
static xxh_u64
XXH3p_mul128_fold64(xxh_u64 lhs, xxh_u64 rhs)
{
    XXH128_hash_t product = XXH_mult64to128(lhs, rhs);
    return product.low64 ^ product.high64;
}


static XXH64_hash_t XXH3p_avalanche(xxh_u64 h64)
{
    h64 ^= h64 >> 37;
    h64 *= PRIME64_3;
    h64 ^= h64 >> 32;
    return h64;
}


/* ==========================================
 * Short keys
 * ========================================== */

XXH_FORCE_INLINE XXH64_hash_t
XXH3p_len_1to3_64b(const xxh_u8* input, size_t len, const xxh_u8* secret, XXH64_hash_t seed)
{
    XXH_ASSERT(input != NULL);
    XXH_ASSERT(1 <= len && len <= 3);
    XXH_ASSERT(secret != NULL);
    {   xxh_u8 const c1 = input[0];
        xxh_u8 const c2 = input[len >> 1];
        xxh_u8 const c3 = input[len - 1];
        xxh_u32  const combined = ((xxh_u32)c1) | (((xxh_u32)c2) << 8) | (((xxh_u32)c3) << 16) | (((xxh_u32)len) << 24);
        xxh_u64  const keyed = (xxh_u64)combined ^ (XXH_readLE32(secret) + seed);
        xxh_u64  const mixed = keyed * PRIME64_1;
        return XXH3p_avalanche(mixed);
    }
}

XXH_FORCE_INLINE XXH64_hash_t
XXH3p_len_4to8_64b(const xxh_u8* input, size_t len, const xxh_u8* secret, XXH64_hash_t seed)
{
    XXH_ASSERT(input != NULL);
    XXH_ASSERT(secret != NULL);
    XXH_ASSERT(4 <= len && len <= 8);
    {   xxh_u32 const input_lo = XXH_readLE32(input);
        xxh_u32 const input_hi = XXH_readLE32(input + len - 4);
        xxh_u64 const input_64 = input_lo | ((xxh_u64)input_hi << 32);
        xxh_u64 const keyed = input_64 ^ (XXH_readLE64(secret) + seed);
        xxh_u64 const mix64 = len + ((keyed ^ (keyed >> 51)) * PRIME32_1);
        return XXH3p_avalanche((mix64 ^ (mix64 >> 47)) * PRIME64_2);
    }
}

XXH_FORCE_INLINE XXH64_hash_t
XXH3p_len_9to16_64b(const xxh_u8* input, size_t len, const xxh_u8* secret, XXH64_hash_t seed)
{
    XXH_ASSERT(input != NULL);
    XXH_ASSERT(secret != NULL);
    XXH_ASSERT(9 <= len && len <= 16);
    {   xxh_u64 const input_lo = XXH_readLE64(input)           ^ (XXH_readLE64(secret)     + seed);
        xxh_u64 const input_hi = XXH_readLE64(input + len - 8) ^ (XXH_readLE64(secret + 8) - seed);
        xxh_u64 const acc = len + (input_lo + input_hi) + XXH3p_mul128_fold64(input_lo, input_hi);
        return XXH3p_avalanche(acc);
    }
}

XXH_FORCE_INLINE XXH64_hash_t
XXH3p_len_0to16_64b(const xxh_u8* input, size_t len, const xxh_u8* secret, XXH64_hash_t seed)
{
    XXH_ASSERT(len <= 16);
    {   if (len > 8) return XXH3p_len_9to16_64b(input, len, secret, seed);
        if (len >= 4) return XXH3p_len_4to8_64b(input, len, secret, seed);
        if (len) return XXH3p_len_1to3_64b(input, len, secret, seed);
        /*
         * RocksDB modification from XXH3 preview: zero result for empty
         * string can be problematic for multiplication-based algorithms.
         * Return a hash of the seed instead.
         */
        return XXH3p_mul128_fold64(seed + XXH_readLE64(secret), PRIME64_2);
    }
}


/* ===    Long Keys    === */

#define STRIPE_LEN 64
#define XXH_SECRET_CONSUME_RATE 8   /* nb of secret bytes consumed at each accumulation */
#define ACC_NB (STRIPE_LEN / sizeof(xxh_u64))

typedef enum { XXH3p_acc_64bits, XXH3p_acc_128bits } XXH3p_accWidth_e;

XXH_FORCE_INLINE void
XXH3p_accumulate_512(      void* XXH_RESTRICT acc,
                    const void* XXH_RESTRICT input,
                    const void* XXH_RESTRICT secret,
                    XXH3p_accWidth_e accWidth)
{
#if (XXH_VECTOR == XXH_AVX2)

    XXH_ASSERT((((size_t)acc) & 31) == 0);
    {   XXH_ALIGN(32) __m256i* const xacc  =       (__m256i *) acc;
        const         __m256i* const xinput = (const __m256i *) input;  /* not really aligned, just for ptr arithmetic, and because _mm256_loadu_si256() requires this type */
        const         __m256i* const xsecret = (const __m256i *) secret;   /* not really aligned, just for ptr arithmetic, and because _mm256_loadu_si256() requires this type */

        size_t i;
        for (i=0; i < STRIPE_LEN/sizeof(__m256i); i++) {
            __m256i const data_vec = _mm256_loadu_si256 (xinput+i);
            __m256i const key_vec = _mm256_loadu_si256 (xsecret+i);
            __m256i const data_key = _mm256_xor_si256 (data_vec, key_vec);                                  /* uint32 dk[8]  = {d0+k0, d1+k1, d2+k2, d3+k3, ...} */
            __m256i const product = _mm256_mul_epu32 (data_key, _mm256_shuffle_epi32 (data_key, 0x31));  /* uint64 mul[4] = {dk0*dk1, dk2*dk3, ...} */
            if (accWidth == XXH3p_acc_128bits) {
                __m256i const data_swap = _mm256_shuffle_epi32(data_vec, _MM_SHUFFLE(1,0,3,2));
                __m256i const sum = _mm256_add_epi64(xacc[i], data_swap);
                xacc[i]  = _mm256_add_epi64(product, sum);
            } else {  /* XXH3p_acc_64bits */
                __m256i const sum = _mm256_add_epi64(xacc[i], data_vec);
                xacc[i]  = _mm256_add_epi64(product, sum);
            }
    }   }

#elif (XXH_VECTOR == XXH_SSE2)

    XXH_ASSERT((((size_t)acc) & 15) == 0);
    {   XXH_ALIGN(16) __m128i* const xacc  =       (__m128i *) acc;
        const         __m128i* const xinput = (const __m128i *) input;  /* not really aligned, just for ptr arithmetic, and because _mm_loadu_si128() requires this type */
        const         __m128i* const xsecret = (const __m128i *) secret;   /* not really aligned, just for ptr arithmetic, and because _mm_loadu_si128() requires this type */

        size_t i;
        for (i=0; i < STRIPE_LEN/sizeof(__m128i); i++) {
            __m128i const data_vec = _mm_loadu_si128 (xinput+i);
            __m128i const key_vec = _mm_loadu_si128 (xsecret+i);
            __m128i const data_key = _mm_xor_si128 (data_vec, key_vec);                                  /* uint32 dk[8]  = {d0+k0, d1+k1, d2+k2, d3+k3, ...} */
            __m128i const product = _mm_mul_epu32 (data_key, _mm_shuffle_epi32 (data_key, 0x31));  /* uint64 mul[4] = {dk0*dk1, dk2*dk3, ...} */
            if (accWidth == XXH3p_acc_128bits) {
                __m128i const data_swap = _mm_shuffle_epi32(data_vec, _MM_SHUFFLE(1,0,3,2));
                __m128i const sum = _mm_add_epi64(xacc[i], data_swap);
                xacc[i]  = _mm_add_epi64(product, sum);
            } else {  /* XXH3p_acc_64bits */
                __m128i const sum = _mm_add_epi64(xacc[i], data_vec);
                xacc[i]  = _mm_add_epi64(product, sum);
            }
    }   }

#elif (XXH_VECTOR == XXH_NEON)

    XXH_ASSERT((((size_t)acc) & 15) == 0);
    {
        XXH_ALIGN(16) uint64x2_t* const xacc = (uint64x2_t *) acc;
        /* We don't use a uint32x4_t pointer because it causes bus errors on ARMv7. */
        uint8_t const* const xinput = (const uint8_t *) input;
        uint8_t const* const xsecret  = (const uint8_t *) secret;

        size_t i;
        for (i=0; i < STRIPE_LEN / sizeof(uint64x2_t); i++) {
#if !defined(__aarch64__) && !defined(__arm64__) && defined(__GNUC__) /* ARM32-specific hack */
            /* vzip on ARMv7 Clang generates a lot of vmovs (technically vorrs) without this.
             * vzip on 32-bit ARM NEON will overwrite the original register, and I think that Clang
             * assumes I don't want to destroy it and tries to make a copy. This slows down the code
             * a lot.
             * aarch64 not only uses an entirely different syntax, but it requires three
             * instructions...
             *    ext    v1.16B, v0.16B, #8    // select high bits because aarch64 can't address them directly
             *    zip1   v3.2s, v0.2s, v1.2s   // first zip
             *    zip2   v2.2s, v0.2s, v1.2s   // second zip
             * ...to do what ARM does in one:
             *    vzip.32 d0, d1               // Interleave high and low bits and overwrite. */

            /* data_vec = xsecret[i]; */
            uint8x16_t const data_vec    = vld1q_u8(xinput + (i * 16));
            /* key_vec  = xsecret[i];  */
            uint8x16_t const key_vec     = vld1q_u8(xsecret  + (i * 16));
            /* data_key = data_vec ^ key_vec; */
            uint32x4_t       data_key;

            if (accWidth == XXH3p_acc_64bits) {
                /* Add first to prevent register swaps */
                /* xacc[i] += data_vec; */
                xacc[i] = vaddq_u64 (xacc[i], vreinterpretq_u64_u8(data_vec));
            } else {  /* XXH3p_acc_128bits */
                /* xacc[i] += swap(data_vec); */
                /* can probably be optimized better */
                uint64x2_t const data64 = vreinterpretq_u64_u8(data_vec);
                uint64x2_t const swapped= vextq_u64(data64, data64, 1);
                xacc[i] = vaddq_u64 (xacc[i], swapped);
            }

            data_key = vreinterpretq_u32_u8(veorq_u8(data_vec, key_vec));

            /* Here's the magic. We use the quirkiness of vzip to shuffle data_key in place.
             * shuffle: data_key[0, 1, 2, 3] = data_key[0, 2, 1, 3] */
            __asm__("vzip.32 %e0, %f0" : "+w" (data_key));
            /* xacc[i] += (uint64x2_t) data_key[0, 1] * (uint64x2_t) data_key[2, 3]; */
            xacc[i] = vmlal_u32(xacc[i], vget_low_u32(data_key), vget_high_u32(data_key));

#else
            /* On aarch64, vshrn/vmovn seems to be equivalent to, if not faster than, the vzip method. */

            /* data_vec = xsecret[i]; */
            uint8x16_t const data_vec    = vld1q_u8(xinput + (i * 16));
            /* key_vec  = xsecret[i];  */
            uint8x16_t const key_vec     = vld1q_u8(xsecret  + (i * 16));
            /* data_key = data_vec ^ key_vec; */
            uint64x2_t const data_key    = vreinterpretq_u64_u8(veorq_u8(data_vec, key_vec));
            /* data_key_lo = (uint32x2_t) (data_key & 0xFFFFFFFF); */
            uint32x2_t const data_key_lo = vmovn_u64  (data_key);
            /* data_key_hi = (uint32x2_t) (data_key >> 32); */
            uint32x2_t const data_key_hi = vshrn_n_u64 (data_key, 32);
            if (accWidth == XXH3p_acc_64bits) {
                /* xacc[i] += data_vec; */
                xacc[i] = vaddq_u64 (xacc[i], vreinterpretq_u64_u8(data_vec));
            } else {  /* XXH3p_acc_128bits */
                /* xacc[i] += swap(data_vec); */
                uint64x2_t const data64 = vreinterpretq_u64_u8(data_vec);
                uint64x2_t const swapped= vextq_u64(data64, data64, 1);
                xacc[i] = vaddq_u64 (xacc[i], swapped);
            }
            /* xacc[i] += (uint64x2_t) data_key_lo * (uint64x2_t) data_key_hi; */
            xacc[i] = vmlal_u32 (xacc[i], data_key_lo, data_key_hi);

#endif
        }
    }

#elif (XXH_VECTOR == XXH_VSX) && /* work around a compiler bug */ (__GNUC__ > 5)
          U64x2* const xacc =        (U64x2*) acc;    /* presumed aligned */
    U64x2 const* const xinput = (U64x2 const*) input;   /* no alignment restriction */
    U64x2 const* const xsecret  = (U64x2 const*) secret;    /* no alignment restriction */
    U64x2 const v32 = { 32,  32 };
#if XXH_VSX_BE
    U8x16 const vXorSwap  = { 0x07, 0x16, 0x25, 0x34, 0x43, 0x52, 0x61, 0x70,
                              0x8F, 0x9E, 0xAD, 0xBC, 0xCB, 0xDA, 0xE9, 0xF8 };
#endif
    size_t i;
    for (i = 0; i < STRIPE_LEN / sizeof(U64x2); i++) {
        /* data_vec = xinput[i]; */
        /* key_vec = xsecret[i]; */
#if XXH_VSX_BE
        /* byteswap */
        U64x2 const data_vec = XXH_vec_revb(vec_vsx_ld(0, xinput + i));
        U64x2 const key_raw = vec_vsx_ld(0, xsecret + i);
        /* See comment above. data_key = data_vec ^ swap(xsecret[i]); */
        U64x2 const data_key = (U64x2)XXH_vec_permxor((U8x16)data_vec, (U8x16)key_raw, vXorSwap);
#else
        U64x2 const data_vec = vec_vsx_ld(0, xinput + i);
        U64x2 const key_vec = vec_vsx_ld(0, xsecret + i);
        U64x2 const data_key = data_vec ^ key_vec;
#endif
        /* shuffled = (data_key << 32) | (data_key >> 32); */
        U32x4 const shuffled = (U32x4)vec_rl(data_key, v32);
        /* product = ((U64x2)data_key & 0xFFFFFFFF) * ((U64x2)shuffled & 0xFFFFFFFF); */
        U64x2 const product = XXH_vec_mulo((U32x4)data_key, shuffled);
        xacc[i] += product;

        if (accWidth == XXH3p_acc_64bits) {
            xacc[i] += data_vec;
        } else {  /* XXH3p_acc_128bits */
            /* swap high and low halves */
            U64x2 const data_swapped = vec_xxpermdi(data_vec, data_vec, 2);
            xacc[i] += data_swapped;
        }
    }

#else   /* scalar variant of Accumulator - universal */

    XXH_ALIGN(XXH_ACC_ALIGN) xxh_u64* const xacc = (xxh_u64*) acc;    /* presumed aligned on 32-bytes boundaries, little hint for the auto-vectorizer */
    const xxh_u8* const xinput = (const xxh_u8*) input;  /* no alignment restriction */
    const xxh_u8* const xsecret  = (const xxh_u8*) secret;   /* no alignment restriction */
    size_t i;
    XXH_ASSERT(((size_t)acc & (XXH_ACC_ALIGN-1)) == 0);
    for (i=0; i < ACC_NB; i++) {
        xxh_u64 const data_val = XXH_readLE64(xinput + 8*i);
        xxh_u64 const data_key = data_val ^ XXH_readLE64(xsecret + i*8);

        if (accWidth == XXH3p_acc_64bits) {
            xacc[i] += data_val;
        } else {
            xacc[i ^ 1] += data_val; /* swap adjacent lanes */
        }
        xacc[i] += XXH_mult32to64(data_key & 0xFFFFFFFF, data_key >> 32);
    }
#endif
}

XXH_FORCE_INLINE void
XXH3p_scrambleAcc(void* XXH_RESTRICT acc, const void* XXH_RESTRICT secret)
{
#if (XXH_VECTOR == XXH_AVX2)

    XXH_ASSERT((((size_t)acc) & 31) == 0);
    {   XXH_ALIGN(32) __m256i* const xacc = (__m256i*) acc;
        const         __m256i* const xsecret = (const __m256i *) secret;   /* not really aligned, just for ptr arithmetic, and because _mm256_loadu_si256() requires this argument type */
        const __m256i prime32 = _mm256_set1_epi32((int)PRIME32_1);

        size_t i;
        for (i=0; i < STRIPE_LEN/sizeof(__m256i); i++) {
            /* xacc[i] ^= (xacc[i] >> 47) */
            __m256i const acc_vec     = xacc[i];
            __m256i const shifted     = _mm256_srli_epi64    (acc_vec, 47);
            __m256i const data_vec    = _mm256_xor_si256     (acc_vec, shifted);
            /* xacc[i] ^= xsecret; */
            __m256i const key_vec     = _mm256_loadu_si256   (xsecret+i);
            __m256i const data_key    = _mm256_xor_si256     (data_vec, key_vec);

            /* xacc[i] *= PRIME32_1; */
            __m256i const data_key_hi = _mm256_shuffle_epi32 (data_key, 0x31);
            __m256i const prod_lo     = _mm256_mul_epu32     (data_key, prime32);
            __m256i const prod_hi     = _mm256_mul_epu32     (data_key_hi, prime32);
            xacc[i] = _mm256_add_epi64(prod_lo, _mm256_slli_epi64(prod_hi, 32));
        }
    }

#elif (XXH_VECTOR == XXH_SSE2)

    XXH_ASSERT((((size_t)acc) & 15) == 0);
    {   XXH_ALIGN(16) __m128i* const xacc = (__m128i*) acc;
        const         __m128i* const xsecret = (const __m128i *) secret;   /* not really aligned, just for ptr arithmetic, and because _mm_loadu_si128() requires this argument type */
        const __m128i prime32 = _mm_set1_epi32((int)PRIME32_1);

        size_t i;
        for (i=0; i < STRIPE_LEN/sizeof(__m128i); i++) {
            /* xacc[i] ^= (xacc[i] >> 47) */
            __m128i const acc_vec     = xacc[i];
            __m128i const shifted     = _mm_srli_epi64    (acc_vec, 47);
            __m128i const data_vec    = _mm_xor_si128     (acc_vec, shifted);
            /* xacc[i] ^= xsecret; */
            __m128i const key_vec     = _mm_loadu_si128   (xsecret+i);
            __m128i const data_key    = _mm_xor_si128     (data_vec, key_vec);

            /* xacc[i] *= PRIME32_1; */
            __m128i const data_key_hi = _mm_shuffle_epi32 (data_key, 0x31);
            __m128i const prod_lo     = _mm_mul_epu32     (data_key, prime32);
            __m128i const prod_hi     = _mm_mul_epu32     (data_key_hi, prime32);
            xacc[i] = _mm_add_epi64(prod_lo, _mm_slli_epi64(prod_hi, 32));
        }
    }

#elif (XXH_VECTOR == XXH_NEON)

    XXH_ASSERT((((size_t)acc) & 15) == 0);

    {   uint64x2_t* const xacc =     (uint64x2_t*) acc;
        uint8_t const* const xsecret = (uint8_t const*) secret;
        uint32x2_t const prime     = vdup_n_u32 (PRIME32_1);

        size_t i;
        for (i=0; i < STRIPE_LEN/sizeof(uint64x2_t); i++) {
            /* data_vec = xacc[i] ^ (xacc[i] >> 47); */
            uint64x2_t const   acc_vec  = xacc[i];
            uint64x2_t const   shifted  = vshrq_n_u64 (acc_vec, 47);
            uint64x2_t const   data_vec = veorq_u64   (acc_vec, shifted);

            /* key_vec  = xsecret[i]; */
            uint32x4_t const   key_vec  = vreinterpretq_u32_u8(vld1q_u8(xsecret + (i * 16)));
            /* data_key = data_vec ^ key_vec; */
            uint32x4_t const   data_key = veorq_u32   (vreinterpretq_u32_u64(data_vec), key_vec);
            /* shuffled = { data_key[0, 2], data_key[1, 3] }; */
            uint32x2x2_t const shuffled = vzip_u32    (vget_low_u32(data_key), vget_high_u32(data_key));

            /* data_key *= PRIME32_1 */

            /* prod_hi = (data_key >> 32) * PRIME32_1; */
            uint64x2_t const   prod_hi = vmull_u32    (shuffled.val[1], prime);
            /* xacc[i] = prod_hi << 32; */
            xacc[i] = vshlq_n_u64(prod_hi, 32);
            /* xacc[i] += (prod_hi & 0xFFFFFFFF) * PRIME32_1; */
            xacc[i] = vmlal_u32(xacc[i], shuffled.val[0], prime);
    }   }

#elif (XXH_VECTOR == XXH_VSX) && /* work around a compiler bug */ (__GNUC__ > 5)

          U64x2* const xacc =       (U64x2*) acc;
    const U64x2* const xsecret = (const U64x2*) secret;
    /* constants */
    U64x2 const v32  = { 32, 32 };
    U64x2 const v47 = { 47, 47 };
    U32x4 const prime = { PRIME32_1, PRIME32_1, PRIME32_1, PRIME32_1 };
    size_t i;
#if XXH_VSX_BE
    /* endian swap */
    U8x16 const vXorSwap  = { 0x07, 0x16, 0x25, 0x34, 0x43, 0x52, 0x61, 0x70,
                              0x8F, 0x9E, 0xAD, 0xBC, 0xCB, 0xDA, 0xE9, 0xF8 };
#endif
    for (i = 0; i < STRIPE_LEN / sizeof(U64x2); i++) {
        U64x2 const acc_vec  = xacc[i];
        U64x2 const data_vec = acc_vec ^ (acc_vec >> v47);
        /* key_vec = xsecret[i]; */
#if XXH_VSX_BE
        /* swap bytes words */
        U64x2 const key_raw  = vec_vsx_ld(0, xsecret + i);
        U64x2 const data_key = (U64x2)XXH_vec_permxor((U8x16)data_vec, (U8x16)key_raw, vXorSwap);
#else
        U64x2 const key_vec  = vec_vsx_ld(0, xsecret + i);
        U64x2 const data_key = data_vec ^ key_vec;
#endif

        /* data_key *= PRIME32_1 */

        /* prod_lo = ((U64x2)data_key & 0xFFFFFFFF) * ((U64x2)prime & 0xFFFFFFFF);  */
        U64x2 const prod_even  = XXH_vec_mule((U32x4)data_key, prime);
        /* prod_hi = ((U64x2)data_key >> 32) * ((U64x2)prime >> 32);  */
        U64x2 const prod_odd  = XXH_vec_mulo((U32x4)data_key, prime);
        xacc[i] = prod_odd + (prod_even << v32);
    }

#else   /* scalar variant of Scrambler - universal */

    XXH_ALIGN(XXH_ACC_ALIGN) xxh_u64* const xacc = (xxh_u64*) acc;   /* presumed aligned on 32-bytes boundaries, little hint for the auto-vectorizer */
    const xxh_u8* const xsecret = (const xxh_u8*) secret;   /* no alignment restriction */
    size_t i;
    XXH_ASSERT((((size_t)acc) & (XXH_ACC_ALIGN-1)) == 0);
    for (i=0; i < ACC_NB; i++) {
        xxh_u64 const key64 = XXH_readLE64(xsecret + 8*i);
        xxh_u64 acc64 = xacc[i];
        acc64 ^= acc64 >> 47;
        acc64 ^= key64;
        acc64 *= PRIME32_1;
        xacc[i] = acc64;
    }

#endif
}

#define XXH_PREFETCH_DIST 384

/* assumption : nbStripes will not overflow secret size */
XXH_FORCE_INLINE void
XXH3p_accumulate(       xxh_u64* XXH_RESTRICT acc,
                const xxh_u8* XXH_RESTRICT input,
                const xxh_u8* XXH_RESTRICT secret,
                      size_t nbStripes,
                      XXH3p_accWidth_e accWidth)
{
    size_t n;
    for (n = 0; n < nbStripes; n++ ) {
        const xxh_u8* const in = input + n*STRIPE_LEN;
        XXH_PREFETCH(in + XXH_PREFETCH_DIST);
        XXH3p_accumulate_512(acc,
                            in,
                            secret + n*XXH_SECRET_CONSUME_RATE,
                            accWidth);
    }
}

/* note : clang auto-vectorizes well in SS2 mode _if_ this function is `static`,
 *        and doesn't auto-vectorize it at all if it is `FORCE_INLINE`.
 *        However, it auto-vectorizes better AVX2 if it is `FORCE_INLINE`
 *        Pretty much every other modes and compilers prefer `FORCE_INLINE`.
 */

#if defined(__clang__) && (XXH_VECTOR==0) && !defined(__AVX2__) && !defined(__arm__) && !defined(__thumb__)
static void
#else
XXH_FORCE_INLINE void
#endif
XXH3p_hashLong_internal_loop( xxh_u64* XXH_RESTRICT acc,
                      const xxh_u8* XXH_RESTRICT input, size_t len,
                      const xxh_u8* XXH_RESTRICT secret, size_t secretSize,
                            XXH3p_accWidth_e accWidth)
{
    size_t const nb_rounds = (secretSize - STRIPE_LEN) / XXH_SECRET_CONSUME_RATE;
    size_t const block_len = STRIPE_LEN * nb_rounds;
    size_t const nb_blocks = len / block_len;

    size_t n;

    XXH_ASSERT(secretSize >= XXH3p_SECRET_SIZE_MIN);

    for (n = 0; n < nb_blocks; n++) {
        XXH3p_accumulate(acc, input + n*block_len, secret, nb_rounds, accWidth);
        XXH3p_scrambleAcc(acc, secret + secretSize - STRIPE_LEN);
    }

    /* last partial block */
    XXH_ASSERT(len > STRIPE_LEN);
    {   size_t const nbStripes = (len - (block_len * nb_blocks)) / STRIPE_LEN;
        XXH_ASSERT(nbStripes <= (secretSize / XXH_SECRET_CONSUME_RATE));
        XXH3p_accumulate(acc, input + nb_blocks*block_len, secret, nbStripes, accWidth);

        /* last stripe */
        if (len & (STRIPE_LEN - 1)) {
            const xxh_u8* const p = input + len - STRIPE_LEN;
#define XXH_SECRET_LASTACC_START 7  /* do not align on 8, so that secret is different from scrambler */
            XXH3p_accumulate_512(acc, p, secret + secretSize - STRIPE_LEN - XXH_SECRET_LASTACC_START, accWidth);
    }   }
}

XXH_FORCE_INLINE xxh_u64
XXH3p_mix2Accs(const xxh_u64* XXH_RESTRICT acc, const xxh_u8* XXH_RESTRICT secret)
{
    return XXH3p_mul128_fold64(
               acc[0] ^ XXH_readLE64(secret),
               acc[1] ^ XXH_readLE64(secret+8) );
}

static XXH64_hash_t
XXH3p_mergeAccs(const xxh_u64* XXH_RESTRICT acc, const xxh_u8* XXH_RESTRICT secret, xxh_u64 start)
{
    xxh_u64 result64 = start;

    result64 += XXH3p_mix2Accs(acc+0, secret +  0);
    result64 += XXH3p_mix2Accs(acc+2, secret + 16);
    result64 += XXH3p_mix2Accs(acc+4, secret + 32);
    result64 += XXH3p_mix2Accs(acc+6, secret + 48);

    return XXH3p_avalanche(result64);
}

#define XXH3p_INIT_ACC { PRIME32_3, PRIME64_1, PRIME64_2, PRIME64_3, \
                        PRIME64_4, PRIME32_2, PRIME64_5, PRIME32_1 };

XXH_FORCE_INLINE XXH64_hash_t
XXH3p_hashLong_internal(const xxh_u8* XXH_RESTRICT input, size_t len,
                       const xxh_u8* XXH_RESTRICT secret, size_t secretSize)
{
    XXH_ALIGN(XXH_ACC_ALIGN) xxh_u64 acc[ACC_NB] = XXH3p_INIT_ACC;

    XXH3p_hashLong_internal_loop(acc, input, len, secret, secretSize, XXH3p_acc_64bits);

    /* converge into final hash */
    XXH_STATIC_ASSERT(sizeof(acc) == 64);
#define XXH_SECRET_MERGEACCS_START 11  /* do not align on 8, so that secret is different from accumulator */
    XXH_ASSERT(secretSize >= sizeof(acc) + XXH_SECRET_MERGEACCS_START);
    return XXH3p_mergeAccs(acc, secret + XXH_SECRET_MERGEACCS_START, (xxh_u64)len * PRIME64_1);
}


XXH_NO_INLINE XXH64_hash_t    /* It's important for performance that XXH3p_hashLong is not inlined. Not sure why (uop cache maybe ?), but difference is large and easily measurable */
XXH3p_hashLong_64b_defaultSecret(const xxh_u8* XXH_RESTRICT input, size_t len)
{
    return XXH3p_hashLong_internal(input, len, kSecret, sizeof(kSecret));
}

XXH_NO_INLINE XXH64_hash_t    /* It's important for performance that XXH3p_hashLong is not inlined. Not sure why (uop cache maybe ?), but difference is large and easily measurable */
XXH3p_hashLong_64b_withSecret(const xxh_u8* XXH_RESTRICT input, size_t len,
                             const xxh_u8* XXH_RESTRICT secret, size_t secretSize)
{
    return XXH3p_hashLong_internal(input, len, secret, secretSize);
}


XXH_FORCE_INLINE void XXH_writeLE64(void* dst, xxh_u64 v64)
{
    if (!XXH_CPU_LITTLE_ENDIAN) v64 = XXH_swap64(v64);
    memcpy(dst, &v64, sizeof(v64));
}

/* XXH3p_initCustomSecret() :
 * destination `customSecret` is presumed allocated and same size as `kSecret`.
 */
XXH_FORCE_INLINE void XXH3p_initCustomSecret(xxh_u8* customSecret, xxh_u64 seed64)
{
    int const nbRounds = XXH_SECRET_DEFAULT_SIZE / 16;
    int i;

    XXH_STATIC_ASSERT((XXH_SECRET_DEFAULT_SIZE & 15) == 0);

    for (i=0; i < nbRounds; i++) {
        XXH_writeLE64(customSecret + 16*i,     XXH_readLE64(kSecret + 16*i)     + seed64);
        XXH_writeLE64(customSecret + 16*i + 8, XXH_readLE64(kSecret + 16*i + 8) - seed64);
    }
}


/* XXH3p_hashLong_64b_withSeed() :
 * Generate a custom key,
 * based on alteration of default kSecret with the seed,
 * and then use this key for long mode hashing.
 * This operation is decently fast but nonetheless costs a little bit of time.
 * Try to avoid it whenever possible (typically when seed==0).
 */
XXH_NO_INLINE XXH64_hash_t    /* It's important for performance that XXH3p_hashLong is not inlined. Not sure why (uop cache maybe ?), but difference is large and easily measurable */
XXH3p_hashLong_64b_withSeed(const xxh_u8* input, size_t len, XXH64_hash_t seed)
{
    XXH_ALIGN(8) xxh_u8 secret[XXH_SECRET_DEFAULT_SIZE];
    if (seed==0) return XXH3p_hashLong_64b_defaultSecret(input, len);
    XXH3p_initCustomSecret(secret, seed);
    return XXH3p_hashLong_internal(input, len, secret, sizeof(secret));
}


XXH_FORCE_INLINE xxh_u64 XXH3p_mix16B(const xxh_u8* XXH_RESTRICT input,
                                 const xxh_u8* XXH_RESTRICT secret, xxh_u64 seed64)
{
    xxh_u64 const input_lo = XXH_readLE64(input);
    xxh_u64 const input_hi = XXH_readLE64(input+8);
    return XXH3p_mul128_fold64(
               input_lo ^ (XXH_readLE64(secret)   + seed64),
               input_hi ^ (XXH_readLE64(secret+8) - seed64) );
}


XXH_FORCE_INLINE XXH64_hash_t
XXH3p_len_17to128_64b(const xxh_u8* XXH_RESTRICT input, size_t len,
                     const xxh_u8* XXH_RESTRICT secret, size_t secretSize,
                     XXH64_hash_t seed)
{
    XXH_ASSERT(secretSize >= XXH3p_SECRET_SIZE_MIN); (void)secretSize;
    XXH_ASSERT(16 < len && len <= 128);

    {   xxh_u64 acc = len * PRIME64_1;
        if (len > 32) {
            if (len > 64) {
                if (len > 96) {
                    acc += XXH3p_mix16B(input+48, secret+96, seed);
                    acc += XXH3p_mix16B(input+len-64, secret+112, seed);
                }
                acc += XXH3p_mix16B(input+32, secret+64, seed);
                acc += XXH3p_mix16B(input+len-48, secret+80, seed);
            }
            acc += XXH3p_mix16B(input+16, secret+32, seed);
            acc += XXH3p_mix16B(input+len-32, secret+48, seed);
        }
        acc += XXH3p_mix16B(input+0, secret+0, seed);
        acc += XXH3p_mix16B(input+len-16, secret+16, seed);

        return XXH3p_avalanche(acc);
    }
}

#define XXH3p_MIDSIZE_MAX 240

XXH_NO_INLINE XXH64_hash_t
XXH3p_len_129to240_64b(const xxh_u8* XXH_RESTRICT input, size_t len,
                      const xxh_u8* XXH_RESTRICT secret, size_t secretSize,
                      XXH64_hash_t seed)
{
    XXH_ASSERT(secretSize >= XXH3p_SECRET_SIZE_MIN); (void)secretSize;
    XXH_ASSERT(128 < len && len <= XXH3p_MIDSIZE_MAX);

    #define XXH3p_MIDSIZE_STARTOFFSET 3
    #define XXH3p_MIDSIZE_LASTOFFSET  17

    {   xxh_u64 acc = len * PRIME64_1;
        int const nbRounds = (int)len / 16;
        int i;
        for (i=0; i<8; i++) {
            acc += XXH3p_mix16B(input+(16*i), secret+(16*i), seed);
        }
        acc = XXH3p_avalanche(acc);
        XXH_ASSERT(nbRounds >= 8);
        for (i=8 ; i < nbRounds; i++) {
            acc += XXH3p_mix16B(input+(16*i), secret+(16*(i-8)) + XXH3p_MIDSIZE_STARTOFFSET, seed);
        }
        /* last bytes */
        acc += XXH3p_mix16B(input + len - 16, secret + XXH3p_SECRET_SIZE_MIN - XXH3p_MIDSIZE_LASTOFFSET, seed);
        return XXH3p_avalanche(acc);
    }
}

/* ===   Public entry point   === */

XXH_PUBLIC_API XXH64_hash_t XXH3p_64bits(const void* input, size_t len)
{
    if (len <= 16) return XXH3p_len_0to16_64b((const xxh_u8*)input, len, kSecret, 0);
    if (len <= 128) return XXH3p_len_17to128_64b((const xxh_u8*)input, len, kSecret, sizeof(kSecret), 0);
    if (len <= XXH3p_MIDSIZE_MAX) return XXH3p_len_129to240_64b((const xxh_u8*)input, len, kSecret, sizeof(kSecret), 0);
    return XXH3p_hashLong_64b_defaultSecret((const xxh_u8*)input, len);
}

XXH_PUBLIC_API XXH64_hash_t
XXH3p_64bits_withSecret(const void* input, size_t len, const void* secret, size_t secretSize)
{
    XXH_ASSERT(secretSize >= XXH3p_SECRET_SIZE_MIN);
    /* if an action must be taken should `secret` conditions not be respected,
     * it should be done here.
     * For now, it's a contract pre-condition.
     * Adding a check and a branch here would cost performance at every hash */
     if (len <= 16) return XXH3p_len_0to16_64b((const xxh_u8*)input, len, (const xxh_u8*)secret, 0);
     if (len <= 128) return XXH3p_len_17to128_64b((const xxh_u8*)input, len, (const xxh_u8*)secret, secretSize, 0);
     if (len <= XXH3p_MIDSIZE_MAX) return XXH3p_len_129to240_64b((const xxh_u8*)input, len, (const xxh_u8*)secret, secretSize, 0);
     return XXH3p_hashLong_64b_withSecret((const xxh_u8*)input, len, (const xxh_u8*)secret, secretSize);
}

XXH_PUBLIC_API XXH64_hash_t
XXH3p_64bits_withSeed(const void* input, size_t len, XXH64_hash_t seed)
{
    if (len <= 16) return XXH3p_len_0to16_64b((const xxh_u8*)input, len, kSecret, seed);
    if (len <= 128) return XXH3p_len_17to128_64b((const xxh_u8*)input, len, kSecret, sizeof(kSecret), seed);
    if (len <= XXH3p_MIDSIZE_MAX) return XXH3p_len_129to240_64b((const xxh_u8*)input, len, kSecret, sizeof(kSecret), seed);
    return XXH3p_hashLong_64b_withSeed((const xxh_u8*)input, len, seed);
}

/* ===   XXH3 streaming   === */

/* RocksDB Note: unused & removed due to bug in preview version */

/* ==========================================
 * XXH3 128 bits (=> XXH128)
 * ========================================== */

XXH_FORCE_INLINE XXH128_hash_t
XXH3p_len_1to3_128b(const xxh_u8* input, size_t len, const xxh_u8* secret, XXH64_hash_t seed)
{
    XXH_ASSERT(input != NULL);
    XXH_ASSERT(1 <= len && len <= 3);
    XXH_ASSERT(secret != NULL);
    {   xxh_u8 const c1 = input[0];
        xxh_u8 const c2 = input[len >> 1];
        xxh_u8 const c3 = input[len - 1];
        xxh_u32  const combinedl = ((xxh_u32)c1) + (((xxh_u32)c2) << 8) + (((xxh_u32)c3) << 16) + (((xxh_u32)len) << 24);
        xxh_u32  const combinedh = XXH_swap32(combinedl);
        xxh_u64  const keyed_lo = (xxh_u64)combinedl ^ (XXH_readLE32(secret)   + seed);
        xxh_u64  const keyed_hi = (xxh_u64)combinedh ^ (XXH_readLE32(secret+4) - seed);
        xxh_u64  const mixedl = keyed_lo * PRIME64_1;
        xxh_u64  const mixedh = keyed_hi * PRIME64_5;
        XXH128_hash_t const h128 = { XXH3p_avalanche(mixedl) /*low64*/, XXH3p_avalanche(mixedh) /*high64*/ };
        return h128;
    }
}


XXH_FORCE_INLINE XXH128_hash_t
XXH3p_len_4to8_128b(const xxh_u8* input, size_t len, const xxh_u8* secret, XXH64_hash_t seed)
{
    XXH_ASSERT(input != NULL);
    XXH_ASSERT(secret != NULL);
    XXH_ASSERT(4 <= len && len <= 8);
    {   xxh_u32 const input_lo = XXH_readLE32(input);
        xxh_u32 const input_hi = XXH_readLE32(input + len - 4);
        xxh_u64 const input_64_lo = input_lo + ((xxh_u64)input_hi << 32);
        xxh_u64 const input_64_hi = XXH_swap64(input_64_lo);
        xxh_u64 const keyed_lo = input_64_lo ^ (XXH_readLE64(secret) + seed);
        xxh_u64 const keyed_hi = input_64_hi ^ (XXH_readLE64(secret + 8) - seed);
        xxh_u64 const mix64l1 = len + ((keyed_lo ^ (keyed_lo >> 51)) * PRIME32_1);
        xxh_u64 const mix64l2 = (mix64l1 ^ (mix64l1 >> 47)) * PRIME64_2;
        xxh_u64 const mix64h1 = ((keyed_hi ^ (keyed_hi >> 47)) * PRIME64_1) - len;
        xxh_u64 const mix64h2 = (mix64h1 ^ (mix64h1 >> 43)) * PRIME64_4;
        {   XXH128_hash_t const h128 = { XXH3p_avalanche(mix64l2) /*low64*/, XXH3p_avalanche(mix64h2) /*high64*/ };
            return h128;
    }   }
}

XXH_FORCE_INLINE XXH128_hash_t
XXH3p_len_9to16_128b(const xxh_u8* input, size_t len, const xxh_u8* secret, XXH64_hash_t seed)
{
    XXH_ASSERT(input != NULL);
    XXH_ASSERT(secret != NULL);
    XXH_ASSERT(9 <= len && len <= 16);
    {   xxh_u64 const input_lo = XXH_readLE64(input) ^ (XXH_readLE64(secret) + seed);
        xxh_u64 const input_hi = XXH_readLE64(input + len - 8) ^ (XXH_readLE64(secret+8) - seed);
        XXH128_hash_t m128 = XXH_mult64to128(input_lo ^ input_hi, PRIME64_1);
        xxh_u64 const lenContrib = XXH_mult32to64(len, PRIME32_5);
        m128.low64 += lenContrib;
        m128.high64 += input_hi * PRIME64_1;
        m128.low64  ^= (m128.high64 >> 32);
        {   XXH128_hash_t h128 = XXH_mult64to128(m128.low64, PRIME64_2);
            h128.high64 += m128.high64 * PRIME64_2;
            h128.low64   = XXH3p_avalanche(h128.low64);
            h128.high64  = XXH3p_avalanche(h128.high64);
            return h128;
    }   }
}

/* Assumption : `secret` size is >= 16
 * Note : it should be >= XXH3p_SECRET_SIZE_MIN anyway */
XXH_FORCE_INLINE XXH128_hash_t
XXH3p_len_0to16_128b(const xxh_u8* input, size_t len, const xxh_u8* secret, XXH64_hash_t seed)
{
    XXH_ASSERT(len <= 16);
    {   if (len > 8) return XXH3p_len_9to16_128b(input, len, secret, seed);
        if (len >= 4) return XXH3p_len_4to8_128b(input, len, secret, seed);
        if (len) return XXH3p_len_1to3_128b(input, len, secret, seed);
        {   XXH128_hash_t const h128 = { 0, 0 };
            return h128;
    }   }
}

XXH_FORCE_INLINE XXH128_hash_t
XXH3p_hashLong_128b_internal(const xxh_u8* XXH_RESTRICT input, size_t len,
                            const xxh_u8* XXH_RESTRICT secret, size_t secretSize)
{
    XXH_ALIGN(XXH_ACC_ALIGN) xxh_u64 acc[ACC_NB] = XXH3p_INIT_ACC;

    XXH3p_hashLong_internal_loop(acc, input, len, secret, secretSize, XXH3p_acc_128bits);

    /* converge into final hash */
    XXH_STATIC_ASSERT(sizeof(acc) == 64);
    XXH_ASSERT(secretSize >= sizeof(acc) + XXH_SECRET_MERGEACCS_START);
    {   xxh_u64 const low64 = XXH3p_mergeAccs(acc, secret + XXH_SECRET_MERGEACCS_START, (xxh_u64)len * PRIME64_1);
        xxh_u64 const high64 = XXH3p_mergeAccs(acc, secret + secretSize - sizeof(acc) - XXH_SECRET_MERGEACCS_START, ~((xxh_u64)len * PRIME64_2));
        XXH128_hash_t const h128 = { low64, high64 };
        return h128;
    }
}

XXH_NO_INLINE XXH128_hash_t    /* It's important for performance that XXH3p_hashLong is not inlined. Not sure why (uop cache maybe ?), but difference is large and easily measurable */
XXH3p_hashLong_128b_defaultSecret(const xxh_u8* input, size_t len)
{
    return XXH3p_hashLong_128b_internal(input, len, kSecret, sizeof(kSecret));
}

XXH_NO_INLINE XXH128_hash_t    /* It's important for performance that XXH3p_hashLong is not inlined. Not sure why (uop cache maybe ?), but difference is large and easily measurable */
XXH3p_hashLong_128b_withSecret(const xxh_u8* input, size_t len,
                              const xxh_u8* secret, size_t secretSize)
{
    return XXH3p_hashLong_128b_internal(input, len, secret, secretSize);
}

XXH_NO_INLINE XXH128_hash_t    /* It's important for performance that XXH3p_hashLong is not inlined. Not sure why (uop cache maybe ?), but difference is large and easily measurable */
XXH3p_hashLong_128b_withSeed(const xxh_u8* input, size_t len, XXH64_hash_t seed)
{
    XXH_ALIGN(8) xxh_u8 secret[XXH_SECRET_DEFAULT_SIZE];
    if (seed == 0) return XXH3p_hashLong_128b_defaultSecret(input, len);
    XXH3p_initCustomSecret(secret, seed);
    return XXH3p_hashLong_128b_internal(input, len, secret, sizeof(secret));
}


XXH_FORCE_INLINE XXH128_hash_t
XXH128_mix32B(XXH128_hash_t acc, const xxh_u8* input_1, const xxh_u8* input_2, const xxh_u8* secret, XXH64_hash_t seed)
{
    acc.low64  += XXH3p_mix16B (input_1, secret+0, seed);
    acc.low64  ^= XXH_readLE64(input_2) + XXH_readLE64(input_2 + 8);
    acc.high64 += XXH3p_mix16B (input_2, secret+16, seed);
    acc.high64 ^= XXH_readLE64(input_1) + XXH_readLE64(input_1 + 8);
    return acc;
}

XXH_NO_INLINE XXH128_hash_t
XXH3p_len_129to240_128b(const xxh_u8* XXH_RESTRICT input, size_t len,
                       const xxh_u8* XXH_RESTRICT secret, size_t secretSize,
                       XXH64_hash_t seed)
{
    XXH_ASSERT(secretSize >= XXH3p_SECRET_SIZE_MIN); (void)secretSize;
    XXH_ASSERT(128 < len && len <= XXH3p_MIDSIZE_MAX);

    {   XXH128_hash_t acc;
        int const nbRounds = (int)len / 32;
        int i;
        acc.low64 = len * PRIME64_1;
        acc.high64 = 0;
        for (i=0; i<4; i++) {
            acc = XXH128_mix32B(acc, input+(32*i), input+(32*i)+16, secret+(32*i), seed);
        }
        acc.low64 = XXH3p_avalanche(acc.low64);
        acc.high64 = XXH3p_avalanche(acc.high64);
        XXH_ASSERT(nbRounds >= 4);
        for (i=4 ; i < nbRounds; i++) {
            acc = XXH128_mix32B(acc, input+(32*i), input+(32*i)+16, secret+XXH3p_MIDSIZE_STARTOFFSET+(32*(i-4)), seed);
        }
        /* last bytes */
        acc = XXH128_mix32B(acc, input + len - 16, input + len - 32, secret + XXH3p_SECRET_SIZE_MIN - XXH3p_MIDSIZE_LASTOFFSET - 16, 0ULL - seed);

        {   xxh_u64 const low64 = acc.low64 + acc.high64;
            xxh_u64 const high64 = (acc.low64 * PRIME64_1) + (acc.high64 * PRIME64_4) + ((len - seed) * PRIME64_2);
            XXH128_hash_t const h128 = { XXH3p_avalanche(low64), (XXH64_hash_t)0 - XXH3p_avalanche(high64) };
            return h128;
        }
    }
}


XXH_FORCE_INLINE XXH128_hash_t
XXH3p_len_17to128_128b(const xxh_u8* XXH_RESTRICT input, size_t len,
                      const xxh_u8* XXH_RESTRICT secret, size_t secretSize,
                      XXH64_hash_t seed)
{
    XXH_ASSERT(secretSize >= XXH3p_SECRET_SIZE_MIN); (void)secretSize;
    XXH_ASSERT(16 < len && len <= 128);

    {   XXH128_hash_t acc;
        acc.low64 = len * PRIME64_1;
        acc.high64 = 0;
        if (len > 32) {
            if (len > 64) {
                if (len > 96) {
                    acc = XXH128_mix32B(acc, input+48, input+len-64, secret+96, seed);
                }
                acc = XXH128_mix32B(acc, input+32, input+len-48, secret+64, seed);
            }
            acc = XXH128_mix32B(acc, input+16, input+len-32, secret+32, seed);
        }
        acc = XXH128_mix32B(acc, input, input+len-16, secret, seed);
        {   xxh_u64 const low64 = acc.low64 + acc.high64;
            xxh_u64 const high64 = (acc.low64 * PRIME64_1) + (acc.high64 * PRIME64_4) + ((len - seed) * PRIME64_2);
            XXH128_hash_t const h128 = { XXH3p_avalanche(low64), (XXH64_hash_t)0 - XXH3p_avalanche(high64) };
            return h128;
        }
    }
}

XXH_PUBLIC_API XXH128_hash_t XXH3p_128bits(const void* input, size_t len)
{
    if (len <= 16) return XXH3p_len_0to16_128b((const xxh_u8*)input, len, kSecret, 0);
    if (len <= 128) return XXH3p_len_17to128_128b((const xxh_u8*)input, len, kSecret, sizeof(kSecret), 0);
    if (len <= XXH3p_MIDSIZE_MAX) return XXH3p_len_129to240_128b((const xxh_u8*)input, len, kSecret, sizeof(kSecret), 0);
    return XXH3p_hashLong_128b_defaultSecret((const xxh_u8*)input, len);
}

XXH_PUBLIC_API XXH128_hash_t
XXH3p_128bits_withSecret(const void* input, size_t len, const void* secret, size_t secretSize)
{
    XXH_ASSERT(secretSize >= XXH3p_SECRET_SIZE_MIN);
    /* if an action must be taken should `secret` conditions not be respected,
     * it should be done here.
     * For now, it's a contract pre-condition.
     * Adding a check and a branch here would cost performance at every hash */
     if (len <= 16) return XXH3p_len_0to16_128b((const xxh_u8*)input, len, (const xxh_u8*)secret, 0);
     if (len <= 128) return XXH3p_len_17to128_128b((const xxh_u8*)input, len, (const xxh_u8*)secret, secretSize, 0);
     if (len <= XXH3p_MIDSIZE_MAX) return XXH3p_len_129to240_128b((const xxh_u8*)input, len, (const xxh_u8*)secret, secretSize, 0);
     return XXH3p_hashLong_128b_withSecret((const xxh_u8*)input, len, (const xxh_u8*)secret, secretSize);
}

XXH_PUBLIC_API XXH128_hash_t
XXH3p_128bits_withSeed(const void* input, size_t len, XXH64_hash_t seed)
{
    if (len <= 16) return XXH3p_len_0to16_128b((const xxh_u8*)input, len, kSecret, seed);
    if (len <= 128) return XXH3p_len_17to128_128b((const xxh_u8*)input, len, kSecret, sizeof(kSecret), seed);
    if (len <= XXH3p_MIDSIZE_MAX) return XXH3p_len_129to240_128b((const xxh_u8*)input, len, kSecret, sizeof(kSecret), seed);
    return XXH3p_hashLong_128b_withSeed((const xxh_u8*)input, len, seed);
}

XXH_PUBLIC_API XXH128_hash_t
XXH128(const void* input, size_t len, XXH64_hash_t seed)
{
    return XXH3p_128bits_withSeed(input, len, seed);
}


/* ===   XXH3 128-bit streaming   === */

/* RocksDB Note: unused & removed due to bug in preview version */

/* 128-bit utility functions */

#include <string.h>   /* memcmp */

/* return : 1 is equal, 0 if different */
XXH_PUBLIC_API int XXH128_isEqual(XXH128_hash_t h1, XXH128_hash_t h2)
{
    /* note : XXH128_hash_t is compact, it has no padding byte */
    return !(memcmp(&h1, &h2, sizeof(h1)));
}

/* This prototype is compatible with stdlib's qsort().
 * return : >0 if *h128_1  > *h128_2
 *          <0 if *h128_1  < *h128_2
 *          =0 if *h128_1 == *h128_2  */
XXH_PUBLIC_API int XXH128_cmp(const void* h128_1, const void* h128_2)
{
    XXH128_hash_t const h1 = *(const XXH128_hash_t*)h128_1;
    XXH128_hash_t const h2 = *(const XXH128_hash_t*)h128_2;
    int const hcmp = (h1.high64 > h2.high64) - (h2.high64 > h1.high64);
    /* note : bets that, in most cases, hash values are different */
    if (hcmp) return hcmp;
    return (h1.low64 > h2.low64) - (h2.low64 > h1.low64);
}


/*======   Canonical representation   ======*/
XXH_PUBLIC_API void
XXH128_canonicalFromHash(XXH128_canonical_t* dst, XXH128_hash_t hash)
{
    XXH_STATIC_ASSERT(sizeof(XXH128_canonical_t) == sizeof(XXH128_hash_t));
    if (XXH_CPU_LITTLE_ENDIAN) {
        hash.high64 = XXH_swap64(hash.high64);
        hash.low64  = XXH_swap64(hash.low64);
    }
    memcpy(dst, &hash.high64, sizeof(hash.high64));
    memcpy((char*)dst + sizeof(hash.high64), &hash.low64, sizeof(hash.low64));
}

XXH_PUBLIC_API XXH128_hash_t
XXH128_hashFromCanonical(const XXH128_canonical_t* src)
{
    XXH128_hash_t h;
    h.high64 = XXH_readBE64(src);
    h.low64  = XXH_readBE64(src->digest + 8);
    return h;
}



#endif  /* XXH3p_H */
