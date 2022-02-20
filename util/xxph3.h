//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
/*
   xxHash - Extremely Fast Hash algorithm
   Header File
   Copyright (C) 2012-2016, Yann Collet.

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

// This is a fork of a preview version of xxHash, as RocksDB depends on
// this preview version of XXH3. To allow this to coexist with the
// standard xxHash, including in the "unity" build where all source files
// and headers go into a single translation unit, here "XXH" has been
// replaced with "XXPH" for XX Preview Hash.

#ifndef XXPHASH_H_5627135585666179
#define XXPHASH_H_5627135585666179 1

/* BEGIN RocksDB customizations */
#ifndef XXPH_STATIC_LINKING_ONLY
#define XXPH_STATIC_LINKING_ONLY 1 /* access experimental APIs like XXPH3 */
#endif
#define XXPH_NAMESPACE ROCKSDB_
#define XXPH_INLINE_ALL
#include <cstring>
/* END RocksDB customizations */

#if defined (__cplusplus)
extern "C" {
#endif


/* ****************************
*  Definitions
******************************/
#include <stddef.h>   /* size_t */
typedef enum { XXPH_OK=0, XXPH_ERROR } XXPH_errorcode;


/* ****************************
 *  API modifier
 ******************************/
/** XXPH_INLINE_ALL (and XXPH_PRIVATE_API)
 *  This build macro includes xxhash functions in `static` mode
 *  in order to inline them, and remove their symbol from the public list.
 *  Inlining offers great performance improvement on small keys,
 *  and dramatic ones when length is expressed as a compile-time constant.
 *  See https://fastcompression.blogspot.com/2018/03/xxhash-for-small-keys-impressive-power.html .
 *  Methodology :
 *     #define XXPH_INLINE_ALL
 *     #include "xxhash.h"
 * `xxhash.c` is automatically included.
 *  It's not useful to compile and link it as a separate object.
 */
#if defined(XXPH_INLINE_ALL) || defined(XXPH_PRIVATE_API)
#  ifndef XXPH_STATIC_LINKING_ONLY
#    define XXPH_STATIC_LINKING_ONLY
#  endif
#  if defined(__GNUC__)
#    define XXPH_PUBLIC_API static __inline __attribute__((unused))
#  elif defined (__cplusplus) || (defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */)
#    define XXPH_PUBLIC_API static inline
#  elif defined(_MSC_VER)
#    define XXPH_PUBLIC_API static __inline
#  else
     /* this version may generate warnings for unused static functions */
#    define XXPH_PUBLIC_API static
#  endif
#else
#  if defined(WIN32) && defined(_MSC_VER) && (defined(XXPH_IMPORT) || defined(XXPH_EXPORT))
#    ifdef XXPH_EXPORT
#      define XXPH_PUBLIC_API __declspec(dllexport)
#    elif XXPH_IMPORT
#      define XXPH_PUBLIC_API __declspec(dllimport)
#    endif
#  else
#    define XXPH_PUBLIC_API   /* do nothing */
#  endif
#endif /* XXPH_INLINE_ALL || XXPH_PRIVATE_API */

/*! XXPH_NAMESPACE, aka Namespace Emulation :
 *
 * If you want to include _and expose_ xxHash functions from within your own library,
 * but also want to avoid symbol collisions with other libraries which may also include xxHash,
 *
 * you can use XXPH_NAMESPACE, to automatically prefix any public symbol from xxhash library
 * with the value of XXPH_NAMESPACE (therefore, avoid NULL and numeric values).
 *
 * Note that no change is required within the calling program as long as it includes `xxhash.h` :
 * regular symbol name will be automatically translated by this header.
 */
#ifdef XXPH_NAMESPACE
#  define XXPH_CAT(A,B) A##B
#  define XXPH_NAME2(A,B) XXPH_CAT(A,B)
#  define XXPH_versionNumber XXPH_NAME2(XXPH_NAMESPACE, XXPH_versionNumber)
#endif


/* *************************************
*  Version
***************************************/
#define XXPH_VERSION_MAJOR    0
#define XXPH_VERSION_MINOR    7
#define XXPH_VERSION_RELEASE  2
#define XXPH_VERSION_NUMBER  (XXPH_VERSION_MAJOR *100*100 + XXPH_VERSION_MINOR *100 + XXPH_VERSION_RELEASE)
XXPH_PUBLIC_API unsigned XXPH_versionNumber (void);


/*-**********************************************************************
*  32-bit hash
************************************************************************/
#if !defined (__VMS) \
  && (defined (__cplusplus) \
  || (defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */) )
#   include <stdint.h>
    typedef uint32_t XXPH32_hash_t;
#else
#   include <limits.h>
#   if UINT_MAX == 0xFFFFFFFFUL
      typedef unsigned int XXPH32_hash_t;
#   else
#     if ULONG_MAX == 0xFFFFFFFFUL
        typedef unsigned long XXPH32_hash_t;
#     else
#       error "unsupported platform : need a 32-bit type"
#     endif
#   endif
#endif

#ifndef XXPH_NO_LONG_LONG
/*-**********************************************************************
*  64-bit hash
************************************************************************/
#if !defined (__VMS) \
  && (defined (__cplusplus) \
  || (defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */) )
#   include <stdint.h>
    typedef uint64_t XXPH64_hash_t;
#else
    /* the following type must have a width of 64-bit */
    typedef unsigned long long XXPH64_hash_t;
#endif

#endif  /* XXPH_NO_LONG_LONG */



#ifdef XXPH_STATIC_LINKING_ONLY

/* ================================================================================================
   This section contains declarations which are not guaranteed to remain stable.
   They may change in future versions, becoming incompatible with a different version of the library.
   These declarations should only be used with static linking.
   Never use them in association with dynamic linking !
=================================================================================================== */


/*-**********************************************************************
*  XXPH3
*  New experimental hash
************************************************************************/
#ifndef XXPH_NO_LONG_LONG


/* ============================================
 * XXPH3 is a new hash algorithm,
 * featuring improved speed performance for both small and large inputs.
 * See full speed analysis at : http://fastcompression.blogspot.com/2019/03/presenting-xxh3.html
 * In general, expect XXPH3 to run about ~2x faster on large inputs,
 * and >3x faster on small ones, though exact differences depend on platform.
 *
 * The algorithm is portable, will generate the same hash on all platforms.
 * It benefits greatly from vectorization units, but does not require it.
 *
 * XXPH3 offers 2 variants, _64bits and _128bits.
 * When only 64 bits are needed, prefer calling the _64bits variant :
 * it reduces the amount of mixing, resulting in faster speed on small inputs.
 * It's also generally simpler to manipulate a scalar return type than a struct.
 *
 * The XXPH3 algorithm is still considered experimental.
 * Produced results can still change between versions.
 * Results produced by v0.7.x are not comparable with results from v0.7.y .
 * It's nonetheless possible to use XXPH3 for ephemeral data (local sessions),
 * but avoid storing values in long-term storage for later reads.
 *
 * The API supports one-shot hashing, streaming mode, and custom secrets.
 *
 * There are still a number of opened questions that community can influence during the experimental period.
 * I'm trying to list a few of them below, though don't consider this list as complete.
 *
 * - 128-bits output type : currently defined as a structure of two 64-bits fields.
 *                          That's because 128-bit values do not exist in C standard.
 *                          Note that it means that, at byte level, result is not identical depending on endianess.
 *                          However, at field level, they are identical on all platforms.
 *                          The canonical representation solves the issue of identical byte-level representation across platforms,
 *                          which is necessary for serialization.
 *                          Q1 : Would there be a better representation for a 128-bit hash result ?
 *                          Q2 : Are the names of the inner 64-bit fields important ? Should they be changed ?
 *
 * - Prototype XXPH128() :   XXPH128() uses the same arguments as XXPH64(), for consistency.
 *                          It means it maps to XXPH3_128bits_withSeed().
 *                          This variant is slightly slower than XXPH3_128bits(),
 *                          because the seed is now part of the algorithm, and can't be simplified.
 *                          Is that a good idea ?
 *
 * - Seed type for XXPH128() : currently, it's a single 64-bit value, like the 64-bit variant.
 *                          It could be argued that it's more logical to offer a 128-bit seed input parameter for a 128-bit hash.
 *                          But 128-bit seed is more difficult to use, since it requires to pass a structure instead of a scalar value.
 *                          Such a variant could either replace current one, or become an additional one.
 *                          Farmhash, for example, offers both variants (the 128-bits seed variant is called `doubleSeed`).
 *                          Follow up question : if both 64-bit and 128-bit seeds are allowed, which variant should be called XXPH128 ?
 *
 * - Result for len==0 :    Currently, the result of hashing a zero-length input is always `0`.
 *                          It seems okay as a return value when using "default" secret and seed.
 *                          But is it still fine to return `0` when secret or seed are non-default ?
 *                          Are there use cases which could depend on generating a different hash result for zero-length input when the secret is different ?
 *
 * - Consistency (1) :      Streaming XXPH128 uses an XXPH3 state, which is the same state as XXPH3_64bits().
 *                          It means a 128bit streaming loop must invoke the following symbols :
 *                          XXPH3_createState(), XXPH3_128bits_reset(), XXPH3_128bits_update() (loop), XXPH3_128bits_digest(), XXPH3_freeState().
 *                          Is that consistent enough ?
 *
 * - Consistency (2) :      The canonical representation of `XXPH3_64bits` is provided by existing functions
 *                          XXPH64_canonicalFromHash(), and reverse operation XXPH64_hashFromCanonical().
 *                          As a mirror, canonical functions for XXPH128_hash_t results generated by `XXPH3_128bits`
 *                          are XXPH128_canonicalFromHash() and XXPH128_hashFromCanonical().
 *                          Which means, `XXPH3` doesn't appear in the names, because canonical functions operate on a type,
 *                          independently of which algorithm was used to generate that type.
 *                          Is that consistent enough ?
 */

#ifdef XXPH_NAMESPACE
#  define XXPH3_64bits XXPH_NAME2(XXPH_NAMESPACE, XXPH3_64bits)
#  define XXPH3_64bits_withSecret XXPH_NAME2(XXPH_NAMESPACE, XXPH3_64bits_withSecret)
#  define XXPH3_64bits_withSeed XXPH_NAME2(XXPH_NAMESPACE, XXPH3_64bits_withSeed)
#endif

/* XXPH3_64bits() :
 * default 64-bit variant, using default secret and default seed of 0.
 * It's the fastest variant. */
XXPH_PUBLIC_API XXPH64_hash_t XXPH3_64bits(const void* data, size_t len);

/* XXPH3_64bits_withSecret() :
 * It's possible to provide any blob of bytes as a "secret" to generate the hash.
 * This makes it more difficult for an external actor to prepare an intentional collision.
 * The secret *must* be large enough (>= XXPH3_SECRET_SIZE_MIN).
 * It should consist of random bytes.
 * Avoid repeating same character, or sequences of bytes,
 * and especially avoid swathes of \0.
 * Failure to respect these conditions will result in a poor quality hash.
 */
#define XXPH3_SECRET_SIZE_MIN 136
XXPH_PUBLIC_API XXPH64_hash_t XXPH3_64bits_withSecret(const void* data, size_t len, const void* secret, size_t secretSize);

/* XXPH3_64bits_withSeed() :
 * This variant generates on the fly a custom secret,
 * based on the default secret, altered using the `seed` value.
 * While this operation is decently fast, note that it's not completely free.
 * note : seed==0 produces same results as XXPH3_64bits() */
XXPH_PUBLIC_API XXPH64_hash_t XXPH3_64bits_withSeed(const void* data, size_t len, XXPH64_hash_t seed);

#if defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 201112L)   /* C11+ */
#  include <stdalign.h>
#  define XXPH_ALIGN(n)      alignas(n)
#elif defined(__GNUC__)
#  define XXPH_ALIGN(n)      __attribute__ ((aligned(n)))
#elif defined(_MSC_VER)
#  define XXPH_ALIGN(n)      __declspec(align(n))
#else
#  define XXPH_ALIGN(n)   /* disabled */
#endif

#define XXPH3_SECRET_DEFAULT_SIZE 192   /* minimum XXPH3_SECRET_SIZE_MIN */

#endif  /* XXPH_NO_LONG_LONG */


/*-**********************************************************************
*  XXPH_INLINE_ALL
************************************************************************/
#if defined(XXPH_INLINE_ALL) || defined(XXPH_PRIVATE_API)

/* === RocksDB modification: was #include here but permanently inlining === */

typedef struct {
    XXPH64_hash_t low64;
    XXPH64_hash_t high64;
} XXPH128_hash_t;

/* *************************************
*  Tuning parameters
***************************************/
/*!XXPH_FORCE_MEMORY_ACCESS :
 * By default, access to unaligned memory is controlled by `memcpy()`, which is safe and portable.
 * Unfortunately, on some target/compiler combinations, the generated assembly is sub-optimal.
 * The below switch allow to select different access method for improved performance.
 * Method 0 (default) : use `memcpy()`. Safe and portable.
 * Method 1 : `__packed` statement. It depends on compiler extension (ie, not portable).
 *            This method is safe if your compiler supports it, and *generally* as fast or faster than `memcpy`.
 * Method 2 : direct access. This method doesn't depend on compiler but violate C standard.
 *            It can generate buggy code on targets which do not support unaligned memory accesses.
 *            But in some circumstances, it's the only known way to get the most performance (ie GCC + ARMv6)
 * See http://stackoverflow.com/a/32095106/646947 for details.
 * Prefer these methods in priority order (0 > 1 > 2)
 */
#ifndef XXPH_FORCE_MEMORY_ACCESS   /* can be defined externally, on command line for example */
#  if !defined(__clang__) && defined(__GNUC__) && defined(__ARM_FEATURE_UNALIGNED) && defined(__ARM_ARCH) && (__ARM_ARCH == 6)
#    define XXPH_FORCE_MEMORY_ACCESS 2
#  elif !defined(__clang__) && ((defined(__INTEL_COMPILER) && !defined(_WIN32)) || \
  (defined(__GNUC__) && (defined(__ARM_ARCH) && __ARM_ARCH >= 7)))
#    define XXPH_FORCE_MEMORY_ACCESS 1
#  endif
#endif

/*!XXPH_ACCEPT_NULL_INPUT_POINTER :
 * If input pointer is NULL, xxHash default behavior is to dereference it, triggering a segfault.
 * When this macro is enabled, xxHash actively checks input for null pointer.
 * It it is, result for null input pointers is the same as a null-length input.
 */
#ifndef XXPH_ACCEPT_NULL_INPUT_POINTER   /* can be defined externally */
#  define XXPH_ACCEPT_NULL_INPUT_POINTER 0
#endif

/*!XXPH_FORCE_ALIGN_CHECK :
 * This is a minor performance trick, only useful with lots of very small keys.
 * It means : check for aligned/unaligned input.
 * The check costs one initial branch per hash;
 * set it to 0 when the input is guaranteed to be aligned,
 * or when alignment doesn't matter for performance.
 */
#ifndef XXPH_FORCE_ALIGN_CHECK /* can be defined externally */
#  if defined(__i386) || defined(_M_IX86) || defined(__x86_64__) || defined(_M_X64)
#    define XXPH_FORCE_ALIGN_CHECK 0
#  else
#    define XXPH_FORCE_ALIGN_CHECK 1
#  endif
#endif

/*!XXPH_REROLL:
 * Whether to reroll XXPH32_finalize, and XXPH64_finalize,
 * instead of using an unrolled jump table/if statement loop.
 *
 * This is automatically defined on -Os/-Oz on GCC and Clang. */
#ifndef XXPH_REROLL
#  if defined(__OPTIMIZE_SIZE__)
#    define XXPH_REROLL 1
#  else
#    define XXPH_REROLL 0
#  endif
#endif

#include <limits.h>   /* ULLONG_MAX */

#ifndef XXPH_STATIC_LINKING_ONLY
#define XXPH_STATIC_LINKING_ONLY
#endif

/* BEGIN RocksDB customizations */
#include "port/lang.h" /* for FALLTHROUGH_INTENDED, inserted as appropriate */
/* END RocksDB customizations */

/* *************************************
*  Compiler Specific Options
***************************************/
#ifdef _MSC_VER    /* Visual Studio */
#  pragma warning(disable : 4127)      /* disable: C4127: conditional expression is constant */
#  define XXPH_FORCE_INLINE static __forceinline
#  define XXPH_NO_INLINE static __declspec(noinline)
#else
#  if defined (__cplusplus) || defined (__STDC_VERSION__) && __STDC_VERSION__ >= 199901L   /* C99 */
#    ifdef __GNUC__
#      define XXPH_FORCE_INLINE static inline __attribute__((always_inline))
#      define XXPH_NO_INLINE static __attribute__((noinline))
#    else
#      define XXPH_FORCE_INLINE static inline
#      define XXPH_NO_INLINE static
#    endif
#  else
#    define XXPH_FORCE_INLINE static
#    define XXPH_NO_INLINE static
#  endif /* __STDC_VERSION__ */
#endif



/* *************************************
*  Debug
***************************************/
/* DEBUGLEVEL is expected to be defined externally,
 * typically through compiler command line.
 * Value must be a number. */
#ifndef DEBUGLEVEL
#  define DEBUGLEVEL 0
#endif

#if (DEBUGLEVEL>=1)
#  include <assert.h>   /* note : can still be disabled with NDEBUG */
#  define XXPH_ASSERT(c)   assert(c)
#else
#  define XXPH_ASSERT(c)   ((void)0)
#endif

/* note : use after variable declarations */
#define XXPH_STATIC_ASSERT(c)  { enum { XXPH_sa = 1/(int)(!!(c)) }; }


/* *************************************
*  Basic Types
***************************************/
#if !defined (__VMS) \
 && (defined (__cplusplus) \
 || (defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */) )
# include <stdint.h>
  typedef uint8_t  xxh_u8;
#else
  typedef unsigned char      xxh_u8;
#endif
typedef XXPH32_hash_t xxh_u32;


/* ===   Memory access   === */

#if (defined(XXPH_FORCE_MEMORY_ACCESS) && (XXPH_FORCE_MEMORY_ACCESS==2))

/* Force direct memory access. Only works on CPU which support unaligned memory access in hardware */
static xxh_u32 XXPH_read32(const void* memPtr) { return *(const xxh_u32*) memPtr; }

#elif (defined(XXPH_FORCE_MEMORY_ACCESS) && (XXPH_FORCE_MEMORY_ACCESS==1))

/* __pack instructions are safer, but compiler specific, hence potentially problematic for some compilers */
/* currently only defined for gcc and icc */
typedef union { xxh_u32 u32; } __attribute__((packed)) unalign;
static xxh_u32 XXPH_read32(const void* ptr) { return ((const unalign*)ptr)->u32; }

#else

/* portable and safe solution. Generally efficient.
 * see : http://stackoverflow.com/a/32095106/646947
 */
static xxh_u32 XXPH_read32(const void* memPtr)
{
    xxh_u32 val;
    memcpy(&val, memPtr, sizeof(val));
    return val;
}

#endif   /* XXPH_FORCE_DIRECT_MEMORY_ACCESS */


/* ===   Endianess   === */

/* XXPH_CPU_LITTLE_ENDIAN can be defined externally, for example on the compiler command line */
#ifndef XXPH_CPU_LITTLE_ENDIAN
#  if defined(_WIN32) /* Windows is always little endian */ \
     || defined(__LITTLE_ENDIAN__) \
     || (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#    define XXPH_CPU_LITTLE_ENDIAN 1
#  elif defined(__BIG_ENDIAN__) \
     || (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#    define XXPH_CPU_LITTLE_ENDIAN 0
#  else
static int XXPH_isLittleEndian(void)
{
    const union { xxh_u32 u; xxh_u8 c[4]; } one = { 1 };   /* don't use static : performance detrimental  */
    return one.c[0];
}
#   define XXPH_CPU_LITTLE_ENDIAN   XXPH_isLittleEndian()
#  endif
#endif




/* ****************************************
*  Compiler-specific Functions and Macros
******************************************/
#define XXPH_GCC_VERSION (__GNUC__ * 100 + __GNUC_MINOR__)

#ifndef __has_builtin
#  define __has_builtin(x) 0
#endif

#if !defined(NO_CLANG_BUILTIN) && __has_builtin(__builtin_rotateleft32) && __has_builtin(__builtin_rotateleft64)
#  define XXPH_rotl32 __builtin_rotateleft32
#  define XXPH_rotl64 __builtin_rotateleft64
/* Note : although _rotl exists for minGW (GCC under windows), performance seems poor */
#elif defined(_MSC_VER)
#  define XXPH_rotl32(x,r) _rotl(x,r)
#  define XXPH_rotl64(x,r) _rotl64(x,r)
#else
#  define XXPH_rotl32(x,r) (((x) << (r)) | ((x) >> (32 - (r))))
#  define XXPH_rotl64(x,r) (((x) << (r)) | ((x) >> (64 - (r))))
#endif

#if defined(_MSC_VER)     /* Visual Studio */
#  define XXPH_swap32 _byteswap_ulong
#elif XXPH_GCC_VERSION >= 403
#  define XXPH_swap32 __builtin_bswap32
#else
static xxh_u32 XXPH_swap32 (xxh_u32 x)
{
    return  ((x << 24) & 0xff000000 ) |
            ((x <<  8) & 0x00ff0000 ) |
            ((x >>  8) & 0x0000ff00 ) |
            ((x >> 24) & 0x000000ff );
}
#endif


/* ***************************
*  Memory reads
*****************************/
typedef enum { XXPH_aligned, XXPH_unaligned } XXPH_alignment;

XXPH_FORCE_INLINE xxh_u32 XXPH_readLE32(const void* ptr)
{
    return XXPH_CPU_LITTLE_ENDIAN ? XXPH_read32(ptr) : XXPH_swap32(XXPH_read32(ptr));
}

XXPH_FORCE_INLINE xxh_u32
XXPH_readLE32_align(const void* ptr, XXPH_alignment align)
{
    if (align==XXPH_unaligned) {
        return XXPH_readLE32(ptr);
    } else {
        return XXPH_CPU_LITTLE_ENDIAN ? *(const xxh_u32*)ptr : XXPH_swap32(*(const xxh_u32*)ptr);
    }
}


/* *************************************
*  Misc
***************************************/
XXPH_PUBLIC_API unsigned XXPH_versionNumber (void) { return XXPH_VERSION_NUMBER; }


static const xxh_u32 PRIME32_1 = 0x9E3779B1U;   /* 0b10011110001101110111100110110001 */
static const xxh_u32 PRIME32_2 = 0x85EBCA77U;   /* 0b10000101111010111100101001110111 */
static const xxh_u32 PRIME32_3 = 0xC2B2AE3DU;   /* 0b11000010101100101010111000111101 */
static const xxh_u32 PRIME32_4 = 0x27D4EB2FU;   /* 0b00100111110101001110101100101111 */
static const xxh_u32 PRIME32_5 = 0x165667B1U;   /* 0b00010110010101100110011110110001 */

#ifndef XXPH_NO_LONG_LONG

/* *******************************************************************
*  64-bit hash functions
*********************************************************************/

/*======   Memory access   ======*/

typedef XXPH64_hash_t xxh_u64;

#if (defined(XXPH_FORCE_MEMORY_ACCESS) && (XXPH_FORCE_MEMORY_ACCESS==2))

/* Force direct memory access. Only works on CPU which support unaligned memory access in hardware */
static xxh_u64 XXPH_read64(const void* memPtr) { return *(const xxh_u64*) memPtr; }

#elif (defined(XXPH_FORCE_MEMORY_ACCESS) && (XXPH_FORCE_MEMORY_ACCESS==1))

/* __pack instructions are safer, but compiler specific, hence potentially problematic for some compilers */
/* currently only defined for gcc and icc */
typedef union { xxh_u32 u32; xxh_u64 u64; } __attribute__((packed)) unalign64;
static xxh_u64 XXPH_read64(const void* ptr) { return ((const unalign64*)ptr)->u64; }

#else

/* portable and safe solution. Generally efficient.
 * see : http://stackoverflow.com/a/32095106/646947
 */

static xxh_u64 XXPH_read64(const void* memPtr)
{
    xxh_u64 val;
    memcpy(&val, memPtr, sizeof(val));
    return val;
}

#endif   /* XXPH_FORCE_DIRECT_MEMORY_ACCESS */

#if defined(_MSC_VER)     /* Visual Studio */
#  define XXPH_swap64 _byteswap_uint64
#elif XXPH_GCC_VERSION >= 403
#  define XXPH_swap64 __builtin_bswap64
#else
static xxh_u64 XXPH_swap64 (xxh_u64 x)
{
    return  ((x << 56) & 0xff00000000000000ULL) |
            ((x << 40) & 0x00ff000000000000ULL) |
            ((x << 24) & 0x0000ff0000000000ULL) |
            ((x << 8)  & 0x000000ff00000000ULL) |
            ((x >> 8)  & 0x00000000ff000000ULL) |
            ((x >> 24) & 0x0000000000ff0000ULL) |
            ((x >> 40) & 0x000000000000ff00ULL) |
            ((x >> 56) & 0x00000000000000ffULL);
}
#endif

XXPH_FORCE_INLINE xxh_u64 XXPH_readLE64(const void* ptr)
{
    return XXPH_CPU_LITTLE_ENDIAN ? XXPH_read64(ptr) : XXPH_swap64(XXPH_read64(ptr));
}

XXPH_FORCE_INLINE xxh_u64
XXPH_readLE64_align(const void* ptr, XXPH_alignment align)
{
    if (align==XXPH_unaligned)
        return XXPH_readLE64(ptr);
    else
        return XXPH_CPU_LITTLE_ENDIAN ? *(const xxh_u64*)ptr : XXPH_swap64(*(const xxh_u64*)ptr);
}


/*======   xxh64   ======*/

static const xxh_u64 PRIME64_1 = 0x9E3779B185EBCA87ULL;   /* 0b1001111000110111011110011011000110000101111010111100101010000111 */
static const xxh_u64 PRIME64_2 = 0xC2B2AE3D27D4EB4FULL;   /* 0b1100001010110010101011100011110100100111110101001110101101001111 */
static const xxh_u64 PRIME64_3 = 0x165667B19E3779F9ULL;   /* 0b0001011001010110011001111011000110011110001101110111100111111001 */
static const xxh_u64 PRIME64_4 = 0x85EBCA77C2B2AE63ULL;   /* 0b1000010111101011110010100111011111000010101100101010111001100011 */
static const xxh_u64 PRIME64_5 = 0x27D4EB2F165667C5ULL;   /* 0b0010011111010100111010110010111100010110010101100110011111000101 */


/* *********************************************************************
*  XXPH3
*  New generation hash designed for speed on small keys and vectorization
************************************************************************ */

/*======== Was #include "xxh3.h", now inlined below ==========*/

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
   version 0.7.2) of XXPH3 that is unlikely to be compatible with the final
   version of XXPH3. We have therefore renamed this XXPH3 ("preview"), for
   clarity so that we can continue to use this version even after
   integrating a newer incompatible version.
*/

/* ===   Dependencies   === */

#undef XXPH_INLINE_ALL   /* in case it's already defined */
#define XXPH_INLINE_ALL


/* ===   Compiler specifics   === */

#if defined (__STDC_VERSION__) && __STDC_VERSION__ >= 199901L   /* >= C99 */
#  define XXPH_RESTRICT   restrict
#else
/* note : it might be useful to define __restrict or __restrict__ for some C++ compilers */
#  define XXPH_RESTRICT   /* disable */
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
 * XXPH3 only requires these features to be efficient:
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
#   warning "XXPH3 is highly inefficient without ARM or Thumb-2."
#endif

/* ==========================================
 * Vectorization detection
 * ========================================== */
#define XXPH_SCALAR 0
#define XXPH_SSE2   1
#define XXPH_AVX2   2
#define XXPH_NEON   3
#define XXPH_VSX    4

#ifndef XXPH_VECTOR    /* can be defined on command line */
#  if defined(__AVX2__)
#    define XXPH_VECTOR XXPH_AVX2
#  elif defined(__SSE2__) || defined(_M_AMD64) || defined(_M_X64) || (defined(_M_IX86_FP) && (_M_IX86_FP == 2))
#    define XXPH_VECTOR XXPH_SSE2
#  elif defined(__GNUC__) /* msvc support maybe later */ \
  && (defined(__ARM_NEON__) || defined(__ARM_NEON)) \
  && (defined(__LITTLE_ENDIAN__) /* We only support little endian NEON */ \
    || (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__))
#    define XXPH_VECTOR XXPH_NEON
#  elif defined(__PPC64__) && defined(__POWER8_VECTOR__) && defined(__GNUC__)
#    define XXPH_VECTOR XXPH_VSX
#  else
#    define XXPH_VECTOR XXPH_SCALAR
#  endif
#endif

/* control alignment of accumulator,
 * for compatibility with fast vector loads */
#ifndef XXPH_ACC_ALIGN
#  if XXPH_VECTOR == 0   /* scalar */
#     define XXPH_ACC_ALIGN 8
#  elif XXPH_VECTOR == 1  /* sse2 */
#     define XXPH_ACC_ALIGN 16
#  elif XXPH_VECTOR == 2  /* avx2 */
#     define XXPH_ACC_ALIGN 32
#  elif XXPH_VECTOR == 3  /* neon */
#     define XXPH_ACC_ALIGN 16
#  elif XXPH_VECTOR == 4  /* vsx */
#     define XXPH_ACC_ALIGN 16
#  endif
#endif

/* xxh_u64 XXPH_mult32to64(xxh_u32 a, xxh_u64 b) { return (xxh_u64)a * (xxh_u64)b; } */
#if defined(_MSC_VER) && defined(_M_IX86)
#    include <intrin.h>
#    define XXPH_mult32to64(x, y) __emulu(x, y)
#else
#    define XXPH_mult32to64(x, y) ((xxh_u64)((x) & 0xFFFFFFFF) * (xxh_u64)((y) & 0xFFFFFFFF))
#endif

/* VSX stuff. It's a lot because VSX support is mediocre across compilers and
 * there is a lot of mischief with endianness. */
#if XXPH_VECTOR == XXPH_VSX
#  include <altivec.h>
#  undef vector
typedef __vector unsigned long long U64x2;
typedef __vector unsigned char U8x16;
typedef __vector unsigned U32x4;

#ifndef XXPH_VSX_BE
#  if defined(__BIG_ENDIAN__) \
  || (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#    define XXPH_VSX_BE 1
#  elif defined(__VEC_ELEMENT_REG_ORDER__) && __VEC_ELEMENT_REG_ORDER__ == __ORDER_BIG_ENDIAN__
#    warning "-maltivec=be is not recommended. Please use native endianness."
#    define XXPH_VSX_BE 1
#  else
#    define XXPH_VSX_BE 0
#  endif
#endif

/* We need some helpers for big endian mode. */
#if XXPH_VSX_BE
/* A wrapper for POWER9's vec_revb. */
#  ifdef __POWER9_VECTOR__
#    define XXPH_vec_revb vec_revb
#  else
XXPH_FORCE_INLINE U64x2 XXPH_vec_revb(U64x2 val)
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
#    define XXPH_vec_permxor vec_permxor
#  else
#    define XXPH_vec_permxor __builtin_crypto_vpermxor
#  endif
#endif  /* XXPH_VSX_BE */
/*
 * Because we reinterpret the multiply, there are endian memes: vec_mulo actually becomes
 * vec_mule.
 *
 * Additionally, the intrinsic wasn't added until GCC 8, despite existing for a while.
 * Clang has an easy way to control this, we can just use the builtin which doesn't swap.
 * GCC needs inline assembly. */
#if __has_builtin(__builtin_altivec_vmuleuw)
#  define XXPH_vec_mulo __builtin_altivec_vmulouw
#  define XXPH_vec_mule __builtin_altivec_vmuleuw
#else
/* Adapted from https://github.com/google/highwayhash/blob/master/highwayhash/hh_vsx.h. */
XXPH_FORCE_INLINE U64x2 XXPH_vec_mulo(U32x4 a, U32x4 b) {
    U64x2 result;
    __asm__("vmulouw %0, %1, %2" : "=v" (result) : "v" (a), "v" (b));
    return result;
}
XXPH_FORCE_INLINE U64x2 XXPH_vec_mule(U32x4 a, U32x4 b) {
    U64x2 result;
    __asm__("vmuleuw %0, %1, %2" : "=v" (result) : "v" (a), "v" (b));
    return result;
}
#endif /* __has_builtin(__builtin_altivec_vmuleuw) */
#endif /* XXPH_VECTOR == XXPH_VSX */

/* prefetch
 * can be disabled, by declaring XXPH_NO_PREFETCH build macro */
#if defined(XXPH_NO_PREFETCH)
#  define XXPH_PREFETCH(ptr)  (void)(ptr)  /* disabled */
#else
#if defined(_MSC_VER) && \
    (defined(_M_X64) ||  \
     defined(_M_IX86)) /* _mm_prefetch() is not defined outside of x86/x64 */
#    include <mmintrin.h>   /* https://msdn.microsoft.com/fr-fr/library/84szxsww(v=vs.90).aspx */
#    define XXPH_PREFETCH(ptr)  _mm_prefetch((const char*)(ptr), _MM_HINT_T0)
#  elif defined(__GNUC__) && ( (__GNUC__ >= 4) || ( (__GNUC__ == 3) && (__GNUC_MINOR__ >= 1) ) )
#    define XXPH_PREFETCH(ptr)  __builtin_prefetch((ptr), 0 /* rw==read */, 3 /* locality */)
#  else
#    define XXPH_PREFETCH(ptr) (void)(ptr)  /* disabled */
#  endif
#endif  /* XXPH_NO_PREFETCH */


/* ==========================================
 * XXPH3 default settings
 * ========================================== */

#define XXPH_SECRET_DEFAULT_SIZE 192   /* minimum XXPH3_SECRET_SIZE_MIN */

#if (XXPH_SECRET_DEFAULT_SIZE < XXPH3_SECRET_SIZE_MIN)
#  error "default keyset is not large enough"
#endif

XXPH_ALIGN(64) static const xxh_u8 kSecret[XXPH_SECRET_DEFAULT_SIZE] = {
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
static XXPH128_hash_t
XXPH_mult64to128(xxh_u64 lhs, xxh_u64 rhs)
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
    XXPH128_hash_t const r128 = { (xxh_u64)(product), (xxh_u64)(product >> 64) };
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
    XXPH128_hash_t const r128 = { product_low, product_high };
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
    xxh_u64 const lo_lo = XXPH_mult32to64(lhs & 0xFFFFFFFF, rhs & 0xFFFFFFFF);
    xxh_u64 const hi_lo = XXPH_mult32to64(lhs >> 32,        rhs & 0xFFFFFFFF);
    xxh_u64 const lo_hi = XXPH_mult32to64(lhs & 0xFFFFFFFF, rhs >> 32);
    xxh_u64 const hi_hi = XXPH_mult32to64(lhs >> 32,        rhs >> 32);

    /* Now add the products together. These will never overflow. */
    xxh_u64 const cross = (lo_lo >> 32) + (hi_lo & 0xFFFFFFFF) + lo_hi;
    xxh_u64 const upper = (hi_lo >> 32) + (cross >> 32)        + hi_hi;
    xxh_u64 const lower = (cross << 32) | (lo_lo & 0xFFFFFFFF);

    XXPH128_hash_t r128 = { lower, upper };
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
XXPH3_mul128_fold64(xxh_u64 lhs, xxh_u64 rhs)
{
    XXPH128_hash_t product = XXPH_mult64to128(lhs, rhs);
    return product.low64 ^ product.high64;
}


static XXPH64_hash_t XXPH3_avalanche(xxh_u64 h64)
{
    h64 ^= h64 >> 37;
    h64 *= PRIME64_3;
    h64 ^= h64 >> 32;
    return h64;
}


/* ==========================================
 * Short keys
 * ========================================== */

XXPH_FORCE_INLINE XXPH64_hash_t
XXPH3_len_1to3_64b(const xxh_u8* input, size_t len, const xxh_u8* secret, XXPH64_hash_t seed)
{
    XXPH_ASSERT(input != NULL);
    XXPH_ASSERT(1 <= len && len <= 3);
    XXPH_ASSERT(secret != NULL);
    {   xxh_u8 const c1 = input[0];
        xxh_u8 const c2 = input[len >> 1];
        xxh_u8 const c3 = input[len - 1];
        xxh_u32  const combined = ((xxh_u32)c1) | (((xxh_u32)c2) << 8) | (((xxh_u32)c3) << 16) | (((xxh_u32)len) << 24);
        xxh_u64  const keyed = (xxh_u64)combined ^ (XXPH_readLE32(secret) + seed);
        xxh_u64  const mixed = keyed * PRIME64_1;
        return XXPH3_avalanche(mixed);
    }
}

XXPH_FORCE_INLINE XXPH64_hash_t
XXPH3_len_4to8_64b(const xxh_u8* input, size_t len, const xxh_u8* secret, XXPH64_hash_t seed)
{
    XXPH_ASSERT(input != NULL);
    XXPH_ASSERT(secret != NULL);
    XXPH_ASSERT(4 <= len && len <= 8);
    {   xxh_u32 const input_lo = XXPH_readLE32(input);
        xxh_u32 const input_hi = XXPH_readLE32(input + len - 4);
        xxh_u64 const input_64 = input_lo | ((xxh_u64)input_hi << 32);
        xxh_u64 const keyed = input_64 ^ (XXPH_readLE64(secret) + seed);
        xxh_u64 const mix64 = len + ((keyed ^ (keyed >> 51)) * PRIME32_1);
        return XXPH3_avalanche((mix64 ^ (mix64 >> 47)) * PRIME64_2);
    }
}

XXPH_FORCE_INLINE XXPH64_hash_t
XXPH3_len_9to16_64b(const xxh_u8* input, size_t len, const xxh_u8* secret, XXPH64_hash_t seed)
{
    XXPH_ASSERT(input != NULL);
    XXPH_ASSERT(secret != NULL);
    XXPH_ASSERT(9 <= len && len <= 16);
    {   xxh_u64 const input_lo = XXPH_readLE64(input)           ^ (XXPH_readLE64(secret)     + seed);
        xxh_u64 const input_hi = XXPH_readLE64(input + len - 8) ^ (XXPH_readLE64(secret + 8) - seed);
        xxh_u64 const acc = len + (input_lo + input_hi) + XXPH3_mul128_fold64(input_lo, input_hi);
        return XXPH3_avalanche(acc);
    }
}

XXPH_FORCE_INLINE XXPH64_hash_t
XXPH3_len_0to16_64b(const xxh_u8* input, size_t len, const xxh_u8* secret, XXPH64_hash_t seed)
{
    XXPH_ASSERT(len <= 16);
    {   if (len > 8) return XXPH3_len_9to16_64b(input, len, secret, seed);
        if (len >= 4) return XXPH3_len_4to8_64b(input, len, secret, seed);
        if (len) return XXPH3_len_1to3_64b(input, len, secret, seed);
        /*
         * RocksDB modification from XXPH3 preview: zero result for empty
         * string can be problematic for multiplication-based algorithms.
         * Return a hash of the seed instead.
         */
        return XXPH3_mul128_fold64(seed + XXPH_readLE64(secret), PRIME64_2);
    }
}


/* ===    Long Keys    === */

#define STRIPE_LEN 64
#define XXPH_SECRET_CONSUME_RATE 8   /* nb of secret bytes consumed at each accumulation */
#define ACC_NB (STRIPE_LEN / sizeof(xxh_u64))

typedef enum { XXPH3_acc_64bits, XXPH3_acc_128bits } XXPH3_accWidth_e;

XXPH_FORCE_INLINE void
XXPH3_accumulate_512(      void* XXPH_RESTRICT acc,
                    const void* XXPH_RESTRICT input,
                    const void* XXPH_RESTRICT secret,
                    XXPH3_accWidth_e accWidth)
{
#if (XXPH_VECTOR == XXPH_AVX2)

    XXPH_ASSERT((((size_t)acc) & 31) == 0);
    {   XXPH_ALIGN(32) __m256i* const xacc  =       (__m256i *) acc;
        const         __m256i* const xinput = (const __m256i *) input;  /* not really aligned, just for ptr arithmetic, and because _mm256_loadu_si256() requires this type */
        const         __m256i* const xsecret = (const __m256i *) secret;   /* not really aligned, just for ptr arithmetic, and because _mm256_loadu_si256() requires this type */

        size_t i;
        for (i=0; i < STRIPE_LEN/sizeof(__m256i); i++) {
            __m256i const data_vec = _mm256_loadu_si256 (xinput+i);
            __m256i const key_vec = _mm256_loadu_si256 (xsecret+i);
            __m256i const data_key = _mm256_xor_si256 (data_vec, key_vec);                                  /* uint32 dk[8]  = {d0+k0, d1+k1, d2+k2, d3+k3, ...} */
            __m256i const product = _mm256_mul_epu32 (data_key, _mm256_shuffle_epi32 (data_key, 0x31));  /* uint64 mul[4] = {dk0*dk1, dk2*dk3, ...} */
            if (accWidth == XXPH3_acc_128bits) {
                __m256i const data_swap = _mm256_shuffle_epi32(data_vec, _MM_SHUFFLE(1,0,3,2));
                __m256i const sum = _mm256_add_epi64(xacc[i], data_swap);
                xacc[i]  = _mm256_add_epi64(product, sum);
            } else {  /* XXPH3_acc_64bits */
                __m256i const sum = _mm256_add_epi64(xacc[i], data_vec);
                xacc[i]  = _mm256_add_epi64(product, sum);
            }
    }   }

#elif (XXPH_VECTOR == XXPH_SSE2)

    XXPH_ASSERT((((size_t)acc) & 15) == 0);
    {   XXPH_ALIGN(16) __m128i* const xacc  =       (__m128i *) acc;
        const         __m128i* const xinput = (const __m128i *) input;  /* not really aligned, just for ptr arithmetic, and because _mm_loadu_si128() requires this type */
        const         __m128i* const xsecret = (const __m128i *) secret;   /* not really aligned, just for ptr arithmetic, and because _mm_loadu_si128() requires this type */

        size_t i;
        for (i=0; i < STRIPE_LEN/sizeof(__m128i); i++) {
            __m128i const data_vec = _mm_loadu_si128 (xinput+i);
            __m128i const key_vec = _mm_loadu_si128 (xsecret+i);
            __m128i const data_key = _mm_xor_si128 (data_vec, key_vec);                                  /* uint32 dk[8]  = {d0+k0, d1+k1, d2+k2, d3+k3, ...} */
            __m128i const product = _mm_mul_epu32 (data_key, _mm_shuffle_epi32 (data_key, 0x31));  /* uint64 mul[4] = {dk0*dk1, dk2*dk3, ...} */
            if (accWidth == XXPH3_acc_128bits) {
                __m128i const data_swap = _mm_shuffle_epi32(data_vec, _MM_SHUFFLE(1,0,3,2));
                __m128i const sum = _mm_add_epi64(xacc[i], data_swap);
                xacc[i]  = _mm_add_epi64(product, sum);
            } else {  /* XXPH3_acc_64bits */
                __m128i const sum = _mm_add_epi64(xacc[i], data_vec);
                xacc[i]  = _mm_add_epi64(product, sum);
            }
    }   }

#elif (XXPH_VECTOR == XXPH_NEON)

    XXPH_ASSERT((((size_t)acc) & 15) == 0);
    {
        XXPH_ALIGN(16) uint64x2_t* const xacc = (uint64x2_t *) acc;
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

            if (accWidth == XXPH3_acc_64bits) {
                /* Add first to prevent register swaps */
                /* xacc[i] += data_vec; */
                xacc[i] = vaddq_u64 (xacc[i], vreinterpretq_u64_u8(data_vec));
            } else {  /* XXPH3_acc_128bits */
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
            if (accWidth == XXPH3_acc_64bits) {
                /* xacc[i] += data_vec; */
                xacc[i] = vaddq_u64 (xacc[i], vreinterpretq_u64_u8(data_vec));
            } else {  /* XXPH3_acc_128bits */
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

#elif (XXPH_VECTOR == XXPH_VSX) && /* work around a compiler bug */ (__GNUC__ > 5)
          U64x2* const xacc =        (U64x2*) acc;    /* presumed aligned */
    U64x2 const* const xinput = (U64x2 const*) input;   /* no alignment restriction */
    U64x2 const* const xsecret  = (U64x2 const*) secret;    /* no alignment restriction */
    U64x2 const v32 = { 32,  32 };
#if XXPH_VSX_BE
    U8x16 const vXorSwap  = { 0x07, 0x16, 0x25, 0x34, 0x43, 0x52, 0x61, 0x70,
                              0x8F, 0x9E, 0xAD, 0xBC, 0xCB, 0xDA, 0xE9, 0xF8 };
#endif
    size_t i;
    for (i = 0; i < STRIPE_LEN / sizeof(U64x2); i++) {
        /* data_vec = xinput[i]; */
        /* key_vec = xsecret[i]; */
#if XXPH_VSX_BE
        /* byteswap */
        U64x2 const data_vec = XXPH_vec_revb(vec_vsx_ld(0, xinput + i));
        U64x2 const key_raw = vec_vsx_ld(0, xsecret + i);
        /* See comment above. data_key = data_vec ^ swap(xsecret[i]); */
        U64x2 const data_key = (U64x2)XXPH_vec_permxor((U8x16)data_vec, (U8x16)key_raw, vXorSwap);
#else
        U64x2 const data_vec = vec_vsx_ld(0, xinput + i);
        U64x2 const key_vec = vec_vsx_ld(0, xsecret + i);
        U64x2 const data_key = data_vec ^ key_vec;
#endif
        /* shuffled = (data_key << 32) | (data_key >> 32); */
        U32x4 const shuffled = (U32x4)vec_rl(data_key, v32);
        /* product = ((U64x2)data_key & 0xFFFFFFFF) * ((U64x2)shuffled & 0xFFFFFFFF); */
        U64x2 const product = XXPH_vec_mulo((U32x4)data_key, shuffled);
        xacc[i] += product;

        if (accWidth == XXPH3_acc_64bits) {
            xacc[i] += data_vec;
        } else {  /* XXPH3_acc_128bits */
            /* swap high and low halves */
            U64x2 const data_swapped = vec_xxpermdi(data_vec, data_vec, 2);
            xacc[i] += data_swapped;
        }
    }

#else   /* scalar variant of Accumulator - universal */

    XXPH_ALIGN(XXPH_ACC_ALIGN) xxh_u64* const xacc = (xxh_u64*) acc;    /* presumed aligned on 32-bytes boundaries, little hint for the auto-vectorizer */
    const xxh_u8* const xinput = (const xxh_u8*) input;  /* no alignment restriction */
    const xxh_u8* const xsecret  = (const xxh_u8*) secret;   /* no alignment restriction */
    size_t i;
    XXPH_ASSERT(((size_t)acc & (XXPH_ACC_ALIGN-1)) == 0);
    for (i=0; i < ACC_NB; i++) {
        xxh_u64 const data_val = XXPH_readLE64(xinput + 8*i);
        xxh_u64 const data_key = data_val ^ XXPH_readLE64(xsecret + i*8);

        if (accWidth == XXPH3_acc_64bits) {
            xacc[i] += data_val;
        } else {
            xacc[i ^ 1] += data_val; /* swap adjacent lanes */
        }
        xacc[i] += XXPH_mult32to64(data_key & 0xFFFFFFFF, data_key >> 32);
    }
#endif
}

XXPH_FORCE_INLINE void
XXPH3_scrambleAcc(void* XXPH_RESTRICT acc, const void* XXPH_RESTRICT secret)
{
#if (XXPH_VECTOR == XXPH_AVX2)

    XXPH_ASSERT((((size_t)acc) & 31) == 0);
    {   XXPH_ALIGN(32) __m256i* const xacc = (__m256i*) acc;
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

#elif (XXPH_VECTOR == XXPH_SSE2)

    XXPH_ASSERT((((size_t)acc) & 15) == 0);
    {   XXPH_ALIGN(16) __m128i* const xacc = (__m128i*) acc;
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

#elif (XXPH_VECTOR == XXPH_NEON)

    XXPH_ASSERT((((size_t)acc) & 15) == 0);

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

#elif (XXPH_VECTOR == XXPH_VSX) && /* work around a compiler bug */ (__GNUC__ > 5)

          U64x2* const xacc =       (U64x2*) acc;
    const U64x2* const xsecret = (const U64x2*) secret;
    /* constants */
    U64x2 const v32  = { 32, 32 };
    U64x2 const v47 = { 47, 47 };
    U32x4 const prime = { PRIME32_1, PRIME32_1, PRIME32_1, PRIME32_1 };
    size_t i;
#if XXPH_VSX_BE
    /* endian swap */
    U8x16 const vXorSwap  = { 0x07, 0x16, 0x25, 0x34, 0x43, 0x52, 0x61, 0x70,
                              0x8F, 0x9E, 0xAD, 0xBC, 0xCB, 0xDA, 0xE9, 0xF8 };
#endif
    for (i = 0; i < STRIPE_LEN / sizeof(U64x2); i++) {
        U64x2 const acc_vec  = xacc[i];
        U64x2 const data_vec = acc_vec ^ (acc_vec >> v47);
        /* key_vec = xsecret[i]; */
#if XXPH_VSX_BE
        /* swap bytes words */
        U64x2 const key_raw  = vec_vsx_ld(0, xsecret + i);
        U64x2 const data_key = (U64x2)XXPH_vec_permxor((U8x16)data_vec, (U8x16)key_raw, vXorSwap);
#else
        U64x2 const key_vec  = vec_vsx_ld(0, xsecret + i);
        U64x2 const data_key = data_vec ^ key_vec;
#endif

        /* data_key *= PRIME32_1 */

        /* prod_lo = ((U64x2)data_key & 0xFFFFFFFF) * ((U64x2)prime & 0xFFFFFFFF);  */
        U64x2 const prod_even  = XXPH_vec_mule((U32x4)data_key, prime);
        /* prod_hi = ((U64x2)data_key >> 32) * ((U64x2)prime >> 32);  */
        U64x2 const prod_odd  = XXPH_vec_mulo((U32x4)data_key, prime);
        xacc[i] = prod_odd + (prod_even << v32);
    }

#else   /* scalar variant of Scrambler - universal */

    XXPH_ALIGN(XXPH_ACC_ALIGN) xxh_u64* const xacc = (xxh_u64*) acc;   /* presumed aligned on 32-bytes boundaries, little hint for the auto-vectorizer */
    const xxh_u8* const xsecret = (const xxh_u8*) secret;   /* no alignment restriction */
    size_t i;
    XXPH_ASSERT((((size_t)acc) & (XXPH_ACC_ALIGN-1)) == 0);
    for (i=0; i < ACC_NB; i++) {
        xxh_u64 const key64 = XXPH_readLE64(xsecret + 8*i);
        xxh_u64 acc64 = xacc[i];
        acc64 ^= acc64 >> 47;
        acc64 ^= key64;
        acc64 *= PRIME32_1;
        xacc[i] = acc64;
    }

#endif
}

#define XXPH_PREFETCH_DIST 384

/* assumption : nbStripes will not overflow secret size */
XXPH_FORCE_INLINE void
XXPH3_accumulate(       xxh_u64* XXPH_RESTRICT acc,
                const xxh_u8* XXPH_RESTRICT input,
                const xxh_u8* XXPH_RESTRICT secret,
                      size_t nbStripes,
                      XXPH3_accWidth_e accWidth)
{
    size_t n;
    for (n = 0; n < nbStripes; n++ ) {
        const xxh_u8* const in = input + n*STRIPE_LEN;
        XXPH_PREFETCH(in + XXPH_PREFETCH_DIST);
        XXPH3_accumulate_512(acc,
                            in,
                            secret + n*XXPH_SECRET_CONSUME_RATE,
                            accWidth);
    }
}

/* note : clang auto-vectorizes well in SS2 mode _if_ this function is `static`,
 *        and doesn't auto-vectorize it at all if it is `FORCE_INLINE`.
 *        However, it auto-vectorizes better AVX2 if it is `FORCE_INLINE`
 *        Pretty much every other modes and compilers prefer `FORCE_INLINE`.
 */

#if defined(__clang__) && (XXPH_VECTOR==0) && !defined(__AVX2__) && !defined(__arm__) && !defined(__thumb__)
static void
#else
XXPH_FORCE_INLINE void
#endif
XXPH3_hashLong_internal_loop( xxh_u64* XXPH_RESTRICT acc,
                      const xxh_u8* XXPH_RESTRICT input, size_t len,
                      const xxh_u8* XXPH_RESTRICT secret, size_t secretSize,
                            XXPH3_accWidth_e accWidth)
{
    size_t const nb_rounds = (secretSize - STRIPE_LEN) / XXPH_SECRET_CONSUME_RATE;
    size_t const block_len = STRIPE_LEN * nb_rounds;
    size_t const nb_blocks = len / block_len;

    size_t n;

    XXPH_ASSERT(secretSize >= XXPH3_SECRET_SIZE_MIN);

    for (n = 0; n < nb_blocks; n++) {
        XXPH3_accumulate(acc, input + n*block_len, secret, nb_rounds, accWidth);
        XXPH3_scrambleAcc(acc, secret + secretSize - STRIPE_LEN);
    }

    /* last partial block */
    XXPH_ASSERT(len > STRIPE_LEN);
    {   size_t const nbStripes = (len - (block_len * nb_blocks)) / STRIPE_LEN;
        XXPH_ASSERT(nbStripes <= (secretSize / XXPH_SECRET_CONSUME_RATE));
        XXPH3_accumulate(acc, input + nb_blocks*block_len, secret, nbStripes, accWidth);

        /* last stripe */
        if (len & (STRIPE_LEN - 1)) {
            const xxh_u8* const p = input + len - STRIPE_LEN;
#define XXPH_SECRET_LASTACC_START 7  /* do not align on 8, so that secret is different from scrambler */
            XXPH3_accumulate_512(acc, p, secret + secretSize - STRIPE_LEN - XXPH_SECRET_LASTACC_START, accWidth);
    }   }
}

XXPH_FORCE_INLINE xxh_u64
XXPH3_mix2Accs(const xxh_u64* XXPH_RESTRICT acc, const xxh_u8* XXPH_RESTRICT secret)
{
    return XXPH3_mul128_fold64(
               acc[0] ^ XXPH_readLE64(secret),
               acc[1] ^ XXPH_readLE64(secret+8) );
}

static XXPH64_hash_t
XXPH3_mergeAccs(const xxh_u64* XXPH_RESTRICT acc, const xxh_u8* XXPH_RESTRICT secret, xxh_u64 start)
{
    xxh_u64 result64 = start;

    result64 += XXPH3_mix2Accs(acc+0, secret +  0);
    result64 += XXPH3_mix2Accs(acc+2, secret + 16);
    result64 += XXPH3_mix2Accs(acc+4, secret + 32);
    result64 += XXPH3_mix2Accs(acc+6, secret + 48);

    return XXPH3_avalanche(result64);
}

#define XXPH3_INIT_ACC { PRIME32_3, PRIME64_1, PRIME64_2, PRIME64_3, \
                        PRIME64_4, PRIME32_2, PRIME64_5, PRIME32_1 };

XXPH_FORCE_INLINE XXPH64_hash_t
XXPH3_hashLong_internal(const xxh_u8* XXPH_RESTRICT input, size_t len,
                       const xxh_u8* XXPH_RESTRICT secret, size_t secretSize)
{
    XXPH_ALIGN(XXPH_ACC_ALIGN) xxh_u64 acc[ACC_NB] = XXPH3_INIT_ACC;

    XXPH3_hashLong_internal_loop(acc, input, len, secret, secretSize, XXPH3_acc_64bits);

    /* converge into final hash */
    XXPH_STATIC_ASSERT(sizeof(acc) == 64);
#define XXPH_SECRET_MERGEACCS_START 11  /* do not align on 8, so that secret is different from accumulator */
    XXPH_ASSERT(secretSize >= sizeof(acc) + XXPH_SECRET_MERGEACCS_START);
    return XXPH3_mergeAccs(acc, secret + XXPH_SECRET_MERGEACCS_START, (xxh_u64)len * PRIME64_1);
}


XXPH_NO_INLINE XXPH64_hash_t    /* It's important for performance that XXPH3_hashLong is not inlined. Not sure why (uop cache maybe ?), but difference is large and easily measurable */
XXPH3_hashLong_64b_defaultSecret(const xxh_u8* XXPH_RESTRICT input, size_t len)
{
    return XXPH3_hashLong_internal(input, len, kSecret, sizeof(kSecret));
}

XXPH_NO_INLINE XXPH64_hash_t    /* It's important for performance that XXPH3_hashLong is not inlined. Not sure why (uop cache maybe ?), but difference is large and easily measurable */
XXPH3_hashLong_64b_withSecret(const xxh_u8* XXPH_RESTRICT input, size_t len,
                             const xxh_u8* XXPH_RESTRICT secret, size_t secretSize)
{
    return XXPH3_hashLong_internal(input, len, secret, secretSize);
}


XXPH_FORCE_INLINE void XXPH_writeLE64(void* dst, xxh_u64 v64)
{
    if (!XXPH_CPU_LITTLE_ENDIAN) v64 = XXPH_swap64(v64);
    memcpy(dst, &v64, sizeof(v64));
}

/* XXPH3_initCustomSecret() :
 * destination `customSecret` is presumed allocated and same size as `kSecret`.
 */
XXPH_FORCE_INLINE void XXPH3_initCustomSecret(xxh_u8* customSecret, xxh_u64 seed64)
{
    int const nbRounds = XXPH_SECRET_DEFAULT_SIZE / 16;
    int i;

    XXPH_STATIC_ASSERT((XXPH_SECRET_DEFAULT_SIZE & 15) == 0);

    for (i=0; i < nbRounds; i++) {
        XXPH_writeLE64(customSecret + 16*i,     XXPH_readLE64(kSecret + 16*i)     + seed64);
        XXPH_writeLE64(customSecret + 16*i + 8, XXPH_readLE64(kSecret + 16*i + 8) - seed64);
    }
}


/* XXPH3_hashLong_64b_withSeed() :
 * Generate a custom key,
 * based on alteration of default kSecret with the seed,
 * and then use this key for long mode hashing.
 * This operation is decently fast but nonetheless costs a little bit of time.
 * Try to avoid it whenever possible (typically when seed==0).
 */
XXPH_NO_INLINE XXPH64_hash_t    /* It's important for performance that XXPH3_hashLong is not inlined. Not sure why (uop cache maybe ?), but difference is large and easily measurable */
XXPH3_hashLong_64b_withSeed(const xxh_u8* input, size_t len, XXPH64_hash_t seed)
{
    XXPH_ALIGN(8) xxh_u8 secret[XXPH_SECRET_DEFAULT_SIZE];
    if (seed==0) return XXPH3_hashLong_64b_defaultSecret(input, len);
    XXPH3_initCustomSecret(secret, seed);
    return XXPH3_hashLong_internal(input, len, secret, sizeof(secret));
}


XXPH_FORCE_INLINE xxh_u64 XXPH3_mix16B(const xxh_u8* XXPH_RESTRICT input,
                                 const xxh_u8* XXPH_RESTRICT secret, xxh_u64 seed64)
{
    xxh_u64 const input_lo = XXPH_readLE64(input);
    xxh_u64 const input_hi = XXPH_readLE64(input+8);
    return XXPH3_mul128_fold64(
               input_lo ^ (XXPH_readLE64(secret)   + seed64),
               input_hi ^ (XXPH_readLE64(secret+8) - seed64) );
}


XXPH_FORCE_INLINE XXPH64_hash_t
XXPH3_len_17to128_64b(const xxh_u8* XXPH_RESTRICT input, size_t len,
                     const xxh_u8* XXPH_RESTRICT secret, size_t secretSize,
                     XXPH64_hash_t seed)
{
    XXPH_ASSERT(secretSize >= XXPH3_SECRET_SIZE_MIN); (void)secretSize;
    XXPH_ASSERT(16 < len && len <= 128);

    {   xxh_u64 acc = len * PRIME64_1;
        if (len > 32) {
            if (len > 64) {
                if (len > 96) {
                    acc += XXPH3_mix16B(input+48, secret+96, seed);
                    acc += XXPH3_mix16B(input+len-64, secret+112, seed);
                }
                acc += XXPH3_mix16B(input+32, secret+64, seed);
                acc += XXPH3_mix16B(input+len-48, secret+80, seed);
            }
            acc += XXPH3_mix16B(input+16, secret+32, seed);
            acc += XXPH3_mix16B(input+len-32, secret+48, seed);
        }
        acc += XXPH3_mix16B(input+0, secret+0, seed);
        acc += XXPH3_mix16B(input+len-16, secret+16, seed);

        return XXPH3_avalanche(acc);
    }
}

#define XXPH3_MIDSIZE_MAX 240

XXPH_NO_INLINE XXPH64_hash_t
XXPH3_len_129to240_64b(const xxh_u8* XXPH_RESTRICT input, size_t len,
                      const xxh_u8* XXPH_RESTRICT secret, size_t secretSize,
                      XXPH64_hash_t seed)
{
    XXPH_ASSERT(secretSize >= XXPH3_SECRET_SIZE_MIN); (void)secretSize;
    XXPH_ASSERT(128 < len && len <= XXPH3_MIDSIZE_MAX);

    #define XXPH3_MIDSIZE_STARTOFFSET 3
    #define XXPH3_MIDSIZE_LASTOFFSET  17

    {   xxh_u64 acc = len * PRIME64_1;
        int const nbRounds = (int)len / 16;
        int i;
        for (i=0; i<8; i++) {
            acc += XXPH3_mix16B(input+(16*i), secret+(16*i), seed);
        }
        acc = XXPH3_avalanche(acc);
        XXPH_ASSERT(nbRounds >= 8);
        for (i=8 ; i < nbRounds; i++) {
            acc += XXPH3_mix16B(input+(16*i), secret+(16*(i-8)) + XXPH3_MIDSIZE_STARTOFFSET, seed);
        }
        /* last bytes */
        acc += XXPH3_mix16B(input + len - 16, secret + XXPH3_SECRET_SIZE_MIN - XXPH3_MIDSIZE_LASTOFFSET, seed);
        return XXPH3_avalanche(acc);
    }
}

/* ===   Public entry point   === */

XXPH_PUBLIC_API XXPH64_hash_t XXPH3_64bits(const void* input, size_t len)
{
    if (len <= 16) return XXPH3_len_0to16_64b((const xxh_u8*)input, len, kSecret, 0);
    if (len <= 128) return XXPH3_len_17to128_64b((const xxh_u8*)input, len, kSecret, sizeof(kSecret), 0);
    if (len <= XXPH3_MIDSIZE_MAX) return XXPH3_len_129to240_64b((const xxh_u8*)input, len, kSecret, sizeof(kSecret), 0);
    return XXPH3_hashLong_64b_defaultSecret((const xxh_u8*)input, len);
}

XXPH_PUBLIC_API XXPH64_hash_t
XXPH3_64bits_withSecret(const void* input, size_t len, const void* secret, size_t secretSize)
{
    XXPH_ASSERT(secretSize >= XXPH3_SECRET_SIZE_MIN);
    /* if an action must be taken should `secret` conditions not be respected,
     * it should be done here.
     * For now, it's a contract pre-condition.
     * Adding a check and a branch here would cost performance at every hash */
     if (len <= 16) return XXPH3_len_0to16_64b((const xxh_u8*)input, len, (const xxh_u8*)secret, 0);
     if (len <= 128) return XXPH3_len_17to128_64b((const xxh_u8*)input, len, (const xxh_u8*)secret, secretSize, 0);
     if (len <= XXPH3_MIDSIZE_MAX) return XXPH3_len_129to240_64b((const xxh_u8*)input, len, (const xxh_u8*)secret, secretSize, 0);
     return XXPH3_hashLong_64b_withSecret((const xxh_u8*)input, len, (const xxh_u8*)secret, secretSize);
}

XXPH_PUBLIC_API XXPH64_hash_t
XXPH3_64bits_withSeed(const void* input, size_t len, XXPH64_hash_t seed)
{
    if (len <= 16) return XXPH3_len_0to16_64b((const xxh_u8*)input, len, kSecret, seed);
    if (len <= 128) return XXPH3_len_17to128_64b((const xxh_u8*)input, len, kSecret, sizeof(kSecret), seed);
    if (len <= XXPH3_MIDSIZE_MAX) return XXPH3_len_129to240_64b((const xxh_u8*)input, len, kSecret, sizeof(kSecret), seed);
    return XXPH3_hashLong_64b_withSeed((const xxh_u8*)input, len, seed);
}

/* ===   XXPH3 streaming   === */

/* RocksDB Note: unused & removed due to bug in preview version */

/*======== END #include "xxh3.h", now inlined above ==========*/

#endif  /* XXPH_NO_LONG_LONG */

/* === END RocksDB modification of permanently inlining === */

#endif /*  defined(XXPH_INLINE_ALL) || defined(XXPH_PRIVATE_API) */

#endif /* XXPH_STATIC_LINKING_ONLY */

#if defined (__cplusplus)
}
#endif

#endif /* XXPHASH_H_5627135585666179 */
