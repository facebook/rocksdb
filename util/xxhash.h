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

/* Notice extracted from xxHash homepage :

xxHash is an extremely fast Hash algorithm, running at RAM speed limits.
It also successfully passes all tests from the SMHasher suite.

Comparison (single thread, Windows Seven 32 bits, using SMHasher on a Core 2 Duo @3GHz)

Name            Speed       Q.Score   Author
xxHash          5.4 GB/s     10
CrapWow         3.2 GB/s      2       Andrew
MumurHash 3a    2.7 GB/s     10       Austin Appleby
SpookyHash      2.0 GB/s     10       Bob Jenkins
SBox            1.4 GB/s      9       Bret Mulvey
Lookup3         1.2 GB/s      9       Bob Jenkins
SuperFastHash   1.2 GB/s      1       Paul Hsieh
CityHash64      1.05 GB/s    10       Pike & Alakuijala
FNV             0.55 GB/s     5       Fowler, Noll, Vo
CRC32           0.43 GB/s †   9
MD5-32          0.33 GB/s    10       Ronald L. Rivest
SHA1-32         0.28 GB/s    10

Note †: other CRC32 implementations can be over 40x faster than SMHasher's:
http://fastcompression.blogspot.com/2019/03/presenting-xxh3.html?showComment=1552696407071#c3490092340461170735

Q.Score is a measure of quality of the hash function.
It depends on successfully passing SMHasher test set.
10 is a perfect score.

A 64-bit version, named XXH64, is available since r35.
It offers much better speed, but for 64-bit applications only.
Name     Speed on 64 bits    Speed on 32 bits
XXH64       13.8 GB/s            1.9 GB/s
XXH32        6.8 GB/s            6.0 GB/s
*/

#ifndef XXHASH_H_5627135585666179
#define XXHASH_H_5627135585666179 1

#if defined (__cplusplus)
extern "C" {
#endif


/* ****************************
*  Definitions
******************************/
#include <stddef.h>   /* size_t */
typedef enum { XXH_OK=0, XXH_ERROR } XXH_errorcode;


/* ****************************
 *  API modifier
 ******************************/
/** XXH_INLINE_ALL (and XXH_PRIVATE_API)
 *  This build macro includes xxhash functions in `static` mode
 *  in order to inline them, and remove their symbol from the public list.
 *  Inlining offers great performance improvement on small keys,
 *  and dramatic ones when length is expressed as a compile-time constant.
 *  See https://fastcompression.blogspot.com/2018/03/xxhash-for-small-keys-impressive-power.html .
 *  Methodology :
 *     #define XXH_INLINE_ALL
 *     #include "xxhash.h"
 * `xxhash.c` is automatically included.
 *  It's not useful to compile and link it as a separate object.
 */
#if defined(XXH_INLINE_ALL) || defined(XXH_PRIVATE_API)
#  ifndef XXH_STATIC_LINKING_ONLY
#    define XXH_STATIC_LINKING_ONLY
#  endif
#  if defined(__GNUC__)
#    define XXH_PUBLIC_API static __inline __attribute__((unused))
#  elif defined (__cplusplus) || (defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */)
#    define XXH_PUBLIC_API static inline
#  elif defined(_MSC_VER)
#    define XXH_PUBLIC_API static __inline
#  else
     /* this version may generate warnings for unused static functions */
#    define XXH_PUBLIC_API static
#  endif
#else
#  if defined(WIN32) && defined(_MSC_VER) && (defined(XXH_IMPORT) || defined(XXH_EXPORT))
#    ifdef XXH_EXPORT
#      define XXH_PUBLIC_API __declspec(dllexport)
#    elif XXH_IMPORT
#      define XXH_PUBLIC_API __declspec(dllimport)
#    endif
#  else
#    define XXH_PUBLIC_API   /* do nothing */
#  endif
#endif /* XXH_INLINE_ALL || XXH_PRIVATE_API */

/*! XXH_NAMESPACE, aka Namespace Emulation :
 *
 * If you want to include _and expose_ xxHash functions from within your own library,
 * but also want to avoid symbol collisions with other libraries which may also include xxHash,
 *
 * you can use XXH_NAMESPACE, to automatically prefix any public symbol from xxhash library
 * with the value of XXH_NAMESPACE (therefore, avoid NULL and numeric values).
 *
 * Note that no change is required within the calling program as long as it includes `xxhash.h` :
 * regular symbol name will be automatically translated by this header.
 */
#ifdef XXH_NAMESPACE
#  define XXH_CAT(A,B) A##B
#  define XXH_NAME2(A,B) XXH_CAT(A,B)
#  define XXH_versionNumber XXH_NAME2(XXH_NAMESPACE, XXH_versionNumber)
#  define XXH32 XXH_NAME2(XXH_NAMESPACE, XXH32)
#  define XXH32_createState XXH_NAME2(XXH_NAMESPACE, XXH32_createState)
#  define XXH32_freeState XXH_NAME2(XXH_NAMESPACE, XXH32_freeState)
#  define XXH32_reset XXH_NAME2(XXH_NAMESPACE, XXH32_reset)
#  define XXH32_update XXH_NAME2(XXH_NAMESPACE, XXH32_update)
#  define XXH32_digest XXH_NAME2(XXH_NAMESPACE, XXH32_digest)
#  define XXH32_copyState XXH_NAME2(XXH_NAMESPACE, XXH32_copyState)
#  define XXH32_canonicalFromHash XXH_NAME2(XXH_NAMESPACE, XXH32_canonicalFromHash)
#  define XXH32_hashFromCanonical XXH_NAME2(XXH_NAMESPACE, XXH32_hashFromCanonical)
#  define XXH64 XXH_NAME2(XXH_NAMESPACE, XXH64)
#  define XXH64_createState XXH_NAME2(XXH_NAMESPACE, XXH64_createState)
#  define XXH64_freeState XXH_NAME2(XXH_NAMESPACE, XXH64_freeState)
#  define XXH64_reset XXH_NAME2(XXH_NAMESPACE, XXH64_reset)
#  define XXH64_update XXH_NAME2(XXH_NAMESPACE, XXH64_update)
#  define XXH64_digest XXH_NAME2(XXH_NAMESPACE, XXH64_digest)
#  define XXH64_copyState XXH_NAME2(XXH_NAMESPACE, XXH64_copyState)
#  define XXH64_canonicalFromHash XXH_NAME2(XXH_NAMESPACE, XXH64_canonicalFromHash)
#  define XXH64_hashFromCanonical XXH_NAME2(XXH_NAMESPACE, XXH64_hashFromCanonical)
#endif


/* *************************************
*  Version
***************************************/
#define XXH_VERSION_MAJOR    0
#define XXH_VERSION_MINOR    7
#define XXH_VERSION_RELEASE  2
#define XXH_VERSION_NUMBER  (XXH_VERSION_MAJOR *100*100 + XXH_VERSION_MINOR *100 + XXH_VERSION_RELEASE)
XXH_PUBLIC_API unsigned XXH_versionNumber (void);


/*-**********************************************************************
*  32-bit hash
************************************************************************/
#if !defined (__VMS) \
  && (defined (__cplusplus) \
  || (defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */) )
#   include <stdint.h>
    typedef uint32_t XXH32_hash_t;
#else
#   include <limits.h>
#   if UINT_MAX == 0xFFFFFFFFUL
      typedef unsigned int XXH32_hash_t;
#   else
#     if ULONG_MAX == 0xFFFFFFFFUL
        typedef unsigned long XXH32_hash_t;
#     else
#       error "unsupported platform : need a 32-bit type"
#     endif
#   endif
#endif

/*! XXH32() :
    Calculate the 32-bit hash of sequence "length" bytes stored at memory address "input".
    The memory between input & input+length must be valid (allocated and read-accessible).
    "seed" can be used to alter the result predictably.
    Speed on Core 2 Duo @ 3 GHz (single thread, SMHasher benchmark) : 5.4 GB/s */
XXH_PUBLIC_API XXH32_hash_t XXH32 (const void* input, size_t length, XXH32_hash_t seed);

/*======   Streaming   ======*/

/*
 * Streaming functions generate the xxHash value from an incrememtal input.
 * This method is slower than single-call functions, due to state management.
 * For small inputs, prefer `XXH32()` and `XXH64()`, which are better optimized.
 *
 * XXH state must first be allocated, using XXH*_createState() .
 *
 * Start a new hash by initializing state with a seed, using XXH*_reset().
 *
 * Then, feed the hash state by calling XXH*_update() as many times as necessary.
 * The function returns an error code, with 0 meaning OK, and any other value meaning there is an error.
 *
 * Finally, a hash value can be produced anytime, by using XXH*_digest().
 * This function returns the nn-bits hash as an int or long long.
 *
 * It's still possible to continue inserting input into the hash state after a digest,
 * and generate some new hash values later on, by invoking again XXH*_digest().
 *
 * When done, release the state, using XXH*_freeState().
 */

typedef struct XXH32_state_s XXH32_state_t;   /* incomplete type */
XXH_PUBLIC_API XXH32_state_t* XXH32_createState(void);
XXH_PUBLIC_API XXH_errorcode  XXH32_freeState(XXH32_state_t* statePtr);
XXH_PUBLIC_API void XXH32_copyState(XXH32_state_t* dst_state, const XXH32_state_t* src_state);

XXH_PUBLIC_API XXH_errorcode XXH32_reset  (XXH32_state_t* statePtr, XXH32_hash_t seed);
XXH_PUBLIC_API XXH_errorcode XXH32_update (XXH32_state_t* statePtr, const void* input, size_t length);
XXH_PUBLIC_API XXH32_hash_t  XXH32_digest (const XXH32_state_t* statePtr);

/*======   Canonical representation   ======*/

/* Default return values from XXH functions are basic unsigned 32 and 64 bits.
 * This the simplest and fastest format for further post-processing.
 * However, this leaves open the question of what is the order of bytes,
 * since little and big endian conventions will write the same number differently.
 *
 * The canonical representation settles this issue,
 * by mandating big-endian convention,
 * aka, the same convention as human-readable numbers (large digits first).
 * When writing hash values to storage, sending them over a network, or printing them,
 * it's highly recommended to use the canonical representation,
 * to ensure portability across a wider range of systems, present and future.
 *
 * The following functions allow transformation of hash values into and from canonical format.
 */

typedef struct { unsigned char digest[4]; } XXH32_canonical_t;
XXH_PUBLIC_API void XXH32_canonicalFromHash(XXH32_canonical_t* dst, XXH32_hash_t hash);
XXH_PUBLIC_API XXH32_hash_t XXH32_hashFromCanonical(const XXH32_canonical_t* src);


#ifndef XXH_NO_LONG_LONG
/*-**********************************************************************
*  64-bit hash
************************************************************************/
#if !defined (__VMS) \
  && (defined (__cplusplus) \
  || (defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */) )
#   include <stdint.h>
    typedef uint64_t XXH64_hash_t;
#else
    /* the following type must have a width of 64-bit */
    typedef unsigned long long XXH64_hash_t;
#endif

/*! XXH64() :
    Calculate the 64-bit hash of sequence of length "len" stored at memory address "input".
    "seed" can be used to alter the result predictably.
    This function runs faster on 64-bit systems, but slower on 32-bit systems (see benchmark).
*/
XXH_PUBLIC_API XXH64_hash_t XXH64 (const void* input, size_t length, XXH64_hash_t seed);

/*======   Streaming   ======*/
typedef struct XXH64_state_s XXH64_state_t;   /* incomplete type */
XXH_PUBLIC_API XXH64_state_t* XXH64_createState(void);
XXH_PUBLIC_API XXH_errorcode  XXH64_freeState(XXH64_state_t* statePtr);
XXH_PUBLIC_API void XXH64_copyState(XXH64_state_t* dst_state, const XXH64_state_t* src_state);

XXH_PUBLIC_API XXH_errorcode XXH64_reset  (XXH64_state_t* statePtr, XXH64_hash_t seed);
XXH_PUBLIC_API XXH_errorcode XXH64_update (XXH64_state_t* statePtr, const void* input, size_t length);
XXH_PUBLIC_API XXH64_hash_t  XXH64_digest (const XXH64_state_t* statePtr);

/*======   Canonical representation   ======*/
typedef struct { unsigned char digest[8]; } XXH64_canonical_t;
XXH_PUBLIC_API void XXH64_canonicalFromHash(XXH64_canonical_t* dst, XXH64_hash_t hash);
XXH_PUBLIC_API XXH64_hash_t XXH64_hashFromCanonical(const XXH64_canonical_t* src);


#endif  /* XXH_NO_LONG_LONG */



#ifdef XXH_STATIC_LINKING_ONLY

/* ================================================================================================
   This section contains declarations which are not guaranteed to remain stable.
   They may change in future versions, becoming incompatible with a different version of the library.
   These declarations should only be used with static linking.
   Never use them in association with dynamic linking !
=================================================================================================== */

/* These definitions are only present to allow
 * static allocation of XXH state, on stack or in a struct for example.
 * Never **ever** use members directly. */

struct XXH32_state_s {
   XXH32_hash_t total_len_32;
   XXH32_hash_t large_len;
   XXH32_hash_t v1;
   XXH32_hash_t v2;
   XXH32_hash_t v3;
   XXH32_hash_t v4;
   XXH32_hash_t mem32[4];
   XXH32_hash_t memsize;
   XXH32_hash_t reserved;   /* never read nor write, might be removed in a future version */
};   /* typedef'd to XXH32_state_t */

#ifndef XXH_NO_LONG_LONG  /* remove 64-bit support */
struct XXH64_state_s {
   XXH64_hash_t total_len;
   XXH64_hash_t v1;
   XXH64_hash_t v2;
   XXH64_hash_t v3;
   XXH64_hash_t v4;
   XXH64_hash_t mem64[4];
   XXH32_hash_t memsize;
   XXH32_hash_t reserved32;  /* required for padding anyway */
   XXH64_hash_t reserved64;  /* never read nor write, might be removed in a future version */
};   /* typedef'd to XXH64_state_t */
#endif   /* XXH_NO_LONG_LONG */


/*-**********************************************************************
*  XXH3
*  New experimental hash
************************************************************************/
#ifndef XXH_NO_LONG_LONG


/* ============================================
 * XXH3 is a new hash algorithm,
 * featuring improved speed performance for both small and large inputs.
 * See full speed analysis at : http://fastcompression.blogspot.com/2019/03/presenting-xxh3.html
 * In general, expect XXH3 to run about ~2x faster on large inputs,
 * and >3x faster on small ones, though exact differences depend on platform.
 *
 * The algorithm is portable, will generate the same hash on all platforms.
 * It benefits greatly from vectorization units, but does not require it.
 *
 * XXH3 offers 2 variants, _64bits and _128bits.
 * When only 64 bits are needed, prefer calling the _64bits variant :
 * it reduces the amount of mixing, resulting in faster speed on small inputs.
 * It's also generally simpler to manipulate a scalar return type than a struct.
 *
 * The XXH3 algorithm is still considered experimental.
 * Produced results can still change between versions.
 * Results produced by v0.7.x are not comparable with results from v0.7.y .
 * It's nonetheless possible to use XXH3 for ephemeral data (local sessions),
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
 * - Prototype XXH128() :   XXH128() uses the same arguments as XXH64(), for consistency.
 *                          It means it maps to XXH3_128bits_withSeed().
 *                          This variant is slightly slower than XXH3_128bits(),
 *                          because the seed is now part of the algorithm, and can't be simplified.
 *                          Is that a good idea ?
 *
 * - Seed type for XXH128() : currently, it's a single 64-bit value, like the 64-bit variant.
 *                          It could be argued that it's more logical to offer a 128-bit seed input parameter for a 128-bit hash.
 *                          But 128-bit seed is more difficult to use, since it requires to pass a structure instead of a scalar value.
 *                          Such a variant could either replace current one, or become an additional one.
 *                          Farmhash, for example, offers both variants (the 128-bits seed variant is called `doubleSeed`).
 *                          Follow up question : if both 64-bit and 128-bit seeds are allowed, which variant should be called XXH128 ?
 *
 * - Result for len==0 :    Currently, the result of hashing a zero-length input is always `0`.
 *                          It seems okay as a return value when using "default" secret and seed.
 *                          But is it still fine to return `0` when secret or seed are non-default ?
 *                          Are there use cases which could depend on generating a different hash result for zero-length input when the secret is different ?
 *
 * - Consistency (1) :      Streaming XXH128 uses an XXH3 state, which is the same state as XXH3_64bits().
 *                          It means a 128bit streaming loop must invoke the following symbols :
 *                          XXH3_createState(), XXH3_128bits_reset(), XXH3_128bits_update() (loop), XXH3_128bits_digest(), XXH3_freeState().
 *                          Is that consistent enough ?
 *
 * - Consistency (2) :      The canonical representation of `XXH3_64bits` is provided by existing functions
 *                          XXH64_canonicalFromHash(), and reverse operation XXH64_hashFromCanonical().
 *                          As a mirror, canonical functions for XXH128_hash_t results generated by `XXH3_128bits`
 *                          are XXH128_canonicalFromHash() and XXH128_hashFromCanonical().
 *                          Which means, `XXH3` doesn't appear in the names, because canonical functions operate on a type,
 *                          independently of which algorithm was used to generate that type.
 *                          Is that consistent enough ?
 */

#ifdef XXH_NAMESPACE
#  define XXH3_64bits XXH_NAME2(XXH_NAMESPACE, XXH3_64bits)
#  define XXH3_64bits_withSecret XXH_NAME2(XXH_NAMESPACE, XXH3_64bits_withSecret)
#  define XXH3_64bits_withSeed XXH_NAME2(XXH_NAMESPACE, XXH3_64bits_withSeed)

#  define XXH3_createState XXH_NAME2(XXH_NAMESPACE, XXH3_createState)
#  define XXH3_freeState XXH_NAME2(XXH_NAMESPACE, XXH3_freeState)
#  define XXH3_copyState XXH_NAME2(XXH_NAMESPACE, XXH3_copyState)

#  define XXH3_64bits_reset XXH_NAME2(XXH_NAMESPACE, XXH3_64bits_reset)
#  define XXH3_64bits_reset_withSeed XXH_NAME2(XXH_NAMESPACE, XXH3_64bits_reset_withSeed)
#  define XXH3_64bits_reset_withSecret XXH_NAME2(XXH_NAMESPACE, XXH3_64bits_reset_withSecret)
#  define XXH3_64bits_update XXH_NAME2(XXH_NAMESPACE, XXH3_64bits_update)
#  define XXH3_64bits_digest XXH_NAME2(XXH_NAMESPACE, XXH3_64bits_digest)
#endif

/* XXH3_64bits() :
 * default 64-bit variant, using default secret and default seed of 0.
 * It's the fastest variant. */
XXH_PUBLIC_API XXH64_hash_t XXH3_64bits(const void* data, size_t len);

/* XXH3_64bits_withSecret() :
 * It's possible to provide any blob of bytes as a "secret" to generate the hash.
 * This makes it more difficult for an external actor to prepare an intentional collision.
 * The secret *must* be large enough (>= XXH3_SECRET_SIZE_MIN).
 * It should consist of random bytes.
 * Avoid repeating same character, or sequences of bytes,
 * and especially avoid swathes of \0.
 * Failure to respect these conditions will result in a poor quality hash.
 */
#define XXH3_SECRET_SIZE_MIN 136
XXH_PUBLIC_API XXH64_hash_t XXH3_64bits_withSecret(const void* data, size_t len, const void* secret, size_t secretSize);

/* XXH3_64bits_withSeed() :
 * This variant generates on the fly a custom secret,
 * based on the default secret, altered using the `seed` value.
 * While this operation is decently fast, note that it's not completely free.
 * note : seed==0 produces same results as XXH3_64bits() */
XXH_PUBLIC_API XXH64_hash_t XXH3_64bits_withSeed(const void* data, size_t len, XXH64_hash_t seed);


/* streaming 64-bit */

#if defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 201112L)   /* C11+ */
#  include <stdalign.h>
#  define XXH_ALIGN(n)      alignas(n)
#elif defined(__GNUC__)
#  define XXH_ALIGN(n)      __attribute__ ((aligned(n)))
#elif defined(_MSC_VER)
#  define XXH_ALIGN(n)      __declspec(align(n))
#else
#  define XXH_ALIGN(n)   /* disabled */
#endif

typedef struct XXH3_state_s XXH3_state_t;

#define XXH3_SECRET_DEFAULT_SIZE 192   /* minimum XXH3_SECRET_SIZE_MIN */
#define XXH3_INTERNALBUFFER_SIZE 256
struct XXH3_state_s {
   XXH_ALIGN(64) XXH64_hash_t acc[8];
   XXH_ALIGN(64) unsigned char customSecret[XXH3_SECRET_DEFAULT_SIZE];  /* used to store a custom secret generated from the seed. Makes state larger. Design might change */
   XXH_ALIGN(64) unsigned char buffer[XXH3_INTERNALBUFFER_SIZE];
   XXH32_hash_t bufferedSize;
   XXH32_hash_t nbStripesPerBlock;
   XXH32_hash_t nbStripesSoFar;
   XXH32_hash_t secretLimit;
   XXH32_hash_t reserved32;
   XXH32_hash_t reserved32_2;
   XXH64_hash_t totalLen;
   XXH64_hash_t seed;
   XXH64_hash_t reserved64;
   const unsigned char* secret;    /* note : there is some padding after, due to alignment on 64 bytes */
};   /* typedef'd to XXH3_state_t */

/* Streaming requires state maintenance.
 * This operation costs memory and cpu.
 * As a consequence, streaming is slower than one-shot hashing.
 * For better performance, prefer using one-shot functions whenever possible. */

XXH_PUBLIC_API XXH3_state_t* XXH3_createState(void);
XXH_PUBLIC_API XXH_errorcode XXH3_freeState(XXH3_state_t* statePtr);
XXH_PUBLIC_API void XXH3_copyState(XXH3_state_t* dst_state, const XXH3_state_t* src_state);


/* XXH3_64bits_reset() :
 * initialize with default parameters.
 * result will be equivalent to `XXH3_64bits()`. */
XXH_PUBLIC_API XXH_errorcode XXH3_64bits_reset(XXH3_state_t* statePtr);
/* XXH3_64bits_reset_withSeed() :
 * generate a custom secret from `seed`, and store it into state.
 * digest will be equivalent to `XXH3_64bits_withSeed()`. */
XXH_PUBLIC_API XXH_errorcode XXH3_64bits_reset_withSeed(XXH3_state_t* statePtr, XXH64_hash_t seed);
/* XXH3_64bits_reset_withSecret() :
 * `secret` is referenced, and must outlive the hash streaming session.
 * secretSize must be >= XXH3_SECRET_SIZE_MIN.
 */
XXH_PUBLIC_API XXH_errorcode XXH3_64bits_reset_withSecret(XXH3_state_t* statePtr, const void* secret, size_t secretSize);

XXH_PUBLIC_API XXH_errorcode XXH3_64bits_update (XXH3_state_t* statePtr, const void* input, size_t length);
XXH_PUBLIC_API XXH64_hash_t  XXH3_64bits_digest (const XXH3_state_t* statePtr);


/* 128-bit */

#ifdef XXH_NAMESPACE
#  define XXH128 XXH_NAME2(XXH_NAMESPACE, XXH128)
#  define XXH3_128bits XXH_NAME2(XXH_NAMESPACE, XXH3_128bits)
#  define XXH3_128bits_withSeed XXH_NAME2(XXH_NAMESPACE, XXH3_128bits_withSeed)
#  define XXH3_128bits_withSecret XXH_NAME2(XXH_NAMESPACE, XXH3_128bits_withSecret)

#  define XXH3_128bits_reset XXH_NAME2(XXH_NAMESPACE, XXH3_128bits_reset)
#  define XXH3_128bits_reset_withSeed XXH_NAME2(XXH_NAMESPACE, XXH3_128bits_reset_withSeed)
#  define XXH3_128bits_reset_withSecret XXH_NAME2(XXH_NAMESPACE, XXH3_128bits_reset_withSecret)
#  define XXH3_128bits_update XXH_NAME2(XXH_NAMESPACE, XXH3_128bits_update)
#  define XXH3_128bits_digest XXH_NAME2(XXH_NAMESPACE, XXH3_128bits_digest)

#  define XXH128_isEqual XXH_NAME2(XXH_NAMESPACE, XXH128_isEqual)
#  define XXH128_cmp     XXH_NAME2(XXH_NAMESPACE, XXH128_cmp)
#  define XXH128_canonicalFromHash XXH_NAME2(XXH_NAMESPACE, XXH128_canonicalFromHash)
#  define XXH128_hashFromCanonical XXH_NAME2(XXH_NAMESPACE, XXH128_hashFromCanonical)
#endif

typedef struct {
    XXH64_hash_t low64;
    XXH64_hash_t high64;
} XXH128_hash_t;

XXH_PUBLIC_API XXH128_hash_t XXH128(const void* data, size_t len, XXH64_hash_t seed);
XXH_PUBLIC_API XXH128_hash_t XXH3_128bits(const void* data, size_t len);
XXH_PUBLIC_API XXH128_hash_t XXH3_128bits_withSeed(const void* data, size_t len, XXH64_hash_t seed);  /* == XXH128() */
XXH_PUBLIC_API XXH128_hash_t XXH3_128bits_withSecret(const void* data, size_t len, const void* secret, size_t secretSize);

XXH_PUBLIC_API XXH_errorcode XXH3_128bits_reset(XXH3_state_t* statePtr);
XXH_PUBLIC_API XXH_errorcode XXH3_128bits_reset_withSeed(XXH3_state_t* statePtr, XXH64_hash_t seed);
XXH_PUBLIC_API XXH_errorcode XXH3_128bits_reset_withSecret(XXH3_state_t* statePtr, const void* secret, size_t secretSize);

XXH_PUBLIC_API XXH_errorcode XXH3_128bits_update (XXH3_state_t* statePtr, const void* input, size_t length);
XXH_PUBLIC_API XXH128_hash_t XXH3_128bits_digest (const XXH3_state_t* statePtr);


/* Note : for better performance, following functions can be inlined,
 * using XXH_INLINE_ALL */

/* return : 1 is equal, 0 if different */
XXH_PUBLIC_API int XXH128_isEqual(XXH128_hash_t h1, XXH128_hash_t h2);

/* This comparator is compatible with stdlib's qsort().
 * return : >0 if *h128_1  > *h128_2
 *          <0 if *h128_1  < *h128_2
 *          =0 if *h128_1 == *h128_2  */
XXH_PUBLIC_API int XXH128_cmp(const void* h128_1, const void* h128_2);


/*======   Canonical representation   ======*/
typedef struct { unsigned char digest[16]; } XXH128_canonical_t;
XXH_PUBLIC_API void XXH128_canonicalFromHash(XXH128_canonical_t* dst, XXH128_hash_t hash);
XXH_PUBLIC_API XXH128_hash_t XXH128_hashFromCanonical(const XXH128_canonical_t* src);


#endif  /* XXH_NO_LONG_LONG */


/*-**********************************************************************
*  XXH_INLINE_ALL
************************************************************************/
#if defined(XXH_INLINE_ALL) || defined(XXH_PRIVATE_API)
#  include "xxhash.c"   /* include xxhash function bodies as `static`, for inlining */
#endif



#endif /* XXH_STATIC_LINKING_ONLY */


#if defined (__cplusplus)
}
#endif

#endif /* XXHASH_H_5627135585666179 */
