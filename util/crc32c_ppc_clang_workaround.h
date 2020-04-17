//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  Copyright (C) 2015, 2017 International Business Machines Corp.
//  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef CLANG_WORKAROUND_H
#define CLANG_WORKAROUND_H

/*
 * These stubs fix clang incompatibilities with GCC builtins.
 */

#ifndef __builtin_crypto_vpmsumw
#define __builtin_crypto_vpmsumw __builtin_crypto_vpmsumb
#endif
#ifndef __builtin_crypto_vpmsumd
#define __builtin_crypto_vpmsumd __builtin_crypto_vpmsumb
#endif

static inline
__vector unsigned long long __attribute__((overloadable))
vec_ld(int __a, const __vector unsigned long long* __b)
{
	return (__vector unsigned long long)__builtin_altivec_lvx(__a, __b);
}

/*
 * GCC __builtin_pack_vector_int128 returns a vector __int128_t but Clang
 * does not recognize this type. On GCC this builtin is translated to a
 * xxpermdi instruction that only moves the registers __a, __b instead generates
 * a load.
 *
 * Clang has vec_xxpermdi intrinsics. It was implemented in 4.0.0.
 */
static inline
__vector unsigned long long  __builtin_pack_vector (unsigned long __a,
						    unsigned long __b)
{
	#if defined(__BIG_ENDIAN__)
	__vector unsigned long long __v = {__a, __b};
	#else
	__vector unsigned long long __v = {__b, __a};
	#endif
	return __v;
}

/*
 * Clang 7 changed the behavior of vec_xxpermdi in order to provide the same
 * behavior of GCC. That means code adapted to Clang >= 7 does not work on
 * Clang <= 6.  So, fallback to __builtin_unpack_vector() on Clang <= 6.
 */
#if !defined vec_xxpermdi || __clang_major__ <= 6

static inline
unsigned long __builtin_unpack_vector (__vector unsigned long long __v,
				       int __o)
{
	return __v[__o];
}

#if defined(__BIG_ENDIAN__)
#define __builtin_unpack_vector_0(a) __builtin_unpack_vector ((a), 0)
#define __builtin_unpack_vector_1(a) __builtin_unpack_vector ((a), 1)
#else
#define __builtin_unpack_vector_0(a) __builtin_unpack_vector ((a), 1)
#define __builtin_unpack_vector_1(a) __builtin_unpack_vector ((a), 0)
#endif

#else

static inline
unsigned long __builtin_unpack_vector_0 (__vector unsigned long long __v)
{
	#if defined(__BIG_ENDIAN__)
	return vec_xxpermdi(__v, __v, 0x0)[0];
	#else
	return vec_xxpermdi(__v, __v, 0x3)[0];
	#endif
}

static inline
unsigned long __builtin_unpack_vector_1 (__vector unsigned long long __v)
{
	#if defined(__BIG_ENDIAN__)
	return vec_xxpermdi(__v, __v, 0x3)[0];
	#else
	return vec_xxpermdi(__v, __v, 0x0)[0];
	#endif
}
#endif /* vec_xxpermdi */

#endif
