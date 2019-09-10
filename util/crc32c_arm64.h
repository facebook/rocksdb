//  Copyright (c) 2018, Arm Limited and affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef UTIL_CRC32C_ARM64_H
#define UTIL_CRC32C_ARM64_H

#include <cinttypes>

#if defined(__aarch64__) || defined(__AARCH64__)

#ifdef __ARM_FEATURE_CRC32
#define HAVE_ARM64_CRC
#include <arm_acle.h>
#define crc32c_u8(crc, v)  __crc32cb(crc, v)
#define crc32c_u16(crc, v) __crc32ch(crc, v)
#define crc32c_u32(crc, v) __crc32cw(crc, v)
#define crc32c_u64(crc, v) __crc32cd(crc, v)

extern uint32_t crc32c_arm64(uint32_t crc, unsigned char const *data, unsigned len);
extern uint32_t crc32c_runtime_check(void);

#ifdef __ARM_FEATURE_CRYPTO
#define HAVE_ARM64_CRYPTO
#include <arm_neon.h>
#endif  // __ARM_FEATURE_CRYPTO
#endif  // __ARM_FEATURE_CRC32

#endif  // defined(__aarch64__) || defined(__AARCH64__)

#endif
