//  Copyright (c) 2018, Arm Limited and affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef UTIL_CRC32C_ARM64_H
#define UTIL_CRC32C_ARM64_H

#include <inttypes.h>

#if defined(__aarch64__) || defined(__AARCH64__)
#ifdef __ARM_FEATURE_CRC32
#define HAVE_ARM64_CRC
#include <arm_acle.h>
extern uint32_t crc32c_arm64(uint32_t crc, unsigned char const *data, unsigned len);
extern uint32_t crc32c_runtime_check(void);
#endif
#endif


#endif
