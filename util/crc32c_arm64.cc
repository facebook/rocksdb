//  Copyright (c) 2018, Arm Limited and affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/crc32c_arm64.h"

#if defined(__linux__) && defined(HAVE_ARM64_CRC)

#include <asm/hwcap.h>
#include <sys/auxv.h>
#ifndef HWCAP_CRC32
#define HWCAP_CRC32 (1 << 7)
#endif
uint32_t crc32c_runtime_check(void) {
  uint64_t auxv = getauxval(AT_HWCAP);
  return (auxv & HWCAP_CRC32) != 0;
}

uint32_t crc32c_arm64(uint32_t crc, unsigned char const *data,
                             unsigned len) {
  const uint8_t *buf1;
  const uint16_t *buf2;
  const uint32_t *buf4;
  const uint64_t *buf8;

  int64_t length = (int64_t)len;

  crc ^= 0xffffffff;
  buf8 = (const uint64_t *)data;
  while ((length -= sizeof(uint64_t)) >= 0) {
    crc = __crc32cd(crc, *buf8++);
  }

  /* The following is more efficient than the straight loop */
  buf4 = (const uint32_t *)buf8;
  if (length & sizeof(uint32_t)) {
    crc = __crc32cw(crc, *buf4++);
    length -= 4;
  }

  buf2 = (const uint16_t *)buf4;
  if (length & sizeof(uint16_t)) {
    crc = __crc32ch(crc, *buf2++);
    length -= 2;
  }

  buf1 = (const uint8_t *)buf2;
  if (length & sizeof(uint8_t))
    crc = __crc32cb(crc, *buf1);

  crc ^= 0xffffffff;
  return crc;
}

#endif
