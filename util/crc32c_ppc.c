//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  Copyright (c) 2017 International Business Machines Corp.
//  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#define CRC_TABLE
#include <stdint.h>
#include <stdlib.h>
#include <strings.h>

#include "util/crc32c_ppc_constants.h"

#define VMX_ALIGN 16
#define VMX_ALIGN_MASK (VMX_ALIGN - 1)

#ifdef REFLECT
static unsigned int crc32_align(unsigned int crc, unsigned char const *p,
                                unsigned long len) {
  while (len--) crc = crc_table[(crc ^ *p++) & 0xff] ^ (crc >> 8);
  return crc;
}
#endif

#ifdef HAVE_POWER8
unsigned int __crc32_vpmsum(unsigned int crc, unsigned char const *p,
                            unsigned long len);

static uint32_t crc32_vpmsum(uint32_t crc, unsigned char const *data,
                             size_t len) {
  unsigned int prealign;
  unsigned int tail;

#ifdef CRC_XOR
  crc ^= 0xffffffff;
#endif

  if (len < VMX_ALIGN + VMX_ALIGN_MASK) {
    crc = crc32_align(crc, data, (unsigned long)len);
    goto out;
  }

  if ((unsigned long)data & VMX_ALIGN_MASK) {
    prealign = VMX_ALIGN - ((unsigned long)data & VMX_ALIGN_MASK);
    crc = crc32_align(crc, data, prealign);
    len -= prealign;
    data += prealign;
  }

  crc = __crc32_vpmsum(crc, data, (unsigned long)len & ~VMX_ALIGN_MASK);

  tail = len & VMX_ALIGN_MASK;
  if (tail) {
    data += len & ~VMX_ALIGN_MASK;
    crc = crc32_align(crc, data, tail);
  }

out:
#ifdef CRC_XOR
  crc ^= 0xffffffff;
#endif

  return crc;
}

/* This wrapper function works around the fact that crc32_vpmsum
 * does not gracefully handle the case where the data pointer is NULL.  There
 * may be room for performance improvement here.
 */
uint32_t crc32c_ppc(uint32_t crc, unsigned char const *data, size_t len) {
  unsigned char *buf2;

  if (!data) {
    buf2 = (unsigned char *)malloc(len);
    bzero(buf2, len);
    crc = crc32_vpmsum(crc, buf2, len);
    free(buf2);
  } else {
    crc = crc32_vpmsum(crc, data, (unsigned long)len);
  }
  return crc;
}

#else /* HAVE_POWER8 */

/* This symbol has to exist on non-ppc architectures (and on legacy
 * ppc systems using power7 or below) in order to compile properly
 * there, even though it won't be called.
 */
uint32_t crc32c_ppc(uint32_t crc, unsigned char const *data, size_t len) {
  return 0;
}

#endif /* HAVE_POWER8 */
