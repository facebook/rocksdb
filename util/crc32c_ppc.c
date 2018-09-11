//  Copyright (c) 2017 International Business Machines Corp.
//  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#define CRC_TABLE
#define FAST_ZERO_TABLE
#include <inttypes.h>
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
static inline unsigned long polynomial_multiply(unsigned int a, unsigned int b) {
        vector unsigned int va = {a, 0, 0, 0};
        vector unsigned int vb = {b, 0, 0, 0};
        vector unsigned long vt;

        __asm__("vpmsumw %0,%1,%2" : "=v"(vt) : "v"(va), "v"(vb));

        return vt[0];
}

static unsigned int reflect32(unsigned int num) {
        /*
         * this can be improved, but for the simplicity of licensing I have
         * written down the first correct implementation that came to mind
         */
        int i;
        unsigned int result = 0;
        for (i=0; i<32; i++) {
                if (num & (1 << i))
                        result |= 1 << (31 - i);
        }
        return result;
}
                                                                                         
unsigned int barrett_reduction(unsigned long val);

static inline unsigned int gf_multiply(unsigned int a, unsigned int b) {
        return barrett_reduction(polynomial_multiply(a, b));
}

unsigned int append_zeros(unsigned int crc, unsigned long length) {
        unsigned long i = 0;

        while (length) {
                if (length & 1) {
                        crc = gf_multiply(crc, crc_zero[i]);
                }
                i++;
                length /= 2;
        }

        return crc;
}

unsigned int __crc32_vpmsum(unsigned int crc, unsigned char const *p,
                            unsigned long len);

static uint32_t crc32_vpmsum(uint32_t crc, unsigned char const *data,
                             unsigned len) {
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
uint32_t crc32c_ppc(uint32_t crc, unsigned char const *data, unsigned len) {
  if (!data) {
    /* Handle the NULL buffer case. */
#ifdef REFLECT
    crc = reflect32(crc);
#endif

    crc = append_zeros(crc, len);

#ifdef REFLECT
    crc = reflect32(crc);
#endif
  } else {
    /* Handle the valid buffer case. */
    crc = crc32_vpmsum(crc, data, (unsigned long)len);
  }
  return crc;
}

#else /* HAVE_POWER8 */

/* This symbol has to exist on non-ppc architectures (and on legacy
 * ppc systems using power7 or below) in order to compile properly
 * there, even though it won't be called.
 */
uint32_t crc32c_ppc(uint32_t crc, unsigned char const *data, unsigned len) {
  return 0;
}

#endif /* HAVE_POWER8 */
