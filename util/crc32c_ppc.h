//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  Copyright (c) 2017 International Business Machines Corp.
//  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

extern uint32_t crc32c_ppc(uint32_t crc, unsigned char const *buffer,
                           size_t len);

#ifdef __cplusplus
}
#endif
