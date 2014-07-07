// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#if defined(ROCKSDB_PLATFORM_WIN)

#include <stdint.h>

#define __attribute__(x)
#define __thread __declspec(thread)

extern int snprintf(char *str, size_t size, const char *format, ...);

#endif
