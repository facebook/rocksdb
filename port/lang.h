// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef FALLTHROUGH_INTENDED
#if defined(__clang__)
#define FALLTHROUGH_INTENDED [[clang::fallthrough]]
#elif defined(__GNUC__) && __GNUC__ >= 7
#define FALLTHROUGH_INTENDED [[gnu::fallthrough]]
#else
#define FALLTHROUGH_INTENDED do {} while (0)
#endif
#endif // FALLTHROUGH_INTENDED

// Callback sync point for any read IO errors that should be ignored by
// the fault injection framework
#ifndef NDEBUG
#define IGNORE_STATUS_IF_ERROR(_status_)          \
{                                                 \
  if (!_status_.ok()) {                           \
    TEST_SYNC_POINT("FaultInjectionIgnoreError"); \
  }                                               \
}
#endif // NDEBUG
