//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

/**
 * Macro for marking functions as having public visibility.
 */
#if defined(__GNUC__)
#define FOLLY_EXPORT __attribute__((__visibility__("default")))
#else
#define FOLLY_EXPORT
#endif

#if defined(__has_feature)
#define FOLLY_HAS_FEATURE(...) __has_feature(__VA_ARGS__)
#else
#define FOLLY_HAS_FEATURE(...) 0
#endif

#if FOLLY_HAS_FEATURE(thread_sanitizer) || __SANITIZE_THREAD__
#ifndef FOLLY_SANITIZE_THREAD
#define FOLLY_SANITIZE_THREAD 1
#endif
#endif
