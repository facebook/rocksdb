//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if defined(__clang__) && defined(__GLIBC__)
// glibc's `posix_memalign()` declaration specifies `throw()` while clang's
// declaration does not. There is a hack in clang to make its re-declaration
// compatible with glibc's if they are declared consecutively. That hack breaks
// if yet another `posix_memalign()` declaration comes between glibc's and
// clang's declarations. Include "mm_malloc.h" here ensures glibc's and clang's
// declarations both come before "jemalloc.h"'s `posix_memalign()` declaration.
//
// This problem could also be avoided if "jemalloc.h"'s `posix_memalign()`
// declaration did not specify `throw()` when built with clang.
#include <mm_malloc.h>
#endif

#ifdef ROCKSDB_JEMALLOC
#ifdef __FreeBSD__
#include <malloc_np.h>
#define JEMALLOC_USABLE_SIZE_CONST const
#else
#define JEMALLOC_MANGLE
#include <jemalloc/jemalloc.h>
#endif

#ifndef JEMALLOC_CXX_THROW
#define JEMALLOC_CXX_THROW
#endif

#if defined(OS_WIN) && defined(_MSC_VER)

// MSVC does not have weak symbol support. As long as ROCKSDB_JEMALLOC is
// defined, Jemalloc memory allocator is used.
static inline bool HasJemalloc() { return true; }

#else

// definitions for compatibility with older versions of jemalloc
#if !defined(JEMALLOC_ALLOCATOR)
#define JEMALLOC_ALLOCATOR
#endif
#if !defined(JEMALLOC_RESTRICT_RETURN)
#define JEMALLOC_RESTRICT_RETURN
#endif
#if !defined(JEMALLOC_NOTHROW)
#define JEMALLOC_NOTHROW JEMALLOC_ATTR(nothrow)
#endif
#if !defined(JEMALLOC_ALLOC_SIZE)
#ifdef JEMALLOC_HAVE_ATTR_ALLOC_SIZE
#define JEMALLOC_ALLOC_SIZE(s) JEMALLOC_ATTR(alloc_size(s))
#else
#define JEMALLOC_ALLOC_SIZE(s)
#endif
#endif

// Declare non-standard jemalloc APIs as weak symbols. We can null-check these
// symbols to detect whether jemalloc is linked with the binary.
extern "C" JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN void JEMALLOC_NOTHROW *
mallocx(size_t, int) JEMALLOC_ATTR(malloc) JEMALLOC_ALLOC_SIZE(1)
    __attribute__((__weak__));
extern "C" JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN void JEMALLOC_NOTHROW *
rallocx(void *, size_t, int) JEMALLOC_ALLOC_SIZE(2) __attribute__((__weak__));
extern "C" size_t JEMALLOC_NOTHROW xallocx(void *, size_t, size_t, int)
    __attribute__((__weak__));
extern "C" size_t JEMALLOC_NOTHROW sallocx(const void *, int)
    JEMALLOC_ATTR(pure) __attribute__((__weak__));
extern "C" void JEMALLOC_NOTHROW dallocx(void *, int) __attribute__((__weak__));
extern "C" void JEMALLOC_NOTHROW sdallocx(void *, size_t, int)
    __attribute__((__weak__));
extern "C" size_t JEMALLOC_NOTHROW nallocx(size_t, int) JEMALLOC_ATTR(pure)
    __attribute__((__weak__));
extern "C" int JEMALLOC_NOTHROW mallctl(const char *, void *, size_t *, void *,
                                        size_t) __attribute__((__weak__));
extern "C" int JEMALLOC_NOTHROW mallctlnametomib(const char *, size_t *,
                                                 size_t *)
    __attribute__((__weak__));
extern "C" int JEMALLOC_NOTHROW mallctlbymib(const size_t *, size_t, void *,
                                             size_t *, void *, size_t)
    __attribute__((__weak__));
extern "C" void JEMALLOC_NOTHROW
malloc_stats_print(void (*)(void *, const char *), void *, const char *)
    __attribute__((__weak__));
extern "C" size_t JEMALLOC_NOTHROW
malloc_usable_size(JEMALLOC_USABLE_SIZE_CONST void *) JEMALLOC_CXX_THROW
    __attribute__((__weak__));

// Check if Jemalloc is linked with the binary. Note the main program might be
// using a different memory allocator even this method return true.
// It is loosely based on folly::usingJEMalloc(), minus the check that actually
// allocate memory and see if it is through jemalloc, to handle the dlopen()
// case:
// https://github.com/facebook/folly/blob/76cf8b5841fb33137cfbf8b224f0226437c855bc/folly/memory/Malloc.h#L147
static inline bool HasJemalloc() {
  return mallocx != nullptr && rallocx != nullptr && xallocx != nullptr &&
         sallocx != nullptr && dallocx != nullptr && sdallocx != nullptr &&
         nallocx != nullptr && mallctl != nullptr &&
         mallctlnametomib != nullptr && mallctlbymib != nullptr &&
         malloc_stats_print != nullptr && malloc_usable_size != nullptr;
}

#endif

#endif  // ROCKSDB_JEMALLOC
