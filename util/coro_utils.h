//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if defined(USE_COROUTINES)
#include "folly/experimental/coro/Coroutine.h"
#include "folly/experimental/coro/Task.h"
#endif
#include "rocksdb/rocksdb_namespace.h"

// This file has two sctions. The first section applies to all instances of
// header file inclusion and has an include guard. The second section is
// meant for multiple inclusions in the same source file, and is idempotent.
namespace ROCKSDB_NAMESPACE {

#ifndef UTIL_CORO_UTILS_H_
#define UTIL_CORO_UTILS_H_

#if defined(USE_COROUTINES)

// The follwoing macros expand to regular and coroutine function
// declarations for a given function
#define DECLARE_SYNC_AND_ASYNC(__ret_type__, __func_name__, ...) \
  __ret_type__ __func_name__(__VA_ARGS__);                       \
  folly::coro::Task<__ret_type__> __func_name__##Coroutine(__VA_ARGS__);

#define DECLARE_SYNC_AND_ASYNC_OVERRIDE(__ret_type__, __func_name__, ...) \
  __ret_type__ __func_name__(__VA_ARGS__) override;                       \
  folly::coro::Task<__ret_type__> __func_name__##Coroutine(__VA_ARGS__)   \
      override;

#define DECLARE_SYNC_AND_ASYNC_CONST(__ret_type__, __func_name__, ...) \
  __ret_type__ __func_name__(__VA_ARGS__) const;                       \
  folly::coro::Task<__ret_type__> __func_name__##Coroutine(__VA_ARGS__) const;

constexpr bool using_coroutines() { return true; }
#else  // !USE_COROUTINES

// The follwoing macros expand to a regular function declaration for a given
// function
#define DECLARE_SYNC_AND_ASYNC(__ret_type__, __func_name__, ...) \
  __ret_type__ __func_name__(__VA_ARGS__);

#define DECLARE_SYNC_AND_ASYNC_OVERRIDE(__ret_type__, __func_name__, ...) \
  __ret_type__ __func_name__(__VA_ARGS__) override;

#define DECLARE_SYNC_AND_ASYNC_CONST(__ret_type__, __func_name__, ...) \
  __ret_type__ __func_name__(__VA_ARGS__) const;

constexpr bool using_coroutines() { return false; }
#endif  // USE_COROUTINES
#endif  // UTIL_CORO_UTILS_H_

// The following section of the file is meant to be included twice in a
// source file - once defining WITH_COROUTINES and once defining
// WITHOUT_COROUTINES
#undef DEFINE_SYNC_AND_ASYNC
#undef CO_AWAIT
#undef CO_RETURN

#if defined(WITH_COROUTINES) && defined(USE_COROUTINES)

// This macro should be used in the beginning of the function
// definition. The declaration should have been done using one of the
// DECLARE_SYNC_AND_ASYNC* macros. It expands to the return type and
// the function name with the Coroutine suffix. For example -
// DEFINE_SYNC_AND_ASYNC(int, foo)(bool bar) {}
// would expand to -
// folly::coro::Task<int> fooCoroutine(bool bar) {}
#define DEFINE_SYNC_AND_ASYNC(__ret_type__, __func_name__) \
  folly::coro::Task<__ret_type__> __func_name__##Coroutine

// This macro should be used to call a function that might be a
// coroutine. It expands to the correct function name and prefixes
// the co_await operator if necessary. For example -
// s = CO_AWAIT(foo)(true);
// if the code is compiled WITH_COROUTINES, would expand to
// s = co_await fooCoroutine(true);
// if compiled WITHOUT_COROUTINES, would expand to
// s = foo(true);
#define CO_AWAIT(__func_name__) co_await __func_name__##Coroutine

#define CO_RETURN co_return

#elif defined(WITHOUT_COROUTINES)

// This macro should be used in the beginning of the function
// definition. The declaration should have been done using one of the
// DECLARE_SYNC_AND_ASYNC* macros. It expands to the return type and
// the function name without the Coroutine suffix. For example -
// DEFINE_SYNC_AND_ASYNC(int, foo)(bool bar) {}
// would expand to -
// int foo(bool bar) {}
#define DEFINE_SYNC_AND_ASYNC(__ret_type__, __func_name__) \
  __ret_type__ __func_name__

// This macro should be used to call a function that might be a
// coroutine. It expands to the correct function name and prefixes
// the co_await operator if necessary. For example -
// s = CO_AWAIT(foo)(true);
// if the code is compiled WITH_COROUTINES, would expand to
// s = co_await fooCoroutine(true);
// if compiled WITHOUT_COROUTINES, would expand to
// s = foo(true);
#define CO_AWAIT(__func_name__) __func_name__

#define CO_RETURN return

#endif  // DO_NOT_USE_COROUTINES
}  // namespace ROCKSDB_NAMESPACE
