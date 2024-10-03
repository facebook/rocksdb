//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port/port_posix.h"

#include <assert.h>
#if defined(__i386__) || defined(__x86_64__)
#include <cpuid.h>
#endif
#include <errno.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <cstdlib>
#include "util/logging.h"

namespace rocksdb {

// We want to give users opportunity to default all the mutexes to adaptive if
// not specified otherwise. This enables a quick way to conduct various
// performance related experiements.
//
// NB! Support for adaptive mutexes is turned on by definining
// ROCKSDB_PTHREAD_ADAPTIVE_MUTEX during the compilation. If you use RocksDB
// build environment then this happens automatically; otherwise it's up to the
// consumer to define the identifier.
#ifdef ROCKSDB_DEFAULT_TO_ADAPTIVE_MUTEX
extern const bool kDefaultToAdaptiveMutex = true;
#else
extern const bool kDefaultToAdaptiveMutex = false;
#endif

namespace port {

// static int PthreadCall(const char* label, int result) {
//   if (result != 0 && result != ETIMEDOUT) {
//     fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
//     abort();
//   }
//   return result;
// }

Mutex::Mutex(bool adaptive) {
}

Mutex::~Mutex() { }

void Mutex::Lock() {
  mu_.lock();
#ifndef NDEBUG
  locked_ = true;
#endif
}

void Mutex::Unlock() {
#ifndef NDEBUG
  locked_ = false;
#endif
  mu_.unlock();
}

void Mutex::AssertHeld() {
#ifndef NDEBUG
  assert(locked_);
#endif
}

CondVar::CondVar(Mutex* mu)
    : mu_(mu) {
}

CondVar::~CondVar() {}

void CondVar::Wait() {
#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  cv_.wait(mu_->mu_);
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
}

bool CondVar::TimedWait(uint64_t abs_time_us) {
#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  auto abs_now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();
  uint64_t timeout = abs_time_us > uint64_t(abs_now_us) ? abs_time_us - abs_now_us : 0;
  int ret = cv_.wait(mu_->mu_, timeout);
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
  if (ret != 0 || timeout == 0) {
    return true;
  }
  return false;
}

void CondVar::Signal() {
  cv_.notify_one();
}

void CondVar::SignalAll() {
  cv_.notify_all();
}

RWMutex::RWMutex() {
}

RWMutex::~RWMutex() { }

void RWMutex::ReadLock() {  mu_.lock(photon::RLOCK); }

void RWMutex::WriteLock() { mu_.lock(photon::WLOCK); }

void RWMutex::ReadUnlock() { mu_.unlock(); }

void RWMutex::WriteUnlock() { mu_.unlock(); }

int PhysicalCoreID() {
#if defined(ROCKSDB_SCHED_GETCPU_PRESENT) && defined(__x86_64__) && \
    (__GNUC__ > 2 || (__GNUC__ == 2 && __GNUC_MINOR__ >= 22))
  // sched_getcpu uses VDSO getcpu() syscall since 2.22. I believe Linux offers VDSO
  // support only on x86_64. This is the fastest/preferred method if available.
  int cpuno = sched_getcpu();
  if (cpuno < 0) {
    return -1;
  }
  return cpuno;
#elif defined(__x86_64__) || defined(__i386__)
  // clang/gcc both provide cpuid.h, which defines __get_cpuid(), for x86_64 and i386.
  unsigned eax, ebx = 0, ecx, edx;
  if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
    return -1;
  }
  return ebx >> 24;
#else
  // give up, the caller can generate a random number or something.
  return -1;
#endif
}

void Crash(const std::string& srcfile, int srcline) {
  fprintf(stdout, "Crashing at %s:%d\n", srcfile.c_str(), srcline);
  fflush(stdout);
  kill(getpid(), SIGTERM);
}

int GetMaxOpenFiles() {
#if defined(RLIMIT_NOFILE)
  struct rlimit no_files_limit;
  if (getrlimit(RLIMIT_NOFILE, &no_files_limit) != 0) {
    return -1;
  }
  // protect against overflow
  if (no_files_limit.rlim_cur >= std::numeric_limits<int>::max()) {
    return std::numeric_limits<int>::max();
  }
  return static_cast<int>(no_files_limit.rlim_cur);
#endif
  return -1;
}

void *cacheline_aligned_alloc(size_t size) {
#if __GNUC__ < 5 && defined(__SANITIZE_ADDRESS__)
  return malloc(size);
#elif ( _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600 || defined(__APPLE__))
  void *m;
  errno = posix_memalign(&m, CACHE_LINE_SIZE, size);
  return errno ? nullptr : m;
#else
  return malloc(size);
#endif
}

void cacheline_aligned_free(void *memblock) {
  free(memblock);
}


}  // namespace port
}  // namespace rocksdb
