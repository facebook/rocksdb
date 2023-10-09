/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ident "$Id$"
/*======
This file is part of PerconaFT.


Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved.

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License, version 2,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License, version 3,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
======= */

#ident \
    "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#pragma once

#include <pthread.h>
#include <stdint.h>
#include <time.h>

#include "toku_portability.h"
// PORT2: #include "toku_assert.h"

// TODO: some things moved toku_instrumentation.h, not necessarily the best
// place
typedef pthread_attr_t toku_pthread_attr_t;
typedef pthread_t toku_pthread_t;
typedef pthread_mutex_t toku_pthread_mutex_t;
typedef pthread_condattr_t toku_pthread_condattr_t;
typedef pthread_cond_t toku_pthread_cond_t;
typedef pthread_rwlockattr_t toku_pthread_rwlockattr_t;
typedef pthread_key_t toku_pthread_key_t;
typedef struct timespec toku_timespec_t;

// TODO: break this include loop
#include <pthread.h>
typedef pthread_mutexattr_t toku_pthread_mutexattr_t;

struct toku_mutex_t {
  pthread_mutex_t pmutex;
  struct PSI_mutex *psi_mutex; /* The performance schema instrumentation hook */
#if defined(TOKU_PTHREAD_DEBUG)
  pthread_t owner;  // = pthread_self(); // for debugging
  bool locked;
  bool valid;
  pfs_key_t instr_key_id;
#endif  // defined(TOKU_PTHREAD_DEBUG)
};

struct toku_cond_t {
  pthread_cond_t pcond;
  struct PSI_cond *psi_cond;
#if defined(TOKU_PTHREAD_DEBUG)
  pfs_key_t instr_key_id;
#endif  // defined(TOKU_PTHREAD_DEBUG)
};

#if defined(TOKU_PTHREAD_DEBUG)
#define TOKU_COND_INITIALIZER \
  { .pcond = PTHREAD_COND_INITIALIZER, .psi_cond = nullptr, .instr_key_id = 0 }
#else
#define TOKU_COND_INITIALIZER \
  { .pcond = PTHREAD_COND_INITIALIZER, .psi_cond = nullptr }
#endif  // defined(TOKU_PTHREAD_DEBUG)

struct toku_pthread_rwlock_t {
  pthread_rwlock_t rwlock;
  struct PSI_rwlock *psi_rwlock;
#if defined(TOKU_PTHREAD_DEBUG)
  pfs_key_t instr_key_id;
#endif  // defined(TOKU_PTHREAD_DEBUG)
};

typedef struct toku_mutex_aligned {
  toku_mutex_t aligned_mutex __attribute__((__aligned__(64)));
} toku_mutex_aligned_t;

// Initializing with {} will fill in a struct with all zeros.
// But you may also need a pragma to suppress the warnings, as follows
//
//   #pragma GCC diagnostic push
//   #pragma GCC diagnostic ignored "-Wmissing-field-initializers"
//   toku_mutex_t foo = ZERO_MUTEX_INITIALIZER;
//   #pragma GCC diagnostic pop
//
// In general it will be a lot of busy work to make this codebase compile
// cleanly with -Wmissing-field-initializers

#define ZERO_MUTEX_INITIALIZER \
  {}

#if defined(TOKU_PTHREAD_DEBUG)
#define TOKU_MUTEX_INITIALIZER                                             \
  {                                                                        \
    .pmutex = PTHREAD_MUTEX_INITIALIZER, .psi_mutex = nullptr, .owner = 0, \
    .locked = false, .valid = true, .instr_key_id = 0                      \
  }
#else
#define TOKU_MUTEX_INITIALIZER \
  { .pmutex = PTHREAD_MUTEX_INITIALIZER, .psi_mutex = nullptr }
#endif  // defined(TOKU_PTHREAD_DEBUG)

// Darwin doesn't provide adaptive mutexes
#if defined(__APPLE__)
#define TOKU_MUTEX_ADAPTIVE PTHREAD_MUTEX_DEFAULT
#if defined(TOKU_PTHREAD_DEBUG)
#define TOKU_ADAPTIVE_MUTEX_INITIALIZER                                    \
  {                                                                        \
    .pmutex = PTHREAD_MUTEX_INITIALIZER, .psi_mutex = nullptr, .owner = 0, \
    .locked = false, .valid = true, .instr_key_id = 0                      \
  }
#else
#define TOKU_ADAPTIVE_MUTEX_INITIALIZER \
  { .pmutex = PTHREAD_MUTEX_INITIALIZER, .psi_mutex = nullptr }
#endif  // defined(TOKU_PTHREAD_DEBUG)
#else   // __FreeBSD__, __linux__, at least
#if defined(__GLIBC__)
#define TOKU_MUTEX_ADAPTIVE PTHREAD_MUTEX_ADAPTIVE_NP
#else
// not all libc (e.g. musl) implement NP (Non-POSIX) attributes
#define TOKU_MUTEX_ADAPTIVE PTHREAD_MUTEX_DEFAULT
#endif
#if defined(TOKU_PTHREAD_DEBUG)
#define TOKU_ADAPTIVE_MUTEX_INITIALIZER                                    \
  {                                                                        \
    .pmutex = PTHREAD_ADAPTIVE_MUTEX_INITIALIZER_NP, .psi_mutex = nullptr, \
    .owner = 0, .locked = false, .valid = true, .instr_key_id = 0          \
  }
#else
#define TOKU_ADAPTIVE_MUTEX_INITIALIZER \
  { .pmutex = PTHREAD_ADAPTIVE_MUTEX_INITIALIZER_NP, .psi_mutex = nullptr }
#endif  // defined(TOKU_PTHREAD_DEBUG)
#endif  // defined(__APPLE__)

// Different OSes implement mutexes as different amounts of nested structs.
// C++ will fill out all missing values with zeroes if you provide at least one
// zero, but it needs the right amount of nesting.
#if defined(__FreeBSD__)
#define ZERO_COND_INITIALIZER \
  { 0 }
#elif defined(__APPLE__)
#define ZERO_COND_INITIALIZER \
  {                           \
    { 0 }                     \
  }
#else  // __linux__, at least
#define ZERO_COND_INITIALIZER \
  {}
#endif

static inline void toku_mutexattr_init(toku_pthread_mutexattr_t *attr) {
  int r = pthread_mutexattr_init(attr);
  assert_zero(r);
}

static inline void toku_mutexattr_settype(toku_pthread_mutexattr_t *attr,
                                          int type) {
  int r = pthread_mutexattr_settype(attr, type);
  assert_zero(r);
}

static inline void toku_mutexattr_destroy(toku_pthread_mutexattr_t *attr) {
  int r = pthread_mutexattr_destroy(attr);
  assert_zero(r);
}

#if defined(TOKU_PTHREAD_DEBUG)
static inline void toku_mutex_assert_locked(const toku_mutex_t *mutex) {
  invariant(mutex->locked);
  invariant(mutex->owner == pthread_self());
}
#else
static inline void toku_mutex_assert_locked(const toku_mutex_t *mutex
                                            __attribute__((unused))) {}
#endif  // defined(TOKU_PTHREAD_DEBUG)

// asserting that a mutex is unlocked only makes sense
// if the calling thread can guaruntee that no other threads
// are trying to lock this mutex at the time of the assertion
//
// a good example of this is a tree with mutexes on each node.
// when a node is locked the caller knows that no other threads
// can be trying to lock its childrens' mutexes. the children
// are in one of two fixed states: locked or unlocked.
#if defined(TOKU_PTHREAD_DEBUG)
static inline void toku_mutex_assert_unlocked(toku_mutex_t *mutex) {
  invariant(mutex->owner == 0);
  invariant(!mutex->locked);
}
#else
static inline void toku_mutex_assert_unlocked(toku_mutex_t *mutex
                                              __attribute__((unused))) {}
#endif  // defined(TOKU_PTHREAD_DEBUG)

#define toku_mutex_lock(M) \
  toku_mutex_lock_with_source_location(M, __FILE__, __LINE__)

static inline void toku_cond_init(toku_cond_t *cond,
                                  const toku_pthread_condattr_t *attr) {
  int r = pthread_cond_init(&cond->pcond, attr);
  assert_zero(r);
}

#define toku_mutex_trylock(M) \
  toku_mutex_trylock_with_source_location(M, __FILE__, __LINE__)

inline void toku_mutex_unlock(toku_mutex_t *mutex) {
#if defined(TOKU_PTHREAD_DEBUG)
  invariant(mutex->owner == pthread_self());
  invariant(mutex->valid);
  invariant(mutex->locked);
  mutex->locked = false;
  mutex->owner = 0;
#endif  // defined(TOKU_PTHREAD_DEBUG)
  toku_instr_mutex_unlock(mutex->psi_mutex);
  int r = pthread_mutex_unlock(&mutex->pmutex);
  assert_zero(r);
}

inline void toku_mutex_lock_with_source_location(toku_mutex_t *mutex,
                                                 const char *src_file,
                                                 int src_line) {
  toku_mutex_instrumentation mutex_instr;
  toku_instr_mutex_lock_start(mutex_instr, *mutex, src_file, src_line);

  const int r = pthread_mutex_lock(&mutex->pmutex);
  toku_instr_mutex_lock_end(mutex_instr, r);

  assert_zero(r);
#if defined(TOKU_PTHREAD_DEBUG)
  invariant(mutex->valid);
  invariant(!mutex->locked);
  invariant(mutex->owner == 0);
  mutex->locked = true;
  mutex->owner = pthread_self();
#endif  // defined(TOKU_PTHREAD_DEBUG)
}

inline int toku_mutex_trylock_with_source_location(toku_mutex_t *mutex,
                                                   const char *src_file,
                                                   int src_line) {
  toku_mutex_instrumentation mutex_instr;
  toku_instr_mutex_trylock_start(mutex_instr, *mutex, src_file, src_line);

  const int r = pthread_mutex_lock(&mutex->pmutex);
  toku_instr_mutex_lock_end(mutex_instr, r);

#if defined(TOKU_PTHREAD_DEBUG)
  if (r == 0) {
    invariant(mutex->valid);
    invariant(!mutex->locked);
    invariant(mutex->owner == 0);
    mutex->locked = true;
    mutex->owner = pthread_self();
  }
#endif  // defined(TOKU_PTHREAD_DEBUG)
  return r;
}

#define toku_cond_wait(C, M) \
  toku_cond_wait_with_source_location(C, M, __FILE__, __LINE__)

#define toku_cond_timedwait(C, M, W) \
  toku_cond_timedwait_with_source_location(C, M, W, __FILE__, __LINE__)

inline void toku_cond_init(const toku_instr_key &key, toku_cond_t *cond,
                           const pthread_condattr_t *attr) {
  toku_instr_cond_init(key, *cond);
  int r = pthread_cond_init(&cond->pcond, attr);
  assert_zero(r);
}

inline void toku_cond_destroy(toku_cond_t *cond) {
  toku_instr_cond_destroy(cond->psi_cond);
  int r = pthread_cond_destroy(&cond->pcond);
  assert_zero(r);
}

inline void toku_cond_wait_with_source_location(toku_cond_t *cond,
                                                toku_mutex_t *mutex,
                                                const char *src_file,
                                                int src_line) {
#if defined(TOKU_PTHREAD_DEBUG)
  invariant(mutex->locked);
  mutex->locked = false;
  mutex->owner = 0;
#endif  // defined(TOKU_PTHREAD_DEBUG)

  /* Instrumentation start */
  toku_cond_instrumentation cond_instr;
  toku_instr_cond_wait_start(cond_instr, toku_instr_cond_op::cond_wait, *cond,
                             *mutex, src_file, src_line);

  /* Instrumented code */
  const int r = pthread_cond_wait(&cond->pcond, &mutex->pmutex);

  /* Instrumentation end */
  toku_instr_cond_wait_end(cond_instr, r);

  assert_zero(r);
#if defined(TOKU_PTHREAD_DEBUG)
  invariant(!mutex->locked);
  mutex->locked = true;
  mutex->owner = pthread_self();
#endif  // defined(TOKU_PTHREAD_DEBUG)
}

inline int toku_cond_timedwait_with_source_location(toku_cond_t *cond,
                                                    toku_mutex_t *mutex,
                                                    toku_timespec_t *wakeup_at,
                                                    const char *src_file,
                                                    int src_line) {
#if defined(TOKU_PTHREAD_DEBUG)
  invariant(mutex->locked);
  mutex->locked = false;
  mutex->owner = 0;
#endif  // defined(TOKU_PTHREAD_DEBUG)

  /* Instrumentation start */
  toku_cond_instrumentation cond_instr;
  toku_instr_cond_wait_start(cond_instr, toku_instr_cond_op::cond_timedwait,
                             *cond, *mutex, src_file, src_line);

  /* Instrumented code */
  const int r = pthread_cond_timedwait(&cond->pcond, &mutex->pmutex, wakeup_at);

  /* Instrumentation end */
  toku_instr_cond_wait_end(cond_instr, r);

#if defined(TOKU_PTHREAD_DEBUG)
  invariant(!mutex->locked);
  mutex->locked = true;
  mutex->owner = pthread_self();
#endif  // defined(TOKU_PTHREAD_DEBUG)
  return r;
}

inline void toku_cond_signal(toku_cond_t *cond) {
  toku_instr_cond_signal(*cond);
  const int r = pthread_cond_signal(&cond->pcond);
  assert_zero(r);
}

inline void toku_cond_broadcast(toku_cond_t *cond) {
  toku_instr_cond_broadcast(*cond);
  const int r = pthread_cond_broadcast(&cond->pcond);
  assert_zero(r);
}

inline void toku_mutex_init(const toku_instr_key &key, toku_mutex_t *mutex,
                            const toku_pthread_mutexattr_t *attr) {
#if defined(TOKU_PTHREAD_DEBUG)
  mutex->valid = true;
#endif  // defined(TOKU_PTHREAD_DEBUG)
  toku_instr_mutex_init(key, *mutex);
  const int r = pthread_mutex_init(&mutex->pmutex, attr);
  assert_zero(r);
#if defined(TOKU_PTHREAD_DEBUG)
  mutex->locked = false;
  invariant(mutex->valid);
  mutex->valid = true;
  mutex->owner = 0;
#endif  // defined(TOKU_PTHREAD_DEBUG)
}

inline void toku_mutex_destroy(toku_mutex_t *mutex) {
#if defined(TOKU_PTHREAD_DEBUG)
  invariant(mutex->valid);
  mutex->valid = false;
  invariant(!mutex->locked);
#endif  // defined(TOKU_PTHREAD_DEBUG)
  toku_instr_mutex_destroy(mutex->psi_mutex);
  int r = pthread_mutex_destroy(&mutex->pmutex);
  assert_zero(r);
}

#define toku_pthread_rwlock_rdlock(RW) \
  toku_pthread_rwlock_rdlock_with_source_location(RW, __FILE__, __LINE__)

#define toku_pthread_rwlock_wrlock(RW) \
  toku_pthread_rwlock_wrlock_with_source_location(RW, __FILE__, __LINE__)

#if 0
inline void toku_pthread_rwlock_init(
    const toku_instr_key &key,
    toku_pthread_rwlock_t *__restrict rwlock,
    const toku_pthread_rwlockattr_t *__restrict attr) {
    toku_instr_rwlock_init(key, *rwlock);
    int r = pthread_rwlock_init(&rwlock->rwlock, attr);
    assert_zero(r);
}

inline void toku_pthread_rwlock_destroy(toku_pthread_rwlock_t *rwlock) {
    toku_instr_rwlock_destroy(rwlock->psi_rwlock);
    int r = pthread_rwlock_destroy(&rwlock->rwlock);
    assert_zero(r);
}

inline void toku_pthread_rwlock_rdlock_with_source_location(
    toku_pthread_rwlock_t *rwlock,
    const char *src_file,
    uint src_line) {

    /* Instrumentation start */
    toku_rwlock_instrumentation rwlock_instr;
    toku_instr_rwlock_rdlock_wait_start(
        rwlock_instr, *rwlock, src_file, src_line);
    /* Instrumented code */
    const int r = pthread_rwlock_rdlock(&rwlock->rwlock);

    /* Instrumentation end */
    toku_instr_rwlock_rdlock_wait_end(rwlock_instr, r);

    assert_zero(r);
}

inline void toku_pthread_rwlock_wrlock_with_source_location(
    toku_pthread_rwlock_t *rwlock,
    const char *src_file,
    uint src_line) {

    /* Instrumentation start */
    toku_rwlock_instrumentation rwlock_instr;
    toku_instr_rwlock_wrlock_wait_start(
        rwlock_instr, *rwlock, src_file, src_line);
    /* Instrumented code */
    const int r = pthread_rwlock_wrlock(&rwlock->rwlock);

    /* Instrumentation end */
    toku_instr_rwlock_wrlock_wait_end(rwlock_instr, r);

    assert_zero(r);
}

inline void toku_pthread_rwlock_rdunlock(toku_pthread_rwlock_t *rwlock) {
    toku_instr_rwlock_unlock(*rwlock);
    const int r = pthread_rwlock_unlock(&rwlock->rwlock);
    assert_zero(r);
}

inline void toku_pthread_rwlock_wrunlock(toku_pthread_rwlock_t *rwlock) {
    toku_instr_rwlock_unlock(*rwlock);
    const int r = pthread_rwlock_unlock(&rwlock->rwlock);
    assert_zero(r);
}
#endif

static inline int toku_pthread_join(toku_pthread_t thread, void **value_ptr) {
  return pthread_join(thread, value_ptr);
}

static inline int toku_pthread_detach(toku_pthread_t thread) {
  return pthread_detach(thread);
}

static inline int toku_pthread_key_create(toku_pthread_key_t *key,
                                          void (*destroyf)(void *)) {
  return pthread_key_create(key, destroyf);
}

static inline int toku_pthread_key_delete(toku_pthread_key_t key) {
  return pthread_key_delete(key);
}

static inline void *toku_pthread_getspecific(toku_pthread_key_t key) {
  return pthread_getspecific(key);
}

static inline int toku_pthread_setspecific(toku_pthread_key_t key, void *data) {
  return pthread_setspecific(key, data);
}

int toku_pthread_yield(void) __attribute__((__visibility__("default")));

static inline toku_pthread_t toku_pthread_self(void) { return pthread_self(); }

static inline void *toku_pthread_done(void *exit_value) {
  toku_instr_delete_current_thread();
  pthread_exit(exit_value);
  return nullptr;  // Avoid compiler warning
}
