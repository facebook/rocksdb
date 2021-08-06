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

#pragma once

#include <stdio.h>  // FILE

// Performance instrumentation object identifier type
typedef unsigned int pfs_key_t;

enum class toku_instr_object_type { mutex, rwlock, cond, thread, file };

struct PSI_file;

struct TOKU_FILE {
  /** The real file. */
  FILE *file;
  struct PSI_file *key;
  TOKU_FILE() : file(nullptr), key(nullptr) {}
};

struct PSI_mutex;
struct PSI_cond;
struct PSI_rwlock;

struct toku_mutex_t;
struct toku_cond_t;
struct toku_pthread_rwlock_t;

class toku_instr_key;

class toku_instr_probe_empty {
 public:
  explicit toku_instr_probe_empty(UU(const toku_instr_key &key)) {}

  void start_with_source_location(UU(const char *src_file), UU(int src_line)) {}

  void stop() {}
};

#define TOKU_PROBE_START(p) p->start_with_source_location(__FILE__, __LINE__)
#define TOKU_PROBE_STOP(p) p->stop

extern toku_instr_key toku_uninstrumented;

#ifndef MYSQL_TOKUDB_ENGINE

#include <pthread.h>

class toku_instr_key {
 public:
  toku_instr_key(UU(toku_instr_object_type type), UU(const char *group),
                 UU(const char *name)) {}

  explicit toku_instr_key(UU(pfs_key_t key_id)) {}
  // No-instrumentation constructor:
  toku_instr_key() {}
  ~toku_instr_key() {}
};

typedef toku_instr_probe_empty toku_instr_probe;

enum class toku_instr_file_op {
  file_stream_open,
  file_create,
  file_open,
  file_delete,
  file_rename,
  file_read,
  file_write,
  file_sync,
  file_stream_close,
  file_close,
  file_stat
};

struct PSI_file {};
struct PSI_mutex {};

struct toku_io_instrumentation {};

inline int toku_pthread_create(UU(const toku_instr_key &key), pthread_t *thread,
                               const pthread_attr_t *attr,
                               void *(*start_routine)(void *), void *arg) {
  return pthread_create(thread, attr, start_routine, arg);
}

inline void toku_instr_register_current_thread() {}

inline void toku_instr_delete_current_thread() {}

// Instrument file creation, opening, closing, and renaming
inline void toku_instr_file_open_begin(UU(toku_io_instrumentation &io_instr),
                                       UU(const toku_instr_key &key),
                                       UU(toku_instr_file_op op),
                                       UU(const char *name),
                                       UU(const char *src_file),
                                       UU(int src_line)) {}

inline void toku_instr_file_stream_open_end(
    UU(toku_io_instrumentation &io_instr), UU(TOKU_FILE &file)) {}

inline void toku_instr_file_open_end(UU(toku_io_instrumentation &io_instr),
                                     UU(int fd)) {}

inline void toku_instr_file_name_close_begin(
    UU(toku_io_instrumentation &io_instr), UU(const toku_instr_key &key),
    UU(toku_instr_file_op op), UU(const char *name), UU(const char *src_file),
    UU(int src_line)) {}

inline void toku_instr_file_stream_close_begin(
    UU(toku_io_instrumentation &io_instr), UU(toku_instr_file_op op),
    UU(TOKU_FILE &file), UU(const char *src_file), UU(int src_line)) {}

inline void toku_instr_file_fd_close_begin(
    UU(toku_io_instrumentation &io_instr), UU(toku_instr_file_op op),
    UU(int fd), UU(const char *src_file), UU(int src_line)) {}

inline void toku_instr_file_close_end(UU(toku_io_instrumentation &io_instr),
                                      UU(int result)) {}

inline void toku_instr_file_io_begin(UU(toku_io_instrumentation &io_instr),
                                     UU(toku_instr_file_op op), UU(int fd),
                                     UU(unsigned int count),
                                     UU(const char *src_file),
                                     UU(int src_line)) {}

inline void toku_instr_file_name_io_begin(
    UU(toku_io_instrumentation &io_instr), UU(const toku_instr_key &key),
    UU(toku_instr_file_op op), UU(const char *name), UU(unsigned int count),
    UU(const char *src_file), UU(int src_line)) {}

inline void toku_instr_file_stream_io_begin(
    UU(toku_io_instrumentation &io_instr), UU(toku_instr_file_op op),
    UU(TOKU_FILE &file), UU(unsigned int count), UU(const char *src_file),
    UU(int src_line)) {}

inline void toku_instr_file_io_end(UU(toku_io_instrumentation &io_instr),
                                   UU(unsigned int count)) {}

struct toku_mutex_t;

struct toku_mutex_instrumentation {};

inline PSI_mutex *toku_instr_mutex_init(UU(const toku_instr_key &key),
                                        UU(toku_mutex_t &mutex)) {
  return nullptr;
}

inline void toku_instr_mutex_destroy(UU(PSI_mutex *&mutex_instr)) {}

inline void toku_instr_mutex_lock_start(
    UU(toku_mutex_instrumentation &mutex_instr), UU(toku_mutex_t &mutex),
    UU(const char *src_file), UU(int src_line)) {}

inline void toku_instr_mutex_trylock_start(
    UU(toku_mutex_instrumentation &mutex_instr), UU(toku_mutex_t &mutex),
    UU(const char *src_file), UU(int src_line)) {}

inline void toku_instr_mutex_lock_end(
    UU(toku_mutex_instrumentation &mutex_instr),
    UU(int pthread_mutex_lock_result)) {}

inline void toku_instr_mutex_unlock(UU(PSI_mutex *mutex_instr)) {}

struct toku_cond_instrumentation {};

enum class toku_instr_cond_op {
  cond_wait,
  cond_timedwait,
};

inline PSI_cond *toku_instr_cond_init(UU(const toku_instr_key &key),
                                      UU(toku_cond_t &cond)) {
  return nullptr;
}

inline void toku_instr_cond_destroy(UU(PSI_cond *&cond_instr)) {}

inline void toku_instr_cond_wait_start(
    UU(toku_cond_instrumentation &cond_instr), UU(toku_instr_cond_op op),
    UU(toku_cond_t &cond), UU(toku_mutex_t &mutex), UU(const char *src_file),
    UU(int src_line)) {}

inline void toku_instr_cond_wait_end(UU(toku_cond_instrumentation &cond_instr),
                                     UU(int pthread_cond_wait_result)) {}

inline void toku_instr_cond_signal(UU(toku_cond_t &cond)) {}

inline void toku_instr_cond_broadcast(UU(toku_cond_t &cond)) {}

#if 0
// rw locks are not used 
// rwlock instrumentation
struct toku_rwlock_instrumentation {};

inline PSI_rwlock *toku_instr_rwlock_init(UU(const toku_instr_key &key),
                                          UU(toku_pthread_rwlock_t &rwlock)) {
    return nullptr;
}

inline void toku_instr_rwlock_destroy(UU(PSI_rwlock *&rwlock_instr)) {}

inline void toku_instr_rwlock_rdlock_wait_start(
    UU(toku_rwlock_instrumentation &rwlock_instr),
    UU(toku_pthread_rwlock_t &rwlock),
    UU(const char *src_file),
    UU(int src_line)) {}

inline void toku_instr_rwlock_wrlock_wait_start(
    UU(toku_rwlock_instrumentation &rwlock_instr),
    UU(toku_pthread_rwlock_t &rwlock),
    UU(const char *src_file),
    UU(int src_line)) {}

inline void toku_instr_rwlock_rdlock_wait_end(
    UU(toku_rwlock_instrumentation &rwlock_instr),
    UU(int pthread_rwlock_wait_result)) {}

inline void toku_instr_rwlock_wrlock_wait_end(
    UU(toku_rwlock_instrumentation &rwlock_instr),
    UU(int pthread_rwlock_wait_result)) {}

inline void toku_instr_rwlock_unlock(UU(toku_pthread_rwlock_t &rwlock)) {}
#endif

#else  // MYSQL_TOKUDB_ENGINE
// There can be not only mysql but also mongodb or any other PFS stuff
#include <toku_instr_mysql.h>
#endif  // MYSQL_TOKUDB_ENGINE

// Mutexes
extern toku_instr_key manager_escalation_mutex_key;
extern toku_instr_key manager_escalator_mutex_key;
extern toku_instr_key manager_mutex_key;
extern toku_instr_key treenode_mutex_key;
extern toku_instr_key locktree_request_info_mutex_key;
extern toku_instr_key locktree_request_info_retry_mutex_key;

// condition vars
extern toku_instr_key lock_request_m_wait_cond_key;
extern toku_instr_key locktree_request_info_retry_cv_key;
extern toku_instr_key manager_m_escalator_done_key;  // unused
