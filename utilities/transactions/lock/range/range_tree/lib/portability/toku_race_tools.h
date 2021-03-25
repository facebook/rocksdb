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

// PORT2: #include <portability/toku_config.h>

#ifdef HAVE_valgrind
#undef USE_VALGRIND
#define USE_VALGRIND 1
#endif

#if defined(__linux__) && USE_VALGRIND

#include <valgrind/drd.h>
#include <valgrind/helgrind.h>

#define TOKU_ANNOTATE_NEW_MEMORY(p, size) ANNOTATE_NEW_MEMORY(p, size)
#define TOKU_VALGRIND_HG_ENABLE_CHECKING(p, size) \
  VALGRIND_HG_ENABLE_CHECKING(p, size)
#define TOKU_VALGRIND_HG_DISABLE_CHECKING(p, size) \
  VALGRIND_HG_DISABLE_CHECKING(p, size)
#define TOKU_DRD_IGNORE_VAR(v) DRD_IGNORE_VAR(v)
#define TOKU_DRD_STOP_IGNORING_VAR(v) DRD_STOP_IGNORING_VAR(v)
#define TOKU_ANNOTATE_IGNORE_READS_BEGIN() ANNOTATE_IGNORE_READS_BEGIN()
#define TOKU_ANNOTATE_IGNORE_READS_END() ANNOTATE_IGNORE_READS_END()
#define TOKU_ANNOTATE_IGNORE_WRITES_BEGIN() ANNOTATE_IGNORE_WRITES_BEGIN()
#define TOKU_ANNOTATE_IGNORE_WRITES_END() ANNOTATE_IGNORE_WRITES_END()

/*
 * How to make helgrind happy about tree rotations and new mutex orderings:
 *
 * // Tell helgrind that we unlocked it so that the next call doesn't get a
 * "destroyed a locked mutex" error.
 * // Tell helgrind that we destroyed the mutex.
 * VALGRIND_HG_MUTEX_UNLOCK_PRE(&locka);
 * VALGRIND_HG_MUTEX_DESTROY_PRE(&locka);
 *
 * // And recreate it.  It would be better to simply be able to say that the
 * order on these two can now be reversed, because this code forgets all the
 * ordering information for this mutex.
 * // Then tell helgrind that we have locked it again.
 * VALGRIND_HG_MUTEX_INIT_POST(&locka, 0);
 * VALGRIND_HG_MUTEX_LOCK_POST(&locka);
 *
 * When the ordering of two locks changes, we don't need tell Helgrind about do
 * both locks.  Just one is good enough.
 */

#define TOKU_VALGRIND_RESET_MUTEX_ORDERING_INFO(mutex) \
  VALGRIND_HG_MUTEX_UNLOCK_PRE(mutex);                 \
  VALGRIND_HG_MUTEX_DESTROY_PRE(mutex);                \
  VALGRIND_HG_MUTEX_INIT_POST(mutex, 0);               \
  VALGRIND_HG_MUTEX_LOCK_POST(mutex);

#else  // !defined(__linux__) || !USE_VALGRIND

#define NVALGRIND 1
#define TOKU_ANNOTATE_NEW_MEMORY(p, size) ((void)0)
#define TOKU_VALGRIND_HG_ENABLE_CHECKING(p, size) ((void)0)
#define TOKU_VALGRIND_HG_DISABLE_CHECKING(p, size) ((void)0)
#define TOKU_DRD_IGNORE_VAR(v)
#define TOKU_DRD_STOP_IGNORING_VAR(v)
#define TOKU_ANNOTATE_IGNORE_READS_BEGIN() ((void)0)
#define TOKU_ANNOTATE_IGNORE_READS_END() ((void)0)
#define TOKU_ANNOTATE_IGNORE_WRITES_BEGIN() ((void)0)
#define TOKU_ANNOTATE_IGNORE_WRITES_END() ((void)0)
#define TOKU_VALGRIND_RESET_MUTEX_ORDERING_INFO(mutex)
#undef RUNNING_ON_VALGRIND
#define RUNNING_ON_VALGRIND (0U)
#endif

// Valgrind 3.10.1 (and previous versions).
// Problems with VALGRIND_HG_DISABLE_CHECKING and VALGRIND_HG_ENABLE_CHECKING.
// Helgrind's implementation of disable and enable checking causes false races
// to be reported.  In addition, the race report does not include ANY
// information about the code that uses the helgrind disable and enable
// functions.  Therefore, it is very difficult to figure out the cause of the
// race. DRD does implement the disable and enable functions.

// Problems with ANNOTATE_IGNORE_READS.
// Helgrind does not implement ignore reads.
// Annotate ignore reads is the way to inform DRD to ignore racy reads.

// FT code uses unsafe reads in several places.  These unsafe reads have been
// noted as valid since they use the toku_unsafe_fetch function. Unfortunately,
// this causes helgrind to report erroneous data races which makes use of
// helgrind problematic.

// Unsafely fetch and return a `T' from src, telling drd to ignore
// racey access to src for the next sizeof(*src) bytes
template <typename T>
T toku_unsafe_fetch(T *src) {
  if (0)
    TOKU_VALGRIND_HG_DISABLE_CHECKING(src,
                                      sizeof *src);  // disabled, see comment
  TOKU_ANNOTATE_IGNORE_READS_BEGIN();
  T r = *src;
  TOKU_ANNOTATE_IGNORE_READS_END();
  if (0)
    TOKU_VALGRIND_HG_ENABLE_CHECKING(src,
                                     sizeof *src);  // disabled, see comment
  return r;
}

template <typename T>
T toku_unsafe_fetch(T &src) {
  return toku_unsafe_fetch(&src);
}

// Unsafely set a `T' value into *dest from src, telling drd to ignore
// racey access to dest for the next sizeof(*dest) bytes
template <typename T>
void toku_unsafe_set(T *dest, const T src) {
  if (0)
    TOKU_VALGRIND_HG_DISABLE_CHECKING(dest,
                                      sizeof *dest);  // disabled, see comment
  TOKU_ANNOTATE_IGNORE_WRITES_BEGIN();
  *dest = src;
  TOKU_ANNOTATE_IGNORE_WRITES_END();
  if (0)
    TOKU_VALGRIND_HG_ENABLE_CHECKING(dest,
                                     sizeof *dest);  // disabled, see comment
}

template <typename T>
void toku_unsafe_set(T &dest, const T src) {
  toku_unsafe_set(&dest, src);
}
