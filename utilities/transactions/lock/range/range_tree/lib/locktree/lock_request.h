/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=2:softtabstop=2:
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

#include "../db.h"
#include "../ft/comparator.h"
#include "../portability/toku_pthread.h"
#include "locktree.h"
#include "txnid_set.h"
#include "wfg.h"

namespace toku {

// Information about a lock wait
struct lock_wait_info {
  locktree *ltree;  // the tree where wait happens
  TXNID waiter;     // the waiting transaction
  void *m_extra;    // lock_request's m_extra

  // The transactions that are waited for.
  std::vector<TXNID> waitees;
};

typedef std::vector<lock_wait_info> lock_wait_infos;

// A lock request contains the db, the key range, the lock type, and
// the transaction id that describes a potential row range lock.
//
// the typical use case is:
// - initialize a lock request
// - start to try to acquire the lock
// - do something else
// - wait for the lock request to be resolved on a timed condition
// - destroy the lock request
// a lock request is resolved when its state is no longer pending, or
// when it becomes granted, or timedout, or deadlocked. when resolved, the
// state of the lock request is changed and any waiting threads are awakened.

class lock_request {
 public:
  enum type { UNKNOWN, READ, WRITE };

  // effect: Initializes a lock request.
  void create(toku_external_mutex_factory_t mutex_factory);

  // effect: Destroys a lock request.
  void destroy(void);

  // effect: Resets the lock request parameters, allowing it to be reused.
  // requires: Lock request was already created at some point
  void set(locktree *lt, TXNID txnid, const DBT *left_key, const DBT *right_key,
           type lock_type, bool big_txn, void *extra = nullptr);

  // effect: Tries to acquire a lock described by this lock request.
  // returns: The return code of locktree::acquire_[write,read]_lock()
  //          or DB_LOCK_DEADLOCK if this request would end up deadlocked.
  int start(void);

  // effect: Sleeps until either the request is granted or the wait time
  // expires. returns: The return code of locktree::acquire_[write,read]_lock()
  //          or simply DB_LOCK_NOTGRANTED if the wait time expired.
  int wait(uint64_t wait_time_ms);
  int wait(uint64_t wait_time_ms, uint64_t killed_time_ms,
           int (*killed_callback)(void),
           void (*lock_wait_callback)(void *, lock_wait_infos *) = nullptr,
           void *callback_arg = nullptr);

  // return: left end-point of the lock range
  const DBT *get_left_key(void) const;

  // return: right end-point of the lock range
  const DBT *get_right_key(void) const;

  // return: the txnid waiting for a lock
  TXNID get_txnid(void) const;

  // return: when this lock request started, as milliseconds from epoch
  uint64_t get_start_time(void) const;

  // return: which txnid is blocking this request (there may be more, though)
  TXNID get_conflicting_txnid(void) const;

  // effect: Retries all of the lock requests for the given locktree.
  //         Any lock requests successfully restarted is completed and woken
  //         up.
  //         The rest remain pending.
  static void retry_all_lock_requests(
      locktree *lt,
      void (*lock_wait_callback)(void *, lock_wait_infos *) = nullptr,
      void *callback_arg = nullptr,
      void (*after_retry_test_callback)(void) = nullptr);
  static void retry_all_lock_requests_info(
      lt_lock_request_info *info,
      void (*lock_wait_callback)(void *, lock_wait_infos *),
      void *callback_arg);

  void set_start_test_callback(void (*f)(void));
  void set_start_before_pending_test_callback(void (*f)(void));
  void set_retry_test_callback(void (*f)(void));

  void *get_extra(void) const;

  void kill_waiter(void);
  static void kill_waiter(locktree *lt, void *extra);

 private:
  enum state {
    UNINITIALIZED,
    INITIALIZED,
    PENDING,
    COMPLETE,
    DESTROYED,
  };

  // The keys for a lock request are stored "unowned" in m_left_key
  // and m_right_key. When the request is about to go to sleep, it
  // copies these keys and stores them in m_left_key_copy etc and
  // sets the temporary pointers to null.
  TXNID m_txnid;
  TXNID m_conflicting_txnid;
  uint64_t m_start_time;
  const DBT *m_left_key;
  const DBT *m_right_key;
  DBT m_left_key_copy;
  DBT m_right_key_copy;

  // The lock request type and associated locktree
  type m_type;
  locktree *m_lt;

  // If the lock request is in the completed state, then its
  // final return value is stored in m_complete_r
  int m_complete_r;
  state m_state;

  toku_external_cond_t m_wait_cond;

  bool m_big_txn;

  // the lock request info state stored in the
  // locktree that this lock request is for.
  struct lt_lock_request_info *m_info;

  void *m_extra;

  // effect: tries again to acquire the lock described by this lock request
  // returns: 0 if retrying the request succeeded and is now complete
  int retry(lock_wait_infos *collector);

  void complete(int complete_r);

  // effect: Finds another lock request by txnid.
  // requires: The lock request info mutex is held
  lock_request *find_lock_request(const TXNID &txnid);

  // effect: Insert this lock request into the locktree's set.
  // requires: the locktree's mutex is held
  void insert_into_lock_requests(void);

  // effect: Removes this lock request from the locktree's set.
  // requires: The lock request info mutex is held
  void remove_from_lock_requests(void);

  // effect: Asks this request's locktree which txnids are preventing
  //         us from getting the lock described by this request.
  // returns: conflicts is populated with the txnid's that this request
  //          is blocked on
  void get_conflicts(txnid_set *conflicts);

  // effect: Builds a wait-for-graph for this lock request and the given
  // conflict set
  void build_wait_graph(wfg *wait_graph, const txnid_set &conflicts);

  // returns: True if this lock request is in deadlock with the given conflicts
  // set
  bool deadlock_exists(const txnid_set &conflicts);

  void copy_keys(void);

  static int find_by_txnid(lock_request *const &request, const TXNID &txnid);

  // Report list of conflicts to lock wait callback.
  static void report_waits(lock_wait_infos *wait_conflicts,
                           void (*lock_wait_callback)(void *,
                                                      lock_wait_infos *),
                           void *callback_arg);
  void add_conflicts_to_waits(txnid_set *conflicts,
                              lock_wait_infos *wait_conflicts);

  void (*m_start_test_callback)(void);
  void (*m_start_before_pending_test_callback)(void);
  void (*m_retry_test_callback)(void);

 public:
  std::function<void(TXNID, bool, const DBT *, const DBT *)> m_deadlock_cb;

  friend class lock_request_unit_test;
};
// PORT: lock_request is not a POD anymore due to use of toku_external_cond_t
//  This is ok as the PODness is not really required: lock_request objects are
//  not moved in memory or anything.
// ENSURE_POD(lock_request);

} /* namespace toku */
