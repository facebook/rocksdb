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

#include <atomic>

#include "../db.h"
#include "../ft/comparator.h"
#include "../portability/toku_external_pthread.h"
#include "../portability/toku_pthread.h"
#include "../portability/toku_time.h"
// PORT #include <ft/ft-ops.h>  // just for DICTIONARY_ID..
// PORT: ft-status for LTM_STATUS:
#include "../ft/ft-status.h"

struct DICTIONARY_ID {
  uint64_t dictid;
};

#include "../util/omt.h"
#include "range_buffer.h"
#include "txnid_set.h"
#include "wfg.h"

namespace toku {

class locktree;
class locktree_manager;
class lock_request;
class concurrent_tree;

typedef int (*lt_create_cb)(locktree *lt, void *extra);
typedef void (*lt_destroy_cb)(locktree *lt);
typedef void (*lt_escalate_cb)(TXNID txnid, const locktree *lt,
                               const range_buffer &buffer, void *extra);

typedef bool (*lt_escalation_barrier_check_func)(const DBT *a, const DBT *b,
                                                 void *extra);

struct lt_counters {
  uint64_t wait_count, wait_time;
  uint64_t long_wait_count, long_wait_time;
  uint64_t timeout_count;

  void add(const lt_counters &rhs) {
    wait_count += rhs.wait_count;
    wait_time += rhs.wait_time;
    long_wait_count += rhs.long_wait_count;
    long_wait_time += rhs.long_wait_time;
    timeout_count += rhs.timeout_count;
  }
};

// Lock request state for some locktree
struct lt_lock_request_info {
  omt<lock_request *> pending_lock_requests;
  std::atomic_bool pending_is_empty;
  toku_external_mutex_t mutex;
  bool should_retry_lock_requests;
  lt_counters counters;
  std::atomic_ullong retry_want;
  unsigned long long retry_done;
  toku_mutex_t retry_mutex;
  toku_cond_t retry_cv;
  bool running_retry;

  void init(toku_external_mutex_factory_t mutex_factory);
  void destroy(void);
};

// The locktree manager manages a set of locktrees, one for each open
// dictionary. Locktrees are retrieved from the manager. When they are no
// longer needed, they are be released by the user.
class locktree_manager {
 public:
  // param: create_cb, called just after a locktree is first created.
  //        destroy_cb, called just before a locktree is destroyed.
  //        escalate_cb, called after a locktree is escalated (with extra
  //        param)
  void create(lt_create_cb create_cb, lt_destroy_cb destroy_cb,
              lt_escalate_cb escalate_cb, void *extra,
              toku_external_mutex_factory_t mutex_factory_arg);

  void destroy(void);

  size_t get_max_lock_memory(void);

  int set_max_lock_memory(size_t max_lock_memory);

  // effect: Get a locktree from the manager. If a locktree exists with the
  // given
  //         dict_id, it is referenced and then returned. If one did not exist,
  //         it is created. It will use the comparator for comparing keys. The
  //         on_create callback (passed to locktree_manager::create()) will be
  //         called with the given extra parameter.
  locktree *get_lt(DICTIONARY_ID dict_id, const comparator &cmp,
                   void *on_create_extra);

  void reference_lt(locktree *lt);

  // effect: Releases one reference on a locktree. If the reference count
  // transitions
  //         to zero, the on_destroy callback is called before it gets
  //         destroyed.
  void release_lt(locktree *lt);

  void get_status(LTM_STATUS status);

  // effect: calls the iterate function on each pending lock request
  // note: holds the manager's mutex
  typedef int (*lock_request_iterate_callback)(DICTIONARY_ID dict_id,
                                               TXNID txnid, const DBT *left_key,
                                               const DBT *right_key,
                                               TXNID blocking_txnid,
                                               uint64_t start_time,
                                               void *extra);
  int iterate_pending_lock_requests(lock_request_iterate_callback cb,
                                    void *extra);

  // effect: Determines if too many locks or too much memory is being used,
  //         Runs escalation on the manager if so.
  // param: big_txn, if the current transaction is 'big' (has spilled rollback
  // logs) returns: 0 if there enough resources to create a new lock, or
  // TOKUDB_OUT_OF_LOCKS
  //          if there are not enough resources and lock escalation failed to
  //          free up enough resources for a new lock.
  int check_current_lock_constraints(bool big_txn);

  bool over_big_threshold(void);

  void note_mem_used(uint64_t mem_used);

  void note_mem_released(uint64_t mem_freed);

  bool out_of_locks(void) const;

  // Escalate all locktrees
  void escalate_all_locktrees(void);

  // Escalate a set of locktrees
  void escalate_locktrees(locktree **locktrees, int num_locktrees);

  // effect: calls the private function run_escalation(), only ok to
  //         do for tests.
  // rationale: to get better stress test coverage, we want a way to
  //            deterministicly trigger lock escalation.
  void run_escalation_for_test(void);
  void run_escalation(void);

  // Add time t to the escalator's wait time statistics
  void add_escalator_wait_time(uint64_t t);

  void kill_waiter(void *extra);

 private:
  static const uint64_t DEFAULT_MAX_LOCK_MEMORY = 64L * 1024 * 1024;

  // tracks the current number of locks and lock memory
  uint64_t m_max_lock_memory;
  uint64_t m_current_lock_memory;

  struct lt_counters m_lt_counters;

  // the create and destroy callbacks for the locktrees
  lt_create_cb m_lt_create_callback;
  lt_destroy_cb m_lt_destroy_callback;
  lt_escalate_cb m_lt_escalate_callback;
  void *m_lt_escalate_callback_extra;

  omt<locktree *> m_locktree_map;

  toku_external_mutex_factory_t mutex_factory;

  // the manager's mutex protects the locktree map
  toku_mutex_t m_mutex;

  void mutex_lock(void);

  void mutex_unlock(void);

  // Manage the set of open locktrees
  locktree *locktree_map_find(const DICTIONARY_ID &dict_id);
  void locktree_map_put(locktree *lt);
  void locktree_map_remove(locktree *lt);

  static int find_by_dict_id(locktree *const &lt, const DICTIONARY_ID &dict_id);

  void escalator_init(void);
  void escalator_destroy(void);

  // statistics about lock escalation.
  toku_mutex_t m_escalation_mutex;
  uint64_t m_escalation_count;
  tokutime_t m_escalation_time;
  uint64_t m_escalation_latest_result;
  uint64_t m_wait_escalation_count;
  uint64_t m_wait_escalation_time;
  uint64_t m_long_wait_escalation_count;
  uint64_t m_long_wait_escalation_time;

  // the escalator coordinates escalation on a set of locktrees for a bunch of
  // threads
  class locktree_escalator {
   public:
    void create(void);
    void destroy(void);
    void run(locktree_manager *mgr, void (*escalate_locktrees_fun)(void *extra),
             void *extra);

   private:
    toku_mutex_t m_escalator_mutex;
    toku_cond_t m_escalator_done;
    bool m_escalator_running;
  };

  locktree_escalator m_escalator;

  friend class manager_unit_test;
};

// A locktree represents the set of row locks owned by all transactions
// over an open dictionary. Read and write ranges are represented as
// a left and right key which are compared with the given comparator
//
// Locktrees are not created and destroyed by the user. Instead, they are
// referenced and released using the locktree manager.
//
// A sample workflow looks like this:
// - Create a manager.
// - Get a locktree by dictionaroy id from the manager.
// - Perform read/write lock acquision on the locktree, add references to
//   the locktree using the manager, release locks, release references, etc.
// - ...
// - Release the final reference to the locktree. It will be destroyed.
// - Destroy the manager.
class locktree {
 public:
  // effect: Creates a locktree
  void create(locktree_manager *mgr, DICTIONARY_ID dict_id,
              const comparator &cmp,
              toku_external_mutex_factory_t mutex_factory);

  void destroy(void);

  // For thread-safe, external reference counting
  void add_reference(void);

  // requires: the reference count is > 0
  // returns: the reference count, after decrementing it by one
  uint32_t release_reference(void);

  // returns: the current reference count
  uint32_t get_reference_count(void);

  // effect: Attempts to grant a read lock for the range of keys between
  // [left_key, right_key]. returns: If the lock cannot be granted, return
  // DB_LOCK_NOTGRANTED, and populate the
  //          given conflicts set with the txnids that hold conflicting locks in
  //          the range. If the locktree cannot create more locks, return
  //          TOKUDB_OUT_OF_LOCKS.
  // note: Read locks cannot be shared between txnids, as one would expect.
  //       This is for simplicity since read locks are rare in MySQL.
  int acquire_read_lock(TXNID txnid, const DBT *left_key, const DBT *right_key,
                        txnid_set *conflicts, bool big_txn);

  // effect: Attempts to grant a write lock for the range of keys between
  // [left_key, right_key]. returns: If the lock cannot be granted, return
  // DB_LOCK_NOTGRANTED, and populate the
  //          given conflicts set with the txnids that hold conflicting locks in
  //          the range. If the locktree cannot create more locks, return
  //          TOKUDB_OUT_OF_LOCKS.
  int acquire_write_lock(TXNID txnid, const DBT *left_key, const DBT *right_key,
                         txnid_set *conflicts, bool big_txn);

  // effect: populate the conflicts set with the txnids that would preventing
  //         the given txnid from getting a lock on [left_key, right_key]
  void get_conflicts(bool is_write_request, TXNID txnid, const DBT *left_key,
                     const DBT *right_key, txnid_set *conflicts);

  // effect: Release all of the lock ranges represented by the range buffer for
  // a txnid.
  void release_locks(TXNID txnid, const range_buffer *ranges,
                     bool all_trx_locks_hint = false);

  // effect: Runs escalation on this locktree
  void escalate(lt_escalate_cb after_escalate_callback, void *extra);

  // returns: The userdata associated with this locktree, or null if it has not
  // been set.
  void *get_userdata(void) const;

  void set_userdata(void *userdata);

  locktree_manager *get_manager(void) const;

  void set_comparator(const comparator &cmp);

  // Set the user-provided Lock Escalation Barrier check function and its
  // argument
  //
  // Lock Escalation Barrier limits the scope of Lock Escalation.
  // For two keys A and B (such that A < B),
  // escalation_barrier_check_func(A, B)==true means that there's a lock
  // escalation barrier between A and B, and lock escalation is not allowed to
  // bridge the gap between A and B.
  //
  // This method sets the user-provided barrier check function and its
  // parameter.
  void set_escalation_barrier_func(lt_escalation_barrier_check_func func,
                                   void *extra);

  int compare(const locktree *lt) const;

  DICTIONARY_ID get_dict_id() const;

  // Private info struct for storing pending lock request state.
  // Only to be used by lock requests. We store it here as
  // something less opaque than usual to strike a tradeoff between
  // abstraction and code complexity. It is still fairly abstract
  // since the lock_request object is opaque
  struct lt_lock_request_info *get_lock_request_info(void);

  typedef void (*dump_callback)(void *cdata, const DBT *left, const DBT *right,
                                TXNID txnid, bool is_shared,
                                TxnidVector *owners);
  void dump_locks(void *cdata, dump_callback cb);

 private:
  locktree_manager *m_mgr;
  DICTIONARY_ID m_dict_id;
  uint32_t m_reference_count;

  // Since the memory referenced by this comparator is not owned by the
  // locktree, the user must guarantee it will outlive the locktree.
  //
  // The ydb API accomplishes this by opening an ft_handle in the on_create
  // callback, which will keep the underlying FT (and its descriptor) in memory
  // for as long as the handle is open. The ft_handle is stored opaquely in the
  // userdata pointer below. see locktree_manager::get_lt w/ on_create_extra
  comparator m_cmp;

  lt_escalation_barrier_check_func m_escalation_barrier;
  void *m_escalation_barrier_arg;

  concurrent_tree *m_rangetree;

  void *m_userdata;
  struct lt_lock_request_info m_lock_request_info;

  // psergey-todo:
  //  Each transaction also keeps a list of ranges it has locked.
  //  So, when a transaction is running in STO mode, two identical
  //  lists are kept: the STO lock list and transaction's owned locks
  //  list. Why can't we do with just one list?

  // The following fields and members prefixed with "sto_" are for
  // the single txnid optimization, intended to speed up the case
  // when only one transaction is using the locktree. If we know
  // the locktree has only one transaction, then acquiring locks
  // takes O(1) work and releasing all locks takes O(1) work.
  //
  // How do we know that the locktree only has a single txnid?
  // What do we do if it does?
  //
  // When a txn with txnid T requests a lock:
  // - If the tree is empty, the optimization is possible. Set the single
  // txnid to T, and insert the lock range into the buffer.
  // - If the tree is not empty, check if the single txnid is T. If so,
  // append the lock range to the buffer. Otherwise, migrate all of
  // the locks in the buffer into the rangetree on behalf of txnid T,
  // and invalid the single txnid.
  //
  // When a txn with txnid T releases its locks:
  // - If the single txnid is valid, it must be for T. Destroy the buffer.
  // - If it's not valid, release locks the normal way in the rangetree.
  //
  // To carry out the optimization we need to record a single txnid
  // and a range buffer for each locktree, each protected by the root
  // lock of the locktree's rangetree. The root lock for a rangetree
  // is grabbed by preparing a locked keyrange on the rangetree.
  TXNID m_sto_txnid;
  range_buffer m_sto_buffer;

  // The single txnid optimization speeds up the case when only one
  // transaction is using the locktree. But it has the potential to
  // hurt the case when more than one txnid exists.
  //
  // There are two things we need to do to make the optimization only
  // optimize the case we care about, and not hurt the general case.
  //
  // Bound the worst-case latency for lock migration when the
  // optimization stops working:
  // - Idea: Stop the optimization and migrate immediate if we notice
  // the single txnid has takes many locks in the range buffer.
  // - Implementation: Enforce a max size on the single txnid range buffer.
  // - Analysis: Choosing the perfect max value, M, is difficult to do
  // without some feedback from the field. Intuition tells us that M should
  // not be so small that the optimization is worthless, and it should not
  // be so big that it's unreasonable to have to wait behind a thread doing
  // the work of converting M buffer locks into rangetree locks.
  //
  // Prevent concurrent-transaction workloads from trying the optimization
  // in vain:
  // - Idea: Don't even bother trying the optimization if we think the
  // system is in a concurrent-transaction state.
  // - Implementation: Do something even simpler than detecting whether the
  // system is in a concurent-transaction state. Just keep a "score" value
  // and some threshold. If at any time the locktree is eligible for the
  // optimization, only do it if the score is at this threshold. When you
  // actually do the optimization but someone has to migrate locks in the buffer
  // (expensive), then reset the score back to zero. Each time a txn
  // releases locks, the score is incremented by 1.
  // - Analysis: If you let the threshold be "C", then at most 1 / C txns will
  // do the optimization in a concurrent-transaction system. Similarly, it
  // takes at most C txns to start using the single txnid optimzation, which
  // is good when the system transitions from multithreaded to single threaded.
  //
  // STO_BUFFER_MAX_SIZE:
  //
  // We choose the max value to be 1 million since most transactions are smaller
  // than 1 million and we can create a rangetree of 1 million elements in
  // less than a second. So we can be pretty confident that this threshold
  // enables the optimization almost always, and prevents super pathological
  // latency issues for the first lock taken by a second thread.
  //
  // STO_SCORE_THRESHOLD:
  //
  // A simple first guess at a good value for the score threshold is 100.
  // By our analysis, we'd end up doing the optimization in vain for
  // around 1% of all transactions, which seems reasonable. Further,
  // if the system goes single threaded, it ought to be pretty quick
  // for 100 transactions to go by, so we won't have to wait long before
  // we start doing the single txind optimzation again.
  static const int STO_BUFFER_MAX_SIZE = 50 * 1024;
  static const int STO_SCORE_THRESHOLD = 100;
  int m_sto_score;

  // statistics about time spent ending the STO early
  uint64_t m_sto_end_early_count;
  tokutime_t m_sto_end_early_time;

  // effect: begins the single txnid optimizaiton, setting m_sto_txnid
  //         to the given txnid.
  // requires: m_sto_txnid is invalid
  void sto_begin(TXNID txnid);

  // effect: append a range to the sto buffer
  // requires: m_sto_txnid is valid
  void sto_append(const DBT *left_key, const DBT *right_key,
                  bool is_write_request);

  // effect: ends the single txnid optimization, releaseing any memory
  //         stored in the sto buffer, notifying the tracker, and
  //         invalidating m_sto_txnid.
  // requires: m_sto_txnid is valid
  void sto_end(void);

  // params: prepared_lkr is a void * to a prepared locked keyrange. see below.
  // effect: ends the single txnid optimization early, migrating buffer locks
  //         into the rangetree, calling sto_end(), and then setting the
  //         sto_score back to zero.
  // requires: m_sto_txnid is valid
  void sto_end_early(void *prepared_lkr);
  void sto_end_early_no_accounting(void *prepared_lkr);

  // params: prepared_lkr is a void * to a prepared locked keyrange. we can't
  // use
  //         the real type because the compiler won't allow us to forward
  //         declare concurrent_tree::locked_keyrange without including
  //         concurrent_tree.h, which we cannot do here because it is a template
  //         implementation.
  // requires: the prepared locked keyrange is for the locktree's rangetree
  // requires: m_sto_txnid is valid
  // effect: migrates each lock in the single txnid buffer into the locktree's
  //         rangetree, notifying the memory tracker as necessary.
  void sto_migrate_buffer_ranges_to_tree(void *prepared_lkr);

  // effect: If m_sto_txnid is valid, then release the txnid's locks
  //         by ending the optimization.
  // requires: If m_sto_txnid is valid, it is equal to the given txnid
  // returns: True if locks were released for this txnid
  bool sto_try_release(TXNID txnid);

  // params: prepared_lkr is a void * to a prepared locked keyrange. see above.
  // requires: the prepared locked keyrange is for the locktree's rangetree
  // effect: If m_sto_txnid is valid and equal to the given txnid, then
  // append a range onto the buffer. Otherwise, if m_sto_txnid is valid
  //        but not equal to this txnid, then migrate the buffer's locks
  //        into the rangetree and end the optimization, setting the score
  //        back to zero.
  // returns: true if the lock was acquired for this txnid
  bool sto_try_acquire(void *prepared_lkr, TXNID txnid, const DBT *left_key,
                       const DBT *right_key, bool is_write_request);

  // Effect:
  //  Provides a hook for a helgrind suppression.
  // Returns:
  //  true if m_sto_txnid is not TXNID_NONE
  bool sto_txnid_is_valid_unsafe(void) const;

  // Effect:
  //  Provides a hook for a helgrind suppression.
  // Returns:
  //  m_sto_score
  int sto_get_score_unsafe(void) const;

  void remove_overlapping_locks_for_txnid(TXNID txnid, const DBT *left_key,
                                          const DBT *right_key);

  int acquire_lock_consolidated(void *prepared_lkr, TXNID txnid,
                                const DBT *left_key, const DBT *right_key,
                                bool is_write_request, txnid_set *conflicts);

  int acquire_lock(bool is_write_request, TXNID txnid, const DBT *left_key,
                   const DBT *right_key, txnid_set *conflicts);

  int try_acquire_lock(bool is_write_request, TXNID txnid, const DBT *left_key,
                       const DBT *right_key, txnid_set *conflicts,
                       bool big_txn);

  friend class locktree_unit_test;
  friend class manager_unit_test;
  friend class lock_request_unit_test;

  // engine status reaches into the locktree to read some stats
  friend void locktree_manager::get_status(LTM_STATUS status);
};

} /* namespace toku */
