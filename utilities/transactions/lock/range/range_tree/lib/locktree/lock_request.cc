/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=2:softtabstop=2:
#ifndef ROCKSDB_LITE
#ifndef OS_WIN
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

#include "lock_request.h"

#include "../portability/toku_race_tools.h"
#include "../portability/txn_subst.h"
#include "../util/dbt.h"
#include "locktree.h"

namespace toku {

// initialize a lock request's internals
void lock_request::create(toku_external_mutex_factory_t mutex_factory) {
  m_txnid = TXNID_NONE;
  m_conflicting_txnid = TXNID_NONE;
  m_start_time = 0;
  m_left_key = nullptr;
  m_right_key = nullptr;
  toku_init_dbt(&m_left_key_copy);
  toku_init_dbt(&m_right_key_copy);

  m_type = type::UNKNOWN;
  m_lt = nullptr;

  m_complete_r = 0;
  m_state = state::UNINITIALIZED;
  m_info = nullptr;

  // psergey-todo: this condition is for interruptible wait
  // note: moved to here from lock_request::create:
  toku_external_cond_init(mutex_factory, &m_wait_cond);

  m_start_test_callback = nullptr;
  m_start_before_pending_test_callback = nullptr;
  m_retry_test_callback = nullptr;
}

// destroy a lock request.
void lock_request::destroy(void) {
  invariant(m_state != state::PENDING);
  invariant(m_state != state::DESTROYED);
  m_state = state::DESTROYED;
  toku_destroy_dbt(&m_left_key_copy);
  toku_destroy_dbt(&m_right_key_copy);
  toku_external_cond_destroy(&m_wait_cond);
}

// set the lock request parameters. this API allows a lock request to be reused.
void lock_request::set(locktree *lt, TXNID txnid, const DBT *left_key,
                       const DBT *right_key, lock_request::type lock_type,
                       bool big_txn, void *extra) {
  invariant(m_state != state::PENDING);
  m_lt = lt;

  m_txnid = txnid;
  m_left_key = left_key;
  m_right_key = right_key;
  toku_destroy_dbt(&m_left_key_copy);
  toku_destroy_dbt(&m_right_key_copy);
  m_type = lock_type;
  m_state = state::INITIALIZED;
  m_info = lt ? lt->get_lock_request_info() : nullptr;
  m_big_txn = big_txn;
  m_extra = extra;
}

// get rid of any stored left and right key copies and
// replace them with copies of the given left and right key
void lock_request::copy_keys() {
  if (!toku_dbt_is_infinite(m_left_key)) {
    toku_clone_dbt(&m_left_key_copy, *m_left_key);
    m_left_key = &m_left_key_copy;
  }
  if (!toku_dbt_is_infinite(m_right_key)) {
    toku_clone_dbt(&m_right_key_copy, *m_right_key);
    m_right_key = &m_right_key_copy;
  }
}

// what are the conflicts for this pending lock request?
void lock_request::get_conflicts(txnid_set *conflicts) {
  invariant(m_state == state::PENDING);
  const bool is_write_request = m_type == type::WRITE;
  m_lt->get_conflicts(is_write_request, m_txnid, m_left_key, m_right_key,
                      conflicts);
}

// build a wait-for-graph for this lock request and the given conflict set
// for each transaction B that blocks A's lock request
//     if B is blocked then
//         add (A,T) to the WFG and if B is new, fill in the WFG from B
void lock_request::build_wait_graph(wfg *wait_graph,
                                    const txnid_set &conflicts) {
  uint32_t num_conflicts = conflicts.size();
  for (uint32_t i = 0; i < num_conflicts; i++) {
    TXNID conflicting_txnid = conflicts.get(i);
    lock_request *conflicting_request = find_lock_request(conflicting_txnid);
    invariant(conflicting_txnid != m_txnid);
    invariant(conflicting_request != this);
    if (conflicting_request) {
      bool already_exists = wait_graph->node_exists(conflicting_txnid);
      wait_graph->add_edge(m_txnid, conflicting_txnid);
      if (!already_exists) {
        // recursively build the wait for graph rooted at the conflicting
        // request, given its set of lock conflicts.
        txnid_set other_conflicts;
        other_conflicts.create();
        conflicting_request->get_conflicts(&other_conflicts);
        conflicting_request->build_wait_graph(wait_graph, other_conflicts);
        other_conflicts.destroy();
      }
    }
  }
}

// returns: true if the current set of lock requests contains
//          a deadlock, false otherwise.
bool lock_request::deadlock_exists(const txnid_set &conflicts) {
  wfg wait_graph;
  wait_graph.create();

  build_wait_graph(&wait_graph, conflicts);

  std::function<void(TXNID)> reporter;
  if (m_deadlock_cb) {
    reporter = [this](TXNID a) {
      lock_request *req = find_lock_request(a);
      if (req) {
        m_deadlock_cb(req->m_txnid, (req->m_type == lock_request::WRITE),
                      req->m_left_key, req->m_right_key);
      }
    };
  }

  bool deadlock = wait_graph.cycle_exists_from_txnid(m_txnid, reporter);
  wait_graph.destroy();
  return deadlock;
}

// try to acquire a lock described by this lock request.
int lock_request::start(void) {
  int r;

  txnid_set conflicts;
  conflicts.create();
  if (m_type == type::WRITE) {
    r = m_lt->acquire_write_lock(m_txnid, m_left_key, m_right_key, &conflicts,
                                 m_big_txn);
  } else {
    invariant(m_type == type::READ);
    r = m_lt->acquire_read_lock(m_txnid, m_left_key, m_right_key, &conflicts,
                                m_big_txn);
  }

  // if the lock is not granted, save it to the set of lock requests
  // and check for a deadlock. if there is one, complete it as failed
  if (r == DB_LOCK_NOTGRANTED) {
    copy_keys();
    m_state = state::PENDING;
    m_start_time = toku_current_time_microsec() / 1000;
    m_conflicting_txnid = conflicts.get(0);
    if (m_start_before_pending_test_callback)
      m_start_before_pending_test_callback();
    toku_external_mutex_lock(&m_info->mutex);
    insert_into_lock_requests();
    if (deadlock_exists(conflicts)) {
      remove_from_lock_requests();
      r = DB_LOCK_DEADLOCK;
    }
    toku_external_mutex_unlock(&m_info->mutex);
    if (m_start_test_callback) m_start_test_callback();  // test callback
  }

  if (r != DB_LOCK_NOTGRANTED) {
    complete(r);
  }

  conflicts.destroy();
  return r;
}

// sleep on the lock request until it becomes resolved or the wait time has
// elapsed.
int lock_request::wait(uint64_t wait_time_ms) {
  return wait(wait_time_ms, 0, nullptr);
}

int lock_request::wait(uint64_t wait_time_ms, uint64_t killed_time_ms,
                       int (*killed_callback)(void),
                       void (*lock_wait_callback)(void *, lock_wait_infos *),
                       void *callback_arg) {
  uint64_t t_now = toku_current_time_microsec();
  uint64_t t_start = t_now;
  uint64_t t_end = t_start + wait_time_ms * 1000;

  toku_external_mutex_lock(&m_info->mutex);

  // check again, this time locking out other retry calls
  if (m_state == state::PENDING) {
    lock_wait_infos conflicts_collector;
    retry(&conflicts_collector);
    if (m_state == state::PENDING) {
      report_waits(&conflicts_collector, lock_wait_callback, callback_arg);
    }
  }

  while (m_state == state::PENDING) {
    // check if this thread is killed
    if (killed_callback && killed_callback()) {
      remove_from_lock_requests();
      complete(DB_LOCK_NOTGRANTED);
      continue;
    }

    // compute the time until we should wait
    uint64_t t_wait;
    if (killed_time_ms == 0) {
      t_wait = t_end;
    } else {
      t_wait = t_now + killed_time_ms * 1000;
      if (t_wait > t_end) t_wait = t_end;
    }

    int r = toku_external_cond_timedwait(&m_wait_cond, &m_info->mutex,
                                         (int64_t)(t_wait - t_now));
    invariant(r == 0 || r == ETIMEDOUT);

    t_now = toku_current_time_microsec();
    if (m_state == state::PENDING && (t_now >= t_end)) {
      m_info->counters.timeout_count += 1;

      // if we're still pending and we timed out, then remove our
      // request from the set of lock requests and fail.
      remove_from_lock_requests();

      // complete sets m_state to COMPLETE, breaking us out of the loop
      complete(DB_LOCK_NOTGRANTED);
    }
  }

  uint64_t t_real_end = toku_current_time_microsec();
  uint64_t duration = t_real_end - t_start;
  m_info->counters.wait_count += 1;
  m_info->counters.wait_time += duration;
  if (duration >= 1000000) {
    m_info->counters.long_wait_count += 1;
    m_info->counters.long_wait_time += duration;
  }
  toku_external_mutex_unlock(&m_info->mutex);

  invariant(m_state == state::COMPLETE);
  return m_complete_r;
}

// complete this lock request with the given return value
void lock_request::complete(int complete_r) {
  m_complete_r = complete_r;
  m_state = state::COMPLETE;
}

const DBT *lock_request::get_left_key(void) const { return m_left_key; }

const DBT *lock_request::get_right_key(void) const { return m_right_key; }

TXNID lock_request::get_txnid(void) const { return m_txnid; }

uint64_t lock_request::get_start_time(void) const { return m_start_time; }

TXNID lock_request::get_conflicting_txnid(void) const {
  return m_conflicting_txnid;
}

int lock_request::retry(lock_wait_infos *conflicts_collector) {
  invariant(m_state == state::PENDING);
  int r;
  txnid_set conflicts;
  conflicts.create();

  if (m_type == type::WRITE) {
    r = m_lt->acquire_write_lock(m_txnid, m_left_key, m_right_key, &conflicts,
                                 m_big_txn);
  } else {
    r = m_lt->acquire_read_lock(m_txnid, m_left_key, m_right_key, &conflicts,
                                m_big_txn);
  }

  // if the acquisition succeeded then remove ourselves from the
  // set of lock requests, complete, and signal the waiting thread.
  if (r == 0) {
    remove_from_lock_requests();
    complete(r);
    if (m_retry_test_callback) m_retry_test_callback();  // test callback
    toku_external_cond_broadcast(&m_wait_cond);
  } else {
    m_conflicting_txnid = conflicts.get(0);
    add_conflicts_to_waits(&conflicts, conflicts_collector);
  }
  conflicts.destroy();

  return r;
}

void lock_request::retry_all_lock_requests(
    locktree *lt, void (*lock_wait_callback)(void *, lock_wait_infos *),
    void *callback_arg, void (*after_retry_all_test_callback)(void)) {
  lt_lock_request_info *info = lt->get_lock_request_info();

  // if there are no pending lock requests than there is nothing to do
  // the unlocked data race on pending_is_empty is OK since lock requests
  // are retried after added to the pending set.
  if (info->pending_is_empty) return;

  // get my retry generation (post increment of retry_want)
  unsigned long long my_retry_want = (info->retry_want += 1);

  toku_mutex_lock(&info->retry_mutex);

  // here is the group retry algorithm.
  // get the latest retry_want count and use it as the generation number of
  // this retry operation. if this retry generation is > the last retry
  // generation, then do the lock retries.  otherwise, no lock retries
  // are needed.
  if ((my_retry_want - 1) == info->retry_done) {
    for (;;) {
      if (!info->running_retry) {
        info->running_retry = true;
        info->retry_done = info->retry_want;
        toku_mutex_unlock(&info->retry_mutex);
        retry_all_lock_requests_info(info, lock_wait_callback, callback_arg);
        if (after_retry_all_test_callback) after_retry_all_test_callback();
        toku_mutex_lock(&info->retry_mutex);
        info->running_retry = false;
        toku_cond_broadcast(&info->retry_cv);
        break;
      } else {
        toku_cond_wait(&info->retry_cv, &info->retry_mutex);
      }
    }
  }
  toku_mutex_unlock(&info->retry_mutex);
}

void lock_request::retry_all_lock_requests_info(
    lt_lock_request_info *info,
    void (*lock_wait_callback)(void *, lock_wait_infos *), void *callback_arg) {
  toku_external_mutex_lock(&info->mutex);
  // retry all of the pending lock requests.
  lock_wait_infos conflicts_collector;
  for (uint32_t i = 0; i < info->pending_lock_requests.size();) {
    lock_request *request;
    int r = info->pending_lock_requests.fetch(i, &request);
    invariant_zero(r);

    // retry the lock request. if it didn't succeed,
    // move on to the next lock request. otherwise
    // the request is gone from the list so we may
    // read the i'th entry for the next one.
    r = request->retry(&conflicts_collector);
    if (r != 0) {
      i++;
    }
  }

  // call report_waits while holding the pending queue lock since
  // the waiter object is still valid while it's in the queue
  report_waits(&conflicts_collector, lock_wait_callback, callback_arg);

  // future threads should only retry lock requests if some still exist
  info->should_retry_lock_requests = info->pending_lock_requests.size() > 0;
  toku_external_mutex_unlock(&info->mutex);
}

void lock_request::add_conflicts_to_waits(txnid_set *conflicts,
                                          lock_wait_infos *wait_conflicts) {
  wait_conflicts->push_back({m_lt, get_txnid(), m_extra, {}});
  uint32_t num_conflicts = conflicts->size();
  for (uint32_t i = 0; i < num_conflicts; i++) {
    wait_conflicts->back().waitees.push_back(conflicts->get(i));
  }
}

void lock_request::report_waits(lock_wait_infos *wait_conflicts,
                                void (*lock_wait_callback)(void *,
                                                           lock_wait_infos *),
                                void *callback_arg) {
  if (lock_wait_callback) (*lock_wait_callback)(callback_arg, wait_conflicts);
}

void *lock_request::get_extra(void) const { return m_extra; }

void lock_request::kill_waiter(void) {
  remove_from_lock_requests();
  complete(DB_LOCK_NOTGRANTED);
  toku_external_cond_broadcast(&m_wait_cond);
}

void lock_request::kill_waiter(locktree *lt, void *extra) {
  lt_lock_request_info *info = lt->get_lock_request_info();
  toku_external_mutex_lock(&info->mutex);
  for (uint32_t i = 0; i < info->pending_lock_requests.size(); i++) {
    lock_request *request;
    int r = info->pending_lock_requests.fetch(i, &request);
    if (r == 0 && request->get_extra() == extra) {
      request->kill_waiter();
      break;
    }
  }
  toku_external_mutex_unlock(&info->mutex);
}

// find another lock request by txnid. must hold the mutex.
lock_request *lock_request::find_lock_request(const TXNID &txnid) {
  lock_request *request;
  int r = m_info->pending_lock_requests.find_zero<TXNID, find_by_txnid>(
      txnid, &request, nullptr);
  if (r != 0) {
    request = nullptr;
  }
  return request;
}

// insert this lock request into the locktree's set. must hold the mutex.
void lock_request::insert_into_lock_requests(void) {
  uint32_t idx;
  lock_request *request;
  int r = m_info->pending_lock_requests.find_zero<TXNID, find_by_txnid>(
      m_txnid, &request, &idx);
  invariant(r == DB_NOTFOUND);
  r = m_info->pending_lock_requests.insert_at(this, idx);
  invariant_zero(r);
  m_info->pending_is_empty = false;
}

// remove this lock request from the locktree's set. must hold the mutex.
void lock_request::remove_from_lock_requests(void) {
  uint32_t idx;
  lock_request *request;
  int r = m_info->pending_lock_requests.find_zero<TXNID, find_by_txnid>(
      m_txnid, &request, &idx);
  invariant_zero(r);
  invariant(request == this);
  r = m_info->pending_lock_requests.delete_at(idx);
  invariant_zero(r);
  if (m_info->pending_lock_requests.size() == 0)
    m_info->pending_is_empty = true;
}

int lock_request::find_by_txnid(lock_request *const &request,
                                const TXNID &txnid) {
  TXNID request_txnid = request->m_txnid;
  if (request_txnid < txnid) {
    return -1;
  } else if (request_txnid == txnid) {
    return 0;
  } else {
    return 1;
  }
}

void lock_request::set_start_test_callback(void (*f)(void)) {
  m_start_test_callback = f;
}

void lock_request::set_start_before_pending_test_callback(void (*f)(void)) {
  m_start_before_pending_test_callback = f;
}

void lock_request::set_retry_test_callback(void (*f)(void)) {
  m_retry_test_callback = f;
}

} /* namespace toku */
#endif  // OS_WIN
#endif  // ROCKSDB_LITE
