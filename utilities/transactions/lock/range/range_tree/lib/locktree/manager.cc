/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
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

#include <stdlib.h>
#include <string.h>

#include "../portability/toku_pthread.h"
#include "../util/status.h"
#include "lock_request.h"
#include "locktree.h"

namespace toku {

void locktree_manager::create(lt_create_cb create_cb, lt_destroy_cb destroy_cb,
                              lt_escalate_cb escalate_cb, void *escalate_extra,
                              toku_external_mutex_factory_t mutex_factory_arg) {
  mutex_factory = mutex_factory_arg;
  m_max_lock_memory = DEFAULT_MAX_LOCK_MEMORY;
  m_current_lock_memory = 0;

  m_locktree_map.create();
  m_lt_create_callback = create_cb;
  m_lt_destroy_callback = destroy_cb;
  m_lt_escalate_callback = escalate_cb;
  m_lt_escalate_callback_extra = escalate_extra;
  ZERO_STRUCT(m_mutex);
  toku_mutex_init(manager_mutex_key, &m_mutex, nullptr);

  ZERO_STRUCT(m_lt_counters);

  escalator_init();
}

void locktree_manager::destroy(void) {
  escalator_destroy();
  invariant(m_current_lock_memory == 0);
  invariant(m_locktree_map.size() == 0);
  m_locktree_map.destroy();
  toku_mutex_destroy(&m_mutex);
}

void locktree_manager::mutex_lock(void) { toku_mutex_lock(&m_mutex); }

void locktree_manager::mutex_unlock(void) { toku_mutex_unlock(&m_mutex); }

size_t locktree_manager::get_max_lock_memory(void) { return m_max_lock_memory; }

int locktree_manager::set_max_lock_memory(size_t max_lock_memory) {
  int r = 0;
  mutex_lock();
  if (max_lock_memory < m_current_lock_memory) {
    r = EDOM;
  } else {
    m_max_lock_memory = max_lock_memory;
  }
  mutex_unlock();
  return r;
}

int locktree_manager::find_by_dict_id(locktree *const &lt,
                                      const DICTIONARY_ID &dict_id) {
  if (lt->get_dict_id().dictid < dict_id.dictid) {
    return -1;
  } else if (lt->get_dict_id().dictid == dict_id.dictid) {
    return 0;
  } else {
    return 1;
  }
}

locktree *locktree_manager::locktree_map_find(const DICTIONARY_ID &dict_id) {
  locktree *lt;
  int r = m_locktree_map.find_zero<DICTIONARY_ID, find_by_dict_id>(dict_id, &lt,
                                                                   nullptr);
  return r == 0 ? lt : nullptr;
}

void locktree_manager::locktree_map_put(locktree *lt) {
  int r = m_locktree_map.insert<DICTIONARY_ID, find_by_dict_id>(
      lt, lt->get_dict_id(), nullptr);
  invariant_zero(r);
}

void locktree_manager::locktree_map_remove(locktree *lt) {
  uint32_t idx;
  locktree *found_lt;
  int r = m_locktree_map.find_zero<DICTIONARY_ID, find_by_dict_id>(
      lt->get_dict_id(), &found_lt, &idx);
  invariant_zero(r);
  invariant(found_lt == lt);
  r = m_locktree_map.delete_at(idx);
  invariant_zero(r);
}

locktree *locktree_manager::get_lt(DICTIONARY_ID dict_id, const comparator &cmp,
                                   void *on_create_extra) {
  // hold the mutex around searching and maybe
  // inserting into the locktree map
  mutex_lock();

  locktree *lt = locktree_map_find(dict_id);
  if (lt == nullptr) {
    XCALLOC(lt);
    lt->create(this, dict_id, cmp, mutex_factory);

    // new locktree created - call the on_create callback
    // and put it in the locktree map
    if (m_lt_create_callback) {
      int r = m_lt_create_callback(lt, on_create_extra);
      if (r != 0) {
        lt->release_reference();
        lt->destroy();
        toku_free(lt);
        lt = nullptr;
      }
    }
    if (lt) {
      locktree_map_put(lt);
    }
  } else {
    reference_lt(lt);
  }

  mutex_unlock();

  return lt;
}

void locktree_manager::reference_lt(locktree *lt) {
  // increment using a sync fetch and add.
  // the caller guarantees that the lt won't be
  // destroyed while we increment the count here.
  //
  // the caller can do this by already having an lt
  // reference or by holding the manager mutex.
  //
  // if the manager's mutex is held, it is ok for the
  // reference count to transition from 0 to 1 (no race),
  // since we're serialized with other opens and closes.
  lt->add_reference();
}

void locktree_manager::release_lt(locktree *lt) {
  bool do_destroy = false;
  DICTIONARY_ID dict_id = lt->get_dict_id();

  // Release a reference on the locktree. If the count transitions to zero,
  // then we *may* need to do the cleanup.
  //
  // Grab the manager's mutex and look for a locktree with this locktree's
  // dictionary id. Since dictionary id's never get reused, any locktree
  // found must be the one we just released a reference on.
  //
  // At least two things could have happened since we got the mutex:
  // - Another thread gets a locktree with the same dict_id, increments
  // the reference count. In this case, we shouldn't destroy it.
  // - Another thread gets a locktree with the same dict_id and then
  // releases it quickly, transitioning the reference count from zero to
  // one and back to zero. In this case, only one of us should destroy it.
  // It doesn't matter which. We originally missed this case, see #5776.
  //
  // After 5776, the high level rule for release is described below.
  //
  // If a thread releases a locktree and notices the reference count transition
  // to zero, then that thread must immediately:
  // - assume the locktree object is invalid
  // - grab the manager's mutex
  // - search the locktree map for a locktree with the same dict_id and remove
  // it, if it exists. the destroy may be deferred.
  // - release the manager's mutex
  //
  // This way, if many threads transition the same locktree's reference count
  // from 1 to zero and wait behind the manager's mutex, only one of them will
  // do the actual destroy and the others will happily do nothing.
  uint32_t refs = lt->release_reference();
  if (refs == 0) {
    mutex_lock();
    // lt may not have already been destroyed, so look it up.
    locktree *find_lt = locktree_map_find(dict_id);
    if (find_lt != nullptr) {
      // A locktree is still in the map with that dict_id, so it must be
      // equal to lt. This is true because dictionary ids are never reused.
      // If the reference count is zero, it's our responsibility to remove
      // it and do the destroy. Otherwise, someone still wants it.
      // If the locktree is still valid then check if it should be deleted.
      if (find_lt == lt) {
        if (lt->get_reference_count() == 0) {
          locktree_map_remove(lt);
          do_destroy = true;
        }
        m_lt_counters.add(lt->get_lock_request_info()->counters);
      }
    }
    mutex_unlock();
  }

  // if necessary, do the destroy without holding the mutex
  if (do_destroy) {
    if (m_lt_destroy_callback) {
      m_lt_destroy_callback(lt);
    }
    lt->destroy();
    toku_free(lt);
  }
}

void locktree_manager::run_escalation(void) {
  struct escalation_fn {
    static void run(void *extra) {
      locktree_manager *mgr = (locktree_manager *)extra;
      mgr->escalate_all_locktrees();
    };
  };
  m_escalator.run(this, escalation_fn::run, this);
}

// test-only version of lock escalation
void locktree_manager::run_escalation_for_test(void) { run_escalation(); }

void locktree_manager::escalate_all_locktrees(void) {
  uint64_t t0 = toku_current_time_microsec();

  // get all locktrees
  mutex_lock();
  int num_locktrees = m_locktree_map.size();
  locktree **locktrees = new locktree *[num_locktrees];
  for (int i = 0; i < num_locktrees; i++) {
    int r = m_locktree_map.fetch(i, &locktrees[i]);
    invariant_zero(r);
    reference_lt(locktrees[i]);
  }
  mutex_unlock();

  // escalate them
  escalate_locktrees(locktrees, num_locktrees);

  delete[] locktrees;

  uint64_t t1 = toku_current_time_microsec();
  add_escalator_wait_time(t1 - t0);
}

void locktree_manager::note_mem_used(uint64_t mem_used) {
  (void)toku_sync_fetch_and_add(&m_current_lock_memory, mem_used);
}

void locktree_manager::note_mem_released(uint64_t mem_released) {
  uint64_t old_mem_used =
      toku_sync_fetch_and_sub(&m_current_lock_memory, mem_released);
  invariant(old_mem_used >= mem_released);
}

bool locktree_manager::out_of_locks(void) const {
  return m_current_lock_memory >= m_max_lock_memory;
}

bool locktree_manager::over_big_threshold(void) {
  return m_current_lock_memory >= m_max_lock_memory / 2;
}

int locktree_manager::iterate_pending_lock_requests(
    lock_request_iterate_callback callback, void *extra) {
  mutex_lock();
  int r = 0;
  uint32_t num_locktrees = m_locktree_map.size();
  for (uint32_t i = 0; i < num_locktrees && r == 0; i++) {
    locktree *lt;
    r = m_locktree_map.fetch(i, &lt);
    invariant_zero(r);
    if (r == EINVAL)  // Shouldn't happen, avoid compiler warning
      continue;

    struct lt_lock_request_info *info = lt->get_lock_request_info();
    toku_external_mutex_lock(&info->mutex);

    uint32_t num_requests = info->pending_lock_requests.size();
    for (uint32_t k = 0; k < num_requests && r == 0; k++) {
      lock_request *req;
      r = info->pending_lock_requests.fetch(k, &req);
      invariant_zero(r);
      if (r == EINVAL) /* Shouldn't happen, avoid compiler warning */
        continue;
      r = callback(lt->get_dict_id(), req->get_txnid(), req->get_left_key(),
                   req->get_right_key(), req->get_conflicting_txnid(),
                   req->get_start_time(), extra);
    }

    toku_external_mutex_unlock(&info->mutex);
  }
  mutex_unlock();
  return r;
}

int locktree_manager::check_current_lock_constraints(bool big_txn) {
  int r = 0;
  if (big_txn && over_big_threshold()) {
    run_escalation();
    if (over_big_threshold()) {
      r = TOKUDB_OUT_OF_LOCKS;
    }
  }
  if (r == 0 && out_of_locks()) {
    run_escalation();
    if (out_of_locks()) {
      // return an error if we're still out of locks after escalation.
      r = TOKUDB_OUT_OF_LOCKS;
    }
  }
  return r;
}

void locktree_manager::escalator_init(void) {
  ZERO_STRUCT(m_escalation_mutex);
  toku_mutex_init(manager_escalation_mutex_key, &m_escalation_mutex, nullptr);
  m_escalation_count = 0;
  m_escalation_time = 0;
  m_wait_escalation_count = 0;
  m_wait_escalation_time = 0;
  m_long_wait_escalation_count = 0;
  m_long_wait_escalation_time = 0;
  m_escalation_latest_result = 0;
  m_escalator.create();
}

void locktree_manager::escalator_destroy(void) {
  m_escalator.destroy();
  toku_mutex_destroy(&m_escalation_mutex);
}

void locktree_manager::add_escalator_wait_time(uint64_t t) {
  toku_mutex_lock(&m_escalation_mutex);
  m_wait_escalation_count += 1;
  m_wait_escalation_time += t;
  if (t >= 1000000) {
    m_long_wait_escalation_count += 1;
    m_long_wait_escalation_time += t;
  }
  toku_mutex_unlock(&m_escalation_mutex);
}

void locktree_manager::escalate_locktrees(locktree **locktrees,
                                          int num_locktrees) {
  // there are too many row locks in the system and we need to tidy up.
  //
  // a simple implementation of escalation does not attempt
  // to reduce the memory foot print of each txn's range buffer.
  // doing so would require some layering hackery (or a callback)
  // and more complicated locking. for now, just escalate each
  // locktree individually, in-place.
  tokutime_t t0 = toku_time_now();
  for (int i = 0; i < num_locktrees; i++) {
    locktrees[i]->escalate(m_lt_escalate_callback,
                           m_lt_escalate_callback_extra);
    release_lt(locktrees[i]);
  }
  tokutime_t t1 = toku_time_now();

  toku_mutex_lock(&m_escalation_mutex);
  m_escalation_count++;
  m_escalation_time += (t1 - t0);
  m_escalation_latest_result = m_current_lock_memory;
  toku_mutex_unlock(&m_escalation_mutex);
}

struct escalate_args {
  locktree_manager *mgr;
  locktree **locktrees;
  int num_locktrees;
};

void locktree_manager::locktree_escalator::create(void) {
  ZERO_STRUCT(m_escalator_mutex);
  toku_mutex_init(manager_escalator_mutex_key, &m_escalator_mutex, nullptr);
  toku_cond_init(manager_m_escalator_done_key, &m_escalator_done, nullptr);
  m_escalator_running = false;
}

void locktree_manager::locktree_escalator::destroy(void) {
  toku_cond_destroy(&m_escalator_done);
  toku_mutex_destroy(&m_escalator_mutex);
}

void locktree_manager::locktree_escalator::run(
    locktree_manager *mgr, void (*escalate_locktrees_fun)(void *extra),
    void *extra) {
  uint64_t t0 = toku_current_time_microsec();
  toku_mutex_lock(&m_escalator_mutex);
  if (!m_escalator_running) {
    // run escalation on this thread
    m_escalator_running = true;
    toku_mutex_unlock(&m_escalator_mutex);
    escalate_locktrees_fun(extra);
    toku_mutex_lock(&m_escalator_mutex);
    m_escalator_running = false;
    toku_cond_broadcast(&m_escalator_done);
  } else {
    toku_cond_wait(&m_escalator_done, &m_escalator_mutex);
  }
  toku_mutex_unlock(&m_escalator_mutex);
  uint64_t t1 = toku_current_time_microsec();
  mgr->add_escalator_wait_time(t1 - t0);
}

void locktree_manager::get_status(LTM_STATUS statp) {
  ltm_status.init();
  LTM_STATUS_VAL(LTM_SIZE_CURRENT) = m_current_lock_memory;
  LTM_STATUS_VAL(LTM_SIZE_LIMIT) = m_max_lock_memory;
  LTM_STATUS_VAL(LTM_ESCALATION_COUNT) = m_escalation_count;
  LTM_STATUS_VAL(LTM_ESCALATION_TIME) = m_escalation_time;
  LTM_STATUS_VAL(LTM_ESCALATION_LATEST_RESULT) = m_escalation_latest_result;
  LTM_STATUS_VAL(LTM_WAIT_ESCALATION_COUNT) = m_wait_escalation_count;
  LTM_STATUS_VAL(LTM_WAIT_ESCALATION_TIME) = m_wait_escalation_time;
  LTM_STATUS_VAL(LTM_LONG_WAIT_ESCALATION_COUNT) = m_long_wait_escalation_count;
  LTM_STATUS_VAL(LTM_LONG_WAIT_ESCALATION_TIME) = m_long_wait_escalation_time;

  uint64_t lock_requests_pending = 0;
  uint64_t sto_num_eligible = 0;
  uint64_t sto_end_early_count = 0;
  tokutime_t sto_end_early_time = 0;
  uint32_t num_locktrees = 0;
  struct lt_counters lt_counters;
  ZERO_STRUCT(lt_counters);  // PORT: instead of ={}.

  if (toku_mutex_trylock(&m_mutex) == 0) {
    lt_counters = m_lt_counters;
    num_locktrees = m_locktree_map.size();
    for (uint32_t i = 0; i < num_locktrees; i++) {
      locktree *lt;
      int r = m_locktree_map.fetch(i, &lt);
      invariant_zero(r);
      if (r == EINVAL)  // Shouldn't happen, avoid compiler warning
        continue;
      if (toku_external_mutex_trylock(&lt->m_lock_request_info.mutex) == 0) {
        lock_requests_pending +=
            lt->m_lock_request_info.pending_lock_requests.size();
        lt_counters.add(lt->get_lock_request_info()->counters);
        toku_external_mutex_unlock(&lt->m_lock_request_info.mutex);
      }
      sto_num_eligible += lt->sto_txnid_is_valid_unsafe() ? 1 : 0;
      sto_end_early_count += lt->m_sto_end_early_count;
      sto_end_early_time += lt->m_sto_end_early_time;
    }
    mutex_unlock();
  }

  LTM_STATUS_VAL(LTM_NUM_LOCKTREES) = num_locktrees;
  LTM_STATUS_VAL(LTM_LOCK_REQUESTS_PENDING) = lock_requests_pending;
  LTM_STATUS_VAL(LTM_STO_NUM_ELIGIBLE) = sto_num_eligible;
  LTM_STATUS_VAL(LTM_STO_END_EARLY_COUNT) = sto_end_early_count;
  LTM_STATUS_VAL(LTM_STO_END_EARLY_TIME) = sto_end_early_time;
  LTM_STATUS_VAL(LTM_WAIT_COUNT) = lt_counters.wait_count;
  LTM_STATUS_VAL(LTM_WAIT_TIME) = lt_counters.wait_time;
  LTM_STATUS_VAL(LTM_LONG_WAIT_COUNT) = lt_counters.long_wait_count;
  LTM_STATUS_VAL(LTM_LONG_WAIT_TIME) = lt_counters.long_wait_time;
  LTM_STATUS_VAL(LTM_TIMEOUT_COUNT) = lt_counters.timeout_count;
  *statp = ltm_status;
}

void locktree_manager::kill_waiter(void *extra) {
  mutex_lock();
  int r = 0;
  uint32_t num_locktrees = m_locktree_map.size();
  for (uint32_t i = 0; i < num_locktrees; i++) {
    locktree *lt;
    r = m_locktree_map.fetch(i, &lt);
    invariant_zero(r);
    if (r) continue;  // Get rid of "may be used uninitialized" warning
    lock_request::kill_waiter(lt, extra);
  }
  mutex_unlock();
}

} /* namespace toku */
#endif  // OS_WIN
#endif  // ROCKSDB_LITE
