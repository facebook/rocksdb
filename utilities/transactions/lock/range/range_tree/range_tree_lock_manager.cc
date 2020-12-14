#ifndef ROCKSDB_LITE
#ifndef OS_WIN

#include "utilities/transactions/lock/range/range_tree/range_tree_lock_manager.h"

#include <algorithm>
#include <cinttypes>
#include <mutex>

#include "monitoring/perf_context_imp.h"
#include "range_tree_lock_tracker.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction_db_mutex.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/hash.h"
#include "util/thread_local.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"

namespace ROCKSDB_NAMESPACE {

/////////////////////////////////////////////////////////////////////////////
// RangeTreeLockManager -  A Range Lock Manager that uses PerconaFT's
// locktree library
/////////////////////////////////////////////////////////////////////////////

RangeLockManagerHandle* NewRangeLockManager(
    std::shared_ptr<TransactionDBMutexFactory> mutex_factory) {
  std::shared_ptr<TransactionDBMutexFactory> use_factory;

  if (mutex_factory)
    use_factory = mutex_factory;
  else
    use_factory.reset(new TransactionDBMutexFactoryImpl());

  return new RangeTreeLockManager(use_factory);
}

static const char SUFFIX_INFIMUM = 0x0;
static const char SUFFIX_SUPREMUM = 0x1;

void serialize_endpoint(const Endpoint& endp, std::string* buf) {
  buf->push_back(endp.inf_suffix ? SUFFIX_SUPREMUM : SUFFIX_INFIMUM);
  buf->append(endp.slice.data(), endp.slice.size());
}

void deserialize_endpoint(const DBT* dbt, EndpointWithString* endp) {
  assert(dbt->size >= 1);
  const char* dbt_data = (const char*)dbt->data;
  char suffix = dbt_data[0];
  assert(suffix == SUFFIX_INFIMUM || suffix == SUFFIX_SUPREMUM);
  endp->inf_suffix = (suffix == SUFFIX_SUPREMUM);
  endp->slice.assign(dbt_data + 1, dbt->size - 1);
}

// Get a range lock on [start_key; end_key] range
Status RangeTreeLockManager::TryLock(PessimisticTransaction* txn,
                                     uint32_t column_family_id,
                                     const Endpoint& start_endp,
                                     const Endpoint& end_endp, Env*,
                                     bool exclusive) {
  toku::lock_request request;
  request.create(mutex_factory_);
  DBT start_key_dbt, end_key_dbt;

  TEST_SYNC_POINT("RangeTreeLockManager::TryRangeLock:enter");
  std::string start_key;
  std::string end_key;
  serialize_endpoint(start_endp, &start_key);
  serialize_endpoint(end_endp, &end_key);

  toku_fill_dbt(&start_key_dbt, start_key.data(), start_key.size());
  toku_fill_dbt(&end_key_dbt, end_key.data(), end_key.size());

  // Use the txnid as "extra" in the lock_request. Then, KillLockWait()
  // will be able to use kill_waiter(txn_id) to kill the wait if needed
  // TODO: KillLockWait is gone and we are no longer using
  // locktree::kill_waiter call. Do we need this anymore?
  TransactionID wait_txn_id = txn->GetID();

  auto lt = get_locktree_by_cfid(column_family_id);

  request.set(lt, (TXNID)txn, &start_key_dbt, &end_key_dbt,
              exclusive ? toku::lock_request::WRITE : toku::lock_request::READ,
              false /* not a big txn */, (void*)wait_txn_id);

  // This is for "periodically wake up and check if the wait is killed" feature
  // which we are not using.
  uint64_t killed_time_msec = 0;
  uint64_t wait_time_msec = txn->GetLockTimeout();

  // convert microseconds to milliseconds
  if (wait_time_msec != (uint64_t)-1)
    wait_time_msec = (wait_time_msec + 500) / 1000;

  std::vector<RangeDeadlockInfo> di_path;
  request.m_deadlock_cb = [&](TXNID txnid, bool is_exclusive,
                              const DBT* start_dbt, const DBT* end_dbt) {
    EndpointWithString start;
    EndpointWithString end;
    deserialize_endpoint(start_dbt, &start);
    deserialize_endpoint(end_dbt, &end);

    di_path.push_back({((PessimisticTransaction*)txnid)->GetID(),
                       column_family_id, is_exclusive, std::move(start),
                       std::move(end)});
  };

  request.start();

  /*
    If we are going to wait on the lock, we should set appropriate status in
    the 'txn' object. This is done by the SetWaitingTxn() call below.
    The API we are using are MariaDB's wait notification API, so the way this
    is done is a bit convoluted.
    In MyRocks, the wait details are visible in I_S.rocksdb_trx.
  */
  std::string key_str(start_key.data(), start_key.size());
  struct st_wait_info {
    PessimisticTransaction* txn;
    uint32_t column_family_id;
    std::string* key_ptr;
    autovector<TransactionID> wait_ids;
    bool done = false;

    static void lock_wait_callback(void* cdata, TXNID /*waiter*/,
                                   TXNID waitee) {
      TEST_SYNC_POINT("RangeTreeLockManager::TryRangeLock:WaitingTxn");
      auto self = (struct st_wait_info*)cdata;
      // we know that the waiter is self->txn->GetID() (TODO: assert?)
      if (!self->done) {
        self->wait_ids.push_back(waitee);
        self->txn->SetWaitingTxn(self->wait_ids, self->column_family_id,
                                 self->key_ptr);
        self->done = true;
      }
    }
  } wait_info;

  wait_info.txn = txn;
  wait_info.column_family_id = column_family_id;
  wait_info.key_ptr = &key_str;
  wait_info.done = false;

  const int r =
      request.wait(wait_time_msec, killed_time_msec,
                   nullptr,  // killed_callback
                   st_wait_info::lock_wait_callback, (void*)&wait_info);

  // Inform the txn that we are no longer waiting:
  txn->ClearWaitingTxn();

  request.destroy();
  switch (r) {
    case 0:
      break; /* fall through */
    case DB_LOCK_NOTGRANTED:
      return Status::TimedOut(Status::SubCode::kLockTimeout);
    case TOKUDB_OUT_OF_LOCKS:
      return Status::Busy(Status::SubCode::kLockLimit);
    case DB_LOCK_DEADLOCK: {
      std::reverse(di_path.begin(), di_path.end());
      dlock_buffer_.AddNewPath(
          RangeDeadlockPath(di_path, request.get_start_time()));
      return Status::Busy(Status::SubCode::kDeadlock);
    }
    default:
      assert(0);
      return Status::Busy(Status::SubCode::kLockLimit);
  }

  // Don't: save the acquired lock in txn->owned_locks.
  // It is now responsibility of RangeTreeLockManager
  //  RangeLockList* range_list=
  //  ((RangeTreeLockTracker*)txn->tracked_locks_.get())->getOrCreateList();
  //  range_list->append(column_family_id, &start_key_dbt, &end_key_dbt);

  return Status::OK();
}

static void range_lock_mgr_release_lock_int(toku::locktree* lt,
                                            const PessimisticTransaction* txn,
                                            uint32_t /*column_family_id*/,
                                            const std::string& key) {
  DBT key_dbt;
  Endpoint endp(key.data(), key.size(), false);
  std::string endp_image;
  serialize_endpoint(endp, &endp_image);

  toku_fill_dbt(&key_dbt, endp_image.data(), endp_image.size());
  toku::range_buffer range_buf;
  range_buf.create();
  range_buf.append(&key_dbt, &key_dbt);
  lt->release_locks((TXNID)txn, &range_buf);
  range_buf.destroy();
}

void RangeTreeLockManager::UnLock(PessimisticTransaction* txn,
                                  ColumnFamilyId column_family_id,
                                  const std::string& key, Env*) {
  auto lt = get_locktree_by_cfid(column_family_id);
  range_lock_mgr_release_lock_int(lt, txn, column_family_id, key);
  toku::lock_request::retry_all_lock_requests(
      lt, nullptr /* lock_wait_needed_callback */);
}

void RangeTreeLockManager::UnLock(PessimisticTransaction* txn,
                                  const LockTracker& tracker, Env*) {
  const RangeTreeLockTracker* range_tracker =
      static_cast<const RangeTreeLockTracker*>(&tracker);

  RangeTreeLockTracker* range_trx_tracker =
      static_cast<RangeTreeLockTracker*>(&txn->GetTrackedLocks());

  bool all_keys = (range_trx_tracker == range_tracker);

  // tracked_locks_->range_list may hold nullptr if the transaction has never
  // acquired any locks.
  RangeLockList* range_list = ((RangeTreeLockTracker*)range_tracker)->getList();

  if (range_list) {
    {
      MutexLock l(&range_list->mutex_);
      /*
        The lt->release_locks() call below will walk range_list->buffer_. We
        need to prevent lock escalation callback from replacing
        range_list->buffer_ while we are doing that.

        Additional complication here is internal mutex(es) in the locktree
        (let's call them latches):
        - Lock escalation first obtains latches on the lock tree
        - Then, it calls RangeTreeLockManager::on_escalate to replace
        transaction's range_list->buffer_. = Access to that buffer must be
        synchronized, so it will want to acquire the range_list->mutex_.

        While in this function we would want to do the reverse:
        - Acquire range_list->mutex_ to prevent access to the range_list.
        - Then, lt->release_locks() call will walk through the range_list
        - and acquire latches on parts of the lock tree to remove locks from
          it.

        How do we avoid the deadlock? The idea is that here we set
        releasing_locks_=true, and release the mutex.
        All other users of the range_list must:
        - Acquire the mutex, then check that releasing_locks_=false.
          (the code in this function doesnt do that as there's only one thread
           that releases transaction's locks)
      */
      range_list->releasing_locks_ = true;
    }

    // Don't try to call release_locks() if the buffer is empty! if we are
    //  not holding any locks, the lock tree might be in the STO-mode with
    //  another transaction, and our attempt to release an empty set of locks
    //  will cause an assertion failure.
    for (auto it : range_list->buffers_) {
      if (it.second->get_num_ranges()) {
        toku::locktree* lt = get_locktree_by_cfid(it.first);
        lt->release_locks((TXNID)txn, it.second.get(), all_keys);

        it.second->destroy();
        it.second->create();

        toku::lock_request::retry_all_lock_requests(lt, nullptr);
      }
    }
    range_list->clear();  // TODO: need this?
    range_list->releasing_locks_ = false;
  }
}

int RangeTreeLockManager::compare_dbt_endpoints(void* arg, const DBT* a_key,
                                                const DBT* b_key) {
  const char* a = (const char*)a_key->data;
  const char* b = (const char*)b_key->data;

  size_t a_len = a_key->size;
  size_t b_len = b_key->size;

  size_t min_len = std::min(a_len, b_len);

  // Compare the values. The first byte encodes the endpoint type, its value
  // is either SUFFIX_INFIMUM or SUFFIX_SUPREMUM.
  Comparator* cmp = (Comparator*)arg;
  int res = cmp->Compare(Slice(a + 1, min_len - 1), Slice(b + 1, min_len - 1));
  if (!res) {
    if (b_len > min_len) {
      // a is shorter;
      if (a[0] == SUFFIX_INFIMUM)
        return -1;  //"a is smaller"
      else {
        // a is considered padded with 0xFF:FF:FF:FF...
        return 1;  // "a" is bigger
      }
    } else if (a_len > min_len) {
      // the opposite of the above: b is shorter.
      if (b[0] == SUFFIX_INFIMUM)
        return 1;  //"b is smaller"
      else {
        // b is considered padded with 0xFF:FF:FF:FF...
        return -1;  // "b" is bigger
      }
    } else {
      // the lengths are equal (and the key values, too)
      if (a[0] < b[0])
        return -1;
      else if (a[0] > b[0])
        return 1;
      else
        return 0;
    }
  } else
    return res;
}

namespace {
void UnrefLockTreeMapsCache(void* ptr) {
  // Called when a thread exits or a ThreadLocalPtr gets destroyed.
  auto lock_tree_map_cache =
      static_cast<std::unordered_map<uint32_t, locktree*>*>(ptr);
  delete lock_tree_map_cache;
}
}  // anonymous namespace

RangeTreeLockManager::RangeTreeLockManager(
    std::shared_ptr<TransactionDBMutexFactory> mutex_factory)
    : mutex_factory_(mutex_factory),
      ltree_lookup_cache_(new ThreadLocalPtr(&UnrefLockTreeMapsCache)),
      dlock_buffer_(10) {
  ltm_.create(on_create, on_destroy, on_escalate, NULL, mutex_factory_);
}

void RangeTreeLockManager::SetRangeDeadlockInfoBufferSize(
    uint32_t target_size) {
  dlock_buffer_.Resize(target_size);
}

void RangeTreeLockManager::Resize(uint32_t target_size) {
  SetRangeDeadlockInfoBufferSize(target_size);
}

std::vector<RangeDeadlockPath>
RangeTreeLockManager::GetRangeDeadlockInfoBuffer() {
  return dlock_buffer_.PrepareBuffer();
}

std::vector<DeadlockPath> RangeTreeLockManager::GetDeadlockInfoBuffer() {
  std::vector<DeadlockPath> res;
  std::vector<RangeDeadlockPath> data = GetRangeDeadlockInfoBuffer();
  // report left endpoints
  for (auto it = data.begin(); it != data.end(); ++it) {
    std::vector<DeadlockInfo> path;

    for (auto it2 = it->path.begin(); it2 != it->path.end(); ++it2) {
      path.push_back(
          {it2->m_txn_id, it2->m_cf_id, it2->m_exclusive, it2->m_start.slice});
    }
    res.push_back(DeadlockPath(path, it->deadlock_time));
  }
  return res;
}

/*
  @brief  Lock Escalation Callback function

  @param txnid   Transaction whose locks got escalated
  @param lt      Lock Tree where escalation is happening
  @param buffer  Escalation result: list of locks that this transaction now
                 owns in this lock tree.
  @param void*   Callback context
*/

void RangeTreeLockManager::on_escalate(TXNID txnid, const locktree* lt,
                                       const range_buffer& buffer, void*) {
  auto txn = (PessimisticTransaction*)txnid;

  RangeLockList* trx_locks =
      ((RangeTreeLockTracker*)&txn->GetTrackedLocks())->getList();

  MutexLock l(&trx_locks->mutex_);
  if (trx_locks->releasing_locks_) {
    /*
      Do nothing. The transaction is releasing its locks, so it will not care
      about having a correct list of ranges. (In TokuDB,
      toku_db_txn_escalate_callback() makes use of this property, too)
    */
    return;
  }

  uint32_t cf_id = (uint32_t)lt->get_dict_id().dictid;

  auto it = trx_locks->buffers_.find(cf_id);
  it->second->destroy();
  it->second->create();

  toku::range_buffer::iterator iter(&buffer);
  toku::range_buffer::iterator::record rec;
  while (iter.current(&rec)) {
    it->second->append(rec.get_left_key(), rec.get_right_key());
    iter.next();
  }
}

RangeTreeLockManager::~RangeTreeLockManager() {

  autovector<void*> local_caches;
  ltree_lookup_cache_->Scrape(&local_caches, nullptr);
  for (auto cache : local_caches) {
    delete static_cast<LockTreeMap*>(cache);
  }

  for (auto it : ltree_map_) {
    ltm_.release_lt(it.second);
  }
  ltm_.destroy();
}

RangeLockManagerHandle::Counters RangeTreeLockManager::GetStatus() {
  LTM_STATUS_S ltm_status_test;
  ltm_.get_status(&ltm_status_test);
  Counters res;

  // Searching status variable by its string name is how Toku's unit tests
  // do it (why didn't they make LTM_ESCALATION_COUNT constant visible?)
  // lookup keyname in status
  for (int i = 0; i < LTM_STATUS_S::LTM_STATUS_NUM_ROWS; i++) {
    TOKU_ENGINE_STATUS_ROW status = &ltm_status_test.status[i];
    if (strcmp(status->keyname, "LTM_ESCALATION_COUNT") == 0) {
      res.escalation_count = status->value.num;
      continue;
    }
    if (strcmp(status->keyname, "LTM_SIZE_CURRENT") == 0) {
      res.current_lock_memory = status->value.num;
    }
  }
  return res;
}

void RangeTreeLockManager::AddColumnFamily(const ColumnFamilyHandle* cfh) {
  uint32_t column_family_id = cfh->GetID();

  InstrumentedMutexLock l(&ltree_map_mutex_);
  if (ltree_map_.find(column_family_id) == ltree_map_.end()) {
    DICTIONARY_ID dict_id = {.dictid = column_family_id};
    toku::comparator cmp;
    cmp.create(compare_dbt_endpoints, (void*)cfh->GetComparator());
    toku::locktree* ltree = ltm_.get_lt(dict_id, cmp,
                                        /* on_create_extra*/ nullptr);
    // This is ok to because get_lt has copied the comparator:
    cmp.destroy();
    ltree_map_.emplace(column_family_id, ltree);
  } else {
    // column_family already exists in lock map
    // assert(false);
  }
}

void RangeTreeLockManager::RemoveColumnFamily(const ColumnFamilyHandle* cfh) {
  uint32_t column_family_id = cfh->GetID();
  // Remove lock_map for this column family.  Since the lock map is stored
  // as a shared ptr, concurrent transactions can still keep using it
  // until they release their references to it.

  // TODO^ if one can drop a column family while a transaction is holding a
  // lock in it, is column family's comparator guaranteed to survive until
  // all locks are released? (we depend on this).
  {
    InstrumentedMutexLock l(&ltree_map_mutex_);

    auto lock_maps_iter = ltree_map_.find(column_family_id);
    assert(lock_maps_iter != ltree_map_.end());

    ltm_.release_lt(lock_maps_iter->second);

    ltree_map_.erase(lock_maps_iter);
  }  // lock_map_mutex_

  // TODO: why do we delete first and clear the caches second? Shouldn't this be
  // done in the reverse order? (if we do it in the reverse order, how will we
  // prevent somebody from re-populating the cache?)

  // Clear all thread-local caches. We collect a vector of caches but we dont
  // really need them.
  autovector<void*> local_caches;
  ltree_lookup_cache_->Scrape(&local_caches, nullptr);
}

toku::locktree* RangeTreeLockManager::get_locktree_by_cfid(
    uint32_t column_family_id) {
  // First check thread-local cache
  if (ltree_lookup_cache_->Get() == nullptr) {
    ltree_lookup_cache_->Reset(new LockTreeMap());
  }

  auto ltree_map_cache = static_cast<LockTreeMap*>(ltree_lookup_cache_->Get());

  auto it = ltree_map_cache->find(column_family_id);
  if (it != ltree_map_cache->end()) {
    // Found lock map for this column family.
    return it->second;
  }

  // Not found in local cache, grab mutex and check shared LockMaps
  InstrumentedMutexLock l(&ltree_map_mutex_);

  it = ltree_map_.find(column_family_id);
  if (it == ltree_map_.end()) {
    return nullptr;
  } else {
    // Found lock map.  Store in thread-local cache and return.
    ltree_map_cache->insert({column_family_id, it->second});
    return it->second;
  }

  return nullptr;
}

struct LOCK_PRINT_CONTEXT {
  RangeLockManagerHandle::RangeLockStatus* data;  // Save locks here
  uint32_t cfh_id;  // Column Family whose tree we are traversing
};

// Report left endpoints of the acquired locks
LockManager::PointLockStatus RangeTreeLockManager::GetPointLockStatus() {
  PointLockStatus res;
  LockManager::RangeLockStatus data = GetRangeLockStatus();
  // report left endpoints
  for (auto it = data.begin(); it != data.end(); ++it) {
    auto& val = it->second;
    res.insert({it->first, {val.start.slice, val.ids, val.exclusive}});
  }
  return res;
}

static void push_into_lock_status_data(void* param, const DBT* left,
                                       const DBT* right, TXNID txnid_arg,
                                       bool is_shared, TxnidVector* owners) {
  struct LOCK_PRINT_CONTEXT* ctx = (LOCK_PRINT_CONTEXT*)param;
  struct RangeLockInfo info;

  info.exclusive = !is_shared;

  deserialize_endpoint(left, &info.start);
  deserialize_endpoint(right, &info.end);

  if (txnid_arg != TXNID_SHARED) {
    TXNID txnid = ((PessimisticTransaction*)txnid_arg)->GetID();
    info.ids.push_back(txnid);
  } else {
    for (auto it : *owners) {
      TXNID real_id = ((PessimisticTransaction*)it)->GetID();
      info.ids.push_back(real_id);
    }
  }
  ctx->data->insert({ctx->cfh_id, info});
}

LockManager::RangeLockStatus RangeTreeLockManager::GetRangeLockStatus() {
  LockManager::RangeLockStatus data;
  {
    InstrumentedMutexLock l(&ltree_map_mutex_);
    for (auto it : ltree_map_) {
      LOCK_PRINT_CONTEXT ctx = {&data, it.first};
      it.second->dump_locks((void*)&ctx, push_into_lock_status_data);
    }
  }
  return data;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // OS_WIN
#endif  // ROCKSDB_LITE
