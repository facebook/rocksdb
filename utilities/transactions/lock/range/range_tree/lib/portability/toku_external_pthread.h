/*
  A wrapper around rocksdb::TransactionDBMutexFactory-provided condition and
  mutex that provides toku_pthread_*-like interface. The functions are named

    toku_external_{mutex|cond}_XXX

  Lock Tree uses this mutex and condition for interruptible (long) lock waits.

  (It also still uses toku_pthread_XXX calls for mutexes/conditions for
   shorter waits on internal objects)
*/

#pragma once

#include <pthread.h>
#include <stdint.h>
#include <time.h>

#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/transaction_db_mutex.h"
#include "toku_portability.h"

using ROCKSDB_NAMESPACE::TransactionDBCondVar;
using ROCKSDB_NAMESPACE::TransactionDBMutex;

typedef std::shared_ptr<ROCKSDB_NAMESPACE::TransactionDBMutexFactory>
    toku_external_mutex_factory_t;

typedef std::shared_ptr<TransactionDBMutex> toku_external_mutex_t;
typedef std::shared_ptr<TransactionDBCondVar> toku_external_cond_t;

static inline void toku_external_cond_init(
    toku_external_mutex_factory_t mutex_factory, toku_external_cond_t *cond) {
  *cond = mutex_factory->AllocateCondVar();
}

inline void toku_external_cond_destroy(toku_external_cond_t *cond) {
  cond->reset();  // this will destroy the managed object
}

inline void toku_external_cond_signal(toku_external_cond_t *cond) {
  (*cond)->Notify();
}

inline void toku_external_cond_broadcast(toku_external_cond_t *cond) {
  (*cond)->NotifyAll();
}

inline int toku_external_cond_timedwait(toku_external_cond_t *cond,
                                        toku_external_mutex_t *mutex,
                                        int64_t timeout_microsec) {
  auto res = (*cond)->WaitFor(*mutex, timeout_microsec);
  if (res.ok())
    return 0;
  else
    return ETIMEDOUT;
}

inline void toku_external_mutex_init(toku_external_mutex_factory_t factory,
                                     toku_external_mutex_t *mutex) {
  // Use placement new: the memory has been allocated but constructor wasn't
  // called
  new (mutex) toku_external_mutex_t;
  *mutex = factory->AllocateMutex();
}

inline void toku_external_mutex_lock(toku_external_mutex_t *mutex) {
  (*mutex)->Lock();
}

inline int toku_external_mutex_trylock(toku_external_mutex_t *mutex) {
  (*mutex)->Lock();
  return 0;
}

inline void toku_external_mutex_unlock(toku_external_mutex_t *mutex) {
  (*mutex)->UnLock();
}

inline void toku_external_mutex_destroy(toku_external_mutex_t *mutex) {
  mutex->reset();  // this will destroy the managed object
}
