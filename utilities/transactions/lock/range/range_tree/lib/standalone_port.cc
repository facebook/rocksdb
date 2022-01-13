#ifndef ROCKSDB_LITE
#ifndef OS_WIN
/*
  This is a dump ground to make Lock Tree work without the rest of TokuDB.
*/
#include <string.h>

#include "db.h"
#include "ft/ft-status.h"
#include "portability/memory.h"
#include "util/dbt.h"

// portability/os_malloc.cc

void toku_free(void *p) { free(p); }

void *toku_xmalloc(size_t size) { return malloc(size); }

void *toku_xrealloc(void *v, size_t size) { return realloc(v, size); }

void *toku_xmemdup(const void *v, size_t len) {
  void *p = toku_xmalloc(len);
  memcpy(p, v, len);
  return p;
}

// TODO: what are the X-functions? Xcalloc, Xrealloc?
void *toku_xcalloc(size_t nmemb, size_t size) { return calloc(nmemb, size); }

// ft-ft-opts.cc:

// locktree
toku_instr_key lock_request_m_wait_cond_key;
toku_instr_key manager_m_escalator_done_key;
toku_instr_key locktree_request_info_mutex_key;
toku_instr_key locktree_request_info_retry_mutex_key;
toku_instr_key locktree_request_info_retry_cv_key;

toku_instr_key treenode_mutex_key;
toku_instr_key manager_mutex_key;
toku_instr_key manager_escalation_mutex_key;
toku_instr_key manager_escalator_mutex_key;

// portability/memory.cc
size_t toku_memory_footprint(void *, size_t touched) { return touched; }

// ft/ft-status.c
// PORT2: note: the @c parameter to TOKUFT_STATUS_INIT must not start with
//   "TOKU"
LTM_STATUS_S ltm_status;
void LTM_STATUS_S::init() {
  if (m_initialized) return;
#define LTM_STATUS_INIT(k, c, t, l)                    \
  TOKUFT_STATUS_INIT((*this), k, c, t, "locktree: " l, \
                     TOKU_ENGINE_STATUS | TOKU_GLOBAL_STATUS)
  LTM_STATUS_INIT(LTM_SIZE_CURRENT, LOCKTREE_MEMORY_SIZE, STATUS_UINT64,
                  "memory size");
  LTM_STATUS_INIT(LTM_SIZE_LIMIT, LOCKTREE_MEMORY_SIZE_LIMIT, STATUS_UINT64,
                  "memory size limit");
  LTM_STATUS_INIT(LTM_ESCALATION_COUNT, LOCKTREE_ESCALATION_NUM, STATUS_UINT64,
                  "number of times lock escalation ran");
  LTM_STATUS_INIT(LTM_ESCALATION_TIME, LOCKTREE_ESCALATION_SECONDS,
                  STATUS_TOKUTIME, "time spent running escalation (seconds)");
  LTM_STATUS_INIT(LTM_ESCALATION_LATEST_RESULT,
                  LOCKTREE_LATEST_POST_ESCALATION_MEMORY_SIZE, STATUS_UINT64,
                  "latest post-escalation memory size");
  LTM_STATUS_INIT(LTM_NUM_LOCKTREES, LOCKTREE_OPEN_CURRENT, STATUS_UINT64,
                  "number of locktrees open now");
  LTM_STATUS_INIT(LTM_LOCK_REQUESTS_PENDING, LOCKTREE_PENDING_LOCK_REQUESTS,
                  STATUS_UINT64, "number of pending lock requests");
  LTM_STATUS_INIT(LTM_STO_NUM_ELIGIBLE, LOCKTREE_STO_ELIGIBLE_NUM,
                  STATUS_UINT64, "number of locktrees eligible for the STO");
  LTM_STATUS_INIT(LTM_STO_END_EARLY_COUNT, LOCKTREE_STO_ENDED_NUM,
                  STATUS_UINT64,
                  "number of times a locktree ended the STO early");
  LTM_STATUS_INIT(LTM_STO_END_EARLY_TIME, LOCKTREE_STO_ENDED_SECONDS,
                  STATUS_TOKUTIME, "time spent ending the STO early (seconds)");
  LTM_STATUS_INIT(LTM_WAIT_COUNT, LOCKTREE_WAIT_COUNT, STATUS_UINT64,
                  "number of wait locks");
  LTM_STATUS_INIT(LTM_WAIT_TIME, LOCKTREE_WAIT_TIME, STATUS_UINT64,
                  "time waiting for locks");
  LTM_STATUS_INIT(LTM_LONG_WAIT_COUNT, LOCKTREE_LONG_WAIT_COUNT, STATUS_UINT64,
                  "number of long wait locks");
  LTM_STATUS_INIT(LTM_LONG_WAIT_TIME, LOCKTREE_LONG_WAIT_TIME, STATUS_UINT64,
                  "long time waiting for locks");
  LTM_STATUS_INIT(LTM_TIMEOUT_COUNT, LOCKTREE_TIMEOUT_COUNT, STATUS_UINT64,
                  "number of lock timeouts");
  LTM_STATUS_INIT(LTM_WAIT_ESCALATION_COUNT, LOCKTREE_WAIT_ESCALATION_COUNT,
                  STATUS_UINT64, "number of waits on lock escalation");
  LTM_STATUS_INIT(LTM_WAIT_ESCALATION_TIME, LOCKTREE_WAIT_ESCALATION_TIME,
                  STATUS_UINT64, "time waiting on lock escalation");
  LTM_STATUS_INIT(LTM_LONG_WAIT_ESCALATION_COUNT,
                  LOCKTREE_LONG_WAIT_ESCALATION_COUNT, STATUS_UINT64,
                  "number of long waits on lock escalation");
  LTM_STATUS_INIT(LTM_LONG_WAIT_ESCALATION_TIME,
                  LOCKTREE_LONG_WAIT_ESCALATION_TIME, STATUS_UINT64,
                  "long time waiting on lock escalation");

  m_initialized = true;
#undef LTM_STATUS_INIT
}
void LTM_STATUS_S::destroy() {
  if (!m_initialized) return;
  for (int i = 0; i < LTM_STATUS_NUM_ROWS; ++i) {
    if (status[i].type == STATUS_PARCOUNT) {
      // PORT: TODO?? destroy_partitioned_counter(status[i].value.parcount);
    }
  }
}

int toku_keycompare(const void *key1, size_t key1len, const void *key2,
                    size_t key2len) {
  size_t comparelen = key1len < key2len ? key1len : key2len;
  int c = memcmp(key1, key2, comparelen);
  if (__builtin_expect(c != 0, 1)) {
    return c;
  } else {
    if (key1len < key2len) {
      return -1;
    } else if (key1len > key2len) {
      return 1;
    } else {
      return 0;
    }
  }
}

int toku_builtin_compare_fun(const DBT *a, const DBT *b) {
  return toku_keycompare(a->data, a->size, b->data, b->size);
}
#endif  // OS_WIN
#endif  // ROCKSDB_LITE
