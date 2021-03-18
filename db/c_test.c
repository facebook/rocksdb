/* Copyright (c) 2011 The LevelDB Authors. All rights reserved.
   Use of this source code is governed by a BSD-style license that can be
   found in the LICENSE file. See the AUTHORS file for names of contributors. */
// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

#include <stdio.h>

#ifndef ROCKSDB_LITE  // Lite does not support C API

#include "rocksdb/c.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#ifndef OS_WIN
#include <unistd.h>
#endif
#include <inttypes.h>

// Can not use port/port.h macros as this is a c file
#ifdef OS_WIN
#include <windows.h>

// Ok for uniqueness
int geteuid() {
  int result = 0;

  result = ((int)GetCurrentProcessId() << 16);
  result |= (int)GetCurrentThreadId();

  return result;
}

// VS < 2015
#if defined(_MSC_VER) && (_MSC_VER < 1900)
#define snprintf _snprintf
#endif

#endif

const char* phase = "";
static char dbname[200];
static char sstfilename[200];
static char dbbackupname[200];
static char dbcheckpointname[200];
static char dbpathname[200];
static char secondary_path[200];

static void StartPhase(const char* name) {
  fprintf(stderr, "=== Test %s\n", name);
  phase = name;
}
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning (disable: 4996) // getenv security warning
#endif
static const char* GetTempDir(void) {
    const char* ret = getenv("TEST_TMPDIR");
    if (ret == NULL || ret[0] == '\0')
#ifdef OS_WIN
      ret = getenv("TEMP");
#else
      ret = "/tmp";
#endif
    return ret;
}
#ifdef _MSC_VER
#pragma warning(pop)
#endif

#define CheckNoError(err)                                               \
  if ((err) != NULL) {                                                  \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, phase, (err)); \
    abort();                                                            \
  }

#define CheckCondition(cond)                                            \
  if (!(cond)) {                                                        \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, phase, #cond); \
    abort();                                                            \
  }

static void CheckEqual(const char* expected, const char* v, size_t n) {
  if (expected == NULL && v == NULL) {
    // ok
  } else if (expected != NULL && v != NULL && n == strlen(expected) &&
             memcmp(expected, v, n) == 0) {
    // ok
    return;
  } else {
    fprintf(stderr, "%s: expected '%s', got '%s'\n",
            phase,
            (expected ? expected : "(null)"),
            (v ? v : "(null"));
    abort();
  }
}

static void Free(char** ptr) {
  if (*ptr) {
    free(*ptr);
    *ptr = NULL;
  }
}

static void CheckValue(
    char* err,
    const char* expected,
    char** actual,
    size_t actual_length) {
  CheckNoError(err);
  CheckEqual(expected, *actual, actual_length);
  Free(actual);
}

static void CheckGet(
    rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    const char* key,
    const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = rocksdb_get(db, options, key, strlen(key), &val_len, &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

static void CheckGetCF(
    rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* handle,
    const char* key,
    const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = rocksdb_get_cf(db, options, handle, key, strlen(key), &val_len, &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

static void CheckPinGet(rocksdb_t* db, const rocksdb_readoptions_t* options,
                        const char* key, const char* expected) {
  char* err = NULL;
  size_t val_len;
  const char* val;
  rocksdb_pinnableslice_t* p;
  p = rocksdb_get_pinned(db, options, key, strlen(key), &err);
  CheckNoError(err);
  val = rocksdb_pinnableslice_value(p, &val_len);
  CheckEqual(expected, val, val_len);
  rocksdb_pinnableslice_destroy(p);
}

static void CheckPinGetCF(rocksdb_t* db, const rocksdb_readoptions_t* options,
                          rocksdb_column_family_handle_t* handle,
                          const char* key, const char* expected) {
  char* err = NULL;
  size_t val_len;
  const char* val;
  rocksdb_pinnableslice_t* p;
  p = rocksdb_get_pinned_cf(db, options, handle, key, strlen(key), &err);
  CheckNoError(err);
  val = rocksdb_pinnableslice_value(p, &val_len);
  CheckEqual(expected, val, val_len);
  rocksdb_pinnableslice_destroy(p);
}

static void CheckIter(rocksdb_iterator_t* iter,
                      const char* key, const char* val) {
  size_t len;
  const char* str;
  str = rocksdb_iter_key(iter, &len);
  CheckEqual(key, str, len);
  str = rocksdb_iter_value(iter, &len);
  CheckEqual(val, str, len);
}

// Callback from rocksdb_writebatch_iterate()
static void CheckPut(void* ptr,
                     const char* k, size_t klen,
                     const char* v, size_t vlen) {
  int* state = (int*) ptr;
  CheckCondition(*state < 2);
  switch (*state) {
    case 0:
      CheckEqual("bar", k, klen);
      CheckEqual("b", v, vlen);
      break;
    case 1:
      CheckEqual("box", k, klen);
      CheckEqual("c", v, vlen);
      break;
  }
  (*state)++;
}

// Callback from rocksdb_writebatch_iterate()
static void CheckDel(void* ptr, const char* k, size_t klen) {
  int* state = (int*) ptr;
  CheckCondition(*state == 2);
  CheckEqual("bar", k, klen);
  (*state)++;
}

static void CmpDestroy(void* arg) { (void)arg; }

static int CmpCompare(void* arg, const char* a, size_t alen,
                      const char* b, size_t blen) {
  (void)arg;
  size_t n = (alen < blen) ? alen : blen;
  int r = memcmp(a, b, n);
  if (r == 0) {
    if (alen < blen) r = -1;
    else if (alen > blen) r = +1;
  }
  return r;
}

static const char* CmpName(void* arg) {
  (void)arg;
  return "foo";
}

// Custom filter policy
static unsigned char fake_filter_result = 1;
static void FilterDestroy(void* arg) { (void)arg; }
static const char* FilterName(void* arg) {
  (void)arg;
  return "TestFilter";
}
static char* FilterCreate(
    void* arg,
    const char* const* key_array, const size_t* key_length_array,
    int num_keys,
    size_t* filter_length) {
  (void)arg;
  (void)key_array;
  (void)key_length_array;
  (void)num_keys;
  *filter_length = 4;
  char* result = malloc(4);
  memcpy(result, "fake", 4);
  return result;
}
static unsigned char FilterKeyMatch(
    void* arg,
    const char* key, size_t length,
    const char* filter, size_t filter_length) {
  (void)arg;
  (void)key;
  (void)length;
  CheckCondition(filter_length == 4);
  CheckCondition(memcmp(filter, "fake", 4) == 0);
  return fake_filter_result;
}

// Custom compaction filter
static void CFilterDestroy(void* arg) { (void)arg; }
static const char* CFilterName(void* arg) {
  (void)arg;
  return "foo";
}
static unsigned char CFilterFilter(void* arg, int level, const char* key,
                                   size_t key_length,
                                   const char* existing_value,
                                   size_t value_length, char** new_value,
                                   size_t* new_value_length,
                                   unsigned char* value_changed) {
  (void)arg;
  (void)level;
  (void)existing_value;
  (void)value_length;
  if (key_length == 3) {
    if (memcmp(key, "bar", key_length) == 0) {
      return 1;
    } else if (memcmp(key, "baz", key_length) == 0) {
      *value_changed = 1;
      *new_value = "newbazvalue";
      *new_value_length = 11;
      return 0;
    }
  }
  return 0;
}

static void CFilterFactoryDestroy(void* arg) { (void)arg; }
static const char* CFilterFactoryName(void* arg) {
  (void)arg;
  return "foo";
}
static rocksdb_compactionfilter_t* CFilterCreate(
    void* arg, rocksdb_compactionfiltercontext_t* context) {
  (void)arg;
  (void)context;
  return rocksdb_compactionfilter_create(NULL, CFilterDestroy, CFilterFilter,
                                         CFilterName);
}

static rocksdb_t* CheckCompaction(rocksdb_t* db, rocksdb_options_t* options,
                                  rocksdb_readoptions_t* roptions,
                                  rocksdb_writeoptions_t* woptions) {
  char* err = NULL;
  db = rocksdb_open(options, dbname, &err);
  CheckNoError(err);
  rocksdb_put(db, woptions, "foo", 3, "foovalue", 8, &err);
  CheckNoError(err);
  CheckGet(db, roptions, "foo", "foovalue");
  rocksdb_put(db, woptions, "bar", 3, "barvalue", 8, &err);
  CheckNoError(err);
  CheckGet(db, roptions, "bar", "barvalue");
  rocksdb_put(db, woptions, "baz", 3, "bazvalue", 8, &err);
  CheckNoError(err);
  CheckGet(db, roptions, "baz", "bazvalue");

  // Force compaction
  rocksdb_compact_range(db, NULL, 0, NULL, 0);
  // should have filtered bar, but not foo
  CheckGet(db, roptions, "foo", "foovalue");
  CheckGet(db, roptions, "bar", NULL);
  CheckGet(db, roptions, "baz", "newbazvalue");
  return db;
}

// Custom merge operator
static void MergeOperatorDestroy(void* arg) { (void)arg; }
static const char* MergeOperatorName(void* arg) {
  (void)arg;
  return "TestMergeOperator";
}
static char* MergeOperatorFullMerge(
    void* arg,
    const char* key, size_t key_length,
    const char* existing_value, size_t existing_value_length,
    const char* const* operands_list, const size_t* operands_list_length,
    int num_operands,
    unsigned char* success, size_t* new_value_length) {
  (void)arg;
  (void)key;
  (void)key_length;
  (void)existing_value;
  (void)existing_value_length;
  (void)operands_list;
  (void)operands_list_length;
  (void)num_operands;
  *new_value_length = 4;
  *success = 1;
  char* result = malloc(4);
  memcpy(result, "fake", 4);
  return result;
}
static char* MergeOperatorPartialMerge(
    void* arg,
    const char* key, size_t key_length,
    const char* const* operands_list, const size_t* operands_list_length,
    int num_operands,
    unsigned char* success, size_t* new_value_length) {
  (void)arg;
  (void)key;
  (void)key_length;
  (void)operands_list;
  (void)operands_list_length;
  (void)num_operands;
  *new_value_length = 4;
  *success = 1;
  char* result = malloc(4);
  memcpy(result, "fake", 4);
  return result;
}

static void CheckTxnGet(
        rocksdb_transaction_t* txn,
        const rocksdb_readoptions_t* options,
        const char* key,
        const char* expected) {
        char* err = NULL;
        size_t val_len;
        char* val;
        val = rocksdb_transaction_get(txn, options, key, strlen(key), &val_len, &err);
        CheckNoError(err);
        CheckEqual(expected, val, val_len);
        Free(&val);
}

static void CheckTxnGetCF(rocksdb_transaction_t* txn,
                          const rocksdb_readoptions_t* options,
                          rocksdb_column_family_handle_t* column_family,
                          const char* key, const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = rocksdb_transaction_get_cf(txn, options, column_family, key,
                                   strlen(key), &val_len, &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

static void CheckTxnDBGet(
        rocksdb_transactiondb_t* txn_db,
        const rocksdb_readoptions_t* options,
        const char* key,
        const char* expected) {
        char* err = NULL;
        size_t val_len;
        char* val;
        val = rocksdb_transactiondb_get(txn_db, options, key, strlen(key), &val_len, &err);
        CheckNoError(err);
        CheckEqual(expected, val, val_len);
        Free(&val);
}

static void CheckTxnDBGetCF(rocksdb_transactiondb_t* txn_db,
                            const rocksdb_readoptions_t* options,
                            rocksdb_column_family_handle_t* column_family,
                            const char* key, const char* expected) {
  char* err = NULL;
  size_t val_len;
  char* val;
  val = rocksdb_transactiondb_get_cf(txn_db, options, column_family, key,
                                     strlen(key), &val_len, &err);
  CheckNoError(err);
  CheckEqual(expected, val, val_len);
  Free(&val);
}

int main(int argc, char** argv) {
  (void)argc;
  (void)argv;
  rocksdb_t* db;
  rocksdb_comparator_t* cmp;
  rocksdb_cache_t* cache;
  rocksdb_dbpath_t *dbpath;
  rocksdb_env_t* env;
  rocksdb_options_t* options;
  rocksdb_compactoptions_t* coptions;
  rocksdb_block_based_table_options_t* table_options;
  rocksdb_readoptions_t* roptions;
  rocksdb_writeoptions_t* woptions;
  rocksdb_ratelimiter_t* rate_limiter;
  rocksdb_transactiondb_t* txn_db;
  rocksdb_transactiondb_options_t* txn_db_options;
  rocksdb_transaction_t* txn;
  rocksdb_transaction_options_t* txn_options;
  rocksdb_optimistictransactiondb_t* otxn_db;
  rocksdb_optimistictransaction_options_t* otxn_options;
  char* err = NULL;
  int run = -1;

  snprintf(dbname, sizeof(dbname),
           "%s/rocksdb_c_test-%d",
           GetTempDir(),
           ((int) geteuid()));

  snprintf(dbbackupname, sizeof(dbbackupname),
           "%s/rocksdb_c_test-%d-backup",
           GetTempDir(),
           ((int) geteuid()));

  snprintf(dbcheckpointname, sizeof(dbcheckpointname),
           "%s/rocksdb_c_test-%d-checkpoint",
           GetTempDir(),
           ((int) geteuid()));

  snprintf(sstfilename, sizeof(sstfilename),
           "%s/rocksdb_c_test-%d-sst",
           GetTempDir(),
           ((int)geteuid()));

  snprintf(dbpathname, sizeof(dbpathname),
           "%s/rocksdb_c_test-%d-dbpath",
           GetTempDir(),
           ((int) geteuid()));

  StartPhase("create_objects");
  cmp = rocksdb_comparator_create(NULL, CmpDestroy, CmpCompare, CmpName);
  dbpath = rocksdb_dbpath_create(dbpathname, 1024 * 1024);
  env = rocksdb_create_default_env();
  cache = rocksdb_cache_create_lru(100000);

  options = rocksdb_options_create();
  rocksdb_options_set_comparator(options, cmp);
  rocksdb_options_set_error_if_exists(options, 1);
  rocksdb_options_set_env(options, env);
  rocksdb_options_set_info_log(options, NULL);
  rocksdb_options_set_write_buffer_size(options, 100000);
  rocksdb_options_set_paranoid_checks(options, 1);
  rocksdb_options_set_max_open_files(options, 10);
  rocksdb_options_set_base_background_compactions(options, 1);

  table_options = rocksdb_block_based_options_create();
  rocksdb_block_based_options_set_block_cache(table_options, cache);
  rocksdb_block_based_options_set_data_block_index_type(table_options, 1);
  rocksdb_block_based_options_set_data_block_hash_ratio(table_options, 0.75);
  rocksdb_options_set_block_based_table_factory(options, table_options);

  rocksdb_options_set_compression(options, rocksdb_no_compression);
  rocksdb_options_set_compression_options(options, -14, -1, 0, 0);
  int compression_levels[] = {rocksdb_no_compression, rocksdb_no_compression,
                              rocksdb_no_compression, rocksdb_no_compression};
  rocksdb_options_set_compression_per_level(options, compression_levels, 4);
  rate_limiter = rocksdb_ratelimiter_create(1000 * 1024 * 1024, 100 * 1000, 10);
  rocksdb_options_set_ratelimiter(options, rate_limiter);
  rocksdb_ratelimiter_destroy(rate_limiter);

  roptions = rocksdb_readoptions_create();
  rocksdb_readoptions_set_verify_checksums(roptions, 1);
  rocksdb_readoptions_set_fill_cache(roptions, 1);

  woptions = rocksdb_writeoptions_create();
  rocksdb_writeoptions_set_sync(woptions, 1);

  coptions = rocksdb_compactoptions_create();
  rocksdb_compactoptions_set_exclusive_manual_compaction(coptions, 1);

  StartPhase("destroy");
  rocksdb_destroy_db(options, dbname, &err);
  Free(&err);

  StartPhase("open_error");
  rocksdb_open(options, dbname, &err);
  CheckCondition(err != NULL);
  Free(&err);

  StartPhase("open");
  rocksdb_options_set_create_if_missing(options, 1);
  db = rocksdb_open(options, dbname, &err);
  CheckNoError(err);
  CheckGet(db, roptions, "foo", NULL);

  StartPhase("put");
  rocksdb_put(db, woptions, "foo", 3, "hello", 5, &err);
  CheckNoError(err);
  CheckGet(db, roptions, "foo", "hello");

  StartPhase("backup_and_restore");
  {
    rocksdb_destroy_db(options, dbbackupname, &err);
    CheckNoError(err);

    rocksdb_backup_engine_t *be = rocksdb_backup_engine_open(options, dbbackupname, &err);
    CheckNoError(err);

    rocksdb_backup_engine_create_new_backup(be, db, &err);
    CheckNoError(err);

    // need a change to trigger a new backup
    rocksdb_delete(db, woptions, "does-not-exist", 14, &err);
    CheckNoError(err);

    rocksdb_backup_engine_create_new_backup(be, db, &err);
    CheckNoError(err);

    const rocksdb_backup_engine_info_t* bei = rocksdb_backup_engine_get_backup_info(be);
    CheckCondition(rocksdb_backup_engine_info_count(bei) > 1);
    rocksdb_backup_engine_info_destroy(bei);

    rocksdb_backup_engine_purge_old_backups(be, 1, &err);
    CheckNoError(err);

    bei = rocksdb_backup_engine_get_backup_info(be);
    CheckCondition(rocksdb_backup_engine_info_count(bei) == 1);
    rocksdb_backup_engine_info_destroy(bei);

    rocksdb_delete(db, woptions, "foo", 3, &err);
    CheckNoError(err);

    rocksdb_close(db);

    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);

    rocksdb_restore_options_t *restore_options = rocksdb_restore_options_create();
    rocksdb_restore_options_set_keep_log_files(restore_options, 0);
    rocksdb_backup_engine_restore_db_from_latest_backup(be, dbname, dbname, restore_options, &err);
    CheckNoError(err);
    rocksdb_restore_options_destroy(restore_options);

    rocksdb_options_set_error_if_exists(options, 0);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
    rocksdb_options_set_error_if_exists(options, 1);

    CheckGet(db, roptions, "foo", "hello");

    rocksdb_backup_engine_close(be);
  }

  StartPhase("checkpoint");
  {
    rocksdb_destroy_db(options, dbcheckpointname, &err);
    CheckNoError(err);

    rocksdb_checkpoint_t* checkpoint = rocksdb_checkpoint_object_create(db, &err);
    CheckNoError(err);

    rocksdb_checkpoint_create(checkpoint, dbcheckpointname, 0, &err);
    CheckNoError(err);

    // start a new database from the checkpoint
    rocksdb_close(db);
    rocksdb_options_set_error_if_exists(options, 0);
    db = rocksdb_open(options, dbcheckpointname, &err);
    CheckNoError(err);

    CheckGet(db, roptions, "foo", "hello");

    rocksdb_checkpoint_object_destroy(checkpoint);

    rocksdb_close(db);
    rocksdb_destroy_db(options, dbcheckpointname, &err);
    CheckNoError(err);

    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
    rocksdb_options_set_error_if_exists(options, 1);
  }

  StartPhase("compactall");
  rocksdb_compact_range(db, NULL, 0, NULL, 0);
  CheckGet(db, roptions, "foo", "hello");

  StartPhase("compactrange");
  rocksdb_compact_range(db, "a", 1, "z", 1);
  CheckGet(db, roptions, "foo", "hello");

  StartPhase("compactallopt");
  rocksdb_compact_range_opt(db, coptions, NULL, 0, NULL, 0);
  CheckGet(db, roptions, "foo", "hello");

  StartPhase("compactrangeopt");
  rocksdb_compact_range_opt(db, coptions, "a", 1, "z", 1);
  CheckGet(db, roptions, "foo", "hello");

  // Simple check cache usage
  StartPhase("cache_usage");
  {
    rocksdb_readoptions_set_pin_data(roptions, 1);
    rocksdb_iterator_t* iter = rocksdb_create_iterator(db, roptions);
    rocksdb_iter_seek(iter, "foo", 3);

    size_t usage = rocksdb_cache_get_usage(cache);
    CheckCondition(usage > 0);

    size_t pin_usage = rocksdb_cache_get_pinned_usage(cache);
    CheckCondition(pin_usage > 0);

    rocksdb_iter_next(iter);
    rocksdb_iter_destroy(iter);
    rocksdb_readoptions_set_pin_data(roptions, 0);
  }

  StartPhase("addfile");
  {
    rocksdb_envoptions_t* env_opt = rocksdb_envoptions_create();
    rocksdb_options_t* io_options = rocksdb_options_create();
    rocksdb_sstfilewriter_t* writer =
        rocksdb_sstfilewriter_create(env_opt, io_options);

    remove(sstfilename);
    rocksdb_sstfilewriter_open(writer, sstfilename, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "sstk1", 5, "v1", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "sstk2", 5, "v2", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "sstk3", 5, "v3", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_finish(writer, &err);
    CheckNoError(err);

    rocksdb_ingestexternalfileoptions_t* ing_opt =
        rocksdb_ingestexternalfileoptions_create();
    const char* file_list[1] = {sstfilename};
    rocksdb_ingest_external_file(db, file_list, 1, ing_opt, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "sstk1", "v1");
    CheckGet(db, roptions, "sstk2", "v2");
    CheckGet(db, roptions, "sstk3", "v3");

    remove(sstfilename);
    rocksdb_sstfilewriter_open(writer, sstfilename, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "sstk2", 5, "v4", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "sstk22", 6, "v5", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_put(writer, "sstk3", 5, "v6", 2, &err);
    CheckNoError(err);
    rocksdb_sstfilewriter_finish(writer, &err);
    CheckNoError(err);

    rocksdb_ingest_external_file(db, file_list, 1, ing_opt, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "sstk1", "v1");
    CheckGet(db, roptions, "sstk2", "v4");
    CheckGet(db, roptions, "sstk22", "v5");
    CheckGet(db, roptions, "sstk3", "v6");

    rocksdb_ingestexternalfileoptions_destroy(ing_opt);
    rocksdb_sstfilewriter_destroy(writer);
    rocksdb_options_destroy(io_options);
    rocksdb_envoptions_destroy(env_opt);

    // Delete all keys we just ingested
    rocksdb_delete(db, woptions, "sstk1", 5, &err);
    CheckNoError(err);
    rocksdb_delete(db, woptions, "sstk2", 5, &err);
    CheckNoError(err);
    rocksdb_delete(db, woptions, "sstk22", 6, &err);
    CheckNoError(err);
    rocksdb_delete(db, woptions, "sstk3", 5, &err);
    CheckNoError(err);
  }

  StartPhase("writebatch");
  {
    rocksdb_writebatch_t* wb = rocksdb_writebatch_create();
    rocksdb_writebatch_put(wb, "foo", 3, "a", 1);
    rocksdb_writebatch_clear(wb);
    rocksdb_writebatch_put(wb, "bar", 3, "b", 1);
    rocksdb_writebatch_put(wb, "box", 3, "c", 1);
    rocksdb_writebatch_delete(wb, "bar", 3);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "foo", "hello");
    CheckGet(db, roptions, "bar", NULL);
    CheckGet(db, roptions, "box", "c");
    int pos = 0;
    rocksdb_writebatch_iterate(wb, &pos, CheckPut, CheckDel);
    CheckCondition(pos == 3);
    rocksdb_writebatch_clear(wb);
    rocksdb_writebatch_put(wb, "bar", 3, "b", 1);
    rocksdb_writebatch_put(wb, "bay", 3, "d", 1);
    rocksdb_writebatch_delete_range(wb, "bar", 3, "bay", 3);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "bar", NULL);
    CheckGet(db, roptions, "bay", "d");
    rocksdb_writebatch_clear(wb);
    const char* start_list[1] = {"bay"};
    const size_t start_sizes[1] = {3};
    const char* end_list[1] = {"baz"};
    const size_t end_sizes[1] = {3};
    rocksdb_writebatch_delete_rangev(wb, 1, start_list, start_sizes, end_list,
                                     end_sizes);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "bay", NULL);
    rocksdb_writebatch_destroy(wb);
  }

  StartPhase("writebatch_vectors");
  {
    rocksdb_writebatch_t* wb = rocksdb_writebatch_create();
    const char* k_list[2] = { "z", "ap" };
    const size_t k_sizes[2] = { 1, 2 };
    const char* v_list[3] = { "x", "y", "z" };
    const size_t v_sizes[3] = { 1, 1, 1 };
    rocksdb_writebatch_putv(wb, 2, k_list, k_sizes, 3, v_list, v_sizes);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "zap", "xyz");
    rocksdb_writebatch_delete(wb, "zap", 3);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "zap", NULL);
    rocksdb_writebatch_destroy(wb);
  }

  StartPhase("writebatch_savepoint");
  {
    rocksdb_writebatch_t* wb = rocksdb_writebatch_create();
    rocksdb_writebatch_set_save_point(wb);
    rocksdb_writebatch_set_save_point(wb);
    const char* k_list[2] = {"z", "ap"};
    const size_t k_sizes[2] = {1, 2};
    const char* v_list[3] = {"x", "y", "z"};
    const size_t v_sizes[3] = {1, 1, 1};
    rocksdb_writebatch_pop_save_point(wb, &err);
    CheckNoError(err);
    rocksdb_writebatch_putv(wb, 2, k_list, k_sizes, 3, v_list, v_sizes);
    rocksdb_writebatch_rollback_to_save_point(wb, &err);
    CheckNoError(err);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "zap", NULL);
    rocksdb_writebatch_destroy(wb);
  }

  StartPhase("writebatch_rep");
  {
    rocksdb_writebatch_t* wb1 = rocksdb_writebatch_create();
    rocksdb_writebatch_put(wb1, "baz", 3, "d", 1);
    rocksdb_writebatch_put(wb1, "quux", 4, "e", 1);
    rocksdb_writebatch_delete(wb1, "quux", 4);
    size_t repsize1 = 0;
    const char* rep = rocksdb_writebatch_data(wb1, &repsize1);
    rocksdb_writebatch_t* wb2 = rocksdb_writebatch_create_from(rep, repsize1);
    CheckCondition(rocksdb_writebatch_count(wb1) ==
                   rocksdb_writebatch_count(wb2));
    size_t repsize2 = 0;
    CheckCondition(
        memcmp(rep, rocksdb_writebatch_data(wb2, &repsize2), repsize1) == 0);
    rocksdb_writebatch_destroy(wb1);
    rocksdb_writebatch_destroy(wb2);
  }

  StartPhase("writebatch_wi");
  {
    rocksdb_writebatch_wi_t* wbi = rocksdb_writebatch_wi_create(0, 1);
    rocksdb_writebatch_wi_put(wbi, "foo", 3, "a", 1);
    rocksdb_writebatch_wi_clear(wbi);
    rocksdb_writebatch_wi_put(wbi, "bar", 3, "b", 1);
    rocksdb_writebatch_wi_put(wbi, "box", 3, "c", 1);
    rocksdb_writebatch_wi_delete(wbi, "bar", 3);
    int count = rocksdb_writebatch_wi_count(wbi);
    CheckCondition(count == 3);
    size_t size;
    char* value;
    value = rocksdb_writebatch_wi_get_from_batch(wbi, options, "box", 3, &size, &err);
    CheckValue(err, "c", &value, size);
    value = rocksdb_writebatch_wi_get_from_batch(wbi, options, "bar", 3, &size, &err);
    CheckValue(err, NULL, &value, size);
    value = rocksdb_writebatch_wi_get_from_batch_and_db(wbi, db, roptions, "foo", 3, &size, &err);
    CheckValue(err, "hello", &value, size);
    value = rocksdb_writebatch_wi_get_from_batch_and_db(wbi, db, roptions, "box", 3, &size, &err);
    CheckValue(err, "c", &value, size);
    rocksdb_write_writebatch_wi(db, woptions, wbi, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "foo", "hello");
    CheckGet(db, roptions, "bar", NULL);
    CheckGet(db, roptions, "box", "c");
    int pos = 0;
    rocksdb_writebatch_wi_iterate(wbi, &pos, CheckPut, CheckDel);
    CheckCondition(pos == 3);
    rocksdb_writebatch_wi_clear(wbi);
    rocksdb_writebatch_wi_destroy(wbi);
  }

  StartPhase("writebatch_wi_vectors");
  {
    rocksdb_writebatch_wi_t* wb = rocksdb_writebatch_wi_create(0, 1);
    const char* k_list[2] = { "z", "ap" };
    const size_t k_sizes[2] = { 1, 2 };
    const char* v_list[3] = { "x", "y", "z" };
    const size_t v_sizes[3] = { 1, 1, 1 };
    rocksdb_writebatch_wi_putv(wb, 2, k_list, k_sizes, 3, v_list, v_sizes);
    rocksdb_write_writebatch_wi(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "zap", "xyz");
    rocksdb_writebatch_wi_delete(wb, "zap", 3);
    rocksdb_write_writebatch_wi(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "zap", NULL);
    rocksdb_writebatch_wi_destroy(wb);
  }

  StartPhase("writebatch_wi_savepoint");
  {
    rocksdb_writebatch_wi_t* wb = rocksdb_writebatch_wi_create(0, 1);
    rocksdb_writebatch_wi_set_save_point(wb);
    const char* k_list[2] = {"z", "ap"};
    const size_t k_sizes[2] = {1, 2};
    const char* v_list[3] = {"x", "y", "z"};
    const size_t v_sizes[3] = {1, 1, 1};
    rocksdb_writebatch_wi_putv(wb, 2, k_list, k_sizes, 3, v_list, v_sizes);
    rocksdb_writebatch_wi_rollback_to_save_point(wb, &err);
    CheckNoError(err);
    rocksdb_write_writebatch_wi(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "zap", NULL);
    rocksdb_writebatch_wi_destroy(wb);
  }

  StartPhase("iter");
  {
    rocksdb_iterator_t* iter = rocksdb_create_iterator(db, roptions);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "box", "c");
    rocksdb_iter_next(iter);
    CheckIter(iter, "foo", "hello");
    rocksdb_iter_prev(iter);
    CheckIter(iter, "box", "c");
    rocksdb_iter_prev(iter);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_last(iter);
    CheckIter(iter, "foo", "hello");
    rocksdb_iter_seek(iter, "b", 1);
    CheckIter(iter, "box", "c");
    rocksdb_iter_seek_for_prev(iter, "g", 1);
    CheckIter(iter, "foo", "hello");
    rocksdb_iter_seek_for_prev(iter, "box", 3);
    CheckIter(iter, "box", "c");
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);
  }

  StartPhase("wbwi_iter");
  {
    rocksdb_iterator_t* base_iter = rocksdb_create_iterator(db, roptions);
    rocksdb_writebatch_wi_t* wbi = rocksdb_writebatch_wi_create(0, 1);
    rocksdb_writebatch_wi_put(wbi, "bar", 3, "b", 1);
    rocksdb_writebatch_wi_delete(wbi, "foo", 3);
    rocksdb_iterator_t* iter =
        rocksdb_writebatch_wi_create_iterator_with_base(wbi, base_iter);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "bar", "b");
    rocksdb_iter_next(iter);
    CheckIter(iter, "box", "c");
    rocksdb_iter_prev(iter);
    CheckIter(iter, "bar", "b");
    rocksdb_iter_prev(iter);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_last(iter);
    CheckIter(iter, "box", "c");
    rocksdb_iter_seek(iter, "b", 1);
    CheckIter(iter, "bar", "b");
    rocksdb_iter_seek_for_prev(iter, "c", 1);
    CheckIter(iter, "box", "c");
    rocksdb_iter_seek_for_prev(iter, "box", 3);
    CheckIter(iter, "box", "c");
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);
    rocksdb_writebatch_wi_destroy(wbi);
  }

  StartPhase("multiget");
  {
    const char* keys[3] = { "box", "foo", "notfound" };
    const size_t keys_sizes[3] = { 3, 3, 8 };
    char* vals[3];
    size_t vals_sizes[3];
    char* errs[3];
    rocksdb_multi_get(db, roptions, 3, keys, keys_sizes, vals, vals_sizes, errs);

    int i;
    for (i = 0; i < 3; i++) {
      CheckEqual(NULL, errs[i], 0);
      switch (i) {
      case 0:
        CheckEqual("c", vals[i], vals_sizes[i]);
        break;
      case 1:
        CheckEqual("hello", vals[i], vals_sizes[i]);
        break;
      case 2:
        CheckEqual(NULL, vals[i], vals_sizes[i]);
        break;
      }
      Free(&vals[i]);
    }
  }

  StartPhase("pin_get");
  {
    CheckPinGet(db, roptions, "box", "c");
    CheckPinGet(db, roptions, "foo", "hello");
    CheckPinGet(db, roptions, "notfound", NULL);
  }

  StartPhase("approximate_sizes");
  {
    int i;
    int n = 20000;
    char keybuf[100];
    char valbuf[100];
    uint64_t sizes[2];
    const char* start[2] = { "a", "k00000000000000010000" };
    size_t start_len[2] = { 1, 21 };
    const char* limit[2] = { "k00000000000000010000", "z" };
    size_t limit_len[2] = { 21, 1 };
    rocksdb_writeoptions_set_sync(woptions, 0);
    for (i = 0; i < n; i++) {
      snprintf(keybuf, sizeof(keybuf), "k%020d", i);
      snprintf(valbuf, sizeof(valbuf), "v%020d", i);
      rocksdb_put(db, woptions, keybuf, strlen(keybuf), valbuf, strlen(valbuf),
                  &err);
      CheckNoError(err);
    }
    rocksdb_approximate_sizes(db, 2, start, start_len, limit, limit_len, sizes,
                              &err);
    CheckNoError(err);
    CheckCondition(sizes[0] > 0);
    CheckCondition(sizes[1] > 0);
  }

  StartPhase("property");
  {
    char* prop = rocksdb_property_value(db, "nosuchprop");
    CheckCondition(prop == NULL);
    prop = rocksdb_property_value(db, "rocksdb.stats");
    CheckCondition(prop != NULL);
    Free(&prop);
  }

  StartPhase("snapshot");
  {
    const rocksdb_snapshot_t* snap;
    snap = rocksdb_create_snapshot(db);
    rocksdb_delete(db, woptions, "foo", 3, &err);
    CheckNoError(err);
    rocksdb_readoptions_set_snapshot(roptions, snap);
    CheckGet(db, roptions, "foo", "hello");
    rocksdb_readoptions_set_snapshot(roptions, NULL);
    CheckGet(db, roptions, "foo", NULL);
    rocksdb_release_snapshot(db, snap);
  }

  StartPhase("repair");
  {
    // If we do not compact here, then the lazy deletion of
    // files (https://reviews.facebook.net/D6123) would leave
    // around deleted files and the repair process will find
    // those files and put them back into the database.
    rocksdb_compact_range(db, NULL, 0, NULL, 0);
    rocksdb_close(db);
    rocksdb_options_set_create_if_missing(options, 0);
    rocksdb_options_set_error_if_exists(options, 0);
    rocksdb_options_set_wal_recovery_mode(options, 2);
    rocksdb_repair_db(options, dbname, &err);
    CheckNoError(err);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "foo", NULL);
    CheckGet(db, roptions, "bar", NULL);
    CheckGet(db, roptions, "box", "c");
    rocksdb_options_set_create_if_missing(options, 1);
    rocksdb_options_set_error_if_exists(options, 1);
  }

  StartPhase("filter");
  for (run = 0; run <= 2; run++) {
    // First run uses custom filter
    // Second run uses old block-based bloom filter
    // Third run uses full bloom filter
    CheckNoError(err);
    rocksdb_filterpolicy_t* policy;
    if (run == 0) {
      policy = rocksdb_filterpolicy_create(NULL, FilterDestroy, FilterCreate,
                                           FilterKeyMatch, NULL, FilterName);
    } else if (run == 1) {
      policy = rocksdb_filterpolicy_create_bloom(8);
    } else {
      policy = rocksdb_filterpolicy_create_bloom_full(8);
    }
    rocksdb_block_based_options_set_filter_policy(table_options, policy);

    // Create new database
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    rocksdb_options_set_block_based_table_factory(options, table_options);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "foo", 3, "foovalue", 8, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "bar", 3, "barvalue", 8, &err);
    CheckNoError(err);

    {
      // Add enough keys to get just one reasonably populated Bloom filter
      const int keys_to_add = 1500;
      int i;
      char keybuf[100];
      for (i = 0; i < keys_to_add; i++) {
        snprintf(keybuf, sizeof(keybuf), "yes%020d", i);
        rocksdb_put(db, woptions, keybuf, strlen(keybuf), "val", 3, &err);
        CheckNoError(err);
      }
    }
    rocksdb_compact_range(db, NULL, 0, NULL, 0);

    fake_filter_result = 1;
    CheckGet(db, roptions, "foo", "foovalue");
    CheckGet(db, roptions, "bar", "barvalue");
    if (run == 0) {
      // Must not find value when custom filter returns false
      fake_filter_result = 0;
      CheckGet(db, roptions, "foo", NULL);
      CheckGet(db, roptions, "bar", NULL);
      fake_filter_result = 1;

      CheckGet(db, roptions, "foo", "foovalue");
      CheckGet(db, roptions, "bar", "barvalue");
    }

    {
      // Query some keys not added to identify Bloom filter implementation
      // from false positive queries, using perfcontext to detect Bloom
      // filter behavior
      rocksdb_perfcontext_t* perf = rocksdb_perfcontext_create();
      rocksdb_perfcontext_reset(perf);

      const int keys_to_query = 10000;
      int i;
      char keybuf[100];
      for (i = 0; i < keys_to_query; i++) {
        fake_filter_result = i % 2;
        snprintf(keybuf, sizeof(keybuf), "no%020d", i);
        CheckGet(db, roptions, keybuf, NULL);
      }

      const int hits =
          (int)rocksdb_perfcontext_metric(perf, rocksdb_bloom_sst_hit_count);
      if (run == 0) {
        // Due to half true, half false with fake filter result
        CheckCondition(hits == keys_to_query / 2);
      } else if (run == 1) {
        // Essentially a fingerprint of the block-based Bloom schema
        CheckCondition(hits == 241);
      } else {
        // Essentially a fingerprint of full Bloom schema, format_version=5
        CheckCondition(hits == 188);
      }
      CheckCondition(
          (keys_to_query - hits) ==
          (int)rocksdb_perfcontext_metric(perf, rocksdb_bloom_sst_miss_count));

      rocksdb_perfcontext_destroy(perf);
    }

    // Reset the policy
    rocksdb_block_based_options_set_filter_policy(table_options, NULL);
    rocksdb_options_set_block_based_table_factory(options, table_options);
  }

  StartPhase("compaction_filter");
  {
    rocksdb_options_t* options_with_filter = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(options_with_filter, 1);
    rocksdb_compactionfilter_t* cfilter;
    cfilter = rocksdb_compactionfilter_create(NULL, CFilterDestroy,
                                              CFilterFilter, CFilterName);
    // Create new database
    rocksdb_close(db);
    rocksdb_destroy_db(options_with_filter, dbname, &err);
    rocksdb_options_set_compaction_filter(options_with_filter, cfilter);
    db = CheckCompaction(db, options_with_filter, roptions, woptions);

    rocksdb_options_set_compaction_filter(options_with_filter, NULL);
    rocksdb_compactionfilter_destroy(cfilter);
    rocksdb_options_destroy(options_with_filter);
  }

  StartPhase("compaction_filter_factory");
  {
    rocksdb_options_t* options_with_filter_factory = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(options_with_filter_factory, 1);
    rocksdb_compactionfilterfactory_t* factory;
    factory = rocksdb_compactionfilterfactory_create(
        NULL, CFilterFactoryDestroy, CFilterCreate, CFilterFactoryName);
    // Create new database
    rocksdb_close(db);
    rocksdb_destroy_db(options_with_filter_factory, dbname, &err);
    rocksdb_options_set_compaction_filter_factory(options_with_filter_factory,
                                                  factory);
    db = CheckCompaction(db, options_with_filter_factory, roptions, woptions);

    rocksdb_options_set_compaction_filter_factory(
        options_with_filter_factory, NULL);
    rocksdb_options_destroy(options_with_filter_factory);
  }

  StartPhase("merge_operator");
  {
    rocksdb_mergeoperator_t* merge_operator;
    merge_operator = rocksdb_mergeoperator_create(
        NULL, MergeOperatorDestroy, MergeOperatorFullMerge,
        MergeOperatorPartialMerge, NULL, MergeOperatorName);
    // Create new database
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    rocksdb_options_set_merge_operator(options, merge_operator);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "foo", 3, "foovalue", 8, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "foo", "foovalue");
    rocksdb_merge(db, woptions, "foo", 3, "barvalue", 8, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "foo", "fake");

    // Merge of a non-existing value
    rocksdb_merge(db, woptions, "bar", 3, "barvalue", 8, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "bar", "fake");

  }

  StartPhase("columnfamilies");
  {
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);

    rocksdb_options_t* db_options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(db_options, 1);
    db = rocksdb_open(db_options, dbname, &err);
    CheckNoError(err)
    rocksdb_column_family_handle_t* cfh;
    cfh = rocksdb_create_column_family(db, db_options, "cf1", &err);
    rocksdb_column_family_handle_destroy(cfh);
    CheckNoError(err);
    rocksdb_close(db);

    size_t cflen;
    char** column_fams = rocksdb_list_column_families(db_options, dbname, &cflen, &err);
    CheckNoError(err);
    CheckEqual("default", column_fams[0], 7);
    CheckEqual("cf1", column_fams[1], 3);
    CheckCondition(cflen == 2);
    rocksdb_list_column_families_destroy(column_fams, cflen);

    rocksdb_options_t* cf_options = rocksdb_options_create();

    const char* cf_names[2] = {"default", "cf1"};
    const rocksdb_options_t* cf_opts[2] = {cf_options, cf_options};
    rocksdb_column_family_handle_t* handles[2];
    db = rocksdb_open_column_families(db_options, dbname, 2, cf_names, cf_opts, handles, &err);
    CheckNoError(err);

    rocksdb_put_cf(db, woptions, handles[1], "foo", 3, "hello", 5, &err);
    CheckNoError(err);

    rocksdb_put_cf(db, woptions, handles[1], "foobar1", 7, "hello1", 6, &err);
    CheckNoError(err);
    rocksdb_put_cf(db, woptions, handles[1], "foobar2", 7, "hello2", 6, &err);
    CheckNoError(err);
    rocksdb_put_cf(db, woptions, handles[1], "foobar3", 7, "hello3", 6, &err);
    CheckNoError(err);
    rocksdb_put_cf(db, woptions, handles[1], "foobar4", 7, "hello4", 6, &err);
    CheckNoError(err);

    rocksdb_flushoptions_t *flush_options = rocksdb_flushoptions_create();
    rocksdb_flushoptions_set_wait(flush_options, 1);
    rocksdb_flush_cf(db, flush_options, handles[1], &err);
    CheckNoError(err)
    rocksdb_flushoptions_destroy(flush_options);

    CheckGetCF(db, roptions, handles[1], "foo", "hello");
    CheckPinGetCF(db, roptions, handles[1], "foo", "hello");

    rocksdb_delete_cf(db, woptions, handles[1], "foo", 3, &err);
    CheckNoError(err);

    rocksdb_delete_range_cf(db, woptions, handles[1], "foobar2", 7, "foobar4",
                            7, &err);
    CheckNoError(err);

    CheckGetCF(db, roptions, handles[1], "foo", NULL);
    CheckPinGetCF(db, roptions, handles[1], "foo", NULL);

    rocksdb_writebatch_t* wb = rocksdb_writebatch_create();
    rocksdb_writebatch_put_cf(wb, handles[1], "baz", 3, "a", 1);
    rocksdb_writebatch_clear(wb);
    rocksdb_writebatch_put_cf(wb, handles[1], "bar", 3, "b", 1);
    rocksdb_writebatch_put_cf(wb, handles[1], "box", 3, "c", 1);
    rocksdb_writebatch_delete_cf(wb, handles[1], "bar", 3);
    rocksdb_write(db, woptions, wb, &err);
    CheckNoError(err);
    CheckGetCF(db, roptions, handles[1], "baz", NULL);
    CheckGetCF(db, roptions, handles[1], "bar", NULL);
    CheckGetCF(db, roptions, handles[1], "box", "c");
    CheckPinGetCF(db, roptions, handles[1], "baz", NULL);
    CheckPinGetCF(db, roptions, handles[1], "bar", NULL);
    CheckPinGetCF(db, roptions, handles[1], "box", "c");
    rocksdb_writebatch_destroy(wb);

    const char* keys[3] = { "box", "box", "barfooxx" };
    const rocksdb_column_family_handle_t* get_handles[3] = { handles[0], handles[1], handles[1] };
    const size_t keys_sizes[3] = { 3, 3, 8 };
    char* vals[3];
    size_t vals_sizes[3];
    char* errs[3];
    rocksdb_multi_get_cf(db, roptions, get_handles, 3, keys, keys_sizes, vals, vals_sizes, errs);

    int i;
    for (i = 0; i < 3; i++) {
      CheckEqual(NULL, errs[i], 0);
      switch (i) {
      case 0:
        CheckEqual(NULL, vals[i], vals_sizes[i]); // wrong cf
        break;
      case 1:
        CheckEqual("c", vals[i], vals_sizes[i]); // bingo
        break;
      case 2:
        CheckEqual(NULL, vals[i], vals_sizes[i]); // normal not found
        break;
      }
      Free(&vals[i]);
    }

    {
      unsigned char value_found = 0;

      CheckCondition(!rocksdb_key_may_exist(db, roptions, "invalid_key", 11,
                                            NULL, NULL, NULL, 0, NULL));
      CheckCondition(!rocksdb_key_may_exist(db, roptions, "invalid_key", 11,
                                            &vals[0], &vals_sizes[0], NULL, 0,
                                            &value_found));
      if (value_found) {
        Free(&vals[0]);
      }

      CheckCondition(!rocksdb_key_may_exist_cf(db, roptions, handles[1],
                                               "invalid_key", 11, NULL, NULL,
                                               NULL, 0, NULL));
      CheckCondition(!rocksdb_key_may_exist_cf(db, roptions, handles[1],
                                               "invalid_key", 11, &vals[0],
                                               &vals_sizes[0], NULL, 0, NULL));
      if (value_found) {
        Free(&vals[0]);
      }
    }

    rocksdb_iterator_t* iter = rocksdb_create_iterator_cf(db, roptions, handles[1]);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));

    for (i = 0; rocksdb_iter_valid(iter) != 0; rocksdb_iter_next(iter)) {
      i++;
    }
    CheckCondition(i == 3);
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);

    rocksdb_column_family_handle_t* iters_cf_handles[2] = { handles[0], handles[1] };
    rocksdb_iterator_t* iters_handles[2];
    rocksdb_create_iterators(db, roptions, iters_cf_handles, iters_handles, 2, &err);
    CheckNoError(err);

    iter = iters_handles[0];
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_destroy(iter);

    iter = iters_handles[1];
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));

    for (i = 0; rocksdb_iter_valid(iter) != 0; rocksdb_iter_next(iter)) {
      i++;
    }
    CheckCondition(i == 3);
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);

    rocksdb_drop_column_family(db, handles[1], &err);
    CheckNoError(err);
    for (i = 0; i < 2; i++) {
      rocksdb_column_family_handle_destroy(handles[i]);
    }
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    rocksdb_options_destroy(db_options);
    rocksdb_options_destroy(cf_options);
  }

  StartPhase("prefix");
  {
    // Create new database
    rocksdb_options_set_allow_mmap_reads(options, 1);
    rocksdb_options_set_prefix_extractor(options, rocksdb_slicetransform_create_fixed_prefix(3));
    rocksdb_options_set_hash_skip_list_rep(options, 5000, 4, 4);
    rocksdb_options_set_plain_table_factory(options, 4, 10, 0.75, 16);
    rocksdb_options_set_allow_concurrent_memtable_write(options, 0);

    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);

    rocksdb_put(db, woptions, "foo1", 4, "foo", 3, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "foo2", 4, "foo", 3, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "foo3", 4, "foo", 3, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "bar1", 4, "bar", 3, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "bar2", 4, "bar", 3, &err);
    CheckNoError(err);
    rocksdb_put(db, woptions, "bar3", 4, "bar", 3, &err);
    CheckNoError(err);

    rocksdb_iterator_t* iter = rocksdb_create_iterator(db, roptions);
    CheckCondition(!rocksdb_iter_valid(iter));

    rocksdb_iter_seek(iter, "bar", 3);
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    CheckCondition(rocksdb_iter_valid(iter));

    CheckIter(iter, "bar1", "bar");
    rocksdb_iter_next(iter);
    CheckIter(iter, "bar2", "bar");
    rocksdb_iter_next(iter);
    CheckIter(iter, "bar3", "bar");
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);

    rocksdb_readoptions_set_total_order_seek(roptions, 1);
    iter = rocksdb_create_iterator(db, roptions);
    CheckCondition(!rocksdb_iter_valid(iter));

    rocksdb_iter_seek(iter, "ba", 2);
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "bar1", "bar");

    rocksdb_iter_destroy(iter);
    rocksdb_readoptions_set_total_order_seek(roptions, 0);

    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
  }

  // Check memory usage stats
  StartPhase("approximate_memory_usage");
  {
    // Create database
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);

    rocksdb_memory_consumers_t* consumers;
    consumers = rocksdb_memory_consumers_create();
    rocksdb_memory_consumers_add_db(consumers, db);
    rocksdb_memory_consumers_add_cache(consumers, cache);

    // take memory usage report before write-read operation
    rocksdb_memory_usage_t* mu1;
    mu1 = rocksdb_approximate_memory_usage_create(consumers, &err);
    CheckNoError(err);

    // Put data (this should affect memtables)
    rocksdb_put(db, woptions, "memory", 6, "test", 4, &err);
    CheckNoError(err);
    CheckGet(db, roptions, "memory", "test");

    // take memory usage report after write-read operation
    rocksdb_memory_usage_t* mu2;
    mu2 = rocksdb_approximate_memory_usage_create(consumers, &err);
    CheckNoError(err);

    // amount of memory used within memtables should grow
    CheckCondition(rocksdb_approximate_memory_usage_get_mem_table_total(mu2) >=
                   rocksdb_approximate_memory_usage_get_mem_table_total(mu1));
    CheckCondition(rocksdb_approximate_memory_usage_get_mem_table_unflushed(mu2) >=
                   rocksdb_approximate_memory_usage_get_mem_table_unflushed(mu1));

    rocksdb_memory_consumers_destroy(consumers);
    rocksdb_approximate_memory_usage_destroy(mu1);
    rocksdb_approximate_memory_usage_destroy(mu2);
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);
  }

  StartPhase("cuckoo_options");
  {
    rocksdb_cuckoo_table_options_t* cuckoo_options;
    cuckoo_options = rocksdb_cuckoo_options_create();
    rocksdb_cuckoo_options_set_hash_ratio(cuckoo_options, 0.5);
    rocksdb_cuckoo_options_set_max_search_depth(cuckoo_options, 200);
    rocksdb_cuckoo_options_set_cuckoo_block_size(cuckoo_options, 10);
    rocksdb_cuckoo_options_set_identity_as_first_hash(cuckoo_options, 1);
    rocksdb_cuckoo_options_set_use_module_hash(cuckoo_options, 0);
    rocksdb_options_set_cuckoo_table_factory(options, cuckoo_options);

    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);

    rocksdb_cuckoo_options_destroy(cuckoo_options);
  }

  StartPhase("options");
  {
    rocksdb_options_t* o;
    o = rocksdb_options_create();

    // Set and check options.
    rocksdb_options_set_allow_ingest_behind(o, 1);
    CheckCondition(1 == rocksdb_options_get_allow_ingest_behind(o));

    rocksdb_options_compaction_readahead_size(o, 10);
    CheckCondition(10 == rocksdb_options_get_compaction_readahead_size(o));

    rocksdb_options_set_create_if_missing(o, 1);
    CheckCondition(1 == rocksdb_options_get_create_if_missing(o));

    rocksdb_options_set_create_missing_column_families(o, 1);
    CheckCondition(1 == rocksdb_options_get_create_missing_column_families(o));

    rocksdb_options_set_error_if_exists(o, 1);
    CheckCondition(1 == rocksdb_options_get_error_if_exists(o));

    rocksdb_options_set_paranoid_checks(o, 1);
    CheckCondition(1 == rocksdb_options_get_paranoid_checks(o));

    rocksdb_options_set_info_log_level(o, 3);
    CheckCondition(3 == rocksdb_options_get_info_log_level(o));

    rocksdb_options_set_write_buffer_size(o, 100);
    CheckCondition(100 == rocksdb_options_get_write_buffer_size(o));

    rocksdb_options_set_db_write_buffer_size(o, 1000);
    CheckCondition(1000 == rocksdb_options_get_db_write_buffer_size(o));

    rocksdb_options_set_max_open_files(o, 21);
    CheckCondition(21 == rocksdb_options_get_max_open_files(o));

    rocksdb_options_set_max_file_opening_threads(o, 5);
    CheckCondition(5 == rocksdb_options_get_max_file_opening_threads(o));

    rocksdb_options_set_max_total_wal_size(o, 400);
    CheckCondition(400 == rocksdb_options_get_max_total_wal_size(o));

    rocksdb_options_set_num_levels(o, 7);
    CheckCondition(7 == rocksdb_options_get_num_levels(o));

    rocksdb_options_set_level0_file_num_compaction_trigger(o, 4);
    CheckCondition(4 ==
                   rocksdb_options_get_level0_file_num_compaction_trigger(o));

    rocksdb_options_set_level0_slowdown_writes_trigger(o, 6);
    CheckCondition(6 == rocksdb_options_get_level0_slowdown_writes_trigger(o));

    rocksdb_options_set_level0_stop_writes_trigger(o, 8);
    CheckCondition(8 == rocksdb_options_get_level0_stop_writes_trigger(o));

    rocksdb_options_set_target_file_size_base(o, 256);
    CheckCondition(256 == rocksdb_options_get_target_file_size_base(o));

    rocksdb_options_set_target_file_size_multiplier(o, 3);
    CheckCondition(3 == rocksdb_options_get_target_file_size_multiplier(o));

    rocksdb_options_set_max_bytes_for_level_base(o, 1024);
    CheckCondition(1024 == rocksdb_options_get_max_bytes_for_level_base(o));

    rocksdb_options_set_level_compaction_dynamic_level_bytes(o, 1);
    CheckCondition(1 ==
                   rocksdb_options_get_level_compaction_dynamic_level_bytes(o));

    rocksdb_options_set_max_bytes_for_level_multiplier(o, 2.0);
    CheckCondition(2.0 ==
                   rocksdb_options_get_max_bytes_for_level_multiplier(o));

    rocksdb_options_set_skip_stats_update_on_db_open(o, 1);
    CheckCondition(1 == rocksdb_options_get_skip_stats_update_on_db_open(o));

    rocksdb_options_set_skip_checking_sst_file_sizes_on_db_open(o, 1);
    CheckCondition(
        1 == rocksdb_options_get_skip_checking_sst_file_sizes_on_db_open(o));

    rocksdb_options_set_max_write_buffer_number(o, 97);
    CheckCondition(97 == rocksdb_options_get_max_write_buffer_number(o));

    rocksdb_options_set_min_write_buffer_number_to_merge(o, 23);
    CheckCondition(23 ==
                   rocksdb_options_get_min_write_buffer_number_to_merge(o));

    rocksdb_options_set_max_write_buffer_number_to_maintain(o, 64);
    CheckCondition(64 ==
                   rocksdb_options_get_max_write_buffer_number_to_maintain(o));

    rocksdb_options_set_max_write_buffer_size_to_maintain(o, 50000);
    CheckCondition(50000 ==
                   rocksdb_options_get_max_write_buffer_size_to_maintain(o));

    rocksdb_options_set_enable_pipelined_write(o, 1);
    CheckCondition(1 == rocksdb_options_get_enable_pipelined_write(o));

    rocksdb_options_set_unordered_write(o, 1);
    CheckCondition(1 == rocksdb_options_get_unordered_write(o));

    rocksdb_options_set_max_subcompactions(o, 123456);
    CheckCondition(123456 == rocksdb_options_get_max_subcompactions(o));

    rocksdb_options_set_max_background_jobs(o, 2);
    CheckCondition(2 == rocksdb_options_get_max_background_jobs(o));

    rocksdb_options_set_max_background_compactions(o, 3);
    CheckCondition(3 == rocksdb_options_get_max_background_compactions(o));

    rocksdb_options_set_base_background_compactions(o, 4);
    CheckCondition(4 == rocksdb_options_get_base_background_compactions(o));

    rocksdb_options_set_max_background_flushes(o, 5);
    CheckCondition(5 == rocksdb_options_get_max_background_flushes(o));

    rocksdb_options_set_max_log_file_size(o, 6);
    CheckCondition(6 == rocksdb_options_get_max_log_file_size(o));

    rocksdb_options_set_log_file_time_to_roll(o, 7);
    CheckCondition(7 == rocksdb_options_get_log_file_time_to_roll(o));

    rocksdb_options_set_keep_log_file_num(o, 8);
    CheckCondition(8 == rocksdb_options_get_keep_log_file_num(o));

    rocksdb_options_set_recycle_log_file_num(o, 9);
    CheckCondition(9 == rocksdb_options_get_recycle_log_file_num(o));

    rocksdb_options_set_soft_rate_limit(o, 2.0);
    CheckCondition(2.0 == rocksdb_options_get_soft_rate_limit(o));

    rocksdb_options_set_hard_rate_limit(o, 4.0);
    CheckCondition(4.0 == rocksdb_options_get_hard_rate_limit(o));

    rocksdb_options_set_soft_pending_compaction_bytes_limit(o, 10);
    CheckCondition(10 ==
                   rocksdb_options_get_soft_pending_compaction_bytes_limit(o));

    rocksdb_options_set_hard_pending_compaction_bytes_limit(o, 11);
    CheckCondition(11 ==
                   rocksdb_options_get_hard_pending_compaction_bytes_limit(o));

    rocksdb_options_set_rate_limit_delay_max_milliseconds(o, 1);
    CheckCondition(1 ==
                   rocksdb_options_get_rate_limit_delay_max_milliseconds(o));

    rocksdb_options_set_max_manifest_file_size(o, 12);
    CheckCondition(12 == rocksdb_options_get_max_manifest_file_size(o));

    rocksdb_options_set_table_cache_numshardbits(o, 13);
    CheckCondition(13 == rocksdb_options_get_table_cache_numshardbits(o));

    rocksdb_options_set_arena_block_size(o, 14);
    CheckCondition(14 == rocksdb_options_get_arena_block_size(o));

    rocksdb_options_set_use_fsync(o, 1);
    CheckCondition(1 == rocksdb_options_get_use_fsync(o));

    rocksdb_options_set_WAL_ttl_seconds(o, 15);
    CheckCondition(15 == rocksdb_options_get_WAL_ttl_seconds(o));

    rocksdb_options_set_WAL_size_limit_MB(o, 16);
    CheckCondition(16 == rocksdb_options_get_WAL_size_limit_MB(o));

    rocksdb_options_set_manifest_preallocation_size(o, 17);
    CheckCondition(17 == rocksdb_options_get_manifest_preallocation_size(o));

    rocksdb_options_set_allow_mmap_reads(o, 1);
    CheckCondition(1 == rocksdb_options_get_allow_mmap_reads(o));

    rocksdb_options_set_allow_mmap_writes(o, 1);
    CheckCondition(1 == rocksdb_options_get_allow_mmap_writes(o));

    rocksdb_options_set_use_direct_reads(o, 1);
    CheckCondition(1 == rocksdb_options_get_use_direct_reads(o));

    rocksdb_options_set_use_direct_io_for_flush_and_compaction(o, 1);
    CheckCondition(
        1 == rocksdb_options_get_use_direct_io_for_flush_and_compaction(o));

    rocksdb_options_set_is_fd_close_on_exec(o, 1);
    CheckCondition(1 == rocksdb_options_get_is_fd_close_on_exec(o));

    rocksdb_options_set_skip_log_error_on_recovery(o, 1);
    CheckCondition(1 == rocksdb_options_get_skip_log_error_on_recovery(o));

    rocksdb_options_set_stats_dump_period_sec(o, 18);
    CheckCondition(18 == rocksdb_options_get_stats_dump_period_sec(o));

    rocksdb_options_set_stats_persist_period_sec(o, 5);
    CheckCondition(5 == rocksdb_options_get_stats_persist_period_sec(o));

    rocksdb_options_set_advise_random_on_open(o, 1);
    CheckCondition(1 == rocksdb_options_get_advise_random_on_open(o));

    rocksdb_options_set_access_hint_on_compaction_start(o, 3);
    CheckCondition(3 == rocksdb_options_get_access_hint_on_compaction_start(o));

    rocksdb_options_set_use_adaptive_mutex(o, 1);
    CheckCondition(1 == rocksdb_options_get_use_adaptive_mutex(o));

    rocksdb_options_set_bytes_per_sync(o, 19);
    CheckCondition(19 == rocksdb_options_get_bytes_per_sync(o));

    rocksdb_options_set_wal_bytes_per_sync(o, 20);
    CheckCondition(20 == rocksdb_options_get_wal_bytes_per_sync(o));

    rocksdb_options_set_writable_file_max_buffer_size(o, 21);
    CheckCondition(21 == rocksdb_options_get_writable_file_max_buffer_size(o));

    rocksdb_options_set_allow_concurrent_memtable_write(o, 1);
    CheckCondition(1 == rocksdb_options_get_allow_concurrent_memtable_write(o));

    rocksdb_options_set_enable_write_thread_adaptive_yield(o, 1);
    CheckCondition(1 ==
                   rocksdb_options_get_enable_write_thread_adaptive_yield(o));

    rocksdb_options_set_max_sequential_skip_in_iterations(o, 22);
    CheckCondition(22 ==
                   rocksdb_options_get_max_sequential_skip_in_iterations(o));

    rocksdb_options_set_disable_auto_compactions(o, 1);
    CheckCondition(1 == rocksdb_options_get_disable_auto_compactions(o));

    rocksdb_options_set_optimize_filters_for_hits(o, 1);
    CheckCondition(1 == rocksdb_options_get_optimize_filters_for_hits(o));

    rocksdb_options_set_delete_obsolete_files_period_micros(o, 23);
    CheckCondition(23 ==
                   rocksdb_options_get_delete_obsolete_files_period_micros(o));

    rocksdb_options_set_memtable_prefix_bloom_size_ratio(o, 2.0);
    CheckCondition(2.0 ==
                   rocksdb_options_get_memtable_prefix_bloom_size_ratio(o));

    rocksdb_options_set_max_compaction_bytes(o, 24);
    CheckCondition(24 == rocksdb_options_get_max_compaction_bytes(o));

    rocksdb_options_set_memtable_huge_page_size(o, 25);
    CheckCondition(25 == rocksdb_options_get_memtable_huge_page_size(o));

    rocksdb_options_set_max_successive_merges(o, 26);
    CheckCondition(26 == rocksdb_options_get_max_successive_merges(o));

    rocksdb_options_set_bloom_locality(o, 27);
    CheckCondition(27 == rocksdb_options_get_bloom_locality(o));

    rocksdb_options_set_inplace_update_support(o, 1);
    CheckCondition(1 == rocksdb_options_get_inplace_update_support(o));

    rocksdb_options_set_inplace_update_num_locks(o, 28);
    CheckCondition(28 == rocksdb_options_get_inplace_update_num_locks(o));

    rocksdb_options_set_report_bg_io_stats(o, 1);
    CheckCondition(1 == rocksdb_options_get_report_bg_io_stats(o));

    rocksdb_options_set_wal_recovery_mode(o, 2);
    CheckCondition(2 == rocksdb_options_get_wal_recovery_mode(o));

    rocksdb_options_set_compression(o, 5);
    CheckCondition(5 == rocksdb_options_get_compression(o));

    rocksdb_options_set_bottommost_compression(o, 4);
    CheckCondition(4 == rocksdb_options_get_bottommost_compression(o));

    rocksdb_options_set_compaction_style(o, 2);
    CheckCondition(2 == rocksdb_options_get_compaction_style(o));

    rocksdb_options_set_atomic_flush(o, 1);
    CheckCondition(1 == rocksdb_options_get_atomic_flush(o));

    // Create a copy that should be equal to the original.
    rocksdb_options_t* copy;
    copy = rocksdb_options_create_copy(o);

    CheckCondition(1 == rocksdb_options_get_allow_ingest_behind(copy));
    CheckCondition(10 == rocksdb_options_get_compaction_readahead_size(copy));
    CheckCondition(1 == rocksdb_options_get_create_if_missing(copy));
    CheckCondition(1 ==
                   rocksdb_options_get_create_missing_column_families(copy));
    CheckCondition(1 == rocksdb_options_get_error_if_exists(copy));
    CheckCondition(1 == rocksdb_options_get_paranoid_checks(copy));
    CheckCondition(3 == rocksdb_options_get_info_log_level(copy));
    CheckCondition(100 == rocksdb_options_get_write_buffer_size(copy));
    CheckCondition(1000 == rocksdb_options_get_db_write_buffer_size(copy));
    CheckCondition(21 == rocksdb_options_get_max_open_files(copy));
    CheckCondition(5 == rocksdb_options_get_max_file_opening_threads(copy));
    CheckCondition(400 == rocksdb_options_get_max_total_wal_size(copy));
    CheckCondition(7 == rocksdb_options_get_num_levels(copy));
    CheckCondition(
        4 == rocksdb_options_get_level0_file_num_compaction_trigger(copy));
    CheckCondition(6 ==
                   rocksdb_options_get_level0_slowdown_writes_trigger(copy));
    CheckCondition(8 == rocksdb_options_get_level0_stop_writes_trigger(copy));
    CheckCondition(256 == rocksdb_options_get_target_file_size_base(copy));
    CheckCondition(3 == rocksdb_options_get_target_file_size_multiplier(copy));
    CheckCondition(1024 == rocksdb_options_get_max_bytes_for_level_base(copy));
    CheckCondition(
        1 == rocksdb_options_get_level_compaction_dynamic_level_bytes(copy));
    CheckCondition(2.0 ==
                   rocksdb_options_get_max_bytes_for_level_multiplier(copy));
    CheckCondition(1 == rocksdb_options_get_skip_stats_update_on_db_open(copy));
    CheckCondition(
        1 == rocksdb_options_get_skip_checking_sst_file_sizes_on_db_open(copy));
    CheckCondition(97 == rocksdb_options_get_max_write_buffer_number(copy));
    CheckCondition(23 ==
                   rocksdb_options_get_min_write_buffer_number_to_merge(copy));
    CheckCondition(
        64 == rocksdb_options_get_max_write_buffer_number_to_maintain(copy));
    CheckCondition(50000 ==
                   rocksdb_options_get_max_write_buffer_size_to_maintain(copy));
    CheckCondition(1 == rocksdb_options_get_enable_pipelined_write(copy));
    CheckCondition(1 == rocksdb_options_get_unordered_write(copy));
    CheckCondition(123456 == rocksdb_options_get_max_subcompactions(copy));
    CheckCondition(2 == rocksdb_options_get_max_background_jobs(copy));
    CheckCondition(3 == rocksdb_options_get_max_background_compactions(copy));
    CheckCondition(4 == rocksdb_options_get_base_background_compactions(copy));
    CheckCondition(5 == rocksdb_options_get_max_background_flushes(copy));
    CheckCondition(6 == rocksdb_options_get_max_log_file_size(copy));
    CheckCondition(7 == rocksdb_options_get_log_file_time_to_roll(copy));
    CheckCondition(8 == rocksdb_options_get_keep_log_file_num(copy));
    CheckCondition(9 == rocksdb_options_get_recycle_log_file_num(copy));
    CheckCondition(2.0 == rocksdb_options_get_soft_rate_limit(copy));
    CheckCondition(4.0 == rocksdb_options_get_hard_rate_limit(copy));
    CheckCondition(
        10 == rocksdb_options_get_soft_pending_compaction_bytes_limit(copy));
    CheckCondition(
        11 == rocksdb_options_get_hard_pending_compaction_bytes_limit(copy));
    CheckCondition(1 ==
                   rocksdb_options_get_rate_limit_delay_max_milliseconds(copy));
    CheckCondition(12 == rocksdb_options_get_max_manifest_file_size(copy));
    CheckCondition(13 == rocksdb_options_get_table_cache_numshardbits(copy));
    CheckCondition(14 == rocksdb_options_get_arena_block_size(copy));
    CheckCondition(1 == rocksdb_options_get_use_fsync(copy));
    CheckCondition(15 == rocksdb_options_get_WAL_ttl_seconds(copy));
    CheckCondition(16 == rocksdb_options_get_WAL_size_limit_MB(copy));
    CheckCondition(17 == rocksdb_options_get_manifest_preallocation_size(copy));
    CheckCondition(1 == rocksdb_options_get_allow_mmap_reads(copy));
    CheckCondition(1 == rocksdb_options_get_allow_mmap_writes(copy));
    CheckCondition(1 == rocksdb_options_get_use_direct_reads(copy));
    CheckCondition(
        1 == rocksdb_options_get_use_direct_io_for_flush_and_compaction(copy));
    CheckCondition(1 == rocksdb_options_get_is_fd_close_on_exec(copy));
    CheckCondition(1 == rocksdb_options_get_skip_log_error_on_recovery(copy));
    CheckCondition(18 == rocksdb_options_get_stats_dump_period_sec(copy));
    CheckCondition(5 == rocksdb_options_get_stats_persist_period_sec(copy));
    CheckCondition(1 == rocksdb_options_get_advise_random_on_open(copy));
    CheckCondition(3 ==
                   rocksdb_options_get_access_hint_on_compaction_start(copy));
    CheckCondition(1 == rocksdb_options_get_use_adaptive_mutex(copy));
    CheckCondition(19 == rocksdb_options_get_bytes_per_sync(copy));
    CheckCondition(20 == rocksdb_options_get_wal_bytes_per_sync(copy));
    CheckCondition(21 ==
                   rocksdb_options_get_writable_file_max_buffer_size(copy));
    CheckCondition(1 ==
                   rocksdb_options_get_allow_concurrent_memtable_write(copy));
    CheckCondition(
        1 == rocksdb_options_get_enable_write_thread_adaptive_yield(copy));
    CheckCondition(22 ==
                   rocksdb_options_get_max_sequential_skip_in_iterations(copy));
    CheckCondition(1 == rocksdb_options_get_disable_auto_compactions(copy));
    CheckCondition(1 == rocksdb_options_get_optimize_filters_for_hits(copy));
    CheckCondition(
        23 == rocksdb_options_get_delete_obsolete_files_period_micros(copy));
    CheckCondition(2.0 ==
                   rocksdb_options_get_memtable_prefix_bloom_size_ratio(copy));
    CheckCondition(24 == rocksdb_options_get_max_compaction_bytes(copy));
    CheckCondition(25 == rocksdb_options_get_memtable_huge_page_size(copy));
    CheckCondition(26 == rocksdb_options_get_max_successive_merges(copy));
    CheckCondition(27 == rocksdb_options_get_bloom_locality(copy));
    CheckCondition(1 == rocksdb_options_get_inplace_update_support(copy));
    CheckCondition(28 == rocksdb_options_get_inplace_update_num_locks(copy));
    CheckCondition(1 == rocksdb_options_get_report_bg_io_stats(copy));
    CheckCondition(2 == rocksdb_options_get_wal_recovery_mode(copy));
    CheckCondition(5 == rocksdb_options_get_compression(copy));
    CheckCondition(4 == rocksdb_options_get_bottommost_compression(copy));
    CheckCondition(2 == rocksdb_options_get_compaction_style(copy));
    CheckCondition(1 == rocksdb_options_get_atomic_flush(copy));

    // Copies should be independent.
    rocksdb_options_set_allow_ingest_behind(copy, 0);
    CheckCondition(0 == rocksdb_options_get_allow_ingest_behind(copy));
    CheckCondition(1 == rocksdb_options_get_allow_ingest_behind(o));

    rocksdb_options_compaction_readahead_size(copy, 20);
    CheckCondition(20 == rocksdb_options_get_compaction_readahead_size(copy));
    CheckCondition(10 == rocksdb_options_get_compaction_readahead_size(o));

    rocksdb_options_set_create_if_missing(copy, 0);
    CheckCondition(0 == rocksdb_options_get_create_if_missing(copy));
    CheckCondition(1 == rocksdb_options_get_create_if_missing(o));

    rocksdb_options_set_create_missing_column_families(copy, 0);
    CheckCondition(0 ==
                   rocksdb_options_get_create_missing_column_families(copy));
    CheckCondition(1 == rocksdb_options_get_create_missing_column_families(o));

    rocksdb_options_set_error_if_exists(copy, 0);
    CheckCondition(0 == rocksdb_options_get_error_if_exists(copy));
    CheckCondition(1 == rocksdb_options_get_error_if_exists(o));

    rocksdb_options_set_paranoid_checks(copy, 0);
    CheckCondition(0 == rocksdb_options_get_paranoid_checks(copy));
    CheckCondition(1 == rocksdb_options_get_paranoid_checks(o));

    rocksdb_options_set_info_log_level(copy, 2);
    CheckCondition(2 == rocksdb_options_get_info_log_level(copy));
    CheckCondition(3 == rocksdb_options_get_info_log_level(o));

    rocksdb_options_set_write_buffer_size(copy, 200);
    CheckCondition(200 == rocksdb_options_get_write_buffer_size(copy));
    CheckCondition(100 == rocksdb_options_get_write_buffer_size(o));

    rocksdb_options_set_db_write_buffer_size(copy, 2000);
    CheckCondition(2000 == rocksdb_options_get_db_write_buffer_size(copy));
    CheckCondition(1000 == rocksdb_options_get_db_write_buffer_size(o));

    rocksdb_options_set_max_open_files(copy, 42);
    CheckCondition(42 == rocksdb_options_get_max_open_files(copy));
    CheckCondition(21 == rocksdb_options_get_max_open_files(o));

    rocksdb_options_set_max_file_opening_threads(copy, 3);
    CheckCondition(3 == rocksdb_options_get_max_file_opening_threads(copy));
    CheckCondition(5 == rocksdb_options_get_max_file_opening_threads(o));

    rocksdb_options_set_max_total_wal_size(copy, 4000);
    CheckCondition(4000 == rocksdb_options_get_max_total_wal_size(copy));
    CheckCondition(400 == rocksdb_options_get_max_total_wal_size(o));

    rocksdb_options_set_num_levels(copy, 6);
    CheckCondition(6 == rocksdb_options_get_num_levels(copy));
    CheckCondition(7 == rocksdb_options_get_num_levels(o));

    rocksdb_options_set_level0_file_num_compaction_trigger(copy, 14);
    CheckCondition(
        14 == rocksdb_options_get_level0_file_num_compaction_trigger(copy));
    CheckCondition(4 ==
                   rocksdb_options_get_level0_file_num_compaction_trigger(o));

    rocksdb_options_set_level0_slowdown_writes_trigger(copy, 61);
    CheckCondition(61 ==
                   rocksdb_options_get_level0_slowdown_writes_trigger(copy));
    CheckCondition(6 == rocksdb_options_get_level0_slowdown_writes_trigger(o));

    rocksdb_options_set_level0_stop_writes_trigger(copy, 17);
    CheckCondition(17 == rocksdb_options_get_level0_stop_writes_trigger(copy));
    CheckCondition(8 == rocksdb_options_get_level0_stop_writes_trigger(o));

    rocksdb_options_set_target_file_size_base(copy, 128);
    CheckCondition(128 == rocksdb_options_get_target_file_size_base(copy));
    CheckCondition(256 == rocksdb_options_get_target_file_size_base(o));

    rocksdb_options_set_target_file_size_multiplier(copy, 13);
    CheckCondition(13 == rocksdb_options_get_target_file_size_multiplier(copy));
    CheckCondition(3 == rocksdb_options_get_target_file_size_multiplier(o));

    rocksdb_options_set_max_bytes_for_level_base(copy, 900);
    CheckCondition(900 == rocksdb_options_get_max_bytes_for_level_base(copy));
    CheckCondition(1024 == rocksdb_options_get_max_bytes_for_level_base(o));

    rocksdb_options_set_level_compaction_dynamic_level_bytes(copy, 0);
    CheckCondition(
        0 == rocksdb_options_get_level_compaction_dynamic_level_bytes(copy));
    CheckCondition(1 ==
                   rocksdb_options_get_level_compaction_dynamic_level_bytes(o));

    rocksdb_options_set_max_bytes_for_level_multiplier(copy, 8.0);
    CheckCondition(8.0 ==
                   rocksdb_options_get_max_bytes_for_level_multiplier(copy));
    CheckCondition(2.0 ==
                   rocksdb_options_get_max_bytes_for_level_multiplier(o));

    rocksdb_options_set_skip_stats_update_on_db_open(copy, 0);
    CheckCondition(0 == rocksdb_options_get_skip_stats_update_on_db_open(copy));
    CheckCondition(1 == rocksdb_options_get_skip_stats_update_on_db_open(o));

    rocksdb_options_set_skip_checking_sst_file_sizes_on_db_open(copy, 0);
    CheckCondition(
        0 == rocksdb_options_get_skip_checking_sst_file_sizes_on_db_open(copy));
    CheckCondition(
        1 == rocksdb_options_get_skip_checking_sst_file_sizes_on_db_open(o));

    rocksdb_options_set_max_write_buffer_number(copy, 2000);
    CheckCondition(2000 == rocksdb_options_get_max_write_buffer_number(copy));
    CheckCondition(97 == rocksdb_options_get_max_write_buffer_number(o));

    rocksdb_options_set_min_write_buffer_number_to_merge(copy, 146);
    CheckCondition(146 ==
                   rocksdb_options_get_min_write_buffer_number_to_merge(copy));
    CheckCondition(23 ==
                   rocksdb_options_get_min_write_buffer_number_to_merge(o));

    rocksdb_options_set_max_write_buffer_number_to_maintain(copy, 128);
    CheckCondition(
        128 == rocksdb_options_get_max_write_buffer_number_to_maintain(copy));
    CheckCondition(64 ==
                   rocksdb_options_get_max_write_buffer_number_to_maintain(o));

    rocksdb_options_set_max_write_buffer_size_to_maintain(copy, 9000);
    CheckCondition(9000 ==
                   rocksdb_options_get_max_write_buffer_size_to_maintain(copy));
    CheckCondition(50000 ==
                   rocksdb_options_get_max_write_buffer_size_to_maintain(o));

    rocksdb_options_set_enable_pipelined_write(copy, 0);
    CheckCondition(0 == rocksdb_options_get_enable_pipelined_write(copy));
    CheckCondition(1 == rocksdb_options_get_enable_pipelined_write(o));

    rocksdb_options_set_unordered_write(copy, 0);
    CheckCondition(0 == rocksdb_options_get_unordered_write(copy));
    CheckCondition(1 == rocksdb_options_get_unordered_write(o));

    rocksdb_options_set_max_subcompactions(copy, 90001);
    CheckCondition(90001 == rocksdb_options_get_max_subcompactions(copy));
    CheckCondition(123456 == rocksdb_options_get_max_subcompactions(o));

    rocksdb_options_set_max_background_jobs(copy, 12);
    CheckCondition(12 == rocksdb_options_get_max_background_jobs(copy));
    CheckCondition(2 == rocksdb_options_get_max_background_jobs(o));

    rocksdb_options_set_max_background_compactions(copy, 13);
    CheckCondition(13 == rocksdb_options_get_max_background_compactions(copy));
    CheckCondition(3 == rocksdb_options_get_max_background_compactions(o));

    rocksdb_options_set_base_background_compactions(copy, 14);
    CheckCondition(14 == rocksdb_options_get_base_background_compactions(copy));
    CheckCondition(4 == rocksdb_options_get_base_background_compactions(o));

    rocksdb_options_set_max_background_flushes(copy, 15);
    CheckCondition(15 == rocksdb_options_get_max_background_flushes(copy));
    CheckCondition(5 == rocksdb_options_get_max_background_flushes(o));

    rocksdb_options_set_max_log_file_size(copy, 16);
    CheckCondition(16 == rocksdb_options_get_max_log_file_size(copy));
    CheckCondition(6 == rocksdb_options_get_max_log_file_size(o));

    rocksdb_options_set_log_file_time_to_roll(copy, 17);
    CheckCondition(17 == rocksdb_options_get_log_file_time_to_roll(copy));
    CheckCondition(7 == rocksdb_options_get_log_file_time_to_roll(o));

    rocksdb_options_set_keep_log_file_num(copy, 18);
    CheckCondition(18 == rocksdb_options_get_keep_log_file_num(copy));
    CheckCondition(8 == rocksdb_options_get_keep_log_file_num(o));

    rocksdb_options_set_recycle_log_file_num(copy, 19);
    CheckCondition(19 == rocksdb_options_get_recycle_log_file_num(copy));
    CheckCondition(9 == rocksdb_options_get_recycle_log_file_num(o));

    rocksdb_options_set_soft_rate_limit(copy, 4.0);
    CheckCondition(4.0 == rocksdb_options_get_soft_rate_limit(copy));
    CheckCondition(2.0 == rocksdb_options_get_soft_rate_limit(o));

    rocksdb_options_set_hard_rate_limit(copy, 2.0);
    CheckCondition(2.0 == rocksdb_options_get_hard_rate_limit(copy));
    CheckCondition(4.0 == rocksdb_options_get_hard_rate_limit(o));

    rocksdb_options_set_soft_pending_compaction_bytes_limit(copy, 110);
    CheckCondition(
        110 == rocksdb_options_get_soft_pending_compaction_bytes_limit(copy));
    CheckCondition(10 ==
                   rocksdb_options_get_soft_pending_compaction_bytes_limit(o));

    rocksdb_options_set_hard_pending_compaction_bytes_limit(copy, 111);
    CheckCondition(
        111 == rocksdb_options_get_hard_pending_compaction_bytes_limit(copy));
    CheckCondition(11 ==
                   rocksdb_options_get_hard_pending_compaction_bytes_limit(o));

    rocksdb_options_set_rate_limit_delay_max_milliseconds(copy, 0);
    CheckCondition(0 ==
                   rocksdb_options_get_rate_limit_delay_max_milliseconds(copy));
    CheckCondition(1 ==
                   rocksdb_options_get_rate_limit_delay_max_milliseconds(o));

    rocksdb_options_set_max_manifest_file_size(copy, 112);
    CheckCondition(112 == rocksdb_options_get_max_manifest_file_size(copy));
    CheckCondition(12 == rocksdb_options_get_max_manifest_file_size(o));

    rocksdb_options_set_table_cache_numshardbits(copy, 113);
    CheckCondition(113 == rocksdb_options_get_table_cache_numshardbits(copy));
    CheckCondition(13 == rocksdb_options_get_table_cache_numshardbits(o));

    rocksdb_options_set_arena_block_size(copy, 114);
    CheckCondition(114 == rocksdb_options_get_arena_block_size(copy));
    CheckCondition(14 == rocksdb_options_get_arena_block_size(o));

    rocksdb_options_set_use_fsync(copy, 0);
    CheckCondition(0 == rocksdb_options_get_use_fsync(copy));
    CheckCondition(1 == rocksdb_options_get_use_fsync(o));

    rocksdb_options_set_WAL_ttl_seconds(copy, 115);
    CheckCondition(115 == rocksdb_options_get_WAL_ttl_seconds(copy));
    CheckCondition(15 == rocksdb_options_get_WAL_ttl_seconds(o));

    rocksdb_options_set_WAL_size_limit_MB(copy, 116);
    CheckCondition(116 == rocksdb_options_get_WAL_size_limit_MB(copy));
    CheckCondition(16 == rocksdb_options_get_WAL_size_limit_MB(o));

    rocksdb_options_set_manifest_preallocation_size(copy, 117);
    CheckCondition(117 ==
                   rocksdb_options_get_manifest_preallocation_size(copy));
    CheckCondition(17 == rocksdb_options_get_manifest_preallocation_size(o));

    rocksdb_options_set_allow_mmap_reads(copy, 0);
    CheckCondition(0 == rocksdb_options_get_allow_mmap_reads(copy));
    CheckCondition(1 == rocksdb_options_get_allow_mmap_reads(o));

    rocksdb_options_set_allow_mmap_writes(copy, 0);
    CheckCondition(0 == rocksdb_options_get_allow_mmap_writes(copy));
    CheckCondition(1 == rocksdb_options_get_allow_mmap_writes(o));

    rocksdb_options_set_use_direct_reads(copy, 0);
    CheckCondition(0 == rocksdb_options_get_use_direct_reads(copy));
    CheckCondition(1 == rocksdb_options_get_use_direct_reads(o));

    rocksdb_options_set_use_direct_io_for_flush_and_compaction(copy, 0);
    CheckCondition(
        0 == rocksdb_options_get_use_direct_io_for_flush_and_compaction(copy));
    CheckCondition(
        1 == rocksdb_options_get_use_direct_io_for_flush_and_compaction(o));

    rocksdb_options_set_is_fd_close_on_exec(copy, 0);
    CheckCondition(0 == rocksdb_options_get_is_fd_close_on_exec(copy));
    CheckCondition(1 == rocksdb_options_get_is_fd_close_on_exec(o));

    rocksdb_options_set_skip_log_error_on_recovery(copy, 0);
    CheckCondition(0 == rocksdb_options_get_skip_log_error_on_recovery(copy));
    CheckCondition(1 == rocksdb_options_get_skip_log_error_on_recovery(o));

    rocksdb_options_set_stats_dump_period_sec(copy, 218);
    CheckCondition(218 == rocksdb_options_get_stats_dump_period_sec(copy));
    CheckCondition(18 == rocksdb_options_get_stats_dump_period_sec(o));

    rocksdb_options_set_stats_persist_period_sec(copy, 600);
    CheckCondition(600 == rocksdb_options_get_stats_persist_period_sec(copy));
    CheckCondition(5 == rocksdb_options_get_stats_persist_period_sec(o));

    rocksdb_options_set_advise_random_on_open(copy, 0);
    CheckCondition(0 == rocksdb_options_get_advise_random_on_open(copy));
    CheckCondition(1 == rocksdb_options_get_advise_random_on_open(o));

    rocksdb_options_set_access_hint_on_compaction_start(copy, 2);
    CheckCondition(2 ==
                   rocksdb_options_get_access_hint_on_compaction_start(copy));
    CheckCondition(3 == rocksdb_options_get_access_hint_on_compaction_start(o));

    rocksdb_options_set_use_adaptive_mutex(copy, 0);
    CheckCondition(0 == rocksdb_options_get_use_adaptive_mutex(copy));
    CheckCondition(1 == rocksdb_options_get_use_adaptive_mutex(o));

    rocksdb_options_set_bytes_per_sync(copy, 219);
    CheckCondition(219 == rocksdb_options_get_bytes_per_sync(copy));
    CheckCondition(19 == rocksdb_options_get_bytes_per_sync(o));

    rocksdb_options_set_wal_bytes_per_sync(copy, 120);
    CheckCondition(120 == rocksdb_options_get_wal_bytes_per_sync(copy));
    CheckCondition(20 == rocksdb_options_get_wal_bytes_per_sync(o));

    rocksdb_options_set_writable_file_max_buffer_size(copy, 121);
    CheckCondition(121 ==
                   rocksdb_options_get_writable_file_max_buffer_size(copy));
    CheckCondition(21 == rocksdb_options_get_writable_file_max_buffer_size(o));

    rocksdb_options_set_allow_concurrent_memtable_write(copy, 0);
    CheckCondition(0 ==
                   rocksdb_options_get_allow_concurrent_memtable_write(copy));
    CheckCondition(1 == rocksdb_options_get_allow_concurrent_memtable_write(o));

    rocksdb_options_set_enable_write_thread_adaptive_yield(copy, 0);
    CheckCondition(
        0 == rocksdb_options_get_enable_write_thread_adaptive_yield(copy));
    CheckCondition(1 ==
                   rocksdb_options_get_enable_write_thread_adaptive_yield(o));

    rocksdb_options_set_max_sequential_skip_in_iterations(copy, 122);
    CheckCondition(122 ==
                   rocksdb_options_get_max_sequential_skip_in_iterations(copy));
    CheckCondition(22 ==
                   rocksdb_options_get_max_sequential_skip_in_iterations(o));

    rocksdb_options_set_disable_auto_compactions(copy, 0);
    CheckCondition(0 == rocksdb_options_get_disable_auto_compactions(copy));
    CheckCondition(1 == rocksdb_options_get_disable_auto_compactions(o));

    rocksdb_options_set_optimize_filters_for_hits(copy, 0);
    CheckCondition(0 == rocksdb_options_get_optimize_filters_for_hits(copy));
    CheckCondition(1 == rocksdb_options_get_optimize_filters_for_hits(o));

    rocksdb_options_set_delete_obsolete_files_period_micros(copy, 123);
    CheckCondition(
        123 == rocksdb_options_get_delete_obsolete_files_period_micros(copy));
    CheckCondition(23 ==
                   rocksdb_options_get_delete_obsolete_files_period_micros(o));

    rocksdb_options_set_memtable_prefix_bloom_size_ratio(copy, 4.0);
    CheckCondition(4.0 ==
                   rocksdb_options_get_memtable_prefix_bloom_size_ratio(copy));
    CheckCondition(2.0 ==
                   rocksdb_options_get_memtable_prefix_bloom_size_ratio(o));

    rocksdb_options_set_max_compaction_bytes(copy, 124);
    CheckCondition(124 == rocksdb_options_get_max_compaction_bytes(copy));
    CheckCondition(24 == rocksdb_options_get_max_compaction_bytes(o));

    rocksdb_options_set_memtable_huge_page_size(copy, 125);
    CheckCondition(125 == rocksdb_options_get_memtable_huge_page_size(copy));
    CheckCondition(25 == rocksdb_options_get_memtable_huge_page_size(o));

    rocksdb_options_set_max_successive_merges(copy, 126);
    CheckCondition(126 == rocksdb_options_get_max_successive_merges(copy));
    CheckCondition(26 == rocksdb_options_get_max_successive_merges(o));

    rocksdb_options_set_bloom_locality(copy, 127);
    CheckCondition(127 == rocksdb_options_get_bloom_locality(copy));
    CheckCondition(27 == rocksdb_options_get_bloom_locality(o));

    rocksdb_options_set_inplace_update_support(copy, 0);
    CheckCondition(0 == rocksdb_options_get_inplace_update_support(copy));
    CheckCondition(1 == rocksdb_options_get_inplace_update_support(o));

    rocksdb_options_set_inplace_update_num_locks(copy, 128);
    CheckCondition(128 == rocksdb_options_get_inplace_update_num_locks(copy));
    CheckCondition(28 == rocksdb_options_get_inplace_update_num_locks(o));

    rocksdb_options_set_report_bg_io_stats(copy, 0);
    CheckCondition(0 == rocksdb_options_get_report_bg_io_stats(copy));
    CheckCondition(1 == rocksdb_options_get_report_bg_io_stats(o));

    rocksdb_options_set_wal_recovery_mode(copy, 1);
    CheckCondition(1 == rocksdb_options_get_wal_recovery_mode(copy));
    CheckCondition(2 == rocksdb_options_get_wal_recovery_mode(o));

    rocksdb_options_set_compression(copy, 4);
    CheckCondition(4 == rocksdb_options_get_compression(copy));
    CheckCondition(5 == rocksdb_options_get_compression(o));

    rocksdb_options_set_bottommost_compression(copy, 3);
    CheckCondition(3 == rocksdb_options_get_bottommost_compression(copy));
    CheckCondition(4 == rocksdb_options_get_bottommost_compression(o));

    rocksdb_options_set_compaction_style(copy, 1);
    CheckCondition(1 == rocksdb_options_get_compaction_style(copy));
    CheckCondition(2 == rocksdb_options_get_compaction_style(o));

    rocksdb_options_set_atomic_flush(copy, 0);
    CheckCondition(0 == rocksdb_options_get_atomic_flush(copy));
    CheckCondition(1 == rocksdb_options_get_atomic_flush(o));

    rocksdb_options_destroy(copy);
    rocksdb_options_destroy(o);
  }

  StartPhase("read_options");
  {
    rocksdb_readoptions_t* ro;
    ro = rocksdb_readoptions_create();

    rocksdb_readoptions_set_verify_checksums(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_verify_checksums(ro));

    rocksdb_readoptions_set_fill_cache(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_fill_cache(ro));

    rocksdb_readoptions_set_read_tier(ro, 2);
    CheckCondition(2 == rocksdb_readoptions_get_read_tier(ro));

    rocksdb_readoptions_set_tailing(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_tailing(ro));

    rocksdb_readoptions_set_readahead_size(ro, 100);
    CheckCondition(100 == rocksdb_readoptions_get_readahead_size(ro));

    rocksdb_readoptions_set_prefix_same_as_start(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_prefix_same_as_start(ro));

    rocksdb_readoptions_set_pin_data(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_pin_data(ro));

    rocksdb_readoptions_set_total_order_seek(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_total_order_seek(ro));

    rocksdb_readoptions_set_max_skippable_internal_keys(ro, 200);
    CheckCondition(200 ==
                   rocksdb_readoptions_get_max_skippable_internal_keys(ro));

    rocksdb_readoptions_set_background_purge_on_iterator_cleanup(ro, 1);
    CheckCondition(
        1 == rocksdb_readoptions_get_background_purge_on_iterator_cleanup(ro));

    rocksdb_readoptions_set_ignore_range_deletions(ro, 1);
    CheckCondition(1 == rocksdb_readoptions_get_ignore_range_deletions(ro));

    rocksdb_readoptions_set_deadline(ro, 300);
    CheckCondition(300 == rocksdb_readoptions_get_deadline(ro));

    rocksdb_readoptions_set_io_timeout(ro, 400);
    CheckCondition(400 == rocksdb_readoptions_get_io_timeout(ro));

    rocksdb_readoptions_destroy(ro);
  }

  StartPhase("write_options");
  {
    rocksdb_writeoptions_t* wo;
    wo = rocksdb_writeoptions_create();

    rocksdb_writeoptions_set_sync(wo, 1);
    CheckCondition(1 == rocksdb_writeoptions_get_sync(wo));

    rocksdb_writeoptions_disable_WAL(wo, 1);
    CheckCondition(1 == rocksdb_writeoptions_get_disable_WAL(wo));

    rocksdb_writeoptions_set_ignore_missing_column_families(wo, 1);
    CheckCondition(1 ==
                   rocksdb_writeoptions_get_ignore_missing_column_families(wo));

    rocksdb_writeoptions_set_no_slowdown(wo, 1);
    CheckCondition(1 == rocksdb_writeoptions_get_no_slowdown(wo));

    rocksdb_writeoptions_set_low_pri(wo, 1);
    CheckCondition(1 == rocksdb_writeoptions_get_low_pri(wo));

    rocksdb_writeoptions_set_memtable_insert_hint_per_batch(wo, 1);
    CheckCondition(1 ==
                   rocksdb_writeoptions_get_memtable_insert_hint_per_batch(wo));

    rocksdb_writeoptions_destroy(wo);
  }

  StartPhase("compact_options");
  {
    rocksdb_compactoptions_t* co;
    co = rocksdb_compactoptions_create();

    rocksdb_compactoptions_set_exclusive_manual_compaction(co, 1);
    CheckCondition(1 ==
                   rocksdb_compactoptions_get_exclusive_manual_compaction(co));

    rocksdb_compactoptions_set_bottommost_level_compaction(co, 1);
    CheckCondition(1 ==
                   rocksdb_compactoptions_get_bottommost_level_compaction(co));

    rocksdb_compactoptions_set_change_level(co, 1);
    CheckCondition(1 == rocksdb_compactoptions_get_change_level(co));

    rocksdb_compactoptions_set_target_level(co, 1);
    CheckCondition(1 == rocksdb_compactoptions_get_target_level(co));

    rocksdb_compactoptions_destroy(co);
  }

  StartPhase("flush_options");
  {
    rocksdb_flushoptions_t* fo;
    fo = rocksdb_flushoptions_create();

    rocksdb_flushoptions_set_wait(fo, 1);
    CheckCondition(1 == rocksdb_flushoptions_get_wait(fo));

    rocksdb_flushoptions_destroy(fo);
  }

  StartPhase("cache_options");
  {
    rocksdb_cache_t* co;
    co = rocksdb_cache_create_lru(100);
    CheckCondition(100 == rocksdb_cache_get_capacity(co));

    rocksdb_cache_set_capacity(co, 200);
    CheckCondition(200 == rocksdb_cache_get_capacity(co));

    rocksdb_cache_destroy(co);
  }

  StartPhase("env");
  {
    rocksdb_env_t* e;
    e = rocksdb_create_default_env();

    rocksdb_env_set_background_threads(e, 10);
    CheckCondition(10 == rocksdb_env_get_background_threads(e));

    rocksdb_env_set_high_priority_background_threads(e, 20);
    CheckCondition(20 == rocksdb_env_get_high_priority_background_threads(e));

    rocksdb_env_set_low_priority_background_threads(e, 30);
    CheckCondition(30 == rocksdb_env_get_low_priority_background_threads(e));

    rocksdb_env_set_bottom_priority_background_threads(e, 40);
    CheckCondition(40 == rocksdb_env_get_bottom_priority_background_threads(e));

    rocksdb_env_destroy(e);
  }

  StartPhase("universal_compaction_options");
  {
    rocksdb_universal_compaction_options_t* uco;
    uco = rocksdb_universal_compaction_options_create();

    rocksdb_universal_compaction_options_set_size_ratio(uco, 5);
    CheckCondition(5 ==
                   rocksdb_universal_compaction_options_get_size_ratio(uco));

    rocksdb_universal_compaction_options_set_min_merge_width(uco, 15);
    CheckCondition(
        15 == rocksdb_universal_compaction_options_get_min_merge_width(uco));

    rocksdb_universal_compaction_options_set_max_merge_width(uco, 25);
    CheckCondition(
        25 == rocksdb_universal_compaction_options_get_max_merge_width(uco));

    rocksdb_universal_compaction_options_set_max_size_amplification_percent(uco,
                                                                            35);
    CheckCondition(
        35 ==
        rocksdb_universal_compaction_options_get_max_size_amplification_percent(
            uco));

    rocksdb_universal_compaction_options_set_compression_size_percent(uco, 45);
    CheckCondition(
        45 ==
        rocksdb_universal_compaction_options_get_compression_size_percent(uco));

    rocksdb_universal_compaction_options_set_stop_style(uco, 1);
    CheckCondition(1 ==
                   rocksdb_universal_compaction_options_get_stop_style(uco));

    rocksdb_universal_compaction_options_destroy(uco);
  }

  StartPhase("fifo_compaction_options");
  {
    rocksdb_fifo_compaction_options_t* fco;
    fco = rocksdb_fifo_compaction_options_create();

    rocksdb_fifo_compaction_options_set_max_table_files_size(fco, 100000);
    CheckCondition(
        100000 ==
        rocksdb_fifo_compaction_options_get_max_table_files_size(fco));

    rocksdb_fifo_compaction_options_destroy(fco);
  }

  StartPhase("backupable_db_option");
  {
    rocksdb_backupable_db_options_t* bdo;
    bdo = rocksdb_backupable_db_options_create("path");

    rocksdb_backupable_db_options_set_share_table_files(bdo, 1);
    CheckCondition(1 ==
                   rocksdb_backupable_db_options_get_share_table_files(bdo));

    rocksdb_backupable_db_options_set_sync(bdo, 1);
    CheckCondition(1 == rocksdb_backupable_db_options_get_sync(bdo));

    rocksdb_backupable_db_options_set_destroy_old_data(bdo, 1);
    CheckCondition(1 ==
                   rocksdb_backupable_db_options_get_destroy_old_data(bdo));

    rocksdb_backupable_db_options_set_backup_log_files(bdo, 1);
    CheckCondition(1 ==
                   rocksdb_backupable_db_options_get_backup_log_files(bdo));

    rocksdb_backupable_db_options_set_backup_rate_limit(bdo, 123);
    CheckCondition(123 ==
                   rocksdb_backupable_db_options_get_backup_rate_limit(bdo));

    rocksdb_backupable_db_options_set_restore_rate_limit(bdo, 37);
    CheckCondition(37 ==
                   rocksdb_backupable_db_options_get_restore_rate_limit(bdo));

    rocksdb_backupable_db_options_set_max_background_operations(bdo, 20);
    CheckCondition(
        20 == rocksdb_backupable_db_options_get_max_background_operations(bdo));

    rocksdb_backupable_db_options_set_callback_trigger_interval_size(bdo, 9000);
    CheckCondition(
        9000 ==
        rocksdb_backupable_db_options_get_callback_trigger_interval_size(bdo));

    rocksdb_backupable_db_options_set_max_valid_backups_to_open(bdo, 40);
    CheckCondition(
        40 == rocksdb_backupable_db_options_get_max_valid_backups_to_open(bdo));

    rocksdb_backupable_db_options_set_share_files_with_checksum_naming(bdo, 2);
    CheckCondition(
        2 == rocksdb_backupable_db_options_get_share_files_with_checksum_naming(
                 bdo));

    rocksdb_backupable_db_options_destroy(bdo);
  }

  StartPhase("iterate_upper_bound");
  {
    // Create new empty database
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);

    rocksdb_options_set_prefix_extractor(options, NULL);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);

    rocksdb_put(db, woptions, "a",    1, "0",    1, &err); CheckNoError(err);
    rocksdb_put(db, woptions, "foo",  3, "bar",  3, &err); CheckNoError(err);
    rocksdb_put(db, woptions, "foo1", 4, "bar1", 4, &err); CheckNoError(err);
    rocksdb_put(db, woptions, "g1",   2, "0",    1, &err); CheckNoError(err);

    // testing basic case with no iterate_upper_bound and no prefix_extractor
    {
       rocksdb_readoptions_set_iterate_upper_bound(roptions, NULL, 0);
       rocksdb_iterator_t* iter = rocksdb_create_iterator(db, roptions);

       rocksdb_iter_seek(iter, "foo", 3);
       CheckCondition(rocksdb_iter_valid(iter));
       CheckIter(iter, "foo", "bar");

       rocksdb_iter_next(iter);
       CheckCondition(rocksdb_iter_valid(iter));
       CheckIter(iter, "foo1", "bar1");

       rocksdb_iter_next(iter);
       CheckCondition(rocksdb_iter_valid(iter));
       CheckIter(iter, "g1", "0");

       rocksdb_iter_destroy(iter);
    }

    // testing iterate_upper_bound and forward iterator
    // to make sure it stops at bound
    {
       // iterate_upper_bound points beyond the last expected entry
       rocksdb_readoptions_set_iterate_upper_bound(roptions, "foo2", 4);

       rocksdb_iterator_t* iter = rocksdb_create_iterator(db, roptions);

       rocksdb_iter_seek(iter, "foo", 3);
       CheckCondition(rocksdb_iter_valid(iter));
       CheckIter(iter, "foo", "bar");

       rocksdb_iter_next(iter);
       CheckCondition(rocksdb_iter_valid(iter));
       CheckIter(iter, "foo1", "bar1");

       rocksdb_iter_next(iter);
       // should stop here...
       CheckCondition(!rocksdb_iter_valid(iter));

       rocksdb_iter_destroy(iter);
       rocksdb_readoptions_set_iterate_upper_bound(roptions, NULL, 0);
    }
  }

  StartPhase("transactions");
  {
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);

    // open a TransactionDB
    txn_db_options = rocksdb_transactiondb_options_create();
    txn_options = rocksdb_transaction_options_create();
    rocksdb_options_set_create_if_missing(options, 1);
    txn_db = rocksdb_transactiondb_open(options, txn_db_options, dbname, &err);
    CheckNoError(err);

    // put outside a transaction
    rocksdb_transactiondb_put(txn_db, woptions, "foo", 3, "hello", 5, &err);
    CheckNoError(err);
    CheckTxnDBGet(txn_db, roptions, "foo", "hello");

    // delete from outside transaction
    rocksdb_transactiondb_delete(txn_db, woptions, "foo", 3, &err);
    CheckNoError(err);
    CheckTxnDBGet(txn_db, roptions, "foo", NULL);

    // write batch into TransactionDB
    rocksdb_writebatch_t* wb = rocksdb_writebatch_create();
    rocksdb_writebatch_put(wb, "foo", 3, "a", 1);
    rocksdb_writebatch_clear(wb);
    rocksdb_writebatch_put(wb, "bar", 3, "b", 1);
    rocksdb_writebatch_put(wb, "box", 3, "c", 1);
    rocksdb_writebatch_delete(wb, "bar", 3);
    rocksdb_transactiondb_write(txn_db, woptions, wb, &err);
    rocksdb_writebatch_destroy(wb);
    CheckTxnDBGet(txn_db, roptions, "box", "c");
    CheckNoError(err);

    // begin a transaction
    txn = rocksdb_transaction_begin(txn_db, woptions, txn_options, NULL);
    // put
    rocksdb_transaction_put(txn, "foo", 3, "hello", 5, &err);
    CheckNoError(err);
    CheckTxnGet(txn, roptions, "foo", "hello");
    // delete
    rocksdb_transaction_delete(txn, "foo", 3, &err);
    CheckNoError(err);
    CheckTxnGet(txn, roptions, "foo", NULL);

    rocksdb_transaction_put(txn, "foo", 3, "hello", 5, &err);
    CheckNoError(err);

    // read from outside transaction, before commit
    CheckTxnDBGet(txn_db, roptions, "foo", NULL);

    // commit
    rocksdb_transaction_commit(txn, &err);
    CheckNoError(err);

    // read from outside transaction, after commit
    CheckTxnDBGet(txn_db, roptions, "foo", "hello");

    // reuse old transaction
    txn = rocksdb_transaction_begin(txn_db, woptions, txn_options, txn);

    // snapshot
    const rocksdb_snapshot_t* snapshot;
    snapshot = rocksdb_transactiondb_create_snapshot(txn_db);
    rocksdb_readoptions_set_snapshot(roptions, snapshot);

    rocksdb_transactiondb_put(txn_db, woptions, "foo", 3, "hey", 3,  &err);
    CheckNoError(err);

    CheckTxnDBGet(txn_db, roptions, "foo", "hello");
    rocksdb_readoptions_set_snapshot(roptions, NULL);
    rocksdb_transactiondb_release_snapshot(txn_db, snapshot);
    CheckTxnDBGet(txn_db, roptions, "foo", "hey");

    // iterate
    rocksdb_transaction_put(txn, "bar", 3, "hi", 2, &err);
    rocksdb_iterator_t* iter = rocksdb_transaction_create_iterator(txn, roptions);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "bar", "hi");
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);

    // rollback
    rocksdb_transaction_rollback(txn, &err);
    CheckNoError(err);
    CheckTxnDBGet(txn_db, roptions, "bar", NULL);

    // save point
    rocksdb_transaction_put(txn, "foo1", 4, "hi1", 3, &err);
    rocksdb_transaction_set_savepoint(txn);
    CheckTxnGet(txn, roptions, "foo1", "hi1");
    rocksdb_transaction_put(txn, "foo2", 4, "hi2", 3, &err);
    CheckTxnGet(txn, roptions, "foo2", "hi2");

    // rollback to savepoint
    rocksdb_transaction_rollback_to_savepoint(txn, &err);
    CheckNoError(err);
    CheckTxnGet(txn, roptions, "foo2", NULL);
    CheckTxnGet(txn, roptions, "foo1", "hi1");
    CheckTxnDBGet(txn_db, roptions, "foo1", NULL);
    CheckTxnDBGet(txn_db, roptions, "foo2", NULL);
    rocksdb_transaction_commit(txn, &err);
    CheckNoError(err);
    CheckTxnDBGet(txn_db, roptions, "foo1", "hi1");
    CheckTxnDBGet(txn_db, roptions, "foo2", NULL);

    // Column families.
    rocksdb_column_family_handle_t* cfh;
    cfh = rocksdb_transactiondb_create_column_family(txn_db, options,
                                                     "txn_db_cf", &err);
    CheckNoError(err);

    rocksdb_transactiondb_put_cf(txn_db, woptions, cfh, "cf_foo", 6, "cf_hello",
                                 8, &err);
    CheckNoError(err);
    CheckTxnDBGetCF(txn_db, roptions, cfh, "cf_foo", "cf_hello");

    rocksdb_transactiondb_delete_cf(txn_db, woptions, cfh, "cf_foo", 6, &err);
    CheckNoError(err);
    CheckTxnDBGetCF(txn_db, roptions, cfh, "cf_foo", NULL);

    rocksdb_column_family_handle_destroy(cfh);

    // close and destroy
    rocksdb_transaction_destroy(txn);
    rocksdb_transactiondb_close(txn_db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);
    rocksdb_transaction_options_destroy(txn_options);
    rocksdb_transactiondb_options_destroy(txn_db_options);
  }

  StartPhase("optimistic_transactions");
  {
    rocksdb_options_t* db_options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(db_options, 1);
    rocksdb_options_set_allow_concurrent_memtable_write(db_options, 1);
    otxn_db = rocksdb_optimistictransactiondb_open(db_options, dbname, &err);
    otxn_options = rocksdb_optimistictransaction_options_create();
    rocksdb_transaction_t* txn1 = rocksdb_optimistictransaction_begin(
        otxn_db, woptions, otxn_options, NULL);
    rocksdb_transaction_t* txn2 = rocksdb_optimistictransaction_begin(
        otxn_db, woptions, otxn_options, NULL);
    rocksdb_transaction_put(txn1, "key", 3, "value", 5, &err);
    CheckNoError(err);
    rocksdb_transaction_put(txn2, "key1", 4, "value1", 6, &err);
    CheckNoError(err);
    CheckTxnGet(txn1, roptions, "key", "value");
    rocksdb_transaction_commit(txn1, &err);
    CheckNoError(err);
    rocksdb_transaction_commit(txn2, &err);
    CheckNoError(err);
    rocksdb_transaction_destroy(txn1);
    rocksdb_transaction_destroy(txn2);

    // Check column family
    db = rocksdb_optimistictransactiondb_get_base_db(otxn_db);
    rocksdb_put(db, woptions, "key", 3, "value", 5, &err);
    CheckNoError(err);
    rocksdb_column_family_handle_t *cfh1, *cfh2;
    cfh1 = rocksdb_create_column_family(db, db_options, "txn_db_cf1", &err);
    cfh2 = rocksdb_create_column_family(db, db_options, "txn_db_cf2", &err);
    txn = rocksdb_optimistictransaction_begin(otxn_db, woptions, otxn_options,
                                              NULL);
    rocksdb_transaction_put_cf(txn, cfh1, "key_cf1", 7, "val_cf1", 7, &err);
    CheckNoError(err);
    rocksdb_transaction_put_cf(txn, cfh2, "key_cf2", 7, "val_cf2", 7, &err);
    CheckNoError(err);
    rocksdb_transaction_commit(txn, &err);
    CheckNoError(err);
    txn = rocksdb_optimistictransaction_begin(otxn_db, woptions, otxn_options,
                                              txn);
    CheckGetCF(db, roptions, cfh1, "key_cf1", "val_cf1");
    CheckTxnGetCF(txn, roptions, cfh1, "key_cf1", "val_cf1");

    // Check iterator with column family
    rocksdb_transaction_put_cf(txn, cfh1, "key1_cf", 7, "val1_cf", 7, &err);
    CheckNoError(err);
    rocksdb_iterator_t* iter =
        rocksdb_transaction_create_iterator_cf(txn, roptions, cfh1);
    CheckCondition(!rocksdb_iter_valid(iter));
    rocksdb_iter_seek_to_first(iter);
    CheckCondition(rocksdb_iter_valid(iter));
    CheckIter(iter, "key1_cf", "val1_cf");
    rocksdb_iter_get_error(iter, &err);
    CheckNoError(err);
    rocksdb_iter_destroy(iter);

    rocksdb_transaction_destroy(txn);
    rocksdb_column_family_handle_destroy(cfh1);
    rocksdb_column_family_handle_destroy(cfh2);
    rocksdb_optimistictransactiondb_close_base_db(db);
    rocksdb_optimistictransactiondb_close(otxn_db);

    // Check open optimistic transaction db with column families
    size_t cf_len;
    char** column_fams =
        rocksdb_list_column_families(db_options, dbname, &cf_len, &err);
    CheckNoError(err);
    CheckEqual("default", column_fams[0], 7);
    CheckEqual("txn_db_cf1", column_fams[1], 10);
    CheckEqual("txn_db_cf2", column_fams[2], 10);
    CheckCondition(cf_len == 3);
    rocksdb_list_column_families_destroy(column_fams, cf_len);

    const char* cf_names[3] = {"default", "txn_db_cf1", "txn_db_cf2"};
    rocksdb_options_t* cf_options = rocksdb_options_create();
    const rocksdb_options_t* cf_opts[3] = {cf_options, cf_options, cf_options};

    rocksdb_options_set_error_if_exists(cf_options, 0);
    rocksdb_column_family_handle_t* cf_handles[3];
    otxn_db = rocksdb_optimistictransactiondb_open_column_families(
        db_options, dbname, 3, cf_names, cf_opts, cf_handles, &err);
    CheckNoError(err);
    rocksdb_transaction_t* txn_cf = rocksdb_optimistictransaction_begin(
        otxn_db, woptions, otxn_options, NULL);
    CheckTxnGetCF(txn_cf, roptions, cf_handles[0], "key", "value");
    CheckTxnGetCF(txn_cf, roptions, cf_handles[1], "key_cf1", "val_cf1");
    CheckTxnGetCF(txn_cf, roptions, cf_handles[2], "key_cf2", "val_cf2");
    rocksdb_transaction_destroy(txn_cf);
    rocksdb_options_destroy(cf_options);
    rocksdb_column_family_handle_destroy(cf_handles[0]);
    rocksdb_column_family_handle_destroy(cf_handles[1]);
    rocksdb_column_family_handle_destroy(cf_handles[2]);
    rocksdb_optimistictransactiondb_close(otxn_db);
    rocksdb_destroy_db(db_options, dbname, &err);
    rocksdb_options_destroy(db_options);
    rocksdb_optimistictransaction_options_destroy(otxn_options);
    CheckNoError(err);
  }

  // Simple sanity check that setting memtable rep works.
  StartPhase("memtable_reps");
  {
    // Create database with vector memtable.
    rocksdb_options_set_memtable_vector_rep(options);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);

    // Create database with hash skiplist memtable.
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);
    CheckNoError(err);

    rocksdb_options_set_hash_skip_list_rep(options, 5000, 4, 4);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
  }

  // Check that secondary instance works.
  StartPhase("open_as_secondary");
  {
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);

    rocksdb_options_t* db_options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(db_options, 1);
    db = rocksdb_open(db_options, dbname, &err);
    CheckNoError(err);
    rocksdb_t* db1;
    rocksdb_options_t* opts = rocksdb_options_create();
    rocksdb_options_set_max_open_files(opts, -1);
    rocksdb_options_set_create_if_missing(opts, 1);
    snprintf(secondary_path, sizeof(secondary_path),
             "%s/rocksdb_c_test_secondary-%d", GetTempDir(), ((int)geteuid()));
    db1 = rocksdb_open_as_secondary(opts, dbname, secondary_path, &err);
    CheckNoError(err);

    rocksdb_writeoptions_set_sync(woptions, 0);
    rocksdb_writeoptions_disable_WAL(woptions, 1);
    rocksdb_put(db, woptions, "key0", 4, "value0", 6, &err);
    CheckNoError(err);
    rocksdb_flushoptions_t* flush_opts = rocksdb_flushoptions_create();
    rocksdb_flushoptions_set_wait(flush_opts, 1);
    rocksdb_flush(db, flush_opts, &err);
    CheckNoError(err);
    rocksdb_try_catch_up_with_primary(db1, &err);
    CheckNoError(err);
    rocksdb_readoptions_t* ropts = rocksdb_readoptions_create();
    rocksdb_readoptions_set_verify_checksums(ropts, 1);
    rocksdb_readoptions_set_snapshot(ropts, NULL);
    CheckGet(db, ropts, "key0", "value0");
    CheckGet(db1, ropts, "key0", "value0");

    rocksdb_writeoptions_disable_WAL(woptions, 0);
    rocksdb_put(db, woptions, "key1", 4, "value1", 6, &err);
    CheckNoError(err);
    rocksdb_try_catch_up_with_primary(db1, &err);
    CheckNoError(err);
    CheckGet(db1, ropts, "key0", "value0");
    CheckGet(db1, ropts, "key1", "value1");

    rocksdb_close(db1);
    rocksdb_destroy_db(opts, secondary_path, &err);
    CheckNoError(err);

    rocksdb_options_destroy(db_options);
    rocksdb_options_destroy(opts);
    rocksdb_readoptions_destroy(ropts);
    rocksdb_flushoptions_destroy(flush_opts);
  }

  // Simple sanity check that options setting db_paths work.
  StartPhase("open_db_paths");
  {
    rocksdb_close(db);
    rocksdb_destroy_db(options, dbname, &err);

    const rocksdb_dbpath_t* paths[1] = {dbpath};
    rocksdb_options_set_db_paths(options, paths, 1);
    db = rocksdb_open(options, dbname, &err);
    CheckNoError(err);
  }

  StartPhase("cancel_all_background_work");
  rocksdb_cancel_all_background_work(db, 1);

  StartPhase("cleanup");
  rocksdb_close(db);
  rocksdb_options_destroy(options);
  rocksdb_block_based_options_destroy(table_options);
  rocksdb_readoptions_destroy(roptions);
  rocksdb_writeoptions_destroy(woptions);
  rocksdb_compactoptions_destroy(coptions);
  rocksdb_cache_destroy(cache);
  rocksdb_comparator_destroy(cmp);
  rocksdb_dbpath_destroy(dbpath);
  rocksdb_env_destroy(env);

  fprintf(stderr, "PASS\n");
  return 0;
}

#else

int main(void) {
  fprintf(stderr, "SKIPPED\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
