//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/c.h"

#include <stdlib.h>
#include <unistd.h>
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/universal_compaction.h"

using rocksdb::Cache;
using rocksdb::Comparator;
using rocksdb::CompressionType;
using rocksdb::DB;
using rocksdb::Env;
using rocksdb::FileLock;
using rocksdb::FilterPolicy;
using rocksdb::Iterator;
using rocksdb::Logger;
using rocksdb::NewBloomFilterPolicy;
using rocksdb::NewLRUCache;
using rocksdb::Options;
using rocksdb::RandomAccessFile;
using rocksdb::Range;
using rocksdb::ReadOptions;
using rocksdb::SequentialFile;
using rocksdb::Slice;
using rocksdb::Snapshot;
using rocksdb::Status;
using rocksdb::WritableFile;
using rocksdb::WriteBatch;
using rocksdb::WriteOptions;

using std::shared_ptr;

extern "C" {

struct rocksdb_t              { DB*               rep; };
struct rocksdb_iterator_t     { Iterator*         rep; };
struct rocksdb_writebatch_t   { WriteBatch        rep; };
struct rocksdb_snapshot_t     { const Snapshot*   rep; };
struct rocksdb_readoptions_t  { ReadOptions       rep; };
struct rocksdb_writeoptions_t { WriteOptions      rep; };
struct rocksdb_options_t      { Options           rep; };
struct rocksdb_seqfile_t      { SequentialFile*   rep; };
struct rocksdb_randomfile_t   { RandomAccessFile* rep; };
struct rocksdb_writablefile_t { WritableFile*     rep; };
struct rocksdb_filelock_t     { FileLock*         rep; };
struct rocksdb_logger_t       { shared_ptr<Logger>  rep; };
struct rocksdb_cache_t        { shared_ptr<Cache>   rep; };

struct rocksdb_comparator_t : public Comparator {
  void* state_;
  void (*destructor_)(void*);
  int (*compare_)(
      void*,
      const char* a, size_t alen,
      const char* b, size_t blen);
  const char* (*name_)(void*);

  virtual ~rocksdb_comparator_t() {
    (*destructor_)(state_);
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return (*compare_)(state_, a.data(), a.size(), b.data(), b.size());
  }

  virtual const char* Name() const {
    return (*name_)(state_);
  }

  // No-ops since the C binding does not support key shortening methods.
  virtual void FindShortestSeparator(std::string*, const Slice&) const { }
  virtual void FindShortSuccessor(std::string* key) const { }
};

struct rocksdb_filterpolicy_t : public FilterPolicy {
  void* state_;
  void (*destructor_)(void*);
  const char* (*name_)(void*);
  char* (*create_)(
      void*,
      const char* const* key_array, const size_t* key_length_array,
      int num_keys,
      size_t* filter_length);
  unsigned char (*key_match_)(
      void*,
      const char* key, size_t length,
      const char* filter, size_t filter_length);

  virtual ~rocksdb_filterpolicy_t() {
    (*destructor_)(state_);
  }

  virtual const char* Name() const {
    return (*name_)(state_);
  }

  virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const {
    std::vector<const char*> key_pointers(n);
    std::vector<size_t> key_sizes(n);
    for (int i = 0; i < n; i++) {
      key_pointers[i] = keys[i].data();
      key_sizes[i] = keys[i].size();
    }
    size_t len;
    char* filter = (*create_)(state_, &key_pointers[0], &key_sizes[0], n, &len);
    dst->append(filter, len);
    free(filter);
  }

  virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const {
    return (*key_match_)(state_, key.data(), key.size(),
                         filter.data(), filter.size());
  }
};

struct rocksdb_env_t {
  Env* rep;
  bool is_default;
};

struct rocksdb_universal_compaction_options_t {
  rocksdb::CompactionOptionsUniversal *rep;
};


static bool SaveError(char** errptr, const Status& s) {
  assert(errptr != NULL);
  if (s.ok()) {
    return false;
  } else if (*errptr == NULL) {
    *errptr = strdup(s.ToString().c_str());
  } else {
    // TODO(sanjay): Merge with existing error?
    free(*errptr);
    *errptr = strdup(s.ToString().c_str());
  }
  return true;
}

static char* CopyString(const std::string& str) {
  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

rocksdb_t* rocksdb_open(
    const rocksdb_options_t* options,
    const char* name,
    char** errptr) {
  DB* db;
  if (SaveError(errptr, DB::Open(options->rep, std::string(name), &db))) {
    return NULL;
  }
  rocksdb_t* result = new rocksdb_t;
  result->rep = db;
  return result;
}

void rocksdb_close(rocksdb_t* db) {
  delete db->rep;
  delete db;
}

void rocksdb_put(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    const char* key, size_t keylen,
    const char* val, size_t vallen,
    char** errptr) {
  SaveError(errptr,
            db->rep->Put(options->rep, Slice(key, keylen), Slice(val, vallen)));
}

void rocksdb_delete(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    const char* key, size_t keylen,
    char** errptr) {
  SaveError(errptr, db->rep->Delete(options->rep, Slice(key, keylen)));
}


void rocksdb_write(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    rocksdb_writebatch_t* batch,
    char** errptr) {
  SaveError(errptr, db->rep->Write(options->rep, &batch->rep));
}

char* rocksdb_get(
    rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = NULL;
  std::string tmp;
  Status s = db->rep->Get(options->rep, Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

rocksdb_iterator_t* rocksdb_create_iterator(
    rocksdb_t* db,
    const rocksdb_readoptions_t* options) {
  rocksdb_iterator_t* result = new rocksdb_iterator_t;
  result->rep = db->rep->NewIterator(options->rep);
  return result;
}

const rocksdb_snapshot_t* rocksdb_create_snapshot(
    rocksdb_t* db) {
  rocksdb_snapshot_t* result = new rocksdb_snapshot_t;
  result->rep = db->rep->GetSnapshot();
  return result;
}

void rocksdb_release_snapshot(
    rocksdb_t* db,
    const rocksdb_snapshot_t* snapshot) {
  db->rep->ReleaseSnapshot(snapshot->rep);
  delete snapshot;
}

char* rocksdb_property_value(
    rocksdb_t* db,
    const char* propname) {
  std::string tmp;
  if (db->rep->GetProperty(Slice(propname), &tmp)) {
    // We use strdup() since we expect human readable output.
    return strdup(tmp.c_str());
  } else {
    return NULL;
  }
}

void rocksdb_approximate_sizes(
    rocksdb_t* db,
    int num_ranges,
    const char* const* range_start_key, const size_t* range_start_key_len,
    const char* const* range_limit_key, const size_t* range_limit_key_len,
    uint64_t* sizes) {
  Range* ranges = new Range[num_ranges];
  for (int i = 0; i < num_ranges; i++) {
    ranges[i].start = Slice(range_start_key[i], range_start_key_len[i]);
    ranges[i].limit = Slice(range_limit_key[i], range_limit_key_len[i]);
  }
  db->rep->GetApproximateSizes(ranges, num_ranges, sizes);
  delete[] ranges;
}

void rocksdb_compact_range(
    rocksdb_t* db,
    const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      // Pass NULL Slice if corresponding "const char*" is NULL
      (start_key ? (a = Slice(start_key, start_key_len), &a) : NULL),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : NULL));
}

void rocksdb_destroy_db(
    const rocksdb_options_t* options,
    const char* name,
    char** errptr) {
  SaveError(errptr, DestroyDB(name, options->rep));
}

void rocksdb_repair_db(
    const rocksdb_options_t* options,
    const char* name,
    char** errptr) {
  SaveError(errptr, RepairDB(name, options->rep));
}

void rocksdb_iter_destroy(rocksdb_iterator_t* iter) {
  delete iter->rep;
  delete iter;
}

unsigned char rocksdb_iter_valid(const rocksdb_iterator_t* iter) {
  return iter->rep->Valid();
}

void rocksdb_iter_seek_to_first(rocksdb_iterator_t* iter) {
  iter->rep->SeekToFirst();
}

void rocksdb_iter_seek_to_last(rocksdb_iterator_t* iter) {
  iter->rep->SeekToLast();
}

void rocksdb_iter_seek(rocksdb_iterator_t* iter, const char* k, size_t klen) {
  iter->rep->Seek(Slice(k, klen));
}

void rocksdb_iter_next(rocksdb_iterator_t* iter) {
  iter->rep->Next();
}

void rocksdb_iter_prev(rocksdb_iterator_t* iter) {
  iter->rep->Prev();
}

const char* rocksdb_iter_key(const rocksdb_iterator_t* iter, size_t* klen) {
  Slice s = iter->rep->key();
  *klen = s.size();
  return s.data();
}

const char* rocksdb_iter_value(const rocksdb_iterator_t* iter, size_t* vlen) {
  Slice s = iter->rep->value();
  *vlen = s.size();
  return s.data();
}

void rocksdb_iter_get_error(const rocksdb_iterator_t* iter, char** errptr) {
  SaveError(errptr, iter->rep->status());
}

rocksdb_writebatch_t* rocksdb_writebatch_create() {
  return new rocksdb_writebatch_t;
}

void rocksdb_writebatch_destroy(rocksdb_writebatch_t* b) {
  delete b;
}

void rocksdb_writebatch_clear(rocksdb_writebatch_t* b) {
  b->rep.Clear();
}

void rocksdb_writebatch_put(
    rocksdb_writebatch_t* b,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep.Put(Slice(key, klen), Slice(val, vlen));
}

void rocksdb_writebatch_delete(
    rocksdb_writebatch_t* b,
    const char* key, size_t klen) {
  b->rep.Delete(Slice(key, klen));
}

void rocksdb_writebatch_iterate(
    rocksdb_writebatch_t* b,
    void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen)) {
  class H : public WriteBatch::Handler {
   public:
    void* state_;
    void (*put_)(void*, const char* k, size_t klen, const char* v, size_t vlen);
    void (*deleted_)(void*, const char* k, size_t klen);
    virtual void Put(const Slice& key, const Slice& value) {
      (*put_)(state_, key.data(), key.size(), value.data(), value.size());
    }
    virtual void Delete(const Slice& key) {
      (*deleted_)(state_, key.data(), key.size());
    }
  };
  H handler;
  handler.state_ = state;
  handler.put_ = put;
  handler.deleted_ = deleted;
  b->rep.Iterate(&handler);
}

rocksdb_options_t* rocksdb_options_create() {
  return new rocksdb_options_t;
}

void rocksdb_options_destroy(rocksdb_options_t* options) {
  delete options;
}

void rocksdb_options_set_comparator(
    rocksdb_options_t* opt,
    rocksdb_comparator_t* cmp) {
  opt->rep.comparator = cmp;
}

void rocksdb_options_set_filter_policy(
    rocksdb_options_t* opt,
    rocksdb_filterpolicy_t* policy) {
  opt->rep.filter_policy = policy;
}

void rocksdb_options_set_create_if_missing(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.create_if_missing = v;
}

void rocksdb_options_set_error_if_exists(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.error_if_exists = v;
}

void rocksdb_options_set_paranoid_checks(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.paranoid_checks = v;
}

void rocksdb_options_set_env(rocksdb_options_t* opt, rocksdb_env_t* env) {
  opt->rep.env = (env ? env->rep : NULL);
}

void rocksdb_options_set_info_log(rocksdb_options_t* opt, rocksdb_logger_t* l) {
  if (l) {
    opt->rep.info_log = l->rep;
  }
}

void rocksdb_options_set_write_buffer_size(rocksdb_options_t* opt, size_t s) {
  opt->rep.write_buffer_size = s;
}

void rocksdb_options_set_max_open_files(rocksdb_options_t* opt, int n) {
  opt->rep.max_open_files = n;
}

void rocksdb_options_set_cache(rocksdb_options_t* opt, rocksdb_cache_t* c) {
  if (c) {
    opt->rep.block_cache = c->rep;
  }
}

void rocksdb_options_set_block_size(rocksdb_options_t* opt, size_t s) {
  opt->rep.block_size = s;
}

void rocksdb_options_set_block_restart_interval(rocksdb_options_t* opt, int n) {
  opt->rep.block_restart_interval = n;
}

void rocksdb_options_set_target_file_size_base(
    rocksdb_options_t* opt, uint64_t n) {
  opt->rep.target_file_size_base = n;
}

void rocksdb_options_set_target_file_size_multiplier(
    rocksdb_options_t* opt, int n) {
  opt->rep.target_file_size_multiplier = n;
}

void rocksdb_options_set_max_bytes_for_level_base(
    rocksdb_options_t* opt, uint64_t n) {
  opt->rep.max_bytes_for_level_base = n;
}

void rocksdb_options_set_max_bytes_for_level_multiplier(
    rocksdb_options_t* opt, int n) {
  opt->rep.max_bytes_for_level_multiplier = n;
}

void rocksdb_options_set_expanded_compaction_factor(
    rocksdb_options_t* opt, int n) {
  opt->rep.expanded_compaction_factor = n;
}

void rocksdb_options_set_max_grandparent_overlap_factor(
    rocksdb_options_t* opt, int n) {
  opt->rep.max_grandparent_overlap_factor = n;
}

void rocksdb_options_set_num_levels(rocksdb_options_t* opt, int n) {
  opt->rep.num_levels = n;
}

void rocksdb_options_set_level0_file_num_compaction_trigger(
    rocksdb_options_t* opt, int n) {
  opt->rep.level0_file_num_compaction_trigger = n;
}

void rocksdb_options_set_level0_slowdown_writes_trigger(
    rocksdb_options_t* opt, int n) {
  opt->rep.level0_slowdown_writes_trigger = n;
}

void rocksdb_options_set_level0_stop_writes_trigger(
    rocksdb_options_t* opt, int n) {
  opt->rep.level0_stop_writes_trigger = n;
}

void rocksdb_options_set_max_mem_compaction_level(
    rocksdb_options_t* opt, int n) {
  opt->rep.max_mem_compaction_level = n;
}

void rocksdb_options_set_compression(rocksdb_options_t* opt, int t) {
  opt->rep.compression = static_cast<CompressionType>(t);
}

void rocksdb_options_set_compression_per_level(rocksdb_options_t* opt,
                                               int* level_values,
                                               size_t num_levels) {
  opt->rep.compression_per_level.resize(num_levels);
  for (size_t i = 0; i < num_levels; ++i) {
    opt->rep.compression_per_level[i] =
      static_cast<CompressionType>(level_values[i]);
  }
}

void rocksdb_options_set_compression_options(
    rocksdb_options_t* opt, int w_bits, int level, int strategy) {
  opt->rep.compression_opts.window_bits = w_bits;
  opt->rep.compression_opts.level = level;
  opt->rep.compression_opts.strategy = strategy;
}

void rocksdb_options_set_disable_data_sync(
    rocksdb_options_t* opt, int disable_data_sync) {
  opt->rep.disableDataSync = disable_data_sync;
}

void rocksdb_options_set_use_fsync(
    rocksdb_options_t* opt, int use_fsync) {
  opt->rep.use_fsync = use_fsync;
}

void rocksdb_options_set_db_stats_log_interval(
    rocksdb_options_t* opt, int db_stats_log_interval) {
  opt->rep.db_stats_log_interval = db_stats_log_interval;
}

void rocksdb_options_set_db_log_dir(
    rocksdb_options_t* opt, const char* db_log_dir) {
  opt->rep.db_log_dir = db_log_dir;
}

void rocksdb_options_set_WAL_ttl_seconds(rocksdb_options_t* opt, uint64_t ttl) {
  opt->rep.WAL_ttl_seconds = ttl;
}

void rocksdb_options_set_WAL_size_limit_MB(
    rocksdb_options_t* opt, uint64_t limit) {
  opt->rep.WAL_size_limit_MB = limit;
}

void rocksdb_options_set_max_write_buffer_number(rocksdb_options_t* opt, int n) {
  opt->rep.max_write_buffer_number = n;
}

void rocksdb_options_set_min_write_buffer_number_to_merge(rocksdb_options_t* opt, int n) {
  opt->rep.min_write_buffer_number_to_merge = n;
}

void rocksdb_options_set_max_background_compactions(rocksdb_options_t* opt, int n) {
  opt->rep.max_background_compactions = n;
}

void rocksdb_options_set_max_background_flushes(rocksdb_options_t* opt, int n) {
  opt->rep.max_background_flushes = n;
}

void rocksdb_options_set_disable_auto_compactions(rocksdb_options_t* opt, int disable) {
  opt->rep.disable_auto_compactions = disable;
}

void rocksdb_options_set_disable_seek_compaction(rocksdb_options_t* opt, int disable) {
  opt->rep.disable_seek_compaction = disable;
}

void rocksdb_options_set_source_compaction_factor(
    rocksdb_options_t* opt, int n) {
  opt->rep.expanded_compaction_factor = n;
}

void rocksdb_options_prepare_for_bulk_load(rocksdb_options_t* opt) {
  opt->rep.PrepareForBulkLoad();
}

void rocksdb_options_set_memtable_vector_rep(rocksdb_options_t *opt) {
  static rocksdb::VectorRepFactory* factory = 0;
  if (!factory) {
    factory = new rocksdb::VectorRepFactory;
  }
  opt->rep.memtable_factory.reset(factory);
}

void rocksdb_options_set_compaction_style(rocksdb_options_t *opt, int style) {
  opt->rep.compaction_style = static_cast<rocksdb::CompactionStyle>(style);
}

void rocksdb_options_set_universal_compaction_options(rocksdb_options_t *opt, rocksdb_universal_compaction_options_t *uco) {
  opt->rep.compaction_options_universal = *(uco->rep);
}

/*
TODO:
merge_operator
compaction_filter
prefix_extractor
whole_key_filtering
max_bytes_for_level_multiplier_additional
delete_obsolete_files_period_micros
max_log_file_size
log_file_time_to_roll
keep_log_file_num
soft_rate_limit
hard_rate_limit
rate_limit_delay_max_milliseconds
max_manifest_file_size
no_block_cache
table_cache_numshardbits
table_cache_remove_scan_count_limit
arena_block_size
manifest_preallocation_size
purge_redundant_kvs_while_flush
allow_os_buffer
allow_mmap_reads
allow_mmap_writes
is_fd_close_on_exec
skip_log_error_on_recovery
stats_dump_period_sec
block_size_deviation
advise_random_on_open
access_hint_on_compaction_start
use_adaptive_mutex
bytes_per_sync
filter_deletes
max_sequential_skip_in_iterations
table_factory
table_properties_collectors
inplace_update_support
inplace_update_num_locks
*/

rocksdb_comparator_t* rocksdb_comparator_create(
    void* state,
    void (*destructor)(void*),
    int (*compare)(
        void*,
        const char* a, size_t alen,
        const char* b, size_t blen),
    const char* (*name)(void*)) {
  rocksdb_comparator_t* result = new rocksdb_comparator_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->compare_ = compare;
  result->name_ = name;
  return result;
}

void rocksdb_comparator_destroy(rocksdb_comparator_t* cmp) {
  delete cmp;
}

rocksdb_filterpolicy_t* rocksdb_filterpolicy_create(
    void* state,
    void (*destructor)(void*),
    char* (*create_filter)(
        void*,
        const char* const* key_array, const size_t* key_length_array,
        int num_keys,
        size_t* filter_length),
    unsigned char (*key_may_match)(
        void*,
        const char* key, size_t length,
        const char* filter, size_t filter_length),
    const char* (*name)(void*)) {
  rocksdb_filterpolicy_t* result = new rocksdb_filterpolicy_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->create_ = create_filter;
  result->key_match_ = key_may_match;
  result->name_ = name;
  return result;
}

void rocksdb_filterpolicy_destroy(rocksdb_filterpolicy_t* filter) {
  delete filter;
}

rocksdb_filterpolicy_t* rocksdb_filterpolicy_create_bloom(int bits_per_key) {
  // Make a rocksdb_filterpolicy_t, but override all of its methods so
  // they delegate to a NewBloomFilterPolicy() instead of user
  // supplied C functions.
  struct Wrapper : public rocksdb_filterpolicy_t {
    const FilterPolicy* rep_;
    ~Wrapper() { delete rep_; }
    const char* Name() const { return rep_->Name(); }
    void CreateFilter(const Slice* keys, int n, std::string* dst) const {
      return rep_->CreateFilter(keys, n, dst);
    }
    bool KeyMayMatch(const Slice& key, const Slice& filter) const {
      return rep_->KeyMayMatch(key, filter);
    }
    static void DoNothing(void*) { }
  };
  Wrapper* wrapper = new Wrapper;
  wrapper->rep_ = NewBloomFilterPolicy(bits_per_key);
  wrapper->state_ = NULL;
  wrapper->destructor_ = &Wrapper::DoNothing;
  return wrapper;
}

rocksdb_readoptions_t* rocksdb_readoptions_create() {
  return new rocksdb_readoptions_t;
}

void rocksdb_readoptions_destroy(rocksdb_readoptions_t* opt) {
  delete opt;
}

void rocksdb_readoptions_set_verify_checksums(
    rocksdb_readoptions_t* opt,
    unsigned char v) {
  opt->rep.verify_checksums = v;
}

void rocksdb_readoptions_set_fill_cache(
    rocksdb_readoptions_t* opt, unsigned char v) {
  opt->rep.fill_cache = v;
}

void rocksdb_readoptions_set_snapshot(
    rocksdb_readoptions_t* opt,
    const rocksdb_snapshot_t* snap) {
  opt->rep.snapshot = (snap ? snap->rep : NULL);
}

rocksdb_writeoptions_t* rocksdb_writeoptions_create() {
  return new rocksdb_writeoptions_t;
}

void rocksdb_writeoptions_destroy(rocksdb_writeoptions_t* opt) {
  delete opt;
}

void rocksdb_writeoptions_set_sync(
    rocksdb_writeoptions_t* opt, unsigned char v) {
  opt->rep.sync = v;
}

void rocksdb_writeoptions_disable_WAL(rocksdb_writeoptions_t* opt, int disable) {
  opt->rep.disableWAL = disable;
}


rocksdb_cache_t* rocksdb_cache_create_lru(size_t capacity) {
  rocksdb_cache_t* c = new rocksdb_cache_t;
  c->rep = NewLRUCache(capacity);
  return c;
}

void rocksdb_cache_destroy(rocksdb_cache_t* cache) {
  delete cache;
}

rocksdb_env_t* rocksdb_create_default_env() {
  rocksdb_env_t* result = new rocksdb_env_t;
  result->rep = Env::Default();
  result->is_default = true;
  return result;
}

void rocksdb_env_set_background_threads(rocksdb_env_t* env, int n) {
  env->rep->SetBackgroundThreads(n);
}

void rocksdb_env_set_high_priority_background_threads(rocksdb_env_t* env, int n) {
  env->rep->SetBackgroundThreads(n, Env::HIGH);
}

void rocksdb_env_destroy(rocksdb_env_t* env) {
  if (!env->is_default) delete env->rep;
  delete env;
}

rocksdb_universal_compaction_options_t* rocksdb_universal_compaction_options_create() {
  rocksdb_universal_compaction_options_t* result = new rocksdb_universal_compaction_options_t;
  result->rep = new rocksdb::CompactionOptionsUniversal;
  return result;
}

void rocksdb_universal_compaction_options_set_size_ratio(
  rocksdb_universal_compaction_options_t* uco, int ratio) {
  uco->rep->size_ratio = ratio;
}

void rocksdb_universal_compaction_options_set_min_merge_width(
  rocksdb_universal_compaction_options_t* uco, int w) {
  uco->rep->min_merge_width = w;
}

void rocksdb_universal_compaction_options_set_max_merge_width(
  rocksdb_universal_compaction_options_t* uco, int w) {
  uco->rep->max_merge_width = w;
}

void rocksdb_universal_compaction_options_set_max_size_amplification_percent(
  rocksdb_universal_compaction_options_t* uco, int p) {
  uco->rep->max_size_amplification_percent = p;
}

void rocksdb_universal_compaction_options_set_compression_size_percent(
  rocksdb_universal_compaction_options_t* uco, int p) {
  uco->rep->compression_size_percent = p;
}

void rocksdb_universal_compaction_options_set_stop_style(
  rocksdb_universal_compaction_options_t* uco, int style) {
  uco->rep->stop_style = static_cast<rocksdb::CompactionStopStyle>(style);
}

void rocksdb_universal_compaction_options_destroy(
  rocksdb_universal_compaction_options_t* uco) {
  delete uco->rep;
  delete uco;
}

}  // end extern "C"
