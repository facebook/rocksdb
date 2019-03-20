#include "rocksdb/utilities/titan_c.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/titandb/db.h"

using rocksdb::Cache;
using rocksdb::CompressionType;
using rocksdb::DB;
using rocksdb::Options;
using rocksdb::Status;
using rocksdb::titandb::TitanDB;
using rocksdb::titandb::TitanOptions;

extern "C" {

struct rocksdb_t {
  DB* rep;
};
struct rocksdb_options_t {
  Options rep;
};
struct rocksdb_cache_t {
  std::shared_ptr<Cache> rep;
};

struct titandb_options_t {
  TitanOptions rep;
};

static bool SaveError(char** errptr, const Status& s) {
  assert(errptr != nullptr);
  if (s.ok()) {
    return false;
  } else if (*errptr == nullptr) {
    *errptr = strdup(s.ToString().c_str());
  } else {
    // TODO(sanjay): Merge with existing error?
    // This is a bug if *errptr is not created by malloc()
    free(*errptr);
    *errptr = strdup(s.ToString().c_str());
  }
  return true;
}

rocksdb_t* titandb_open(const titandb_options_t* options, const char* name,
                        char** errptr) {
  TitanDB* db;
  if (SaveError(errptr, TitanDB::Open(options->rep, name, &db))) {
    return nullptr;
  }
  rocksdb_t* result = new rocksdb_t;
  result->rep = db;
  return result;
}

titandb_options_t* titandb_options_create() { return new titandb_options_t; }

void titandb_options_destroy(titandb_options_t* options) { delete options; }

void titandb_options_set_rocksdb(titandb_options_t* options,
                                 rocksdb_options_t* rocksdb) {
  options->rep = TitanOptions(rocksdb->rep);
}

void titandb_options_set_dirname(titandb_options_t* options, const char* name) {
  options->rep.dirname = name;
}

void titandb_options_set_min_blob_size(titandb_options_t* options,
                                       uint64_t size) {
  options->rep.min_blob_size = size;
}

void titandb_options_set_blob_file_compression(titandb_options_t* options,
                                               int compression) {
  options->rep.blob_file_compression =
      static_cast<CompressionType>(compression);
}

void titandb_options_set_blob_cache(titandb_options_t* options,
                                    rocksdb_cache_t* blob_cache) {
  if (blob_cache) {
    options->rep.blob_cache = blob_cache->rep;
  } else {
    options->rep.blob_cache.reset();
  }
}

void titandb_options_set_disable_background_gc(titandb_options_t* options,
                                               unsigned char disable) {
  options->rep.disable_background_gc = disable;
}

void titandb_options_set_max_gc_batch_size(titandb_options_t* options,
                                           uint64_t size) {
  options->rep.max_gc_batch_size = size;
}

void titandb_options_set_min_gc_batch_size(titandb_options_t* options,
                                           uint64_t size) {
  options->rep.min_gc_batch_size = size;
}

void titandb_options_set_blob_file_discardable_ratio(titandb_options_t* options,
                                                     float ratio) {
  options->rep.blob_file_discardable_ratio = ratio;
}

void titandb_options_set_sample_file_size_ratio(titandb_options_t* options,
                                                float ratio) {
  options->rep.sample_file_size_ratio = ratio;
}

void titandb_options_set_merge_small_file_threshold(titandb_options_t* options,
                                                    uint64_t size) {
  options->rep.merge_small_file_threshold = size;
}

}  // end extern "C"
