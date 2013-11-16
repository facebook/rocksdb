//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gflags/gflags.h>

#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/slice_transform.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "port/atomic_pointer.h"
#include "table/block_based_table_factory.h"
#include "util/histogram.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {
// Make a key that i determines the first 4 characters and j determines the
// last 4 characters.
static std::string MakeKey(int i, int j, bool through_db) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%04d__key___%04d", i, j);
  if (through_db) {
    return std::string(buf);
  }
  // If we directly query table, which operates on internal keys
  // instead of user keys, we need to add 8 bytes of internal
  // information (row type etc) to user key to make an internal
  // key.
  InternalKey key(std::string(buf), 0, ValueType::kTypeValue);
  return key.Encode().ToString();
}

static bool DummySaveValue(void* arg, const Slice& ikey, const Slice& v,
                           bool didIO) {
  return false;
}

// A very simple benchmark that.
// Create a table with roughly numKey1 * numKey2 keys,
// where there are numKey1 prefixes of the key, each has numKey2 number of
// distinguished key, differing in the suffix part.
// If if_query_empty_keys = false, query the existing keys numKey1 * numKey2
// times randomly.
// If if_query_empty_keys = true, query numKey1 * numKey2 random empty keys.
// Print out the total time.
// If through_db=true, a full DB will be created and queries will be against
// it. Otherwise, operations will be directly through table level.
//
// If for_terator=true, instead of just query one key each time, it queries
// a range sharing the same prefix.
void TableReaderBenchmark(Options& opts, EnvOptions& env_options,
                          ReadOptions& read_options, int num_keys1,
                          int num_keys2, int num_iter, int prefix_len,
                          bool if_query_empty_keys, bool for_iterator,
                          bool through_db) {
  Slice prefix = Slice();

  std::string file_name = test::TmpDir()
      + "/rocksdb_table_reader_benchmark";
  std::string dbname = test::TmpDir() + "/rocksdb_table_reader_bench_db";
  ReadOptions ro;
  WriteOptions wo;
  unique_ptr<WritableFile> file;
  Env* env = Env::Default();
  TableBuilder* tb = nullptr;
  DB* db = nullptr;
  Status s;
  if (!through_db) {
    env->NewWritableFile(file_name, &file, env_options);
    tb = opts.table_factory->GetTableBuilder(opts, file.get(),
                                             CompressionType::kNoCompression);
  } else {
    s = DB::Open(opts, dbname, &db);
    ASSERT_OK(s);
    ASSERT_TRUE(db != nullptr);
  }
  // Populate slightly more than 1M keys
  for (int i = 0; i < num_keys1; i++) {
    for (int j = 0; j < num_keys2; j++) {
      std::string key = MakeKey(i * 2, j, through_db);
      if (!through_db) {
        tb->Add(key, key);
      } else {
        db->Put(wo, key, key);
      }
    }
  }
  if (!through_db) {
    tb->Finish();
    file->Close();
  } else {
    db->Flush(FlushOptions());
  }

  unique_ptr<TableReader> table_reader;
  unique_ptr<RandomAccessFile> raf;
  if (!through_db) {
    Status s = env->NewRandomAccessFile(file_name, &raf, env_options);
    uint64_t file_size;
    env->GetFileSize(file_name, &file_size);
    s = opts.table_factory->GetTableReader(opts, env_options, std::move(raf),
                                           file_size, &table_reader);
  }

  Random rnd(301);
  std::string result;
  HistogramImpl hist;

  void* arg = nullptr;
  for (int it = 0; it < num_iter; it++) {
    for (int i = 0; i < num_keys1; i++) {
      for (int j = 0; j < num_keys2; j++) {
        int r1 = rnd.Uniform(num_keys1) * 2;
        int r2 = rnd.Uniform(num_keys2);
        if (if_query_empty_keys) {
          r1++;
          r2 = num_keys2 * 2 - r2;
        }

        if (!for_iterator) {
          // Query one existing key;
          std::string key = MakeKey(r1, r2, through_db);
          uint64_t start_micros = env->NowMicros();
          port::MemoryBarrier();
          if (!through_db) {
            s = table_reader->Get(ro, key, arg, DummySaveValue, nullptr);
          } else {
            s = db->Get(ro, key, &result);
          }
          port::MemoryBarrier();
          hist.Add(env->NowMicros() - start_micros);
        } else {
          int r2_len;
          if (if_query_empty_keys) {
            r2_len = 0;
          } else {
            r2_len = rnd.Uniform(num_keys2) + 1;
            if (r2_len + r2 > num_keys2) {
              r2_len = num_keys2 - r2;
            }
          }
          std::string start_key = MakeKey(r1, r2, through_db);
          std::string end_key = MakeKey(r1, r2 + r2_len, through_db);
          if (prefix_len < 16) {
            prefix = Slice(start_key.data(), prefix_len);
            read_options.prefix = &prefix;
          }
          uint64_t total_time = 0;
          uint64_t start_micros = env->NowMicros();
          port::MemoryBarrier();
          Iterator* iter;
          if (!through_db) {
            iter = table_reader->NewIterator(read_options);
          } else {
            iter = db->NewIterator(read_options);
          }
          int count = 0;
          for(iter->Seek(start_key); iter->Valid(); iter->Next()) {
            if (if_query_empty_keys) {
              break;
            }
            // verify key;
            port::MemoryBarrier();
            total_time += env->NowMicros() - start_micros;
            assert(Slice(MakeKey(r1, r2 + count, through_db)) == iter->key());
            start_micros = env->NowMicros();
            if (++count >= r2_len) {
              break;
            }
          }
          if (count != r2_len) {
            fprintf(
                stderr, "Iterator cannot iterate expected number of entries. "
                "Expected %d but got %d\n", r2_len, count);
            assert(false);
          }
          delete iter;
          port::MemoryBarrier();
          total_time += env->NowMicros() - start_micros;
          hist.Add(total_time);
        }
      }
    }
  }

  fprintf(
      stderr,
      "==================================================="
      "====================================================\n"
      "InMemoryTableSimpleBenchmark: %20s   num_key1:  %5d   "
      "num_key2: %5d  %10s\n"
      "==================================================="
      "===================================================="
      "\nHistogram (unit: microseconds): \n%s",
      opts.table_factory->Name(), num_keys1, num_keys2,
      for_iterator? "iterator" : (if_query_empty_keys ? "empty" : "non_empty"),
      hist.ToString().c_str());
  if (!through_db) {
    env->DeleteFile(file_name);
  } else {
    delete db;
    db = nullptr;
    DestroyDB(dbname, opts);
  }
}
} // namespace rocksdb

DEFINE_bool(query_empty, false, "query non-existing keys instead of existing "
            "ones.");
DEFINE_int32(num_keys1, 4096, "number of distinguish prefix of keys");
DEFINE_int32(num_keys2, 512, "number of distinguish keys for each prefix");
DEFINE_int32(iter, 3, "query non-existing keys instead of existing ones");
DEFINE_int32(prefix_len, 16, "Prefix length used for iterators and indexes");
DEFINE_bool(iterator, false, "For test iterator");
DEFINE_bool(through_db, false, "If enable, a DB instance will be created and "
            "the query will be against DB. Otherwise, will be directly against "
            "a table reader.");

int main(int argc, char** argv) {
  google::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                          " [OPTIONS]...");
  google::ParseCommandLineFlags(&argc, &argv, true);

  rocksdb::TableFactory* tf = new rocksdb::BlockBasedTableFactory();
  rocksdb::Options options;
  if (FLAGS_prefix_len < 16) {
    options.prefix_extractor = rocksdb::NewFixedPrefixTransform(
        FLAGS_prefix_len);
  }
  options.SetUpDefaultFlushBlockPolicyFactory();
  rocksdb::ReadOptions ro;
  rocksdb::EnvOptions env_options;
  options.create_if_missing = true;
  options.table_factory =
      std::shared_ptr<rocksdb::TableFactory>(tf);
  TableReaderBenchmark(options, env_options, ro, FLAGS_num_keys1,
                       FLAGS_num_keys2, FLAGS_iter, FLAGS_prefix_len,
                       FLAGS_query_empty, FLAGS_iterator, FLAGS_through_db);
  delete tf;
  return 0;
}
