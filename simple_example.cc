// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include <cstdio>
#include <string>
#include <time.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/cache.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/convenience.h"


using namespace rocksdb;

std::string kDBPath = "/dev/shm//rocksdb-example";
rocksdb::TransactionDB* db;
rocksdb::DB* nontxdb;

typedef unsigned char uchar;

inline void store_big_uint4(uchar *dst, uint32_t n)
{
  uint32_t src= htonl(n);
  memcpy(dst, &src, 4);
}

inline uint32_t read_big_uint4(const uchar* b)
{
  return(((uint32_t)(b[0]) << 24)
    | ((uint32_t)(b[1]) << 16)
    | ((uint32_t)(b[2]) << 8)
    | (uint32_t)(b[3])
    );
}

double gettimeofday_sec()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + (double)tv.tv_usec*1e-6;
}

static rocksdb::BlockBasedTableOptions table_options;
rocksdb::ColumnFamilyOptions default_cf_opts;

int main(int argc, char**argv) {
  if (argc < 6) {
    printf("Usage: num_trans commit_interval use_txdb skip_cc\n");
    exit(1);
  }
  char *mode = argv[1];
  int num_trans = atoi(argv[2]);
  int commit_interval = atoi(argv[3]);
  int use_txdb = atoi(argv[4]);
  int skip_cc = atoi(argv[5]);
  const char *path = kDBPath.c_str();
  //char *path = argv[4];
  srand(time(NULL));
  double t1, t2;

  rocksdb::DBOptions db_options;
  db_options.create_if_missing = true;
  db_options.statistics = CreateDBStatistics();

  rocksdb::TransactionDBOptions txn_db_options;

  table_options.block_cache = rocksdb::NewLRUCache(10*1024*1024);
  table_options.format_version= 2;
  table_options.block_size= 16384;
  table_options.cache_index_and_filter_blocks = 1;

  default_cf_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
  default_cf_opts.write_buffer_size= 128*1024*1024;

  rocksdb::Options main_opts(db_options, default_cf_opts);
  Status s;
  // open DB
  if (use_txdb) {
    s = rocksdb::TransactionDB::Open(main_opts, txn_db_options, path, &db);
    assert(s.ok());
  } else {
    s = rocksdb::DB::Open(main_opts, path, &nontxdb);
    assert(s.ok());
  }

  t1 = gettimeofday_sec();
  if (!strcmp(mode, "insert"))
  {
    TransactionOptions txn_opts;
    WriteOptions write_opts;
    Transaction *txn= db->BeginTransaction(write_opts, txn_opts);
    //txn->SetSnapshot();
    //rocksdb::WriteBatchBase* wb = txn->GetWriteBatch();
    for (int i= 1; i <= num_trans; i++) {
      uchar key[128] = {0};
      uchar value[128] = {0};
      uint id2_1 = 0;
      uint id2_2 = i/5+1;
      uint id_type_1 = 0;
      uint id_type_2 = 123456789;
      int data1 = rand();
      int data2 = rand();
      store_big_uint4(key, 260);
      store_big_uint4(key+4, i);

      store_big_uint4(value, id2_1);
      store_big_uint4(value+4, id2_2);
      store_big_uint4(value+8, id_type_1);
      store_big_uint4(value+12, id_type_2);
      store_big_uint4(value+16, data1);
      store_big_uint4(value+20, data2);

      Slice key_slice = Slice((char*)key, 8);
      Slice value_slice = Slice((char*)value, 24);

if (skip_cc) {
// TODO: PutUntracked could be optimized for skip_cc skipping locks
      s= txn->PutUntracked(key_slice, value_slice);
} else {
      s= txn->Put(key_slice, value_slice);
}
      assert(s.ok());
      if (i % commit_interval == 0) {
        //printf("Inserted %i records..\n", i);
        s = txn->Commit();
        assert(s.ok());
        txn= db->BeginTransaction(write_opts, txn_opts, txn);
        //txn->SetSnapshot();
        //wb = txn->GetWriteBatch();
      }
      if (i % 1000000 == 0) {
        t2 = gettimeofday_sec();
        double delta = t2 - t1;
        t1 = t2;
        printf("%10.3f puts per sec, %10.3f commits per sec\n",
               1000000 / delta, 1000000 / commit_interval / delta);
      }
    }
  }else if (!strcmp(mode, "nontxinsert")) {
    WriteBatch batch;
    for (int i= 1; i <= num_trans; i++) {
      uchar key[128] = {0};
      uchar value[128] = {0};
      uint id2_1 = 0;
      uint id2_2 = i/5+1;
      uint id_type_1 = 0;
      uint id_type_2 = 123456789;
      int data1 = rand();
      int data2 = rand();
      store_big_uint4(key, 260);
      store_big_uint4(key+4, i);

      store_big_uint4(value, id2_1);
      store_big_uint4(value+4, id2_2);
      store_big_uint4(value+8, id_type_1);
      store_big_uint4(value+12, id_type_2);
      store_big_uint4(value+16, data1);
      store_big_uint4(value+20, data2);

      Slice key_slice = Slice((char*)key, 8);
      Slice value_slice = Slice((char*)value, 24);

      batch.Put(key_slice, value_slice);
      if (i % commit_interval == 0) {
        if (use_txdb) {
          s = db->Write(WriteOptions(), &batch, skip_cc);
        } else {
          s = nontxdb->Write(WriteOptions(), &batch);
        }
        assert(s.ok());
        batch.Clear();
      }
      if (i % 1000000 == 0) {
        t2 = gettimeofday_sec();
        double delta = t2 - t1;
        t1 = t2;
        printf("%10.3f puts per sec, %10.3f commits per sec\n",
               1000000 / delta, 1000000 / commit_interval / delta);
      }
    }
  }else if (!strcmp(mode, "update")) {
    rocksdb::ReadOptions read_opts;
    TransactionOptions txn_opts;
    WriteOptions write_opts;
    Transaction *txn= db->BeginTransaction(write_opts, txn_opts);
    txn->SetSnapshot();
    read_opts.snapshot = txn->GetSnapshot();
    //rocksdb::WriteBatchBase* wb = txn->GetWriteBatch();
    std::string retrieved_record;
    for (int i= 1; i <= num_trans; i++) {
      uchar key[128] = {0};
      uchar value[128] = {0};
      int data1 = rand();
      int data2 = rand();
      store_big_uint4(key, 260);
      store_big_uint4(key+4, i);

      store_big_uint4(value, 1);
      store_big_uint4(value+4, data1);
      store_big_uint4(value+8, data2);

      Slice key_slice = Slice((char*)key, 8);
      Slice value_slice = Slice((char*)value, 12);

      s= txn->GetForUpdate(read_opts, key_slice, &retrieved_record);
      assert(s.ok());

      s= txn->Put(key_slice, value_slice);
      assert(s.ok());
      if (i % commit_interval == 0) {
        printf("Updated %i records..\n", i);
        s = txn->Commit();
        assert(s.ok());
        txn= db->BeginTransaction(write_opts, txn_opts);
        txn->SetSnapshot();
        read_opts.snapshot = txn->GetSnapshot();
        //wb = txn->GetWriteBatch();
      }
    }
  }

  delete db;

  return 0;
}

