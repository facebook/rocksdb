//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else

#include <atomic>
#include <cstdio>

#include "db/write_batch_internal.h"
#include "rocksdb/db.h"
#include "rocksdb/types.h"
#include "test_util/testutil.h"
#include "util/gflags_compat.h"

// Run a thread to perform Put's.
// Another thread uses GetUpdatesSince API to keep getting the updates.
// options :
// --num_inserts = the num of inserts the first thread should perform.
// --wal_ttl = the wal ttl for the run.

DEFINE_uint64(num_inserts, 1000,
              "the num of inserts the first thread should"
              " perform.");
DEFINE_uint64(wal_ttl_seconds, 1000, "the wal ttl for the run(in seconds)");
DEFINE_uint64(wal_size_limit_MB, 10,
              "the wal size limit for the run"
              "(in MB)");

using ROCKSDB_NAMESPACE::BatchResult;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::DestroyDB;
using ROCKSDB_NAMESPACE::Env;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::Random;
using ROCKSDB_NAMESPACE::SequenceNumber;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::TransactionLogIterator;
using ROCKSDB_NAMESPACE::WriteOptions;

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::SetUsageMessage;

struct DataPumpThread {
  DB* db;  // Assumption DB is Open'ed already.
};

static void DataPumpThreadBody(void* arg) {
  DataPumpThread* t = static_cast<DataPumpThread*>(arg);
  DB* db = t->db;
  Random rnd(301);
  uint64_t i = 0;
  while (i++ < FLAGS_num_inserts) {
    if (!db->Put(WriteOptions(), Slice(rnd.RandomString(500)),
                 Slice(rnd.RandomString(500)))
             .ok()) {
      fprintf(stderr, "Error in put\n");
      exit(1);
    }
  }
}

int main(int argc, const char** argv) {
  SetUsageMessage(
      std::string("\nUSAGE:\n") + std::string(argv[0]) +
      " --num_inserts=<num_inserts> --wal_ttl_seconds=<WAL_ttl_seconds>" +
      " --wal_size_limit_MB=<WAL_size_limit_MB>");
  ParseCommandLineFlags(&argc, const_cast<char***>(&argv), true);

  Env* env = Env::Default();
  std::string default_db_path;
  env->GetTestDirectory(&default_db_path);
  default_db_path += "db_repl_stress";
  Options options;
  options.create_if_missing = true;
  options.WAL_ttl_seconds = FLAGS_wal_ttl_seconds;
  options.WAL_size_limit_MB = FLAGS_wal_size_limit_MB;
  DB* db;
  DestroyDB(default_db_path, options);

  Status s = DB::Open(options, default_db_path, &db);

  if (!s.ok()) {
    fprintf(stderr, "Could not open DB due to %s\n", s.ToString().c_str());
    exit(1);
  }

  DataPumpThread dataPump;
  dataPump.db = db;
  env->StartThread(DataPumpThreadBody, &dataPump);

  std::unique_ptr<TransactionLogIterator> iter;
  SequenceNumber currentSeqNum = 1;
  uint64_t num_read = 0;
  for (;;) {
    iter.reset();
    // Continue to probe a bit more after all received
    size_t probes = 0;
    while (!db->GetUpdatesSince(currentSeqNum, &iter).ok()) {
      probes++;
      if (probes > 100 && num_read >= FLAGS_num_inserts) {
        if (num_read > FLAGS_num_inserts) {
          fprintf(stderr, "Too many updates read: %ld expected: %ld\n",
                  (long)num_read, (long)FLAGS_num_inserts);
          exit(1);
        }
        fprintf(stderr, "Successful!\n");
        return 0;
      }
    }
    fprintf(stderr, "Refreshing iterator\n");
    for (; iter->Valid(); iter->Next(), num_read++, currentSeqNum++) {
      BatchResult res = iter->GetBatch();
      if (res.sequence != currentSeqNum) {
        fprintf(stderr, "Missed a seq no. b/w %ld and %ld\n",
                (long)currentSeqNum, (long)res.sequence);
        exit(1);
      }
    }
  }
}

#endif  // GFLAGS
