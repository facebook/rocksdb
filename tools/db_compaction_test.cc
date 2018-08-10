//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else

#include <atomic>
#include <cstdio>
#include <vector>
#include <iostream>

#include "db/write_batch_internal.h"
#include "rocksdb/db.h"
#include "rocksdb/types.h"
#include "util/gflags_compat.h"
#include "util/testutil.h"

// Run a thread to perform Put's.
// Another thread uses GetUpdatesSince API to keep getting the updates.
// options :
// --num_inserts = the num of inserts the first thread should perform.
// --wal_ttl = the wal ttl for the run.

using namespace rocksdb;

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::SetUsageMessage;

/*
static std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}
*/

DEFINE_uint32(max_key_value, 64,
              "the maximum key value inserted");
DEFINE_uint32(random_seed, 42,
              "random seed to initialize");
DEFINE_uint64(number_of_iterations, 100,
              "number of operations to perform");
DEFINE_uint32(number_of_open_snapshots, 3,
              "number of snapshots to keep open");

/*
static void dump_state(std::string prefix,
                       std::vector<bool>& expected_value,
                      int max_key_value) {
  for (int i = 0; i < max_key_value; i++)
    if (expected_value[i])
      std::cout << prefix << i << std::endl;
}
*/

static bool validate_snapshot(DB* db, const Snapshot *s,
                              std::vector<bool>& expected_value,
                              int max_key_value) {

  ReadOptions ropt;
  ropt.snapshot = s;
  std::unique_ptr<Iterator> iterator(db->NewIterator(ropt));
  std::unique_ptr<std::vector<bool>> tmp_bitvec(new std::vector<bool>(max_key_value));
  //dump_state("Pre state ", *tmp_bitvec.get(), max_key_value);
  int key_count = 0;
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      int key = std::stoi(iterator->key().ToString());
      //std::cout << "Got " << key << std::endl;
      (*tmp_bitvec.get())[key] = true;
      key_count++;
  }
  std::cout << " Found " << key_count++ << " unique keys at "
            << s->GetSequenceNumber() << std::endl;
  //dump_state("Expected ", expected_value, max_key_value);
  //dump_state("Got ", *tmp_bitvec.get(), max_key_value);
  return (std::equal(expected_value.begin(), expected_value.end(),
                     tmp_bitvec.get()->begin()));
}

static unsigned
       int add_snapshot(DB* db,
                        unsigned int snapshot_count,
                        unsigned int num_of_open_snapshots,
                        unsigned int max_key_value,
                        Random* rnd,
                        std::vector<bool>** snapshot_state,
                        const Snapshot** snapshots,
                        std::vector<bool>& curr_snapshot_state,
                        int unique_key_count) {
  unsigned int snapshot_to_replace = num_of_open_snapshots;
  if (snapshot_count == num_of_open_snapshots) {
    snapshot_to_replace = rnd->Next() % num_of_open_snapshots;
    std::cout << "Validating snapshot  at "
              << snapshot_to_replace
              << " with sequence # "
              << snapshots[snapshot_to_replace]->GetSequenceNumber()
              << std::endl;
    assert(validate_snapshot(db, snapshots[snapshot_to_replace],
                             *snapshot_state[snapshot_to_replace],
                             max_key_value));
    db->ReleaseSnapshot(snapshots[snapshot_to_replace]);
    delete snapshot_state[snapshot_to_replace];
    snapshot_state[snapshot_to_replace] = nullptr;
    for (; snapshot_to_replace < num_of_open_snapshots - 1; snapshot_to_replace++) {
      snapshot_state[snapshot_to_replace] =
        snapshot_state[snapshot_to_replace+1];
      snapshots[snapshot_to_replace] = snapshots[snapshot_to_replace+1];
    }
  } else {
    snapshot_to_replace = snapshot_count;
  }
  assert(snapshot_to_replace < num_of_open_snapshots);
  snapshots[snapshot_to_replace] = db->GetSnapshot();
  snapshot_state[snapshot_to_replace] =
    new std::vector<bool>(curr_snapshot_state);
  std::cout << "New snapshot at " << snapshot_to_replace
            << " with sequence number "
            << snapshots[snapshot_to_replace]->GetSequenceNumber()
            << " unique count is "
            << unique_key_count
            << std::endl;
  return snapshot_to_replace+1;
}

int main(int argc, const char** argv) {
  SetUsageMessage(
      std::string("\nUSAGE:\n") + std::string(argv[0]) +
      " --max_key_value=<max_key_value> --random_seed=<random_seed>" +
      " --number_of_iterations=<number_of_iterations>" +
      " --number_of_open_snapshots=<number_of_open_snapshots>");
  ParseCommandLineFlags(&argc, const_cast<char***>(&argv), true);

  Env* env = Env::Default();
  std::string default_db_path;
  env->GetTestDirectory(&default_db_path);
  default_db_path += "db_compaction_test";
  Options options;
  options.create_if_missing = true;
  DB *db;
  DestroyDB(default_db_path, options);

  Status s = DB::Open(options, default_db_path, &db);

  Random rnd(FLAGS_random_seed);

  if (!s.ok()) {
    fprintf(stderr, "Could not open DB due to %s\n", s.ToString().c_str());
    exit(1);
  }

  unsigned int snapshot_count = 0;
  std::vector<bool>** snapshot_state =
    new std::vector<bool>* [FLAGS_number_of_open_snapshots];

  const Snapshot** snapshots = new const Snapshot* [FLAGS_number_of_open_snapshots];

  std::vector<bool> curr_state(FLAGS_max_key_value);

  unsigned int i;
  char key[20];
  char val[] = "foo";
  int unique_key_count = 0;
  // First 10% of the ops will be inserts
  for (i = 0; i < FLAGS_number_of_iterations/10; i++) {
    int key_int = rnd.Next()%FLAGS_max_key_value;
    //std::cout << "Putting " << key_int << std::endl;
    sprintf(key, "%019d",key_int);
    if (!curr_state[key_int])
      unique_key_count++;
    curr_state[key_int] = true;
    s = db->Put(WriteOptions(), Slice(key), val);
    if (!s.ok()) {
      fprintf(stderr, "could not put because of %s\n", s.ToString().c_str());
    }
  }
  std::cout << "added " << unique_key_count << " unique keys initially"
            << std::endl;
  // get one snapshot at least
  snapshot_count = add_snapshot(db,
                                snapshot_count,
                                FLAGS_number_of_open_snapshots,
                                FLAGS_max_key_value,
                                &rnd,
                                snapshot_state,
                                snapshots,
                                curr_state,
                                unique_key_count);
  for (; i < FLAGS_number_of_iterations; i++) {
    int next = rnd.Next();
    if (next % 1001 == 0) {
      snapshot_count = add_snapshot(db,
                                    snapshot_count,
                                    FLAGS_number_of_open_snapshots,
                                    FLAGS_max_key_value,
                                    &rnd,
                                    snapshot_state,
                                    snapshots,
                                    curr_state,
                                    unique_key_count);
    } else {
      int key_int = rnd.Next()%FLAGS_max_key_value;
      sprintf(key, "%019d",key_int);
      if (next % 2 == 0) {
        if (!curr_state[key_int])
          unique_key_count++;
        curr_state[key_int] = true;
        s = db->Put(WriteOptions(), Slice(key), val);
      } else {
        if (curr_state[key_int])
          unique_key_count--;
        curr_state[key_int] = false;
        s = db->Delete(WriteOptions(), Slice(key));
      }
      if (!s.ok()) {
      fprintf(stderr, "could not put because of %s\n", s.ToString().c_str());
      }
    }
  }
  for (i = 0; i < snapshot_count; i++) {
    //std::cout << "ptr " << snapshot_state[0] << std::endl;
    assert(validate_snapshot(db, snapshots[i], *snapshot_state[i],
                             FLAGS_max_key_value));
    db->ReleaseSnapshot(snapshots[i]);
    delete snapshot_state[i];
  }
  exit(0);
}

#endif  // GFLAGS

#else  // ROCKSDB_LITE
#include <stdio.h>
int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "Not supported in lite mode.\n");
  return 1;
}
#endif  // ROCKSDB_LITE
