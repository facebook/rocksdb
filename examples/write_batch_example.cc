// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_write_batch_example";

int main() {
  DB* db;
  Options options;
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  Slice key1 = "one";
  Slice key2 = "two";
  std::string bar = "foo";
  db->Put(WriteOptions(), key1, bar);

  std::string value;
  s = db->Get(ReadOptions(), key1, &value);

  if (s.ok()) {
    // atomically apply a set of updates
    WriteBatch batch;
    batch.Delete(key1);
    batch.Put(key2, value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), key1, &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), key2, &value);
  assert(value == bar);
}
