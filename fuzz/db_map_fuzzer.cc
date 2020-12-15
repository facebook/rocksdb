// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <iostream>
#include <map>
#include <string>

#include "proto/gen/db_operation.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "src/libfuzzer/libfuzzer_macro.h"
#include "util.h"

protobuf_mutator::libfuzzer::PostProcessorRegistration<DBOperations> reg = {
    [](DBOperations* input, unsigned int /* seed */) {
      const rocksdb::Comparator* comparator = rocksdb::BytewiseComparator();
      auto ops = input->mutable_operations();
      // Make sure begin <= end for DELETE_RANGE.
      for (DBOperation& op : *ops) {
        if (op.type() == OpType::DELETE_RANGE) {
          auto begin = op.key();
          auto end = op.value();
          if (comparator->Compare(begin, end) > 0) {
            std::swap(begin, end);
            op.set_key(begin);
            op.set_value(end);
          }
        }
      }
    }};

// Execute randomly generated operations on both a DB and a std::map,
// then reopen the DB and make sure that iterating the DB produces the
// same key-value pairs as iterating through the std::map.
DEFINE_PROTO_FUZZER(DBOperations& input) {
  if (input.operations().empty()) {
    return;
  }

  const std::string kDbPath = "/tmp/db_map_fuzzer_test";
  auto fs = rocksdb::FileSystem::Default();
  if (fs->FileExists(kDbPath, rocksdb::IOOptions(), /*dbg=*/nullptr).ok()) {
    std::cerr << "db path " << kDbPath << " already exists" << std::endl;
    abort();
  }

  std::map<std::string, std::string> kv;
  rocksdb::DB* db = nullptr;
  rocksdb::Options options;
  options.create_if_missing = true;
  CHECK_OK(rocksdb::DB::Open(options, kDbPath, &db));

  for (const DBOperation& op : input.operations()) {
    switch (op.type()) {
      case OpType::PUT: {
        CHECK_OK(db->Put(rocksdb::WriteOptions(), op.key(), op.value()));
        kv[op.key()] = op.value();
        break;
      }
      case OpType::MERGE: {
        break;
      }
      case OpType::DELETE: {
        CHECK_OK(db->Delete(rocksdb::WriteOptions(), op.key()));
        kv.erase(op.key());
        break;
      }
      case OpType::DELETE_RANGE: {
        // [op.key(), op.value()) corresponds to [begin, end).
        CHECK_OK(db->DeleteRange(rocksdb::WriteOptions(),
                                 db->DefaultColumnFamily(), op.key(),
                                 op.value()));
        kv.erase(kv.lower_bound(op.key()), kv.lower_bound(op.value()));
        break;
      }
      default: {
        std::cerr << "Unsupported operation" << static_cast<int>(op.type());
        return;
      }
    }
  }
  CHECK_OK(db->Close());
  delete db;
  db = nullptr;

  CHECK_OK(rocksdb::DB::Open(options, kDbPath, &db));
  auto kv_it = kv.begin();
  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next(), kv_it++) {
    CHECK_TRUE(kv_it != kv.end());
    CHECK_EQ(it->key().ToString(), kv_it->first);
    CHECK_EQ(it->value().ToString(), kv_it->second);
  }
  CHECK_TRUE(kv_it == kv.end());
  delete it;

  CHECK_OK(db->Close());
  delete db;
  CHECK_OK(rocksdb::DestroyDB(kDbPath, options));
}
