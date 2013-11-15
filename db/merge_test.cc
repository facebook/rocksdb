//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <assert.h>
#include <memory>
#include <iostream>

#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "db/dbformat.h"
#include "db/db_impl.h"
#include "utilities/merge_operators.h"
#include "util/testharness.h"
#include "utilities/utility_db.h"

using namespace std;
using namespace rocksdb;


std::shared_ptr<DB> OpenDb(const string& dbname, const bool ttl = false) {
  DB* db;
  StackableDB* sdb;
  Options options;
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  Status s;
  DestroyDB(dbname, Options());
  if (ttl) {
    cout << "Opening database with TTL\n";
    s = UtilityDB::OpenTtlDB(options, dbname, &sdb);
    db = sdb;
  } else {
    s = DB::Open(options, dbname, &db);
  }
  if (!s.ok()) {
    cerr << s.ToString() << endl;
    assert(false);
  }
  return std::shared_ptr<DB>(db);
}

// Imagine we are maintaining a set of uint64 counters.
// Each counter has a distinct name. And we would like
// to support four high level operations:
// set, add, get and remove
// This is a quick implementation without a Merge operation.
class Counters {

 protected:
  std::shared_ptr<DB> db_;

  WriteOptions put_option_;
  ReadOptions get_option_;
  WriteOptions delete_option_;

  uint64_t default_;

 public:
  explicit Counters(std::shared_ptr<DB> db, uint64_t defaultCount = 0)
      : db_(db),
        put_option_(),
        get_option_(),
        delete_option_(),
        default_(defaultCount) {
    assert(db_);
  }

  virtual ~Counters() {}

  // public interface of Counters.
  // All four functions return false
  // if the underlying level db operation failed.

  // mapped to a levedb Put
  bool set(const string& key, uint64_t value) {
    // just treat the internal rep of int64 as the string
    Slice slice((char *)&value, sizeof(value));
    auto s = db_->Put(put_option_, key, slice);

    if (s.ok()) {
      return true;
    } else {
      cerr << s.ToString() << endl;
      return false;
    }
  }

  // mapped to a rocksdb Delete
  bool remove(const string& key) {
    auto s = db_->Delete(delete_option_, key);

    if (s.ok()) {
      return true;
    } else {
      cerr << s.ToString() << std::endl;
      return false;
    }
  }

  // mapped to a rocksdb Get
  bool get(const string& key, uint64_t *value) {
    string str;
    auto s = db_->Get(get_option_, key, &str);

    if (s.IsNotFound()) {
      // return default value if not found;
      *value = default_;
      return true;
    } else if (s.ok()) {
      // deserialization
      if (str.size() != sizeof(uint64_t)) {
        cerr << "value corruption\n";
        return false;
      }
      *value = DecodeFixed64(&str[0]);
      return true;
    } else {
      cerr << s.ToString() << std::endl;
      return false;
    }
  }

  // 'add' is implemented as get -> modify -> set
  // An alternative is a single merge operation, see MergeBasedCounters
  virtual bool add(const string& key, uint64_t value) {
    uint64_t base = default_;
    return get(key, &base) && set(key, base + value);
  }


  // convenience functions for testing
  void assert_set(const string& key, uint64_t value) {
    assert(set(key, value));
  }

  void assert_remove(const string& key) {
    assert(remove(key));
  }

  uint64_t assert_get(const string& key) {
    uint64_t value = default_;
    assert(get(key, &value));
    return value;
  }

  void assert_add(const string& key, uint64_t value) {
    assert(add(key, value));
  }
};

// Implement 'add' directly with the new Merge operation
class MergeBasedCounters : public Counters {
 private:
  WriteOptions merge_option_; // for merge

 public:
  explicit MergeBasedCounters(std::shared_ptr<DB> db, uint64_t defaultCount = 0)
      : Counters(db, defaultCount),
        merge_option_() {
  }

  // mapped to a rocksdb Merge operation
  virtual bool add(const string& key, uint64_t value) override {
    char encoded[sizeof(uint64_t)];
    EncodeFixed64(encoded, value);
    Slice slice(encoded, sizeof(uint64_t));
    auto s = db_->Merge(merge_option_, key, slice);

    if (s.ok()) {
      return true;
    } else {
      cerr << s.ToString() << endl;
      return false;
    }
  }
};

void dumpDb(DB* db) {
  auto it = unique_ptr<Iterator>(db->NewIterator(ReadOptions()));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    uint64_t value = DecodeFixed64(it->value().data());
    cout << it->key().ToString() << ": "  << value << endl;
  }
  assert(it->status().ok());  // Check for any errors found during the scan
}

void testCounters(Counters& counters, DB* db, bool test_compaction) {

  FlushOptions o;
  o.wait = true;

  counters.assert_set("a", 1);

  if (test_compaction) db->Flush(o);

  assert(counters.assert_get("a") == 1);

  counters.assert_remove("b");

  // defaut value is 0 if non-existent
  assert(counters.assert_get("b") == 0);

  counters.assert_add("a", 2);

  if (test_compaction) db->Flush(o);

  // 1+2 = 3
  assert(counters.assert_get("a")== 3);

  dumpDb(db);

  std::cout << "1\n";

  // 1+...+49 = ?
  uint64_t sum = 0;
  for (int i = 1; i < 50; i++) {
    counters.assert_add("b", i);
    sum += i;
  }
  assert(counters.assert_get("b") == sum);

  std::cout << "2\n";
  dumpDb(db);

  std::cout << "3\n";

  if (test_compaction) {
    db->Flush(o);

    cout << "Compaction started ...\n";
    db->CompactRange(nullptr, nullptr);
    cout << "Compaction ended\n";

    dumpDb(db);

    assert(counters.assert_get("a")== 3);
    assert(counters.assert_get("b") == sum);
  }
}

void runTest(int argc, const string& dbname, const bool use_ttl = false) {
  auto db = OpenDb(dbname, use_ttl);

  {
    cout << "Test read-modify-write counters... \n";
    Counters counters(db, 0);
    testCounters(counters, db.get(), true);
  }

  bool compact = false;
  if (argc > 1) {
    compact = true;
    cout << "Turn on Compaction\n";
  }

  {
    cout << "Test merge-based counters... \n";
    MergeBasedCounters counters(db, 0);
    testCounters(counters, db.get(), compact);
  }

  DestroyDB(dbname, Options());
}

int main(int argc, char *argv[]) {
  //TODO: Make this test like a general rocksdb unit-test
  runTest(argc, test::TmpDir() + "/merge_testdb");
  runTest(argc, test::TmpDir() + "/merge_testdbttl", true); // Run test on TTL database
  return 0;
}
