//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <cstdio>
#include <cstdlib>
#include <vector>
#include <memory>

#include "include/rocksdb/db.h"
#include "include/rocksdb/options.h"
#include "include/rocksdb/env.h"
#include "include/rocksdb/slice.h"
#include "include/rocksdb/status.h"
#include "include/rocksdb/comparator.h"
#include "include/rocksdb/table.h"
#include "include/rocksdb/slice_transform.h"

namespace rocksdb {

class SanityTest {
 public:
  explicit SanityTest(const std::string& path)
      : env_(Env::Default()), path_(path) {
    env_->CreateDirIfMissing(path);
  }
  virtual ~SanityTest() {}

  virtual std::string Name() const = 0;
  virtual Options GetOptions() const = 0;

  Status Create() {
    Options options = GetOptions();
    options.create_if_missing = true;
    std::string dbname = path_ + Name();
    DestroyDB(dbname, options);
    DB* db;
    Status s = DB::Open(options, dbname, &db);
    std::unique_ptr<DB> db_guard(db);
    if (!s.ok()) {
      return s;
    }
    for (int i = 0; i < 1000000; ++i) {
      std::string k = "key" + std::to_string(i);
      std::string v = "value" + std::to_string(i);
      s = db->Put(WriteOptions(), Slice(k), Slice(v));
      if (!s.ok()) {
        return s;
      }
    }
    return Status::OK();
  }
  Status Verify() {
    DB* db;
    std::string dbname = path_ + Name();
    Status s = DB::Open(GetOptions(), dbname, &db);
    std::unique_ptr<DB> db_guard(db);
    if (!s.ok()) {
      return s;
    }
    for (int i = 0; i < 1000000; ++i) {
      std::string k = "key" + std::to_string(i);
      std::string v = "value" + std::to_string(i);
      std::string result;
      s = db->Get(ReadOptions(), Slice(k), &result);
      if (!s.ok()) {
        return s;
      }
      if (result != v) {
        return Status::Corruption("Unexpected value for key " + k);
      }
    }
    return Status::OK();
  }

 private:
  Env* env_;
  std::string const path_;
};

class SanityTestBasic : public SanityTest {
 public:
  explicit SanityTestBasic(const std::string& path) : SanityTest(path) {}
  virtual Options GetOptions() const {
    Options options;
    options.create_if_missing = true;
    return options;
  }
  virtual std::string Name() const { return "Basic"; }
};

class SanityTestSpecialComparator : public SanityTest {
 public:
  explicit SanityTestSpecialComparator(const std::string& path)
      : SanityTest(path) {
    options_.comparator = new NewComparator();
  }
  ~SanityTestSpecialComparator() { delete options_.comparator; }
  virtual Options GetOptions() const { return options_; }
  virtual std::string Name() const { return "SpecialComparator"; }

 private:
  class NewComparator : public Comparator {
   public:
    virtual const char* Name() const { return "rocksdb.NewComparator"; }
    virtual int Compare(const Slice& a, const Slice& b) const {
      return BytewiseComparator()->Compare(a, b);
    }
    virtual void FindShortestSeparator(std::string* s, const Slice& l) const {
      BytewiseComparator()->FindShortestSeparator(s, l);
    }
    virtual void FindShortSuccessor(std::string* key) const {
      BytewiseComparator()->FindShortSuccessor(key);
    }
  };
  Options options_;
};

class SanityTestZlibCompression : public SanityTest {
 public:
  explicit SanityTestZlibCompression(const std::string& path)
      : SanityTest(path) {
    options_.compression = kZlibCompression;
  }
  virtual Options GetOptions() const { return options_; }
  virtual std::string Name() const { return "ZlibCompression"; }

 private:
  Options options_;
};

class SanityTestPlainTableFactory : public SanityTest {
 public:
  explicit SanityTestPlainTableFactory(const std::string& path)
      : SanityTest(path) {
    options_.table_factory.reset(NewPlainTableFactory());
    options_.prefix_extractor.reset(NewFixedPrefixTransform(2));
    options_.allow_mmap_reads = true;
  }
  ~SanityTestPlainTableFactory() {}
  virtual Options GetOptions() const { return options_; }
  virtual std::string Name() const { return "PlainTable"; }

 private:
  Options options_;
};

namespace {
bool RunSanityTests(const std::string& command, const std::string& path) {
  std::vector<SanityTest*> sanity_tests = {
      new SanityTestBasic(path),
      new SanityTestSpecialComparator(path),
      new SanityTestZlibCompression(path),
      new SanityTestPlainTableFactory(path)};

  if (command == "create") {
    fprintf(stderr, "Creating...\n");
  } else {
    fprintf(stderr, "Verifying...\n");
  }
  for (auto sanity_test : sanity_tests) {
    Status s;
    fprintf(stderr, "%s -- ", sanity_test->Name().c_str());
    if (command == "create") {
      s = sanity_test->Create();
    } else {
      assert(command == "verify");
      s = sanity_test->Verify();
    }
    fprintf(stderr, "%s\n", s.ToString().c_str());
    if (!s.ok()) {
      fprintf(stderr, "FAIL\n");
      return false;
    }

    delete sanity_test;
  }
  return true;
}
}  // namespace

}  // namespace rocksdb

int main(int argc, char** argv) {
  std::string path, command;
  bool ok = (argc == 3);
  if (ok) {
    path = std::string(argv[1]);
    command = std::string(argv[2]);
    ok = (command == "create" || command == "verify");
  }
  if (!ok) {
    fprintf(stderr, "Usage: %s <path> [create|verify] \n", argv[0]);
    exit(1);
  }
  if (path.back() != '/') {
    path += "/";
  }

  bool sanity_ok = rocksdb::RunSanityTests(command, path);

  return sanity_ok ? 0 : 1;
}
