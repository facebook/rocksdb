// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#include <map>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

using std::unique_ptr;

namespace rocksdb {
namespace {

static const Comparator* comparator;

// A comparator for std::map, using comparator
struct MapComparator {
  bool operator()(const std::string& a, const std::string& b) const {
    return comparator->Compare(a, b) < 0;
  }
};

typedef std::map<std::string, std::string, MapComparator> KVMap;

class KVIter : public Iterator {
 public:
  explicit KVIter(const KVMap* map) : map_(map), iter_(map_->end()) {}
  virtual bool Valid() const { return iter_ != map_->end(); }
  virtual void SeekToFirst() { iter_ = map_->begin(); }
  virtual void SeekToLast() {
    if (map_->empty()) {
      iter_ = map_->end();
    } else {
      iter_ = map_->find(map_->rbegin()->first);
    }
  }
  virtual void Seek(const Slice& k) { iter_ = map_->lower_bound(k.ToString()); }
  virtual void Next() { ++iter_; }
  virtual void Prev() {
    if (iter_ == map_->begin()) {
      iter_ = map_->end();
      return;
    }
    --iter_;
  }

  virtual Slice key() const { return iter_->first; }
  virtual Slice value() const { return iter_->second; }
  virtual Status status() const { return Status::OK(); }

 private:
  const KVMap* const map_;
  KVMap::const_iterator iter_;
};

void AssertItersEqual(Iterator* iter1, Iterator* iter2) {
  ASSERT_EQ(iter1->Valid(), iter2->Valid());
  if (iter1->Valid()) {
    ASSERT_EQ(iter1->key().ToString(), iter2->key().ToString());
    ASSERT_EQ(iter1->value().ToString(), iter2->value().ToString());
  }
}

// Measuring operations on DB (expect to be empty).
// source_strings are candidate keys
void DoRandomIteraratorTest(DB* db, std::vector<std::string> source_strings,
                            Random* rnd, int num_writes, int num_iter_ops,
                            int num_trigger_flush) {
  KVMap map;

  for (int i = 0; i < num_writes; i++) {
    if (num_trigger_flush > 0 && i != 0 && i % num_trigger_flush == 0) {
      db->Flush(FlushOptions());
    }

    int type = rnd->Uniform(2);
    int index = rnd->Uniform(source_strings.size());
    auto& key = source_strings[index];
    switch (type) {
      case 0:
        // put
        map[key] = key;
        ASSERT_OK(db->Put(WriteOptions(), key, key));
        break;
      case 1:
        // delete
        if (map.find(key) != map.end()) {
          map.erase(key);
        }
        ASSERT_OK(db->Delete(WriteOptions(), key));
        break;
      default:
        assert(false);
    }
  }

  std::unique_ptr<Iterator> iter(db->NewIterator(ReadOptions()));
  std::unique_ptr<Iterator> result_iter(new KVIter(&map));

  bool is_valid = false;
  for (int i = 0; i < num_iter_ops; i++) {
    // Random walk and make sure iter and result_iter returns the
    // same key and value
    int type = rnd->Uniform(6);
    ASSERT_OK(iter->status());
    switch (type) {
      case 0:
        // Seek to First
        iter->SeekToFirst();
        result_iter->SeekToFirst();
        break;
      case 1:
        // Seek to last
        iter->SeekToLast();
        result_iter->SeekToLast();
        break;
      case 2: {
        // Seek to random key
        auto key_idx = rnd->Uniform(source_strings.size());
        auto key = source_strings[key_idx];
        iter->Seek(key);
        result_iter->Seek(key);
        break;
      }
      case 3:
        // Next
        if (is_valid) {
          iter->Next();
          result_iter->Next();
        } else {
          continue;
        }
        break;
      case 4:
        // Prev
        if (is_valid) {
          iter->Prev();
          result_iter->Prev();
        } else {
          continue;
        }
        break;
      default: {
        assert(type == 5);
        auto key_idx = rnd->Uniform(source_strings.size());
        auto key = source_strings[key_idx];
        std::string result;
        auto status = db->Get(ReadOptions(), key, &result);
        if (map.find(key) == map.end()) {
          ASSERT_TRUE(status.IsNotFound());
        } else {
          ASSERT_EQ(map[key], result);
        }
        break;
      }
    }
    AssertItersEqual(iter.get(), result_iter.get());
    is_valid = iter->Valid();
  }
}
}  // namespace

class ComparatorDBTest {
 private:
  std::string dbname_;
  Env* env_;
  DB* db_;
  Options last_options_;
  std::unique_ptr<const Comparator> comparator_guard;

 public:
  ComparatorDBTest() : env_(Env::Default()), db_(nullptr) {
    comparator = BytewiseComparator();
    dbname_ = test::TmpDir() + "/comparator_db_test";
    ASSERT_OK(DestroyDB(dbname_, last_options_));
  }

  ~ComparatorDBTest() {
    delete db_;
    ASSERT_OK(DestroyDB(dbname_, last_options_));
    comparator = BytewiseComparator();
  }

  DB* GetDB() { return db_; }

  void SetOwnedComparator(const Comparator* cmp) {
    comparator_guard.reset(cmp);
    comparator = cmp;
    last_options_.comparator = cmp;
  }

  // Return the current option configuration.
  Options* GetOptions() { return &last_options_; }

  void DestroyAndReopen() {
    // Destroy using last options
    Destroy();
    ASSERT_OK(TryReopen());
  }

  void Destroy() {
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, last_options_));
  }

  Status TryReopen() {
    delete db_;
    db_ = nullptr;
    last_options_.create_if_missing = true;

    return DB::Open(last_options_, dbname_, &db_);
  }
};

TEST(ComparatorDBTest, Bytewise) {
  for (int rand_seed = 301; rand_seed < 306; rand_seed++) {
    DestroyAndReopen();
    Random rnd(rand_seed);
    DoRandomIteraratorTest(GetDB(),
                           {"a", "b", "c", "d", "e", "f", "g", "h", "i"}, &rnd,
                           8, 100, 3);
  }
}

TEST(ComparatorDBTest, SimpleSuffixReverseComparator) {
  SetOwnedComparator(new test::SimpleSuffixReverseComparator());

  for (int rnd_seed = 301; rnd_seed < 316; rnd_seed++) {
    Options* opt = GetOptions();
    opt->comparator = comparator;
    DestroyAndReopen();
    Random rnd(rnd_seed);

    std::vector<std::string> source_strings;
    std::vector<std::string> source_prefixes;
    // Randomly generate 5 prefixes
    for (int i = 0; i < 5; i++) {
      source_prefixes.push_back(test::RandomHumanReadableString(&rnd, 8));
    }
    for (int j = 0; j < 20; j++) {
      int prefix_index = rnd.Uniform(source_prefixes.size());
      std::string key = source_prefixes[prefix_index] +
                        test::RandomHumanReadableString(&rnd, rnd.Uniform(8));
      source_strings.push_back(key);
    }

    DoRandomIteraratorTest(GetDB(), source_strings, &rnd, 30, 600, 66);
  }
}
}  // namespace rocksdb

int main(int argc, char** argv) { return rocksdb::test::RunAllTests(); }
