// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "include/utilities/utility_db.h"
#include "util/testharness.h"
#include "util/logging.h"
#include <map>
#include <unistd.h>

namespace leveldb {

namespace {
typedef std::map<std::string, std::string> KVMap;
}

class TtlTest {
 public:
  TtlTest() {
    dbname_ = test::TmpDir() + "/db_ttl";
    options_.create_if_missing = true;
    // ensure that compaction is kicked in to always strip timestamp from kvs
    options_.max_grandparent_overlap_factor = 0;
    // compaction should take place always from level0 for determinism
    options_.max_mem_compaction_level = 0;
    db_ttl_ = nullptr;
    DestroyDB(dbname_, Options());
  }

  ~TtlTest() {
    CloseTtl();
    DestroyDB(dbname_, Options());
  }

  // Open database with TTL support when TTL not provided with db_ttl_ pointer
  void OpenTtl() {
    assert(db_ttl_ == nullptr); //  db should be closed before opening again
    ASSERT_OK(UtilityDB::OpenTtlDB(options_, dbname_, &db_ttl_));
  }

  // Open database with TTL support when TTL provided with db_ttl_ pointer
  void OpenTtl(int32_t ttl) {
    assert(db_ttl_ == nullptr);
    ASSERT_OK(UtilityDB::OpenTtlDB(options_, dbname_, &db_ttl_, ttl));
  }

  void CloseTtl() {
    delete db_ttl_;
    db_ttl_ = nullptr;
  }

  // Populates and returns a kv-map
  void MakeKVMap(int64_t num_entries) {
    kvmap_.clear();

    for (int64_t i = 0; i < num_entries; i++) {
      std::string key = "key";
      std::string value = "value";
      AppendNumberTo(&key, i);
      AppendNumberTo(&value, i);
      kvmap_[key] = value;
    }
    ASSERT_EQ((int)kvmap_.size(), num_entries);//check all insertions done
  }

  // Puts num_entries starting from start_pos_map from kvmap_ into the database
  void PutValues(int start_pos_map, int num_entries) {
    assert(db_ttl_);
    ASSERT_LE(start_pos_map + num_entries, (int)kvmap_.size());
    static WriteOptions wopts;
    static FlushOptions flush_opts;
    kv_it_ = kvmap_.begin();
    advance(kv_it_, start_pos_map);
    for (int i = 0; kv_it_ != kvmap_.end(), i < num_entries; i++, kv_it_++) {
      ASSERT_OK(db_ttl_->Put(wopts, kv_it_->first, kv_it_->second));
    }
    // Put a mock kv at the end because CompactionFilter doesn't delete last key
    ASSERT_OK(db_ttl_->Put(wopts, "keymock", "valuemock"));
    db_ttl_->Flush(flush_opts);
  }

  // Runs a manual compaction
  void ManualCompact() {
    db_ttl_->CompactRange(nullptr, nullptr);
  }

  // Sleeps for slp_tim then runs a manual compaction
  // Checks span starting from st_pos from kvmap_ in the db and
  // Gets should return true if check is true and false otherwise
  // Also checks that value that we got is the same as inserted
  void SleepCompactCheck(int slp_tim, int st_pos, int span, bool check = true) {
    assert(db_ttl_);
    sleep(slp_tim);
    ManualCompact();
    static ReadOptions ropts;
    kv_it_ = kvmap_.begin();
    advance(kv_it_, st_pos);
    std::string v;
    for (int i = 0; kv_it_ != kvmap_.end(), i < span; i++, kv_it_++) {
      Status s = db_ttl_->Get(ropts, kv_it_->first, &v);
      if (s.ok() != check) {
        fprintf(stderr, "key=%s ",
                kv_it_->first.c_str());
        if (!s.ok()) {
          fprintf(stderr, "is absent from db but was expected to be present\n");
        } else {
          fprintf(stderr, "is present in db but was expected to be absent\n");
        }
        assert(false);
      } else if (s.ok() && (v.compare(kv_it_->second) != 0)) {
          fprintf(stderr, " value for key=%s present in database is %s but "
                          " should be %s\n", kv_it_->first.c_str(), v.c_str(),
                          kv_it_->second.c_str());
          assert(false);
      }
    }
  }

  // Similar as SleepCompactCheck but uses TtlIterator to read from db
  void SleepCompactCheckIter(int slp, int st_pos, int span, bool check=true) {
    assert(db_ttl_);
    sleep(slp);
    ManualCompact();
    static ReadOptions ropts;
    Iterator *dbiter = db_ttl_->NewIterator(ropts);
    kv_it_ = kvmap_.begin();
    advance(kv_it_, st_pos);

    dbiter->Seek(kv_it_->first);
    if (!check) {
      if (dbiter->Valid()) {
        ASSERT_NE(dbiter->value().compare(kv_it_->second), 0);
      }
    } else {  // dbiter should have found out kvmap_[st_pos]
      for (int i = st_pos;
           kv_it_ != kvmap_.end() && i < st_pos + span;
           i++, kv_it_++)  {
        ASSERT_TRUE(dbiter->Valid());
        ASSERT_EQ(dbiter->value().compare(kv_it_->second), 0);
        dbiter->Next();
      }
    }
    delete dbiter;
  }

  // Choose carefully so that Put, Gets & Compaction complete in 1 second buffer
  const int64_t kSampleSize = 100;

 private:
  std::string dbname_;
  DB* db_ttl_;
  Options options_;
  KVMap kvmap_;
  KVMap::iterator kv_it_;
}; // class TtlTest

// If TTL is non positive or not provided, the behaviour is TTL = infinity
// This test opens the db 3 times with such default behavior and inserts a
// bunch of kvs each time. All kvs should accummulate in the db till the end
// Partitions the sample-size provided into 3 sets over boundary1 and boundary2
TEST(TtlTest, NoEffect) {
  MakeKVMap(kSampleSize);
  int boundary1 = kSampleSize / 3;
  int boundary2 = 2 * boundary1;

  OpenTtl();
  PutValues(0, boundary1);                       //T=0: Set1 never deleted
  SleepCompactCheck(1, 0, boundary1);            //T=1: Set1 still there
  CloseTtl();

  OpenTtl(0);
  PutValues(boundary1, boundary2 - boundary1);   //T=1: Set2 never deleted
  SleepCompactCheck(1, 0, boundary2);            //T=2: Sets1 & 2 still there
  CloseTtl();

  OpenTtl(-1);
  PutValues(boundary2, kSampleSize - boundary2); //T=3: Set3 never deleted
  SleepCompactCheck(1, 0, kSampleSize, true);    //T=4: Sets 1,2,3 still there
  CloseTtl();
}

// Puts a set of values and checks its presence using Get during ttl
TEST(TtlTest, PresentDuringTTL) {
  MakeKVMap(kSampleSize);

  OpenTtl(2);                                 // T=0:Open the db with ttl = 2
  PutValues(0, kSampleSize);                  // T=0:Insert Set1. Delete at t=2
  SleepCompactCheck(1, 0, kSampleSize, true); // T=1:Set1 should still be there
  CloseTtl();
}

// Puts a set of values and checks its absence using Get after ttl
TEST(TtlTest, AbsentAfterTTL) {
  MakeKVMap(kSampleSize);

  OpenTtl(1);                                  // T=0:Open the db with ttl = 2
  PutValues(0, kSampleSize);                   // T=0:Insert Set1. Delete at t=2
  SleepCompactCheck(2, 0, kSampleSize, false); // T=2:Set1 should not be there
  CloseTtl();
}

// Resets the timestamp of a set of kvs by updating them and checks that they
// are not deleted according to the old timestamp
TEST(TtlTest, ResetTimestamp) {
  MakeKVMap(kSampleSize);

  OpenTtl(3);
  PutValues(0, kSampleSize);            // T=0: Insert Set1. Delete at t=3
  sleep(2);                             // T=2
  PutValues(0, kSampleSize);            // T=2: Insert Set1. Delete at t=5
  SleepCompactCheck(2, 0, kSampleSize); // T=4: Set1 should still be there
  CloseTtl();
}

// Similar to PresentDuringTTL but uses Iterator
TEST(TtlTest, IterPresentDuringTTL) {
  MakeKVMap(kSampleSize);

  OpenTtl(2);
  PutValues(0, kSampleSize);                 // T=0: Insert. Delete at t=2
  SleepCompactCheckIter(1, 0, kSampleSize);  // T=1: Set should be there
  CloseTtl();
}

// Similar to AbsentAfterTTL but uses Iterator
TEST(TtlTest, IterAbsentAfterTTL) {
  MakeKVMap(kSampleSize);

  OpenTtl(1);
  PutValues(0, kSampleSize);                       // T=0: Insert. Delete at t=1
  SleepCompactCheckIter(2, 0, kSampleSize, false); // T=2: Should not be there
  CloseTtl();
}

// Checks presence while opening the same db more than once with the same ttl
// Note: The second open will open the same db
TEST(TtlTest, MultiOpenSamePresent) {
  MakeKVMap(kSampleSize);

  OpenTtl(2);
  PutValues(0, kSampleSize);                   // T=0: Insert. Delete at t=2
  CloseTtl();

  OpenTtl(2);                                  // T=0. Delete at t=2
  SleepCompactCheck(1, 0, kSampleSize);        // T=1: Set should be there
  CloseTtl();
}

// Checks absence while opening the same db more than once with the same ttl
// Note: The second open will open the same db
TEST(TtlTest, MultiOpenSameAbsent) {
  MakeKVMap(kSampleSize);

  OpenTtl(1);
  PutValues(0, kSampleSize);                   // T=0: Insert. Delete at t=1
  CloseTtl();

  OpenTtl(1);                                  // T=0.Delete at t=1
  SleepCompactCheck(2, 0, kSampleSize, false); // T=2: Set should not be there
  CloseTtl();
}

// Checks presence while opening the same db more than once with bigger ttl
TEST(TtlTest, MultiOpenDifferent) {
  MakeKVMap(kSampleSize);

  OpenTtl(1);
  PutValues(0, kSampleSize);            // T=0: Insert. Delete at t=1
  CloseTtl();

  OpenTtl(3);                           // T=0: Set deleted at t=3
  SleepCompactCheck(2, 0, kSampleSize); // T=2: Set should be there
  CloseTtl();
}

} //  namespace leveldb

// A black-box test for the ttl wrapper around rocksdb
int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
