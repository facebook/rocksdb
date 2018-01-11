// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#ifndef OS_WIN
#include <unistd.h>
#endif

#include <map>
#include <memory>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/utilities/date_tiered_db.h"
#include "util/logging.h"
#include "util/string_util.h"
#include "util/testharness.h"

namespace rocksdb {

namespace {

typedef std::map<std::string, std::string> KVMap;
}

class SpecialTimeEnv : public EnvWrapper {
 public:
  explicit SpecialTimeEnv(Env* base) : EnvWrapper(base) {
    base->GetCurrentTime(&current_time_);
  }

  void Sleep(int64_t sleep_time) { current_time_ += sleep_time; }
  virtual Status GetCurrentTime(int64_t* current_time) override {
    *current_time = current_time_;
    return Status::OK();
  }

 private:
  int64_t current_time_ = 0;
};

class DateTieredTest : public testing::Test {
 public:
  DateTieredTest() {
    env_.reset(new SpecialTimeEnv(Env::Default()));
    dbname_ = test::TmpDir() + "/date_tiered";
    options_.create_if_missing = true;
    options_.env = env_.get();
    date_tiered_db_.reset(nullptr);
    DestroyDB(dbname_, Options());
  }

  ~DateTieredTest() {
    CloseDateTieredDB();
    DestroyDB(dbname_, Options());
  }

  void OpenDateTieredDB(int64_t ttl, int64_t column_family_interval,
                        bool read_only = false) {
    ASSERT_TRUE(date_tiered_db_.get() == nullptr);
    DateTieredDB* date_tiered_db = nullptr;
    ASSERT_OK(DateTieredDB::Open(options_, dbname_, &date_tiered_db, ttl,
                                 column_family_interval, read_only));
    date_tiered_db_.reset(date_tiered_db);
  }

  void CloseDateTieredDB() { date_tiered_db_.reset(nullptr); }

  Status AppendTimestamp(std::string* key) {
    char ts[8];
    int bytes_to_fill = 8;
    int64_t timestamp_value = 0;
    Status s = env_->GetCurrentTime(&timestamp_value);
    if (!s.ok()) {
      return s;
    }
    if (port::kLittleEndian) {
      for (int i = 0; i < bytes_to_fill; ++i) {
        ts[i] = (timestamp_value >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
      }
    } else {
      memcpy(ts, static_cast<void*>(&timestamp_value), bytes_to_fill);
    }
    key->append(ts, 8);
    return Status::OK();
  }

  // Populates and returns a kv-map
  void MakeKVMap(int64_t num_entries, KVMap* kvmap) {
    kvmap->clear();
    int digits = 1;
    for (int64_t dummy = num_entries; dummy /= 10; ++digits) {
    }
    int digits_in_i = 1;
    for (int64_t i = 0; i < num_entries; i++) {
      std::string key = "key";
      std::string value = "value";
      if (i % 10 == 0) {
        digits_in_i++;
      }
      for (int j = digits_in_i; j < digits; j++) {
        key.append("0");
        value.append("0");
      }
      AppendNumberTo(&key, i);
      AppendNumberTo(&value, i);
      ASSERT_OK(AppendTimestamp(&key));
      (*kvmap)[key] = value;
    }
    // check all insertions done
    ASSERT_EQ(num_entries, static_cast<int64_t>(kvmap->size()));
  }

  size_t GetColumnFamilyCount() {
    DBOptions db_options(options_);
    std::vector<std::string> cf;
    DB::ListColumnFamilies(db_options, dbname_, &cf);
    return cf.size();
  }

  void Sleep(int64_t sleep_time) { env_->Sleep(sleep_time); }

  static const int64_t kSampleSize_ = 100;
  std::string dbname_;
  std::unique_ptr<DateTieredDB> date_tiered_db_;
  std::unique_ptr<SpecialTimeEnv> env_;
  KVMap kvmap_;

 private:
  Options options_;
  KVMap::iterator kv_it_;
  const std::string kNewValue_ = "new_value";
  unique_ptr<CompactionFilter> test_comp_filter_;
};

// Puts a set of values and checks its presence using Get during ttl
TEST_F(DateTieredTest, KeyLifeCycle) {
  WriteOptions wopts;
  ReadOptions ropts;

  // T=0, open the database and insert data
  OpenDateTieredDB(2, 2);
  ASSERT_TRUE(date_tiered_db_.get() != nullptr);

  // Create key value pairs to insert
  KVMap map_insert;
  MakeKVMap(kSampleSize_, &map_insert);

  // Put data in database
  for (auto& kv : map_insert) {
    ASSERT_OK(date_tiered_db_->Put(wopts, kv.first, kv.second));
  }

  Sleep(1);
  // T=1, keys should still reside in database
  for (auto& kv : map_insert) {
    std::string value;
    ASSERT_OK(date_tiered_db_->Get(ropts, kv.first, &value));
    ASSERT_EQ(value, kv.second);
  }

  Sleep(1);
  // T=2, keys should not be retrieved
  for (auto& kv : map_insert) {
    std::string value;
    auto s = date_tiered_db_->Get(ropts, kv.first, &value);
    ASSERT_TRUE(s.IsNotFound());
  }

  CloseDateTieredDB();
}

TEST_F(DateTieredTest, DeleteTest) {
  WriteOptions wopts;
  ReadOptions ropts;

  // T=0, open the database and insert data
  OpenDateTieredDB(2, 2);
  ASSERT_TRUE(date_tiered_db_.get() != nullptr);

  // Create key value pairs to insert
  KVMap map_insert;
  MakeKVMap(kSampleSize_, &map_insert);

  // Put data in database
  for (auto& kv : map_insert) {
    ASSERT_OK(date_tiered_db_->Put(wopts, kv.first, kv.second));
  }

  Sleep(1);
  // Delete keys when they are not obsolete
  for (auto& kv : map_insert) {
    ASSERT_OK(date_tiered_db_->Delete(wopts, kv.first));
  }

  // Key should not be found
  for (auto& kv : map_insert) {
    std::string value;
    auto s = date_tiered_db_->Get(ropts, kv.first, &value);
    ASSERT_TRUE(s.IsNotFound());
  }
}

TEST_F(DateTieredTest, KeyMayExistTest) {
  WriteOptions wopts;
  ReadOptions ropts;

  // T=0, open the database and insert data
  OpenDateTieredDB(2, 2);
  ASSERT_TRUE(date_tiered_db_.get() != nullptr);

  // Create key value pairs to insert
  KVMap map_insert;
  MakeKVMap(kSampleSize_, &map_insert);

  // Put data in database
  for (auto& kv : map_insert) {
    ASSERT_OK(date_tiered_db_->Put(wopts, kv.first, kv.second));
  }

  Sleep(1);
  // T=1, keys should still reside in database
  for (auto& kv : map_insert) {
    std::string value;
    ASSERT_TRUE(date_tiered_db_->KeyMayExist(ropts, kv.first, &value));
    ASSERT_EQ(value, kv.second);
  }
}

// Database open and close should not affect
TEST_F(DateTieredTest, MultiOpen) {
  WriteOptions wopts;
  ReadOptions ropts;

  // T=0, open the database and insert data
  OpenDateTieredDB(4, 4);
  ASSERT_TRUE(date_tiered_db_.get() != nullptr);

  // Create key value pairs to insert
  KVMap map_insert;
  MakeKVMap(kSampleSize_, &map_insert);

  // Put data in database
  for (auto& kv : map_insert) {
    ASSERT_OK(date_tiered_db_->Put(wopts, kv.first, kv.second));
  }
  CloseDateTieredDB();

  Sleep(1);
  OpenDateTieredDB(2, 2);
  // T=1, keys should still reside in database
  for (auto& kv : map_insert) {
    std::string value;
    ASSERT_OK(date_tiered_db_->Get(ropts, kv.first, &value));
    ASSERT_EQ(value, kv.second);
  }

  Sleep(1);
  // T=2, keys should not be retrieved
  for (auto& kv : map_insert) {
    std::string value;
    auto s = date_tiered_db_->Get(ropts, kv.first, &value);
    ASSERT_TRUE(s.IsNotFound());
  }

  CloseDateTieredDB();
}

// If the key in Put() is obsolete, the data should not be written into database
TEST_F(DateTieredTest, InsertObsoleteDate) {
  WriteOptions wopts;
  ReadOptions ropts;

  // T=0, open the database and insert data
  OpenDateTieredDB(2, 2);
  ASSERT_TRUE(date_tiered_db_.get() != nullptr);

  // Create key value pairs to insert
  KVMap map_insert;
  MakeKVMap(kSampleSize_, &map_insert);

  Sleep(2);
  // T=2, keys put into database are already obsolete
  // Put data in database. Operations should not return OK
  for (auto& kv : map_insert) {
    auto s = date_tiered_db_->Put(wopts, kv.first, kv.second);
    ASSERT_TRUE(s.IsInvalidArgument());
  }

  // Data should not be found in database
  for (auto& kv : map_insert) {
    std::string value;
    auto s = date_tiered_db_->Get(ropts, kv.first, &value);
    ASSERT_TRUE(s.IsNotFound());
  }

  CloseDateTieredDB();
}

// Resets the timestamp of a set of kvs by updating them and checks that they
// are not deleted according to the old timestamp
TEST_F(DateTieredTest, ColumnFamilyCounts) {
  WriteOptions wopts;
  ReadOptions ropts;

  // T=0, open the database and insert data
  OpenDateTieredDB(4, 2);
  ASSERT_TRUE(date_tiered_db_.get() != nullptr);
  // Only default column family
  ASSERT_EQ(1, GetColumnFamilyCount());

  // Create key value pairs to insert
  KVMap map_insert;
  MakeKVMap(kSampleSize_, &map_insert);
  for (auto& kv : map_insert) {
    ASSERT_OK(date_tiered_db_->Put(wopts, kv.first, kv.second));
  }
  // A time series column family is created
  ASSERT_EQ(2, GetColumnFamilyCount());

  Sleep(2);
  KVMap map_insert2;
  MakeKVMap(kSampleSize_, &map_insert2);
  for (auto& kv : map_insert2) {
    ASSERT_OK(date_tiered_db_->Put(wopts, kv.first, kv.second));
  }
  // Another time series column family is created
  ASSERT_EQ(3, GetColumnFamilyCount());

  Sleep(4);

  // Data should not be found in database
  for (auto& kv : map_insert) {
    std::string value;
    auto s = date_tiered_db_->Get(ropts, kv.first, &value);
    ASSERT_TRUE(s.IsNotFound());
  }

  // Explicitly drop obsolete column families
  date_tiered_db_->DropObsoleteColumnFamilies();

  // The first column family is deleted from database
  ASSERT_EQ(2, GetColumnFamilyCount());

  CloseDateTieredDB();
}

// Puts a set of values and checks its presence using iterator during ttl
TEST_F(DateTieredTest, IteratorLifeCycle) {
  WriteOptions wopts;
  ReadOptions ropts;

  // T=0, open the database and insert data
  OpenDateTieredDB(2, 2);
  ASSERT_TRUE(date_tiered_db_.get() != nullptr);

  // Create key value pairs to insert
  KVMap map_insert;
  MakeKVMap(kSampleSize_, &map_insert);
  Iterator* dbiter;

  // Put data in database
  for (auto& kv : map_insert) {
    ASSERT_OK(date_tiered_db_->Put(wopts, kv.first, kv.second));
  }

  Sleep(1);
  ASSERT_EQ(2, GetColumnFamilyCount());
  // T=1, keys should still reside in database
  dbiter = date_tiered_db_->NewIterator(ropts);
  dbiter->SeekToFirst();
  for (auto& kv : map_insert) {
    ASSERT_TRUE(dbiter->Valid());
    ASSERT_EQ(0, dbiter->value().compare(kv.second));
    dbiter->Next();
  }
  delete dbiter;

  Sleep(4);
  // T=5, keys should not be retrieved
  for (auto& kv : map_insert) {
    std::string value;
    auto s = date_tiered_db_->Get(ropts, kv.first, &value);
    ASSERT_TRUE(s.IsNotFound());
  }

  // Explicitly drop obsolete column families
  date_tiered_db_->DropObsoleteColumnFamilies();

  // Only default column family
  ASSERT_EQ(1, GetColumnFamilyCount());

  // Empty iterator
  dbiter = date_tiered_db_->NewIterator(ropts);
  dbiter->Seek(map_insert.begin()->first);
  ASSERT_FALSE(dbiter->Valid());
  delete dbiter;

  CloseDateTieredDB();
}

// Iterator should be able to merge data from multiple column families
TEST_F(DateTieredTest, IteratorMerge) {
  WriteOptions wopts;
  ReadOptions ropts;

  // T=0, open the database and insert data
  OpenDateTieredDB(4, 2);
  ASSERT_TRUE(date_tiered_db_.get() != nullptr);

  Iterator* dbiter;

  // Put data in database
  KVMap map_insert1;
  MakeKVMap(kSampleSize_, &map_insert1);
  for (auto& kv : map_insert1) {
    ASSERT_OK(date_tiered_db_->Put(wopts, kv.first, kv.second));
  }
  ASSERT_EQ(2, GetColumnFamilyCount());

  Sleep(2);
  // Put more data
  KVMap map_insert2;
  MakeKVMap(kSampleSize_, &map_insert2);
  for (auto& kv : map_insert2) {
    ASSERT_OK(date_tiered_db_->Put(wopts, kv.first, kv.second));
  }
  // Multiple column families for time series data
  ASSERT_EQ(3, GetColumnFamilyCount());

  // Iterator should be able to merge data from different column families
  dbiter = date_tiered_db_->NewIterator(ropts);
  dbiter->SeekToFirst();
  KVMap::iterator iter1 = map_insert1.begin();
  KVMap::iterator iter2 = map_insert2.begin();
  for (; iter1 != map_insert1.end() && iter2 != map_insert2.end();
       iter1++, iter2++) {
    ASSERT_TRUE(dbiter->Valid());
    ASSERT_EQ(0, dbiter->value().compare(iter1->second));
    dbiter->Next();

    ASSERT_TRUE(dbiter->Valid());
    ASSERT_EQ(0, dbiter->value().compare(iter2->second));
    dbiter->Next();
  }
  delete dbiter;

  CloseDateTieredDB();
}

}  //  namespace rocksdb

// A black-box test for the DateTieredDB around rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr, "SKIPPED as DateTieredDB is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
