//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "util/testharness.h"

#include <algorithm>
#include <vector>
#include <string>

namespace rocksdb {

using namespace std;

class ColumnFamilyTest {
 public:
  ColumnFamilyTest() {
    dbname_ = test::TmpDir() + "/column_family_test";
    db_options_.create_if_missing = true;
    options_.create_if_missing = true;
    DestroyDB(dbname_, options_);
  }

  void Close() {
    delete db_;
    db_ = nullptr;
  }

  void Open() {
    ASSERT_OK(DB::Open(options_, dbname_, &db_));
  }

  Options options_;
  ColumnFamilyOptions column_family_options_;
  DBOptions db_options_;
  string dbname_;
  DB* db_;
};

TEST(ColumnFamilyTest, AddDrop) {
  Open();
  ColumnFamilyHandle handles[4];
  ASSERT_OK(db_->CreateColumnFamily(column_family_options_, Slice("one"),
                                    &handles[0]));
  ASSERT_OK(db_->CreateColumnFamily(column_family_options_, Slice("two"),
                                    &handles[1]));
  ASSERT_OK(db_->CreateColumnFamily(column_family_options_, Slice("three"),
                                    &handles[2]));
  ASSERT_OK(db_->DropColumnFamily(handles[1]));
  ASSERT_OK(db_->CreateColumnFamily(column_family_options_, Slice("four"),
                                    &handles[3]));
  Close();
  Open(); // this will roll the manifest, column families should stay consistent
  Close();

  vector<string> families;
  DB::ListColumnFamilies(db_options_, dbname_, &families);
  sort(families.begin(), families.end());
  ASSERT_TRUE(families == vector<string>({"four", "one", "three"}));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}
