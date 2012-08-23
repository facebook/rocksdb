// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/status.h"
#include "leveldb/env.h"

#include <vector>
#include "util/coding.h"
#include "util/testharness.h"

namespace leveldb {

class LockTest {
 public:
  static LockTest* current_;
  std::string file_;
  leveldb::Env* env_;

  LockTest() : file_(test::TmpDir() + "/db_testlock_file"),
               env_(leveldb::Env::Default()) {
    current_ = this;
  }

  ~LockTest() {
  }

  Status LockFile(FileLock** db_lock) {
    return env_->LockFile(file_, db_lock);
  }

  Status UnlockFile(FileLock* db_lock) {
    return env_->UnlockFile(db_lock);
  }
};
LockTest* LockTest::current_;

TEST(LockTest, LockBySameThread) {
  FileLock* lock1;
  FileLock* lock2;

  // acquire a lock on a file
  ASSERT_OK(LockFile(&lock1));

  // re-acquire the lock on the same file. This should fail.
  ASSERT_TRUE(LockFile(&lock2).IsIOError());

  // release the lock
  ASSERT_OK(UnlockFile(lock1));

}

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
