//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "rocksdb/status.h"
#include "rocksdb/env.h"

#include <fstream>
#include <regex>
#include <vector>
#include "util/coding.h"
#include "util/testharness.h"

namespace rocksdb {

class LockTest : public testing::Test {
 public:
  static LockTest* current_;
  std::string file_;
  rocksdb::Env* env_;

  LockTest() : file_(test::TmpDir() + "/db_testlock_file"),
               env_(rocksdb::Env::Default()) {
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

  bool IsFileLocked(){
    std::regex regex;
    {
      struct stat var;
      if( stat(file_.c_str(),&var) < 0 ) {
	return false;
      }
      ino_t inode = var.st_ino;

      std::ostringstream oss;
      oss << ".*POSIX +ADVISORY + WRITE +" << getpid() << ".*:" << inode << " +0 +EOF";
      regex =  std::regex( oss.str() );
    }

    std::ifstream ifs("/proc/locks", std::ios::in);
    ifs.exceptions(std::ios::badbit);
    while (ifs.good() && !ifs.eof()) {
      std::string line;
      if (!getline(ifs, line, '\n')) { break; }
      if ( std::regex_match (line, regex) ) {
	return true;
      }
    }
    return false;
  }

};
LockTest* LockTest::current_;

TEST_F(LockTest, LockBySameThread) {
  FileLock* lock1;
  FileLock* lock2;

  // acquire a lock on a file
  ASSERT_OK(LockFile(&lock1));

  // check the file is locked
  ASSERT_TRUE( IsFileLocked() );

  // re-acquire the lock on the same file. This should fail.
  ASSERT_TRUE(LockFile(&lock2).IsIOError());

  // check the file is locked
  ASSERT_TRUE( IsFileLocked() );

  // release the lock
  ASSERT_OK(UnlockFile(lock1));

  // check the file is not locked
  ASSERT_TRUE( ! IsFileLocked() );

}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
