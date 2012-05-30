// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdlib.h>
#include "leveldb/statistics.h"
#include "port/port.h"
#include "util/mutexlock.h"

namespace leveldb {

class DBStatistics: public Statistics {
 public:
  DBStatistics() { }

  void incNumFileOpens() {
    MutexLock l(&mu_);
    numFileOpens_++;
  }

  void incNumFileCloses() {
    MutexLock l(&mu_);
    numFileCloses_++;
  }

  void incNumFileErrors() {
    MutexLock l(&mu_);
    numFileErrors_++;
  }

 private:
    port::Mutex mu_;
};
}


