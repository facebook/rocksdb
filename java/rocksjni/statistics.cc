// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::Statistics methods from Java side.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>

#include "include/org_rocksdb_Statistics.h"
#include "rocksjni/portal.h"
#include "rocksdb/statistics.h"

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getTickerCount0
 * Signature: (IJ)J
 */
jlong Java_org_rocksdb_Statistics_getTickerCount0(
    JNIEnv* env, jobject jobj, int ticker, jlong handle) {
  auto st = reinterpret_cast<rocksdb::Statistics*>(handle);
  assert(st != nullptr);
  
  return st->getTickerCount(static_cast<rocksdb::Tickers>(ticker));
}
