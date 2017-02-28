// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
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
    JNIEnv* env, jobject jobj, jint tickerType, jlong handle) {
  auto* st = reinterpret_cast<rocksdb::Statistics*>(handle);
  assert(st != nullptr);
  return st->getTickerCount(static_cast<rocksdb::Tickers>(tickerType));
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getHistogramData0
 * Signature: (IJ)Lorg/rocksdb/HistogramData;
 */
jobject Java_org_rocksdb_Statistics_getHistogramData0(
    JNIEnv* env, jobject jobj, jint histogramType, jlong handle) {
  auto* st = reinterpret_cast<rocksdb::Statistics*>(handle);
  assert(st != nullptr);

  rocksdb::HistogramData data;
  st->histogramData(static_cast<rocksdb::Histograms>(histogramType),
    &data);

  jclass jclazz = rocksdb::HistogramDataJni::getJClass(env);
  if(jclazz == nullptr) {
    // exception occurred accessing class
    return nullptr;
  }

  jmethodID mid = rocksdb::HistogramDataJni::getConstructorMethodId(
      env);
  if(mid == nullptr) {
    // exception occurred accessing method
    return nullptr;
  }

  return env->NewObject(
      jclazz,
      mid, data.median, data.percentile95,data.percentile99, data.average,
      data.standard_deviation);
}
