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
 * Method:    statsLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Statistics_statsLevel(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto* st = reinterpret_cast<rocksdb::Statistics*>(handle);
  assert(st != nullptr);
  return rocksdb::StatsLevelJni::toJavaStatsLevel(st->stats_level_);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    setStatsLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_Statistics_setStatsLevel(
    JNIEnv* env, jobject jobj, jlong handle, jbyte jstats_level) {
  auto* st = reinterpret_cast<rocksdb::Statistics*>(handle);
  assert(st != nullptr);
  auto stats_level = rocksdb::StatsLevelJni::toCppStatsLevel(jstats_level);
  st->stats_level_ = stats_level;
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getTickerCount0
 * Signature: (JB)J
 */
jlong Java_org_rocksdb_Statistics_getTickerCount0(
    JNIEnv* env, jobject jobj, jlong handle, jbyte jticker_type) {
  auto* st = reinterpret_cast<rocksdb::Statistics*>(handle);
  assert(st != nullptr);
  auto ticker = rocksdb::TickerTypeJni::toCppTickers(jticker_type);
  return st->getTickerCount(ticker);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getHistogramData0
 * Signature: (JB)Lorg/rocksdb/HistogramData;
 */
jobject Java_org_rocksdb_Statistics_getHistogramData0(
    JNIEnv* env, jobject jobj, jlong handle, jbyte jhistogram_type) {
  auto* st = reinterpret_cast<rocksdb::Statistics*>(handle);
  assert(st != nullptr);

  rocksdb::HistogramData data;
  auto histogram = rocksdb::HistogramTypeJni::toCppHistograms(jhistogram_type);
  st->histogramData(static_cast<rocksdb::Histograms>(histogram),
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
