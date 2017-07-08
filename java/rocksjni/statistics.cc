// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::Statistics methods from Java side.

#include <jni.h>
#include <memory>
#include <set>

#include "include/org_rocksdb_Statistics.h"
#include "rocksjni/portal.h"
#include "rocksjni/statisticsjni.h"
#include "rocksdb/statistics.h"

/*
 * Class:     org_rocksdb_Statistics
 * Method:    newStatistics
 * Signature: ()J
 */
jlong Java_org_rocksdb_Statistics_newStatistics__(JNIEnv* env, jclass jcls) {
  return Java_org_rocksdb_Statistics_newStatistics___3BJ(
      env, jcls, nullptr, 0);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    newStatistics
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Statistics_newStatistics__J(
    JNIEnv* env, jclass jcls, jlong jother_statistics_handle) {
  return Java_org_rocksdb_Statistics_newStatistics___3BJ(
      env, jcls, nullptr, jother_statistics_handle);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    newStatistics
 * Signature: ([B)J
 */
jlong Java_org_rocksdb_Statistics_newStatistics___3B(
    JNIEnv* env, jclass jcls, jbyteArray jhistograms) {
  return Java_org_rocksdb_Statistics_newStatistics___3BJ(
      env, jcls, jhistograms, 0);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    newStatistics
 * Signature: ([BJ)J
 */
jlong Java_org_rocksdb_Statistics_newStatistics___3BJ(
    JNIEnv* env, jclass jcls, jbyteArray jhistograms,
    jlong jother_statistics_handle) {

  std::shared_ptr<rocksdb::Statistics>* pSptr_other_statistics = nullptr;
  if (jother_statistics_handle > 0) {
    pSptr_other_statistics =
        reinterpret_cast<std::shared_ptr<rocksdb::Statistics>*>(
            jother_statistics_handle);
  }

  std::set<uint32_t> histograms;
  if (jhistograms != nullptr) {
    const jsize len = env->GetArrayLength(jhistograms);
    if (len > 0) {
      jbyte* jhistogram = env->GetByteArrayElements(jhistograms, nullptr);
      if (jhistogram == nullptr ) {
        // exception thrown: OutOfMemoryError
        return 0;
      }

      for (jsize i = 0; i < len; i++) {
        const rocksdb::Histograms histogram =
            rocksdb::HistogramTypeJni::toCppHistograms(jhistogram[i]);
        histograms.emplace(histogram);
      }

      env->ReleaseByteArrayElements(jhistograms, jhistogram, JNI_ABORT);
    }
  }

  std::shared_ptr<rocksdb::Statistics> sptr_other_statistics = nullptr;
  if (pSptr_other_statistics != nullptr) {
      sptr_other_statistics =   *pSptr_other_statistics;
  }

  auto* pSptr_statistics = new std::shared_ptr<rocksdb::StatisticsJni>(
      new rocksdb::StatisticsJni(sptr_other_statistics, histograms));

  return reinterpret_cast<jlong>(pSptr_statistics);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Statistics_disposeInternal(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  if(jhandle > 0) {
    auto* pSptr_statistics =
        reinterpret_cast<std::shared_ptr<rocksdb::Statistics>*>(jhandle);
    delete pSptr_statistics;
  }
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    statsLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Statistics_statsLevel(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<rocksdb::Statistics>*>(jhandle);
  assert(pSptr_statistics != nullptr);
  return rocksdb::StatsLevelJni::toJavaStatsLevel(pSptr_statistics->get()->stats_level_);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    setStatsLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_Statistics_setStatsLevel(
    JNIEnv* env, jobject jobj, jlong jhandle, jbyte jstats_level) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<rocksdb::Statistics>*>(jhandle);
  assert(pSptr_statistics != nullptr);
  auto stats_level = rocksdb::StatsLevelJni::toCppStatsLevel(jstats_level);
  pSptr_statistics->get()->stats_level_ = stats_level;
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getTickerCount0
 * Signature: (JB)J
 */
jlong Java_org_rocksdb_Statistics_getTickerCount0(
    JNIEnv* env, jobject jobj, jlong jhandle, jbyte jticker_type) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<rocksdb::Statistics>*>(jhandle);
  assert(pSptr_statistics != nullptr);
  auto ticker = rocksdb::TickerTypeJni::toCppTickers(jticker_type);
  return pSptr_statistics->get()->getTickerCount(ticker);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getHistogramData0
 * Signature: (JB)Lorg/rocksdb/HistogramData;
 */
jobject Java_org_rocksdb_Statistics_getHistogramData0(
    JNIEnv* env, jobject jobj, jlong jhandle, jbyte jhistogram_type) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<rocksdb::Statistics>*>(jhandle);
  assert(pSptr_statistics != nullptr);

  rocksdb::HistogramData data;  // TODO(AR) perhaps better to construct a Java Object Wrapper that uses ptr to C++ `new HistogramData`
  auto histogram = rocksdb::HistogramTypeJni::toCppHistograms(jhistogram_type);
  pSptr_statistics->get()->histogramData(
      static_cast<rocksdb::Histograms>(histogram), &data);

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
