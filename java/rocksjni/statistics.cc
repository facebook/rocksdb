// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::Statistics methods from Java side.

#include "rocksdb/statistics.h"

#include <jni.h>

#include <memory>
#include <set>

#include "include/org_rocksdb_Statistics.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"
#include "rocksjni/statisticsjni.h"

/*
 * Class:     org_rocksdb_Statistics
 * Method:    newStatistics
 * Signature: ()J
 */
jlong Java_org_rocksdb_Statistics_newStatistics__(JNIEnv* env, jclass jcls) {
  return Java_org_rocksdb_Statistics_newStatistics___3BJ(env, jcls, nullptr, 0);
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
jlong Java_org_rocksdb_Statistics_newStatistics___3B(JNIEnv* env, jclass jcls,
                                                     jbyteArray jhistograms) {
  return Java_org_rocksdb_Statistics_newStatistics___3BJ(env, jcls, jhistograms,
                                                         0);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    newStatistics
 * Signature: ([BJ)J
 */
jlong Java_org_rocksdb_Statistics_newStatistics___3BJ(
    JNIEnv* env, jclass, jbyteArray jhistograms,
    jlong jother_statistics_handle) {
  std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>* pSptr_other_statistics =
      nullptr;
  if (jother_statistics_handle > 0) {
    pSptr_other_statistics =
        reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>*>(
            jother_statistics_handle);
  }

  std::set<uint32_t> histograms;
  if (jhistograms != nullptr) {
    const jsize len = env->GetArrayLength(jhistograms);
    if (len > 0) {
      jbyte* jhistogram = env->GetByteArrayElements(jhistograms, nullptr);
      if (jhistogram == nullptr) {
        // exception thrown: OutOfMemoryError
        return 0;
      }

      for (jsize i = 0; i < len; i++) {
        const ROCKSDB_NAMESPACE::Histograms histogram =
            ROCKSDB_NAMESPACE::HistogramTypeJni::toCppHistograms(jhistogram[i]);
        histograms.emplace(histogram);
      }

      env->ReleaseByteArrayElements(jhistograms, jhistogram, JNI_ABORT);
    }
  }

  std::shared_ptr<ROCKSDB_NAMESPACE::Statistics> sptr_other_statistics =
      nullptr;
  if (pSptr_other_statistics != nullptr) {
    sptr_other_statistics = *pSptr_other_statistics;
  }

  auto* pSptr_statistics =
      new std::shared_ptr<ROCKSDB_NAMESPACE::StatisticsJni>(
          new ROCKSDB_NAMESPACE::StatisticsJni(sptr_other_statistics,
                                               histograms));

  return GET_CPLUSPLUS_POINTER(pSptr_statistics);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Statistics_disposeInternal(JNIEnv*, jobject,
                                                 jlong jhandle) {
  if (jhandle > 0) {
    auto* pSptr_statistics =
        reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>*>(
            jhandle);
    delete pSptr_statistics;
  }
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    statsLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Statistics_statsLevel(JNIEnv*, jobject, jlong jhandle) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>*>(
          jhandle);
  assert(pSptr_statistics != nullptr);
  return ROCKSDB_NAMESPACE::StatsLevelJni::toJavaStatsLevel(
      pSptr_statistics->get()->get_stats_level());
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    setStatsLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_Statistics_setStatsLevel(JNIEnv*, jobject, jlong jhandle,
                                               jbyte jstats_level) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>*>(
          jhandle);
  assert(pSptr_statistics != nullptr);
  auto stats_level =
      ROCKSDB_NAMESPACE::StatsLevelJni::toCppStatsLevel(jstats_level);
  pSptr_statistics->get()->set_stats_level(stats_level);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getTickerCount
 * Signature: (JB)J
 */
jlong Java_org_rocksdb_Statistics_getTickerCount(JNIEnv*, jobject,
                                                 jlong jhandle,
                                                 jbyte jticker_type) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>*>(
          jhandle);
  assert(pSptr_statistics != nullptr);
  auto ticker = ROCKSDB_NAMESPACE::TickerTypeJni::toCppTickers(jticker_type);
  uint64_t count = pSptr_statistics->get()->getTickerCount(ticker);
  return static_cast<jlong>(count);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getAndResetTickerCount
 * Signature: (JB)J
 */
jlong Java_org_rocksdb_Statistics_getAndResetTickerCount(JNIEnv*, jobject,
                                                         jlong jhandle,
                                                         jbyte jticker_type) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>*>(
          jhandle);
  assert(pSptr_statistics != nullptr);
  auto ticker = ROCKSDB_NAMESPACE::TickerTypeJni::toCppTickers(jticker_type);
  return pSptr_statistics->get()->getAndResetTickerCount(ticker);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getHistogramData
 * Signature: (JB)Lorg/rocksdb/HistogramData;
 */
jobject Java_org_rocksdb_Statistics_getHistogramData(JNIEnv* env, jobject,
                                                     jlong jhandle,
                                                     jbyte jhistogram_type) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>*>(
          jhandle);
  assert(pSptr_statistics != nullptr);

  // TODO(AR) perhaps better to construct a Java Object Wrapper that
  //    uses ptr to C++ `new HistogramData`
  ROCKSDB_NAMESPACE::HistogramData data;

  auto histogram =
      ROCKSDB_NAMESPACE::HistogramTypeJni::toCppHistograms(jhistogram_type);
  pSptr_statistics->get()->histogramData(
      static_cast<ROCKSDB_NAMESPACE::Histograms>(histogram), &data);

  jclass jclazz = ROCKSDB_NAMESPACE::HistogramDataJni::getJClass(env);
  if (jclazz == nullptr) {
    // exception occurred accessing class
    return nullptr;
  }

  jmethodID mid =
      ROCKSDB_NAMESPACE::HistogramDataJni::getConstructorMethodId(env);
  if (mid == nullptr) {
    // exception occurred accessing method
    return nullptr;
  }

  return env->NewObject(jclazz, mid, data.median, data.percentile95,
                        data.percentile99, data.average,
                        data.standard_deviation, data.max, data.count, data.sum,
                        data.min);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getHistogramString
 * Signature: (JB)Ljava/lang/String;
 */
jstring Java_org_rocksdb_Statistics_getHistogramString(JNIEnv* env, jobject,
                                                       jlong jhandle,
                                                       jbyte jhistogram_type) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>*>(
          jhandle);
  assert(pSptr_statistics != nullptr);
  auto histogram =
      ROCKSDB_NAMESPACE::HistogramTypeJni::toCppHistograms(jhistogram_type);
  auto str = pSptr_statistics->get()->getHistogramString(histogram);
  return env->NewStringUTF(str.c_str());
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    reset
 * Signature: (J)V
 */
void Java_org_rocksdb_Statistics_reset(JNIEnv* env, jobject, jlong jhandle) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>*>(
          jhandle);
  assert(pSptr_statistics != nullptr);
  ROCKSDB_NAMESPACE::Status s = pSptr_statistics->get()->Reset();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    toString
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_Statistics_toString(JNIEnv* env, jobject,
                                             jlong jhandle) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>*>(
          jhandle);
  assert(pSptr_statistics != nullptr);
  auto str = pSptr_statistics->get()->ToString();
  return env->NewStringUTF(str.c_str());
}
