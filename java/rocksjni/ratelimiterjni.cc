// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ for RateLimiter.

#include "rocksjni/portal.h"
#include "include/org_rocksdb_GenericRateLimiterConfig.h"
#include "include/org_rocksdb_RateLimiter.h"
#include "rocksdb/rate_limiter.h"

/*
 * Class:     org_rocksdb_GenericRateLimiterConfig
 * Method:    newRateLimiterHandle
 * Signature: (JJI)J
 */
jlong Java_org_rocksdb_GenericRateLimiterConfig_newRateLimiterHandle(
    JNIEnv* env, jobject jobj, jlong jrate_bytes_per_second,
    jlong jrefill_period_micros, jint jfairness) {
  return reinterpret_cast<jlong>(rocksdb::NewGenericRateLimiter(
      static_cast<int64_t>(jrate_bytes_per_second),
      static_cast<int64_t>(jrefill_period_micros),
      static_cast<int32_t>(jfairness)));
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    newRateLimiterHandle
 * Signature: (JJI)J
 */
jlong Java_org_rocksdb_RateLimiter_newRateLimiterHandle(
    JNIEnv* env, jclass jclazz, jlong jrate_bytes_per_second,
    jlong jrefill_period_micros, jint jfairness) {
  auto* rate_limiter = rocksdb::NewGenericRateLimiter(
      static_cast<int64_t>(jrate_bytes_per_second),
      static_cast<int64_t>(jrefill_period_micros),
      static_cast<int32_t>(jfairness));

  std::shared_ptr<rocksdb::RateLimiter> *ptr_sptr_rate_limiter =
    new std::shared_ptr<rocksdb::RateLimiter>;
  *ptr_sptr_rate_limiter = std::shared_ptr<rocksdb::RateLimiter>(rate_limiter);

  return reinterpret_cast<jlong>(ptr_sptr_rate_limiter);
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RateLimiter_disposeInternal(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  std::shared_ptr<rocksdb::RateLimiter> *handle =
      reinterpret_cast<std::shared_ptr<rocksdb::RateLimiter> *>(jhandle);
  handle->reset();
  delete handle;
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    setBytesPerSecond
 * Signature: (JJ)V
 */
void Java_org_rocksdb_RateLimiter_setBytesPerSecond(
    JNIEnv* env, jobject jobj, jlong handle,
    jlong jbytes_per_second) {
  reinterpret_cast<rocksdb::RateLimiter*>(
      handle)->SetBytesPerSecond(jbytes_per_second);
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    request
 * Signature: (JJ)V
 */
void Java_org_rocksdb_RateLimiter_request(
    JNIEnv* env, jobject jobj, jlong handle,
    jlong jbytes) {
  reinterpret_cast<rocksdb::RateLimiter*>(
      handle)->Request(jbytes,
      rocksdb::Env::IO_TOTAL);
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    getSingleBurstBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RateLimiter_getSingleBurstBytes(
    JNIEnv* env, jobject jobj, jlong handle,
    jlong jbytes) {
  return reinterpret_cast<rocksdb::RateLimiter*>(
      handle)->GetSingleBurstBytes();
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    getTotalBytesThrough
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RateLimiter_getTotalBytesThrough(
    JNIEnv* env, jobject jobj, jlong handle,
    jlong jbytes) {
  return reinterpret_cast<rocksdb::RateLimiter*>(
      handle)->GetTotalBytesThrough();
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    getTotalRequests
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RateLimiter_getTotalRequests(
    JNIEnv* env, jobject jobj, jlong handle,
    jlong jbytes) {
  return reinterpret_cast<rocksdb::RateLimiter*>(
      handle)->GetTotalRequests();
}
