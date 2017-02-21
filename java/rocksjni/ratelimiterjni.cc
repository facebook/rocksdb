// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ for RateLimiter.

#include "rocksjni/portal.h"
#include "include/org_rocksdb_RateLimiter.h"
#include "rocksdb/rate_limiter.h"

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    newRateLimiterHandle
 * Signature: (JJI)J
 */
jlong Java_org_rocksdb_RateLimiter_newRateLimiterHandle(
    JNIEnv* env, jclass jclazz, jlong jrate_bytes_per_second,
    jlong jrefill_period_micros, jint jfairness) {
  auto * sptr_rate_limiter =
      new std::shared_ptr<rocksdb::RateLimiter>(rocksdb::NewGenericRateLimiter(
          static_cast<int64_t>(jrate_bytes_per_second),
          static_cast<int64_t>(jrefill_period_micros),
          static_cast<int32_t>(jfairness)));

  return reinterpret_cast<jlong>(sptr_rate_limiter);
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RateLimiter_disposeInternal(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<rocksdb::RateLimiter> *>(jhandle);
  delete handle;  // delete std::shared_ptr
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    setBytesPerSecond
 * Signature: (JJ)V
 */
void Java_org_rocksdb_RateLimiter_setBytesPerSecond(
    JNIEnv* env, jobject jobj, jlong handle,
    jlong jbytes_per_second) {
  reinterpret_cast<std::shared_ptr<rocksdb::RateLimiter> *>(handle)->get()->
      SetBytesPerSecond(jbytes_per_second);
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    request
 * Signature: (JJ)V
 */
void Java_org_rocksdb_RateLimiter_request(
    JNIEnv* env, jobject jobj, jlong handle,
    jlong jbytes) {
  reinterpret_cast<std::shared_ptr<rocksdb::RateLimiter> *>(handle)->get()->
      Request(jbytes, rocksdb::Env::IO_TOTAL);
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    getSingleBurstBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RateLimiter_getSingleBurstBytes(
    JNIEnv* env, jobject jobj, jlong handle) {
  return reinterpret_cast<std::shared_ptr<rocksdb::RateLimiter> *>(handle)->
      get()->GetSingleBurstBytes();
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    getTotalBytesThrough
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RateLimiter_getTotalBytesThrough(
    JNIEnv* env, jobject jobj, jlong handle) {
  return reinterpret_cast<std::shared_ptr<rocksdb::RateLimiter> *>(handle)->
      get()->GetTotalBytesThrough();
}

/*
 * Class:     org_rocksdb_RateLimiter
 * Method:    getTotalRequests
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RateLimiter_getTotalRequests(
    JNIEnv* env, jobject jobj, jlong handle) {
  return reinterpret_cast<std::shared_ptr<rocksdb::RateLimiter> *>(handle)->
      get()->GetTotalRequests();
}
