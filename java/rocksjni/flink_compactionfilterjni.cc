#include <climits>// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include "include/org_rocksdb_FlinkCompactionFilter.h"
#include "utilities/flink/flink_compaction_filter.h"
#include "rocksjni/jnicallback.h"

#define TIME_PROVIDER_METHOD "currentTimestamp"
#define JTIME_PROVIDER_CLASS "org/rocksdb/FlinkCompactionFilter$TimeProvider"

/**
 * Wrapper around org/rocksdb/FlinkCompactionFilter$TimeProvider::currentTimestamp method.
 */
class FlinkTimeProvider : rocksdb::JniCallback, public rocksdb::flink::TimeProvider {
  public:
    FlinkTimeProvider(JNIEnv* env, jobject jtime_provider) : JniCallback(env, jtime_provider) {
      jclass jcls = env->FindClass(JTIME_PROVIDER_CLASS);
      assert(jcls != nullptr);
      m_jcurrent_timestamp_method_methodid = env->GetMethodID(jcls, TIME_PROVIDER_METHOD, "()J");
      assert(m_jcurrent_timestamp_method_methodid != nullptr);
    }

    int64_t CurrentTimestamp() const override {
      jboolean attached_thread = JNI_FALSE;
      JNIEnv* env = getJniEnv(&attached_thread);
      assert(env != nullptr);

      jlong ts = env->CallLongMethod(m_jcallback_obj, m_jcurrent_timestamp_method_methodid);

      if(env->ExceptionCheck()) {
          // exception thrown from CallVoidMethod
          env->ExceptionDescribe();  // print out exception to stderr
          ts = 0;
      }
      releaseJniEnv(attached_thread);
      return static_cast<int64_t>(ts);
    };

  private:
    jmethodID m_jcurrent_timestamp_method_methodid;
};

/*
 * Class:     org_rocksdb_FlinkCompactionFilter
 * Method:    createNewFlinkCompactionFilter0
 * Signature: (Ljava/lang/Object;IJZ)J
 */
jlong  __unused Java_org_rocksdb_FlinkCompactionFilter_createNewFlinkCompactionFilter0(
    JNIEnv* env, jclass /* jcls */, jobject jtime_provider, jint ji_state_type, jlong jl_ttl_milli, jboolean use_system_time) {
  using namespace rocksdb::flink;
  auto state_type = static_cast<FlinkCompactionFilter::StateType>(ji_state_type);
  auto ttl = static_cast<int64_t>(jl_ttl_milli);
  auto* compaction_filter = (bool)(use_system_time == JNI_TRUE) ?
    new FlinkCompactionFilter(state_type, ttl) :
    new FlinkCompactionFilter(state_type, ttl, new FlinkTimeProvider(env, jtime_provider));
  // set the native handle to our native compaction filter
  return reinterpret_cast<jlong>(compaction_filter);
}
