#include <climits>// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include "include/org_rocksdb_FlinkCompactionFilter.h"
#include <include/rocksdb/env.h>
#include "utilities/flink/flink_compaction_filter.h"
#include "rocksjni/jnicallback.h"
#include "loggerjnicallback.h"
#include "portal.h"

using namespace rocksdb::flink;

class JavaListElementIter : public rocksdb::flink::FlinkCompactionFilter::ListElementIter, rocksdb::JniCallback {
public:
  JavaListElementIter(JNIEnv* env, jobject jlist_iter) : JniCallback(env, jlist_iter) {
      jclass jclazz = rocksdb::JavaClass::getJClass(env, "org/rocksdb/FlinkCompactionFilter$ListElementIter");
      if(jclazz == nullptr) {
        // exception occurred accessing class
        return;
      }
      m_jset_list_data_methodid = env->GetMethodID(jclazz, "setListBytes", "([B)V");
      assert(m_jset_list_data_methodid != nullptr);
      m_jnext_offset_methodid = env->GetMethodID(jclazz, "nextOffset", "(I)I");
      assert(m_jnext_offset_methodid != nullptr);

  }

  inline void SetListBytes(const rocksdb::Slice& list) const override {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv* env = getJniEnv(&attached_thread);
    jbyteArray jlist = rocksdb::JniUtil::copyBytes(env, list);
    CheckAndRethrowException(env);
    if (jlist == nullptr) {
      return;
    }
    env->CallVoidMethod(m_jcallback_obj, m_jset_list_data_methodid, jlist);
    CheckAndRethrowException(env);
    env->DeleteLocalRef(jlist);
    releaseJniEnv(attached_thread);
  };

  inline std::size_t NextOffset(std::size_t current_offset) const override {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv* env = getJniEnv(&attached_thread);
    auto ji_current_offset = static_cast<jint>(current_offset);
    jint next_offset = env->CallIntMethod(m_jcallback_obj, m_jnext_offset_methodid, ji_current_offset);
    CheckAndRethrowException(env);
    releaseJniEnv(attached_thread);
    return static_cast<std::size_t>(next_offset);
  };

private:
  inline void CheckAndRethrowException(JNIEnv* env) const {
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->Throw(env->ExceptionOccurred());
    }
  }

  jmethodID m_jset_list_data_methodid;
  jmethodID m_jnext_offset_methodid;
};

static FlinkCompactionFilter::ListElementIter* createListElementIter(JNIEnv* env, jint ji_list_elem_len, jobject jlist_iter) {
  FlinkCompactionFilter::ListElementIter* list_iter = nullptr;
  if (ji_list_elem_len > 0) {
    std::size_t fixed_size = static_cast<std::size_t>(ji_list_elem_len);
    list_iter = new FlinkCompactionFilter::FixedListElementIter(fixed_size);
  } else if (jlist_iter != nullptr) {
    list_iter = new JavaListElementIter(env, jlist_iter);
  }
  return list_iter;
}

/*
 * Class:     org_rocksdb_FlinkCompactionFilter
 * Method:    createNewFlinkCompactionFilter0
 * Signature: (J)J
 */
jlong Java_org_rocksdb_FlinkCompactionFilter_createNewFlinkCompactionFilter0(
        JNIEnv* /* env */, jclass /* jcls */, jlong logger_handle) {
  using namespace rocksdb::flink;
  auto logger = logger_handle == 0 ? nullptr :
          *(reinterpret_cast<std::shared_ptr<rocksdb::LoggerJniCallback>*>(logger_handle));
  return reinterpret_cast<jlong>(new FlinkCompactionFilter(logger));
}

/*
 * Class:     org_rocksdb_FlinkCompactionFilter
 * Method:    configureFlinkCompactionFilter
 * Signature: (JIIJZILorg/rocksdb/FlinkCompactionFilter$ListElementIter;)J
 */
jlong Java_org_rocksdb_FlinkCompactionFilter_configureFlinkCompactionFilter(
        JNIEnv* env, jclass /* jcls */,
        jlong handle, jint ji_state_type, jint ji_timestamp_offset,
        jlong jl_ttl_milli, jboolean jb_use_system_time, jint ji_list_elem_len, jobject jlist_iter) {
  auto state_type = static_cast<FlinkCompactionFilter::StateType>(ji_state_type);
  auto timestamp_offset = static_cast<size_t>(ji_timestamp_offset);
  auto ttl = static_cast<int64_t>(jl_ttl_milli);
  auto use_system_time = (bool)(jb_use_system_time == JNI_TRUE);
  auto filter = reinterpret_cast<FlinkCompactionFilter*>(handle);
  auto list_iter = createListElementIter(env, ji_list_elem_len, jlist_iter);
  filter->Configure(new FlinkCompactionFilter::Config{state_type, timestamp_offset, ttl, use_system_time, list_iter});
  return handle;
}

/*
 * Class:     org_rocksdb_FlinkCompactionFilter
 * Method:    setCurrentTimestamp
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_FlinkCompactionFilter_setCurrentTimestamp(
        JNIEnv* /* env */, jclass /* jcls */, jlong handle, jlong jl_current_timestamp) {
  using namespace rocksdb::flink;
  auto filter = reinterpret_cast<FlinkCompactionFilter*>(handle);
  auto current_timestamp = static_cast<int64_t>(jl_current_timestamp);
  filter->SetCurrentTimestamp(current_timestamp);
  return handle;
}