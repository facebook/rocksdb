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

class JniCallbackBase : public rocksdb::JniCallback {
public:
    JniCallbackBase(JNIEnv *env, jobject jcallback_obj) : JniCallback(env, jcallback_obj) {}
protected:
    inline void CheckAndRethrowException(JNIEnv* env) const {
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->Throw(env->ExceptionOccurred());
    }
  }
};

// This list element filter operates on list state for which byte length of elements is unknown (variable),
// the list element serializer has to be used in this case to compute the offset of the next element.
// The filter wraps java object implenented in Flink. The java object holds element serializer and performs filtering.
class JavaListElementFilter : public rocksdb::flink::FlinkCompactionFilter::ListElementFilter, JniCallbackBase {
public:
  JavaListElementFilter(JNIEnv* env, jobject jlist_filter) : JniCallbackBase(env, jlist_filter) {
      jclass jclazz = rocksdb::JavaClass::getJClass(env, "org/rocksdb/FlinkCompactionFilter$ListElementFilter");
      if(jclazz == nullptr) {
        // exception occurred accessing class
        return;
      }
      m_jnext_unexpired_offset_methodid = env->GetMethodID(jclazz, "nextUnexpiredOffset", "([BJJ)I");
      assert(m_jnext_unexpired_offset_methodid != nullptr);

  }

  std::size_t NextUnexpiredOffset(const rocksdb::Slice& list, int64_t ttl, int64_t current_timestamp) const override {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv* env = getJniEnv(&attached_thread);
    jbyteArray jlist = rocksdb::JniUtil::copyBytes(env, list);
    CheckAndRethrowException(env);
    if (jlist == nullptr) {
      return static_cast<std::size_t>(-1);
    }
    auto jl_ttl = static_cast<jlong>(ttl);
    auto jl_current_timestamp = static_cast<jlong>(current_timestamp);
    jint next_offset = env->CallIntMethod(m_jcallback_obj, m_jnext_unexpired_offset_methodid, jlist, jl_ttl, jl_current_timestamp);
    CheckAndRethrowException(env);
    env->DeleteLocalRef(jlist);
    releaseJniEnv(attached_thread);
    return static_cast<std::size_t>(next_offset);
  };

private:
  jmethodID m_jnext_unexpired_offset_methodid;
};

class JavaListElemenFilterFactory : public rocksdb::flink::FlinkCompactionFilter::ListElementFilterFactory, JniCallbackBase {
public:
    JavaListElemenFilterFactory(JNIEnv* env, jobject jlist_filter_factory) : JniCallbackBase(env, jlist_filter_factory) {
        jclass jclazz = rocksdb::JavaClass::getJClass(env, "org/rocksdb/FlinkCompactionFilter$ListElementFilterFactory");
        if(jclazz == nullptr) {
            // exception occurred accessing class
            return;
        }
        m_jcreate_filter_methodid = env->GetMethodID(jclazz,
                "createListElementFilter", "()Lorg/rocksdb/FlinkCompactionFilter$ListElementFilter;");
        assert(m_jcreate_filter_methodid != nullptr);
    }

    FlinkCompactionFilter::ListElementFilter* CreateListElementFilter(std::shared_ptr<rocksdb::Logger> /*logger*/) const override {
        jboolean attached_thread = JNI_FALSE;
        JNIEnv* env = getJniEnv(&attached_thread);
        auto jlist_filter = env->CallObjectMethod(m_jcallback_obj, m_jcreate_filter_methodid);
        auto list_filter = new JavaListElementFilter(env, jlist_filter);
        CheckAndRethrowException(env);
        releaseJniEnv(attached_thread);
        return list_filter;
    };

private:
    jmethodID m_jcreate_filter_methodid;
};

class JavaTimeProvider : public rocksdb::flink::FlinkCompactionFilter::TimeProvider, JniCallbackBase {
public:
    JavaTimeProvider(JNIEnv* env, jobject jtime_provider) : JniCallbackBase(env, jtime_provider) {
        jclass jclazz = rocksdb::JavaClass::getJClass(env, "org/rocksdb/FlinkCompactionFilter$TimeProvider");
        if(jclazz == nullptr) {
            // exception occurred accessing class
            return;
        }
        m_jcurrent_timestamp_methodid = env->GetMethodID(jclazz, "currentTimestamp", "()J");
        assert(m_jcurrent_timestamp_methodid != nullptr);
    }

    int64_t CurrentTimestamp() const override {
        jboolean attached_thread = JNI_FALSE;
        JNIEnv* env = getJniEnv(&attached_thread);
        auto jtimestamp = env->CallLongMethod(m_jcallback_obj, m_jcurrent_timestamp_methodid);
        CheckAndRethrowException(env);
        releaseJniEnv(attached_thread);
        return static_cast<int64_t>(jtimestamp);
    };

private:
    jmethodID m_jcurrent_timestamp_methodid;
};

static FlinkCompactionFilter::ListElementFilterFactory* createListElementFilterFactory(
        JNIEnv *env, jint ji_list_elem_len, jobject jlist_filter_factory) {
  FlinkCompactionFilter::ListElementFilterFactory* list_filter_factory = nullptr;
  if (ji_list_elem_len > 0) {
    auto fixed_size = static_cast<std::size_t>(ji_list_elem_len);
    list_filter_factory = new FlinkCompactionFilter::FixedListElementFilterFactory(fixed_size, static_cast<std::size_t>(0));
  } else if (jlist_filter_factory != nullptr) {
    list_filter_factory = new JavaListElemenFilterFactory(env, jlist_filter_factory);
  }
  return list_filter_factory;
}

/*x
 * Class:     org_rocksdb_FlinkCompactionFilter
 * Method:    createNewFlinkCompactionFilterConfigHolder
 * Signature: ()J
 */
jlong Java_org_rocksdb_FlinkCompactionFilter_createNewFlinkCompactionFilterConfigHolder(
        JNIEnv* /* env */, jclass /* jcls */) {
    using namespace rocksdb::flink;
    return reinterpret_cast<jlong>(
            new std::shared_ptr<FlinkCompactionFilter::ConfigHolder>(new FlinkCompactionFilter::ConfigHolder()));
}

/*
 * Class:     org_rocksdb_FlinkCompactionFilter
 * Method:    disposeFlinkCompactionFilterConfigHolder
 * Signature: (J)V
 */
void Java_org_rocksdb_FlinkCompactionFilter_disposeFlinkCompactionFilterConfigHolder(
        JNIEnv* /* env */, jclass /* jcls */, jlong handle) {
    using namespace rocksdb::flink;
    auto* config_holder = reinterpret_cast<std::shared_ptr<FlinkCompactionFilter::ConfigHolder>*>(handle);
    delete config_holder;
}

/*
 * Class:     org_rocksdb_FlinkCompactionFilter
 * Method:    createNewFlinkCompactionFilter0
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_FlinkCompactionFilter_createNewFlinkCompactionFilter0(
        JNIEnv* env, jclass /* jcls */, jlong config_holder_handle, jobject jtime_provider, jlong logger_handle) {
  using namespace rocksdb::flink;
  auto config_holder = *(reinterpret_cast<std::shared_ptr<FlinkCompactionFilter::ConfigHolder>*>(config_holder_handle));
  auto time_provider = new JavaTimeProvider(env, jtime_provider);
  auto logger = logger_handle == 0 ? nullptr :
          *(reinterpret_cast<std::shared_ptr<rocksdb::LoggerJniCallback>*>(logger_handle));
  return reinterpret_cast<jlong>(new FlinkCompactionFilter(
          config_holder, std::unique_ptr<FlinkCompactionFilter::TimeProvider>(time_provider), logger));
}

/*
 * Class:     org_rocksdb_FlinkCompactionFilter
 * Method:    configureFlinkCompactionFilter
 * Signature: (JIIJJILorg/rocksdb/FlinkCompactionFilter$ListElementFilter;)Z
 */
jboolean Java_org_rocksdb_FlinkCompactionFilter_configureFlinkCompactionFilter(
        JNIEnv* env, jclass /* jcls */,
        jlong handle,
        jint ji_state_type,
        jint ji_timestamp_offset,
        jlong jl_ttl_milli,
        jlong jquery_time_after_num_entries,
        jint ji_list_elem_len,
        jobject jlist_filter_factory) {
  auto state_type = static_cast<FlinkCompactionFilter::StateType>(ji_state_type);
  auto timestamp_offset = static_cast<size_t>(ji_timestamp_offset);
  auto ttl = static_cast<int64_t>(jl_ttl_milli);
  auto query_time_after_num_entries = static_cast<int64_t>(jquery_time_after_num_entries);
  auto config_holder = *(reinterpret_cast<std::shared_ptr<FlinkCompactionFilter::ConfigHolder>*>(handle));
  auto list_filter_factory = createListElementFilterFactory(env, ji_list_elem_len, jlist_filter_factory);
  auto config = new FlinkCompactionFilter::Config{state_type, timestamp_offset, ttl, query_time_after_num_entries,
                                                  std::unique_ptr<FlinkCompactionFilter::ListElementFilterFactory>(list_filter_factory)};
  return static_cast<jboolean>(config_holder->Configure(config));
}