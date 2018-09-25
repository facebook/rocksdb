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

//
// Simple logger that prints message on stdout
//
class ConsoleLogger : public rocksdb::Logger {
public:
    using Logger::Logv;
    explicit ConsoleLogger(rocksdb::InfoLogLevel logLevel) : Logger(logLevel) {}

    void Logv(const char* format, va_list ap) override {
        vprintf(format, ap);
        printf("\n");
    }
};

/*
 * Class:     org_rocksdb_FlinkCompactionFilter
 * Method:    createNewFlinkCompactionFilter0
 * Signature: (B)J
 */
jlong Java_org_rocksdb_FlinkCompactionFilter_createNewFlinkCompactionFilter0(
        JNIEnv* /* env */, jclass /* jcls */, jbyte jlog_level) {
  using namespace rocksdb::flink;
  // set the native handle to our native compaction filter
  auto* logger= new ConsoleLogger(static_cast<rocksdb::InfoLogLevel>(jlog_level));
  return reinterpret_cast<jlong>(new FlinkCompactionFilter(logger));
}

/*
 * Class:     org_rocksdb_FlinkCompactionFilter
 * Method:    configureFlinkCompactionFilter
 * Signature: (JIIJZJ)J
 */
jlong Java_org_rocksdb_FlinkCompactionFilter_configureFlinkCompactionFilter(
        JNIEnv* /* env */, jclass /* jcls */,
        jlong handle, jint ji_state_type, jint ji_timestamp_offset,
        jlong jl_ttl_milli, jboolean jb_use_system_time) {
    using namespace rocksdb::flink;
    auto state_type = static_cast<FlinkCompactionFilter::StateType>(ji_state_type);
    auto timestamp_offset = static_cast<size_t>(ji_timestamp_offset);
    auto ttl = static_cast<int64_t>(jl_ttl_milli);
    auto use_system_time = (bool)(jb_use_system_time == JNI_TRUE);
    auto filter = reinterpret_cast<FlinkCompactionFilter*>(handle);
    filter->Configure(new FlinkCompactionFilter::Config{state_type, timestamp_offset, ttl, use_system_time});
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