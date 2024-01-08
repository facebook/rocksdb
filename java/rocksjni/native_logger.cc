// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksjni/native_logger.h"

#include <jni.h>

#include <memory>

#include "include/org_rocksdb_NativeLogger.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"
#include "test_util/testutil.h"
#include "util/stderr_logger.h"

/*
 * Class:     org_rocksdb_NativeLogger
 * Method:    newNativeStderrLogger
 * Signature: (BLjava/lang/String;)J
 */
jlong Java_org_rocksdb_NativeLogger_newNativeStderrLogger(JNIEnv* env,
                                                          jclass /*jcls*/,
                                                          jbyte jlog_level,
                                                          jstring jlog_prefix) {
  jboolean has_exception = JNI_FALSE;
  auto log_prefix = ROCKSDB_NAMESPACE::JniUtil::copyString(
      env, jlog_prefix, &has_exception);  // also releases jlog_prefix
  if (has_exception == JNI_TRUE) {
    return 0;
  }

  std::string str_log_prefix(log_prefix.get());
  auto log_level = static_cast<ROCKSDB_NAMESPACE::InfoLogLevel>(jlog_level);
  auto* stderr_logger =
      new ROCKSDB_NAMESPACE::StderrLogger(log_level, str_log_prefix);

  auto* native_logger_wrapper = new ROCKSDB_NAMESPACE::NativeLogger(
      std::shared_ptr<ROCKSDB_NAMESPACE::Logger>(stderr_logger));
  return GET_CPLUSPLUS_POINTER(native_logger_wrapper);
}

/*
 * Class:     org_rocksdb_NativeLogger
 * Method:    newNativeDevnullLogger
 * Signature: ()J
 */
jlong Java_org_rocksdb_NativeLogger_newNativeDevnullLogger(JNIEnv*,
                                                           jclass /*jcls*/) {
  auto* devnull_logger = new ROCKSDB_NAMESPACE::test::NullLogger();
  auto* native_logger_wrapper = new ROCKSDB_NAMESPACE::NativeLogger(
      std::shared_ptr<ROCKSDB_NAMESPACE::Logger>(devnull_logger));
  return GET_CPLUSPLUS_POINTER(native_logger_wrapper);
}

/*
 * Class:     org_rocksdb_NativeLogger
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_NativeLogger_disposeInternal(JNIEnv* /*env*/,
                                                   jobject /*jopt*/,
                                                   jlong jhandle) {
  auto* native_logger_wrapper =
      reinterpret_cast<ROCKSDB_NAMESPACE::NativeLogger*>(jhandle);
  assert(native_logger_wrapper != nullptr);
  delete native_logger_wrapper;
}
