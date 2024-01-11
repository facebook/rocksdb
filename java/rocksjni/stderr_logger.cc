// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/stderr_logger.h"

#include <jni.h>

#include <memory>

#include "include/org_rocksdb_util_StdErrLogger.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_util_StdErrLogger
 * Method:    newStdErrLogger
 * Signature: (BLjava/lang/String;)J
 */
jlong Java_org_rocksdb_util_StdErrLogger_newStdErrLogger(JNIEnv* env,
                                                         jclass /*jcls*/,
                                                         jbyte jlog_level,
                                                         jstring jlog_prefix) {
  auto log_level = static_cast<ROCKSDB_NAMESPACE::InfoLogLevel>(jlog_level);
  std::shared_ptr<ROCKSDB_NAMESPACE::StderrLogger>* sptr_logger = nullptr;
  if (jlog_prefix == nullptr) {
    sptr_logger = new std::shared_ptr<ROCKSDB_NAMESPACE::StderrLogger>(
        new ROCKSDB_NAMESPACE::StderrLogger(log_level));
  } else {
    jboolean has_exception = JNI_FALSE;
    auto log_prefix = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
        env, jlog_prefix, &has_exception);  // also releases jlog_prefix
    if (has_exception == JNI_TRUE) {
      return 0;
    }
    sptr_logger = new std::shared_ptr<ROCKSDB_NAMESPACE::StderrLogger>(
        new ROCKSDB_NAMESPACE::StderrLogger(log_level, log_prefix));
  }
  return GET_CPLUSPLUS_POINTER(sptr_logger);
}

/*
 * Class:     org_rocksdb_util_StdErrLogger
 * Method:    setInfoLogLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_util_StdErrLogger_setInfoLogLevel(JNIEnv* /*env*/,
                                                        jclass /*jcls*/,
                                                        jlong jhandle,
                                                        jbyte jlog_level) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::StderrLogger>*>(
          jhandle);
  handle->get()->SetInfoLogLevel(
      static_cast<ROCKSDB_NAMESPACE::InfoLogLevel>(jlog_level));
}

/*
 * Class:     org_rocksdb_util_StdErrLogger
 * Method:    infoLogLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_util_StdErrLogger_infoLogLevel(JNIEnv* /*env*/,
                                                      jclass /*jcls*/,
                                                      jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::StderrLogger>*>(
          jhandle);
  return static_cast<jbyte>(handle->get()->GetInfoLogLevel());
}

/*
 * Class:     org_rocksdb_util_StdErrLogger
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_util_StdErrLogger_disposeInternal(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::StderrLogger>*>(
          jhandle);
  delete handle;  // delete std::shared_ptr
}
