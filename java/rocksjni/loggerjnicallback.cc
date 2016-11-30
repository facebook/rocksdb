// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Logger.

#include "include/org_rocksdb_Logger.h"

#include "rocksjni/loggerjnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {

LoggerJniCallback::LoggerJniCallback(
    JNIEnv* env, jobject jlogger) {
  const jint rs __attribute__((unused)) = env->GetJavaVM(&m_jvm);
  assert(rs == JNI_OK);

  // Note: we want to access the Java Logger instance
  // across multiple method calls, so we create a global ref
  m_jLogger = env->NewGlobalRef(jlogger);
  m_jLogMethodId = LoggerJni::getLogMethodId(env);

  jobject jdebug_level = InfoLogLevelJni::DEBUG_LEVEL(env);
  assert(jdebug_level != nullptr);
  m_jdebug_level = env->NewGlobalRef(jdebug_level);

  jobject jinfo_level = InfoLogLevelJni::INFO_LEVEL(env);
  assert(jinfo_level != nullptr);
  m_jinfo_level = env->NewGlobalRef(jinfo_level);

  jobject jwarn_level = InfoLogLevelJni::WARN_LEVEL(env);
  assert(jwarn_level != nullptr);
  m_jwarn_level = env->NewGlobalRef(jwarn_level);

  jobject jerror_level = InfoLogLevelJni::ERROR_LEVEL(env);
  assert(jerror_level != nullptr);
  m_jerror_level = env->NewGlobalRef(jerror_level);

  jobject jfatal_level = InfoLogLevelJni::FATAL_LEVEL(env);
  assert(jfatal_level != nullptr);
  m_jfatal_level = env->NewGlobalRef(jfatal_level);

  jobject jheader_level = InfoLogLevelJni::HEADER_LEVEL(env);
  assert(jheader_level != nullptr);
  m_jheader_level = env->NewGlobalRef(jheader_level);
}

/**
 * Get JNIEnv for current native thread
 */
JNIEnv* LoggerJniCallback::getJniEnv() const {
  JNIEnv *env;
  jint rs __attribute__((unused)) =
      m_jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);
  assert(rs == JNI_OK);
  return env;
}

void LoggerJniCallback::Logv(const char* format, va_list ap) {
  // We implement this method because it is virtual but we don't
  // use it because we need to know about the log level.
}

void LoggerJniCallback::Logv(const InfoLogLevel log_level,
    const char* format, va_list ap) {
  if (GetInfoLogLevel() <= log_level) {

    // determine InfoLogLevel java enum instance
    jobject jlog_level;
    switch (log_level) {
      case rocksdb::InfoLogLevel::DEBUG_LEVEL:
        jlog_level = m_jdebug_level;
        break;
      case rocksdb::InfoLogLevel::INFO_LEVEL:
        jlog_level = m_jinfo_level;
        break;
      case rocksdb::InfoLogLevel::WARN_LEVEL:
        jlog_level = m_jwarn_level;
        break;
      case rocksdb::InfoLogLevel::ERROR_LEVEL:
        jlog_level = m_jerror_level;
        break;
      case rocksdb::InfoLogLevel::FATAL_LEVEL:
        jlog_level = m_jfatal_level;
        break;
      case rocksdb::InfoLogLevel::HEADER_LEVEL:
        jlog_level = m_jheader_level;
        break;
      default:
        jlog_level = m_jfatal_level;
        break;
    }

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;
      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }
      // Truncate to available space if necessary
      if (p >= limit) {
        if (iter == 0) {
          continue;       // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }
      assert(p < limit);
      *p++ = '\0';

      JNIEnv* env = getJniEnv();

      // pass java string to callback handler
      env->CallVoidMethod(
          m_jLogger,
          m_jLogMethodId,
          jlog_level,
          env->NewStringUTF(base));

      if (base != buffer) {
        delete[] base;
      }
      break;
    }
    m_jvm->DetachCurrentThread();
  }
}

LoggerJniCallback::~LoggerJniCallback() {
  JNIEnv* env = getJniEnv();
  env->DeleteGlobalRef(m_jLogger);

  env->DeleteGlobalRef(m_jdebug_level);
  env->DeleteGlobalRef(m_jinfo_level);
  env->DeleteGlobalRef(m_jwarn_level);
  env->DeleteGlobalRef(m_jerror_level);
  env->DeleteGlobalRef(m_jfatal_level);
  env->DeleteGlobalRef(m_jheader_level);

  m_jvm->DetachCurrentThread();
}

}  // namespace rocksdb

/*
 * Class:     org_rocksdb_Logger
 * Method:    createNewLoggerOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Logger_createNewLoggerOptions(
    JNIEnv* env, jobject jobj, jlong joptions) {
  rocksdb::LoggerJniCallback* c =
      new rocksdb::LoggerJniCallback(env, jobj);
  // set log level
  c->SetInfoLogLevel(reinterpret_cast<rocksdb::Options*>
      (joptions)->info_log_level);
  std::shared_ptr<rocksdb::LoggerJniCallback> *pLoggerJniCallback =
      new std::shared_ptr<rocksdb::LoggerJniCallback>;
  *pLoggerJniCallback = std::shared_ptr<rocksdb::LoggerJniCallback>(c);
  return reinterpret_cast<jlong>(pLoggerJniCallback);
}

/*
 * Class:     org_rocksdb_Logger
 * Method:    createNewLoggerDbOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Logger_createNewLoggerDbOptions(
    JNIEnv* env, jobject jobj, jlong jdb_options) {
  rocksdb::LoggerJniCallback* c =
      new rocksdb::LoggerJniCallback(env, jobj);
  // set log level
  c->SetInfoLogLevel(reinterpret_cast<rocksdb::DBOptions*>
      (jdb_options)->info_log_level);
  std::shared_ptr<rocksdb::LoggerJniCallback> *pLoggerJniCallback =
      new std::shared_ptr<rocksdb::LoggerJniCallback>;
  *pLoggerJniCallback = std::shared_ptr<rocksdb::LoggerJniCallback>(c);
  return reinterpret_cast<jlong>(pLoggerJniCallback);
}

/*
 * Class:     org_rocksdb_Logger
 * Method:    setInfoLogLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_Logger_setInfoLogLevel(
    JNIEnv* env, jobject jobj, jlong jhandle, jbyte jlog_level) {
  std::shared_ptr<rocksdb::LoggerJniCallback> *handle =
      reinterpret_cast<std::shared_ptr<rocksdb::LoggerJniCallback> *>(jhandle);
  (*handle)->SetInfoLogLevel(static_cast<rocksdb::InfoLogLevel>(jlog_level));
}

/*
 * Class:     org_rocksdb_Logger
 * Method:    infoLogLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Logger_infoLogLevel(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  std::shared_ptr<rocksdb::LoggerJniCallback> *handle =
      reinterpret_cast<std::shared_ptr<rocksdb::LoggerJniCallback> *>(jhandle);
  return static_cast<jbyte>((*handle)->GetInfoLogLevel());
}

/*
 * Class:     org_rocksdb_Logger
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Logger_disposeInternal(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  std::shared_ptr<rocksdb::LoggerJniCallback> *handle =
      reinterpret_cast<std::shared_ptr<rocksdb::LoggerJniCallback> *>(jhandle);
  handle->reset();
}
