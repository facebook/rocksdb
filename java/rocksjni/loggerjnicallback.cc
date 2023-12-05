// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::Logger.

#include "rocksjni/loggerjnicallback.h"

#include <cstdarg>
#include <cstdio>

#include "include/org_rocksdb_Logger.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

LoggerJniCallback::LoggerJniCallback(JNIEnv* env, jobject jlogger)
    : JniCallback(env, jlogger) {
  m_jLogMethodId = LoggerJni::getLogMethodId(env);
  if (m_jLogMethodId == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  jobject jdebug_level = InfoLogLevelJni::DEBUG_LEVEL(env);
  if (jdebug_level == nullptr) {
    // exception thrown: NoSuchFieldError, ExceptionInInitializerError
    // or OutOfMemoryError
    return;
  }
  m_jdebug_level = env->NewGlobalRef(jdebug_level);
  if (m_jdebug_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jobject jinfo_level = InfoLogLevelJni::INFO_LEVEL(env);
  if (jinfo_level == nullptr) {
    // exception thrown: NoSuchFieldError, ExceptionInInitializerError
    // or OutOfMemoryError
    return;
  }
  m_jinfo_level = env->NewGlobalRef(jinfo_level);
  if (m_jinfo_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jobject jwarn_level = InfoLogLevelJni::WARN_LEVEL(env);
  if (jwarn_level == nullptr) {
    // exception thrown: NoSuchFieldError, ExceptionInInitializerError
    // or OutOfMemoryError
    return;
  }
  m_jwarn_level = env->NewGlobalRef(jwarn_level);
  if (m_jwarn_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jobject jerror_level = InfoLogLevelJni::ERROR_LEVEL(env);
  if (jerror_level == nullptr) {
    // exception thrown: NoSuchFieldError, ExceptionInInitializerError
    // or OutOfMemoryError
    return;
  }
  m_jerror_level = env->NewGlobalRef(jerror_level);
  if (m_jerror_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jobject jfatal_level = InfoLogLevelJni::FATAL_LEVEL(env);
  if (jfatal_level == nullptr) {
    // exception thrown: NoSuchFieldError, ExceptionInInitializerError
    // or OutOfMemoryError
    return;
  }
  m_jfatal_level = env->NewGlobalRef(jfatal_level);
  if (m_jfatal_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jobject jheader_level = InfoLogLevelJni::HEADER_LEVEL(env);
  if (jheader_level == nullptr) {
    // exception thrown: NoSuchFieldError, ExceptionInInitializerError
    // or OutOfMemoryError
    return;
  }
  m_jheader_level = env->NewGlobalRef(jheader_level);
  if (m_jheader_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  MutexLock lk(&logging_thread_mtx);
  // This checks whether the thread has been initialized. We don't want to do so
  // twice. So, if it is is joinable, it hasn't been initialized.
  if (!logging_thread.joinable()) {
    logging_thread =
        port::Thread([this] { LoggerJniCallback::log_thread_loop(); });
    logging_thread.detach();
  }
}

// [NEIL] Logging thread stuff
port::Mutex LoggerJniCallback::logging_thread_mtx;
port::Thread LoggerJniCallback::logging_thread;
bool LoggerJniCallback::is_logging_thread_active = true;

port::Mutex LoggerJniCallback::message_mtx;
port::CondVar LoggerJniCallback::message_cond(&LoggerJniCallback::message_mtx);
std::unique_ptr<char[]> LoggerJniCallback::message;

void LoggerJniCallback::log_thread_loop() {
  // Attach to the JVM.
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  // In this function, the only point at which this mutex becomes unlocked
  // is when the condition variable releases it. But within the code of
  // this function, it's always locked.
  MutexLock lk(&message_mtx);
  std::cout << "log_thread_loop is running\n";

  while (is_logging_thread_active) {
    std::cout << "log_thread_loop top of loop\n";

    // Wait until there is a message to log (or we become deactivated)
    while (message == nullptr && is_logging_thread_active) {
      message_cond.Wait();
    }
    std::cout << "Finishing waiting\n";

    if (!is_logging_thread_active) {
      std::cout << "Exiting logging thread loop\n";
      break;
    }

    // There needs to be a message at this point
    assert(message);
    jstring jmsg = env->NewStringUTF(message.get());

    if (jmsg == nullptr) {
      // unable to construct string
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();  // print out exception to stderr
      }
      releaseJniEnv(attached_thread);
      return;
    }
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      env->ExceptionDescribe();  // print out exception to stderr
      env->DeleteLocalRef(jmsg);
      releaseJniEnv(attached_thread);
      return;
    }

    // TODO(neil): This needs to be changed to the original log level, not
    // always m_jdebug_level.
    env->CallVoidMethod(m_jcallback_obj, m_jLogMethodId, m_jdebug_level, jmsg);

    if (env->ExceptionCheck()) {
      // exception thrown
      env->ExceptionDescribe();  // print out exception to stderr
      env->DeleteLocalRef(jmsg);
      releaseJniEnv(attached_thread);
      return;
    }

    // Clean up JVM garbage
    env->DeleteLocalRef(jmsg);

    // Reset static state
    message = nullptr;
    message_cond.SignalAll();
  }

  std::cout << "log_thread_loop about to detach from JVM\n";

  releaseJniEnv(attached_thread);
}

void LoggerJniCallback::Logv(const char* /*format*/, va_list /*ap*/) {
  // We implement this method because it is virtual but we don't
  // use it because we need to know about the log level.
}

void LoggerJniCallback::Logv(const InfoLogLevel log_level, const char* format,
                             va_list ap) {
  if (GetInfoLogLevel() <= log_level) {
    // determine InfoLogLevel java enum instance
    // jobject jlog_level;
    // switch (log_level) {
    //   case ROCKSDB_NAMESPACE::InfoLogLevel::DEBUG_LEVEL:
    //     jlog_level = m_jdebug_level;
    //     break;
    //   case ROCKSDB_NAMESPACE::InfoLogLevel::INFO_LEVEL:
    //     jlog_level = m_jinfo_level;
    //     break;
    //   case ROCKSDB_NAMESPACE::InfoLogLevel::WARN_LEVEL:
    //     jlog_level = m_jwarn_level;
    //     break;
    //   case ROCKSDB_NAMESPACE::InfoLogLevel::ERROR_LEVEL:
    //     jlog_level = m_jerror_level;
    //     break;
    //   case ROCKSDB_NAMESPACE::InfoLogLevel::FATAL_LEVEL:
    //     jlog_level = m_jfatal_level;
    //     break;
    //   case ROCKSDB_NAMESPACE::InfoLogLevel::HEADER_LEVEL:
    //     jlog_level = m_jheader_level;
    //     break;
    //   default:
    //     jlog_level = m_jfatal_level;
    //     break;
    // }

    // Create the formatted string before creating lock contention
    assert(format != nullptr);
    std::unique_ptr<char[]> msg = format_str(format, ap);

    MutexLock lk(&message_mtx);

    // Loop until the predicate is satisifed, i.e. nothing is
    // currently being logged
    while (message != nullptr) {
      message_cond.Wait();
    }

    assert(message == nullptr);
    message = std::move(msg);
    message_cond.SignalAll();
  }
}

std::unique_ptr<char[]> LoggerJniCallback::format_str(const char* format,
                                                      va_list ap) const {
  va_list ap_copy;

  va_copy(ap_copy, ap);
  const size_t required =
      vsnprintf(nullptr, 0, format, ap_copy) + 1;  // Extra space for '\0'
  va_end(ap_copy);

  std::unique_ptr<char[]> buf(new char[required]);

  va_copy(ap_copy, ap);
  vsnprintf(buf.get(), required, format, ap_copy);
  va_end(ap_copy);

  return buf;
}
LoggerJniCallback::~LoggerJniCallback() {
  std::cout << "[NEIL] logger callback exiting\n";

  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  if (m_jdebug_level != nullptr) {
    env->DeleteGlobalRef(m_jdebug_level);
  }

  if (m_jinfo_level != nullptr) {
    env->DeleteGlobalRef(m_jinfo_level);
  }

  if (m_jwarn_level != nullptr) {
    env->DeleteGlobalRef(m_jwarn_level);
  }

  if (m_jerror_level != nullptr) {
    env->DeleteGlobalRef(m_jerror_level);
  }

  if (m_jfatal_level != nullptr) {
    env->DeleteGlobalRef(m_jfatal_level);
  }

  if (m_jheader_level != nullptr) {
    env->DeleteGlobalRef(m_jheader_level);
  }

  std::cout << "destructor is running\n";

  releaseJniEnv(attached_thread);

  MutexLock lk(&logging_thread_mtx);
  is_logging_thread_active = false;

  std::cout << "Set logging thread active false. Joining...\n";
}

}  // namespace ROCKSDB_NAMESPACE

/*
 * Class:     org_rocksdb_Logger
 * Method:    createNewLoggerOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Logger_createNewLoggerOptions(JNIEnv* env, jobject jobj,
                                                     jlong joptions) {
  auto* sptr_logger = new std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>(
      new ROCKSDB_NAMESPACE::LoggerJniCallback(env, jobj));

  // set log level
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(joptions);
  sptr_logger->get()->SetInfoLogLevel(options->info_log_level);

  return GET_CPLUSPLUS_POINTER(sptr_logger);
}

/*
 * Class:     org_rocksdb_Logger
 * Method:    createNewLoggerDbOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Logger_createNewLoggerDbOptions(JNIEnv* env,
                                                       jobject jobj,
                                                       jlong jdb_options) {
  auto* sptr_logger = new std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>(
      new ROCKSDB_NAMESPACE::LoggerJniCallback(env, jobj));

  // set log level
  auto* db_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jdb_options);
  sptr_logger->get()->SetInfoLogLevel(db_options->info_log_level);

  return GET_CPLUSPLUS_POINTER(sptr_logger);
}

/*
 * Class:     org_rocksdb_Logger
 * Method:    setInfoLogLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_Logger_setInfoLogLevel(JNIEnv* /*env*/, jobject /*jobj*/,
                                             jlong jhandle, jbyte jlog_level) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>*>(
          jhandle);
  handle->get()->SetInfoLogLevel(
      static_cast<ROCKSDB_NAMESPACE::InfoLogLevel>(jlog_level));
}

/*
 * Class:     org_rocksdb_Logger
 * Method:    infoLogLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Logger_infoLogLevel(JNIEnv* /*env*/, jobject /*jobj*/,
                                           jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>*>(
          jhandle);
  return static_cast<jbyte>(handle->get()->GetInfoLogLevel());
}

/*
 * Class:     org_rocksdb_Logger
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Logger_disposeInternal(JNIEnv* /*env*/, jobject /*jobj*/,
                                             jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>*>(
          jhandle);
  delete handle;  // delete std::shared_ptr
}
