// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ ROCKSDB_NAMESPACE::ConfigOptions methods
// from Java side.

#include <jni.h>

#include "include/org_rocksdb_ConfigOptions.h"
#include "rocksdb/convenience.h"
#include "rocksjni/portal/sanity_level_jni.h"

/*
 * Class:     org_rocksdb_ConfigOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ConfigOptions_disposeInternalJni(JNIEnv *, jclass,
                                                       jlong jhandle) {
  auto *co = reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions *>(jhandle);
  assert(co != nullptr);
  delete co;
}

/*
 * Class:     org_rocksdb_ConfigOptions
 * Method:    newConfigOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_ConfigOptions_newConfigOptions(JNIEnv *, jclass) {
  auto *cfg_opt = new ROCKSDB_NAMESPACE::ConfigOptions();
  return GET_CPLUSPLUS_POINTER(cfg_opt);
}

/*
 * Class:     org_rocksdb_ConfigOptions
 * Method:    setEnv
 * Signature: (JJ;)V
 */
void Java_org_rocksdb_ConfigOptions_setEnv(JNIEnv *, jclass, jlong handle,
                                           jlong rocksdb_env_handle) {
  auto *cfg_opt = reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions *>(handle);
  auto *rocksdb_env =
      reinterpret_cast<ROCKSDB_NAMESPACE::Env *>(rocksdb_env_handle);
  cfg_opt->env = rocksdb_env;
}

/*
 * Class:     org_rocksdb_ConfigOptions
 * Method:    setDelimiter
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_ConfigOptions_setDelimiter(JNIEnv *env, jclass,
                                                 jlong handle, jstring s) {
  auto *cfg_opt = reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions *>(handle);
  const char *delim = env->GetStringUTFChars(s, nullptr);
  if (delim == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  cfg_opt->delimiter = delim;
  env->ReleaseStringUTFChars(s, delim);
}

/*
 * Class:     org_rocksdb_ConfigOptions
 * Method:    setIgnoreUnknownOptions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ConfigOptions_setIgnoreUnknownOptions(JNIEnv *, jclass,
                                                            jlong handle,
                                                            jboolean b) {
  auto *cfg_opt = reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions *>(handle);
  cfg_opt->ignore_unknown_options = static_cast<bool>(b);
}

/*
 * Class:     org_rocksdb_ConfigOptions
 * Method:    setInputStringsEscaped
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ConfigOptions_setInputStringsEscaped(JNIEnv *, jclass,
                                                           jlong handle,
                                                           jboolean b) {
  auto *cfg_opt = reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions *>(handle);
  cfg_opt->input_strings_escaped = static_cast<bool>(b);
}

/*
 * Class:     org_rocksdb_ConfigOptions
 * Method:    setSanityLevel
 * Signature: (JI)V
 */
void Java_org_rocksdb_ConfigOptions_setSanityLevel(JNIEnv *, jclass,
                                                   jlong handle, jbyte level) {
  auto *cfg_opt = reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions *>(handle);
  cfg_opt->sanity_level =
      ROCKSDB_NAMESPACE::SanityLevelJni::toCppSanityLevel(level);
}
