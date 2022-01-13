// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ ROCKSDB_NAMESPACE::OptionsUtil methods from Java side.

#include <jni.h>
#include <string>

#include "include/org_rocksdb_OptionsUtil.h"

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksjni/portal.h"

void build_column_family_descriptor_list(
    JNIEnv* env, jobject jcfds,
    std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor>& cf_descs) {
  jmethodID add_mid = ROCKSDB_NAMESPACE::ListJni::getListAddMethodId(env);
  if (add_mid == nullptr) {
    // exception occurred accessing method
    return;
  }

  // Column family descriptor
  for (ROCKSDB_NAMESPACE::ColumnFamilyDescriptor& cfd : cf_descs) {
    // Construct a ColumnFamilyDescriptor java object
    jobject jcfd =
        ROCKSDB_NAMESPACE::ColumnFamilyDescriptorJni::construct(env, &cfd);
    if (env->ExceptionCheck()) {
      // exception occurred constructing object
      if (jcfd != nullptr) {
        env->DeleteLocalRef(jcfd);
      }
      return;
    }

    // Add the object to java list.
    jboolean rs = env->CallBooleanMethod(jcfds, add_mid, jcfd);
    if (env->ExceptionCheck() || rs == JNI_FALSE) {
      // exception occurred calling method, or could not add
      if (jcfd != nullptr) {
        env->DeleteLocalRef(jcfd);
      }
      return;
    }
  }
}

/*
 * Class:     org_rocksdb_OptionsUtil
 * Method:    loadLatestOptions
 * Signature: (Ljava/lang/String;JLjava/util/List;Z)V
 */
void Java_org_rocksdb_OptionsUtil_loadLatestOptions__Ljava_lang_String_2JJLjava_util_List_2Z(
    JNIEnv* env, jclass /*jcls*/, jstring jdbpath, jlong jenv_handle,
    jlong jdb_opts_handle, jobject jcfds, jboolean ignore_unknown_options) {
  jboolean has_exception = JNI_FALSE;
  auto db_path =
      ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, jdbpath, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor> cf_descs;
  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::LoadLatestOptions(
      db_path, reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jenv_handle),
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jdb_opts_handle),
      &cf_descs, ignore_unknown_options);
  if (!s.ok()) {
    // error, raise an exception
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  } else {
    build_column_family_descriptor_list(env, jcfds, cf_descs);
  }
}

/*
 * Class:     org_rocksdb_OptionsUtil
 * Method:    loadLatestOptions_1
 * Signature: (JLjava/lang/String;JLjava/util/List;)V
 */
void Java_org_rocksdb_OptionsUtil_loadLatestOptions__JLjava_lang_String_2JLjava_util_List_2(
    JNIEnv* env, jclass /*jcls*/, jlong cfg_handle, jstring jdbpath,
    jlong jdb_opts_handle, jobject jcfds) {
  jboolean has_exception = JNI_FALSE;
  auto db_path =
      ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, jdbpath, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor> cf_descs;
  auto* config_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions*>(cfg_handle);
  auto* db_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jdb_opts_handle);
  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::LoadLatestOptions(
      *config_options, db_path, db_options, &cf_descs);
  if (!s.ok()) {
    // error, raise an exception
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  } else {
    build_column_family_descriptor_list(env, jcfds, cf_descs);
  }
}

/*
 * Class:     org_rocksdb_OptionsUtil
 * Method:    loadOptionsFromFile
 * Signature: (Ljava/lang/String;JJLjava/util/List;Z)V
 */
void Java_org_rocksdb_OptionsUtil_loadOptionsFromFile__Ljava_lang_String_2JJLjava_util_List_2Z(
    JNIEnv* env, jclass /*jcls*/, jstring jopts_file_name, jlong jenv_handle,
    jlong jdb_opts_handle, jobject jcfds, jboolean ignore_unknown_options) {
  jboolean has_exception = JNI_FALSE;
  auto opts_file_name = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
      env, jopts_file_name, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor> cf_descs;
  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::LoadOptionsFromFile(
      opts_file_name, reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jenv_handle),
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jdb_opts_handle),
      &cf_descs, ignore_unknown_options);
  if (!s.ok()) {
    // error, raise an exception
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  } else {
    build_column_family_descriptor_list(env, jcfds, cf_descs);
  }
}

/*
 * Class:     org_rocksdb_OptionsUtil
 * Method:    loadOptionsFromFile
 * Signature: (JLjava/lang/String;JLjava/util/List;)V
 */
void Java_org_rocksdb_OptionsUtil_loadOptionsFromFile__JLjava_lang_String_2JLjava_util_List_2(
    JNIEnv* env, jclass /*jcls*/, jlong cfg_handle, jstring jopts_file_name,
    jlong jdb_opts_handle, jobject jcfds) {
  jboolean has_exception = JNI_FALSE;
  auto opts_file_name = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
      env, jopts_file_name, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor> cf_descs;
  auto* config_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions*>(cfg_handle);
  auto* db_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jdb_opts_handle);
  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::LoadOptionsFromFile(
      *config_options, opts_file_name, db_options, &cf_descs);
  if (!s.ok()) {
    // error, raise an exception
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  } else {
    build_column_family_descriptor_list(env, jcfds, cf_descs);
  }
}

/*
 * Class:     org_rocksdb_OptionsUtil
 * Method:    getLatestOptionsFileName
 * Signature: (Ljava/lang/String;J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_OptionsUtil_getLatestOptionsFileName(
    JNIEnv* env, jclass /*jcls*/, jstring jdbpath, jlong jenv_handle) {
  jboolean has_exception = JNI_FALSE;
  auto db_path =
      ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, jdbpath, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return nullptr;
  }
  std::string options_file_name;
  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::GetLatestOptionsFileName(
      db_path, reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jenv_handle),
      &options_file_name);
  if (!s.ok()) {
    // error, raise an exception
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  } else {
    return env->NewStringUTF(options_file_name.c_str());
  }
}
