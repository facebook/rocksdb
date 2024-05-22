// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::Iterator methods from Java side.

#include <jni.h>
#include <stdlib.h>

#include "include/org_rocksdb_ParsedEntryInfo.h"
#include "rocksdb/types.h"
#include "rocksdb/options.h"
#include "rocksjni/portal.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksdb/utilities/types_util.h"



/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    newParseEntryInstance
 * Signature: ()J
 */
jlong JNICALL Java_org_rocksdb_ParsedEntryInfo_newParseEntryInstance
    (JNIEnv *env, jclass /*cls*/) {
  ROCKSDB_NAMESPACE::ParsedEntryInfo *parsed_entry_info =
      new ROCKSDB_NAMESPACE::ParsedEntryInfo();
  return GET_CPLUSPLUS_POINTER(parsed_entry_info);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    parseEntry
 * Signature: (JJ[BI)V
 */
void JNICALL Java_org_rocksdb_ParsedEntryInfo_parseEntry(JNIEnv *env,
                                                         jclass cls,
                                                         jlong handle,
                                                         jlong options_handle,
                                                         jbyteArray jtarget,
                                                         jint len) {
  auto *options =
      reinterpret_cast<const ROCKSDB_NAMESPACE::Options*>(options_handle);
  auto *parsed_entry_info = reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo*>(handle);
  jbyte* target = env->GetByteArrayElements(jtarget, nullptr);
  if (target == nullptr) {
  // exception thrown: OutOfMemoryError
    return;
  }
  ROCKSDB_NAMESPACE::Slice target_slice(reinterpret_cast<char*>(target), len);
  ROCKSDB_NAMESPACE::ParseEntry(target_slice, options->comparator,
                                parsed_entry_info);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    parseEntryDirect
 * Signature: (JJLjava/nio/ByteBuffer;II)V
 */
void JNICALL Java_org_rocksdb_ParsedEntryInfo_parseEntryDirect(JNIEnv *env,
                                                               jclass clz,
                                                               jlong handle,
                                                               jlong options_handle,
                                                               jobject jbuffer,
                                                               jint jbuffer_off,
                                                               jint jbuffer_len) {
  auto *options =
      reinterpret_cast<const ROCKSDB_NAMESPACE::Options*>(options_handle);
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo*>(handle);
  auto parse = [&parsed_entry_info, &options](
                   ROCKSDB_NAMESPACE::Slice& target_slice) {
    ROCKSDB_NAMESPACE::ParseEntry(target_slice, options->comparator,
                                  parsed_entry_info);
  };
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(parse, env, jbuffer, jbuffer_off,
                                          jbuffer_len);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    parseEntryByteArray
 * Signature: (JJ[BII)V
 */
void JNICALL Java_org_rocksdb_ParsedEntryInfo_parseEntryByteArray(JNIEnv *env,
                                                                  jclass clz,
                                                                  jlong handle,
                                                                  jlong options_handle,
                                                                  jbyteArray jtarget,
                                                                  jint off,
                                                                  jint len) {
  const std::unique_ptr<char[]> target(new char[len]);
  if (target == nullptr) {
    jclass oom_class = env->FindClass("java/lang/OutOfMemoryError");
    env->ThrowNew(oom_class,
                  "Memory allocation failed in RocksDB JNI function");
    return;
  }
  env->GetByteArrayRegion(jtarget, off, len,
                          reinterpret_cast<jbyte*>(target.get()));
  ROCKSDB_NAMESPACE::Slice target_slice(target.get(), len);
  auto *options =
      reinterpret_cast<const ROCKSDB_NAMESPACE::Options*>(options_handle);
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo*>(handle);
  ROCKSDB_NAMESPACE::ParseEntry(target_slice, options->comparator,
                                parsed_entry_info);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    userKeyDirect
 * Signature: (JLjava/nio/ByteBuffer;II)I
 */
jint JNICALL Java_org_rocksdb_ParsedEntryInfo_userKeyDirect(JNIEnv *env,
                                                            jclass clz,
                                                            jlong handle,
                                                            jobject jtarget,
                                                            jint joffset,
                                                            jint jlen) {
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo*>(handle);
  ROCKSDB_NAMESPACE::Slice key_slice = parsed_entry_info->user_key;
  return ROCKSDB_NAMESPACE::JniUtil::copyToDirect(env, key_slice, jtarget,
                                                  joffset, jlen);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    userKeyByteArray
 * Signature: (J[BII)I
 */
jint JNICALL Java_org_rocksdb_ParsedEntryInfo_userKeyByteArray(JNIEnv *env,
                                                               jclass clz,
                                                               jlong handle,
                                                               jbyteArray jtarget,
                                                               jint joffset,
                                                               jint jlen) {
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo*>(handle);
  ROCKSDB_NAMESPACE::Slice key_slice = parsed_entry_info->user_key;
  auto slice_size = key_slice.size();
  jsize copy_size = std::min(static_cast<uint32_t>(slice_size),
                             static_cast<uint32_t>(jlen));
  env->SetByteArrayRegion(
      jtarget, joffset, copy_size,
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(key_slice.data())));
  return static_cast<jsize>(slice_size);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    userKeyJni
 * Signature: (J)[B
 */
jbyteArray JNICALL Java_org_rocksdb_ParsedEntryInfo_userKeyJni(JNIEnv *env,
                                                               jclass clz,
                                                               jlong handle) {
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo*>(handle);
  ROCKSDB_NAMESPACE::Slice key_slice = parsed_entry_info->user_key;
  jbyteArray jkey = env->NewByteArray(static_cast<jsize>(key_slice.size()));
  if (jkey == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  env->SetByteArrayRegion(
      jkey, 0, static_cast<jsize>(key_slice.size()),
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(key_slice.data())));
  return jkey;
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    getSequenceNumberJni
 * Signature: (J)J
 */
jlong JNICALL Java_org_rocksdb_ParsedEntryInfo_getSequenceNumberJni(JNIEnv *env,
                                                                    jclass clz,
                                                                    jlong handle) {
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo*>(handle);
  uint64_t sequence_number = parsed_entry_info->sequence;
  return static_cast<jlong>(sequence_number);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    getValueTypeJni
 * Signature: (J)B
 */
jbyte JNICALL Java_org_rocksdb_ParsedEntryInfo_getEntryTypeJni(JNIEnv *env,
                                                               jclass clz,
                                                               jlong handle) {
  auto *parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo*>(handle);
  ROCKSDB_NAMESPACE::EntryType type = parsed_entry_info->type;
  return ROCKSDB_NAMESPACE::EntryTypeJni::toJavaEntryType(type);
}

/*
 * Class:     org_rocksdb_ParsedEntryInfo
 * Method:    disposeInternalJni
 * Signature: (J)V
 */
void JNICALL Java_org_rocksdb_ParsedEntryInfo_disposeInternalJni(JNIEnv* env,
                                                                 jclass clz,
                                                                 jlong handle) {
  auto* parsed_entry_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::ParsedEntryInfo*>(handle);
  assert(parsed_entry_info != nullptr);
  delete parsed_entry_info;
}

