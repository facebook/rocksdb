// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ for rocksdb::Options.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>

#include "include/org_rocksdb_Options.h"
#include "include/org_rocksdb_WriteOptions.h"
#include "rocksjni/portal.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

/*
 * Class:     org_rocksdb_Options
 * Method:    newOptions
 * Signature: ()V
 */
void Java_org_rocksdb_Options_newOptions(JNIEnv* env, jobject jobj) {
  rocksdb::Options* op = new rocksdb::Options();
  rocksdb::OptionsJni::setHandle(env, jobj, op);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dispose0
 * Signature: ()V
 */
void Java_org_rocksdb_Options_dispose0(JNIEnv* env, jobject jobj) {
  rocksdb::Options* op = rocksdb::OptionsJni::getHandle(env, jobj);
  delete op;

  rocksdb::OptionsJni::setHandle(env, jobj, op);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCreateIfMissing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setCreateIfMissing(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean flag) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->create_if_missing = flag;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    createIfMissing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_createIfMissing(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->create_if_missing;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteBufferSize
 * Signature: (JJ)I
 */
void Java_org_rocksdb_Options_setWriteBufferSize(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong jwrite_buffer_size) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->write_buffer_size =
          static_cast<size_t>(jwrite_buffer_size);
}


/*
 * Class:     org_rocksdb_Options
 * Method:    writeBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writeBufferSize(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->write_buffer_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxWriteBufferNumber
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxWriteBufferNumber(
    JNIEnv* env, jobject jobj, jlong jhandle, jint jmax_write_buffer_number) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_write_buffer_number =
          jmax_write_buffer_number;
}


/*
 * Class:     org_rocksdb_Options
 * Method:    maxWriteBufferNumber
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxWriteBufferNumber(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_write_buffer_number;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBlockSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setBlockSize(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong jblock_size) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->block_size =
          static_cast<size_t>(jblock_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    blockSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_blockSize(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->block_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDisableSeekCompaction
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setDisableSeekCompaction(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jboolean jdisable_seek_compaction) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->disable_seek_compaction =
         jdisable_seek_compaction;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    disableSeekCompaction
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_disableSeekCompaction(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->disable_seek_compaction;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBackgroundCompactions(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jmax_background_compactions) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_background_compactions =
         jmax_background_compactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBackgroundCompactions(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return
    reinterpret_cast<rocksdb::Options*>(jhandle)->max_background_compactions;
}


//////////////////////////////////////////////////////////////////////////////
// WriteOptions

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    newWriteOptions
 * Signature: ()V
 */
void Java_org_rocksdb_WriteOptions_newWriteOptions(
    JNIEnv* env, jobject jwrite_options) {
  rocksdb::WriteOptions* op = new rocksdb::WriteOptions();
  rocksdb::WriteOptionsJni::setHandle(env, jwrite_options, op);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    dispose0
 * Signature: ()V
 */
void Java_org_rocksdb_WriteOptions_dispose0(
    JNIEnv* env, jobject jwrite_options, jlong jhandle) {
  auto write_options = reinterpret_cast<rocksdb::WriteOptions*>(jhandle);
  delete write_options;

  rocksdb::WriteOptionsJni::setHandle(env, jwrite_options, nullptr);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setSync(
  JNIEnv* env, jobject jwrite_options, jlong jhandle, jboolean jflag) {
  reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->sync = jflag;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    sync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_sync(
    JNIEnv* env, jobject jwrite_options, jlong jhandle) {
  return reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->sync;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setDisableWAL
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setDisableWAL(
    JNIEnv* env, jobject jwrite_options, jlong jhandle, jboolean jflag) {
  reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->disableWAL = jflag;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    disableWAL
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_disableWAL(
    JNIEnv* env, jobject jwrite_options, jlong jhandle) {
  return reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->disableWAL;
}


