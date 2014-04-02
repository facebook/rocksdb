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
#include "rocksjni/portal.h"
#include "rocksdb/db.h"

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
