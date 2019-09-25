// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::DbPathSupplierFactory.

#include <jni.h>

#include "db/db_path_supplier.h"

/************************************************************
 *                                                          *
 * GradualMoveOldDataDbPathSupplierFactory native functions *
 *                                                          *
 ************************************************************/

/*
 * Class:     org_rocksdb_GradualMoveOldDataDbPathSupplierFactory
 * Method:    newFactoryObject
 * Signature: ()J
 */
jlong Java_org_rocksdb_GradualMoveOldDataDbPathSupplierFactory_newFactoryObject(
    JNIEnv*, jclass) {
  auto* factory = rocksdb::NewGradualMoveOldDataDbPathSupplierFactory();
  return reinterpret_cast<jlong>(factory);
}

/*
 * Class:     org_rocksdb_GradualMoveOldDataDbPathSupplierFactory
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_GradualMoveOldDataDbPathSupplierFactory_disposeInternal(
    JNIEnv *, jobject, jlong jhandle) {
  auto* factory =
      reinterpret_cast<rocksdb::GradualMoveOldDataDbPathSupplierFactory*>(jhandle);
  delete factory;
}

/************************************************************
 *                                                          *
 * RandomDbPathSupplierFactory native functions             *
 *                                                          *
 ************************************************************/

/*
 * Class:     org_rocksdb_RandomDbPathSupplierFactory
 * Method:    newFactoryObject
 * Signature: ()J
 */
jlong Java_org_rocksdb_RandomDbPathSupplierFactory_newFactoryObject(
    JNIEnv*, jclass) {
  auto* factory = rocksdb::NewRandomDbPathSupplierFactory();
  return reinterpret_cast<jlong>(factory);
}

/*
 * Class:     org_rocksdb_RandomDbPathSupplierFactory
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RandomDbPathSupplierFactory_disposeInternal(
    JNIEnv *, jobject, jlong jhandle) {
  auto* factory =
      reinterpret_cast<rocksdb::RandomDbPathSupplierFactory*>(jhandle);
  delete factory;
}
