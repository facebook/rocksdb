// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::DbPathSupplierFactory.

#include "include/org_rocksdb_GradualMoveOldDataDbPathSupplierFactory.h"
#include "include/org_rocksdb_RandomDbPathSupplierFactory.h"

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
    JNIEnv* /* env */, jclass /* jclazz */) {
  auto* factory = rocksdb::NewGradualMoveOldDataDbPathSupplierFactory();
  auto* factory_shared_ptr =
    new std::shared_ptr<rocksdb::DbPathSupplierFactory>(factory);
  return reinterpret_cast<jlong>(factory_shared_ptr);
}

/*
 * Class:     org_rocksdb_GradualMoveOldDataDbPathSupplierFactory
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_GradualMoveOldDataDbPathSupplierFactory_disposeInternal(
    JNIEnv* /* env */, jobject /* jobject */, jlong jhandle) {
  auto* factory_shared_ptr =
      reinterpret_cast<std::shared_ptr<rocksdb::DbPathSupplierFactory>*>(jhandle);
  delete factory_shared_ptr;
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
    JNIEnv* /* env */, jclass /* jclazz */) {
  auto* factory = rocksdb::NewRandomDbPathSupplierFactory();
  auto* factory_shared_ptr =
    new std::shared_ptr<rocksdb::DbPathSupplierFactory>(factory);
  return reinterpret_cast<jlong>(factory_shared_ptr);
}

/*
 * Class:     org_rocksdb_RandomDbPathSupplierFactory
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RandomDbPathSupplierFactory_disposeInternal(
    JNIEnv* /* env */, jobject /* jobject */, jlong jhandle) {
  auto* factory_shared_ptr =
      reinterpret_cast<std::shared_ptr<rocksdb::DbPathSupplierFactory>*>(jhandle);
  delete factory_shared_ptr;
}
