//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>
#include <memory>

#include "include/org_rocksdb_CollectionMergeOperator.h"

#include "rocksjni/portal.h"
#include "utilities/merge_operators/collection/collection_merge_operator.h"

/*
 * Class:     org_rocksdb_CollectionMergeOperator
 * Method:    newCollectionMergeOperator
 * Signature: (SJBB)J
 */
jlong Java_org_rocksdb_CollectionMergeOperator_newCollectionMergeOperator__SJBB(
    JNIEnv*, jclass, jshort jfixed_record_len,
    jlong jcomparator_handle, jbyte jcomparator_type,
    jbyte junique_constraint) {
  rocksdb::Comparator* comparator = nullptr;
  if (jcomparator_handle != 0) {
    comparator =
        rocksdb::AbstractComparatorJni::castCppComparator(jcomparator_handle, jcomparator_type);
  }

  const rocksdb::UniqueConstraint unique_constraint =
      rocksdb::UniqueConstraintJni::toCppUniqueConstraint(junique_constraint);
  auto* sptr_collection_merge_operator =
      new std::shared_ptr<rocksdb::MergeOperator>(
        std::make_shared<rocksdb::CollectionMergeOperator>(
          jfixed_record_len, comparator, unique_constraint));
  return reinterpret_cast<jlong>(sptr_collection_merge_operator);
}

/*
 * Class:     org_rocksdb_CollectionMergeOperator
 * Method:    newCollectionMergeOperator
 * Signature: (SBB)J
 */
jlong Java_org_rocksdb_CollectionMergeOperator_newCollectionMergeOperator__SBB(
    JNIEnv*, jclass, jshort jfixed_record_len, jbyte jbuiltin_comparator,
    jbyte junique_constraint) {
  auto* comparator =
    rocksdb::BuiltinComparatorJni::toCppBuiltinComparator(jbuiltin_comparator);

  const rocksdb::UniqueConstraint unique_constraint =
      rocksdb::UniqueConstraintJni::toCppUniqueConstraint(junique_constraint);
  auto* sptr_collection_merge_operator =
      new std::shared_ptr<rocksdb::MergeOperator>(
        std::make_shared<rocksdb::CollectionMergeOperator>(
          jfixed_record_len, comparator, unique_constraint));
  return reinterpret_cast<jlong>(sptr_collection_merge_operator);
}

/*
 * Class:     org_rocksdb_CollectionMergeOperator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_CollectionMergeOperator_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* sptr_collection_merge_operator =
      reinterpret_cast<std::shared_ptr<rocksdb::MergeOperator>*>(jhandle);
  delete sptr_collection_merge_operator;
}
