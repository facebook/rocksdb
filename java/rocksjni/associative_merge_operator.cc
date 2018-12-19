//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>
#include <memory>
#include <string>

#include "include/org_rocksdb_AbstractAssociativeMergeOperator.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksjni/portal.h"

jlong Java_org_rocksdb_AbstractAssociativeMergeOperator_newAssociativeMergeOperator(JNIEnv* env, jobject jobj) {
  auto* sptr_asso_merge_op = new std::shared_ptr<rocksdb::AssociativeMergeOperator>(
      new rocksdb::AssociativeMergeOperatorJniCallback(env, jobj));
  return reinterpret_cast<jlong>(sptr_asso_merge_op);
}

/*
 * Class:     org_rocksdb_AbstractAssociativeMergeOperator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_AbstractAssociativeMergeOperator_disposeInternal(JNIEnv* /*env*/,
                                                                       jobject /*jobj*/,
                                                                       jlong jhandle) {
  auto* sptr_asso_merge_op =
      reinterpret_cast<std::shared_ptr<rocksdb::AssociativeMergeOperator>*>(jhandle);
  delete sptr_asso_merge_op;  // delete std::shared_ptr
}

