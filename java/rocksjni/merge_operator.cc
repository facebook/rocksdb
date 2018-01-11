// Copyright (c) 2014, Vlad Balan (vlad.gm@gmail.com).  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++
// for rocksdb::MergeOperator.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>
#include <memory>

#include "include/org_rocksdb_StringAppendOperator.h"
#include "rocksjni/portal.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/table.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"

/*
 * Class:     org_rocksdb_StringAppendOperator
 * Method:    newSharedStringAppendOperator
 * Signature: ()J
 */
jlong Java_org_rocksdb_StringAppendOperator_newSharedStringAppendOperator
(JNIEnv* env, jclass jclazz) {
  auto* sptr_string_append_op = new std::shared_ptr<rocksdb::MergeOperator>(
    rocksdb::MergeOperators::CreateFromStringId("stringappend"));
  return reinterpret_cast<jlong>(sptr_string_append_op);
}

/*
 * Class:     org_rocksdb_StringAppendOperator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_StringAppendOperator_disposeInternal(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto* sptr_string_append_op =
      reinterpret_cast<std::shared_ptr<rocksdb::MergeOperator>* >(jhandle);
  delete sptr_string_append_op;  // delete std::shared_ptr
}
