//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory>
#include <string>

#include "include/org_rocksdb_CassandraPartitionMetaMergeOperator.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksjni/portal.h"
#include "utilities/cassandra/merge_operator.h"

/*
 * Class:     org_rocksdb_CassandraPartitionMetaMergeOperator
 * Method:    newCassandraPartitionMetaMergeOperator
 * Signature: ()J
 */
jlong JNICALL
Java_org_rocksdb_CassandraPartitionMetaMergeOperator_newCassandraPartitionMetaMergeOperator(
    JNIEnv* /*env*/, jclass /*jclazz*/) {
  auto* op = new std::shared_ptr<rocksdb::MergeOperator>(
      new rocksdb::cassandra::CassandraPartitionMetaMergeOperator());
  return reinterpret_cast<jlong>(op);
}

/*
 * Class:     org_rocksdb_CassandraPartitionMetaMergeOperator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void JNICALL
Java_org_rocksdb_CassandraPartitionMetaMergeOperator_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* op =
      reinterpret_cast<std::shared_ptr<rocksdb::MergeOperator>*>(jhandle);
  delete op;
}
