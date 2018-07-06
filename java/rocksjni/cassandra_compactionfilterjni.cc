// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include "include/org_rocksdb_CassandraCompactionFilter.h"
#include "utilities/cassandra/cassandra_compaction_filter.h"

/*
 * Class:     org_rocksdb_CassandraCompactionFilter
 * Method:    createNewCassandraCompactionFilter0
 * Signature: (ZI)J
 */
jlong Java_org_rocksdb_CassandraCompactionFilter_createNewCassandraCompactionFilter0(
    JNIEnv* /*env*/, jclass /*jcls*/, jboolean purge_ttl_on_expiration,
    jboolean ignore_range_delete_on_read, jint gc_grace_period_in_seconds) {
  auto* compaction_filter = new rocksdb::cassandra::CassandraCompactionFilter(
      purge_ttl_on_expiration, ignore_range_delete_on_read,
      gc_grace_period_in_seconds);
  // set the native handle to our native compaction filter
  return reinterpret_cast<jlong>(compaction_filter);
}

/*
 * Class:     org_rocksdb_CassandraCompactionFilter
 * Method:    setMetaCfHandle
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL
Java_org_rocksdb_CassandraCompactionFilter_setMetaCfHandle(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong compaction_filter_pointer,
    jlong rocksdb_pointer, jlong meta_cf_handle_pointer) {
  auto* compaction_filter =
      reinterpret_cast<rocksdb::cassandra::CassandraCompactionFilter*>(
          compaction_filter_pointer);
  auto* db = reinterpret_cast<rocksdb::DB*>(rocksdb_pointer);
  auto* meta_cf_handle =
      reinterpret_cast<rocksdb::ColumnFamilyHandle*>(meta_cf_handle_pointer);

  compaction_filter->SetMetaCfHandle(db, meta_cf_handle);
}
