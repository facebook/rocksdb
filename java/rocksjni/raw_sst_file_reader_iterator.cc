// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::Iterator methods from Java side.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include "include/org_rocksdb_RawSstFileReaderIterator.h"
#include "rocksdb/iterator.h"
#include "rocksjni/portal.h"
#include "table/table_iterator.h"


/*
 * Class:     org_rocksdb_RawSstFileReaderIterator
 * Method:    sequenceNumber
 * Signature: (J)[B
 */
jlong Java_org_rocksdb_RawSstFileReaderIterator_sequenceNumber(JNIEnv* env, jclass /*jcls*/,
                                                               jlong handle) {
  auto* it = reinterpret_cast<ROCKSDB_NAMESPACE::TableIterator*>(handle);
  return static_cast<jlong>(it->SequenceNumber());
}

/*
 * Class:     org_rocksdb_RawSstFileReaderIterator
 * Method:    type
 * Signature: (J)[B
 */
jbyte Java_org_rocksdb_RawSstFileReaderIterator_type(JNIEnv* env, jclass /*jcls*/,
                                                    jlong handle) {
    auto* it = reinterpret_cast<ROCKSDB_NAMESPACE::TableIterator*>(handle);
    return ROCKSDB_NAMESPACE::ValueTypeJni::toJavaValueType(it->type());
}

