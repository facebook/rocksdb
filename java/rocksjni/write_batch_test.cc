// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::WriteBatch methods testing from Java side.
#include <memory>

#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "include/org_rocksdb_WriteBatch.h"
#include "include/org_rocksdb_WriteBatchTest.h"
#include "include/org_rocksdb_WriteBatchTestInternalHelper.h"
#include "include/org_rocksdb_WriteBatch_Handler.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/immutable_options.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_buffer_manager.h"
#include "rocksjni/portal.h"
#include "table/scoped_arena_iterator.h"
#include "util/logging.h"
#include "util/testharness.h"

/*
 * Class:     org_rocksdb_WriteBatchTest
 * Method:    getContents
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchTest_getContents(
    JNIEnv* env, jclass jclazz, jlong jwb_handle) {
  auto* b = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(b != nullptr);

  // todo: Currently the following code is directly copied from
  // db/write_bench_test.cc.  It could be implemented in java once
  // all the necessary components can be accessed via jni api.

  rocksdb::InternalKeyComparator cmp(rocksdb::BytewiseComparator());
  auto factory = std::make_shared<rocksdb::SkipListFactory>();
  rocksdb::Options options;
  rocksdb::WriteBufferManager wb(options.db_write_buffer_size);
  options.memtable_factory = factory;
  rocksdb::MemTable* mem = new rocksdb::MemTable(
      cmp, rocksdb::ImmutableCFOptions(options),
      rocksdb::MutableCFOptions(options, rocksdb::ImmutableCFOptions(options)),
      &wb, rocksdb::kMaxSequenceNumber);
  mem->Ref();
  std::string state;
  rocksdb::ColumnFamilyMemTablesDefault cf_mems_default(mem);
  rocksdb::Status s =
      rocksdb::WriteBatchInternal::InsertInto(b, &cf_mems_default, nullptr);
  int count = 0;
  rocksdb::Arena arena;
  rocksdb::ScopedArenaIterator iter(mem->NewIterator(
      rocksdb::ReadOptions(), &arena));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    rocksdb::ParsedInternalKey ikey;
    memset(reinterpret_cast<void*>(&ikey), 0, sizeof(ikey));
    bool parsed = rocksdb::ParseInternalKey(iter->key(), &ikey);
    assert(parsed);
    switch (ikey.type) {
      case rocksdb::kTypeValue:
        state.append("Put(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case rocksdb::kTypeMerge:
        state.append("Merge(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case rocksdb::kTypeDeletion:
        state.append("Delete(");
        state.append(ikey.user_key.ToString());
        state.append(")");
        count++;
        break;
      default:
        assert(false);
        break;
    }
    state.append("@");
    state.append(rocksdb::NumberToString(ikey.sequence));
  }
  if (!s.ok()) {
    state.append(s.ToString());
  } else if (count != rocksdb::WriteBatchInternal::Count(b)) {
    state.append("CountMismatch()");
  }
  delete mem->Unref();

  jbyteArray jstate = env->NewByteArray(static_cast<jsize>(state.size()));
  env->SetByteArrayRegion(jstate, 0, static_cast<jsize>(state.size()),
                          reinterpret_cast<const jbyte*>(state.c_str()));

  return jstate;
}

/*
 * Class:     org_rocksdb_WriteBatchTestInternalHelper
 * Method:    setSequence
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBatchTestInternalHelper_setSequence(
    JNIEnv* env, jclass jclazz, jlong jwb_handle, jlong jsn) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  rocksdb::WriteBatchInternal::SetSequence(
      wb, static_cast<rocksdb::SequenceNumber>(jsn));
}

/*
 * Class:     org_rocksdb_WriteBatchTestInternalHelper
 * Method:    sequence
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBatchTestInternalHelper_sequence(
    JNIEnv* env, jclass jclazz, jlong jwb_handle) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return static_cast<jlong>(rocksdb::WriteBatchInternal::Sequence(wb));
}

/*
 * Class:     org_rocksdb_WriteBatchTestInternalHelper
 * Method:    append
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBatchTestInternalHelper_append(
    JNIEnv* env, jclass jclazz, jlong jwb_handle_1, jlong jwb_handle_2) {
  auto* wb1 = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle_1);
  assert(wb1 != nullptr);
  auto* wb2 = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle_2);
  assert(wb2 != nullptr);

  rocksdb::WriteBatchInternal::Append(wb1, wb2);
}
