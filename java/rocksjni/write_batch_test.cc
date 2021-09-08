// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::WriteBatch methods testing from Java side.
#include <memory>

#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "include/org_rocksdb_WriteBatch.h"
#include "include/org_rocksdb_WriteBatchTest.h"
#include "include/org_rocksdb_WriteBatchTestInternalHelper.h"
#include "include/org_rocksdb_WriteBatch_Handler.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_buffer_manager.h"
#include "rocksjni/portal.h"
#include "table/scoped_arena_iterator.h"
#include "test_util/testharness.h"
#include "util/string_util.h"

/*
 * Class:     org_rocksdb_WriteBatchTest
 * Method:    getContents
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchTest_getContents(JNIEnv* env,
                                                       jclass /*jclazz*/,
                                                       jlong jwb_handle) {
  auto* b = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(b != nullptr);

  // todo: Currently the following code is directly copied from
  // db/write_bench_test.cc.  It could be implemented in java once
  // all the necessary components can be accessed via jni api.

  ROCKSDB_NAMESPACE::InternalKeyComparator cmp(
      ROCKSDB_NAMESPACE::BytewiseComparator());
  auto factory = std::make_shared<ROCKSDB_NAMESPACE::SkipListFactory>();
  ROCKSDB_NAMESPACE::Options options;
  ROCKSDB_NAMESPACE::WriteBufferManager wb(options.db_write_buffer_size);
  options.memtable_factory = factory;
  ROCKSDB_NAMESPACE::MemTable* mem = new ROCKSDB_NAMESPACE::MemTable(
      cmp, ROCKSDB_NAMESPACE::ImmutableOptions(options),
      ROCKSDB_NAMESPACE::MutableCFOptions(options), &wb,
      ROCKSDB_NAMESPACE::kMaxSequenceNumber, 0 /* column_family_id */);
  mem->Ref();
  std::string state;
  ROCKSDB_NAMESPACE::ColumnFamilyMemTablesDefault cf_mems_default(mem);
  ROCKSDB_NAMESPACE::Status s =
      ROCKSDB_NAMESPACE::WriteBatchInternal::InsertInto(b, &cf_mems_default,
                                                        nullptr, nullptr);
  unsigned int count = 0;
  ROCKSDB_NAMESPACE::Arena arena;
  ROCKSDB_NAMESPACE::ScopedArenaIterator iter(
      mem->NewIterator(ROCKSDB_NAMESPACE::ReadOptions(), &arena));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ROCKSDB_NAMESPACE::ParsedInternalKey ikey;
    ikey.clear();
    ROCKSDB_NAMESPACE::Status pik_status = ROCKSDB_NAMESPACE::ParseInternalKey(
        iter->key(), &ikey, true /* log_err_key */);
    pik_status.PermitUncheckedError();
    assert(pik_status.ok());
    switch (ikey.type) {
      case ROCKSDB_NAMESPACE::kTypeValue:
        state.append("Put(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case ROCKSDB_NAMESPACE::kTypeMerge:
        state.append("Merge(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case ROCKSDB_NAMESPACE::kTypeDeletion:
        state.append("Delete(");
        state.append(ikey.user_key.ToString());
        state.append(")");
        count++;
        break;
      case ROCKSDB_NAMESPACE::kTypeSingleDeletion:
        state.append("SingleDelete(");
        state.append(ikey.user_key.ToString());
        state.append(")");
        count++;
        break;
      case ROCKSDB_NAMESPACE::kTypeRangeDeletion:
        state.append("DeleteRange(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case ROCKSDB_NAMESPACE::kTypeLogData:
        state.append("LogData(");
        state.append(ikey.user_key.ToString());
        state.append(")");
        count++;
        break;
      default:
        assert(false);
        state.append("Err:Expected(");
        state.append(std::to_string(ikey.type));
        state.append(")");
        count++;
        break;
    }
    state.append("@");
    state.append(ROCKSDB_NAMESPACE::ToString(ikey.sequence));
  }
  if (!s.ok()) {
    state.append(s.ToString());
  } else if (ROCKSDB_NAMESPACE::WriteBatchInternal::Count(b) != count) {
    state.append("Err:CountMismatch(expected=");
    state.append(
        std::to_string(ROCKSDB_NAMESPACE::WriteBatchInternal::Count(b)));
    state.append(", actual=");
    state.append(std::to_string(count));
    state.append(")");
  }
  delete mem->Unref();

  jbyteArray jstate = env->NewByteArray(static_cast<jsize>(state.size()));
  if (jstate == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  env->SetByteArrayRegion(
      jstate, 0, static_cast<jsize>(state.size()),
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(state.c_str())));
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jstate);
    return nullptr;
  }

  return jstate;
}

/*
 * Class:     org_rocksdb_WriteBatchTestInternalHelper
 * Method:    setSequence
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBatchTestInternalHelper_setSequence(
    JNIEnv* /*env*/, jclass /*jclazz*/, jlong jwb_handle, jlong jsn) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  ROCKSDB_NAMESPACE::WriteBatchInternal::SetSequence(
      wb, static_cast<ROCKSDB_NAMESPACE::SequenceNumber>(jsn));
}

/*
 * Class:     org_rocksdb_WriteBatchTestInternalHelper
 * Method:    sequence
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBatchTestInternalHelper_sequence(JNIEnv* /*env*/,
                                                             jclass /*jclazz*/,
                                                             jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return static_cast<jlong>(
      ROCKSDB_NAMESPACE::WriteBatchInternal::Sequence(wb));
}

/*
 * Class:     org_rocksdb_WriteBatchTestInternalHelper
 * Method:    append
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBatchTestInternalHelper_append(JNIEnv* /*env*/,
                                                          jclass /*jclazz*/,
                                                          jlong jwb_handle_1,
                                                          jlong jwb_handle_2) {
  auto* wb1 = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle_1);
  assert(wb1 != nullptr);
  auto* wb2 = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle_2);
  assert(wb2 != nullptr);

  ROCKSDB_NAMESPACE::WriteBatchInternal::Append(wb1, wb2);
}
