//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/xfunc_test_points.h"
#include "util/xfunc.h"

namespace rocksdb {

#ifdef XFUNC

void xf_manage_release(ManagedIterator* iter) {
  if (!(XFuncPoint::GetSkip() & kSkipNoPrefix)) {
    iter->ReleaseIter(false);
  }
}

void xf_manage_create(ManagedIterator* iter) { iter->SetDropOld(false); }

void xf_manage_new(DBImpl* db, ReadOptions* read_options,
                   bool is_snapshot_supported) {
  if ((!XFuncPoint::Check("managed_xftest_dropold") &&
       (!XFuncPoint::Check("managed_xftest_release"))) ||
      (!read_options->managed)) {
    return;
  }
  if ((!read_options->tailing) && (read_options->snapshot == nullptr) &&
      (!is_snapshot_supported)) {
    read_options->managed = false;
    return;
  }
  if (db->GetOptions().prefix_extractor != nullptr) {
    if (strcmp(db->GetOptions().table_factory.get()->Name(), "PlainTable")) {
      if (!(XFuncPoint::GetSkip() & kSkipNoPrefix)) {
        read_options->total_order_seek = true;
      }
    } else {
      read_options->managed = false;
    }
  }
}

class XFTransactionWriteHandler : public WriteBatch::Handler {
 public:
  Transaction* txn_;
  DBImpl* db_impl_;

  XFTransactionWriteHandler(Transaction* txn, DBImpl* db_impl)
      : txn_(txn), db_impl_(db_impl) {}

  virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                       const Slice& value) override {
    InstrumentedMutexLock l(&db_impl_->mutex_);

    ColumnFamilyHandle* cfh = db_impl_->GetColumnFamilyHandle(column_family_id);
    if (cfh == nullptr) {
      return Status::InvalidArgument(
          "XFUNC test could not find column family "
          "handle for id ",
          ToString(column_family_id));
    }

    txn_->Put(cfh, key, value);

    return Status::OK();
  }

  virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
    InstrumentedMutexLock l(&db_impl_->mutex_);

    ColumnFamilyHandle* cfh = db_impl_->GetColumnFamilyHandle(column_family_id);
    if (cfh == nullptr) {
      return Status::InvalidArgument(
          "XFUNC test could not find column family "
          "handle for id ",
          ToString(column_family_id));
    }

    txn_->Merge(cfh, key, value);

    return Status::OK();
  }

  virtual Status DeleteCF(uint32_t column_family_id,
                          const Slice& key) override {
    InstrumentedMutexLock l(&db_impl_->mutex_);

    ColumnFamilyHandle* cfh = db_impl_->GetColumnFamilyHandle(column_family_id);
    if (cfh == nullptr) {
      return Status::InvalidArgument(
          "XFUNC test could not find column family "
          "handle for id ",
          ToString(column_family_id));
    }

    txn_->Delete(cfh, key);

    return Status::OK();
  }

  virtual void LogData(const Slice& blob) override { txn_->PutLogData(blob); }
};

// Whenever DBImpl::Write is called, create a transaction and do the write via
// the transaction.
void xf_transaction_write(const WriteOptions& write_options,
                          const DBOptions& db_options, WriteBatch* my_batch,
                          WriteCallback* callback, DBImpl* db_impl, Status* s,
                          bool* write_attempted) {
  if (callback != nullptr) {
    // We may already be in a transaction, don't force a transaction
    *write_attempted = false;
    return;
  }

  OptimisticTransactionDB* txn_db = new OptimisticTransactionDB(db_impl);
  Transaction* txn = Transaction::BeginTransaction(txn_db, write_options);

  XFTransactionWriteHandler handler(txn, db_impl);
  *s = my_batch->Iterate(&handler);

  if (!s->ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, db_options.info_log,
        "XFUNC test could not iterate batch.  status: $s\n",
        s->ToString().c_str());
  }

  *s = txn->Commit();

  if (!s->ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, db_options.info_log,
        "XFUNC test could not commit transaction.  status: $s\n",
        s->ToString().c_str());
  }

  *write_attempted = true;
  delete txn;
  delete txn_db;
}

#endif  // XFUNC

}  // namespace rocksdb
