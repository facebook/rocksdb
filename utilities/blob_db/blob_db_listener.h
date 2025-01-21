//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>

#include "rocksdb/listener.h"
#include "util/mutexlock.h"
#include "utilities/blob_db/blob_db_impl.h"

namespace ROCKSDB_NAMESPACE {
namespace blob_db {

class BlobDBListener : public EventListener {
 public:
  explicit BlobDBListener(BlobDBImpl* blob_db_impl)
      : blob_db_impl_(blob_db_impl) {}

  void OnFlushBegin(DB* /*db*/, const FlushJobInfo& /*info*/) override {
    assert(blob_db_impl_ != nullptr);
    blob_db_impl_->SyncBlobFiles(WriteOptions(Env::IOActivity::kFlush))
        .PermitUncheckedError();
  }

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& /*info*/) override {
    assert(blob_db_impl_ != nullptr);
    blob_db_impl_->UpdateLiveSSTSize(WriteOptions(Env::IOActivity::kFlush));
  }

  void OnCompactionCompleted(DB* /*db*/,
                             const CompactionJobInfo& /*info*/) override {
    assert(blob_db_impl_ != nullptr);
    blob_db_impl_->UpdateLiveSSTSize(
        WriteOptions(Env::IOActivity::kCompaction));
  }

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "BlobDBListener"; }

 protected:
  BlobDBImpl* blob_db_impl_;
};

class BlobDBListenerGC : public BlobDBListener {
 public:
  explicit BlobDBListenerGC(BlobDBImpl* blob_db_impl)
      : BlobDBListener(blob_db_impl) {}

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "BlobDBListenerGC"; }
  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    BlobDBListener::OnFlushCompleted(db, info);

    assert(blob_db_impl_);
    blob_db_impl_->ProcessFlushJobInfo(info);
  }

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& info) override {
    BlobDBListener::OnCompactionCompleted(db, info);

    assert(blob_db_impl_);
    blob_db_impl_->ProcessCompactionJobInfo(info);
  }
};

}  // namespace blob_db
}  // namespace ROCKSDB_NAMESPACE
