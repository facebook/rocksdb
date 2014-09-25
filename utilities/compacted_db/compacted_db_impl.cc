//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE
#include "utilities/compacted_db/compacted_db_impl.h"
#include "db/db_impl.h"
#include "db/version_set.h"
#include "db/merge_context.h"

namespace rocksdb {

extern void MarkKeyMayExist(void* arg);
extern bool SaveValue(void* arg, const ParsedInternalKey& parsed_key,
                      const Slice& v);

CompactedDBImpl::CompactedDBImpl(
  const DBOptions& options, const std::string& dbname)
  : DBImpl(options, dbname) {
}

CompactedDBImpl::~CompactedDBImpl() {
}

Status CompactedDBImpl::Get(const ReadOptions& options,
     ColumnFamilyHandle*, const Slice& key, std::string* value) {
  size_t left = 0;
  size_t right = files_.num_files - 1;
  while (left < right) {
    size_t mid = (left + right) >> 1;
    const FdWithKeyRange& f = files_.files[mid];
    if (user_comparator_->Compare(ExtractUserKey(f.largest_key), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  const FdWithKeyRange& f = files_.files[right];

  bool value_found;
  MergeContext merge_context;
  Version::Saver saver;
  saver.state = Version::kNotFound;
  saver.ucmp = user_comparator_;
  saver.user_key = key;
  saver.value_found = &value_found;
  saver.value = value;
  saver.merge_operator = nullptr;
  saver.merge_context = &merge_context;
  saver.logger = info_log_;
  saver.statistics = statistics_;
  LookupKey lkey(key, kMaxSequenceNumber);
  f.fd.table_reader->Get(options, lkey.internal_key(),
                         reinterpret_cast<void*>(&saver), SaveValue,
                         MarkKeyMayExist);
  if (saver.state == Version::kFound) {
    return Status::OK();
  }
  return Status::NotFound();
}

Status CompactedDBImpl::Init(const Options& options) {
  mutex_.Lock();
  ColumnFamilyDescriptor cf(kDefaultColumnFamilyName,
                            ColumnFamilyOptions(options));
  Status s = Recover({ cf }, true /* read only */, false);
  if (s.ok()) {
    cfd_ = reinterpret_cast<ColumnFamilyHandleImpl*>(
              DefaultColumnFamily())->cfd();
    delete cfd_->InstallSuperVersion(new SuperVersion(), &mutex_);
  }
  mutex_.Unlock();
  if (!s.ok()) {
    return s;
  }
  version_ = cfd_->GetSuperVersion()->current;
  user_comparator_ = cfd_->user_comparator();
  statistics_ = cfd_->ioptions()->statistics;
  info_log_ = cfd_->ioptions()->info_log;
  // L0 should not have files
  if (version_->file_levels_[0].num_files > 1) {
    return Status::NotSupported("L0 contain more than 1 file");
  }
  if (version_->file_levels_[0].num_files == 1) {
    if (version_->num_non_empty_levels_ > 1) {
      return Status::NotSupported("Both L0 and other level contain files");
    }
    files_ = version_->file_levels_[0];
    return Status::OK();
  }

  for (int i = 1; i < version_->num_non_empty_levels_ - 1; ++i) {
    if (version_->file_levels_[i].num_files > 0) {
      return Status::NotSupported("Other levels also contain files");
    }
  }

  int level = version_->num_non_empty_levels_ - 1;
  if (version_->file_levels_[level].num_files > 0) {
    files_ = version_->file_levels_[version_->num_non_empty_levels_ - 1];
    return Status::OK();
  }
  return Status::NotSupported("no file exists");
}

Status CompactedDBImpl::Open(const Options& options,
                             const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  if (options.max_open_files != -1) {
    return Status::InvalidArgument("require max_open_files = -1");
  }
  if (options.merge_operator.get() != nullptr) {
    return Status::InvalidArgument("merge operator is not supported");
  }
  DBOptions db_options(options);
  std::unique_ptr<CompactedDBImpl> db(new CompactedDBImpl(db_options, dbname));
  Status s = db->Init(options);
  if (s.ok()) {
    *dbptr = db.release();
    Log(options.info_log, "Opened the db as fully compacted mode");
  }
  return s;
}

}   // namespace rocksdb
#endif  // ROCKSDB_LITE
