// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "utilities/ttl/db_ttl.h"
#include "db/filename.h"
#include "util/coding.h"
#include "include/rocksdb/env.h"
#include "include/rocksdb/iterator.h"

namespace rocksdb {

// Open the db inside DBWithTTL because options needs pointer to its ttl
DBWithTTL::DBWithTTL(const int32_t ttl,
                     const Options& options,
                     const std::string& dbname,
                     Status& st,
                     bool read_only)
    : StackableDB(nullptr) {
  Options options_to_open = options;

  if (options.compaction_filter) {
    ttl_comp_filter_.reset(
        new TtlCompactionFilter(ttl, options.compaction_filter));
    options_to_open.compaction_filter = ttl_comp_filter_.get();
  } else {
    options_to_open.compaction_filter_factory =
      std::shared_ptr<CompactionFilterFactory>(
          new TtlCompactionFilterFactory(
            ttl, options.compaction_filter_factory));
  }

  if (options.merge_operator) {
    options_to_open.merge_operator.reset(
      new TtlMergeOperator(options.merge_operator));
  }

  if (read_only) {
    st = DB::OpenForReadOnly(options_to_open, dbname, &db_);
  } else {
    st = DB::Open(options_to_open, dbname, &db_);
  }
}

DBWithTTL::~DBWithTTL() {
  delete db_;
}

Status UtilityDB::OpenTtlDB(
    const Options& options,
    const std::string& dbname,
    StackableDB** dbptr,
    int32_t ttl,
    bool read_only) {
  Status st;
  *dbptr = new DBWithTTL(ttl, options, dbname, st, read_only);
  if (!st.ok()) {
    delete *dbptr;
  }
  return st;
}

// Gives back the current time
Status DBWithTTL::GetCurrentTime(int32_t& curtime) {
  return Env::Default()->GetCurrentTime((int64_t*)&curtime);
}

// Appends the current timestamp to the string.
// Returns false if could not get the current_time, true if append succeeds
Status DBWithTTL::AppendTS(const Slice& val, std::string& val_with_ts) {
  val_with_ts.reserve(kTSLength + val.size());
  char ts_string[kTSLength];
  int32_t curtime;
  Status st = GetCurrentTime(curtime);
  if (!st.ok()) {
    return st;
  }
  EncodeFixed32(ts_string, curtime);
  val_with_ts.append(val.data(), val.size());
  val_with_ts.append(ts_string, kTSLength);
  return st;
}

// Returns corruption if the length of the string is lesser than timestamp, or
// timestamp refers to a time lesser than ttl-feature release time
Status DBWithTTL::SanityCheckTimestamp(const Slice& str) {
  if (str.size() < kTSLength) {
    return Status::Corruption("Error: value's length less than timestamp's\n");
  }
  // Checks that TS is not lesser than kMinTimestamp
  // Gaurds against corruption & normal database opened incorrectly in ttl mode
  int32_t timestamp_value =
    DecodeFixed32(str.data() + str.size() - kTSLength);
  if (timestamp_value < kMinTimestamp){
    return Status::Corruption("Error: Timestamp < ttl feature release time!\n");
  }
  return Status::OK();
}

// Checks if the string is stale or not according to TTl provided
bool DBWithTTL::IsStale(const Slice& value, int32_t ttl) {
  if (ttl <= 0) { // Data is fresh if TTL is non-positive
    return false;
  }
  int32_t curtime;
  if (!GetCurrentTime(curtime).ok()) {
    return false; // Treat the data as fresh if could not get current time
  }
  int32_t timestamp_value =
    DecodeFixed32(value.data() + value.size() - kTSLength);
  return (timestamp_value + ttl) < curtime;
}

// Strips the TS from the end of the string
Status DBWithTTL::StripTS(std::string* str) {
  Status st;
  if (str->length() < kTSLength) {
    return Status::Corruption("Bad timestamp in key-value");
  }
  // Erasing characters which hold the TS
  str->erase(str->length() - kTSLength, kTSLength);
  return st;
}

Status DBWithTTL::Put(
    const WriteOptions& opt,
    const Slice& key,
    const Slice& val) {
  WriteBatch batch;
  batch.Put(key, val);
  return Write(opt, &batch);
}

Status DBWithTTL::Get(const ReadOptions& options,
                      const Slice& key,
                      std::string* value) {
  Status st = db_->Get(options, key, value);
  if (!st.ok()) {
    return st;
  }
  st = SanityCheckTimestamp(*value);
  if (!st.ok()) {
    return st;
  }
  return StripTS(value);
}

std::vector<Status> DBWithTTL::MultiGet(const ReadOptions& options,
                                        const std::vector<Slice>& keys,
                                        std::vector<std::string>* values) {
  return std::vector<Status>(keys.size(),
                             Status::NotSupported("MultiGet not\
                               supported with TTL"));
}

bool DBWithTTL::KeyMayExist(const ReadOptions& options,
                            const Slice& key,
                            std::string* value,
                            bool* value_found) {
  bool ret = db_->KeyMayExist(options, key, value, value_found);
  if (ret && value != nullptr && value_found != nullptr && *value_found) {
    if (!SanityCheckTimestamp(*value).ok() || !StripTS(value).ok()) {
      return false;
    }
  }
  return ret;
}

Status DBWithTTL::Delete(const WriteOptions& wopts, const Slice& key) {
  return db_->Delete(wopts, key);
}

Status DBWithTTL::Merge(const WriteOptions& opt,
                        const Slice& key,
                        const Slice& value) {
  WriteBatch batch;
  batch.Merge(key, value);
  return Write(opt, &batch);
}

Status DBWithTTL::Write(const WriteOptions& opts, WriteBatch* updates) {
  class Handler : public WriteBatch::Handler {
   public:
    WriteBatch updates_ttl;
    Status batch_rewrite_status;
    virtual void Put(const Slice& key, const Slice& value) {
      std::string value_with_ts;
      Status st = AppendTS(value, value_with_ts);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        updates_ttl.Put(key, value_with_ts);
      }
    }
    virtual void Merge(const Slice& key, const Slice& value) {
      std::string value_with_ts;
      Status st = AppendTS(value, value_with_ts);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        updates_ttl.Merge(key, value_with_ts);
      }
    }
    virtual void Delete(const Slice& key) {
      updates_ttl.Delete(key);
    }
    virtual void LogData(const Slice& blob) {
      updates_ttl.PutLogData(blob);
    }
  };
  Handler handler;
  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Iterator* DBWithTTL::NewIterator(const ReadOptions& opts) {
  return new TtlIterator(db_->NewIterator(opts));
}

const Snapshot* DBWithTTL::GetSnapshot() {
  return db_->GetSnapshot();
}

void DBWithTTL::ReleaseSnapshot(const Snapshot* snapshot) {
  db_->ReleaseSnapshot(snapshot);
}

bool DBWithTTL::GetProperty(const Slice& property, std::string* value) {
  return db_->GetProperty(property, value);
}

void DBWithTTL::GetApproximateSizes(const Range* r, int n, uint64_t* sizes) {
  db_->GetApproximateSizes(r, n, sizes);
}

void DBWithTTL::CompactRange(const Slice* begin, const Slice* end,
                             bool reduce_level, int target_level) {
  db_->CompactRange(begin, end, reduce_level, target_level);
}

int DBWithTTL::NumberLevels() {
  return db_->NumberLevels();
}

int DBWithTTL::MaxMemCompactionLevel() {
  return db_->MaxMemCompactionLevel();
}

int DBWithTTL::Level0StopWriteTrigger() {
  return db_->Level0StopWriteTrigger();
}

Status DBWithTTL::Flush(const FlushOptions& fopts) {
  return db_->Flush(fopts);
}

Status DBWithTTL::DisableFileDeletions() {
  return db_->DisableFileDeletions();
}

Status DBWithTTL::EnableFileDeletions() {
  return db_->EnableFileDeletions();
}

Status DBWithTTL::GetLiveFiles(std::vector<std::string>& vec, uint64_t* mfs,
                               bool flush_memtable) {
  return db_->GetLiveFiles(vec, mfs, flush_memtable);
}

SequenceNumber DBWithTTL::GetLatestSequenceNumber() const {
  return db_->GetLatestSequenceNumber();
}

Status DBWithTTL::GetSortedWalFiles(VectorLogPtr& files) {
  return db_->GetSortedWalFiles(files);
}

Status DBWithTTL::DeleteFile(std::string name) {
  return db_->DeleteFile(name);
}

Status DBWithTTL::GetUpdatesSince(
    SequenceNumber seq_number,
    unique_ptr<TransactionLogIterator>* iter) {
  return db_->GetUpdatesSince(seq_number, iter);
}

void DBWithTTL::TEST_Destroy_DBWithTtl() {
  ((DBImpl*) db_)->TEST_Destroy_DBImpl();
}

}  // namespace rocksdb
