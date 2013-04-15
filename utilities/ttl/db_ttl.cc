// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "utilities/ttl/db_ttl.h"
#include "include/utilities/utility_db.h"
#include "db/filename.h"
#include "util/coding.h"
#include "include/leveldb/env.h"
#include "include/leveldb/iterator.h"

namespace leveldb {

class TtlIterator : public Iterator {

 public:
  TtlIterator(Iterator* iter, int32_t ts_len)
    : iter_(iter),
      ts_len_(ts_len) {
    assert(iter_);
  }

  ~TtlIterator() {
    delete iter_;
  }

  bool Valid() const {
    return iter_->Valid();
  }

  void SeekToFirst() {
    iter_->SeekToFirst();
  }

  void SeekToLast() {
    iter_->SeekToLast();
  }

  void Seek(const Slice& target) {
    iter_->Seek(target);
  }

  void Next() {
    iter_->Next();
  }

  void Prev() {
    iter_->Prev();
  }

  Slice key() const {
    return iter_->key();
  }

  Slice value() const {
    assert(iter_->value().size() >= (unsigned)ts_len_);
    return std::string(iter_->value().data(), iter_->value().size() - ts_len_);
  }

  Status status() const {
    return iter_->status();
  }

 private:
  Iterator* iter_;
  int32_t ts_len_;
};

// Open the db inside DBWithTTL because options needs pointer to its ttl
DBWithTTL::DBWithTTL(const int32_t ttl,
                     const Options& options,
                     const std::string& dbname,
                     Status& st)
    : ttl_(ttl) {
  assert(options.CompactionFilter == nullptr);
  Options options_to_open = options;
  options_to_open.compaction_filter_args = &ttl_;
  options_to_open.CompactionFilter = DeleteByTS;
  st = DB::Open(options_to_open, dbname, &db_);
}

DBWithTTL::~DBWithTTL() {
  delete db_;
}

Status UtilityDB::OpenTtlDB(
    const Options& options,
    const std::string& dbname,
    DB** dbptr,
    int32_t ttl) {
  Status st;
  *dbptr = new DBWithTTL(ttl, options, dbname, st);
  if (!st.ok()) {
    delete dbptr;
  }
  return st;
}

// returns true(i.e. key-value to be deleted) if its TS has expired based on ttl
bool DBWithTTL::DeleteByTS(
    void* args,
    int level,
    const Slice& key,
    const Slice& old_val,
    std::string* new_val,
    bool* value_changed) {
  return IsStale(old_val, *(int32_t*)args);
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

// Checks if the string is stale or not according to TTl provided
bool DBWithTTL::IsStale(const Slice& value, int32_t ttl) {
  if (ttl <= 0) { // Data is fresh if TTL is non-positive
    return false;
  }
  int32_t curtime;
  if (!GetCurrentTime(curtime).ok()) {
    return false; // Treat the data as fresh if could not get current time
  } else {
    int32_t timestamp_value =
      DecodeFixed32(value.data() + value.size() - kTSLength);
    if ((timestamp_value + ttl) < curtime) {
      return true; // Data is stale
    }
  }
  return false;
}

// Strips the TS from the end of the string
Status DBWithTTL::StripTS(std::string* str) {
  Status st;
  if (str->length() < (unsigned)kTSLength) {
    return Status::IOError("Error: value's length less than timestamp's\n");
  }
  // Erasing characters which hold the TS
  str->erase(str->length() - kTSLength, kTSLength);
  return st;
}

Status DBWithTTL::Put(
    const WriteOptions& o,
    const Slice& key,
    const Slice& val) {
  std::string value_with_ts;
  Status st = AppendTS(val, value_with_ts);
  if (!st.ok()) {
    return st;
  }
  return db_->Put(o, key, value_with_ts);
}

Status DBWithTTL::Get(const ReadOptions& options,
                      const Slice& key,
                      std::string* value) {
  Status st = db_->Get(options, key, value);
  if (!st.ok()) {
    return st;
  }
  return StripTS(value);
}

Status DBWithTTL::Delete(const WriteOptions& wopts, const Slice& key) {
  return db_->Delete(wopts, key);
}

Status DBWithTTL::Write(const WriteOptions& opts, WriteBatch* updates) {
  return db_->Write(opts, updates);
}

Iterator* DBWithTTL::NewIterator(const ReadOptions& opts) {
  return new TtlIterator(db_->NewIterator(opts), kTSLength);
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

void DBWithTTL::CompactRange(const Slice* begin, const Slice* end) {
  db_->CompactRange(begin, end);
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

Status DBWithTTL::GetLiveFiles(std::vector<std::string>& vec, uint64_t* mfs) {
  return db_->GetLiveFiles(vec, mfs);
}

SequenceNumber DBWithTTL::GetLatestSequenceNumber() {
  return db_->GetLatestSequenceNumber();
}

Status DBWithTTL::GetUpdatesSince(
    SequenceNumber seq_number,
    unique_ptr<TransactionLogIterator>* iter) {
  return db_->GetUpdatesSince(seq_number, iter);
}

void DBWithTTL::TEST_Destroy_DBWithTtl() {
  ((DBImpl*) db_)->TEST_Destroy_DBImpl();
}

}  // namespace leveldb
