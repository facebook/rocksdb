// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef LEVELDB_UTILITIES_TTL_DB_TTL_H_
#define LEVELDB_UTILITIES_TTL_DB_TTL_H_

#include "include/leveldb/db.h"
#include "db/db_impl.h"

namespace leveldb {

class DBWithTTL : public DB {
 public:
  DBWithTTL(const int32_t ttl,
            const Options& options,
            const std::string& dbname,
            Status& st);

  virtual ~DBWithTTL();

  virtual Status Put(const WriteOptions& o,
                     const Slice& key,
                     const Slice& val);

  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);

  virtual Status Delete(const WriteOptions& wopts, const Slice& key);

  virtual Status Write(const WriteOptions& opts, WriteBatch* updates);

  virtual Iterator* NewIterator(const ReadOptions& opts);

  virtual const Snapshot* GetSnapshot();

  virtual void ReleaseSnapshot(const Snapshot* snapshot);

  virtual bool GetProperty(const Slice& property, std::string* value);

  virtual void GetApproximateSizes(const Range* r, int n, uint64_t* sizes);

  virtual void CompactRange(const Slice* begin, const Slice* end);

  virtual int NumberLevels();

  virtual int MaxMemCompactionLevel();

  virtual int Level0StopWriteTrigger();

  virtual Status Flush(const FlushOptions& fopts);

  virtual Status DisableFileDeletions();

  virtual Status EnableFileDeletions();

  virtual Status GetLiveFiles(std::vector<std::string>& vec, uint64_t* mfs);

  virtual SequenceNumber GetLatestSequenceNumber();

  virtual Status GetUpdatesSince(SequenceNumber seq_number,
                                 unique_ptr<TransactionLogIterator>* iter);

  // Simulate a db crash, no elegant closing of database.
  void TEST_Destroy_DBWithTtl();

  static bool DeleteByTS(void* args,
                         int level,
                         const Slice& key,
                         const Slice& old_val,
                         std::string* new_val,
                         bool* value_changed);

  static bool IsStale(const Slice& value, int32_t ttl);

  static Status AppendTS(const Slice& val, std::string& val_with_ts);

  static Status SanityCheckTimestamp(const std::string& str);

  static Status StripTS(std::string* str);

  static Status GetCurrentTime(int32_t& curtime);

  static const int32_t kTSLength = sizeof(int32_t); // size of timestamp

  static const int32_t kMinTimestamp = 1368146402; // 05/09/2013:5:40PM

 private:
  DB* db_;
  int32_t ttl_;
};

}
#endif  // LEVELDB_UTILITIES_TTL_DB_TTL_H_
