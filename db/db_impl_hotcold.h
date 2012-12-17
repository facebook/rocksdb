// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_HOTCOLD_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_HOTCOLD_H_

#include "db/db_impl.h"

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "util/stats_logger.h"

#ifdef USE_SCRIBE
#include "scribe/scribe_logger.h"
#endif

namespace leveldb {

class DBImplHotCold : public DB {
 public:
  // s indicates whether the construction of this database was successful, if
  // s.ok() == false, this instance must be deleted immediately.
  DBImplHotCold(const Options& options, const std::string& dbname, Status& s);
  virtual ~DBImplHotCold();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end);
  virtual int NumberLevels();
  virtual int MaxMemCompactionLevel();
  virtual int Level0StopWriteTrigger();
  virtual Status Flush(const FlushOptions& options);
  virtual Status DisableFileDeletions();
  virtual Status EnableFileDeletions();
  virtual Status GetLiveFiles(std::vector<std::string>&,
                              uint64_t* manifest_file_size);
  virtual SequenceNumber GetLatestSequenceNumber();
  virtual Status GetUpdatesSince(SequenceNumber seq_number,
                                 TransactionLogIterator** iter);

 private:
  friend class DB;

  // No copying allowed
  DBImplHotCold(const DBImplHotCold&);
  void operator=(const DBImplHotCold&);

  DB* dataDB_;
  DB* metricsDB_;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_DB_IMPL_HOTCOLD_H_
