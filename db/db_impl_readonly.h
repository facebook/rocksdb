// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_READONLY_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_READONLY_H_

#include "db/db_impl.h"

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "port/port.h"
#include "util/stats_logger.h"

#ifdef USE_SCRIBE
#include "scribe/scribe_logger.h"
#endif

namespace leveldb {

class DBImplReadOnly : public DBImpl {
public:
  DBImplReadOnly(const Options& options, const std::string& dbname);
 virtual ~DBImplReadOnly();

 // Implementations of the DB interface
 virtual Status Get(const ReadOptions& options,
                    const Slice& key,
                    std::string* value);

 // TODO: Implement ReadOnly MultiGet?

 virtual Iterator* NewIterator(const ReadOptions&);

 virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value) {
   return Status::NotSupported("Not supported operation in read only mode.");
 }
 virtual Status Merge(const WriteOptions&, const Slice& key,
                      const Slice& value) {
   return Status::NotSupported("Not supported operation in read only mode.");
 }
 virtual Status Delete(const WriteOptions&, const Slice& key) {
   return Status::NotSupported("Not supported operation in read only mode.");
 }
 virtual Status Write(const WriteOptions& options, WriteBatch* updates) {
   return Status::NotSupported("Not supported operation in read only mode.");
 }
 virtual void CompactRange(const Slice* begin, const Slice* end,
                           bool reduce_level = false) {
 }
 virtual Status DisableFileDeletions() {
   return Status::NotSupported("Not supported operation in read only mode.");
 }
 virtual Status EnableFileDeletions() {
   return Status::NotSupported("Not supported operation in read only mode.");
 }
 virtual Status GetLiveFiles(std::vector<std::string>&,
                             uint64_t* manifest_file_size) {
   return Status::NotSupported("Not supported operation in read only mode.");
 }
 virtual Status Flush(const FlushOptions& options) {
   return Status::NotSupported("Not supported operation in read only mode.");
 }

private:
 friend class DB;

 // No copying allowed
 DBImplReadOnly(const DBImplReadOnly&);
 void operator=(const DBImplReadOnly&);
};

}

#endif
