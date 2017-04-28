//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once
#ifndef ROCKSDB_LITE

#include <map>
#include <string>
#include <vector>

#include "rocksdb/db.h"

namespace rocksdb {

// Date tiered database is a wrapper of DB that implements
// a simplified DateTieredCompactionStrategy by using multiple column famillies
// as time windows.
//
// DateTieredDB provides an interface similar to DB, but it assumes that user
// provides keys with last 8 bytes encoded as timestamp in seconds. DateTieredDB
// is assigned with a TTL to declare when data should be deleted.
//
// DateTieredDB hides column families layer from standard RocksDB instance. It
// uses multiple column families to manage time series data, each containing a
// specific range of time. Column families are named by its maximum possible
// timestamp. A column family is created automatically when data newer than
// latest timestamp of all existing column families. The time range of a column
// family is configurable by `column_family_interval`. By doing this, we
// guarantee that compaction will only happen in a column family.
//
// DateTieredDB is assigned with a TTL. When all data in a column family are
// expired (CF_Timestamp <= CUR_Timestamp - TTL), we directly drop the whole
// column family.
//
// TODO(jhli): This is only a simplified version of DTCS. In a complete DTCS,
// time windows can be merged over time, so that older time windows will have
// larger time range. Also, compaction are executed only for adjacent SST files
// to guarantee there is no time overlap between SST files.

class DateTieredDB {
 public:
  // Open a DateTieredDB whose name is `dbname`.
  // Similar to DB::Open(), created database object is stored in dbptr.
  //
  // Two parameters can be configured: `ttl` to specify the length of time that
  // keys should exist in the database, and `column_family_interval` to specify
  // the time range of a column family interval.
  //
  // Open a read only database if read only is set as true.
  // TODO(jhli): Should use an option object that includes ttl and
  // column_family_interval.
  static Status Open(const Options& options, const std::string& dbname,
                     DateTieredDB** dbptr, int64_t ttl,
                     int64_t column_family_interval, bool read_only = false);

  explicit DateTieredDB() {}

  virtual ~DateTieredDB() {}

  // Wrapper for Put method. Similar to DB::Put(), but column family to be
  // inserted is decided by the timestamp in keys, i.e. the last 8 bytes of user
  // key. If key is already obsolete, it will not be inserted.
  //
  // When client put a key value pair in DateTieredDB, it assumes last 8 bytes
  // of keys are encoded as timestamp. Timestamp is a 64-bit signed integer
  // encoded as the number of seconds since 1970-01-01 00:00:00 (UTC) (Same as
  // Env::GetCurrentTime()). Timestamp should be encoded in big endian.
  virtual Status Put(const WriteOptions& options, const Slice& key,
                     const Slice& val) = 0;

  // Wrapper for Get method. Similar to DB::Get() but column family is decided
  // by timestamp in keys. If key is already obsolete, it will not be found.
  virtual Status Get(const ReadOptions& options, const Slice& key,
                     std::string* value) = 0;

  // Wrapper for Delete method. Similar to DB::Delete() but column family is
  // decided by timestamp in keys. If key is already obsolete, return NotFound
  // status.
  virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

  // Wrapper for KeyMayExist method. Similar to DB::KeyMayExist() but column
  // family is decided by timestamp in keys. Return false when key is already
  // obsolete.
  virtual bool KeyMayExist(const ReadOptions& options, const Slice& key,
                           std::string* value, bool* value_found = nullptr) = 0;

  // Wrapper for Merge method. Similar to DB::Merge() but column family is
  // decided by timestamp in keys.
  virtual Status Merge(const WriteOptions& options, const Slice& key,
                       const Slice& value) = 0;

  // Create an iterator that hides low level details. This iterator internally
  // merge results from all active time series column families. Note that
  // column families are not deleted until all data are obsolete, so this
  // iterator can possibly access obsolete key value pairs.
  virtual Iterator* NewIterator(const ReadOptions& opts) = 0;

  // Explicitly drop column families in which all keys are obsolete. This
  // process is also inplicitly done in Put() operation.
  virtual Status DropObsoleteColumnFamilies() = 0;

  static const uint64_t kTSLength = sizeof(int64_t);  // size of timestamp
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
