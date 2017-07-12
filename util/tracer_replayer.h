// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include "db/column_family.h"
#include "port/port.h"
#include "rocksdb/trace_reader_writer.h"
#include "util/coding.h"

namespace rocksdb {

Status StartTrace(DB* db, const std::string& trace_filename);
Status StartTrace(DB* db, std::unique_ptr<TraceWriter>&& trace_writer);
Status EndTrace(DB* db);

Status StartReplay(DB* db, std::vector<ColumnFamilyHandle*>& handles,
                   std::unique_ptr<TraceReader>&& trace_reader,
                   bool no_wait = false);
Status StartReplay(DB* db, std::vector<ColumnFamilyHandle*>& handles,
                   const std::string& trace_filename, bool no_wait = false);

class DBImpl;
class Slice;

enum TraceType : char {
  kTraceStart = 0,
  kTraceEnd,
  kTraceWrite,
  kTraceRead,
  kTraceIterator,
};

using TraceRecord = std::pair<std::string, std::string>;

class Tracer {
 public:
  // 8-byte timestamp(ms) + 1-byte type
  static const size_t kKeySize = 9;
  Tracer(Env* env, std::unique_ptr<TraceWriter>&& writer);

  ~Tracer();

  void Write(WriteBatch* batch);

  void Get(ColumnFamilyHandle* column_family, const Slice& key);

  void BackgroundTrace();

 private:
  inline void Start();
  inline void Finish();
  inline void AppendTrace(uint64_t timestamp, TraceType type,
                          std::string&& value);
  std::string EncodeKey(uint64_t timestamp, TraceType type);

  Env* const env_;
  bool stop_;
  uint64_t num_ops_;
  std::unique_ptr<port::Thread> bg_thread_;
  std::unique_ptr<TraceWriter> trace_writer_;
  std::queue<TraceRecord> log_queue_;
  std::queue<TraceRecord> user_queue_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

class Replayer {
 public:
  Replayer(DBImpl* db, std::vector<ColumnFamilyHandle*>& handles,
           std::unique_ptr<TraceReader>&& reader);

  ~Replayer();

  Status WaitForReplay();

  void BackgroundReplay();

 private:
  void DecodeKey(const std::string& key, uint64_t* timestamp, TraceType* type);

  DBImpl* db_;
  bool stop_;
  std::unique_ptr<TraceReader> trace_reader_;
  std::unique_ptr<port::Thread> bg_thread_;
  std::unordered_map<uint32_t, ColumnFamilyHandle*> handle_map_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

}  // namespace rocksdb
