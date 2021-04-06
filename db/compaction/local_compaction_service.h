// Copyright (c) 2019-present, Rockset, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/compaction_service.h"

namespace ROCKSDB_NAMESPACE {
class BlobFileCompletionCallback;
class Cache;
class ErrorHandler;
class EventLogger;
class FSDirectory;
class InstrumentedMutex;
class IOTracer;
class VersionSet;

struct FileOptions;
struct ImmutableDBOptions;

class LocalCompactionService {
 public:
  const std::string& dbname_;
  const std::string& db_id_;
  const std::string& db_session_id_;
  const ImmutableDBOptions& db_options_;

  FileSystemPtr fs_;
  SystemClock* clock_;
  FileOptions* file_options_;
  const std::atomic<bool>* shutting_down_;
  const std::atomic<int>* manual_compaction_paused_;
  const std::atomic<SequenceNumber>* preserve_deletes_seqnum_;
  VersionSet* versions_;

  FSDirectory* db_directory_;
  InstrumentedMutex* db_mutex_;
  ErrorHandler* db_error_handler_;
  std::shared_ptr<Cache> table_cache_;
  EventLogger* event_logger_;
  std::shared_ptr<IOTracer> io_tracer_;
  Statistics* stats_;
  BlobFileCompletionCallback* blob_callback_;

 public:
  LocalCompactionService(
      const std::string& dbname, const std::string& db_id,
      const std::string& db_session_id, const ImmutableDBOptions& db_options,
      FileOptions* file_options_, const std::atomic<bool>* shutting_down,
      const std::atomic<int>* manual_compaction_paused,
      const std::atomic<SequenceNumber>* preserve_deletes_seqnum,
      VersionSet* versions, FSDirectory* db_directory,
      InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
      const std::shared_ptr<Cache>& table_cache, EventLogger* event_logger,
      const std::shared_ptr<IOTracer>& io_tracer,
      BlobFileCompletionCallback* blob_callback);
};
}  // namespace ROCKSDB_NAMESPACE
