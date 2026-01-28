//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Recovery logic for partitioned WAL files.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/partitioned_log_format.h"
#include "db/partitioned_log_reader.h"
#include "db/write_batch_internal.h"
#include "options/db_options.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyMemTables;
class FlushScheduler;
class TrimHistoryScheduler;

// PartitionedWALRecovery handles recovery from partitioned WAL files.
//
// The recovery process works as follows:
// 1. The main WAL is read first (by the traditional WAL recovery logic)
//    which collects CompletionRecords
// 2. CompletionRecords are sorted by first_sequence (ascending order)
// 3. For each CompletionRecord:
//    a. Open the partitioned WAL file (cached for reuse)
//    b. Read the body from the partitioned WAL at the recorded offset
//    c. Verify the CRC matches
//    d. Rebuild the WriteBatch with the sequence number from the record
//    e. Insert into the appropriate column family's memtable
// 4. Return the maximum recovered sequence number
//
// Thread safety: This class is not thread-safe. It is designed to be used
// during single-threaded database recovery.
class PartitionedWALRecovery {
 public:
  // Create a recovery handler.
  //
  // Parameters:
  //   db_options: Database options
  //   db_path: Path to the database directory
  //   fs: FileSystem to use for file operations (not owned)
  PartitionedWALRecovery(const ImmutableDBOptions& db_options,
                         const std::string& db_path, FileSystem* fs);

  ~PartitionedWALRecovery();

  // Recover from partitioned WAL files using completion records from main WAL.
  //
  // This method should be called after traditional WAL recovery has read
  // the main WAL and collected completion records.
  //
  // Parameters:
  //   completion_records: Records read from the main WAL indicating completed
  //                       writes to partitioned WAL files
  //   cf_mems: ColumnFamilyMemTables for inserting recovered data
  //   flush_scheduler: FlushScheduler for scheduling memtable flushes
  //   trim_history_scheduler: TrimHistoryScheduler for trimming history
  //   log_number: The WAL log number being recovered
  //   max_sequence: Output parameter set to the maximum recovered sequence
  //                 number. Only updated if recovery produces a higher value.
  //
  // Returns:
  //   Status::OK() on success
  //   Status::Corruption() if a partitioned WAL file is missing or corrupted
  //   Other errors on I/O failures
  // Parameters:
  //   completion_records: Records read from the main WAL indicating completed
  //                       writes to partitioned WAL files
  //   cf_mems: ColumnFamilyMemTables for inserting recovered data
  //   flush_scheduler: FlushScheduler for scheduling memtable flushes
  //   trim_history_scheduler: TrimHistoryScheduler for trimming history
  //   log_number: The WAL log number being recovered
  //   min_wal_number: The minimum WAL number that needs recovery. Partition
  //                   files from logs older than this may have been GC'd.
  //   max_sequence: Output parameter set to the maximum recovered sequence
  //                 number. Only updated if recovery produces a higher value.
  Status Recover(const std::vector<log::CompletionRecord>& completion_records,
                 ColumnFamilyMemTables* cf_mems,
                 FlushScheduler* flush_scheduler,
                 TrimHistoryScheduler* trim_history_scheduler,
                 uint64_t log_number, uint64_t min_wal_number,
                 SequenceNumber* max_sequence);

 private:
  // Read body from partitioned WAL file and verify CRC.
  //
  // Parameters:
  //   record: The completion record containing location and CRC info
  //   body: Output parameter filled with the body data
  //
  // Returns:
  //   Status::OK() on success
  //   Status::Corruption() if the partitioned WAL file is missing, truncated,
  //                        or the CRC doesn't match
  //   Other errors on I/O failures
  Status ReadAndVerifyBody(const log::CompletionRecord& record,
                           std::string* body);

  // Get or open a partitioned log reader for the given WAL number.
  // Readers are cached for reuse.
  //
  // Parameters:
  //   partition_wal_number: The partitioned WAL file number
  //   reader: Output parameter set to the reader (not owned by caller)
  //
  // Returns:
  //   Status::OK() on success
  //   Status::Corruption() if the file doesn't exist
  //   Other errors on I/O failures
  Status GetOrOpenReader(uint64_t partition_wal_number,
                         log::PartitionedLogReader** reader);

  // Generate the file path for a partitioned WAL file.
  // Uses the PartitionedWALManager naming convention.
  std::string GetPartitionedWALPath(uint64_t partition_wal_number) const;

  const ImmutableDBOptions& db_options_;
  std::string wal_dir_;
  FileSystem* fs_;

  // Cache of open partitioned log readers.
  // Key: partition_wal_number
  // Value: reader for that partition file
  std::unordered_map<uint64_t, std::unique_ptr<log::PartitionedLogReader>>
      readers_;
};

}  // namespace ROCKSDB_NAMESPACE
