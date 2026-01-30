//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Recovery implementation for partitioned WAL files.

#include "db/partitioned_wal_recovery.h"

#include <algorithm>
#include <cinttypes>
#include <cstdint>

#include "db/flush_scheduler.h"
#include "db/partitioned_log_writer.h"
#include "db/partitioned_wal_manager.h"
#include "db/trim_history_scheduler.h"
#include "file/random_access_file_reader.h"
#include "file/sequence_file_reader.h"
#include "logging/logging.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

PartitionedWALRecovery::PartitionedWALRecovery(
    const ImmutableDBOptions& db_options, const std::string& db_path,
    FileSystem* fs)
    : db_options_(db_options), fs_(fs) {
  // Use WAL directory if configured, otherwise use db_path
  wal_dir_ = db_options_.GetWalDir(db_path);
}

PartitionedWALRecovery::~PartitionedWALRecovery() = default;

Status PartitionedWALRecovery::Recover(
    const std::vector<log::CompletionRecord>& completion_records,
    ColumnFamilyMemTables* cf_mems, FlushScheduler* flush_scheduler,
    TrimHistoryScheduler* trim_history_scheduler, uint64_t log_number,
    uint64_t min_wal_number, SequenceNumber* max_sequence) {
  if (completion_records.empty()) {
    // Nothing to recover
    return Status::OK();
  }

  TEST_SYNC_POINT_CALLBACK(
      "PartitionedWALRecovery::Recover:BeforeSort",
      const_cast<std::vector<log::CompletionRecord>*>(&completion_records));

  // Sort completion records by first_sequence (ascending order)
  // This ensures we replay writes in the correct order
  std::vector<log::CompletionRecord> sorted_records = completion_records;
  std::sort(sorted_records.begin(), sorted_records.end(),
            [](const log::CompletionRecord& a, const log::CompletionRecord& b) {
              return a.first_sequence < b.first_sequence;
            });

  ROCKS_LOG_INFO(db_options_.info_log,
                 "Recovering %zu completion records from partitioned WAL",
                 sorted_records.size());

  SequenceNumber recovered_max_seq = 0;
  size_t skipped_records = 0;

  for (const auto& record : sorted_records) {
    TEST_SYNC_POINT_CALLBACK("PartitionedWALRecovery::Recover:BeforeReadBody",
                             const_cast<log::CompletionRecord*>(&record));

    // Check if this completion record references an old partition file that
    // may have been garbage collected. This can happen because:
    // 1. A write starts when partition log number is X
    // 2. Body is written to partition file X
    // 3. Main WAL rotates multiple times (due to stalls, slow writes, etc.)
    // 4. Completion record is written to main WAL Y where Y >> X
    // 5. Partition files with log number < min_wal_number get GC'd
    // 6. Main WAL Y still contains completion records referencing log X
    //
    // If the partition log number is < min_wal_number, the data has already
    // been flushed to SST (otherwise min_wal_number would be lower), so we
    // can safely skip this record.
    uint64_t partition_log_number =
        log::PartitionedLogWriter::WriteResult::DecodeWalNumber(
            record.partition_wal_number);
    if (partition_log_number < min_wal_number) {
      ROCKS_LOG_INFO(db_options_.info_log,
                     "Skipping completion record referencing old partition "
                     "file: partition_log_number=%" PRIu64
                     " < min_wal_number=%" PRIu64 ", record: %s",
                     partition_log_number, min_wal_number,
                     record.DebugString().c_str());
      skipped_records++;
      continue;
    }

    // Read the body from the partitioned WAL file
    std::string body;
    Status s = ReadAndVerifyBody(record, &body);
    if (!s.ok()) {
      // If the partition file is missing and its log number is < current WAL
      // log number, the data may have been flushed. Log a warning and continue.
      // This is a less strict check for edge cases not caught above.
      if (s.IsCorruption() && partition_log_number < log_number) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Partition file missing for old log number, skipping: "
                       "partition_log_number=%" PRIu64 " < log_number=%" PRIu64
                       ", record: %s, error: %s",
                       partition_log_number, log_number,
                       record.DebugString().c_str(), s.ToString().c_str());
        skipped_records++;
        continue;
      }
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Failed to read partitioned WAL body: %s, record: %s",
                      s.ToString().c_str(), record.DebugString().c_str());
      return s;
    }

    // Rebuild the WriteBatch from the body with the recorded sequence number
    WriteBatch batch;
    s = WriteBatchInternal::SetSequenceAndRebuildFromBody(
        &batch, record.first_sequence, Slice(body));
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Failed to rebuild WriteBatch from body: %s, record: %s",
                      s.ToString().c_str(), record.DebugString().c_str());
      return Status::Corruption("Failed to rebuild WriteBatch from body",
                                s.ToString());
    }

    // Verify the batch count matches the record count
    uint32_t batch_count = WriteBatchInternal::Count(&batch);
    if (batch_count != record.record_count) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "WriteBatch count mismatch: expected %u, got %u",
                      record.record_count, batch_count);
      return Status::Corruption(
          "WriteBatch count mismatch during recovery: expected " +
          std::to_string(record.record_count) + ", got " +
          std::to_string(batch_count));
    }

    TEST_SYNC_POINT_CALLBACK(
        "PartitionedWALRecovery::Recover:BeforeMemtableInsert",
        const_cast<log::CompletionRecord*>(&record));

    // Insert into memtable
    // If column family was not found, it might mean that the WAL write
    // batch references to the column family that was dropped after the
    // insert. We don't want to fail the whole write batch in that case --
    // we just ignore the update. That's why we set
    // ignore_missing_column_families to true.
    SequenceNumber next_seq;
    bool has_valid_writes = false;
    s = WriteBatchInternal::InsertInto(
        &batch, cf_mems, flush_scheduler, trim_history_scheduler,
        true /* ignore_missing_column_families */, log_number, nullptr /* db */,
        false /* concurrent_memtable_writes */, &next_seq, &has_valid_writes,
        false /* seq_per_batch */, true /* batch_per_txn */);

    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Failed to insert into memtable during recovery: %s",
                      s.ToString().c_str());
      return s;
    }

    // Track the maximum sequence number
    if (record.last_sequence > recovered_max_seq) {
      recovered_max_seq = record.last_sequence;
    }

    ROCKS_LOG_DEBUG(db_options_.info_log,
                    "Recovered batch from partitioned WAL: seq=[%llu, %llu], "
                    "count=%u, cf=%u",
                    static_cast<unsigned long long>(record.first_sequence),
                    static_cast<unsigned long long>(record.last_sequence),
                    record.record_count, record.column_family_id);
  }

  // Update max_sequence if we recovered anything
  if (recovered_max_seq > 0 && max_sequence != nullptr) {
    // The next sequence number to use is one more than the max recovered
    SequenceNumber new_next_seq = recovered_max_seq + 1;
    // If max_sequence is kMaxSequenceNumber (initial value) or less than what
    // we recovered, update it
    if (*max_sequence == kMaxSequenceNumber || new_next_seq > *max_sequence) {
      *max_sequence = new_next_seq;
    }
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "Recovered %zu records from partitioned WAL (skipped %zu), "
                 "max_seq=%llu",
                 sorted_records.size() - skipped_records, skipped_records,
                 static_cast<unsigned long long>(recovered_max_seq));

  TEST_SYNC_POINT_CALLBACK("PartitionedWALRecovery::Recover:AfterRecover",
                           &recovered_max_seq);

  return Status::OK();
}

Status PartitionedWALRecovery::ReadAndVerifyBody(
    const log::CompletionRecord& record, std::string* body) {
  assert(body);

  // Get or open the reader for this partition WAL number
  log::PartitionedLogReader* reader = nullptr;
  Status s = GetOrOpenReader(record.partition_wal_number, &reader);
  if (!s.ok()) {
    return s;
  }
  assert(reader);

  // Read the body at the recorded offset and verify CRC
  s = reader->ReadBodyAt(record.partition_offset, record.body_size,
                         record.body_crc, body);
  if (!s.ok()) {
    return Status::Corruption("Failed to read body from partitioned WAL " +
                                  std::to_string(record.partition_wal_number) +
                                  " at offset " +
                                  std::to_string(record.partition_offset),
                              s.ToString());
  }

  return Status::OK();
}

Status PartitionedWALRecovery::GetOrOpenReader(
    uint64_t partition_wal_number, log::PartitionedLogReader** reader) {
  assert(reader);

  // Check if reader is already cached
  auto it = readers_.find(partition_wal_number);
  if (it != readers_.end()) {
    *reader = it->second.get();
    return Status::OK();
  }

  // Open the partitioned WAL file
  std::string path = GetPartitionedWALPath(partition_wal_number);

  // Check if file exists
  IOStatus ios = fs_->FileExists(path, IOOptions(), nullptr);
  if (!ios.ok()) {
    return Status::Corruption(
        "Partitioned WAL file missing: " + path +
            " for partition_wal_number=" + std::to_string(partition_wal_number),
        ios.ToString());
  }

  // Open for sequential reading
  std::unique_ptr<FSSequentialFile> seq_file;
  ios = fs_->NewSequentialFile(path, FileOptions(), &seq_file, nullptr);
  if (!ios.ok()) {
    return ios;
  }
  std::unique_ptr<SequentialFileReader> seq_reader =
      std::make_unique<SequentialFileReader>(std::move(seq_file), path);

  // Open for random access reading
  std::unique_ptr<FSRandomAccessFile> random_file;
  ios = fs_->NewRandomAccessFile(path, FileOptions(), &random_file, nullptr);
  if (!ios.ok()) {
    return ios;
  }
  std::unique_ptr<RandomAccessFileReader> random_reader =
      std::make_unique<RandomAccessFileReader>(std::move(random_file), path);

  // Create the partitioned log reader
  auto new_reader = std::make_unique<log::PartitionedLogReader>(
      std::move(seq_reader), std::move(random_reader), partition_wal_number);

  *reader = new_reader.get();
  readers_[partition_wal_number] = std::move(new_reader);

  return Status::OK();
}

std::string PartitionedWALRecovery::GetPartitionedWALPath(
    uint64_t partition_wal_number) const {
  // Decode the encoded partition_wal_number
  // Encoding: partition_wal_number = (log_number << 8) | partition_id
  // This allows up to 256 partitions per log
  uint64_t log_number = log::PartitionedLogWriter::WriteResult::DecodeWalNumber(
      partition_wal_number);
  uint32_t partition_id =
      log::PartitionedLogWriter::WriteResult::DecodePartitionId(
          partition_wal_number);

  return PartitionedWALManager::PartitionFileName(wal_dir_, log_number,
                                                  partition_id);
}

}  // namespace ROCKSDB_NAMESPACE
