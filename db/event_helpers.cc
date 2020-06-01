//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/event_helpers.h"

namespace ROCKSDB_NAMESPACE {

namespace {
template <class T>
inline T SafeDivide(T a, T b) {
  return b == 0 ? 0 : a / b;
}
}  // namespace

void EventHelpers::AppendCurrentTime(JSONWriter* jwriter) {
  *jwriter << "time_micros"
           << std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::system_clock::now().time_since_epoch())
                  .count();
}

#ifndef ROCKSDB_LITE
void EventHelpers::NotifyTableFileCreationStarted(
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    const std::string& db_name, const std::string& cf_name,
    const std::string& file_path, int job_id, TableFileCreationReason reason) {
  TableFileCreationBriefInfo info;
  info.db_name = db_name;
  info.cf_name = cf_name;
  info.file_path = file_path;
  info.job_id = job_id;
  info.reason = reason;
  for (auto& listener : listeners) {
    listener->OnTableFileCreationStarted(info);
  }
}
#endif  // !ROCKSDB_LITE

void EventHelpers::NotifyOnBackgroundError(
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    BackgroundErrorReason reason, Status* bg_error, InstrumentedMutex* db_mutex,
    bool* auto_recovery) {
#ifndef ROCKSDB_LITE
  if (listeners.size() == 0U) {
    return;
  }
  db_mutex->AssertHeld();
  // release lock while notifying events
  db_mutex->Unlock();
  for (auto& listener : listeners) {
    listener->OnBackgroundError(reason, bg_error);
    if (*auto_recovery) {
      listener->OnErrorRecoveryBegin(reason, *bg_error, auto_recovery);
    }
  }
  db_mutex->Lock();
#else
  (void)listeners;
  (void)reason;
  (void)bg_error;
  (void)db_mutex;
  (void)auto_recovery;
#endif  // ROCKSDB_LITE
}

void EventHelpers::LogAndNotifyTableFileCreationFinished(
    EventLogger* event_logger,
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    const std::string& db_name, const std::string& cf_name,
    const std::string& file_path, int job_id, const FileDescriptor& fd,
    uint64_t oldest_blob_file_number, const TableProperties& table_properties,
    TableFileCreationReason reason, const Status& s) {
  if (s.ok() && event_logger) {
    JSONWriter jwriter;
    AppendCurrentTime(&jwriter);
    jwriter << "cf_name" << cf_name << "job" << job_id << "event"
            << "table_file_creation"
            << "file_number" << fd.GetNumber() << "file_size"
            << fd.GetFileSize();

    // table_properties
    {
      jwriter << "table_properties";
      jwriter.StartObject();

      // basic properties:
      jwriter << "data_size" << table_properties.data_size << "index_size"
              << table_properties.index_size << "index_partitions"
              << table_properties.index_partitions << "top_level_index_size"
              << table_properties.top_level_index_size
              << "index_key_is_user_key"
              << table_properties.index_key_is_user_key
              << "index_value_is_delta_encoded"
              << table_properties.index_value_is_delta_encoded << "filter_size"
              << table_properties.filter_size << "raw_key_size"
              << table_properties.raw_key_size << "raw_average_key_size"
              << SafeDivide(table_properties.raw_key_size,
                            table_properties.num_entries)
              << "raw_value_size" << table_properties.raw_value_size
              << "raw_average_value_size"
              << SafeDivide(table_properties.raw_value_size,
                            table_properties.num_entries)
              << "num_data_blocks" << table_properties.num_data_blocks
              << "num_entries" << table_properties.num_entries
              << "num_deletions" << table_properties.num_deletions
              << "num_merge_operands" << table_properties.num_merge_operands
              << "num_range_deletions" << table_properties.num_range_deletions
              << "format_version" << table_properties.format_version
              << "fixed_key_len" << table_properties.fixed_key_len
              << "filter_policy" << table_properties.filter_policy_name
              << "column_family_name" << table_properties.column_family_name
              << "column_family_id" << table_properties.column_family_id
              << "comparator" << table_properties.comparator_name
              << "merge_operator" << table_properties.merge_operator_name
              << "prefix_extractor_name"
              << table_properties.prefix_extractor_name << "property_collectors"
              << table_properties.property_collectors_names << "compression"
              << table_properties.compression_name << "compression_options"
              << table_properties.compression_options << "creation_time"
              << table_properties.creation_time << "oldest_key_time"
              << table_properties.oldest_key_time << "file_creation_time"
              << table_properties.file_creation_time;

      // user collected properties
      for (const auto& prop : table_properties.readable_properties) {
        jwriter << prop.first << prop.second;
      }
      jwriter.EndObject();
    }

    if (oldest_blob_file_number != kInvalidBlobFileNumber) {
      jwriter << "oldest_blob_file_number" << oldest_blob_file_number;
    }

    jwriter.EndObject();

    event_logger->Log(jwriter);
  }

#ifndef ROCKSDB_LITE
  if (listeners.size() == 0) {
    return;
  }
  TableFileCreationInfo info;
  info.db_name = db_name;
  info.cf_name = cf_name;
  info.file_path = file_path;
  info.file_size = fd.file_size;
  info.job_id = job_id;
  info.table_properties = table_properties;
  info.reason = reason;
  info.status = s;
  for (auto& listener : listeners) {
    listener->OnTableFileCreated(info);
  }
#else
  (void)listeners;
  (void)db_name;
  (void)cf_name;
  (void)file_path;
  (void)reason;
#endif  // !ROCKSDB_LITE
}

void EventHelpers::LogAndNotifyTableFileDeletion(
    EventLogger* event_logger, int job_id, uint64_t file_number,
    const std::string& file_path, const Status& status,
    const std::string& dbname,
    const std::vector<std::shared_ptr<EventListener>>& listeners) {
  JSONWriter jwriter;
  AppendCurrentTime(&jwriter);

  jwriter << "job" << job_id << "event"
          << "table_file_deletion"
          << "file_number" << file_number;
  if (!status.ok()) {
    jwriter << "status" << status.ToString();
  }

  jwriter.EndObject();

  event_logger->Log(jwriter);

#ifndef ROCKSDB_LITE
  TableFileDeletionInfo info;
  info.db_name = dbname;
  info.job_id = job_id;
  info.file_path = file_path;
  info.status = status;
  for (auto& listener : listeners) {
    listener->OnTableFileDeleted(info);
  }
#else
  (void)file_path;
  (void)dbname;
  (void)listeners;
#endif  // !ROCKSDB_LITE
}

void EventHelpers::NotifyOnErrorRecoveryCompleted(
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    Status old_bg_error, InstrumentedMutex* db_mutex) {
#ifndef ROCKSDB_LITE
  if (listeners.size() == 0U) {
    return;
  }
  db_mutex->AssertHeld();
  // release lock while notifying events
  db_mutex->Unlock();
  for (auto& listener : listeners) {
    listener->OnErrorRecoveryCompleted(old_bg_error);
  }
  db_mutex->Lock();
#else
  (void)listeners;
  (void)old_bg_error;
  (void)db_mutex;
#endif  // ROCKSDB_LITE
}

}  // namespace ROCKSDB_NAMESPACE
