//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/version_edit.h"
#include "rocksdb/listener.h"
#include "rocksdb/table_properties.h"
#include "util/event_logger.h"

namespace rocksdb {

class EventHelpers {
 public:
  static void AppendCurrentTime(JSONWriter* json_writer);
#ifndef ROCKSDB_LITE
  static void NotifyTableFileCreationStarted(
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const std::string& db_name, const std::string& cf_name,
      const std::string& file_path, int job_id, TableFileCreationReason reason);
#endif  // !ROCKSDB_LITE
  static void LogAndNotifyTableFileCreationFinished(
      EventLogger* event_logger,
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const std::string& db_name, const std::string& cf_name,
      const std::string& file_path, int job_id, const FileDescriptor& fd,
      const TableProperties& table_properties, TableFileCreationReason reason,
      const Status& s);
  static void LogAndNotifyTableFileDeletion(
      EventLogger* event_logger, int job_id,
      uint64_t file_number, const std::string& file_path,
      const Status& status, const std::string& db_name,
      const std::vector<std::shared_ptr<EventListener>>& listeners);

 private:
  static void LogAndNotifyTableFileCreation(
      EventLogger* event_logger,
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const FileDescriptor& fd, const TableFileCreationInfo& info);
};

}  // namespace rocksdb
