#pragma once

#include <string>

#include "db/memtable.h"
#include "rocksdb/options.h"
#include "util/autovector.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// A record corresponds to `kMemtableSwitch` event
struct MemTableSwitchRecord {
  // next_log_num for the switched memtables. All CFDs flushed atomically will
  // share same next_log_num. next_log_num is used to determine whether a
  // memtable is flushed and can be removed
  uint64_t next_log_num;
};

Status SerializeMemTableSwitchRecord(
    std::string* dst,
    const MemTableSwitchRecord &record);
Status DeserializeMemTableSwitchRecord(
    Slice* src,
    MemTableSwitchRecord* record);

// Record `kMemtableSwitch` event, also initializes `mem_switch_record`
//
// @return replication_sequence for the log record
//
// NOTE: this function has to be called before corresponding `kManifestWrite`.
// We rely on this assumption during recovery based on Manifest and repliation
// log
std::string RecordMemTableSwitch(
    const std::shared_ptr<rocksdb::ReplicationLogListener>&
        replication_log_listener,
    const MemTableSwitchRecord& mem_switch_record);
}  // namespace ROCKSDB_NAMESPACE
