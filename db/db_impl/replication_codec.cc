#include "db/db_impl/replication_codec.h"
#include "db/memtable.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

Status SerializeMemTableSwitchRecord(std::string* dst, const MemTableSwitchRecord &record) {
  PutVarint64(dst, record.next_log_num);
  return Status::OK();
}
Status DeserializeMemTableSwitchRecord(Slice* src, MemTableSwitchRecord* record) {
  uint64_t next_log_num;
  if (!GetVarint64(src, &next_log_num)) {
    return Status::Corruption("Unable to decode memtable switch next_log_num");
  }

  record->next_log_num = next_log_num;
  return Status::OK();
}

std::string RecordMemTableSwitch(
    const std::shared_ptr<rocksdb::ReplicationLogListener>&
        replication_log_listener,
    const MemTableSwitchRecord& record) {
  ReplicationLogRecord rlr;
  rlr.type = ReplicationLogRecord::kMemtableSwitch;
  SerializeMemTableSwitchRecord(&rlr.contents, record);
  return replication_log_listener->OnReplicationLogRecord(std::move(rlr));
}
}
