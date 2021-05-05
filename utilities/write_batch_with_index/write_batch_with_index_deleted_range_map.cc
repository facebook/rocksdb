#ifndef ROCKSDB_LITE

#include "utilities/write_batch_with_index/write_batch_with_index_deleted_range_map.h"

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/string_util.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

namespace ROCKSDB_NAMESPACE {

void DeletedRangeMap::AddInterval(const uint32_t cf_id, const Slice& from_key,
                                  const Slice& to_key) {
  const auto batch_data = indexed_write_batch->Data().data();
  assert(from_key.data() + from_key.size() <=
         batch_data + indexed_write_batch->GetDataSize());
  assert(from_key.data() >= batch_data);
  const WriteBatchIndexEntry from(0, cf_id, from_key.data() - batch_data,
                                  from_key.size());

  assert(to_key.data() + to_key.size() <=
         batch_data + indexed_write_batch->GetDataSize());
  assert(to_key.data() >= batch_data);
  const WriteBatchIndexEntry to(0, cf_id, to_key.data() - batch_data,
                                to_key.size());

  IntervalMap::AddInterval(from, to);
}

bool DeletedRangeMap::IsInInterval(const uint32_t cf_id, const Slice& key) {
  const WriteBatchIndexEntry search_entry(
      &key, cf_id, true,
      false);  // true:forward, false:seek to first
  return IntervalMap::IsInInterval(search_entry);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
