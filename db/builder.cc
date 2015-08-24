//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/internal_stats.h"
#include "db/merge_helper.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based_table_builder.h"
#include "util/file_reader_writer.h"
#include "util/iostats_context_imp.h"
#include "util/stop_watch.h"
#include "util/thread_status_util.h"

namespace rocksdb {

namespace {
inline SequenceNumber EarliestVisibleSnapshot(
    SequenceNumber in, const std::vector<SequenceNumber>& snapshots,
    SequenceNumber* prev_snapshot) {
  if (snapshots.empty()) {
    *prev_snapshot = 0;  // 0 means no previous snapshot
    return kMaxSequenceNumber;
  }
  SequenceNumber prev = 0;
  for (const auto cur : snapshots) {
    assert(prev <= cur);
    if (cur >= in) {
      *prev_snapshot = prev;
      return cur;
    }
    prev = cur;  // assignment
  }
  *prev_snapshot = prev;
  return kMaxSequenceNumber;
}
}  // namespace

class TableFactory;

TableBuilder* NewTableBuilder(
    const ImmutableCFOptions& ioptions,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    WritableFileWriter* file, const CompressionType compression_type,
    const CompressionOptions& compression_opts, const bool skip_filters) {
  return ioptions.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, internal_comparator,
                          int_tbl_prop_collector_factories, compression_type,
                          compression_opts, skip_filters),
      file);
}

Status BuildTable(
    const std::string& dbname, Env* env, const ImmutableCFOptions& ioptions,
    const EnvOptions& env_options, TableCache* table_cache, Iterator* iter,
    FileMetaData* meta, const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    std::vector<SequenceNumber> snapshots, const CompressionType compression,
    const CompressionOptions& compression_opts, bool paranoid_file_checks,
    InternalStats* internal_stats, const Env::IOPriority io_priority,
    TableProperties* table_properties) {
  // Reports the IOStats for flush for every following bytes.
  const size_t kReportFlushIOStatsEvery = 1048576;
  Status s;
  meta->fd.file_size = 0;
  meta->smallest_seqno = meta->largest_seqno = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(ioptions.db_paths, meta->fd.GetNumber(),
                                    meta->fd.GetPathId());
  if (iter->Valid()) {
    TableBuilder* builder;
    unique_ptr<WritableFileWriter> file_writer;
    {
      unique_ptr<WritableFile> file;
      s = env->NewWritableFile(fname, &file, env_options);
      if (!s.ok()) {
        return s;
      }
      file->SetIOPriority(io_priority);

      file_writer.reset(new WritableFileWriter(std::move(file), env_options));

      builder = NewTableBuilder(
          ioptions, internal_comparator, int_tbl_prop_collector_factories,
          file_writer.get(), compression, compression_opts);
    }

    {
      // the first key is the smallest key
      Slice key = iter->key();
      meta->smallest.DecodeFrom(key);
      meta->smallest_seqno = GetInternalKeySeqno(key);
      meta->largest_seqno = meta->smallest_seqno;
    }

    MergeHelper merge(internal_comparator.user_comparator(),
                      ioptions.merge_operator, ioptions.info_log,
                      ioptions.min_partial_merge_operands,
                      true /* internal key corruption is not ok */);

    IterKey current_user_key;
    bool has_current_user_key = false;
    // If has_current_user_key == true, this variable remembers the earliest
    // snapshot in which this current key already exists. If two internal keys
    // have the same user key AND the earlier one should be visible in the
    // snapshot in which we already have a user key, we can drop the earlier
    // user key
    SequenceNumber current_user_key_exists_in_snapshot = kMaxSequenceNumber;

    while (iter->Valid()) {
      // Get current key
      ParsedInternalKey ikey;
      Slice key = iter->key();
      Slice value = iter->value();

      // In-memory key corruption is not ok;
      // TODO: find a clean way to treat in memory key corruption
      // Ugly walkaround to avoid compiler error for release build
      bool ok __attribute__((unused)) = true;
      ok = ParseInternalKey(key, &ikey);
      assert(ok);

      meta->smallest_seqno = std::min(meta->smallest_seqno, ikey.sequence);
      meta->largest_seqno = std::max(meta->largest_seqno, ikey.sequence);

      // If the key is the same as the previous key (and it is not the
      // first key), then we skip it, since it is an older version.
      // Otherwise we output the key and mark it as the "new" previous key.
      if (!has_current_user_key ||
          internal_comparator.user_comparator()->Compare(
              ikey.user_key, current_user_key.GetKey()) != 0) {
        // First occurrence of this user key
        current_user_key.SetKey(ikey.user_key);
        has_current_user_key = true;
        current_user_key_exists_in_snapshot = 0;
      }

      // If there are no snapshots, then this kv affect visibility at tip.
      // Otherwise, search though all existing snapshots to find
      // the earlist snapshot that is affected by this kv.
      SequenceNumber prev_snapshot = 0;  // 0 means no previous snapshot
      SequenceNumber key_needs_to_exist_in_snapshot =
          EarliestVisibleSnapshot(ikey.sequence, snapshots, &prev_snapshot);

      if (current_user_key_exists_in_snapshot ==
          key_needs_to_exist_in_snapshot) {
        // If this user key already exists in snapshot in which it needs to
        // exist, we can drop it.
        // In other words, if the earliest snapshot is which this key is visible
        // in is the same as the visibily of a previous instance of the
        // same key, then this kv is not visible in any snapshot.
        // Hidden by an newer entry for same user key
        iter->Next();
      } else if (ikey.type == kTypeMerge) {
        meta->largest.DecodeFrom(key);

        // TODO(tbd): Add a check here to prevent RocksDB from crash when
        // reopening a DB w/o properly specifying the merge operator.  But
        // currently we observed a memory leak on failing in RocksDB
        // recovery, so we decide to let it crash instead of causing
        // memory leak for now before we have identified the real cause
        // of the memory leak.

        // Handle merge-type keys using the MergeHelper
        // TODO: pass statistics to MergeUntil
        merge.MergeUntil(iter, prev_snapshot, false, nullptr, env);
        // IMPORTANT: Slice key doesn't point to a valid value anymore!!

        const auto& keys = merge.keys();
        const auto& values = merge.values();
        assert(!keys.empty());
        assert(keys.size() == values.size());

        // largest possible sequence number in a merge queue is already stored
        // in ikey.sequence.
        // we additionally have to consider the front of the merge queue, which
        // might have the smallest sequence number (out of all the merges with
        // the same key)
        meta->smallest_seqno =
            std::min(meta->smallest_seqno, GetInternalKeySeqno(keys.front()));

        // We have a list of keys to write, write all keys in the list.
        for (auto key_iter = keys.rbegin(), value_iter = values.rbegin();
             key_iter != keys.rend(); key_iter++, value_iter++) {
          key = Slice(*key_iter);
          value = Slice(*value_iter);
          bool valid_key __attribute__((__unused__)) =
              ParseInternalKey(key, &ikey);
          // MergeUntil stops when it encounters a corrupt key and does not
          // include them in the result, so we expect the keys here to valid.
          assert(valid_key);
          builder->Add(key, value);
        }
      } else {  // just write out the key-value
        builder->Add(key, value);
        meta->largest.DecodeFrom(key);
        iter->Next();
      }

      current_user_key_exists_in_snapshot = key_needs_to_exist_in_snapshot;

      if (io_priority == Env::IO_HIGH &&
          IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
        ThreadStatusUtil::IncreaseThreadOperationProperty(
            ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
        IOSTATS_RESET(bytes_written);
      }
    }

    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();
    } else {
      builder->Abandon();
    }
    if (s.ok()) {
      meta->fd.file_size = builder->FileSize();
      meta->marked_for_compaction = builder->NeedCompact();
      assert(meta->fd.GetFileSize() > 0);
      if (table_properties) {
        *table_properties = builder->GetTableProperties();
      }
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok() && !ioptions.disable_data_sync) {
      StopWatch sw(env, ioptions.statistics, TABLE_SYNC_MICROS);
      file_writer->Sync(ioptions.use_fsync);
    }
    if (s.ok()) {
      s = file_writer->Close();
    }

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(
          ReadOptions(), env_options, internal_comparator, meta->fd, nullptr,
          (internal_stats == nullptr) ? nullptr
                                      : internal_stats->GetFileReadHist(0),
          false);
      s = it->status();
      if (s.ok() && paranoid_file_checks) {
        for (it->SeekToFirst(); it->Valid(); it->Next()) {}
        s = it->status();
      }

      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->fd.GetFileSize() > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace rocksdb
