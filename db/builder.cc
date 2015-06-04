//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <vector>
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/merge_helper.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based_table_builder.h"
#include "util/iostats_context_imp.h"
#include "util/thread_status_util.h"
#include "util/stop_watch.h"

namespace rocksdb {

class TableFactory;

TableBuilder* NewTableBuilder(
    const ImmutableCFOptions& ioptions,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    WritableFile* file, const CompressionType compression_type,
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
    const SequenceNumber newest_snapshot,
    const SequenceNumber earliest_seqno_in_memtable,
    const CompressionType compression,
    const CompressionOptions& compression_opts, bool paranoid_file_checks,
    const Env::IOPriority io_priority, TableProperties* table_properties) {
  // Reports the IOStats for flush for every following bytes.
  const size_t kReportFlushIOStatsEvery = 1048576;
  Status s;
  meta->fd.file_size = 0;
  meta->smallest_seqno = meta->largest_seqno = 0;
  iter->SeekToFirst();

  // If the sequence number of the smallest entry in the memtable is
  // smaller than the most recent snapshot, then we do not trigger
  // removal of duplicate/deleted keys as part of this builder.
  bool purge = ioptions.purge_redundant_kvs_while_flush;
  if (earliest_seqno_in_memtable <= newest_snapshot) {
    purge = false;
  }

  std::string fname = TableFileName(ioptions.db_paths, meta->fd.GetNumber(),
                                    meta->fd.GetPathId());
  if (iter->Valid()) {
    unique_ptr<WritableFile> file;
    s = env->NewWritableFile(fname, &file, env_options);
    if (!s.ok()) {
      return s;
    }
    file->SetIOPriority(io_priority);

    TableBuilder* builder = NewTableBuilder(
        ioptions, internal_comparator, int_tbl_prop_collector_factories,
        file.get(), compression, compression_opts);

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

    if (purge) {
      // Ugly walkaround to avoid compiler error for release build
      bool ok __attribute__((unused)) = true;

      // Will write to builder if current key != prev key
      ParsedInternalKey prev_ikey;
      std::string prev_key;
      bool is_first_key = true;    // Also write if this is the very first key

      while (iter->Valid()) {
        bool iterator_at_next = false;

        // Get current key
        ParsedInternalKey this_ikey;
        Slice key = iter->key();
        Slice value = iter->value();

        // In-memory key corruption is not ok;
        // TODO: find a clean way to treat in memory key corruption
        ok = ParseInternalKey(key, &this_ikey);
        assert(ok);
        assert(this_ikey.sequence >= earliest_seqno_in_memtable);

        // If the key is the same as the previous key (and it is not the
        // first key), then we skip it, since it is an older version.
        // Otherwise we output the key and mark it as the "new" previous key.
        if (!is_first_key && !internal_comparator.user_comparator()->Compare(
                                  prev_ikey.user_key, this_ikey.user_key)) {
          // seqno within the same key are in decreasing order
          assert(this_ikey.sequence < prev_ikey.sequence);
        } else {
          is_first_key = false;

          if (this_ikey.type == kTypeMerge) {
            // TODO(tbd): Add a check here to prevent RocksDB from crash when
            // reopening a DB w/o properly specifying the merge operator.  But
            // currently we observed a memory leak on failing in RocksDB
            // recovery, so we decide to let it crash instead of causing
            // memory leak for now before we have identified the real cause
            // of the memory leak.

            // Handle merge-type keys using the MergeHelper
            // TODO: pass statistics to MergeUntil
            merge.MergeUntil(iter, 0 /* don't worry about snapshot */);
            iterator_at_next = true;
            if (merge.IsSuccess()) {
              // Merge completed correctly.
              // Add the resulting merge key/value and continue to next
              builder->Add(merge.key(), merge.value());
              prev_key.assign(merge.key().data(), merge.key().size());
              ok = ParseInternalKey(Slice(prev_key), &prev_ikey);
              assert(ok);
            } else {
              // Merge did not find a Put/Delete.
              // Can not compact these merges into a kValueType.
              // Write them out one-by-one. (Proceed back() to front())
              const std::deque<std::string>& keys = merge.keys();
              const std::deque<std::string>& values = merge.values();
              assert(keys.size() == values.size() && keys.size() >= 1);
              std::deque<std::string>::const_reverse_iterator key_iter;
              std::deque<std::string>::const_reverse_iterator value_iter;
              for (key_iter=keys.rbegin(), value_iter = values.rbegin();
                   key_iter != keys.rend() && value_iter != values.rend();
                   ++key_iter, ++value_iter) {

                builder->Add(Slice(*key_iter), Slice(*value_iter));
              }

              // Sanity check. Both iterators should end at the same time
              assert(key_iter == keys.rend() && value_iter == values.rend());

              prev_key.assign(keys.front());
              ok = ParseInternalKey(Slice(prev_key), &prev_ikey);
              assert(ok);
            }
          } else {
            // Handle Put/Delete-type keys by simply writing them
            builder->Add(key, value);
            prev_key.assign(key.data(), key.size());
            ok = ParseInternalKey(Slice(prev_key), &prev_ikey);
            assert(ok);
          }
        }

        if (io_priority == Env::IO_HIGH &&
            IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
          ThreadStatusUtil::IncreaseThreadOperationProperty(
              ThreadStatus::FLUSH_BYTES_WRITTEN,
              IOSTATS(bytes_written));
          IOSTATS_RESET(bytes_written);
        }
        if (!iterator_at_next) iter->Next();
      }

      // The last key is the largest key
      meta->largest.DecodeFrom(Slice(prev_key));
      SequenceNumber seqno = GetInternalKeySeqno(Slice(prev_key));
      meta->smallest_seqno = std::min(meta->smallest_seqno, seqno);
      meta->largest_seqno = std::max(meta->largest_seqno, seqno);

    } else {
      for (; iter->Valid(); iter->Next()) {
        Slice key = iter->key();
        meta->largest.DecodeFrom(key);
        builder->Add(key, iter->value());
        SequenceNumber seqno = GetInternalKeySeqno(key);
        meta->smallest_seqno = std::min(meta->smallest_seqno, seqno);
        meta->largest_seqno = std::max(meta->largest_seqno, seqno);
        if (io_priority == Env::IO_HIGH &&
            IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
          ThreadStatusUtil::IncreaseThreadOperationProperty(
              ThreadStatus::FLUSH_BYTES_WRITTEN,
              IOSTATS(bytes_written));
          IOSTATS_RESET(bytes_written);
        }
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
      if (ioptions.use_fsync) {
        StopWatch sw(env, ioptions.statistics, TABLE_SYNC_MICROS);
        s = file->Fsync();
      } else {
        StopWatch sw(env, ioptions.statistics, TABLE_SYNC_MICROS);
        s = file->Sync();
      }
    }
    if (s.ok()) {
      s = file->Close();
    }

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), env_options,
                                              internal_comparator, meta->fd);
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
