//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "db/blob/blob_file_builder.h"
#include "db/compaction/compaction_iterator.h"
#include "db/db_impl/db_impl.h"
#include "db/event_helpers.h"
#include "db/internal_stats.h"
#include "db/merge_helper.h"
#include "db/output_validator.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

class TableFactory;

TableBuilder* NewTableBuilder(const TableBuilderOptions& tboptions,
                              WritableFileWriter* file) {
  assert((tboptions.column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         tboptions.column_family_name.empty());
  return tboptions.ioptions.table_factory->NewTableBuilder(tboptions, file);
}

// Add the given key and value to output table through `builder`.
// Key and value are validated through output validator first.
// File metadata and thread status are updated accordingly.
Status AddKeyValueToOutput(OutputValidator& output_validator, const Slice& key,
                           const Slice& value, TableBuilder* builder,
                           FileMetaData* meta,
                           const Env::IOPriority& io_priority,
                           const SequenceNumber& seq, const ValueType& type) {
  // Reports the IOStats for flush for every following bytes.
  static const size_t kReportFlushIOStatsEvery = 1048576;
  Status s;
  s = output_validator.Add(key, value);
  if (!s.ok()) {
    return s;
  }
  builder->Add(key, value);

  s = meta->UpdateBoundaries(key, value, seq, type);
  if (!s.ok()) {
    return s;
  }

  // TODO(noetzli): Update stats after flush, too.
  if (io_priority == Env::IO_HIGH &&
      IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
    ThreadStatusUtil::SetThreadOperationProperty(
        ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
  }

  return s;
}

// Try to convert point tombstones in `tombstones` to a range tombstone.
// For example, given point tombstones 1, 3, 4, this function tries
// to convert them into a single range tombstone [1, 4) and a point
// tombstone 4 (since end key is exclusive). Make sure that we do not
// accidentally delete key 2 from any older level by doing a DBIter seek
// on key 1, and check if DBIter is at a key > 4. If DBIter seek lands on a key
// > 4, say 9. We advance `c_iter` to look for more point tombstones until 9.
// `at_next_key` is set to true if `c_iter` points to a key that
// should be processed when this function returns.
//
// If we decide to convert point tombstones, the new range tombstone will have
// the sequence number that is the max among all point tombstones. Any point
// tombstone that is visible to a snapshot smaller than the range tombstone's
// seq will be emitted to the output table. Range tombstone information will be
// recorded in `start_keys`, `end_keys` and `seqs`.
//
// If we decide not to convert, all point tombstones in `tombstones` are added
// to output table.
//
// Assumes the value for point tombstones is empty string.
Status TryConvertPointToRangeTombstone(
    std::vector<std::string>& tombstones, ColumnFamilyData* cfd,
    std::vector<std::string>& start_keys, std::vector<std::string>& end_keys,
    std::vector<SequenceNumber>& seqs,
    const std::vector<SequenceNumber>& snapshots,
    OutputValidator& output_validator, TableBuilder* builder,
    FileMetaData* meta, const Env::IOPriority& io_priority,
    std::unique_ptr<Iterator>& db_iter, std::string& last_seek_result,
    bool& db_iter_reached_end, CompactionIterator& c_iter, bool& at_next_key) {
  Status s;
  const auto ucmp = cfd->user_comparator();
  Slice front_user_key = ExtractUserKey(tombstones.front());
  // last seek result is too staledP
  if (!db_iter_reached_end &&
      (last_seek_result.empty() ||
       ucmp->Compare(last_seek_result, front_user_key) < 0)) {
    db_iter->Seek(front_user_key);
    if (!db_iter->status().ok()) {
      return db_iter->status();
    }
    if (db_iter->Valid()) {
      last_seek_result.assign(db_iter->key().data(), db_iter->key().size());
    } else {
      db_iter_reached_end = true;
    }
  }
  // either we've reached end of DBIter, or that last_seek_result is useful: it
  // is after current tombstone start
  assert(db_iter_reached_end ||
         ucmp->Compare(last_seek_result, front_user_key) >= 0);
  Slice back_user_key = ExtractUserKey(tombstones.back());
  if (db_iter_reached_end ||
      ucmp->Compare(last_seek_result, back_user_key) > 0) {
    // There is no keys from older level that is logically visble in range
    // covered by `tombstones`
    SequenceNumber max_seq = 0;
    for (auto& k : tombstones) {
      max_seq = std::max(max_seq, GetInternalKeySeqno(k));
    }
    // Try to extend to seek_result as much as possible
    at_next_key = true;
    c_iter.Next();
    while (c_iter.Valid()) {
      const ParsedInternalKey& ikey = c_iter.ikey();
      if ((ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion) &&
          (db_iter_reached_end ||
           ucmp->Compare(last_seek_result, ikey.user_key) > 0)) {
        Slice k = c_iter.key();
        tombstones.emplace_back(k.data(), k.size());
        max_seq = std::max(max_seq, ikey.sequence);
      } else {
        break;
      }
      c_iter.Next();
    }
    // content of `tombstones` changed, need to reinit `front_user_key`
    front_user_key = ExtractUserKey(tombstones.front());
    back_user_key = ExtractUserKey(tombstones.back());
    // Convert to range tombstone
    start_keys.emplace_back(front_user_key.data(), front_user_key.size());
    end_keys.emplace_back(back_user_key.data(), back_user_key.size());
    seqs.emplace_back(max_seq);
    // Find the largest snapshot less than max_seq given that `snapshots` is
    // ascending. Emit tombstones that are visible to some snapshot.
    auto snapshot = std::upper_bound(snapshots.rbegin(), snapshots.rend(),
                                     max_seq, std::greater<SequenceNumber>());
    if (snapshot != snapshots.rend()) {
      for (size_t i = 0; i < tombstones.size() - 1; ++i) {
        auto& k = tombstones[i];
        auto k_seq = GetInternalKeySeqno(k);
        // TODO: maybe use ParsedInternalKey for tombstones given that we parse
        //  max_seq and op_type sometimes.
        if (*snapshot >= k_seq) {
          s = AddKeyValueToOutput(output_validator, k, "" /* value */, builder,
                                  meta, io_priority, k_seq,
                                  ExtractValueType(k));
          if (!s.ok()) {
            return s;
          }
        }
      }
    }
    // Add the last tombstone to output since range tombstone end key is
    // exclusive
    s = AddKeyValueToOutput(output_validator, tombstones.back(), "" /* value */,
                            builder, meta, io_priority,
                            GetInternalKeySeqno(tombstones.back()),
                            ExtractValueType(tombstones.back()));
  } else {
    // TODO: it is not necessary to flush all tombstones here. Say we cannot
    //  convert 1, 3 to [1, 3) due to lower level having point key 2. We can
    //  just flush point tombstone 1 here, and continue buffering tombstones
    //  starting from 3.
    for (auto& k : tombstones) {
      s = AddKeyValueToOutput(output_validator, k, "" /* value */, builder,
                              meta, io_priority, GetInternalKeySeqno(k),
                              ExtractValueType(k));
      if (!s.ok()) {
        return s;
      }
    }
  }
  return s;
}

Status BuildTable(
    const std::string& dbname, VersionSet* versions,
    const ImmutableDBOptions& db_options, const TableBuilderOptions& tboptions,
    const FileOptions& file_options, TableCache* table_cache,
    InternalIterator* iter,
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
        range_del_iters,
    FileMetaData* meta, std::vector<BlobFileAddition>* blob_file_additions,
    std::vector<SequenceNumber> snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SequenceNumber job_snapshot, SnapshotChecker* snapshot_checker,
    bool paranoid_file_checks, InternalStats* internal_stats,
    IOStatus* io_status, const std::shared_ptr<IOTracer>& io_tracer,
    BlobFileCreationReason blob_creation_reason,
    const SeqnoToTimeMapping& seqno_to_time_mapping, EventLogger* event_logger,
    int job_id, const Env::IOPriority io_priority,
    TableProperties* table_properties, Env::WriteLifeTimeHint write_hint,
    const std::string* full_history_ts_low,
    BlobFileCompletionCallback* blob_callback, DBImpl* db,
    ColumnFamilyData* cfd, autovector<MemTable*>* mems,
    uint64_t* num_input_entries, uint64_t* memtable_payload_bytes,
    uint64_t* memtable_garbage_bytes) {
  TEST_SYNC_POINT_CALLBACK("BuildTable:Start", nullptr);
  assert((tboptions.column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         tboptions.column_family_name.empty());
  auto& mutable_cf_options = tboptions.moptions;
  auto& ioptions = tboptions.ioptions;
  OutputValidator output_validator(
      tboptions.internal_comparator,
      /*enable_order_check=*/
      mutable_cf_options.check_flush_compaction_key_order,
      /*enable_hash=*/paranoid_file_checks);
  Status s;
  meta->fd.file_size = 0;
  iter->SeekToFirst();
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&tboptions.internal_comparator,
                                       snapshots, full_history_ts_low));
  uint64_t num_unfragmented_tombstones = 0;
  uint64_t total_tombstone_payload_bytes = 0;
  for (auto& range_del_iter : range_del_iters) {
    num_unfragmented_tombstones +=
        range_del_iter->num_unfragmented_tombstones();
    total_tombstone_payload_bytes +=
        range_del_iter->total_tombstone_payload_bytes();
    range_del_agg->AddTombstones(std::move(range_del_iter));
  }

  std::string fname = TableFileName(ioptions.cf_paths, meta->fd.GetNumber(),
                                    meta->fd.GetPathId());
  std::vector<std::string> blob_file_paths;
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(ioptions.listeners, dbname,
                                               tboptions.column_family_name,
                                               fname, job_id, tboptions.reason);
#endif  // !ROCKSDB_LITE
  Env* env = db_options.env;
  assert(env);
  FileSystem* fs = db_options.fs.get();
  assert(fs);

  TableProperties tp;
  bool table_file_created = false;
  if (iter->Valid() || !range_del_agg->IsEmpty()) {
    std::unique_ptr<CompactionFilter> compaction_filter;
    if (ioptions.compaction_filter_factory != nullptr &&
        ioptions.compaction_filter_factory->ShouldFilterTableFileCreation(
            tboptions.reason)) {
      CompactionFilter::Context context;
      context.is_full_compaction = false;
      context.is_manual_compaction = false;
      context.column_family_id = tboptions.column_family_id;
      context.reason = tboptions.reason;
      compaction_filter =
          ioptions.compaction_filter_factory->CreateCompactionFilter(context);
      if (compaction_filter != nullptr &&
          !compaction_filter->IgnoreSnapshots()) {
        s.PermitUncheckedError();
        return Status::NotSupported(
            "CompactionFilter::IgnoreSnapshots() = false is not supported "
            "anymore.");
      }
    }

    TableBuilder* builder;
    std::unique_ptr<WritableFileWriter> file_writer;
    {
      std::unique_ptr<FSWritableFile> file;
#ifndef NDEBUG
      bool use_direct_writes = file_options.use_direct_writes;
      TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG
      IOStatus io_s = NewWritableFile(fs, fname, &file, file_options);
      assert(s.ok());
      s = io_s;
      if (io_status->ok()) {
        *io_status = io_s;
      }
      if (!s.ok()) {
        EventHelpers::LogAndNotifyTableFileCreationFinished(
            event_logger, ioptions.listeners, dbname,
            tboptions.column_family_name, fname, job_id, meta->fd,
            kInvalidBlobFileNumber, tp, tboptions.reason, s, file_checksum,
            file_checksum_func_name);
        return s;
      }

      table_file_created = true;
      FileTypeSet tmp_set = ioptions.checksum_handoff_file_types;
      file->SetIOPriority(io_priority);
      file->SetWriteLifeTimeHint(write_hint);
      file_writer.reset(new WritableFileWriter(
          std::move(file), fname, file_options, ioptions.clock, io_tracer,
          ioptions.stats, ioptions.listeners,
          ioptions.file_checksum_gen_factory.get(),
          tmp_set.Contains(FileType::kTableFile), false));

      builder = NewTableBuilder(tboptions, file_writer.get());
    }

    MergeHelper merge(
        env, tboptions.internal_comparator.user_comparator(),
        ioptions.merge_operator.get(), compaction_filter.get(), ioptions.logger,
        true /* internal key corruption is not ok */,
        snapshots.empty() ? 0 : snapshots.back(), snapshot_checker);

    std::unique_ptr<BlobFileBuilder> blob_file_builder(
        (mutable_cf_options.enable_blob_files &&
         tboptions.level_at_creation >=
             mutable_cf_options.blob_file_starting_level &&
         blob_file_additions)
            ? new BlobFileBuilder(
                  versions, fs, &ioptions, &mutable_cf_options, &file_options,
                  tboptions.db_id, tboptions.db_session_id, job_id,
                  tboptions.column_family_id, tboptions.column_family_name,
                  io_priority, write_hint, io_tracer, blob_callback,
                  blob_creation_reason, &blob_file_paths, blob_file_additions)
            : nullptr);

    const std::atomic<bool> kManualCompactionCanceledFalse{false};
    CompactionIterator c_iter(
        iter, tboptions.internal_comparator.user_comparator(), &merge,
        kMaxSequenceNumber, &snapshots, earliest_write_conflict_snapshot,
        job_snapshot, snapshot_checker, env,
        ShouldReportDetailedTime(env, ioptions.stats),
        true /* internal key corruption is not ok */, range_del_agg.get(),
        blob_file_builder.get(), ioptions.allow_data_in_errors,
        ioptions.enforce_single_del_contracts,
        /*manual_compaction_canceled=*/kManualCompactionCanceledFalse,
        /*compaction=*/nullptr, compaction_filter.get(),
        /*shutting_down=*/nullptr, db_options.info_log, full_history_ts_low);

    // TODO: reduce copying of tombstones, keys should already be pinned as they
    //  are from memetables. This requires change in CompactionIterator.
    // states for point to range tombstone conversion
    std::vector<std::string> tombstones;
    std::vector<std::string> start_keys;
    std::vector<std::string> end_keys;
    std::vector<SequenceNumber> seqs;
    const uint32_t kConvertThreshold =
        mutable_cf_options.tombstone_conversion_threshold;
    const bool try_convert = kConvertThreshold > 0 && db && cfd;
    std::unique_ptr<Iterator> db_iter{nullptr};
    std::string last_seek_result;
    bool db_iter_reached_end = false;
    if (try_convert) {
      assert(cfd->user_comparator()->timestamp_size() == 0);
      ReadOptions ro;
      ro.total_order_seek = true;
      db_iter.reset(db->NewInternalIterator(ro, cfd, *mems));
    }

    c_iter.SeekToFirst();
    for (; c_iter.Valid(); c_iter.Next()) {
      const Slice& key = c_iter.key();
      const Slice& value = c_iter.value();
      const ParsedInternalKey& ikey = c_iter.ikey();

      if (try_convert) {
        // ignores kTypeDeletionWithTimestamp, not supporting user-defined
        // timestamp for now
        if (ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion) {
          tombstones.emplace_back(key.data(), key.size());
          bool at_next_key = false;
          if (tombstones.size() >= kConvertThreshold) {
            s = TryConvertPointToRangeTombstone(
                tombstones, cfd, start_keys, end_keys, seqs, snapshots,
                output_validator, builder, meta, io_priority, db_iter,
                last_seek_result, db_iter_reached_end, c_iter, at_next_key);
            tombstones.clear();
            if (!s.ok()) {
              break;
            }
          }
          if (at_next_key) {
            if (!c_iter.Valid()) {
              break;
            }
            const ParsedInternalKey& ik = c_iter.ikey();
            const Slice& k = c_iter.key();
            if (ik.type == kTypeDeletion || ik.type == kTypeSingleDeletion) {
              tombstones.emplace_back(k.data(), k.size());
            } else {
              s = AddKeyValueToOutput(output_validator, k, c_iter.value(),
                                      builder, meta, io_priority, ik.sequence,
                                      ik.type);
              if (!s.ok()) {
                break;
              }
            }
            continue;
          } else {
            continue;
          }
        } else {
          for (auto& k : tombstones) {
            s = AddKeyValueToOutput(
                output_validator, k, "" /* value */, builder, meta, io_priority,
                GetInternalKeySeqno(k), ExtractValueType(k));
            if (!s.ok()) {
              break;
            }
          }
          tombstones.clear();
        }
      }

      // Generate a rolling 64-bit hash of the key and values
      // Note :
      // Here "key" integrates 'sequence_number'+'kType'+'user key'.
      s = AddKeyValueToOutput(output_validator, key, value, builder, meta,
                              io_priority, ikey.sequence, ikey.type);
      if (!s.ok()) {
        break;
      }
    }
    if (s.ok() && !tombstones.empty()) {
      // flush the remaining buffered point tombstones
      for (auto& k : tombstones) {
        s = AddKeyValueToOutput(output_validator, k, "", builder, meta,
                                io_priority, GetInternalKeySeqno(k),
                                ExtractValueType(k));
        if (!s.ok()) {
          break;
        }
      }
      tombstones.clear();
    }
    if (!s.ok()) {
      c_iter.status().PermitUncheckedError();
    } else if (!c_iter.status().ok()) {
      s = c_iter.status();
    }

    if (s.ok()) {
      // Add range tombstones converted from point tombstones to output
      FragmentedRangeTombstoneList f(start_keys, end_keys, seqs);
      if (!start_keys.empty()) {
        assert(!f.empty());
        // construct fragmented tombstone list
        range_del_agg->AddTombstones(
            std::make_unique<FragmentedRangeTombstoneIterator>(
                &f, cfd->internal_comparator(), kMaxSequenceNumber));
      }
      if (!range_del_agg->IsEmpty()) {
        auto range_del_it = range_del_agg->NewIterator();
        for (range_del_it->SeekToFirst(); range_del_it->Valid();
             range_del_it->Next()) {
          auto tombstone = range_del_it->Tombstone();
          auto kv = tombstone.Serialize();
          builder->Add(kv.first.Encode(), kv.second);
          meta->UpdateBoundariesForRange(kv.first,  tombstone.SerializeEndKey(),
                                         tombstone.seq_,
                                         tboptions.internal_comparator);
        }
      }
    }

    TEST_SYNC_POINT("BuildTable:BeforeFinishBuildTable");
    const bool empty = builder->IsEmpty();
    if (num_input_entries != nullptr) {
      *num_input_entries =
          c_iter.num_input_entry_scanned() + num_unfragmented_tombstones;
    }
    if (!s.ok() || empty) {
      builder->Abandon();
    } else {
      std::string seqno_time_mapping_str;
      seqno_to_time_mapping.Encode(
          seqno_time_mapping_str, meta->fd.smallest_seqno,
          meta->fd.largest_seqno, meta->file_creation_time);
      builder->SetSeqnoTimeTableProperties(
          seqno_time_mapping_str,
          ioptions.compaction_style == CompactionStyle::kCompactionStyleFIFO
              ? meta->file_creation_time
              : meta->oldest_ancester_time);
      s = builder->Finish();
    }
    if (io_status->ok()) {
      *io_status = builder->io_status();
    }

    if (s.ok() && !empty) {
      uint64_t file_size = builder->FileSize();
      meta->fd.file_size = file_size;
      meta->marked_for_compaction = builder->NeedCompact();
      assert(meta->fd.GetFileSize() > 0);
      tp = builder->GetTableProperties(); // refresh now that builder is finished
      if (memtable_payload_bytes != nullptr &&
          memtable_garbage_bytes != nullptr) {
        const CompactionIterationStats& ci_stats = c_iter.iter_stats();
        uint64_t total_payload_bytes = ci_stats.total_input_raw_key_bytes +
                                       ci_stats.total_input_raw_value_bytes +
                                       total_tombstone_payload_bytes;
        uint64_t total_payload_bytes_written =
            (tp.raw_key_size + tp.raw_value_size);
        // Prevent underflow, which may still happen at this point
        // since we only support inserts, deletes, and deleteRanges.
        if (total_payload_bytes_written <= total_payload_bytes) {
          *memtable_payload_bytes = total_payload_bytes;
          *memtable_garbage_bytes =
              total_payload_bytes - total_payload_bytes_written;
        } else {
          *memtable_payload_bytes = 0;
          *memtable_garbage_bytes = 0;
        }
      }
      if (table_properties) {
        *table_properties = tp;
      }
    }
    delete builder;

    // Finish and check for file errors
    TEST_SYNC_POINT("BuildTable:BeforeSyncTable");
    if (s.ok() && !empty) {
      StopWatch sw(ioptions.clock, ioptions.stats, TABLE_SYNC_MICROS);
      *io_status = file_writer->Sync(ioptions.use_fsync);
    }
    TEST_SYNC_POINT("BuildTable:BeforeCloseTableFile");
    if (s.ok() && io_status->ok() && !empty) {
      *io_status = file_writer->Close();
    }
    if (s.ok() && io_status->ok() && !empty) {
      // Add the checksum information to file metadata.
      meta->file_checksum = file_writer->GetFileChecksum();
      meta->file_checksum_func_name = file_writer->GetFileChecksumFuncName();
      file_checksum = meta->file_checksum;
      file_checksum_func_name = meta->file_checksum_func_name;
      // Set unique_id only if db_id and db_session_id exist
      if (!tboptions.db_id.empty() && !tboptions.db_session_id.empty()) {
        if (!GetSstInternalUniqueId(tboptions.db_id, tboptions.db_session_id,
                                    meta->fd.GetNumber(), &(meta->unique_id))
                 .ok()) {
          // if failed to get unique id, just set it Null
          meta->unique_id = kNullUniqueId64x2;
        }
      }
    }

    if (s.ok()) {
      s = *io_status;
    }

    if (blob_file_builder) {
      if (s.ok()) {
        s = blob_file_builder->Finish();
      } else {
        blob_file_builder->Abandon(s);
      }
      blob_file_builder.reset();
    }

    // TODO Also check the IO status when create the Iterator.

    TEST_SYNC_POINT("BuildTable:BeforeOutputValidation");
    if (s.ok() && !empty) {
      // Verify that the table is usable
      // We set for_compaction to false and don't OptimizeForCompactionTableRead
      // here because this is a special case after we finish the table building.
      // No matter whether use_direct_io_for_flush_and_compaction is true,
      // the goal is to cache it here for further user reads.
      ReadOptions read_options;
      std::unique_ptr<InternalIterator> it(table_cache->NewIterator(
          read_options, file_options, tboptions.internal_comparator, *meta,
          nullptr /* range_del_agg */, mutable_cf_options.prefix_extractor,
          nullptr,
          (internal_stats == nullptr) ? nullptr
                                      : internal_stats->GetFileReadHist(0),
          TableReaderCaller::kFlush, /*arena=*/nullptr,
          /*skip_filter=*/false, tboptions.level_at_creation,
          MaxFileSizeForL0MetaPin(mutable_cf_options),
          /*smallest_compaction_key=*/nullptr,
          /*largest_compaction_key*/ nullptr,
          /*allow_unprepared_value*/ false));
      s = it->status();
      if (s.ok() && paranoid_file_checks) {
        OutputValidator file_validator(tboptions.internal_comparator,
                                       /*enable_order_check=*/true,
                                       /*enable_hash=*/true);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          // Generate a rolling 64-bit hash of the key and values
          file_validator.Add(it->key(), it->value()).PermitUncheckedError();
        }
        s = it->status();
        if (s.ok() && !output_validator.CompareValidator(file_validator)) {
          s = Status::Corruption("Paranoid checksums do not match");
        }
      }
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (!s.ok() || meta->fd.GetFileSize() == 0) {
    TEST_SYNC_POINT("BuildTable:BeforeDeleteFile");

    constexpr IODebugContext* dbg = nullptr;

    if (table_file_created) {
      Status ignored = fs->DeleteFile(fname, IOOptions(), dbg);
      ignored.PermitUncheckedError();
    }

    assert(blob_file_additions || blob_file_paths.empty());

    if (blob_file_additions) {
      for (const std::string& blob_file_path : blob_file_paths) {
        Status ignored = DeleteDBFile(&db_options, blob_file_path, dbname,
                                      /*force_bg=*/false, /*force_fg=*/false);
        ignored.PermitUncheckedError();
        TEST_SYNC_POINT("BuildTable::AfterDeleteFile");
      }
    }
  }

  Status status_for_listener = s;
  if (meta->fd.GetFileSize() == 0) {
    fname = "(nil)";
    if (s.ok()) {
      status_for_listener = Status::Aborted("Empty SST file not kept");
    }
  }
  // Output to event logger and fire events.
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger, ioptions.listeners, dbname, tboptions.column_family_name,
      fname, job_id, meta->fd, meta->oldest_blob_file_number, tp,
      tboptions.reason, status_for_listener, file_checksum,
      file_checksum_func_name);

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
