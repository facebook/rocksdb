//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/map_builder.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <algorithm>
#include <functional>
#include <list>
#include <memory>
#include <random>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "db/builder.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/version_set.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/block.h"
#include "table/block_based_table_factory.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/iterator_cache.h"
#include "util/log_buffer.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/range_partition.h"
#include "util/sst_file_manager_impl.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {

MapBuilder::MapBuilder(
    int job_id, const ImmutableDBOptions& db_options,
    const EnvOptions env_options, VersionSet* versions,
    LogBuffer* log_buffer, Statistics* stats, InstrumentedMutex* db_mutex,
    ErrorHandler* db_error_handler,
    std::vector<SequenceNumber> existing_snapshots,
    std::shared_ptr<Cache> table_cache, EventLogger* event_logger,
    const std::string& dbname)
    : job_id_(job_id),
      dbname_(dbname),
      db_options_(db_options),
      env_options_(env_options),
      env_(db_options.env),
      env_optiosn_for_read_(
          env_->OptimizeForCompactionTableRead(env_options, db_options_)),
      versions_(versions),
      log_buffer_(log_buffer),
      stats_(stats),
      db_mutex_(db_mutex),
      db_error_handler_(db_error_handler),
      existing_snapshots_(std::move(existing_snapshots)),
      table_cache_(std::move(table_cache)),
      event_logger_(event_logger) {
  assert(log_buffer_ != nullptr);
}

Status MapBuilder::Build(const std::vector<CompactionInputFiles>& inputs,
                         const std::vector<RangePtr>& deleted_range,
                         const std::vector<const FileMetaData*>& added_files,
                         uint32_t output_path_id, VersionStorageInfo* vstorage,
                         ColumnFamilyData* cfd, VersionEdit* edit,
                         FileMetaData* file_meta, TableProperties* porp) {

  std::unordered_map<uint64_t, FileMetaData*> empty_delend_files;
  auto& depend_files = vstorage->depend_files();

  auto create_iterator = [&](const FileMetaData* f, uint64_t sst_id,
                             Arena* arena, RangeDelAggregator* range_del_agg,
                             TableReader** reader_ptr)->InternalIterator* {
    
    if (f == nullptr) {
      auto find = depend_files.find(sst_id);
      if (find == depend_files.end()) {
        if (reader_ptr != nullptr) {
          *reader_ptr = nullptr;
        }
        auto s = Status::Corruption("Variety sst depend files missing");
        return NewErrorInternalIterator<Slice>(s, arena);
      }
      f = find->second;
    }
    ReadOptions read_options;
    read_options.verify_checksums = true;
    read_options.fill_cache = false;
    read_options.total_order_seek = true;

    return cfd->table_cache()->NewIterator(
               read_options, env_optiosn_for_read_,
               cfd->internal_comparator(), *f,
               f->sst_variety == kMapSst ? empty_delend_files : depend_files,
               range_del_agg,
               cfd->GetCurrentMutableCFOptions()->prefix_extractor.get(),
               reader_ptr, nullptr /* no per level latency histogram */,
               true /* for_compaction */, arena,
               false /* skip_filters */, -1);
  };

  IteratorCache iterator_cache(std::ref(create_iterator));
  iterator_cache.NewRangeDelAgg(cfd->internal_comparator(),
                                existing_snapshots_);

  std::list<std::vector<RangeWithDepend>> level_ranges;
  MapSstElement map_element;
  FileMetaDataBoundBuilder bound_builder(cfd->internal_comparator());

  Status s;

  // load input files into level_ranges
  for (auto& level_files : inputs) {
    if (level_files.files.empty()) {
      continue;
    }
    if (level_files.level == 0) {
      for (auto f : level_files.files) {
        std::vector<RangeWithDepend> ranges;
        s = LoadRangeWithDepend(ranges, &bound_builder, iterator_cache, &f, 1);
        if (!s.ok()) {
          return s;
        }
        level_ranges.emplace_back(std::move(ranges));
      }
    } else {
      std::vector<RangeWithDepend> ranges;
      s = LoadRangeWithDepend(ranges, &bound_builder, iterator_cache,
                              level_files.files.data(),
                              level_files.files.size());
      if (!s.ok()) {
        return s;
      }
      level_ranges.emplace_back(std::move(ranges));
    }
  }

  // merge segments
  // TODO(zouzhizhang): multi way union
  while (level_ranges.size() > 1) {
    auto union_a = level_ranges.begin();
    auto union_b = std::next(union_a);
    size_t min_sum = union_a->size() + union_b->size();
    for (auto next = std::next(union_b); next != level_ranges.end();
         ++union_b, ++next) {
      size_t sum = union_b->size() + next->size();
      if (sum < min_sum) {
        min_sum = sum;
        union_a = union_b;
      }
    }
    union_b = std::next(union_a);
    level_ranges.insert(
        union_a, MergeRangeWithDepend(*union_a, *union_b,
                                      cfd->internal_comparator()));
    level_ranges.erase(union_a);
    level_ranges.erase(union_b);
  }

  if (!deleted_range.empty()) {
    level_ranges.front() =
        DeleteRangeWithDepend(level_ranges.front(), deleted_range,
                              cfd->internal_comparator());
  }
  if (!added_files.empty()) {
    std::vector<RangeWithDepend> ranges;
    s = LoadRangeWithDepend(ranges, &bound_builder, iterator_cache,
                            added_files.data(), added_files.size());
    if (!s.ok()) {
      return s;
    }
    level_ranges.front() =
        MergeRangeWithDepend(level_ranges.front(), ranges,
                             cfd->internal_comparator());
  }

  // Used for write properties
  std::vector<uint64_t> sst_depend;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>> collectors;
  collectors.emplace_back(
      new SSTLinkPropertiesCollectorFactory((uint8_t)kMapSst, &sst_depend));

  // no need to lock because VersionSet::next_file_number_ is atomic
  uint64_t file_number = versions_->NewFileNumber();
  std::string fname =
      TableFileName(cfd->ioptions()->cf_paths, file_number, output_path_id);
  // Fire events.
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname, 0,
      TableFileCreationReason::kCompaction);
#endif  // !ROCKSDB_LITE

  // Make the output file
  unique_ptr<WritableFile> writable_file;
  s = NewWritableFile(env_, fname, &writable_file, env_options_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "[%s] [JOB %d] BuildMapSst for table #%" PRIu64
        " fails at NewWritableFile with status %s",
        cfd->GetName().c_str(), job_id_, file_number, s.ToString().c_str());
    LogFlush(db_options_.info_log);
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        nullptr, cfd->ioptions()->listeners, dbname_, cfd->GetName(),
        fname, -1, FileDescriptor(), TableProperties(),
        TableFileCreationReason::kCompaction, s);
    return s;
  }

  file_meta->fd =
      FileDescriptor(file_number, output_path_id, 0);

  writable_file->SetIOPriority(Env::IO_LOW);
  writable_file->SetWriteLifeTimeHint(Env::WLTH_SHORT);
  // map sst always small
  writable_file->SetPreallocationBlockSize(4ULL << 20);
  std::unique_ptr<WritableFileWriter> outfile(
      new WritableFileWriter(std::move(writable_file), env_options_, stats_));

  uint64_t output_file_creation_time = bound_builder.creation_time;
  if (output_file_creation_time == 0) {
    int64_t _current_time = 0;
    auto status = env_->GetCurrentTime(&_current_time);
    // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
    if (!status.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "Failed to get current time to populate creation_time property. "
          "Status: %s",
          status.ToString().c_str());
    }
    output_file_creation_time = static_cast<uint64_t>(_current_time);
  }

  // map sst don't need compression or filters
  std::unique_ptr<TableBuilder> builder(
      NewTableBuilder(*cfd->ioptions(), *cfd->GetCurrentMutableCFOptions(),
                      cfd->internal_comparator(), &collectors,
                      cfd->GetID(), cfd->GetName(), outfile.get(),
                      kNoCompression, CompressionOptions(), -1, nullptr,
                      false, output_file_creation_time));
  LogFlush(db_options_.info_log);

  // Update boundaries
  file_meta->smallest = bound_builder.smallest;
  file_meta->largest = bound_builder.largest;
  file_meta->fd.smallest_seqno = bound_builder.smallest_seqno;
  file_meta->fd.largest_seqno = bound_builder.largest_seqno;

  std::unordered_set<uint64_t> sst_depend_build;
  ScopedArenaIterator output_iter(
      NewShrinkRangeWithDependIterator(level_ranges.front(), sst_depend_build,
                                       iterator_cache,
                                       cfd->internal_comparator(),
                                       iterator_cache.GetArena()));
  for (output_iter->SeekToFirst(); output_iter->Valid(); output_iter->Next()) {
    builder->Add(output_iter->key(), output_iter->value());
  }
  if (!output_iter->status().ok()) {
    return output_iter->status();
  }

  // Prepare sst_depend, IntTblPropCollector::Finish will read it
  sst_depend.reserve(sst_depend_build.size());
  sst_depend.insert(sst_depend.end(), sst_depend_build.begin(),
                    sst_depend_build.end());
  sst_depend_build.clear();
  std::sort(sst_depend.begin(), sst_depend.end());

  CompactionIterationStats range_del_out_stats;
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_SYNC_FILE);

  const Comparator* ucmp = cfd->user_comparator();
  auto range_del_agg = iterator_cache.GetRangeDelAggregator();
  auto earliest_snapshot = kMaxSequenceNumber;
  if (existing_snapshots_.size() > 0) {
    earliest_snapshot = existing_snapshots_[0];
  }
  auto it = range_del_agg->NewIterator();
  for (it->Seek(bound_builder.smallest.user_key()); it->Valid(); it->Next()) {
    auto tombstone = it->Tombstone();
    if (ucmp->Compare(bound_builder.largest.user_key(),
                      tombstone.start_key_) <= 0) {
      // Tombstones starting at upper_bound or later only need to be included
      // in the next table. Break because subsequent tombstones will start
      // even later.
      break;
    }
  }
  file_meta->marked_for_compaction = builder->NeedCompact();
  const uint64_t current_entries = builder->NumEntries();
  if (s.ok()) {
    s = builder->Finish();
  } else {
    builder->Abandon();
  }
  const uint64_t current_bytes = builder->FileSize();
  if (s.ok()) {
    file_meta->fd.file_size = current_bytes;
  }
  // Finish and check for file errors
  if (s.ok()) {
    StopWatch sw(env_, stats_, COMPACTION_OUTFILE_SYNC_MICROS);
    s = outfile->Sync(db_options_.use_fsync);
  }
  if (s.ok()) {
    s = outfile->Close();
  }
  outfile.reset();

  if (s.ok()) {
    *porp = builder->GetTableProperties();
    // Output to event logger and fire events.
    const char* compaction_msg =
        file_meta->marked_for_compaction ? " (need compaction)" : "";
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Generated map table #%" PRIu64 ": %" PRIu64
                   " keys, %" PRIu64 " bytes%s",
                   cfd->GetName().c_str(), job_id_, file_number,
                   current_entries, current_bytes, compaction_msg);
  }
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      nullptr, cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname,
      -1, file_meta->fd, *porp, TableFileCreationReason::kCompaction, s);

#ifndef ROCKSDB_LITE
  // Report new file to SstFileManagerImpl
  auto sfm = static_cast<SstFileManagerImpl*>(
                 db_options_.sst_file_manager.get());
  if (sfm && file_meta->fd.GetPathId() == 0) {
    sfm->OnAddFile(fname);
    if (sfm->IsMaxAllowedSpaceReached()) {
      // TODO(ajkr): should we return OK() if max space was reached by the final
      // compaction output file (similarly to how flush works when full)?
      s = Status::SpaceLimit("Max allowed space was reached");
    }
  }
#endif

  builder.reset();

  // Update metadata
  file_meta->sst_variety = kMapSst;
  file_meta->sst_depend = std::move(sst_depend);

  if (edit != nullptr) {
    for (auto& input_level : inputs) {
      for (auto f : input_level.files) {
        edit->DeleteFile(input_level.level, f->fd.GetNumber());
      }
    }
    for (auto f : added_files) {
      edit->AddFile(vstorage->num_levels(), *f);
    }
  }
  return Status::OK();
}

}  // namespace rocksdb
