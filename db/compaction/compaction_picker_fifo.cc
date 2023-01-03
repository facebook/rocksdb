//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_fifo.h"
#ifndef ROCKSDB_LITE

#include <cinttypes>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
uint64_t GetTotalFilesSize(const std::vector<FileMetaData*>& files) {
  uint64_t total_size = 0;
  for (const auto& f : files) {
    total_size += f->fd.file_size;
  }
  return total_size;
}
}  // anonymous namespace

bool FIFOCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  const int kLevel0 = 0;
  return vstorage->CompactionScore(kLevel0) >= 1;
}

Compaction* FIFOCompactionPicker::PickTTLCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  assert(mutable_cf_options.ttl > 0);

  const int kLevel0 = 0;
  const std::vector<FileMetaData*>& level_files = vstorage->LevelFiles(kLevel0);
  uint64_t total_size = GetTotalFilesSize(level_files);

  int64_t _current_time;
  auto status = ioptions_.clock->GetCurrentTime(&_current_time);
  if (!status.ok()) {
    ROCKS_LOG_BUFFER(log_buffer,
                     "[%s] FIFO compaction: Couldn't get current time: %s. "
                     "Not doing compactions based on TTL. ",
                     cf_name.c_str(), status.ToString().c_str());
    return nullptr;
  }
  const uint64_t current_time = static_cast<uint64_t>(_current_time);

  if (!level0_compactions_in_progress_.empty()) {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] FIFO compaction: Already executing compaction. No need "
        "to run parallel compactions since compactions are very fast",
        cf_name.c_str());
    return nullptr;
  }

  std::vector<CompactionInputFiles> inputs;
  inputs.emplace_back();
  inputs[0].level = 0;

  // avoid underflow
  if (current_time > mutable_cf_options.ttl) {
    for (auto ritr = level_files.rbegin(); ritr != level_files.rend(); ++ritr) {
      FileMetaData* f = *ritr;
      assert(f);
      if (f->fd.table_reader && f->fd.table_reader->GetTableProperties()) {
        uint64_t creation_time =
            f->fd.table_reader->GetTableProperties()->creation_time;
        if (creation_time == 0 ||
            creation_time >= (current_time - mutable_cf_options.ttl)) {
          break;
        }
      }
      total_size -= f->fd.file_size;
      inputs[0].files.push_back(f);
    }
  }

  // Return a nullptr and proceed to size-based FIFO compaction if:
  // 1. there are no files older than ttl OR
  // 2. there are a few files older than ttl, but deleting them will not bring
  //    the total size to be less than max_table_files_size threshold.
  if (inputs[0].files.empty() ||
      total_size >
          mutable_cf_options.compaction_options_fifo.max_table_files_size) {
    return nullptr;
  }

  for (const auto& f : inputs[0].files) {
    uint64_t creation_time = 0;
    assert(f);
    if (f->fd.table_reader && f->fd.table_reader->GetTableProperties()) {
      creation_time = f->fd.table_reader->GetTableProperties()->creation_time;
    }
    ROCKS_LOG_BUFFER(log_buffer,
                     "[%s] FIFO compaction: picking file %" PRIu64
                     " with creation time %" PRIu64 " for deletion",
                     cf_name.c_str(), f->fd.GetNumber(), creation_time);
  }

  Compaction* c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), 0, 0, 0, 0, kNoCompression,
      mutable_cf_options.compression_opts, Temperature::kUnknown,
      /* max_subcompactions */ 0, {}, /* is manual */ false,
      /* trim_ts */ "", vstorage->CompactionScore(0),
      /* is deletion compaction */ true, /* l0_files_might_overlap */ true,
      CompactionReason::kFIFOTtl);
  return c;
}

// The size-based compaction picker for FIFO.
//
// When the entire column family size exceeds max_table_files_size, FIFO will
// try to delete the oldest sst file(s) until the resulting column family size
// is smaller than max_table_files_size.
//
// This function also takes care the case where a DB is migrating from level /
// universal compaction to FIFO compaction.  During the migration, the column
// family will also have non-L0 files while FIFO can only create L0 files.
// In this case, this function will first purge the sst files in the bottom-
// most non-empty level first, and the DB will eventually converge to the
// regular FIFO case where there're only L0 files.  Note that during the
// migration case, the purge order will only be an approximation of "FIFO"
// as entries inside lower-level files might sometimes be newer than some
// entries inside upper-level files.
Compaction* FIFOCompactionPicker::PickSizeCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  // compute the total size and identify the last non-empty level
  int last_level = 0;
  uint64_t total_size = 0;
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    auto level_size = GetTotalFilesSize(vstorage->LevelFiles(level));
    total_size += level_size;
    if (level_size > 0) {
      last_level = level;
    }
  }
  const std::vector<FileMetaData*>& last_level_files =
      vstorage->LevelFiles(last_level);

  if (last_level == 0 &&
      total_size <=
          mutable_cf_options.compaction_options_fifo.max_table_files_size) {
    // total size not exceeded, try to find intra level 0 compaction if enabled
    const std::vector<FileMetaData*>& level0_files = vstorage->LevelFiles(0);
    if (mutable_cf_options.compaction_options_fifo.allow_compaction &&
        level0_files.size() > 0) {
      CompactionInputFiles comp_inputs;
      // try to prevent same files from being compacted multiple times, which
      // could produce large files that may never TTL-expire. Achieve this by
      // disallowing compactions with files larger than memtable (inflate its
      // size by 10% to account for uncompressed L0 files that may have size
      // slightly greater than memtable size limit).
      size_t max_compact_bytes_per_del_file =
          static_cast<size_t>(MultiplyCheckOverflow(
              static_cast<uint64_t>(mutable_cf_options.write_buffer_size),
              1.1));
      if (FindIntraL0Compaction(
              level0_files,
              mutable_cf_options
                  .level0_file_num_compaction_trigger /* min_files_to_compact */
              ,
              max_compact_bytes_per_del_file,
              mutable_cf_options.max_compaction_bytes, &comp_inputs)) {
        Compaction* c = new Compaction(
            vstorage, ioptions_, mutable_cf_options, mutable_db_options,
            {comp_inputs}, 0, 16 * 1024 * 1024 /* output file size limit */,
            0 /* max compaction bytes, not applicable */,
            0 /* output path ID */, mutable_cf_options.compression,
            mutable_cf_options.compression_opts, Temperature::kUnknown,
            0 /* max_subcompactions */, {}, /* is manual */ false,
            /* trim_ts */ "", vstorage->CompactionScore(0),
            /* is deletion compaction */ false,
            /* l0_files_might_overlap */ true,
            CompactionReason::kFIFOReduceNumFiles);
        return c;
      }
    }

    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] FIFO compaction: nothing to do. Total size %" PRIu64
        ", max size %" PRIu64 "\n",
        cf_name.c_str(), total_size,
        mutable_cf_options.compaction_options_fifo.max_table_files_size);
    return nullptr;
  }

  if (!level0_compactions_in_progress_.empty()) {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] FIFO compaction: Already executing compaction. No need "
        "to run parallel compactions since compactions are very fast",
        cf_name.c_str());
    return nullptr;
  }

  std::vector<CompactionInputFiles> inputs;
  inputs.emplace_back();
  inputs[0].level = last_level;

  if (last_level == 0) {
    // In L0, right-most files are the oldest files.
    for (auto ritr = last_level_files.rbegin(); ritr != last_level_files.rend();
         ++ritr) {
      auto f = *ritr;
      total_size -= f->fd.file_size;
      inputs[0].files.push_back(f);
      char tmp_fsize[16];
      AppendHumanBytes(f->fd.GetFileSize(), tmp_fsize, sizeof(tmp_fsize));
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] FIFO compaction: picking file %" PRIu64
                       " with size %s for deletion",
                       cf_name.c_str(), f->fd.GetNumber(), tmp_fsize);
      if (total_size <=
          mutable_cf_options.compaction_options_fifo.max_table_files_size) {
        break;
      }
    }
  } else if (total_size >
             mutable_cf_options.compaction_options_fifo.max_table_files_size) {
    // If the last level is non-L0, we actually don't know which file is
    // logically the oldest since the file creation time only represents
    // when this file was compacted to this level, which is independent
    // to when the entries in this file were first inserted.
    //
    // As a result, we delete files from the left instead.  This means the sst
    // file with the smallest key will be deleted first.  This design decision
    // better serves a major type of FIFO use cases where smaller keys are
    // associated with older data.
    for (const auto& f : last_level_files) {
      total_size -= f->fd.file_size;
      inputs[0].files.push_back(f);
      char tmp_fsize[16];
      AppendHumanBytes(f->fd.GetFileSize(), tmp_fsize, sizeof(tmp_fsize));
      ROCKS_LOG_BUFFER(
          log_buffer,
          "[%s] FIFO compaction: picking file %" PRIu64
          " with size %s for deletion under total size %" PRIu64
          " vs max table files size %" PRIu64,
          cf_name.c_str(), f->fd.GetNumber(), tmp_fsize, total_size,
          mutable_cf_options.compaction_options_fifo.max_table_files_size);

      if (total_size <=
          mutable_cf_options.compaction_options_fifo.max_table_files_size) {
        break;
      }
    }
  } else {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] FIFO compaction: nothing to do. Total size %" PRIu64
        ", max size %" PRIu64 "\n",
        cf_name.c_str(), total_size,
        mutable_cf_options.compaction_options_fifo.max_table_files_size);
    return nullptr;
  }

  Compaction* c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), last_level,
      /* target_file_size */ 0,
      /* max_compaction_bytes */ 0,
      /* output_path_id */ 0, kNoCompression,
      mutable_cf_options.compression_opts, Temperature::kUnknown,
      /* max_subcompactions */ 0, {}, /* is manual */ false,
      /* trim_ts */ "", vstorage->CompactionScore(0),
      /* is deletion compaction */ true,
      /* l0_files_might_overlap */ true, CompactionReason::kFIFOMaxSize);
  return c;
}

Compaction* FIFOCompactionPicker::PickCompactionToWarm(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  if (mutable_cf_options.compaction_options_fifo.age_for_warm == 0) {
    return nullptr;
  }

  // PickCompactionToWarm is only triggered if there is no non-L0 files.
  for (int level = 1; level < vstorage->num_levels(); ++level) {
    if (GetTotalFilesSize(vstorage->LevelFiles(level)) > 0) {
      return nullptr;
    }
  }

  const int kLevel0 = 0;
  const std::vector<FileMetaData*>& level_files = vstorage->LevelFiles(kLevel0);

  int64_t _current_time;
  auto status = ioptions_.clock->GetCurrentTime(&_current_time);
  if (!status.ok()) {
    ROCKS_LOG_BUFFER(log_buffer,
                     "[%s] FIFO compaction: Couldn't get current time: %s. "
                     "Not doing compactions based on warm threshold. ",
                     cf_name.c_str(), status.ToString().c_str());
    return nullptr;
  }
  const uint64_t current_time = static_cast<uint64_t>(_current_time);

  if (!level0_compactions_in_progress_.empty()) {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] FIFO compaction: Already executing compaction. Parallel "
        "compactions are not supported",
        cf_name.c_str());
    return nullptr;
  }

  std::vector<CompactionInputFiles> inputs;
  inputs.emplace_back();
  inputs[0].level = 0;

  // avoid underflow
  if (current_time > mutable_cf_options.compaction_options_fifo.age_for_warm) {
    uint64_t create_time_threshold =
        current_time - mutable_cf_options.compaction_options_fifo.age_for_warm;
    uint64_t compaction_size = 0;
    // We will ideally identify a file qualifying for warm tier by knowing
    // the timestamp for the youngest entry in the file. However, right now
    // we don't have the information. We infer it by looking at timestamp
    // of the next file's (which is just younger) oldest entry's timestamp.
    FileMetaData* prev_file = nullptr;
    for (auto ritr = level_files.rbegin(); ritr != level_files.rend(); ++ritr) {
      FileMetaData* f = *ritr;
      assert(f);
      if (f->being_compacted) {
        // Right now this probably won't happen as we never try to schedule
        // two compactions in parallel, so here we just simply don't schedule
        // anything.
        return nullptr;
      }
      uint64_t oldest_ancester_time = f->TryGetOldestAncesterTime();
      if (oldest_ancester_time == kUnknownOldestAncesterTime) {
        // Older files might not have enough information. It is possible to
        // handle these files by looking at newer files, but maintaining the
        // logic isn't worth it.
        break;
      }
      if (oldest_ancester_time > create_time_threshold) {
        // The previous file (which has slightly older data) doesn't qualify
        // for warm tier.
        break;
      }
      if (prev_file != nullptr) {
        compaction_size += prev_file->fd.GetFileSize();
        if (compaction_size > mutable_cf_options.max_compaction_bytes) {
          break;
        }
        inputs[0].files.push_back(prev_file);
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] FIFO compaction: picking file %" PRIu64
                         " with next file's oldest time %" PRIu64 " for warm",
                         cf_name.c_str(), prev_file->fd.GetNumber(),
                         oldest_ancester_time);
      }
      if (f->temperature == Temperature::kUnknown ||
          f->temperature == Temperature::kHot) {
        prev_file = f;
      } else if (!inputs[0].files.empty()) {
        // A warm file newer than files picked.
        break;
      } else {
        assert(prev_file == nullptr);
      }
    }
  }

  if (inputs[0].files.empty()) {
    return nullptr;
  }

  Compaction* c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), 0, 0 /* output file size limit */,
      0 /* max compaction bytes, not applicable */, 0 /* output path ID */,
      mutable_cf_options.compression, mutable_cf_options.compression_opts,
      Temperature::kWarm,
      /* max_subcompactions */ 0, {}, /* is manual */ false, /* trim_ts */ "",
      vstorage->CompactionScore(0),
      /* is deletion compaction */ false, /* l0_files_might_overlap */ true,
      CompactionReason::kChangeTemperature);
  return c;
}

Compaction* FIFOCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  Compaction* c = nullptr;
  if (mutable_cf_options.ttl > 0) {
    c = PickTTLCompaction(cf_name, mutable_cf_options, mutable_db_options,
                          vstorage, log_buffer);
  }
  if (c == nullptr) {
    c = PickSizeCompaction(cf_name, mutable_cf_options, mutable_db_options,
                           vstorage, log_buffer);
  }
  if (c == nullptr) {
    c = PickCompactionToWarm(cf_name, mutable_cf_options, mutable_db_options,
                             vstorage, log_buffer);
  }
  RegisterCompaction(c);
  return c;
}

Compaction* FIFOCompactionPicker::CompactRange(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    int input_level, int output_level,
    const CompactRangeOptions& /*compact_range_options*/,
    const InternalKey* /*begin*/, const InternalKey* /*end*/,
    InternalKey** compaction_end, bool* /*manual_conflict*/,
    uint64_t /*max_file_num_to_ignore*/, const std::string& /*trim_ts*/) {
#ifdef NDEBUG
  (void)input_level;
  (void)output_level;
#endif
  assert(input_level == 0);
  assert(output_level == 0);
  *compaction_end = nullptr;
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, ioptions_.logger);
  Compaction* c = PickCompaction(cf_name, mutable_cf_options,
                                 mutable_db_options, vstorage, &log_buffer);
  log_buffer.FlushBufferToLog();
  return c;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
