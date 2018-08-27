//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction_picker_universal.h"
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <numeric>
#include <inttypes.h>
#include <limits>
#include <queue>
#include <string>
#include <utility>
#include "db/column_family.h"
#include "db/map_builder.h"
#include "monitoring/statistics.h"
#include "util/filename.h"
#include "util/log_buffer.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {
namespace {
// Used in universal compaction when trivial move is enabled.
// This structure is used for the construction of min heap
// that contains the file meta data, the level of the file
// and the index of the file in that level

struct InputFileInfo {
  InputFileInfo() : f(nullptr), level(0), index(0) {}

  FileMetaData* f;
  size_t level;
  size_t index;
};

// Used in universal compaction when trivial move is enabled.
// This comparator is used for the construction of min heap
// based on the smallest key of the file.
struct SmallestKeyHeapComparator {
  explicit SmallestKeyHeapComparator(const Comparator* ucmp) { ucmp_ = ucmp; }

  bool operator()(InputFileInfo i1, InputFileInfo i2) const {
    return (ucmp_->Compare(i1.f->smallest.user_key(),
                           i2.f->smallest.user_key()) > 0);
  }

 private:
  const Comparator* ucmp_;
};

typedef std::priority_queue<InputFileInfo, std::vector<InputFileInfo>,
                            SmallestKeyHeapComparator>
    SmallestKeyHeap;

// This function creates the heap that is used to find if the files are
// overlapping during universal compaction when the allow_trivial_move
// is set.
SmallestKeyHeap create_level_heap(Compaction* c, const Comparator* ucmp) {
  SmallestKeyHeap smallest_key_priority_q =
      SmallestKeyHeap(SmallestKeyHeapComparator(ucmp));

  InputFileInfo input_file;

  for (size_t l = 0; l < c->num_input_levels(); l++) {
    if (c->num_input_files(l) != 0) {
      if (l == 0 && c->start_level() == 0) {
        for (size_t i = 0; i < c->num_input_files(0); i++) {
          input_file.f = c->input(0, i);
          input_file.level = 0;
          input_file.index = i;
          smallest_key_priority_q.push(std::move(input_file));
        }
      } else {
        input_file.f = c->input(l, 0);
        input_file.level = l;
        input_file.index = 0;
        smallest_key_priority_q.push(std::move(input_file));
      }
    }
  }
  return smallest_key_priority_q;
}

#ifndef NDEBUG
// smallest_seqno and largest_seqno are set iff. `files` is not empty.
void GetSmallestLargestSeqno(const std::vector<FileMetaData*>& files,
                             SequenceNumber* smallest_seqno,
                             SequenceNumber* largest_seqno) {
  bool is_first = true;
  for (FileMetaData* f : files) {
    assert(f->fd.smallest_seqno <= f->fd.largest_seqno);
    if (is_first) {
      is_first = false;
      *smallest_seqno = f->fd.smallest_seqno;
      *largest_seqno = f->fd.largest_seqno;
    } else {
      if (f->fd.smallest_seqno < *smallest_seqno) {
        *smallest_seqno = f->fd.smallest_seqno;
      }
      if (f->fd.largest_seqno > *largest_seqno) {
        *largest_seqno = f->fd.largest_seqno;
      }
    }
  }
}
#endif
}  // namespace

// Algorithm that checks to see if there are any overlapping
// files in the input
bool UniversalCompactionPicker::IsInputFilesNonOverlapping(Compaction* c) {
  auto comparator = icmp_->user_comparator();
  int first_iter = 1;

  InputFileInfo prev, curr, next;

  SmallestKeyHeap smallest_key_priority_q =
      create_level_heap(c, icmp_->user_comparator());

  while (!smallest_key_priority_q.empty()) {
    curr = smallest_key_priority_q.top();
    smallest_key_priority_q.pop();

    if (first_iter) {
      prev = curr;
      first_iter = 0;
    } else {
      if (comparator->Compare(prev.f->largest.user_key(),
                              curr.f->smallest.user_key()) >= 0) {
        // found overlapping files, return false
        return false;
      }
      assert(comparator->Compare(curr.f->largest.user_key(),
                                 prev.f->largest.user_key()) > 0);
      prev = curr;
    }

    next.f = nullptr;

    if (curr.level != 0 && curr.index < c->num_input_files(curr.level) - 1) {
      next.f = c->input(curr.level, curr.index + 1);
      next.level = curr.level;
      next.index = curr.index + 1;
    }

    if (next.f) {
      smallest_key_priority_q.push(std::move(next));
    }
  }
  return true;
}

bool UniversalCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  const int kLevel0 = 0;
  if (vstorage->CompactionScore(kLevel0) >= 1) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  if (vstorage->has_space_amplification()) {
    return true;
  }
  return false;
}

void UniversalCompactionPicker::SortedRun::Dump(char* out_buf,
                                                size_t out_buf_size,
                                                bool print_path) const {
  if (level == 0) {
    assert(file != nullptr);
    if (file->fd.GetPathId() == 0 || !print_path) {
      snprintf(out_buf, out_buf_size, "file %" PRIu64, file->fd.GetNumber());
    } else {
      snprintf(out_buf, out_buf_size, "file %" PRIu64
                                      "(path "
                                      "%" PRIu32 ")",
               file->fd.GetNumber(), file->fd.GetPathId());
    }
  } else {
    snprintf(out_buf, out_buf_size, "level %d", level);
  }
}

void UniversalCompactionPicker::SortedRun::DumpSizeInfo(
    char* out_buf, size_t out_buf_size, size_t sorted_run_count) const {
  if (level == 0) {
    assert(file != nullptr);
    snprintf(out_buf, out_buf_size,
             "file %" PRIu64 "[%" ROCKSDB_PRIszt
             "] "
             "with size %" PRIu64 " (compensated size %" PRIu64 ")",
             file->fd.GetNumber(), sorted_run_count, file->fd.GetFileSize(),
             file->compensated_file_size);
  } else {
    snprintf(out_buf, out_buf_size,
             "level %d[%" ROCKSDB_PRIszt
             "] "
             "with size %" PRIu64 " (compensated size %" PRIu64 ")",
             level, sorted_run_count, size, compensated_file_size);
  }
}

std::vector<UniversalCompactionPicker::SortedRun>
UniversalCompactionPicker::CalculateSortedRuns(
    const VersionStorageInfo& vstorage, const ImmutableCFOptions& /*ioptions*/,
    const MutableCFOptions& mutable_cf_options) {
  
  std::function<uint64_t(const FileMetaData*)> get_files_size;
  auto get_files_size_lambda
      = [&vstorage, &get_files_size](const FileMetaData* f) {
    uint64_t file_size = f->fd.GetFileSize();
    if (f->sst_variety == 0) {
      file_size = f->fd.GetFileSize();
    } else {
      for (auto sst_id : f->sst_depend) {
        auto find = vstorage.depend_files().find(sst_id);
        if (find == vstorage.depend_files().end()) {
          // TODO log error
          continue;
        }
        file_size += get_files_size(find->second);
      }
    }
    return file_size;
  };
  get_files_size = std::ref(get_files_size_lambda);

  std::vector<UniversalCompactionPicker::SortedRun> ret;
  for (FileMetaData* f : vstorage.LevelFiles(0)) {
    ret.emplace_back(0, f, f->fd.GetFileSize(), f->compensated_file_size,
                     f->being_compacted);
  }
  for (int level = 1; level < vstorage.num_levels(); level++) {
    uint64_t total_compensated_size = 0U;
    uint64_t total_size = 0U;
    bool being_compacted = false;
    bool is_first = true;
    for (FileMetaData* f : vstorage.LevelFiles(level)) {
      total_compensated_size += f->compensated_file_size;
      total_size += get_files_size(f);
      if (mutable_cf_options.compaction_options_universal.allow_trivial_move) {
        if (f->being_compacted) {
          being_compacted = f->being_compacted;
        }
      } else {
        // Compaction always includes all files for a non-zero level, so for a
        // non-zero level, all the files should share the same being_compacted
        // value.
        // This assumption is only valid when
        // mutable_cf_options.compaction_options_universal.allow_trivial_move is
        // false
        assert(is_first || f->being_compacted == being_compacted);
      }
      if (is_first) {
        being_compacted = f->being_compacted;
        is_first = false;
      }
    }
    if (total_compensated_size > 0) {
      ret.emplace_back(level, nullptr, total_size, total_compensated_size,
                       being_compacted);
    }
  }
  return ret;
}

// Universal style of compaction. Pick files that are contiguous in
// time-range to compact.
Compaction* UniversalCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, LogBuffer* log_buffer) {
  const int kLevel0 = 0;
  double score = vstorage->CompactionScore(kLevel0);
  std::vector<SortedRun> sorted_runs =
      CalculateSortedRuns(*vstorage, ioptions_, mutable_cf_options);

  if (sorted_runs.size() == 0 ||
      (vstorage->FilesMarkedForCompaction().empty() &&
       !vstorage->has_space_amplification() &&
       sorted_runs.size() < (unsigned int)mutable_cf_options
                                .level0_file_num_compaction_trigger)) {
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: nothing to do\n",
                     cf_name.c_str());
    TEST_SYNC_POINT_CALLBACK("UniversalCompactionPicker::PickCompaction:Return",
                             nullptr);
    return nullptr;
  }
  VersionStorageInfo::LevelSummaryStorage tmp;
  ROCKS_LOG_BUFFER_MAX_SZ(
      log_buffer, 3072,
      "[%s] Universal: sorted runs files(%" ROCKSDB_PRIszt "): %s\n",
      cf_name.c_str(), sorted_runs.size(), vstorage->LevelSummary(&tmp));

  // Check for size amplification first.
  Compaction* c = nullptr;
  if (sorted_runs.size() >=
      static_cast<size_t>(
          mutable_cf_options.level0_file_num_compaction_trigger)) {
    if ((c = PickCompactionToReduceSizeAmp(cf_name, mutable_cf_options,
                                           vstorage, score, sorted_runs,
                                           log_buffer)) != nullptr) {
      ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: compacting for size amp\n",
                       cf_name.c_str());
    } else {
      // Size amplification is within limits. Try reducing read
      // amplification while maintaining file size ratios.
      unsigned int ratio =
          mutable_cf_options.compaction_options_universal.size_ratio;

      if ((c = PickCompactionToReduceSortedRuns(
               cf_name, mutable_cf_options, vstorage, score, ratio, UINT_MAX,
               sorted_runs, log_buffer)) != nullptr) {
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] Universal: compacting for size ratio\n",
                         cf_name.c_str());
      }
    }
  }
  if (c == nullptr) {
    c = PickGeneralCompaction(cf_name, mutable_cf_options, vstorage, log_buffer);
  }

  if (c == nullptr) {
    if ((c = PickDeleteTriggeredCompaction(cf_name, mutable_cf_options,
                                           vstorage, score, sorted_runs,
                                           log_buffer)) != nullptr) {
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Universal: delete triggered compaction\n",
                       cf_name.c_str());
    }
  }

  if (c == nullptr) {
    TEST_SYNC_POINT_CALLBACK("UniversalCompactionPicker::PickCompaction:Return",
                             nullptr);
    return nullptr;
  }

  bool allow_trivial_move
      = mutable_cf_options.compaction_options_universal.allow_trivial_move;
  if (allow_trivial_move) {
    // check level has map or link sst
    for (auto& level_files : *c->inputs()) {
      if (vstorage->has_space_amplification(level_files.level)) {
        allow_trivial_move = false;
        break;
      }
    }
  }
  if (allow_trivial_move) {
    c->set_is_trivial_move(IsInputFilesNonOverlapping(c));
  }

// validate that all the chosen files of L0 are non overlapping in time
#ifndef NDEBUG
  SequenceNumber prev_smallest_seqno = 0U;
  bool is_first = true;

  size_t level_index = 0U;
  if (c->start_level() == 0) {
    for (auto f : *c->inputs(0)) {
      assert(f->fd.smallest_seqno <= f->fd.largest_seqno);
      if (is_first) {
        is_first = false;
      }
      prev_smallest_seqno = f->fd.smallest_seqno;
    }
    level_index = 1U;
  }
  for (; level_index < c->num_input_levels(); level_index++) {
    if (c->num_input_files(level_index) != 0) {
      SequenceNumber smallest_seqno = 0U;
      SequenceNumber largest_seqno = 0U;
      GetSmallestLargestSeqno(*(c->inputs(level_index)), &smallest_seqno,
                              &largest_seqno);
      if (is_first) {
        is_first = false;
      } else if (prev_smallest_seqno > 0) {
        // A level is considered as the bottommost level if there are
        // no files in higher levels or if files in higher levels do
        // not overlap with the files being compacted. Sequence numbers
        // of files in bottommost level can be set to 0 to help
        // compression. As a result, the following assert may not hold
        // if the prev_smallest_seqno is 0.
        assert(prev_smallest_seqno > largest_seqno);
      }
      prev_smallest_seqno = smallest_seqno;
    }
  }
#endif
  // update statistics
  MeasureTime(ioptions_.statistics, NUM_FILES_IN_SINGLE_COMPACTION,
              c->inputs(0)->size());

  RegisterCompaction(c);
  vstorage->ComputeCompactionScore(ioptions_, mutable_cf_options);

  TEST_SYNC_POINT_CALLBACK("UniversalCompactionPicker::PickCompaction:Return",
                           c);
  return c;
}

uint32_t UniversalCompactionPicker::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, uint64_t file_size) {
  // Two conditions need to be satisfied:
  // (1) the target path needs to be able to hold the file's size
  // (2) Total size left in this and previous paths need to be not
  //     smaller than expected future file size before this new file is
  //     compacted, which is estimated based on size_ratio.
  // For example, if now we are compacting files of size (1, 1, 2, 4, 8),
  // we will make sure the target file, probably with size of 16, will be
  // placed in a path so that eventually when new files are generated and
  // compacted to (1, 1, 2, 4, 8, 16), all those files can be stored in or
  // before the path we chose.
  //
  // TODO(sdong): now the case of multiple column families is not
  // considered in this algorithm. So the target size can be violated in
  // that case. We need to improve it.
  uint64_t accumulated_size = 0;
  uint64_t future_size =
      file_size *
      (100 - mutable_cf_options.compaction_options_universal.size_ratio) / 100;
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());
  for (; p < ioptions.cf_paths.size() - 1; p++) {
    uint64_t target_size = ioptions.cf_paths[p].target_size;
    if (target_size > file_size &&
        accumulated_size + (target_size - file_size) > future_size) {
      return p;
    }
    accumulated_size += target_size;
  }
  return p;
}
 
Compaction* UniversalCompactionPicker::PickGeneralCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, LogBuffer* log_buffer) {
  if (!vstorage->has_space_amplification()) {
    return nullptr;
  }
  CompactionInputFiles inputs;
  inputs.level = -1;
  size_t max_read_amp = 0;
  for (int level = vstorage->num_levels() - 1; level > 0; --level) {
    if (!vstorage->has_space_amplification(level)) {
      continue;
    }
    auto& level_files = vstorage->LevelFiles(level);
    if (!AreFilesInCompaction(level_files)) {
      if (level_files.size() > 1) {
        inputs.level = level;
        break;
      }
      std::shared_ptr<const TableProperties> porps;
      auto s = table_cache_->GetTableProperties(
          env_options_, *icmp_, level_files.front()->fd, &porps,
          mutable_cf_options.prefix_extractor.get(), false);
      if (s.ok()) {
        size_t level_space_amplification =
            GetSstReadAmp(porps->user_collected_properties);
        if (level_space_amplification > max_read_amp) {
          max_read_amp = level_space_amplification;
          inputs.level = level;
        }
      }
    }
  }
  if (inputs.level == -1) {
    return nullptr;
  }
  inputs.files = vstorage->LevelFiles(inputs.level);
  size_t max_file_size_for_leval =
      MaxFileSizeForLevel(mutable_cf_options, inputs.level,
                          kCompactionStyleUniversal);
  SstVarieties compaction_varieties = kGeneralSst;
  bool single_output = false;
  uint32_t max_subcompactions = ioptions_.max_subcompactions;
  std::vector<RangeStorage> input_range;

  auto new_compaction = [&]{
    auto uc = ioptions_.user_comparator;
    // remove empty ranges
    for (auto it = input_range.begin() + 1; it != input_range.end(); ) {
      if (uc->Compare(it->start, it[-1].start) == 0) {
        it[-1].limit = std::move(it->limit);
        it[-1].include_limit = it->include_limit;
        it = input_range.erase(it);
      } else {
        ++it;
      }
    }
    uint64_t estimated_total_size = 0;
    for (auto f : inputs.files) {
      estimated_total_size += f->fd.file_size;
    }
    uint32_t path_id =
        GetPathId(ioptions_, mutable_cf_options, estimated_total_size);
    return new Compaction(
        vstorage, ioptions_, mutable_cf_options, {inputs}, inputs.level,
        MaxFileSizeForLevel(mutable_cf_options, inputs.level,
                            kCompactionStyleUniversal),
        LLONG_MAX, path_id,
        GetCompressionType(ioptions_, vstorage, mutable_cf_options,
                           inputs.level, 1, true),
        GetCompressionOptions(ioptions_, vstorage, inputs.level, true),
        max_subcompactions, /* grandparents */ {}, /* is manual */ false,
        0, false /* deletion_compaction */, single_output,
        true /* enable_partial_compaction */, compaction_varieties,
        input_range, CompactionReason::kVarietiesAmplification);
  };

  if (inputs.files.size() > 1) {
    compaction_varieties = kMapSst;
    single_output = false;
    max_subcompactions = 1;
    return new_compaction();
  }
  Arena arena;
  DependFileMap empty_depend_files;
  ReadOptions options;
  ScopedArenaIterator iter(table_cache_->NewIterator(
      options, env_options_, *icmp_, *inputs.files.front(), empty_depend_files,
      nullptr, mutable_cf_options.prefix_extractor.get(), nullptr, nullptr,
      false, &arena, true, inputs.level));
  if (!iter->status().ok()) {
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: Read map sst error %s.",
                     cf_name.c_str(), iter->status().getState());
    return nullptr;
  }
  MapSstElement map_element;
  RangeStorage range;
  std::multimap<size_t, InternalKey, std::greater<size_t>> link_count_map;
  auto range_size = [](const MapSstElement& range) {
    size_t sum = 0;
    size_t max = 0;
    for (auto& l : range.link_) {
      sum += l.size;
      max = std::max(max, l.size);
    }
    return std::make_pair(sum, max);
  };
  auto assign_user_key = [](std::string& key, const Slice& ikey) {
    Slice ukey = ExtractUserKey(ikey);
    key.assign(ukey.data(), ukey.size());
  };

  auto uc = ioptions_.internal_comparator.user_comparator();
  bool has_start = false;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (!map_element.Decode(iter->key(), iter->value())) {
      // TODO log error info
      return nullptr;
    }
    if (map_element.link_.size() > 1) {
      InternalKey internal_key;
      internal_key.DecodeFrom(iter->key());
      link_count_map.emplace(map_element.link_.size(), std::move(internal_key));
    }
    size_t sum, max;
    std::tie(sum, max) = range_size(map_element);
    if (map_element.link_.size() > 2 && (sum - max) * 2 < max) {
      if (!has_start) {
        has_start = true;
        assign_user_key(range.start, map_element.smallest_key_);
      }
      assign_user_key(range.limit, map_element.largest_key_);
    } else {
      if (has_start) {
        has_start = false;
        if (uc->Compare(ExtractUserKey(map_element.smallest_key_),
                        range.limit) != 0) {
          assign_user_key(range.limit, map_element.smallest_key_);
          range.include_start = true;
          range.include_limit = false;
          input_range.emplace_back(std::move(range));
          if (input_range.size() >= ioptions_.max_subcompactions) {
            break;
          }
        }
      }
    }
  }
  if (has_start) {
    range.include_limit = true;
    input_range.emplace_back(std::move(range));
  }
  if (!input_range.empty()) {
    compaction_varieties = kLinkSst;
    single_output = false;
    return new_compaction();
  }
  struct Comp {
    const InternalKeyComparator* c;
    bool operator()(const Slice&a, const Slice& b) const {
      return c->Compare(a, b) < 0;
    }
  } c = {icmp_};
  std::set<Slice, Comp> unique_check(c);
  std::vector<InternalKey> unique_check_storage;
  unique_check_storage.reserve(inputs.files.front()->num_entries);
  auto push_unique = [&](const Slice& key) {
    unique_check_storage.emplace_back();
    auto& ikey = unique_check_storage.back();
    ikey.DecodeFrom(key);
    unique_check.emplace(ikey.Encode());
  };
  auto is_perfect = [=](const MapSstElement& e) {
    if (e.link_.size() != 1) {
      return false;
    }
    auto& depend_files = vstorage->depend_files();
    auto find = depend_files.find(e.link_.front().sst_id);
    if (find == depend_files.end()) {
      // TODO log error
      return false;
    }
    auto f = find->second;
    if (f->sst_variety != 0) {
      return false;
    }
    Range r(e.smallest_key_, e.largest_key_, e.include_smallest_,
            e.include_largest_);
    return IsPrefaceRange(r, f, *icmp_);
  };
  for (auto it = link_count_map.begin(); it != link_count_map.end(); ++it) {
    iter->Seek(it->second.Encode());
    assert(iter->Valid());
    if (unique_check.count(iter->key()) > 0) {
      continue;
    }
    map_element.Decode(iter->key(), iter->value());
    assign_user_key(range.start, map_element.smallest_key_);
    assign_user_key(range.limit, map_element.largest_key_);
    range.include_start = true;
    range.include_limit = false;
    size_t sum = range_size(map_element).first;
    push_unique(iter->key());
    while (sum < max_file_size_for_leval) {
      iter->Next();
      if (!iter->Valid()) {
        range.include_limit = true;
        break;
      }
      map_element.Decode(iter->key(), iter->value());
      if (unique_check.count(iter->key()) > 0 || is_perfect(map_element)) {
        assign_user_key(range.limit, map_element.smallest_key_);
        break;
      } else {
        assign_user_key(range.limit, map_element.largest_key_);
      }
      sum += range_size(map_element).first;
      push_unique(iter->key());
    }
    if (sum < max_file_size_for_leval) {
      iter->SeekForPrev(it->second.Encode());
      do {
        iter->Prev();
        if (!iter->Valid() || unique_check.count(iter->key()) > 0) {
          break;
        }
        map_element.Decode(iter->key(), iter->value());
        if (is_perfect(map_element)) {
          break;
        }
        assign_user_key(range.start, map_element.smallest_key_);
        sum += range_size(map_element).first;
        push_unique(iter->key());
      } while (sum < max_file_size_for_leval);
    }
    input_range.emplace_back(std::move(range));
    if (input_range.size() >= ioptions_.max_subcompactions) {
      break;
    }
  }
  if (!input_range.empty()) {
    std::sort(input_range.begin(), input_range.end(),
              [=](const RangeStorage& a, const RangeStorage& b) {
                return uc->Compare(a.limit, b.limit) < 0;
              });
    compaction_varieties = kGeneralSst;
    single_output = false;
    return new_compaction();
  }
  has_start = false;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    map_element.Decode(iter->key(), iter->value());
    assert(map_element.link_.size() == 1);

    if (has_start) {
      if (is_perfect(map_element) &&
          uc->Compare(ExtractUserKey(map_element.smallest_key_),
                      range.limit) != 0) {
        has_start = false;
        assign_user_key(range.limit, map_element.smallest_key_);
        range.include_start = true;
        range.include_limit = false;
        input_range.emplace_back(std::move(range));
        if (input_range.size() >= ioptions_.max_subcompactions) {
          break;
        }
      } else {
        assign_user_key(range.limit, map_element.largest_key_);
      }
    } else {
      if (is_perfect(map_element)) {
        continue;
      }
      has_start = true;
      assign_user_key(range.start, map_element.smallest_key_);
      assign_user_key(range.limit, map_element.largest_key_);
    }
  }
  if (has_start) {
    range.include_start = true;
    range.include_limit = true;
    input_range.emplace_back(std::move(range));
  }
  if (!input_range.empty()) {
    compaction_varieties = kGeneralSst;
    single_output = false;
    return new_compaction();
  }
  return nullptr;
}

namespace {
  struct RankingElem {
    double   cur_sr_ratio; // = sr->size / size_sum
    double   max_sr_ratio;
    uint64_t size_sum;
    uint64_t size_max_val; // most cases is sr->size
    size_t   size_max_idx;
    size_t   real_idx;
  };
  struct Candidate {
    size_t   start;
    size_t   count;
    uint64_t max_sr_size;
    double   max_sr_ratio;
  };
}  // namespace

//
// Consider compaction files based on their size differences with
// the next file in time order.
//
Compaction* UniversalCompactionPicker::PickCompactionToReduceSortedRuns(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, double score, unsigned int ratio,
    unsigned int max_number_of_files_to_compact,
    const std::vector<SortedRun>& sorted_runs, LogBuffer* log_buffer) {
  unsigned int min_merge_width =
      mutable_cf_options.compaction_options_universal.min_merge_width;
  unsigned int max_merge_width =
      mutable_cf_options.compaction_options_universal.max_merge_width;

  size_t start_index = 0;
  size_t candidate_count = 0;

  size_t write_buffer_size = mutable_cf_options.write_buffer_size;
  double skip_min_ratio = max_merge_width * std::log2(max_merge_width);
  unsigned int max_files_to_compact =
      std::max(2U, std::min(max_merge_width, max_number_of_files_to_compact));
  min_merge_width = std::max(min_merge_width, 2U);
  min_merge_width = std::min(min_merge_width, max_files_to_compact);

  // Caller checks the size before executing this function. This invariant is
  // important because otherwise we may have a possible integer underflow when
  // dealing with unsigned types.
  assert(sorted_runs.size() > 0);

  std::vector<RankingElem> rankingVec(sorted_runs.size());
  auto computeRanking = [&](size_t start_idx, size_t count) {
    auto& rv = rankingVec;
    rv.resize(count);
    rv[0].cur_sr_ratio = 1.0;
    rv[0].max_sr_ratio = 1.0;
    rv[0].size_max_idx = 0;
    rv[0].size_max_val = sorted_runs[start_idx].compensated_file_size;
    rv[0].size_sum = sorted_runs[start_idx].compensated_file_size;
    rv[0].real_idx = start_idx;
    for (size_t i = 1; i < count; i++) {
      auto& sr1 = sorted_runs[start_idx + i];
      if (sr1.compensated_file_size > rv[i-1].size_max_val) {
        rv[i].size_max_val = sr1.compensated_file_size;
        rv[i].size_max_idx = i;
      } else {
        rv[i].size_max_val = rv[i-1].size_max_val;
        rv[i].size_max_idx = rv[i-1].size_max_idx;
      }
      rv[i].size_sum = rv[i-1].size_sum + sr1.compensated_file_size;
      rv[i].cur_sr_ratio = double(sr1.compensated_file_size) / rv[i].size_sum;
      rv[i].max_sr_ratio = double(rv[i].size_max_val) / rv[i].size_sum;
      rv[i].real_idx = i;
    }
    // a best candidate is which max_sr_ratio is the smallest
    std::sort(rv.begin(), rv.begin() + count,
              [](const RankingElem& x, const RankingElem& y) {
                return x.max_sr_ratio < y.max_sr_ratio;
              });
  };         
  std::vector<const SortedRun*> sr_bysize(sorted_runs.size());
  for (size_t i = 0; i < sr_bysize.size(); i++) {
    sr_bysize[i] = &sorted_runs[i];
  }
  std::stable_sort(sr_bysize.begin(), sr_bysize.end(),
                   [write_buffer_size](const SortedRun* x, const SortedRun* y) {
                     size_t x_rough =
                        x->compensated_file_size / write_buffer_size;
                     size_t y_rough =
                        y->compensated_file_size / write_buffer_size;
                     return x_rough < y_rough;
                   });

  std::vector<Candidate> candidate_vec;
  candidate_vec.reserve(sorted_runs.size());

  auto discard_small_sr = [&](uint64_t max_sr_size) {
    while (candidate_count > min_merge_width &&
           sorted_runs[start_index].size * skip_min_ratio < max_sr_size) {
      char file_num_buf[kFormatFileNumberBufSize];
      sorted_runs[start_index].Dump(file_num_buf, sizeof(file_num_buf),
                                    true);
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Universal: min/max = %7.5f too small, "
                       "Skipping %s", cf_name.c_str(),
                       sorted_runs[start_index].size / double(max_sr_size),
                       file_num_buf);
      ++start_index;
      --candidate_count;
    }
  };

  // Considers a candidate file only if it is smaller than the
  // total size accumulated so far.
  const SortedRun* sr = nullptr;
  for (size_t loop1 = 0; loop1 < sr_bysize.size(); loop1++) {
    sr = sr_bysize[loop1];
    if (sr->being_compacted) {
      continue;
    }
    size_t loop = sr - &sorted_runs[0];
    candidate_count = 0;

    char file_num_buf[kFormatFileNumberBufSize];
    sr->Dump(file_num_buf, sizeof(file_num_buf), true);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: Possible candidate %s[%d].",
                     cf_name.c_str(), file_num_buf, loop);

    // ignore compaction_options_universal.size_ratio and stop_style
    uint64_t sum_sr_size = 0;
    uint64_t max_sr_size = 0;
    size_t limit = std::min(sorted_runs.size(), loop + max_files_to_compact);
    for (size_t i = loop; i < limit && !sorted_runs[i].being_compacted; i++) {
      auto x = sorted_runs[i].compensated_file_size;
      sum_sr_size += x;
      max_sr_size = std::max(max_sr_size, x);
      candidate_count++;
    }

    auto logSkipping = [&](const char* reason) {
      size_t limit2 = std::min(loop + candidate_count, sorted_runs.size());
      for (size_t i = loop; i < limit2; i++) {
        sorted_runs[i].DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
        ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal:%s Skipping %s",
                         cf_name.c_str(), reason, file_num_buf);
      }
    };

    // Found a series of consecutive files that need compaction.
    if (candidate_count >= min_merge_width) {
      computeRanking(loop, candidate_count);
      double max_sr_ratio = double(max_sr_size) / sum_sr_size;
      for (size_t rank = 0; rank < candidate_count - min_merge_width; rank++) {
        size_t merge_width = rankingVec[rank].real_idx + 1;
        if (merge_width >= min_merge_width) {
          max_sr_ratio = rankingVec[rank].max_sr_ratio;
          max_sr_size = rankingVec[rank].size_max_val;
          sum_sr_size = rankingVec[rank].size_sum;
          candidate_count = merge_width;
          // found a best picker which start from loop and has
          // at least min_merge_width sorted runs
          break;
        }
      }
      Candidate cand;
      cand.count = candidate_count;
      cand.start = loop;
      cand.max_sr_size  = max_sr_size;
      cand.max_sr_ratio = max_sr_ratio;
      candidate_vec.push_back(cand);
    } else if (candidate_count > 1) { // do not print if candidate_count == 1
      char buf[80];
      sprintf(buf, " candidate_count(%zd) < min_merge_width(%d),"
                 , candidate_count, min_merge_width);
      logSkipping(buf);
    }
  }
  if (candidate_vec.size() < 1) {
    return nullptr;
  }
  auto best = candidate_vec.begin();
  for (auto iter = best + 1; iter < candidate_vec.end(); ++iter) {
    if (iter->max_sr_ratio < best->max_sr_ratio) {
      best = iter;
    }
  }
  char file_num_buf[kFormatFileNumberBufSize];
  sorted_runs[best->start].Dump(file_num_buf, sizeof(file_num_buf), true);
  ROCKS_LOG_BUFFER(log_buffer,
                   "[%s] Universal: repicked candidate %s[%d], "
                   "count = %zd, max/sum = %7.5f",
                   cf_name.c_str(), file_num_buf, best->start,
                   best->count, best->max_sr_ratio);
  start_index = best->start;
  candidate_count = best->count;
  discard_small_sr(best->max_sr_size);

  size_t first_index_after = start_index + candidate_count;
  // Compression is enabled if files compacted earlier already reached
  // size ratio of compression.
  bool enable_compression = true;
  int ratio_to_compress =
      mutable_cf_options.compaction_options_universal.compression_size_percent;
  if (ratio_to_compress >= 0) {
    uint64_t total_size = 0;
    for (auto& sorted_run : sorted_runs) {
      total_size += sorted_run.compensated_file_size;
    }

    uint64_t older_file_size = 0;
    for (size_t i = sorted_runs.size() - 1; i >= first_index_after; i--) {
      older_file_size += sorted_runs[i].size;
      if (older_file_size * 100L >= total_size * (long)ratio_to_compress) {
        enable_compression = false;
        break;
      }
    }
  }

  uint64_t estimated_total_size = 0;
  for (size_t i = start_index; i < first_index_after; i++) {
    estimated_total_size += sorted_runs[i].size;
  }
  uint32_t path_id = GetPathId(ioptions_, mutable_cf_options,
                               estimated_total_size);
  int start_level = sorted_runs[start_index].level;
  int output_level;
  if (first_index_after == sorted_runs.size()) {
    output_level = vstorage->num_levels() - 1;
  } else if (sorted_runs[first_index_after].level == 0) {
    output_level = 0;
  } else {
    output_level = sorted_runs[first_index_after].level - 1;
  }

  // last level is reserved for the files ingested behind
  if (ioptions_.allow_ingest_behind &&
      (output_level == vstorage->num_levels() - 1)) {
    assert(output_level > 1);
    output_level--;
  }

  std::vector<CompactionInputFiles> inputs(vstorage->num_levels());
  for (size_t i = 0; i < inputs.size(); ++i) {
    inputs[i].level = start_level + static_cast<int>(i);
  }
  for (size_t i = start_index; i < first_index_after; i++) {
    auto& picking_sr = sorted_runs[i];
    if (picking_sr.level == 0) {
      FileMetaData* picking_file = picking_sr.file;
      inputs[0].files.push_back(picking_file);
    } else {
      auto& files = inputs[picking_sr.level - start_level].files;
      for (auto* f : vstorage->LevelFiles(picking_sr.level)) {
        files.push_back(f);
      }
    }
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), i);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: Picking %s", cf_name.c_str(),
                     file_num_buf);
  }

  CompactionReason compaction_reason;
  if (max_number_of_files_to_compact == UINT_MAX) {
    compaction_reason = CompactionReason::kUniversalSortedRunNum;
  } else {
    compaction_reason = CompactionReason::kUniversalSizeRatio;
  }
  SstVarieties compaction_varieties = kGeneralSst;
  uint32_t max_subcompactions = 0;
  if (mutable_cf_options.enable_lazy_compaction && output_level != 0) {
    compaction_varieties = kMapSst;
    max_subcompactions = 1;
  }
  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, std::move(inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          kCompactionStyleUniversal),
      LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, start_level,
                         1, enable_compression),
      GetCompressionOptions(ioptions_, vstorage, start_level,
                            enable_compression),
      max_subcompactions, /* grandparents */ {}, /* is manual */ false,
      score, false /* deletion_compaction */, false /* single_output */,
      false /* enable_partial_compaction */, compaction_varieties,
      {} /* input_range */, compaction_reason);
}

// Look at overall size amplification. If size amplification
// exceeeds the configured value, then do a compaction
// of the candidate files all the way upto the earliest
// base file (overrides configured values of file-size ratios,
// min_merge_width and max_merge_width).
//
Compaction* UniversalCompactionPicker::PickCompactionToReduceSizeAmp(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, double score,
    const std::vector<SortedRun>& sorted_runs, LogBuffer* log_buffer) {
  // percentage flexibility while reducing size amplification
  uint64_t ratio = mutable_cf_options.compaction_options_universal
                       .max_size_amplification_percent;

  unsigned int candidate_count = 0;
  uint64_t candidate_size = 0;
  size_t start_index = 0;
  const SortedRun* sr = nullptr;

  if (sorted_runs.back().being_compacted) {
    return nullptr;
  }

  // Skip files that are already being compacted
  for (size_t loop = 0; loop < sorted_runs.size() - 1; loop++) {
    sr = &sorted_runs[loop];
    if (!sr->being_compacted) {
      start_index = loop;  // Consider this as the first candidate.
      break;
    }
    char file_num_buf[kFormatFileNumberBufSize];
    sr->Dump(file_num_buf, sizeof(file_num_buf), true);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: skipping %s[%d] compacted %s",
                     cf_name.c_str(), file_num_buf, loop,
                     " cannot be a candidate to reduce size amp.\n");
    sr = nullptr;
  }

  if (sr == nullptr) {
    return nullptr;  // no candidate files
  }
  {
    char file_num_buf[kFormatFileNumberBufSize];
    sr->Dump(file_num_buf, sizeof(file_num_buf), true);
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Universal: First candidate %s[%" ROCKSDB_PRIszt "] %s",
        cf_name.c_str(), file_num_buf, start_index, " to reduce size amp.\n");
  }

  // keep adding up all the remaining files
  for (size_t loop = start_index; loop < sorted_runs.size() - 1; loop++) {
    sr = &sorted_runs[loop];
    if (sr->being_compacted) {
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf), true);
      ROCKS_LOG_BUFFER(
          log_buffer, "[%s] Universal: Possible candidate %s[%d] %s",
          cf_name.c_str(), file_num_buf, start_index,
          " is already being compacted. No size amp reduction possible.\n");
      return nullptr;
    }
    candidate_size += sr->compensated_file_size;
    candidate_count++;
  }
  if (candidate_count == 0) {
    return nullptr;
  }

  // size of earliest file
  uint64_t earliest_file_size = sorted_runs.back().size;

  // size amplification = percentage of additional size
  if (candidate_size * 100 < ratio * earliest_file_size) {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Universal: size amp not needed. newer-files-total-size %" PRIu64
        " earliest-file-size %" PRIu64,
        cf_name.c_str(), candidate_size, earliest_file_size);
    return nullptr;
  } else {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Universal: size amp needed. newer-files-total-size %" PRIu64
        " earliest-file-size %" PRIu64,
        cf_name.c_str(), candidate_size, earliest_file_size);
  }
  assert(start_index < sorted_runs.size() - 1);

  // Estimate total file size
  uint64_t estimated_total_size = 0;
  for (size_t loop = start_index; loop < sorted_runs.size(); loop++) {
    estimated_total_size += sorted_runs[loop].size;
  }
  uint32_t path_id =
      GetPathId(ioptions_, mutable_cf_options, estimated_total_size);
  int start_level = sorted_runs[start_index].level;

  std::vector<CompactionInputFiles> inputs(vstorage->num_levels());
  for (size_t i = 0; i < inputs.size(); ++i) {
    inputs[i].level = start_level + static_cast<int>(i);
  }
  // We always compact all the files, so always compress.
  for (size_t loop = start_index; loop < sorted_runs.size(); loop++) {
    auto& picking_sr = sorted_runs[loop];
    if (picking_sr.level == 0) {
      FileMetaData* f = picking_sr.file;
      inputs[0].files.push_back(f);
    } else {
      auto& files = inputs[picking_sr.level - start_level].files;
      for (auto* f : vstorage->LevelFiles(picking_sr.level)) {
        files.push_back(f);
      }
    }
    char file_num_buf[256];
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: size amp picking %s",
                     cf_name.c_str(), file_num_buf);
  }

  // output files at the bottom most level, unless it's reserved
  int output_level = vstorage->num_levels() - 1;
  // last level is reserved for the files ingested behind
  if (ioptions_.allow_ingest_behind) {
    assert(output_level > 1);
    output_level--;
  }

  SstVarieties compaction_varieties = kGeneralSst;
  uint32_t max_subcompactions = 0;
  if (mutable_cf_options.enable_lazy_compaction && output_level != 0) {
    compaction_varieties = kMapSst;
    max_subcompactions = 1;
  }
  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, std::move(inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          kCompactionStyleUniversal),
      /* max_grandparent_overlap_bytes */ LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, output_level,
                         1),
      GetCompressionOptions(ioptions_, vstorage, output_level),
      max_subcompactions, /* grandparents */ {}, /* is manual */ false,
      score, false /* deletion_compaction */, false /* single_output */,
      false /* enable_partial_compaction */, compaction_varieties,
      {} /* input_range */, CompactionReason::kUniversalSizeAmplification);
}

// Pick files marked for compaction. Typically, files are marked by
// CompactOnDeleteCollector due to the presence of tombstones.
Compaction* UniversalCompactionPicker::PickDeleteTriggeredCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, double score,
    const std::vector<SortedRun>& /*sorted_runs*/, LogBuffer* /*log_buffer*/) {
  CompactionInputFiles start_level_inputs;
  int output_level;
  std::vector<CompactionInputFiles> inputs;

  if (vstorage->num_levels() == 1) {
    // This is single level universal. Since we're basically trying to reclaim
    // space by processing files marked for compaction due to high tombstone
    // density, let's do the same thing as compaction to reduce size amp which
    // has the same goals.
    bool compact = false;

    start_level_inputs.level = 0;
    start_level_inputs.files.clear();
    output_level = 0;
    for (FileMetaData* f : vstorage->LevelFiles(0)) {
      if (f->marked_for_compaction) {
        compact = true;
      }
      if (compact) {
        start_level_inputs.files.push_back(f);
      }
    }
    if (start_level_inputs.size() <= 1) {
      // If only the last file in L0 is marked for compaction, ignore it
      return nullptr;
    }
    inputs.push_back(start_level_inputs);
  } else {
    int start_level;

    // For multi-level universal, the strategy is to make this look more like
    // leveled. We pick one of the files marked for compaction and compact with
    // overlapping files in the adjacent level.
    PickFilesMarkedForCompaction(cf_name, vstorage, &start_level, &output_level,
                                 &start_level_inputs);
    if (start_level_inputs.empty()) {
      return nullptr;
    }

    // Pick the first non-empty level after the start_level
    for (output_level = start_level + 1; output_level < vstorage->num_levels();
         output_level++) {
      if (vstorage->NumLevelFiles(output_level) != 0) {
        break;
      }
    }

    // If all higher levels are empty, pick the highest level as output level
    if (output_level == vstorage->num_levels()) {
      if (start_level == 0) {
        output_level = vstorage->num_levels() - 1;
      } else {
        // If start level is non-zero and all higher levels are empty, this
        // compaction will translate into a trivial move. Since the idea is
        // to reclaim space and trivial move doesn't help with that, we
        // skip compaction in this case and return nullptr
        return nullptr;
      }
    }
    if (ioptions_.allow_ingest_behind &&
        output_level == vstorage->num_levels() - 1) {
      assert(output_level > 1);
      output_level--;
    }

    if (output_level != 0) {
      if (start_level == 0) {
        if (!GetOverlappingL0Files(vstorage, &start_level_inputs, output_level,
                                   nullptr)) {
          return nullptr;
        }
      }

      CompactionInputFiles output_level_inputs;
      int parent_index = -1;

      output_level_inputs.level = output_level;
      if (!SetupOtherInputs(cf_name, mutable_cf_options, vstorage,
                            &start_level_inputs, &output_level_inputs,
                            &parent_index, -1)) {
        return nullptr;
      }
      inputs.push_back(start_level_inputs);
      if (!output_level_inputs.empty()) {
        inputs.push_back(output_level_inputs);
      }
      if (FilesRangeOverlapWithCompaction(inputs, output_level)) {
        return nullptr;
      }
    } else {
      inputs.push_back(start_level_inputs);
    }
  }

  uint64_t estimated_total_size = 0;
  // Use size of the output level as estimated file size
  for (FileMetaData* f : vstorage->LevelFiles(output_level)) {
    estimated_total_size += f->fd.GetFileSize();
  }
  uint32_t path_id =
      GetPathId(ioptions_, mutable_cf_options, estimated_total_size);
  SstVarieties compaction_varieties = kGeneralSst;
  uint32_t max_subcompactions = 0;
  if (mutable_cf_options.enable_lazy_compaction && output_level != 0) {
    compaction_varieties = kMapSst;
    max_subcompactions = 1;
  }
  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, std::move(inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          kCompactionStyleUniversal),
      /* max_grandparent_overlap_bytes */ LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, output_level,
                         1),
      GetCompressionOptions(ioptions_, vstorage, output_level),
      max_subcompactions, /* grandparents */ {}, /* is manual */ true,
      score, false /* deletion_compaction */, false /* single_output */,
      false /* enable_partial_compaction */, compaction_varieties,
      {} /* input_range */, CompactionReason::kFilesMarkedForCompaction);
}
}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
