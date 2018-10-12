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
#include <list>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "db/builder.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "monitoring/thread_status_util.h"
#include "util/c_style_callback.h"
#include "util/iterator_cache.h"
#include "util/sst_file_manager_impl.h"

namespace rocksdb {

struct FileMetaDataBoundBuilder {
  const InternalKeyComparator& icomp;
  InternalKey smallest, largest;
  SequenceNumber smallest_seqno;
  SequenceNumber largest_seqno;
  uint64_t creation_time;

  FileMetaDataBoundBuilder(const InternalKeyComparator& _icomp)
      : icomp(_icomp),
        smallest_seqno(kMaxSequenceNumber),
        largest_seqno(0),
        creation_time(0) {}

  void Update(const FileMetaData* f) {
    if (smallest.size() == 0 || icomp.Compare(f->smallest, smallest) < 0) {
      smallest = f->smallest;
    }
    if (largest.size() == 0 || icomp.Compare(f->largest, largest) > 0) {
      largest = f->largest;
    }
    smallest_seqno = std::min(smallest_seqno, f->fd.smallest_seqno);
    largest_seqno = std::max(largest_seqno, f->fd.largest_seqno);
  }
};

bool IsPrefaceRange(const Range& range, const FileMetaData* f,
                    const InternalKeyComparator& icomp) {
  assert(f->sst_purpose != kMapSst);
  if (f->sst_purpose != kEssenceSst || !range.include_start ||
      icomp.Compare(f->smallest.Encode(), range.start) != 0) {
    return false;
  }
  if (range.include_limit) {
    if (icomp.Compare(f->largest.Encode(), range.limit) != 0) {
      return false;
    }
  } else {
    ParsedInternalKey pikey;
    if (!ParseInternalKey(range.limit, &pikey)) {
      // TODO log error
      return false;
    }
    if (pikey.sequence != kMaxSequenceNumber ||
        icomp.user_comparator()->Compare(f->largest.user_key(),
                                         pikey.user_key) >= 0) {
      return false;
    }
  }
  return true;
}

namespace {

struct RangeWithDepend {
  InternalKey point[2];
  bool include[2];
  std::vector<uint64_t> depend;

  RangeWithDepend() = default;

  RangeWithDepend(const FileMetaData* f) {
    point[0] = f->smallest;
    point[1] = f->largest;
    include[0] = true;
    include[1] = true;
    depend.push_back(f->fd.GetNumber());
  }

  RangeWithDepend(const MapSstElement& map_element) {
    point[0].DecodeFrom(map_element.smallest_key_);
    point[1].DecodeFrom(map_element.largest_key_);
    include[0] = map_element.include_smallest_;
    include[1] = map_element.include_largest_;
    depend.resize(map_element.link_.size());
    for (size_t i = 0; i < depend.size(); ++i) {
      depend[i] = map_element.link_[i].file_number;
    }
  }
  RangeWithDepend(const Range& range) {
    point[0].DecodeFrom(range.start);
    point[1].DecodeFrom(range.limit);
    include[0] = range.include_start;
    include[1] = range.include_limit;
  }
};

bool IsEmptyMapSstElement(const RangeWithDepend& range,
                          const InternalKeyComparator& icomp) {
  if (range.depend.size() != 1) {
    return false;
  }
  if (icomp.user_comparator()->Compare(range.point[0].user_key(),
                                       range.point[1].user_key()) != 0) {
    return false;
  }
  ParsedInternalKey pikey;
  if (!ParseInternalKey(range.point[1].Encode(), &pikey)) {
    // TODO log error
    return false;
  }
  return pikey.sequence == kMaxSequenceNumber;
}

int CompInclude(int c, size_t ab, size_t ai, size_t bb, size_t bi) {
#define CASE(a, b, c, d) \
  (((a) ? 1 : 0) | ((b) ? 2 : 0) | ((c) ? 4 : 0) | ((d) ? 8 : 0))
  if (c != 0) {
    return c;
  }
  switch (CASE(ab, ai, bb, bi)) {
    // a: [   [   (   )   )   [
    // b: (   )   ]   ]   (   ]
    case CASE(0, 1, 0, 0):
    case CASE(0, 1, 1, 0):
    case CASE(0, 0, 1, 1):
    case CASE(1, 0, 1, 1):
    case CASE(1, 0, 0, 0):
    case CASE(0, 1, 1, 1):
      return -1;
    // a: (   )   ]   ]   (   ]
    // b: [   [   (   )   )   [
    case CASE(0, 0, 0, 1):
    case CASE(1, 0, 0, 1):
    case CASE(1, 1, 0, 0):
    case CASE(1, 1, 1, 0):
    case CASE(0, 0, 1, 0):
    case CASE(1, 1, 0, 1):
      return 1;
    // a: [   ]   (   )
    // b: [   ]   (   )
    default:
      return 0;
  }
#undef CASE
}
}  // namespace

class MapSstElementIterator {
 public:
  MapSstElementIterator(const std::vector<RangeWithDepend>& ranges,
                        IteratorCache& iterator_cache,
                        const InternalKeyComparator& icomp)
      : ranges_(ranges), iterator_cache_(iterator_cache), icomp_(icomp) {}
  bool Valid() const { return !buffer_.empty(); }
  void SeekToFirst() {
    where_ = ranges_.begin();
    PrepareNext();
  }
  void Next() { PrepareNext(); }
  Slice key() const { return map_elements_.Key(); }
  Slice value() const { return buffer_; }
  Status status() const { return status_; }

  const std::unordered_set<uint64_t>& GetSstDepend() const {
    return sst_depend_build_;
  }

  size_t GetSstReadAmp() const { return sst_read_amp_; }

 private:
  void PrepareNext() {
    if (where_ == ranges_.end()) {
      buffer_.clear();
      return;
    }
    auto& start = map_elements_.smallest_key_ = where_->point[0].Encode();
    auto& end = map_elements_.largest_key_ = where_->point[1].Encode();
    assert(icomp_.Compare(start, end) <= 0);
    bool& include_start = map_elements_.include_smallest_ = where_->include[0];
    bool& include_end = map_elements_.include_largest_ = where_->include[1];
    bool no_records = true;
    map_elements_.link_.clear();
    for (auto file_number : where_->depend) {
      map_elements_.link_.emplace_back(
          MapSstElement::LinkTarget{file_number, 0});
    }

    auto merge_depend = [](MapSstElement& e, const std::vector<uint64_t>& d) {
      size_t insert_pos = e.link_.size();
      for (auto rit = d.rbegin(); rit != d.rend(); ++rit) {
        size_t new_pos;
        for (new_pos = 0; new_pos < insert_pos; ++new_pos) {
          if (e.link_[new_pos].file_number == *rit) {
            break;
          }
        }
        if (new_pos == insert_pos) {
          e.link_.emplace(e.link_.begin() + new_pos,
                          MapSstElement::LinkTarget{*rit, 0});
        } else {
          insert_pos = new_pos;
        }
      }
    };

    ++where_;
    if (where_ != ranges_.end() &&
        icomp_.Compare(start, where_->point[0].Encode()) == 0) {
      assert(include_start && include_end && !where_->include[0]);
      assert(icomp_.Compare(start, end) == 0);
      end = where_->point[1].Encode();
      include_end = where_->include[1];
      merge_depend(map_elements_, where_->depend);
      ++where_;
    }
    if (where_ != ranges_.end() &&
        icomp_.Compare(end, where_->point[1].Encode()) == 0) {
      assert(!include_end && where_->include[0] && where_->include[1]);
      assert(icomp_.Compare(where_->point[0], where_->point[1]) == 0);
      include_end = true;
      merge_depend(map_elements_, where_->depend);
      ++where_;
    }

    for (auto& link : map_elements_.link_) {
      sst_depend_build_.emplace(link.file_number);
      TableReader* reader;
      auto iter = iterator_cache_.GetIterator(link.file_number, &reader);
      if (!iter->status().ok()) {
        buffer_.clear();
        status_ = iter->status();
        return;
      }
      iter->Seek(start);
      if (!iter->Valid()) {
        continue;
      }
      if (!include_start && icomp_.Compare(iter->key(), start) == 0) {
        iter->Next();
        if (!iter->Valid()) {
          continue;
        }
      }
      temp_start_.DecodeFrom(iter->key());
      iter->SeekForPrev(end);
      if (!iter->Valid()) {
        continue;
      }
      if (!include_end && icomp_.Compare(iter->key(), end) == 0) {
        iter->Prev();
        if (!iter->Valid()) {
          continue;
        }
      }
      temp_end_.DecodeFrom(iter->key());
      if (icomp_.Compare(temp_start_, temp_end_) <= 0) {
        uint64_t start_offset =
            reader->ApproximateOffsetOf(temp_start_.Encode());
        uint64_t end_offset = reader->ApproximateOffsetOf(temp_end_.Encode());
        no_records = false;
        link.size = end_offset - start_offset;
      }
    }
    map_elements_.no_records_ = no_records;
    sst_read_amp_ = std::max(sst_read_amp_, map_elements_.link_.size());
    map_elements_.Value(&buffer_);  // Encode value
  }

 private:
  Status status_;
  MapSstElement map_elements_;
  InternalKey temp_start_, temp_end_;
  std::string buffer_;
  std::vector<RangeWithDepend>::const_iterator where_;
  const std::vector<RangeWithDepend>& ranges_;
  std::unordered_set<uint64_t> sst_depend_build_;
  size_t sst_read_amp_ = 0;
  IteratorCache& iterator_cache_;
  const InternalKeyComparator& icomp_;
};

namespace {

Status LoadRangeWithDepend(std::vector<RangeWithDepend>& ranges,
                           FileMetaDataBoundBuilder* bound_builder,
                           IteratorCache& iterator_cache,
                           const FileMetaData* const* file_meta, size_t n) {
  MapSstElement map_element;
  for (size_t i = 0; i < n; ++i) {
    auto f = file_meta[i];
    TableReader* reader;
    if (f->sst_purpose == kMapSst) {
      auto iter = iterator_cache.GetIterator(f, &reader);
      assert(iter != nullptr);
      if (!iter->status().ok()) {
        return iter->status();
      }
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (!map_element.Decode(iter->key(), iter->value())) {
          return Status::Corruption("Map sst invalid key or value");
        }
        ranges.emplace_back(map_element);
      }
    } else {
      auto iter = iterator_cache.GetIterator(f, &reader);
      assert(iter != nullptr);
      if (!iter->status().ok()) {
        return iter->status();
      }
      ranges.emplace_back(f);
    }
    if (bound_builder != nullptr) {
      bound_builder->Update(f);
      bound_builder->creation_time =
          std::max(bound_builder->creation_time,
                   reader->GetTableProperties()->creation_time);
    }
  }
  return Status::OK();
}

enum class PartitionType {
  kMerge,
  kReplace,
  kDelete,
};

// Partition two sorted non-overlap range vector
// a: [ -------- )      [ -------- ]
// b:       ( -------------- ]
// r: [ -- ]( -- )[ -- )[ -- ]( -- ]
std::vector<RangeWithDepend> PartitionRangeWithDepend(
    const std::vector<RangeWithDepend>& ranges_a,
    const std::vector<RangeWithDepend>& ranges_b,
    const InternalKeyComparator& icomp, PartitionType type) {
  std::vector<RangeWithDepend> output;
  assert(!ranges_a.empty() && !ranges_b.empty());
  auto put_left = [&](const InternalKey& key, bool include) {
    assert(output.empty() || icomp.Compare(output.back().point[1], key) < 0 ||
           !output.back().include[1] || !include);
    output.emplace_back();
    auto& back = output.back();
    back.point[0] = key;
    back.include[0] = include;
  };
  auto put_right = [&](const InternalKey& key, bool include) {
    auto& back = output.back();
    if (back.depend.empty() || (icomp.Compare(key, back.point[0]) == 0 &&
                                (!back.include[0] || !include))) {
      output.pop_back();
      return;
    }
    back.point[1] = key;
    back.include[1] = include;
    assert(icomp.Compare(back.point[0], back.point[1]) <= 0);
    if (IsEmptyMapSstElement(back, icomp)) {
      output.pop_back();
    }
  };
  auto put_depend = [&](const RangeWithDepend* a, const RangeWithDepend* b) {
    auto& depend = output.back().depend;
    assert(a != nullptr || b != nullptr);
    switch (type) {
      case PartitionType::kMerge:
        if (a != nullptr) {
          depend.insert(depend.end(), a->depend.begin(), a->depend.end());
        }
        if (b != nullptr) {
          depend.insert(depend.end(), b->depend.begin(), b->depend.end());
        }
        assert(!depend.empty());
        break;
      case PartitionType::kReplace:
        if (b == nullptr) {
          depend.insert(depend.end(), a->depend.begin(), a->depend.end());
        } else if (a != nullptr) {
          depend.insert(depend.end(), b->depend.begin(), b->depend.end());
        }
        break;
      case PartitionType::kDelete:
        if (b == nullptr) {
          depend.insert(depend.end(), a->depend.begin(), a->depend.end());
        } else {
          assert(b->depend.empty());
        }
        break;
    }
  };
  size_t ai = 0, bi = 0;  // range index
  size_t ac, bc;          // changed
  size_t ab = 0, bb = 0;  // left bound or right bound
#define CASE(a, b, c, d) \
  (((a) ? 1 : 0) | ((b) ? 2 : 0) | ((c) ? 4 : 0) | ((d) ? 8 : 0))
  do {
    int c;
    if (ai < ranges_a.size() && bi < ranges_b.size()) {
      c = icomp.Compare(ranges_a[ai].point[ab], ranges_b[bi].point[bb]);
      c = CompInclude(c, ab, ranges_a[ai].include[ab], bb,
                      ranges_b[bi].include[bb]);
    } else {
      c = ai < ranges_a.size() ? -1 : 1;
    }
    ac = c <= 0;
    bc = c >= 0;
    switch (CASE(ab, bb, ac, bc)) {
      // out ranges_a , out ranges_b , enter ranges_a
      case CASE(0, 0, 1, 0):
        put_left(ranges_a[ai].point[ab], ranges_a[ai].include[ab]);
        put_depend(&ranges_a[ai], nullptr);
        break;
      // in ranges_a , out ranges_b , leave ranges_a
      case CASE(1, 0, 1, 0):
        put_right(ranges_a[ai].point[ab], ranges_a[ai].include[ab]);
        break;
      // out ranges_a , out ranges_b , enter ranges_b
      case CASE(0, 0, 0, 1):
        put_left(ranges_b[bi].point[bb], ranges_b[bi].include[bb]);
        put_depend(nullptr, &ranges_b[bi]);
        break;
      // out ranges_a , in ranges_b , leave ranges_b
      case CASE(0, 1, 0, 1):
        put_right(ranges_b[bi].point[bb], ranges_b[bi].include[bb]);
        break;
      // in ranges_a , out ranges_b , begin ranges_b
      case CASE(1, 0, 0, 1):
        put_right(ranges_b[bi].point[bb], !ranges_b[bi].include[bb]);
        put_left(ranges_b[bi].point[bb], ranges_b[bi].include[bb]);
        put_depend(&ranges_a[ai], &ranges_b[bi]);
        break;
      // in ranges_a , in ranges_b , leave ranges_b
      case CASE(1, 1, 0, 1):
        put_right(ranges_b[bi].point[bb], ranges_b[bi].include[bb]);
        put_left(ranges_b[bi].point[bb], !ranges_b[bi].include[bb]);
        put_depend(&ranges_a[ai], nullptr);
        break;
      // out ranges_a , in ranges_b , begin ranges_a
      case CASE(0, 1, 1, 0):
        put_right(ranges_a[ai].point[ab], !ranges_a[ai].include[ab]);
        put_left(ranges_a[ai].point[ab], ranges_a[ai].include[ab]);
        put_depend(&ranges_a[ai], &ranges_b[bi]);
        break;
      // in ranges_a , in ranges_b , leave ranges_a
      case CASE(1, 1, 1, 0):
        put_right(ranges_a[ai].point[ab], ranges_a[ai].include[ab]);
        put_left(ranges_a[ai].point[ab], !ranges_a[ai].include[ab]);
        put_depend(nullptr, &ranges_b[bi]);
        break;
      // out ranges_a , out ranges_b , enter ranges_a , enter ranges_b
      case CASE(0, 0, 1, 1):
        put_left(ranges_a[ai].point[ab], ranges_a[ai].include[ab]);
        put_depend(&ranges_a[ai], &ranges_b[bi]);
        break;
      // in ranges_a , in ranges_b , leave ranges_a , leave ranges_b
      case CASE(1, 1, 1, 1):
        put_right(ranges_a[ai].point[ab], ranges_a[ai].include[ab]);
        break;
      default:
        assert(false);
    }
    ai += (ab + ac) / 2;
    bi += (bb + bc) / 2;
    ab = (ab + ac) % 2;
    bb = (bb + bc) % 2;
  } while (ai != ranges_a.size() || bi != ranges_b.size());
#undef CASE
  return output;
}

}  // namespace

MapBuilder::MapBuilder(int job_id, const ImmutableDBOptions& db_options,
                       const EnvOptions& env_options, VersionSet* versions,
                       Statistics* stats, const std::string& dbname)
    : job_id_(job_id),
      dbname_(dbname),
      db_options_(db_options),
      env_options_(env_options),
      env_(db_options.env),
      env_options_for_read_(
          env_->OptimizeForCompactionTableRead(env_options, db_options_)),
      versions_(versions),
      stats_(stats) {}

Status MapBuilder::Build(const std::vector<CompactionInputFiles>& inputs,
                         const std::vector<Range>& deleted_range,
                         const std::vector<const FileMetaData*>& added_files,
                         SstPurpose compaction_purpose, int output_level,
                         uint32_t output_path_id, VersionStorageInfo* vstorage,
                         ColumnFamilyData* cfd, VersionEdit* edit,
                         FileMetaData* file_meta_ptr,
                         std::unique_ptr<TableProperties>* prop_ptr) {
  assert(compaction_purpose != kMapSst || deleted_range.empty() ||
         added_files.empty());
  assert(compaction_purpose != kLinkSst || !added_files.empty());

  auto& icomp = cfd->internal_comparator();
  DependFileMap empty_delend_files;

  auto create_iterator = [&](const FileMetaData* f,
                             const DependFileMap& depend_files, Arena* arena,
                             TableReader** reader_ptr) -> InternalIterator* {
    ReadOptions read_options;
    read_options.verify_checksums = true;
    read_options.fill_cache = false;
    read_options.total_order_seek = true;

    return cfd->table_cache()->NewIterator(
        read_options, env_options_for_read_, cfd->internal_comparator(), *f,
        f->sst_purpose == kMapSst ? empty_delend_files : depend_files, nullptr,
        cfd->GetCurrentMutableCFOptions()->prefix_extractor.get(), reader_ptr,
        nullptr /* no per level latency histogram */, true /* for_compaction */,
        arena, false /* skip_filters */, -1);
  };

  IteratorCache iterator_cache(vstorage->depend_files(), &create_iterator,
                               c_style_callback(create_iterator));

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
        assert(std::is_sorted(
            ranges.begin(), ranges.end(),
            [&icomp](const RangeWithDepend& a, const RangeWithDepend& b) {
              return icomp.Compare(a.point[1], b.point[1]) < 0;
            }));
        level_ranges.emplace_back(std::move(ranges));
      }
    } else {
      std::vector<RangeWithDepend> ranges;
      assert(std::is_sorted(
          level_files.files.begin(), level_files.files.end(),
          [&icomp](const FileMetaData* f1, const FileMetaData* f2) {
            return icomp.Compare(f1->largest, f2->largest) < 0;
          }));
      s = LoadRangeWithDepend(ranges, &bound_builder, iterator_cache,
                              level_files.files.data(),
                              level_files.files.size());
      if (!s.ok()) {
        return s;
      }
      assert(std::is_sorted(
          ranges.begin(), ranges.end(),
          [&icomp](const RangeWithDepend& a, const RangeWithDepend& b) {
            return icomp.Compare(a.point[1], b.point[1]) < 0;
          }));
      level_ranges.emplace_back(std::move(ranges));
    }
  }

  // merge ranges
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
        union_a,
        PartitionRangeWithDepend(*union_a, *union_b, cfd->internal_comparator(),
                                 PartitionType::kMerge));
    level_ranges.erase(union_a);
    level_ranges.erase(union_b);
  }

  if (!level_ranges.empty() && !deleted_range.empty() &&
      compaction_purpose != kLinkSst) {
    std::vector<RangeWithDepend> ranges;
    ranges.reserve(deleted_range.size());
    for (auto& r : deleted_range) {
      ranges.emplace_back(r);
    }
    assert(std::is_sorted(
        ranges.begin(), ranges.end(),
        [&icomp](const RangeWithDepend& a, const RangeWithDepend& b) {
          return icomp.Compare(a.point[1], b.point[1]) < 0;
        }));
    level_ranges.front() = PartitionRangeWithDepend(
        level_ranges.front(), ranges, cfd->internal_comparator(),
        PartitionType::kDelete);
    if (level_ranges.front().empty()) {
      level_ranges.pop_front();
    }
  }
  if (!added_files.empty()) {
    std::vector<RangeWithDepend> ranges;
    assert(std::is_sorted(
        added_files.begin(), added_files.end(),
        [&icomp](const FileMetaData* f1, const FileMetaData* f2) {
          return icomp.Compare(f1->largest, f2->largest) < 0;
        }));
    s = LoadRangeWithDepend(ranges, &bound_builder, iterator_cache,
                            added_files.data(), added_files.size());
    if (!s.ok()) {
      return s;
    }
    if (level_ranges.empty()) {
      assert(compaction_purpose == kEssenceSst);
      level_ranges.emplace_back(std::move(ranges));
    } else {
      PartitionType type = compaction_purpose == kLinkSst
                               ? PartitionType::kReplace
                               : PartitionType::kMerge;
      level_ranges.front() = PartitionRangeWithDepend(
          level_ranges.front(), ranges, cfd->internal_comparator(), type);
    }
  }

  if (level_ranges.empty()) {
    for (auto& input_level : inputs) {
      for (auto f : input_level.files) {
        edit->DeleteFile(input_level.level, f->fd.GetNumber());
      }
    }
    return s;
  }
  auto& ranges = level_ranges.front();
  std::unordered_map<uint64_t, const FileMetaData*> sst_live;
  bool build_map_sst = false;
  // check is need build map
  for (auto it = ranges.begin(); it != ranges.end(); ++it) {
    if (it->depend.size() > 1) {
      build_map_sst = true;
      break;
    }
    auto f = iterator_cache.GetFileMetaData(it->depend.front());
    assert(f != nullptr);
    Range r(it->point[0].Encode(), it->point[1].Encode(), it->include[0],
            it->include[1]);
    if (!IsPrefaceRange(r, f, icomp)) {
      build_map_sst = true;
      break;
    }
    sst_live.emplace(it->depend.front(), f);
  }
  if (!build_map_sst) {
    // unnecessary build map sst
    for (auto& input_level : inputs) {
      for (auto f : input_level.files) {
        uint64_t file_number = f->fd.GetNumber();
        if (sst_live.count(file_number) > 0) {
          if (output_level != input_level.level) {
            edit->DeleteFile(input_level.level, file_number);
            edit->AddFile(output_level, *f);
          }
          sst_live.erase(file_number);
        } else {
          edit->DeleteFile(input_level.level, file_number);
        }
      }
    }
    for (auto& pair : sst_live) {
      edit->AddFile(output_level, *pair.second);
    }
    return s;
  }
  sst_live.clear();

  using IterType = MapSstElementIterator;
  void* buffer = iterator_cache.GetArena()->AllocateAligned(sizeof(IterType));
  std::unique_ptr<IterType, void (*)(IterType*)> output_iter(
      new (buffer) IterType(ranges, iterator_cache, cfd->internal_comparator()),
      [](IterType* iter) { iter->~IterType(); });

  assert(std::is_sorted(
      ranges.begin(), ranges.end(),
      [&icomp](const RangeWithDepend& f1, const RangeWithDepend& f2) {
        return icomp.Compare(f1.point[1], f2.point[1]) < 0;
      }));

  FileMetaData file_meta;
  std::unique_ptr<TableProperties> prop;

  s = WriteOutputFile(bound_builder, output_iter.get(), output_path_id, cfd,
                      &file_meta, &prop);

  if (s.ok()) {
    for (auto& input_level : inputs) {
      for (auto f : input_level.files) {
        edit->DeleteFile(input_level.level, f->fd.GetNumber());
      }
    }
    for (auto f : added_files) {
      edit->AddFile(-1, *f);
    }
    edit->AddFile(output_level, file_meta);
  }
  if (file_meta_ptr != nullptr) {
    *file_meta_ptr = std::move(file_meta);
  }
  if (prop_ptr != nullptr) {
    prop_ptr->swap(prop);
  }
  return s;
}

Status MapBuilder::WriteOutputFile(
    const FileMetaDataBoundBuilder& bound_builder,
    MapSstElementIterator* range_iter, uint32_t output_path_id,
    ColumnFamilyData* cfd, FileMetaData* file_meta,
    std::unique_ptr<TableProperties>* prop) {
  // Used for write properties
  std::vector<uint64_t> sst_depend;
  size_t sst_read_amp = 1;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>> collectors;
  collectors.emplace_back(new SstPurposePropertiesCollectorFactory(
      (uint8_t)kMapSst, &sst_depend, &sst_read_amp));

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
  auto s = NewWritableFile(env_, fname, &writable_file, env_options_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "[%s] [JOB %d] BuildMapSst for table #%" PRIu64
                    " fails at NewWritableFile with status %s",
                    cfd->GetName().c_str(), job_id_, file_number,
                    s.ToString().c_str());
    LogFlush(db_options_.info_log);
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        nullptr, cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname, -1,
        FileDescriptor(), TableProperties(),
        TableFileCreationReason::kCompaction, s);
    return s;
  }

  file_meta->fd = FileDescriptor(file_number, output_path_id, 0);

  writable_file->SetIOPriority(Env::IO_LOW);
  writable_file->SetWriteLifeTimeHint(Env::WLTH_SHORT);
  // map sst always small
  writable_file->SetPreallocationBlockSize(4ULL << 20);
  std::unique_ptr<WritableFileWriter> outfile(new WritableFileWriter(
      std::move(writable_file), fname, env_options_, stats_));

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
  std::unique_ptr<TableBuilder> builder(NewTableBuilder(
      *cfd->ioptions(), *cfd->GetCurrentMutableCFOptions(),
      cfd->internal_comparator(), &collectors, cfd->GetID(), cfd->GetName(),
      outfile.get(), kNoCompression, CompressionOptions(), -1 /*level*/,
      nullptr /*compression_dict*/, true /*skip_filters*/,
      true /*ignore_key_type*/, output_file_creation_time));
  LogFlush(db_options_.info_log);

  // Update boundaries
  file_meta->smallest = bound_builder.smallest;
  file_meta->largest = bound_builder.largest;
  file_meta->fd.smallest_seqno = bound_builder.smallest_seqno;
  file_meta->fd.largest_seqno = bound_builder.largest_seqno;

  for (range_iter->SeekToFirst(); range_iter->Valid(); range_iter->Next()) {
    builder->Add(range_iter->key(), range_iter->value());
  }
  if (!range_iter->status().ok()) {
    s = range_iter->status();
  }

  // Prepare sst_depend, IntTblPropCollector::Finish will read it
  auto& sst_depend_build = range_iter->GetSstDepend();
  sst_depend.reserve(sst_depend_build.size());
  sst_depend.insert(sst_depend.end(), sst_depend_build.begin(),
                    sst_depend_build.end());
  std::sort(sst_depend.begin(), sst_depend.end());
  sst_read_amp = range_iter->GetSstReadAmp();

  // Map sst don't write tombstones
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
    prop->reset(new TableProperties(builder->GetTableProperties()));
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
      nullptr, cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname, -1,
      file_meta->fd, **prop, TableFileCreationReason::kCompaction, s);

#ifndef ROCKSDB_LITE
  // Report new file to SstFileManagerImpl
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());
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
  file_meta->sst_purpose = kMapSst;
  file_meta->sst_depend = std::move(sst_depend);

  return s;
}

struct MapElementIterator : public InternalIterator {
  explicit MapElementIterator(FileMetaData* const* meta_array, size_t meta_size,
                              TableCache* table_cache,
                              const ReadOptions& read_options,
                              const EnvOptions& env_options,
                              const InternalKeyComparator* icmp,
                              const SliceTransform* slice_transform)
      : meta_array_(meta_array),
        meta_size_(meta_size),
        table_cache_(table_cache),
        read_options_(read_options),
        env_options_(env_options),
        icmp_(icmp),
        slice_transform_(slice_transform),
        where_(meta_size) {
    assert(meta_size > 0);
  }
  virtual bool Valid() const override { return where_ < meta_size_; }
  virtual void Seek(const Slice& target) override {
    where_ =
        std::lower_bound(meta_array_, meta_array_ + meta_size_, target,
                         [this](FileMetaData* f, const Slice& t) {
                           return icmp_->Compare(f->largest.Encode(), t) < 0;
                         }) -
        meta_array_;
    if (where_ == meta_size_) {
      return;
    }
    if (meta_array_[where_]->sst_purpose == kMapSst) {
      if (!InitMapSstIterator()) {
        return;
      }
      iter_->Seek(target);
      if (!iter_->Valid()) {
        iter_.reset();
        if (++where_ == meta_size_) {
          return;
        }
        if (meta_array_[where_]->sst_purpose == kMapSst) {
          if (!InitMapSstIterator()) {
            return;
          }
          iter_->SeekToFirst();
        }
      }
    }
    Update();
  }
  virtual void SeekForPrev(const Slice& target) override {
    where_ =
        std::upper_bound(meta_array_, meta_array_ + meta_size_, target,
                         [this](const Slice& t, FileMetaData* f) {
                           return icmp_->Compare(t, f->largest.Encode()) < 0;
                         }) -
        meta_array_;
    if (where_-- == 0) {
      where_ = meta_size_;
      return;
    }
    if (meta_array_[where_]->sst_purpose == kMapSst) {
      if (!InitMapSstIterator()) {
        return;
      }
      iter_->SeekForPrev(target);
      if (!iter_->Valid()) {
        iter_.reset();
        if (where_-- == 0) {
          where_ = meta_size_;
          return;
        }
        if (meta_array_[where_]->sst_purpose == kMapSst) {
          if (!InitMapSstIterator()) {
            return;
          }
          iter_->SeekToLast();
        }
      }
    }
    Update();
  }
  virtual void SeekToFirst() override {
    where_ = 0;
    if (meta_array_[where_]->sst_purpose == kMapSst) {
      if (!InitMapSstIterator()) {
        return;
      }
      iter_->SeekToFirst();
    }
    Update();
  }
  virtual void SeekToLast() override {
    where_ = meta_size_ - 1;
    if (meta_array_[where_]->sst_purpose == kMapSst) {
      if (!InitMapSstIterator()) {
        return;
      }
      iter_->SeekToLast();
    }
    Update();
  }
  virtual void Next() override {
    if (iter_) {
      assert(iter_->Valid());
      iter_->Next();
      if (iter_->Valid()) {
        return;
      }
    }
    if (++where_ == meta_size_) {
      iter_.reset();
      return;
    }
    if (meta_array_[where_]->sst_purpose == kMapSst) {
      if (!InitMapSstIterator()) {
        return;
      }
      iter_->SeekToFirst();
    }
    Update();
  }
  virtual void Prev() override {
    if (iter_) {
      assert(iter_->Valid());
      iter_->Prev();
      if (iter_->Valid()) {
        return;
      }
    }
    if (where_-- == 0) {
      where_ = meta_size_;
      iter_.reset();
      return;
    }
    if (meta_array_[where_]->sst_purpose == kMapSst) {
      if (!InitMapSstIterator()) {
        return;
      }
      iter_->SeekToLast();
    }
    Update();
  }
  Slice key() const override { return key_slice; }
  Slice value() const override { return value_slice; }
  virtual Status status() const override {
    return iter_ ? iter_->status() : Status::OK();
  }

  bool InitMapSstIterator() {
    DependFileMap empty_depend_files;
    iter_.reset(table_cache_->NewIterator(
        read_options_, env_options_, *icmp_, *meta_array_[where_],
        empty_depend_files, nullptr, slice_transform_, nullptr, nullptr, false,
        nullptr, true, -1));
    if (iter_->status().ok()) {
      return true;
    }
    where_ = meta_size_;
    return false;
  }
  void Update() {
    if (iter_) {
      key_slice = iter_->key();
      value_slice = iter_->value();
    } else {
      FileMetaData* f = meta_array_[where_];
      element_.smallest_key_ = f->smallest.Encode();
      element_.largest_key_ = f->largest.Encode();
      element_.include_smallest_ = true;
      element_.include_largest_ = true;
      element_.no_records_ = false;
      element_.link_.clear();
      element_.link_.emplace_back(
          MapSstElement::LinkTarget{f->fd.GetNumber(), f->fd.GetFileSize()});
      key_slice = element_.Key();
      value_slice = element_.Value(&buffer_);
    }
  }

  FileMetaData* const* meta_array_;
  size_t meta_size_;
  TableCache* table_cache_;
  const ReadOptions& read_options_;
  const EnvOptions& env_options_;
  const InternalKeyComparator* icmp_;
  const SliceTransform* slice_transform_;
  size_t where_;
  MapSstElement element_;
  std::string buffer_;
  std::unique_ptr<InternalIterator> iter_;
  Slice key_slice, value_slice;
};

InternalIterator* NewMapElementIterator(
    FileMetaData* const* meta_array, size_t meta_size, TableCache* table_cache,
    const ReadOptions& read_options, const EnvOptions& env_options,
    const InternalKeyComparator* icmp, const SliceTransform* slice_transform,
    Arena* arena) {
  if (meta_size == 0) {
    return NewEmptyInternalIterator(arena);
  } else if (meta_size == 1 && meta_array[0]->sst_purpose == kMapSst) {
    DependFileMap empty_depend_files;
    return table_cache->NewIterator(
        read_options, env_options, *icmp, *meta_array[0], empty_depend_files,
        nullptr, slice_transform, nullptr, nullptr, false, arena, true, -1);
  } else if (arena == nullptr) {
    return new MapElementIterator(meta_array, meta_size, table_cache,
                                  read_options, env_options, icmp,
                                  slice_transform);
  } else {
    return new (arena->AllocateAligned(sizeof(MapElementIterator)))
        MapElementIterator(meta_array, meta_size, table_cache, read_options,
                           env_options, icmp, slice_transform);
  }
}

}  // namespace rocksdb
