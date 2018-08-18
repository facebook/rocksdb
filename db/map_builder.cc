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

#include <algorithm>
#include <inttypes.h>
#include <list>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "db/builder.h"
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
    include[0] = true;
    include[1] = true;
    depend.resize(map_element.link_.size());
    for (size_t i = 0; i < depend.size(); ++i) {
      depend[i] = map_element.link_[i].sst_id;
    }
  }
};
  
const Slice& RangePoint(const RangePtr& r, size_t i) {
  return i == 0 ? *r.start : *r.limit;
}
bool RangeInclude(const RangePtr& r, size_t i) {
  return i == 0 ? r.include_start : r.include_limit;
}

int CompRange(const InternalKeyComparator& icomp, const RangeWithDepend& a,
              size_t ao, const RangePtr& b, size_t bo) {
  if (bo == 0) {
    if (b.start == nullptr) {
      return 1;
    }
    return icomp.Compare(a.point[ao].Encode(), *b.start);
  } else {
    if (b.limit == nullptr) {
      return -1;
    }
    return icomp.Compare(a.point[ao].Encode(), *b.limit);
  }
}
}

class MapSstElementIterator {
 public:
  MapSstElementIterator(
      const std::vector<RangeWithDepend>& ranges,
      IteratorCache& iterator_cache, const InternalKeyComparator& icomp)
      : ranges_(ranges),
        iterator_cache_(iterator_cache),
        icomp_(icomp) {}
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

 private:

  void PrepareNext() {
    while (TryPrepareNext());
  }

  bool TryPrepareNext() {
    if (where_ == ranges_.end()) {
      buffer_.clear();
      return false;
    }
    auto& range_with_depend = *where_;
    auto& start = range_with_depend.point[0];
    auto& end = range_with_depend.point[1];
    bool include_start = range_with_depend.include[0];
    bool include_end = range_with_depend.include[1];
    new_start_.Clear();
    new_end_.Clear();
    map_elements_.link_.clear();

    for (auto sst_id : range_with_depend.depend) {
      TableReader* reader;
      auto iter = iterator_cache_.GetIterator(sst_id, &reader);
      if (!iter->status().ok()) {
        buffer_.clear();
        status_ = iter->status();
        return false;
      }
      iter->Seek(start.Encode());
      if (!iter->Valid()) {
        continue;
      }
      if (!include_start && icomp_.Compare(iter->key(), start.Encode()) == 0) {
        iter->Next();
        if (!iter->Valid()) {
          continue;
        }
      }
      start_.DecodeFrom(iter->key());
      iter->SeekForPrev(end.Encode());
      if (!iter->Valid()) {
        continue;
      }
      if (!include_end && icomp_.Compare(iter->key(), end.Encode()) == 0) {
        iter->Prev();
        if (!iter->Valid()) {
          continue;
        }
      }
      end_.DecodeFrom(iter->key());
      if (icomp_.Compare(start_, end_) > 0) {
        continue;
      }
      if (new_start_.size() == 0 || icomp_.Compare(start_, new_start_) < 0) {
        new_start_ = start_;
      }
      if (new_end_.size() == 0 || icomp_.Compare(end_, new_end_) > 0) {
        new_end_ = end_;
      }
      uint64_t left_offset =
          reader->ApproximateOffsetOf(start_.Encode());
      uint64_t right_offset =
          reader->ApproximateOffsetOf(end_.Encode());
      sst_depend_build_.emplace(sst_id);

      // append a link
      MapSstElement::LinkTarget link;
      link.sst_id = sst_id;
      link.size = right_offset - left_offset;
      map_elements_.link_.emplace_back(link);
    }
    ++where_;
    if (!map_elements_.link_.empty()) {
      // output map_element
      map_elements_.smallest_key_ = new_start_.Encode();
      map_elements_.largest_key_ = new_end_.Encode();
      map_elements_.Value(&buffer_);
      return false;
    }
    return true;
  }

 private:
  Status status_;
  MapSstElement map_elements_;
  InternalKey new_start_, new_end_;
  InternalKey start_, end_;
  std::string buffer_;
  std::vector<RangeWithDepend>::const_iterator where_;
  const std::vector<RangeWithDepend>& ranges_;
  std::unordered_set<uint64_t> sst_depend_build_;
  IteratorCache& iterator_cache_;
  const InternalKeyComparator& icomp_;
};

namespace {

Status LoadRangeWithDepend(
    std::vector<RangeWithDepend>& ranges,
    FileMetaDataBoundBuilder* bound_builder, IteratorCache& iterator_cache,
    const FileMetaData* const* file_meta, size_t n) {
  MapSstElement map_element;
  for (size_t i = 0; i < n; ++i) {
    auto f = file_meta[i];
    TableReader* reader;
    if (f->sst_variety == kMapSst) {
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

// Merge two sorted non-overlap range vector
// a: [ -------- )      [ -------- ]
// b:       ( -------------- ]
// r: [ -- ]( -- )[ -- )[ -- ]( -- ]
std::vector<RangeWithDepend> MergeRangeWithDepend(
    const std::vector<RangeWithDepend>& ranges_a,
    const std::vector<RangeWithDepend>& ranges_b,
    const InternalKeyComparator& icomp) {
  std::vector<RangeWithDepend> output;
  assert(!ranges_a.empty() && !ranges_b.empty());
  auto put_left = [&](const InternalKey& key, bool include) {
    output.emplace_back();
    auto& back = output.back();
    back.point[0] = key;
    back.include[0] = include;
  };
  auto put_right = [&](const InternalKey& key, bool include) {
    auto& back = output.back();
    back.point[1] = key;
    back.include[1] = include;
  };
  auto put_depend = [&](const RangeWithDepend* a, const RangeWithDepend* b) {
    auto& depend = output.back().depend;
    if (a != nullptr) {
      depend.insert(depend.end(), a->depend.begin(), a->depend.end());
    }
    if (b != nullptr) {
      depend.insert(depend.end(), b->depend.begin(), b->depend.end());
    }
    assert(!depend.empty());
  };
  size_t ai = 0, bi = 0;  // range index
  size_t ac, bc;          // changed
  size_t ab = 0, bb = 0;  // left bound or right bound
#define CASE(a,b,c,d) (!!(a) | (!!(b) << 1) | (!!(c) << 2) | (!!(d) << 3))
  do {
    int c;
    if (ai < ranges_a.size() && bi < ranges_b.size()) {
      c = icomp.Compare(ranges_a[ai].point[ab].Encode(),
                        ranges_b[bi].point[bb].Encode());
      if (c == 0) {
        switch (CASE(ab, ranges_a[ai].include[ab], bb,
                     ranges_b[bi].include[bb])) {
        // ranges_e: [   [   (   )   )   [
        // ranges_d: (   )   ]   ]   (   ]
        case CASE(0, 1, 0, 0):
        case CASE(0, 1, 1, 0):
        case CASE(0, 0, 1, 1):
        case CASE(1, 0, 1, 1):
        case CASE(1, 0, 0, 0):
        case CASE(0, 1, 1, 1):
          c = -1;
          break;
        // ranges_e: (   )   ]   ]   (   ]
        // ranges_d: [   [   (   )   )   [
        case CASE(0, 0, 0, 1):
        case CASE(1, 0, 0, 1):
        case CASE(1, 1, 0, 0):
        case CASE(1, 1, 1, 0):
        case CASE(0, 0, 1, 0):
        case CASE(1, 1, 0, 1):
          c = 1;
          break;
        // ranges_e: [   ]   (   )
        // ranges_d: [   ]   (   )
        default:
          c = 0;
          break;
        }
      }
    } else {
      c = ai < ranges_a.size() ? -1 : 1;
    }
    ac = c <= 0;
    bc = c >= 0;
    switch (CASE(ab, bb, ac, bc)) {
    // out ranges_e , out ranges_d , enter ranges_e
    case CASE(0, 0, 1, 0):
      put_left(ranges_a[ai].point[ab], ranges_a[ai].include[ab]);
      put_depend(&ranges_a[ai], nullptr);
      break;
    // in ranges_e , out ranges_d , leave ranges_e
    case CASE(1, 0, 1, 0):
      put_right(ranges_a[ai].point[ab], ranges_a[ai].include[ab]);
      break;
    // out ranges_e , out ranges_d , enter ranges_d
    case CASE(0, 0, 0, 1):
      put_left(ranges_b[bi].point[bb], ranges_b[bi].include[bb]);
      put_depend(nullptr, &ranges_b[bi]);
      break;
    // out ranges_e , in ranges_d , leave ranges_d
    case CASE(0, 1, 0, 1):
      put_right(ranges_b[bi].point[bb], ranges_b[bi].include[bb]);
      break;
    // in ranges_e , out ranges_d , begin ranges_d
    case CASE(1, 0, 0, 1):
      put_right(ranges_b[bi].point[bb], !ranges_b[bi].include[bb]);
      put_left(ranges_b[bi].point[bb], ranges_b[bi].include[bb]);
      put_depend(&ranges_a[ai], &ranges_b[bi]);
      break;
    // in ranges_e , in ranges_d , leave ranges_d
    case CASE(1, 1, 0, 1):
      put_right(ranges_b[bi].point[bb], ranges_b[bi].include[bb]);
      put_left(ranges_b[bi].point[bb], !ranges_b[bi].include[bb]);
      put_depend(&ranges_a[ai], nullptr);
      break;
    // out ranges_e , in ranges_d , begin ranges_e
    case CASE(0, 1, 1, 0):
      put_right(ranges_a[ai].point[ab], !ranges_a[ai].include[ab]);
      put_left(ranges_a[ai].point[ab], ranges_a[ai].include[ab]);
      put_depend(&ranges_a[ai], &ranges_b[bi]);
      break;
    // in ranges_e , in ranges_d , leave ranges_e
    case CASE(1, 1, 1, 0):
      put_right(ranges_a[ai].point[ab], ranges_a[ai].include[ab]);
      put_left(ranges_a[ai].point[ab], !ranges_a[ai].include[ab]);
      put_depend(nullptr, &ranges_b[bi]);
      break;
    // out ranges_e , out ranges_d , enter ranges_e , enter ranges_d
    case CASE(0, 0, 1, 1):
      put_left(ranges_a[ai].point[ab], ranges_a[ai].include[ab]);
      put_depend(&ranges_a[ai], &ranges_b[bi]);
      break;
    // in ranges_e , in ranges_d , leave ranges_e , leave ranges_d
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

// Delete range from sorted non-overlap range vector
// e: [ -------- )      [ -------- ]
// d:       ( -------------- ]
// r: [ -- ]                  ( -- ]
std::vector<RangeWithDepend> DeleteRangeWithDepend(
    const std::vector<RangeWithDepend>& ranges_e,
    const std::vector<RangePtr>& ranges_d,
    const InternalKeyComparator& icomp) {
  std::vector<RangeWithDepend> output;
  assert(!ranges_e.empty() && !ranges_d.empty());
  auto put_left = [&](const Slice& key, bool include,
                      const std::vector<uint64_t>& depend) {
    output.emplace_back();
    auto& back = output.back();
    back.point[0].DecodeFrom(key);
    back.include[0] = include;
    back.depend = depend;
  };
  auto put_right = [&](const Slice& key, bool include) {
    auto& back = output.back();
    back.point[1].DecodeFrom(key);
    back.include[1] = include;
  };
  size_t ei = 0, di = 0;  // range index
  size_t ec, dc;          // changed
  size_t eb = 0, db = 0;  // left bound or right bound
#define CASE(a,b,c,d) (!!(a) | (!!(b) << 1) | (!!(c) << 2) | (!!(d) << 3))
  do {
    int c;
    if (ei < ranges_e.size() && di < ranges_d.size()) {
      c = CompRange(icomp, ranges_e[ei], eb, ranges_d[di], db);
      if (c == 0) {
        switch (CASE(eb, ranges_e[ei].include[eb], db,
                     RangeInclude(ranges_d[di], db))) {
        // ranges_e: [   [   (   )   )   [
        // ranges_d: (   )   ]   ]   (   ]
        case CASE(0, 1, 0, 0):
        case CASE(0, 1, 1, 0):
        case CASE(0, 0, 1, 1):
        case CASE(1, 0, 1, 1):
        case CASE(1, 0, 0, 0):
        case CASE(0, 1, 1, 1):
          c = -1;
          break;
        // ranges_e: (   )   ]   ]   (   ]
        // ranges_d: [   [   (   )   )   [
        case CASE(0, 0, 0, 1):
        case CASE(1, 0, 0, 1):
        case CASE(1, 1, 0, 0):
        case CASE(1, 1, 1, 0):
        case CASE(0, 0, 1, 0):
        case CASE(1, 1, 0, 1):
          c = 1;
          break;
        // ranges_e: [   ]   (   )
        // ranges_d: [   ]   (   )
        default:
          c = 0;
          break;
        }
      }
    } else {
      c = ei < ranges_e.size() ? -1 : 1;
    }
    ec = c <= 0;
    dc = c >= 0;
    switch (CASE(eb, db, ec, dc)) {
    // out ranges_e , out ranges_d , enter ranges_e
    case CASE(0, 0, 1, 0):
      put_left(ranges_e[ei].point[eb].Encode(), ranges_e[ei].include[eb],
               ranges_e[ei].depend);
      break;
    // in ranges_e , out ranges_d , leave ranges_e
    case CASE(1, 0, 1, 0):
      put_right(ranges_e[ei].point[eb].Encode(), ranges_e[ei].include[eb]);
      break;
    // in ranges_e , out ranges_d , begin ranges_d
    case CASE(1, 0, 0, 1):
    // in ranges_e , out ranges_d , leave ranges_e , enter ranges_d
    case CASE(1, 0, 1, 1):
      put_right(RangePoint(ranges_d[di], db), !RangeInclude(ranges_d[di], db));
      break;
    // in ranges_e , in ranges_d , leave ranges_d
    case CASE(1, 1, 0, 1):
    // out ranges_e , in ranges_d , enter ranges_e & leave ranges_d
    case CASE(0, 1, 1, 1):
      put_left(RangePoint(ranges_d[di], db), !RangeInclude(ranges_d[di], db),
               ranges_e[ei].depend);
      break;
    }
    ei += (eb + ec) / 2;
    di += (db + dc) / 2;
    eb = (eb + ec) % 2;
    db = (db + dc) % 2;
  } while (ei != ranges_e.size() || di != ranges_d.size());
#undef CASE
  return output;
}

}

MapBuilder::MapBuilder(
    int job_id, const ImmutableDBOptions& db_options,
    const EnvOptions env_options, VersionSet* versions,
    Statistics* stats, InstrumentedMutex* db_mutex,
    const std::vector<SequenceNumber>& existing_snapshots,
    std::shared_ptr<Cache> table_cache, const std::string& dbname)
    : job_id_(job_id),
      dbname_(dbname),
      db_options_(db_options),
      env_options_(env_options),
      env_(db_options.env),
      env_optiosn_for_read_(
          env_->OptimizeForCompactionTableRead(env_options, db_options_)),
      versions_(versions),
      stats_(stats),
      db_mutex_(db_mutex),
      existing_snapshots_(std::move(existing_snapshots)),
      table_cache_(std::move(table_cache)) {}

Status MapBuilder::Build(const std::vector<CompactionInputFiles>& inputs,
                         const std::vector<RangePtr>& deleted_range,
                         const std::vector<const FileMetaData*>& added_files,
                         int output_level, uint32_t output_path_id,
                         VersionStorageInfo* vstorage, ColumnFamilyData* cfd,
                         VersionEdit* edit, FileMetaData* file_meta,
                         std::unique_ptr<TableProperties>* porp) {

  DependFileMap empty_delend_files;
  auto& depend_files = vstorage->depend_files();

  auto create_iterator = [&](const FileMetaData* f,
                             const DependFileMap& depend_files, Arena* arena,
                             TableReader** reader_ptr)->InternalIterator* {
    ReadOptions read_options;
    read_options.verify_checksums = true;
    read_options.fill_cache = false;
    read_options.total_order_seek = true;

    return cfd->table_cache()->NewIterator(
               read_options, env_optiosn_for_read_,
               cfd->internal_comparator(), *f,
               f->sst_variety == kMapSst ? empty_delend_files : depend_files,
               nullptr,
               cfd->GetCurrentMutableCFOptions()->prefix_extractor.get(),
               reader_ptr, nullptr /* no per level latency histogram */,
               true /* for_compaction */, arena,
               false /* skip_filters */, -1);
  };

  IteratorCache iterator_cache(depend_files, &create_iterator,
                               c_style_callback(create_iterator));

  std::list<std::vector<RangeWithDepend>> level_ranges;
  MapSstElement map_element;
  FileMetaDataBoundBuilder bound_builder(cfd->internal_comparator());

  size_t totla_range_count = added_files.size();
  Status s;

  if (deleted_range.size() != 1 || deleted_range.front().start != nullptr ||
      deleted_range.front().limit != nullptr) {
    // load input files into level_ranges
    for (auto& level_files : inputs) {
      if (level_files.files.empty()) {
        continue;
      }
      totla_range_count += level_files.files.size();
      if (level_files.level == 0) {
        for (auto f : level_files.files) {
          std::vector<RangeWithDepend> ranges;
          s = LoadRangeWithDepend(ranges, &bound_builder, iterator_cache, &f,
                                  1);
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

  if (!level_ranges.empty() && !deleted_range.empty()) {
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
    if (level_ranges.empty()) {
      level_ranges.emplace_back(std::move(ranges));
    } else {
      level_ranges.front() =
          MergeRangeWithDepend(level_ranges.front(), ranges,
                               cfd->internal_comparator());
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
  auto& icomp = cfd->internal_comparator();
  DependFileMap sst_live;
  // check is need build map
  if (ranges.size() == totla_range_count) {
    for (auto it = ranges.begin(); it != ranges.end(); ++it) {
      if (it->depend.size() > 1) {
        sst_live.clear();
        break;
      }
      auto f = iterator_cache.GetFileMetaData(it->depend.front());
      assert(f != nullptr);
      if (icomp.Compare(it->point[0], f->smallest) != 0 ||
          icomp.Compare(it->point[1], f->largest) != 0) {
        sst_live.clear();
        break;
      }
      sst_live.emplace(it->depend.front(), f);
    }
  }
  if (!sst_live.empty()) {
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
  
  using IterType = MapSstElementIterator;
  void* buffer = iterator_cache.GetArena()->AllocateAligned(sizeof(IterType));
  std::unique_ptr<IterType, void(*)(IterType*)> output_iter(
      new(buffer) IterType(ranges, iterator_cache, cfd->internal_comparator()),
      [](IterType* iter) { iter->~IterType(); });

  s = WriteOutputFile(bound_builder, output_iter.get(), output_path_id, cfd,
                      file_meta, porp);

  if (s.ok()) {
    for (auto& input_level : inputs) {
      for (auto f : input_level.files) {
        edit->DeleteFile(input_level.level, f->fd.GetNumber());
      }
    }
    for (auto f : added_files) {
      edit->AddFile(vstorage->num_levels(), *f);
    }
    edit->AddFile(output_level, *file_meta);
  }
  return s;
}


Status MapBuilder::WriteOutputFile(
    const FileMetaDataBoundBuilder& bound_builder,
    MapSstElementIterator* range_iter, uint32_t output_path_id,
    ColumnFamilyData* cfd, FileMetaData* file_meta,
    std::unique_ptr<TableProperties>* porp) {

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
  auto s = NewWritableFile(env_, fname, &writable_file, env_options_);
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
                      true, output_file_creation_time));
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
    return range_iter->status();
  }

  // Prepare sst_depend, IntTblPropCollector::Finish will read it
  auto& sst_depend_build = range_iter->GetSstDepend();
  sst_depend.reserve(sst_depend_build.size());
  sst_depend.insert(sst_depend.end(), sst_depend_build.begin(),
                    sst_depend_build.end());
  std::sort(sst_depend.begin(), sst_depend.end());

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
    porp->reset(new TableProperties(builder->GetTableProperties()));
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
      -1, file_meta->fd, **porp, TableFileCreationReason::kCompaction, s);

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

  return Status::OK();
}

}  // namespace rocksdb
