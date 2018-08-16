//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/range_partition.h"

#include "table/table_reader.h"
#include "db/version_edit.h"
#include "util/iterator_cache.h"

namespace rocksdb {

namespace {
  
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

class ShrinkRangeWithDependIterator : public InternalIterator {
 public:
  ShrinkRangeWithDependIterator(
      const std::vector<RangeWithDepend>& ranges,
      std::unordered_set<uint64_t>& sst_depend_build,
      IteratorCache& iterator_cache, const InternalKeyComparator& icomp)
      : ranges_(ranges),
        sst_depend_build_(sst_depend_build),
        iterator_cache_(iterator_cache),
        icomp_(icomp) {}
  virtual bool Valid() const override { return !buffer_.empty(); }
  virtual void Seek(const Slice& /*target*/) override {
    buffer_.clear();
    assert(false);
  }
  virtual void SeekForPrev(const Slice& /*target*/) override {
    buffer_.clear();
    assert(false);
  }
  virtual void SeekToFirst() override {
    where_ = ranges_.begin();
    PrepareNext();
  }
  virtual void SeekToLast() override {
    buffer_.clear();
    assert(false);
  }
  virtual void Next() override { PrepareNext(); }
  virtual void Prev() override { assert(false); }
  Slice key() const override { return map_elements_.Key(); }
  Slice value() const override { return buffer_; }
  virtual Status status() const override { return status_; }

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
  std::unordered_set<uint64_t>& sst_depend_build_;
  IteratorCache& iterator_cache_;
  const InternalKeyComparator& icomp_;
};


RangeWithDepend::RangeWithDepend(const FileMetaData* f) {
  point[0] = f->smallest;
  point[1] = f->largest;
  include[0] = true;
  include[1] = true;
  depend.push_back(f->fd.GetNumber());
}

RangeWithDepend::RangeWithDepend(const MapSstElement& map_element) {
  point[0].DecodeFrom(map_element.smallest_key_);
  point[1].DecodeFrom(map_element.largest_key_);
  include[0] = true;
  include[1] = true;
  depend.resize(map_element.link_.size());
  for (size_t i = 0; i < depend.size(); ++i) {
    depend[i] = map_element.link_[i].sst_id;
  }
}

void FileMetaDataBoundBuilder::Update(const FileMetaData* f) {
  if (smallest.size() == 0 || icomp.Compare(f->smallest, smallest) < 0) {
    smallest = f->smallest;
  }
  if (largest.size() == 0 || icomp.Compare(f->largest, largest) > 0) {
    largest = f->largest;
  }
  smallest_seqno = std::min(smallest_seqno, f->fd.smallest_seqno);
  largest_seqno = std::max(largest_seqno, f->fd.largest_seqno);
}

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
      iterator_cache.GetIterator(f, &reader);
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

InternalIterator* NewShrinkRangeWithDependIterator(
    const std::vector<RangeWithDepend>& ranges,
    std::unordered_set<uint64_t>& sst_depend_build,
    IteratorCache& iterator_cache, const InternalKeyComparator& icomp,
    Arena* arena) {
  typedef ShrinkRangeWithDependIterator IterType;
  if (arena == nullptr) {
    return new IterType(ranges, sst_depend_build, iterator_cache, icomp);
  } else {
    void* buffer = arena->AllocateAligned(sizeof(IterType));
    return new(buffer) IterType(ranges, sst_depend_build, iterator_cache,
                                icomp);
  }
}

}
