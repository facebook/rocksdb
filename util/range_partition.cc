//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include "util/range_partition.h"

#include "db/version_edit.h"

namespace rocksdb {

namespace {
  
const Slice& RangePoint(const RangePtr& r, size_t i) {
  return i == 0 ? r.start() : r.limit();
}
bool RangeInclude(const RangePtr& r, size_t i) {
  return i == 0 ? r.include_start() : r.include_limit();
}

int CompRange(const InternalKeyComparator& icomp, const RangeWithDepend& a,
              size_t ao, const RangePtr& b, size_t bo) {
  if (bo == 0) {
    if (b.infinite_start()) {
      return 1;
    }
    return icomp.Compare(a.point[ao].Encode(), b.start());
  } else {
    if (b.include_limit()) {
      return -1;
    }
    return icomp.Compare(a.point[ao].Encode(), b.limit());
  }
}

}


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


std::vector<RangeWithDepend> MergeRangeWithDepend(
    const std::vector<RangeWithDepend>& range_a,
    const std::vector<RangeWithDepend>& range_b,
    const InternalKeyComparator& icomp) {
  std::vector<RangeWithDepend> output;
  assert(!range_a.empty() && !range_b.empty());
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
    if (ai < range_a.size() && bi < range_b.size()) {
      c = icomp.Compare(range_a[ai].point[ab].Encode(),
                        range_b[bi].point[bb].Encode());
      if (c == 0) {
        switch (CASE(ab, range_a[ai].include[ab], bb,
                     range_b[bi].include[bb])) {
        // range_e: [   [   (   )   )   [
        // range_d: (   )   ]   ]   (   ]
        case CASE(0, 1, 0, 0):
        case CASE(0, 1, 1, 0):
        case CASE(0, 0, 1, 1):
        case CASE(1, 0, 1, 1):
        case CASE(1, 0, 0, 0):
        case CASE(0, 1, 1, 1):
          c = -1;
          break;
        // range_e: (   )   ]   ]   (   ]
        // range_d: [   [   (   )   )   [
        case CASE(0, 0, 0, 1):
        case CASE(1, 0, 0, 1):
        case CASE(1, 1, 0, 0):
        case CASE(1, 1, 1, 0):
        case CASE(0, 0, 1, 0):
        case CASE(1, 1, 0, 1):
          c = 1;
          break;
        // range_e: [   ]   (   )
        // range_d: [   ]   (   )
        default:
          c = 0;
          break;
        }
      }
    } else {
      c = ai < range_a.size() ? -1 : 1;
    }
    ac = c <= 0;
    bc = c >= 0;
    switch (CASE(ab, bb, ac, bc)) {
    // out range_e , out range_d , enter range_e
    case CASE(0, 0, 1, 0):
      put_left(range_a[ai].point[ab], range_a[ai].include[ab]);
      put_depend(&range_a[ai], nullptr);
      break;
    // in range_e , out range_d , leave range_e
    case CASE(1, 0, 1, 0):
      put_right(range_a[ai].point[ab], range_a[ai].include[ab]);
      break;
    // out range_e , out range_d , enter range_d
    case CASE(0, 0, 0, 1):
      put_left(range_b[bi].point[bb], range_b[bi].include[bb]);
      put_depend(nullptr, &range_b[bi]);
      break;
    // out range_e , in range_d , leave range_d
    case CASE(0, 1, 0, 1):
      put_right(range_b[bi].point[bb], range_b[bi].include[bb]);
      break;
    // in range_e , out range_d , begin range_d
    case CASE(1, 0, 0, 1):
      put_right(range_b[bi].point[bb], !range_b[bi].include[bb]);
      put_left(range_b[bi].point[bb], range_b[bi].include[bb]);
      put_depend(&range_a[ai], &range_b[bi]);
      break;
    // in range_e , in range_d , leave range_d
    case CASE(1, 1, 0, 1):
      put_right(range_b[bi].point[bb], range_b[bi].include[bb]);
      put_left(range_b[bi].point[bb], !range_b[bi].include[bb]);
      put_depend(&range_a[ai], nullptr);
      break;
    // out range_e , in range_d , begin range_e
    case CASE(0, 1, 1, 0):
      put_right(range_a[ai].point[ab], !range_a[ai].include[ab]);
      put_left(range_a[ai].point[ab], range_a[ai].include[ab]);
      put_depend(&range_a[ai], &range_b[bi]);
      break;
    // in range_e , in range_d , leave range_e
    case CASE(1, 1, 1, 0):
      put_right(range_a[ai].point[ab], range_a[ai].include[ab]);
      put_left(range_a[ai].point[ab], !range_a[ai].include[ab]);
      put_depend(nullptr, &range_b[bi]);
      break;
    // out range_e , out range_d , enter range_e , enter range_d
    case CASE(0, 0, 1, 1):
      put_left(range_a[ai].point[ab], range_a[ai].include[ab]);
      put_depend(&range_a[ai], &range_b[bi]);
      break;
    // in range_e , in range_d , leave range_e , leave range_d
    case CASE(1, 1, 1, 1):
      put_right(range_a[ai].point[ab], range_a[ai].include[ab]);
      break;
    default:
      assert(false);
    }
    ai += (ab + ac) / 2;
    bi += (bb + bc) / 2;
    ab = (ab + ac) % 2;
    bb = (bb + bc) % 2;
  } while (ai != range_a.size() || bi != range_b.size());
#undef CASE
  return output;
}

std::vector<RangeWithDepend> DeleteRangeWithDepend(
    const std::vector<RangeWithDepend>& range_e,
    const std::vector<RangePtr>& range_d,
    const InternalKeyComparator& icomp) {
  std::vector<RangeWithDepend> output;
  assert(!range_e.empty() && !range_d.empty());
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
    if (ei < range_e.size() && di < range_d.size()) {
      c = CompRange(icomp, range_e[ei], eb, range_d[di], db);
      if (c == 0) {
        switch (CASE(eb, range_e[ei].include[eb], db,
                     RangeInclude(range_d[di], db))) {
        // range_e: [   [   (   )   )   [
        // range_d: (   )   ]   ]   (   ]
        case CASE(0, 1, 0, 0):
        case CASE(0, 1, 1, 0):
        case CASE(0, 0, 1, 1):
        case CASE(1, 0, 1, 1):
        case CASE(1, 0, 0, 0):
        case CASE(0, 1, 1, 1):
          c = -1;
          break;
        // range_e: (   )   ]   ]   (   ]
        // range_d: [   [   (   )   )   [
        case CASE(0, 0, 0, 1):
        case CASE(1, 0, 0, 1):
        case CASE(1, 1, 0, 0):
        case CASE(1, 1, 1, 0):
        case CASE(0, 0, 1, 0):
        case CASE(1, 1, 0, 1):
          c = 1;
          break;
        // range_e: [   ]   (   )
        // range_d: [   ]   (   )
        default:
          c = 0;
          break;
        }
      }
    } else {
      c = ei < range_e.size() ? -1 : 1;
    }
    ec = c <= 0;
    dc = c >= 0;
    switch (CASE(eb, db, ec, dc)) {
    // out range_e , out range_d , enter range_e
    case CASE(0, 0, 1, 0):
      put_left(range_e[ei].point[eb].Encode(), range_e[ei].include[eb],
               range_e[ei].depend);
      break;
    // in range_e , out range_d , leave range_e
    case CASE(1, 0, 1, 0):
      put_right(range_e[ei].point[eb].Encode(), range_e[ei].include[eb]);
      break;
    // in range_e , out range_d , begin range_d
    case CASE(1, 0, 0, 1):
    // in range_e , out range_d , leave range_e , enter range_d
    case CASE(1, 0, 1, 1):
      put_right(RangePoint(range_d[di], db), !RangeInclude(range_d[di], db));
      break;
    // in range_e , in range_d , leave range_d
    case CASE(1, 1, 0, 1):
    // out range_e , in range_d , enter range_e & leave range_d
    case CASE(0, 1, 1, 1):
      put_left(RangePoint(range_d[di], db), !RangeInclude(range_d[di], db),
               range_e[ei].depend);
      break;
    }
    ei += (eb + ec) / 2;
    di += (db + dc) / 2;
    eb = (eb + ec) % 2;
    db = (db + dc) % 2;
  } while (ei != range_e.size() || di != range_d.size());
#undef CASE
  return output;
}

}
