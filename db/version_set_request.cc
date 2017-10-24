//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#include "db/column_family.h"
#include "db/internal_stats.h"
#include "db/merge_helper.h"
#include "db/table_cache_request.h"
#include "db/version_set_request.h"
#include "db/version_edit.h"

namespace rocksdb {
namespace async {

Status VersionSetGetContext::IterateFilePicker(const Status& status) {
  async(status);

  Status s(status);
  bool done = false;
  while (s.ok()) {

    switch (get_context()->State()) {
    case GetContext::kNotFound:
      // Keep searching in other files
      break;
    case GetContext::kFound: {
      unsigned int hit_level = file_picker()->GetHitFileLevel();
      if (hit_level == 0) {
        RecordTick(version_->db_statistics_, GET_HIT_L0);
      } else if (hit_level == 1) {
        RecordTick(version_->db_statistics_, GET_HIT_L1);
      } else if (hit_level >= 2) {
        RecordTick(version_->db_statistics_, GET_HIT_L2_AND_UP);
      }
      done = true;
    }
    break;
    case GetContext::kDeleted:
      // Use empty error message for speed
      s = Status::NotFound();
      done = true;
      break;
    case GetContext::kCorrupt:
      s = Status::Corruption("corrupted key for ", user_key_);
      done = true;
      break;
    case GetContext::kMerge:
      break;
    }

    if (done) {
      break;
    }

    FdWithKeyRange* f = file_picker()->GetNextFile();

    if (f == nullptr) {
      s = HandleMerge();
      break;
    }

    s = CacheGet(f);
    if (s.IsIOPending()) {
      return s;
    }
  }

  return OnComplete(s);
}
}
}
