// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <chrono>
#include "db/db_path_supplier.h"

using namespace std::chrono;
namespace rocksdb {

Status DbPathSupplier::FsyncDbPath(uint32_t path_id) const {
  auto size_u32 = static_cast<uint32_t>(ioptions_.cf_paths.size());

  if (path_id >= size_u32) {
    return Status::InvalidArgument("path_id "
                                   + std::to_string(path_id)
                                   + " is too big; expecting <= "
                                   + std::to_string(size_u32));
  }

  const std::string path_name = ioptions_.cf_paths[path_id].path;

  std::unique_ptr<Directory> dir;
  Status s = ioptions_.env->NewDirectory(path_name, &dir);

  if (!s.ok()) {
    return s;
  }

  if (dir == nullptr) {
    return Status::InvalidArgument("no dir to sync for path_id "
                                   + std::to_string(path_id));
  }

  return dir->Fsync();
}

Random init_random() {
  milliseconds ms = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch());
  auto seed = static_cast<uint32_t>(ms.count());
  return Random(seed);
}

uint32_t RandomDbPathSupplier::GetPathId(int level) const {
  UNUSED(level);
  thread_local Random rand = init_random();
  size_t paths_size = ioptions_.cf_paths.size();
  auto size_u32 = static_cast<uint32_t >(paths_size);
  return rand.Next() % size_u32;
}

bool RandomDbPathSupplier::AcceptPathId(uint32_t path_id, int output_level) const {
  UNUSED(output_level);
  size_t paths_size = ioptions_.cf_paths.size();

  if (paths_size == 0) {
    return path_id == 0;
  }

  auto size_u32 = static_cast<uint32_t>(paths_size);
  return path_id < size_u32;
}

uint32_t LeveledTargetSizeDbPathSupplier::GetPathId(int level) const {
  uint32_t p = 0;
  assert(!ioptions_.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions_.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = moptions_.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions_.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (ioptions_.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/master/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * moptions_.max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * moptions_.max_bytes_for_level_multiplier *
                moptions_.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions_.cf_paths[p].target_size;
  }
  return p;
}

bool LeveledTargetSizeDbPathSupplier::AcceptPathId(
    uint32_t path_id, int output_level) const {
  return path_id == GetPathId(output_level);
}

uint32_t UniversalTargetSizeDbPathSupplier::GetPathId(int level) const {
  UNUSED(level);

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
      file_size_ *
      (100 - moptions_.compaction_options_universal.size_ratio) / 100;
  uint32_t p = 0;
  assert(!ioptions_.cf_paths.empty());
  for (; p < ioptions_.cf_paths.size() - 1; p++) {
    uint64_t target_size = ioptions_.cf_paths[p].target_size;
    if (target_size > file_size_ &&
        accumulated_size + (target_size - file_size_) > future_size) {
      return p;
    }
    accumulated_size += target_size;
  }
  return p;
}

bool UniversalTargetSizeDbPathSupplier::AcceptPathId(
    uint32_t path_id, int output_level) const {
  return path_id == GetPathId(output_level);
}

}
