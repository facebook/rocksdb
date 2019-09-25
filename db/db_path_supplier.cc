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
  auto size_u32 = static_cast<uint32_t>(cf_paths_.size());

  if (path_id >= size_u32) {
    return Status::InvalidArgument("path_id "
                                   + std::to_string(path_id)
                                   + " is too big; expecting <= "
                                   + std::to_string(size_u32));
  }

  const std::string path_name = cf_paths_[path_id].path;

  std::unique_ptr<Directory> dir;
  Status s = env->NewDirectory(path_name, &dir);

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

uint32_t RandomDbPathSupplier::GetPathId(int /* level */) const {
  thread_local Random rand = init_random();
  size_t paths_size = cf_paths_.size();
  auto size_u32 = static_cast<uint32_t >(paths_size);
  return rand.Next() % size_u32;
}

bool RandomDbPathSupplier::AcceptPathId(uint32_t path_id, int /* output_level */) const {
  size_t paths_size = cf_paths_.size();

  if (paths_size == 0) {
    return path_id == 0;
  }

  auto size_u32 = static_cast<uint32_t>(paths_size);
  return path_id < size_u32;
}

LeveledTargetSizeDbPathSupplier::LeveledTargetSizeDbPathSupplier(
    const rocksdb::ImmutableCFOptions &ioptions, const rocksdb::MutableCFOptions &moptions):
    DbPathSupplier(ioptions),
    level_compaction_dynamic_level_bytes(ioptions.level_compaction_dynamic_level_bytes),
    max_bytes_for_level_multiplier(moptions.max_bytes_for_level_multiplier),
    max_bytes_for_level_base(moptions.max_bytes_for_level_base),
    max_bytes_for_level_multiplier_additional(moptions.max_bytes_for_level_multiplier_additional)
    {}

uint32_t LeveledTargetSizeDbPathSupplier::GetPathId(int level) const {
  uint32_t p = 0;
  assert(!cf_paths_.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = cf_paths_[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = max_bytes_for_level_base;

  // Last path is the fallback
  while (p < cf_paths_.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/master/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * max_bytes_for_level_multiplier *
                MutableCFOptions::MaxBytesMultiplerAdditional(
                    max_bytes_for_level_multiplier_additional, cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = cf_paths_[p].target_size;
  }
  return p;
}

bool LeveledTargetSizeDbPathSupplier::AcceptPathId(
    uint32_t path_id, int output_level) const {
  return path_id == GetPathId(output_level);
}

UniversalTargetSizeDbPathSupplier::UniversalTargetSizeDbPathSupplier(
    const rocksdb::ImmutableCFOptions &ioptions, const rocksdb::MutableCFOptions &moptions,
    uint64_t file_size):
    DbPathSupplier(ioptions),
    file_size_(file_size),
    size_ratio(moptions.compaction_options_universal.size_ratio) {}

uint32_t UniversalTargetSizeDbPathSupplier::GetPathId(int /* level */) const {
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
  uint64_t future_size = file_size_ * (100 - size_ratio) / 100;
  uint32_t p = 0;
  assert(!cf_paths_.empty());
  for (; p < cf_paths_.size() - 1; p++) {
    uint64_t target_size = cf_paths_[p].target_size;
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

// The default impl of the factory method. Always returns FixedDbPathSupplier
// that gives path_id as 0.
std::unique_ptr<DbPathSupplier> DbPathSupplierFactory::CreateDbPathSupplier(
    const rocksdb::DbPathSupplierContext &ctx) {
    return std::unique_ptr<DbPathSupplier>(new FixedDbPathSupplier(ctx.ioptions, 0));
}

Status DbPathSupplierFactory::CfPathsSanityCheck(
    const rocksdb::ColumnFamilyOptions& /* cf_options */,
    const rocksdb::DBOptions& /* db_options */) {
  return Status::OK();
}

std::unique_ptr<DbPathSupplier> GradualMoveOldDataDbPathSupplierFactory::CreateDbPathSupplier(
    const rocksdb::DbPathSupplierContext &ctx) {
  std::unique_ptr<DbPathSupplier> ret;

  switch (ctx.call_site) {
    case kDbPathSupplierFactoryCallSiteFromFlush:
      ret.reset(new FixedDbPathSupplier(ctx.ioptions, 0));
      break;

    case kDbPathSupplierFactoryCallSiteFromAutoCompaction:
      switch (ctx.ioptions.compaction_style) {
        case kCompactionStyleLevel:
          ret.reset(new LeveledTargetSizeDbPathSupplier(ctx.ioptions, ctx.moptions));
          break;

        case kCompactionStyleUniversal:
          ret.reset(new UniversalTargetSizeDbPathSupplier(ctx.ioptions, ctx.moptions,
              ctx.estimated_file_size));
          break;

        case kCompactionStyleFIFO:
        default:
          ret.reset(new FixedDbPathSupplier(ctx.ioptions, 0));
      }
      break;

    case kDbPathSupplierFactoryCallSiteFromManualCompaction:
      // an illegal argument from the manual compaction: the provided path id is
      // greater than the cf_paths array. we're just silently giving path 0 here.
      if (ctx.manual_compaction_specified_path_id >= ctx.ioptions.cf_paths.size()) {
        ret.reset(new FixedDbPathSupplier(ctx.ioptions, 0));
      }

      ret.reset(new FixedDbPathSupplier(ctx.ioptions, ctx.manual_compaction_specified_path_id));
      break;

    default:
      ret.reset(new FixedDbPathSupplier(ctx.ioptions, ctx.manual_compaction_specified_path_id));
  }

  return ret;
}

Status GradualMoveOldDataDbPathSupplierFactory::CfPathsSanityCheck(const rocksdb::ColumnFamilyOptions& cf_options,
                                                                   const rocksdb::DBOptions& db_options) {
  // More than one cf_paths are supported only in universal
  // and level compaction styles. This function also checks the case
  // in which cf_paths is not specified, which results in db_paths
  // being used.
  if ((cf_options.compaction_style != kCompactionStyleUniversal) &&
      (cf_options.compaction_style != kCompactionStyleLevel)) {
    if (cf_options.cf_paths.size() > 1) {
      return Status::NotSupported(
          "More than one CF paths are only supported in "
          "universal and level compaction styles");
    } else if (cf_options.cf_paths.empty() &&
               db_options.db_paths.size() > 1) {
      return Status::NotSupported(
          "More than one DB paths are only supported in "
          "universal and level compaction styles");
    }
  }
  return Status::OK();
}

std::unique_ptr<DbPathSupplier> RandomDbPathSupplierFactory::CreateDbPathSupplier(
    const rocksdb::DbPathSupplierContext &ctx) {
  std::unique_ptr<DbPathSupplier> ret;

  switch (ctx.call_site) {
    case kDbPathSupplierFactoryCallSiteFromManualCompaction:
      // an illegal argument from the manual compaction: the provided path id is
      // greater than the cf_paths array. we're just silently giving path 0 here.
      if (ctx.manual_compaction_specified_path_id >= ctx.ioptions.cf_paths.size()) {
        ret.reset(new FixedDbPathSupplier(ctx.ioptions, 0));
      }

      ret.reset(new FixedDbPathSupplier(ctx.ioptions, ctx.manual_compaction_specified_path_id));
      break;

    default:
      ret.reset(new RandomDbPathSupplier(ctx.ioptions));
  }

  return ret;
}

Status RandomDbPathSupplierFactory::CfPathsSanityCheck(
    const rocksdb::ColumnFamilyOptions& /* cf_options */,
    const rocksdb::DBOptions& /* db_options */) {
  return Status::OK();
}

DbPathSupplierFactory* NewGradualMoveOldDataDbPathSupplierFactory() {
  return new GradualMoveOldDataDbPathSupplierFactory();
}

DbPathSupplierFactory* NewRandomDbPathSupplierFactory() {
  return new RandomDbPathSupplierFactory();
}

}
