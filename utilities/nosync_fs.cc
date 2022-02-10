//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/nosync_fs.h"

#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE {
namespace {
static std::unordered_map<std::string, OptionTypeInfo> no_sync_fs_option_info =
    {
#ifndef ROCKSDB_LITE
        {"sync",
         {offsetof(struct NoSyncOptions, do_sync), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever}},
        {"fsync",
         {offsetof(struct NoSyncOptions, do_fsync), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever}},
        {"range_sync",
         {offsetof(struct NoSyncOptions, do_rsync), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever}},
        {"dir_sync",
         {offsetof(struct NoSyncOptions, do_dsync), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever}},
#endif  // ROCKSDB_LITE
};
}  // namespace

NoSyncFileSystem::NoSyncFileSystem(const std::shared_ptr<FileSystem>& base,
                                   bool enabled)
    : InjectionFileSystem(base), sync_opts_(enabled) {
  RegisterOptions(&sync_opts_, &no_sync_fs_option_info);
}
}  // namespace ROCKSDB_NAMESPACE
