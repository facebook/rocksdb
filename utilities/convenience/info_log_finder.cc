//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "rocksdb/utilities/info_log_finder.h"

#include "file/filename.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

Status GetInfoLogList(DB* db, std::vector<std::string>* info_log_list) {
  if (!db) {
    return Status::InvalidArgument("DB pointer is not valid");
  }
  std::string parent_path;
  const Options& options = db->GetOptions();
  return GetInfoLogFiles(options.env->GetFileSystem(), options.db_log_dir,
                         db->GetName(), &parent_path, info_log_list);
}
}  // namespace ROCKSDB_NAMESPACE
