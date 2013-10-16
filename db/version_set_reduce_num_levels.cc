//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "util/logging.h"

namespace rocksdb {

Status VersionSet::ReduceNumberOfLevels(int new_levels, port::Mutex* mu) {

  if(new_levels <= 1) {
    return Status::InvalidArgument(
        "Number of levels needs to be bigger than 1");
  }

  Version* current_version = current_;
  int current_levels = NumberLevels();

  if (current_levels <= new_levels) {
    return Status::OK();
  }

  // Make sure there are file only on one level from
  // (new_levels-1) to (current_levels-1)
  int first_nonempty_level = -1;
  int first_nonempty_level_filenum = 0;
  for (int i = new_levels - 1; i < current_levels; i++) {
    int file_num = NumLevelFiles(i);
    if (file_num != 0) {
      if (first_nonempty_level < 0) {
        first_nonempty_level = i;
        first_nonempty_level_filenum = file_num;
      } else {
        char msg[255];
        sprintf(msg, "Found at least two levels containing files: "
            "[%d:%d],[%d:%d].\n",
            first_nonempty_level, first_nonempty_level_filenum, i, file_num);
        return Status::InvalidArgument(msg);
      }
    }
  }

  Status st;
  std::vector<FileMetaData*>*  old_files_list = current_version->files_;
  std::vector<FileMetaData*>* new_files_list =
      new std::vector<FileMetaData*>[new_levels];
  for (int i = 0; i < new_levels - 1; i++) {
    new_files_list[i] = old_files_list[i];
  }

  if (first_nonempty_level > 0) {
    new_files_list[new_levels - 1] = old_files_list[first_nonempty_level];
  }

  delete[] current_version->files_;
  current_version->files_ = new_files_list;

  delete[] compact_pointer_;
  delete[] max_file_size_;
  delete[] level_max_bytes_;
  num_levels_ = new_levels;
  compact_pointer_ = new std::string[new_levels];
  Init(new_levels);
  VersionEdit ve(new_levels);
  st = LogAndApply(&ve , mu, true);
  return st;
}

}
