//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {
class FIFOCompactionPicker : public CompactionPicker {
 public:
  FIFOCompactionPicker(const ImmutableOptions& ioptions,
                       const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}

  Compaction* PickCompaction(const std::string& cf_name,
                             const MutableCFOptions& mutable_cf_options,
                             const MutableDBOptions& mutable_db_options,
                             VersionStorageInfo* version,
                             LogBuffer* log_buffer) override;

  Compaction* CompactRange(const std::string& cf_name,
                           const MutableCFOptions& mutable_cf_options,
                           const MutableDBOptions& mutable_db_options,
                           VersionStorageInfo* vstorage, int input_level,
                           int output_level,
                           const CompactRangeOptions& compact_range_options,
                           const InternalKey* begin, const InternalKey* end,
                           InternalKey** compaction_end, bool* manual_conflict,
                           uint64_t max_file_num_to_ignore,
                           const std::string& trim_ts) override;

  // The maximum allowed output level.  Always returns 0.
  int MaxOutputLevel() const override { return 0; }

  bool NeedsCompaction(const VersionStorageInfo* vstorage) const override;

 private:
  Compaction* PickTTLCompaction(const std::string& cf_name,
                                const MutableCFOptions& mutable_cf_options,
                                const MutableDBOptions& mutable_db_options,
                                VersionStorageInfo* version,
                                LogBuffer* log_buffer);

  Compaction* PickSizeCompaction(const std::string& cf_name,
                                 const MutableCFOptions& mutable_cf_options,
                                 const MutableDBOptions& mutable_db_options,
                                 VersionStorageInfo* version,
                                 LogBuffer* log_buffer);

  // Will pick one file to compact at a time, starting from the oldest file.
  Compaction* PickTemperatureChangeCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer) const;
};
}  // namespace ROCKSDB_NAMESPACE
