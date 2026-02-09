// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {
Status VerifySstFileChecksumInternal(const Options& options,
                                     const EnvOptions& env_options,
                                     const ReadOptions& read_options,
                                     const std::string& file_path,
                                     const SequenceNumber& largest_seqno = 0);
}  // namespace ROCKSDB_NAMESPACE
