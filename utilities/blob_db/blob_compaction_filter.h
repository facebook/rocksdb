//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE

#include <unordered_set>

#include "db/blob_index.h"
#include "monitoring/statistics.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "utilities/blob_db/blob_db_impl.h"

namespace rocksdb {
namespace blob_db {

struct BlobCompactionContext {
  uint64_t next_file_number = 0;
  std::unordered_set<uint64_t> current_blob_files;
  SequenceNumber fifo_eviction_seq = 0;
  uint64_t evict_expiration_up_to = 0;
};

struct BlobCompactionContextGC {
  BlobDBImpl* blob_db_impl = nullptr;
  uint64_t cutoff_file_number = 0;
};

class BlobIndexCompactionFilter;
class BlobIndexCompactionFilterGC;

template <typename Filter>
class BlobIndexCompactionFilterFactoryBase : public CompactionFilterFactory {
 public:
  BlobIndexCompactionFilterFactoryBase(BlobDBImpl* blob_db_impl, Env* env,
                                       Statistics* statistics)
      : blob_db_impl_(blob_db_impl), env_(env), statistics_(statistics) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override;

 private:
  BlobDBImpl* blob_db_impl_;
  Env* env_;
  Statistics* statistics_;
};

class BlobIndexCompactionFilterFactory
    : public BlobIndexCompactionFilterFactoryBase<BlobIndexCompactionFilter> {
 public:
  BlobIndexCompactionFilterFactory(BlobDBImpl* blob_db_impl, Env* env,
                                   Statistics* statistics)
      : BlobIndexCompactionFilterFactoryBase(blob_db_impl, env, statistics) {}

  const char* Name() const override {
    return "BlobIndexCompactionFilterFactory";
  }
};

class BlobIndexCompactionFilterFactoryGC
    : public BlobIndexCompactionFilterFactoryBase<BlobIndexCompactionFilterGC> {
 public:
  BlobIndexCompactionFilterFactoryGC(BlobDBImpl* blob_db_impl, Env* env,
                                     Statistics* statistics)
      : BlobIndexCompactionFilterFactoryBase(blob_db_impl, env, statistics) {}

  const char* Name() const override {
    return "BlobIndexCompactionFilterFactoryGC";
  }
};

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
