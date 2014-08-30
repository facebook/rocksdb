// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE
#include "table/cuckoo_table_factory.h"

#include "db/dbformat.h"
#include "table/cuckoo_table_builder.h"
#include "table/cuckoo_table_reader.h"

namespace rocksdb {
Status CuckooTableFactory::NewTableReader(const Options& options,
    const EnvOptions& soptions, const InternalKeyComparator& icomp,
    std::unique_ptr<RandomAccessFile>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table) const {
  std::unique_ptr<CuckooTableReader> new_reader(new CuckooTableReader(options,
      std::move(file), file_size, icomp.user_comparator(), nullptr));
  Status s = new_reader->status();
  if (s.ok()) {
    *table = std::move(new_reader);
  }
  return s;
}

TableBuilder* CuckooTableFactory::NewTableBuilder(
    const Options& options, const InternalKeyComparator& internal_comparator,
    WritableFile* file, CompressionType compression_type) const {
  return new CuckooTableBuilder(file, hash_table_ratio_, 64, max_search_depth_,
      internal_comparator.user_comparator(), cuckoo_block_size_, nullptr);
}

std::string CuckooTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(2000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  hash_table_ratio: %lf\n",
           hash_table_ratio_);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  max_search_depth: %u\n",
           max_search_depth_);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  cuckoo_block_size: %u\n",
           cuckoo_block_size_);
  ret.append(buffer);
  return ret;
}

TableFactory* NewCuckooTableFactory(double hash_table_ratio,
    uint32_t max_search_depth, uint32_t cuckoo_block_size) {
  return new CuckooTableFactory(
      hash_table_ratio, max_search_depth, cuckoo_block_size);
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
