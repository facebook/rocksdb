//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "db/version_edit.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/io_status.h"
#include "rocksdb/table.h"
#include "table/internal_iterator.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/kv_map.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace mock {
using KVPair = std::pair<std::string, std::string>;
using KVVector = std::vector<KVPair>;

KVVector MakeMockFile(std::initializer_list<KVPair> l = {});
void SortKVVector(KVVector* kv_vector,
                  const Comparator* ucmp = BytewiseComparator());

struct MockTableFileSystem {
  port::Mutex mutex;
  std::map<uint32_t, KVVector> files;
};

class MockTableFactory : public TableFactory {
 public:
  enum MockCorruptionMode {
    kCorruptNone,
    kCorruptKey,
    kCorruptValue,
    kCorruptReorderKey,
  };

  MockTableFactory();
  const char* Name() const override { return "MockTable"; }
  using TableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& ro, const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache = true) const override;
  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_familly_id, WritableFileWriter* file) const override;

  // This function will directly create mock table instead of going through
  // MockTableBuilder. file_contents has to have a format of <internal_key,
  // value>. Those key-value pairs will then be inserted into the mock table.
  Status CreateMockTable(Env* env, const std::string& fname,
                         KVVector file_contents);

  virtual std::string GetPrintableOptions() const override {
    return std::string();
  }

  void SetCorruptionMode(MockCorruptionMode mode) { corrupt_mode_ = mode; }
  // This function will assert that only a single file exists and that the
  // contents are equal to file_contents
  void AssertSingleFile(const KVVector& file_contents);
  void AssertLatestFile(const KVVector& file_contents);

 private:
  Status GetAndWriteNextID(WritableFileWriter* file, uint32_t* id) const;
  Status GetIDFromFile(RandomAccessFileReader* file, uint32_t* id) const;

  mutable MockTableFileSystem file_system_;
  mutable std::atomic<uint32_t> next_id_;
  MockCorruptionMode corrupt_mode_;
};

}  // namespace mock
}  // namespace ROCKSDB_NAMESPACE
