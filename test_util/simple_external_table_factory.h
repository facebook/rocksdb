//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/external_table.h"
#include "rocksdb/file_system.h"

// Test-only external table implementation shared by basic and full modes. It
// writes length-prefixed key/value records in caller-provided order, followed
// by RocksDB's properties block and a footer describing the properties and
// checksum. Readers materialize the records in memory and use either the user
// or internal-key comparator according to Mode. The file is immutable after
// Finish(), so ingestion must not rewrite its global sequence number.
//
// File layout:
//
//   +---------+------------+---------+-------+----------+
//   | records | properties | prop sz | magic | checksum |
//   +---------+------------+---------+-------+----------+
//
// Each record is [key size][value size][key][value]. Sizes and footer fields
// use fixed-width encoding.

namespace ROCKSDB_NAMESPACE {

using SimpleExternalTableEntries =
    std::vector<std::pair<std::string, std::string>>;

template <ExternalTableMode Mode>
class SimpleExternalTableReader : public ExternalTableReaderBase<Mode> {
 public:
  using GetArgument = typename ExternalTableReaderLookupBase<Mode>::GetArgument;

  SimpleExternalTableReader(const ReadOptions& read_options,
                            const ExternalTableOptions& options,
                            std::unique_ptr<FSRandomAccessFile>&& file,
                            uint64_t file_size);
  ~SimpleExternalTableReader() override;

  Status status() const;
  ExternalTableIteratorBase* NewIterator(
      const ReadOptions& read_options,
      const SliceTransform* prefix_extractor) override;
  Status Get(const ReadOptions& read_options, const Slice& key,
             const SliceTransform* prefix_extractor,
             GetArgument result) override;
  Status GetPropertiesBlock(std::unique_ptr<char[]>* block, uint64_t* size,
                            uint64_t* file_offset) override;
  std::shared_ptr<const TableProperties> GetTableProperties() const override;
  Status VerifyChecksum(const ReadOptions& read_options) override;

 private:
  Status ReadContents(const ReadOptions& read_options,
                      std::string* contents) const;
  Status DecodeFooter(const Slice& contents, uint64_t* properties_size,
                      uint32_t* contents_checksum) const;
  Status VerifyContentsChecksum(const Slice& contents) const;

  std::unique_ptr<FSRandomAccessFile> file_;
  uint64_t file_size_;
  const CompareInterface* key_comparator_;
  const Comparator* user_comparator_;
  SimpleExternalTableEntries entries_;
  std::string properties_block_;
  uint64_t properties_offset_ = 0;
  Status status_;
  Status checksum_status_;
};

template <ExternalTableMode Mode>
class SimpleExternalTableFactoryBase : public ExternalTableFactoryBase<Mode> {
 public:
  SimpleExternalTableFactoryBase();
  ~SimpleExternalTableFactoryBase() override;

  const char* Name() const override;
  Status NewTableReader(const ReadOptions& read_options,
                        const std::string& file_path,
                        const ExternalTableOptions& table_options,
                        std::unique_ptr<FSRandomAccessFile>&& file,
                        uint64_t file_size,
                        std::unique_ptr<ExternalTableReaderBase<Mode>>*
                            table_reader) const override;
  ExternalTableBuilderBase* NewTableBuilder(
      const ExternalTableBuilderOptions& builder_options,
      const std::string& file_path, FSWritableFile* file) const override;
};

using SimpleExternalTableFactory =
    SimpleExternalTableFactoryBase<ExternalTableMode::kOnlyZeroSeqnoAndPuts>;
using SimpleFullExternalTableFactory =
    SimpleExternalTableFactoryBase<ExternalTableMode::kFull>;

}  // namespace ROCKSDB_NAMESPACE
