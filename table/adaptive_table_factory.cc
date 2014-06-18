// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE
#include "table/adaptive_table_factory.h"

#include "table/format.h"

namespace rocksdb {

AdaptiveTableFactory::AdaptiveTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write,
    std::shared_ptr<TableFactory> block_based_table_factory,
    std::shared_ptr<TableFactory> plain_table_factory)
    : table_factory_to_write_(table_factory_to_write),
      block_based_table_factory_(block_based_table_factory),
      plain_table_factory_(plain_table_factory) {
  if (!table_factory_to_write_) {
    table_factory_to_write_ = block_based_table_factory_;
  }
  if (!plain_table_factory_) {
    plain_table_factory_.reset(NewPlainTableFactory());
  }
  if (!block_based_table_factory_) {
    block_based_table_factory_.reset(NewBlockBasedTableFactory());
  }
}

extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;
extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;

Status AdaptiveTableFactory::NewTableReader(
    const Options& options, const EnvOptions& soptions,
    const InternalKeyComparator& icomp, unique_ptr<RandomAccessFile>&& file,
    uint64_t file_size, unique_ptr<TableReader>* table) const {
  Footer footer;
  auto s = ReadFooterFromFile(file.get(), file_size, &footer);
  if (!s.ok()) {
    return s;
  }
  if (footer.table_magic_number() == kPlainTableMagicNumber ||
      footer.table_magic_number() == kLegacyPlainTableMagicNumber) {
    return plain_table_factory_->NewTableReader(
        options, soptions, icomp, std::move(file), file_size, table);
  } else if (footer.table_magic_number() == kBlockBasedTableMagicNumber ||
      footer.table_magic_number() == kLegacyBlockBasedTableMagicNumber) {
    return block_based_table_factory_->NewTableReader(
        options, soptions, icomp, std::move(file), file_size, table);
  } else {
    return Status::NotSupported("Unidentified table format");
  }
}

TableBuilder* AdaptiveTableFactory::NewTableBuilder(
    const Options& options, const InternalKeyComparator& internal_comparator,
    WritableFile* file, CompressionType compression_type) const {
  return table_factory_to_write_->NewTableBuilder(options, internal_comparator,
                                                  file, compression_type);
}

extern TableFactory* NewAdaptiveTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write,
    std::shared_ptr<TableFactory> block_based_table_factory,
    std::shared_ptr<TableFactory> plain_table_factory) {
  return new AdaptiveTableFactory(
      table_factory_to_write, block_based_table_factory, plain_table_factory);
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
