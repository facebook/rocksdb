//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "table/plain_table_builder.h"

#include <assert.h>

#include <string>
#include <limits>
#include <map>

#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/plain_table_factory.h"
#include "db/dbformat.h"
#include "table/block_builder.h"
#include "table/plain_table_index.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/stop_watch.h"

namespace rocksdb {

namespace {

// a utility that helps writing block content to the file
//   @offset will advance if @block_contents was successfully written.
//   @block_handle the block handle this particular block.
Status WriteBlock(const Slice& block_contents, WritableFileWriter* file,
                  uint64_t* offset, BlockHandle* block_handle) {
  block_handle->set_offset(*offset);
  block_handle->set_size(block_contents.size());
  Status s = file->Append(block_contents);

  if (s.ok()) {
    *offset += block_contents.size();
  }
  return s;
}

}  // namespace

// kPlainTableMagicNumber was picked by running
//    echo rocksdb.table.plain | sha1sum
// and taking the leading 64 bits.
extern const uint64_t kPlainTableMagicNumber = 0x8242229663bf9564ull;
extern const uint64_t kLegacyPlainTableMagicNumber = 0x4f3418eb7a8f13b8ull;

PlainTableBuilder::PlainTableBuilder(
    const ImmutableCFOptions& ioptions, const MutableCFOptions& moptions,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, WritableFileWriter* file, uint32_t user_key_len,
    EncodingType encoding_type, size_t index_sparseness,
    const std::string& column_family_name)
    : ioptions_(ioptions),
      moptions_(moptions),
      file_(file),
      encoder_(encoding_type, user_key_len, moptions.prefix_extractor.get(),
               index_sparseness),
      prefix_extractor_(moptions.prefix_extractor.get()) {
  properties_.fixed_key_len = user_key_len;

  // for plain table, we put all the data in a big chuck.
  properties_.num_data_blocks = 1;
  properties_.index_size = 0;
  properties_.filter_size = 0;
  // To support roll-back to previous version, now still use version 0 for
  // plain encoding.
  properties_.format_version = (encoding_type == kPlain) ? 0 : 1;
  properties_.column_family_id = column_family_id;
  properties_.column_family_name = column_family_name;
  properties_.prefix_extractor_name = moptions_.prefix_extractor != nullptr
                                          ? moptions_.prefix_extractor->Name()
                                          : "nullptr";

  std::string val;
  PutFixed32(&val, static_cast<uint32_t>(encoder_.GetEncodingType()));
  properties_.user_collected_properties
      [PlainTablePropertyNames::kEncodingType] = val;

  for (auto& collector_factories : *int_tbl_prop_collector_factories) {
    table_properties_collectors_.emplace_back(
        collector_factories->CreateIntTblPropCollector(column_family_id));
  }
}

PlainTableBuilder::~PlainTableBuilder() {
}

void PlainTableBuilder::Add(const Slice& key, const Slice& value) {
  // temp buffer for metadata bytes between key and value.
  char meta_bytes_buf[6];
  size_t meta_bytes_buf_size = 0;

  ParsedInternalKey internal_key;
  if (!ParseInternalKey(key, &internal_key)) {
    assert(false);
    return;
  }
  if (internal_key.type == kTypeRangeDeletion) {
    status_ = Status::NotSupported("Range deletion unsupported");
    return;
  }

  // Write value
  assert(offset_ <= std::numeric_limits<uint32_t>::max());
  // Write out the key
  encoder_.AppendKey(key, file_, &offset_, meta_bytes_buf,
                     &meta_bytes_buf_size);

  // Write value length
  uint32_t value_size = static_cast<uint32_t>(value.size());
  char* end_ptr =
      EncodeVarint32(meta_bytes_buf + meta_bytes_buf_size, value_size);
  assert(end_ptr <= meta_bytes_buf + sizeof(meta_bytes_buf));
  meta_bytes_buf_size = end_ptr - meta_bytes_buf;
  file_->Append(Slice(meta_bytes_buf, meta_bytes_buf_size));

  // Write value
  file_->Append(value);
  offset_ += value_size + meta_bytes_buf_size;

  properties_.num_entries++;
  properties_.raw_key_size += key.size();
  properties_.raw_value_size += value.size();
  if (internal_key.type == kTypeDeletion ||
      internal_key.type == kTypeSingleDeletion) {
    properties_.num_deletions++;
  } else if (internal_key.type == kTypeMerge) {
    properties_.num_merge_operands++;
  }

  // notify property collectors
  NotifyCollectTableCollectorsOnAdd(
      key, value, offset_, table_properties_collectors_, ioptions_.info_log);
}

Status PlainTableBuilder::status() const { return status_; }

Status PlainTableBuilder::Finish() {
  assert(!closed_);
  closed_ = true;

  properties_.data_size = offset_;

  //  Write the following blocks
  //  1. [meta block: bloom] - optional
  //  2. [meta block: index] - optional
  //  3. [meta block: properties]
  //  4. [metaindex block]
  //  5. [footer]

  MetaIndexBuilder meta_index_builer;

  // Calculate bloom block size and index block size
  PropertyBlockBuilder property_block_builder;
  // -- Add basic properties
  property_block_builder.AddTableProperty(properties_);

  property_block_builder.Add(properties_.user_collected_properties);

  // -- Add user collected properties
  NotifyCollectTableCollectorsOnFinish(table_properties_collectors_,
                                       ioptions_.info_log,
                                       &property_block_builder);

  // -- Write property block
  BlockHandle property_block_handle;
  auto s = WriteBlock(
      property_block_builder.Finish(),
      file_,
      &offset_,
      &property_block_handle
  );
  if (!s.ok()) {
    return s;
  }
  meta_index_builer.Add(kPropertiesBlock, property_block_handle);

  // -- write metaindex block
  BlockHandle metaindex_block_handle;
  s = WriteBlock(
      meta_index_builer.Finish(),
      file_,
      &offset_,
      &metaindex_block_handle
  );
  if (!s.ok()) {
    return s;
  }

  // Write Footer
  // no need to write out new footer if we're using default checksum
  Footer footer(kLegacyPlainTableMagicNumber, 0);
  footer.set_metaindex_handle(metaindex_block_handle);
  footer.set_index_handle(BlockHandle::NullBlockHandle());
  std::string footer_encoding;
  footer.EncodeTo(&footer_encoding);
  s = file_->Append(footer_encoding);
  if (s.ok()) {
    offset_ += footer_encoding.size();
  }

  return s;
}

void PlainTableBuilder::Abandon() {
  closed_ = true;
}

uint64_t PlainTableBuilder::NumEntries() const {
  return properties_.num_entries;
}

uint64_t PlainTableBuilder::FileSize() const {
  return offset_;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
