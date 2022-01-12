//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "table/meta_blocks.h"

#include <map>
#include <string>

#include "block_fetcher.h"
#include "db/table_properties_collector.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "table/block_based/block.h"
#include "table/block_based/reader_common.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "table/persistent_cache_helper.h"
#include "table/sst_file_writer_collectors.h"
#include "table/table_properties_internal.h"
#include "test_util/sync_point.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

const std::string kPropertiesBlockName = "rocksdb.properties";
// Old property block name for backward compatibility
const std::string kPropertiesBlockOldName = "rocksdb.stats";
const std::string kCompressionDictBlockName = "rocksdb.compression_dict";
const std::string kRangeDelBlockName = "rocksdb.range_del";

MetaIndexBuilder::MetaIndexBuilder()
    : meta_index_block_(new BlockBuilder(1 /* restart interval */)) {}

void MetaIndexBuilder::Add(const std::string& key,
                           const BlockHandle& handle) {
  std::string handle_encoding;
  handle.EncodeTo(&handle_encoding);
  meta_block_handles_.insert({key, handle_encoding});
}

Slice MetaIndexBuilder::Finish() {
  for (const auto& metablock : meta_block_handles_) {
    meta_index_block_->Add(metablock.first, metablock.second);
  }
  return meta_index_block_->Finish();
}

// Property block will be read sequentially and cached in a heap located
// object, so there's no need for restart points. Thus we set the restart
// interval to infinity to save space.
PropertyBlockBuilder::PropertyBlockBuilder()
    : properties_block_(
          new BlockBuilder(port::kMaxInt32 /* restart interval */)) {}

void PropertyBlockBuilder::Add(const std::string& name,
                               const std::string& val) {
  props_.insert({name, val});
}

void PropertyBlockBuilder::Add(const std::string& name, uint64_t val) {
  assert(props_.find(name) == props_.end());

  std::string dst;
  PutVarint64(&dst, val);

  Add(name, dst);
}

void PropertyBlockBuilder::Add(
    const UserCollectedProperties& user_collected_properties) {
  for (const auto& prop : user_collected_properties) {
    Add(prop.first, prop.second);
  }
}

void PropertyBlockBuilder::AddTableProperty(const TableProperties& props) {
  TEST_SYNC_POINT_CALLBACK("PropertyBlockBuilder::AddTableProperty:Start",
                           const_cast<TableProperties*>(&props));

  Add(TablePropertiesNames::kOriginalFileNumber, props.orig_file_number);
  Add(TablePropertiesNames::kRawKeySize, props.raw_key_size);
  Add(TablePropertiesNames::kRawValueSize, props.raw_value_size);
  Add(TablePropertiesNames::kDataSize, props.data_size);
  Add(TablePropertiesNames::kIndexSize, props.index_size);
  if (props.index_partitions != 0) {
    Add(TablePropertiesNames::kIndexPartitions, props.index_partitions);
    Add(TablePropertiesNames::kTopLevelIndexSize, props.top_level_index_size);
  }
  Add(TablePropertiesNames::kIndexKeyIsUserKey, props.index_key_is_user_key);
  Add(TablePropertiesNames::kIndexValueIsDeltaEncoded,
      props.index_value_is_delta_encoded);
  Add(TablePropertiesNames::kNumEntries, props.num_entries);
  Add(TablePropertiesNames::kNumFilterEntries, props.num_filter_entries);
  Add(TablePropertiesNames::kDeletedKeys, props.num_deletions);
  Add(TablePropertiesNames::kMergeOperands, props.num_merge_operands);
  Add(TablePropertiesNames::kNumRangeDeletions, props.num_range_deletions);
  Add(TablePropertiesNames::kNumDataBlocks, props.num_data_blocks);
  Add(TablePropertiesNames::kFilterSize, props.filter_size);
  Add(TablePropertiesNames::kFormatVersion, props.format_version);
  Add(TablePropertiesNames::kFixedKeyLen, props.fixed_key_len);
  Add(TablePropertiesNames::kColumnFamilyId, props.column_family_id);
  Add(TablePropertiesNames::kCreationTime, props.creation_time);
  Add(TablePropertiesNames::kOldestKeyTime, props.oldest_key_time);
  if (props.file_creation_time > 0) {
    Add(TablePropertiesNames::kFileCreationTime, props.file_creation_time);
  }
  if (props.slow_compression_estimated_data_size > 0) {
    Add(TablePropertiesNames::kSlowCompressionEstimatedDataSize,
        props.slow_compression_estimated_data_size);
  }
  if (props.fast_compression_estimated_data_size > 0) {
    Add(TablePropertiesNames::kFastCompressionEstimatedDataSize,
        props.fast_compression_estimated_data_size);
  }
  if (!props.db_id.empty()) {
    Add(TablePropertiesNames::kDbId, props.db_id);
  }
  if (!props.db_session_id.empty()) {
    Add(TablePropertiesNames::kDbSessionId, props.db_session_id);
  }
  if (!props.db_host_id.empty()) {
    Add(TablePropertiesNames::kDbHostId, props.db_host_id);
  }

  if (!props.filter_policy_name.empty()) {
    Add(TablePropertiesNames::kFilterPolicy, props.filter_policy_name);
  }
  if (!props.comparator_name.empty()) {
    Add(TablePropertiesNames::kComparator, props.comparator_name);
  }

  if (!props.merge_operator_name.empty()) {
    Add(TablePropertiesNames::kMergeOperator, props.merge_operator_name);
  }
  if (!props.prefix_extractor_name.empty()) {
    Add(TablePropertiesNames::kPrefixExtractorName,
        props.prefix_extractor_name);
  }
  if (!props.property_collectors_names.empty()) {
    Add(TablePropertiesNames::kPropertyCollectors,
        props.property_collectors_names);
  }
  if (!props.column_family_name.empty()) {
    Add(TablePropertiesNames::kColumnFamilyName, props.column_family_name);
  }

  if (!props.compression_name.empty()) {
    Add(TablePropertiesNames::kCompression, props.compression_name);
  }
  if (!props.compression_options.empty()) {
    Add(TablePropertiesNames::kCompressionOptions, props.compression_options);
  }
}

Slice PropertyBlockBuilder::Finish() {
  for (const auto& prop : props_) {
    properties_block_->Add(prop.first, prop.second);
  }

  return properties_block_->Finish();
}

void LogPropertiesCollectionError(Logger* info_log, const std::string& method,
                                  const std::string& name) {
  assert(method == "Add" || method == "Finish");

  std::string msg =
    "Encountered error when calling TablePropertiesCollector::" +
    method + "() with collector name: " + name;
  ROCKS_LOG_ERROR(info_log, "%s", msg.c_str());
}

bool NotifyCollectTableCollectorsOnAdd(
    const Slice& key, const Slice& value, uint64_t file_size,
    const std::vector<std::unique_ptr<IntTblPropCollector>>& collectors,
    Logger* info_log) {
  bool all_succeeded = true;
  for (auto& collector : collectors) {
    Status s = collector->InternalAdd(key, value, file_size);
    all_succeeded = all_succeeded && s.ok();
    if (!s.ok()) {
      LogPropertiesCollectionError(info_log, "Add" /* method */,
                                   collector->Name());
    }
  }
  return all_succeeded;
}

void NotifyCollectTableCollectorsOnBlockAdd(
    const std::vector<std::unique_ptr<IntTblPropCollector>>& collectors,
    const uint64_t block_raw_bytes, const uint64_t block_compressed_bytes_fast,
    const uint64_t block_compressed_bytes_slow) {
  for (auto& collector : collectors) {
    collector->BlockAdd(block_raw_bytes, block_compressed_bytes_fast,
                        block_compressed_bytes_slow);
  }
}

bool NotifyCollectTableCollectorsOnFinish(
    const std::vector<std::unique_ptr<IntTblPropCollector>>& collectors,
    Logger* info_log, PropertyBlockBuilder* builder) {
  bool all_succeeded = true;
  for (auto& collector : collectors) {
    UserCollectedProperties user_collected_properties;
    Status s = collector->Finish(&user_collected_properties);

    all_succeeded = all_succeeded && s.ok();
    if (!s.ok()) {
      LogPropertiesCollectionError(info_log, "Finish" /* method */,
                                   collector->Name());
    } else {
      builder->Add(user_collected_properties);
    }
  }

  return all_succeeded;
}

// FIXME: should be a parameter for reading table properties to use persistent
// cache?
Status ReadTablePropertiesHelper(
    const ReadOptions& ro, const BlockHandle& handle,
    RandomAccessFileReader* file, FilePrefetchBuffer* prefetch_buffer,
    const Footer& footer, const ImmutableOptions& ioptions,
    std::unique_ptr<TableProperties>* table_properties,
    MemoryAllocator* memory_allocator) {
  assert(table_properties);

  // If this is an external SST file ingested with write_global_seqno set to
  // true, then we expect the checksum mismatch because checksum was written
  // by SstFileWriter, but its global seqno in the properties block may have
  // been changed during ingestion. For this reason, we initially read
  // and process without checksum verification, then later try checksum
  // verification so that if it fails, we can copy to a temporary buffer with
  // global seqno set to its original value, i.e. 0, and attempt checksum
  // verification again.
  ReadOptions modified_ro = ro;
  modified_ro.verify_checksums = false;
  BlockContents block_contents;
  BlockFetcher block_fetcher(file, prefetch_buffer, footer, modified_ro, handle,
                             &block_contents, ioptions, false /* decompress */,
                             false /*maybe_compressed*/, BlockType::kProperties,
                             UncompressionDict::GetEmptyDict(),
                             PersistentCacheOptions::kEmpty, memory_allocator);
  Status s = block_fetcher.ReadBlockContents();
  if (!s.ok()) {
    return s;
  }

  // Unfortunately, Block::size() might not equal block_contents.data.size(),
  // and Block hides block_contents
  uint64_t block_size = block_contents.data.size();
  Block properties_block(std::move(block_contents));
  std::unique_ptr<MetaBlockIter> iter(properties_block.NewMetaIterator());

  std::unique_ptr<TableProperties> new_table_properties{new TableProperties};
  // All pre-defined properties of type uint64_t
  std::unordered_map<std::string, uint64_t*> predefined_uint64_properties = {
      {TablePropertiesNames::kOriginalFileNumber,
       &new_table_properties->orig_file_number},
      {TablePropertiesNames::kDataSize, &new_table_properties->data_size},
      {TablePropertiesNames::kIndexSize, &new_table_properties->index_size},
      {TablePropertiesNames::kIndexPartitions,
       &new_table_properties->index_partitions},
      {TablePropertiesNames::kTopLevelIndexSize,
       &new_table_properties->top_level_index_size},
      {TablePropertiesNames::kIndexKeyIsUserKey,
       &new_table_properties->index_key_is_user_key},
      {TablePropertiesNames::kIndexValueIsDeltaEncoded,
       &new_table_properties->index_value_is_delta_encoded},
      {TablePropertiesNames::kFilterSize, &new_table_properties->filter_size},
      {TablePropertiesNames::kRawKeySize, &new_table_properties->raw_key_size},
      {TablePropertiesNames::kRawValueSize,
       &new_table_properties->raw_value_size},
      {TablePropertiesNames::kNumDataBlocks,
       &new_table_properties->num_data_blocks},
      {TablePropertiesNames::kNumEntries, &new_table_properties->num_entries},
      {TablePropertiesNames::kNumFilterEntries,
       &new_table_properties->num_filter_entries},
      {TablePropertiesNames::kDeletedKeys,
       &new_table_properties->num_deletions},
      {TablePropertiesNames::kMergeOperands,
       &new_table_properties->num_merge_operands},
      {TablePropertiesNames::kNumRangeDeletions,
       &new_table_properties->num_range_deletions},
      {TablePropertiesNames::kFormatVersion,
       &new_table_properties->format_version},
      {TablePropertiesNames::kFixedKeyLen,
       &new_table_properties->fixed_key_len},
      {TablePropertiesNames::kColumnFamilyId,
       &new_table_properties->column_family_id},
      {TablePropertiesNames::kCreationTime,
       &new_table_properties->creation_time},
      {TablePropertiesNames::kOldestKeyTime,
       &new_table_properties->oldest_key_time},
      {TablePropertiesNames::kFileCreationTime,
       &new_table_properties->file_creation_time},
      {TablePropertiesNames::kSlowCompressionEstimatedDataSize,
       &new_table_properties->slow_compression_estimated_data_size},
      {TablePropertiesNames::kFastCompressionEstimatedDataSize,
       &new_table_properties->fast_compression_estimated_data_size},
  };

  std::string last_key;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    s = iter->status();
    if (!s.ok()) {
      break;
    }

    auto key = iter->key().ToString();
    // properties block should be strictly sorted with no duplicate key.
    if (!last_key.empty() &&
        BytewiseComparator()->Compare(key, last_key) <= 0) {
      s = Status::Corruption("properties unsorted");
      break;
    }
    last_key = key;

    auto raw_val = iter->value();
    auto pos = predefined_uint64_properties.find(key);

    if (key == ExternalSstFilePropertyNames::kGlobalSeqno) {
      new_table_properties->external_sst_file_global_seqno_offset =
          handle.offset() + iter->ValueOffset();
    }

    if (pos != predefined_uint64_properties.end()) {
      if (key == TablePropertiesNames::kDeletedKeys ||
          key == TablePropertiesNames::kMergeOperands) {
        // Insert in user-collected properties for API backwards compatibility
        new_table_properties->user_collected_properties.insert(
            {key, raw_val.ToString()});
      }
      // handle predefined rocksdb properties
      uint64_t val;
      if (!GetVarint64(&raw_val, &val)) {
        // skip malformed value
        auto error_msg =
          "Detect malformed value in properties meta-block:"
          "\tkey: " + key + "\tval: " + raw_val.ToString();
        ROCKS_LOG_ERROR(ioptions.logger, "%s", error_msg.c_str());
        continue;
      }
      *(pos->second) = val;
    } else if (key == TablePropertiesNames::kDbId) {
      new_table_properties->db_id = raw_val.ToString();
    } else if (key == TablePropertiesNames::kDbSessionId) {
      new_table_properties->db_session_id = raw_val.ToString();
    } else if (key == TablePropertiesNames::kDbHostId) {
      new_table_properties->db_host_id = raw_val.ToString();
    } else if (key == TablePropertiesNames::kFilterPolicy) {
      new_table_properties->filter_policy_name = raw_val.ToString();
    } else if (key == TablePropertiesNames::kColumnFamilyName) {
      new_table_properties->column_family_name = raw_val.ToString();
    } else if (key == TablePropertiesNames::kComparator) {
      new_table_properties->comparator_name = raw_val.ToString();
    } else if (key == TablePropertiesNames::kMergeOperator) {
      new_table_properties->merge_operator_name = raw_val.ToString();
    } else if (key == TablePropertiesNames::kPrefixExtractorName) {
      new_table_properties->prefix_extractor_name = raw_val.ToString();
    } else if (key == TablePropertiesNames::kPropertyCollectors) {
      new_table_properties->property_collectors_names = raw_val.ToString();
    } else if (key == TablePropertiesNames::kCompression) {
      new_table_properties->compression_name = raw_val.ToString();
    } else if (key == TablePropertiesNames::kCompressionOptions) {
      new_table_properties->compression_options = raw_val.ToString();
    } else {
      // handle user-collected properties
      new_table_properties->user_collected_properties.insert(
          {key, raw_val.ToString()});
    }
  }

  // Modified version of BlockFetcher checksum verification
  // (See write_global_seqno comment above)
  if (s.ok() && footer.GetBlockTrailerSize() > 0) {
    s = VerifyBlockChecksum(footer.checksum_type(), properties_block.data(),
                            block_size, file->file_name(), handle.offset());
    if (s.IsCorruption()) {
      if (new_table_properties->external_sst_file_global_seqno_offset != 0) {
        std::string tmp_buf(properties_block.data(),
                            block_fetcher.GetBlockSizeWithTrailer());
        uint64_t global_seqno_offset =
            new_table_properties->external_sst_file_global_seqno_offset -
            handle.offset();
        EncodeFixed64(&tmp_buf[static_cast<size_t>(global_seqno_offset)], 0);
        s = VerifyBlockChecksum(footer.checksum_type(), tmp_buf.data(),
                                block_size, file->file_name(), handle.offset());
      }
    }
  }

  if (s.ok()) {
    *table_properties = std::move(new_table_properties);
  }

  return s;
}

Status ReadTableProperties(RandomAccessFileReader* file, uint64_t file_size,
                           uint64_t table_magic_number,
                           const ImmutableOptions& ioptions,
                           std::unique_ptr<TableProperties>* properties,
                           MemoryAllocator* memory_allocator,
                           FilePrefetchBuffer* prefetch_buffer) {
  BlockHandle block_handle;
  Footer footer;
  Status s = FindMetaBlockInFile(file, file_size, table_magic_number, ioptions,
                                 kPropertiesBlockName, &block_handle,
                                 memory_allocator, prefetch_buffer, &footer);
  if (!s.ok()) {
    return s;
  }

  if (!block_handle.IsNull()) {
    s = ReadTablePropertiesHelper(ReadOptions(), block_handle, file,
                                  prefetch_buffer, footer, ioptions, properties,
                                  memory_allocator);
  } else {
    s = Status::NotFound();
  }
  return s;
}

Status FindOptionalMetaBlock(InternalIterator* meta_index_iter,
                             const std::string& meta_block_name,
                             BlockHandle* block_handle) {
  assert(block_handle != nullptr);
  meta_index_iter->Seek(meta_block_name);
  if (meta_index_iter->status().ok()) {
    if (meta_index_iter->Valid() && meta_index_iter->key() == meta_block_name) {
      Slice v = meta_index_iter->value();
      return block_handle->DecodeFrom(&v);
    } else if (meta_block_name == kPropertiesBlockName) {
      // Have to try old name for compatibility
      meta_index_iter->Seek(kPropertiesBlockOldName);
      if (meta_index_iter->status().ok() && meta_index_iter->Valid() &&
          meta_index_iter->key() == kPropertiesBlockOldName) {
        Slice v = meta_index_iter->value();
        return block_handle->DecodeFrom(&v);
      }
    }
  }
  // else
  *block_handle = BlockHandle::NullBlockHandle();
  return meta_index_iter->status();
}

Status FindMetaBlock(InternalIterator* meta_index_iter,
                     const std::string& meta_block_name,
                     BlockHandle* block_handle) {
  Status s =
      FindOptionalMetaBlock(meta_index_iter, meta_block_name, block_handle);
  if (s.ok() && block_handle->IsNull()) {
    return Status::Corruption("Cannot find the meta block", meta_block_name);
  } else {
    return s;
  }
}

Status FindMetaBlockInFile(RandomAccessFileReader* file, uint64_t file_size,
                           uint64_t table_magic_number,
                           const ImmutableOptions& ioptions,
                           const std::string& meta_block_name,
                           BlockHandle* block_handle,
                           MemoryAllocator* memory_allocator,
                           FilePrefetchBuffer* prefetch_buffer,
                           Footer* footer_out) {
  Footer footer;
  IOOptions opts;
  auto s = ReadFooterFromFile(opts, file, prefetch_buffer, file_size, &footer,
                              table_magic_number);
  if (!s.ok()) {
    return s;
  }
  if (footer_out) {
    *footer_out = footer;
  }

  auto metaindex_handle = footer.metaindex_handle();
  BlockContents metaindex_contents;
  s = BlockFetcher(file, prefetch_buffer, footer, ReadOptions(),
                   metaindex_handle, &metaindex_contents, ioptions,
                   false /* do decompression */, false /*maybe_compressed*/,
                   BlockType::kMetaIndex, UncompressionDict::GetEmptyDict(),
                   PersistentCacheOptions::kEmpty, memory_allocator)
          .ReadBlockContents();
  if (!s.ok()) {
    return s;
  }
  // meta blocks are never compressed. Need to add uncompress logic if we are to
  // compress it.
  Block metaindex_block(std::move(metaindex_contents));

  std::unique_ptr<InternalIterator> meta_iter;
  meta_iter.reset(metaindex_block.NewMetaIterator());

  return FindMetaBlock(meta_iter.get(), meta_block_name, block_handle);
}

Status ReadMetaBlock(RandomAccessFileReader* file,
                     FilePrefetchBuffer* prefetch_buffer, uint64_t file_size,
                     uint64_t table_magic_number,
                     const ImmutableOptions& ioptions,
                     const std::string& meta_block_name, BlockType block_type,
                     BlockContents* contents,
                     MemoryAllocator* memory_allocator) {
  // TableProperties requires special handling because of checksum issues.
  // Call ReadTableProperties instead for that case.
  assert(block_type != BlockType::kProperties);

  BlockHandle block_handle;
  Footer footer;
  Status status = FindMetaBlockInFile(
      file, file_size, table_magic_number, ioptions, meta_block_name,
      &block_handle, memory_allocator, prefetch_buffer, &footer);
  if (!status.ok()) {
    return status;
  }

  return BlockFetcher(file, prefetch_buffer, footer, ReadOptions(),
                      block_handle, contents, ioptions, false /* decompress */,
                      false /*maybe_compressed*/, block_type,
                      UncompressionDict::GetEmptyDict(),
                      PersistentCacheOptions::kEmpty, memory_allocator)
      .ReadBlockContents();
}

}  // namespace ROCKSDB_NAMESPACE
