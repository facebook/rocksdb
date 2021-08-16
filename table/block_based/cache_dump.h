//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <set>

#include "cache/cache_entry_roles.h"
#include "file/writable_file_writer.h"
#include "port/lang.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "table/block_based/block.h"
#include "table/block_based/block_like_traits.h"
#include "table/block_based/block_type.h"
#include "table/block_based/cachable_entry.h"
#include "table/block_based/parsed_full_filter_block.h"
#include "table/block_based/reader_common.h"
#include "table/format.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

enum CacheDumpBlockType : unsigned char {
  kHeader = 1,
  kFooter = 2,
  kData = 3,
  kFilter = 4,
  kProperties = 5,
  kCompressionDictionary = 6,
  kRangeDeletion = 7,
  kHashIndexPrefixes = 8,
  kHashIndexMetadata = 9,
  kMetaIndex = 10,
  kIndex = 11,
  kDeprecatedFilterBlock = 12,
  kFilterMetaBlock = 13,
  kBlockTypeMax,
};

/*
std::unordered_map<CacheDumpBlockType, CacheEntryRole> CacheDumpBlockTypeMap =
{            {CacheDumpBlockType::kData, CacheEntryRole::kDataBlock},
              {CacheDumpBlockType::kFilter, CacheEntryRole::kFilterBlock},
  {CacheDumpBlockType::kProperties, CacheEntryRole::kOtherBlock},
  {CacheDumpBlockType::kCompressionDictionary, CacheEntryRole::kOtherBlock},
    {CacheDumpBlockType::kRangeDeletion, CacheEntryRole::kOtherBlock},
      {CacheDumpBlockType::kHashIndexPrefixes, CacheEntryRole::kOtherBlock},
        {CacheDumpBlockType::kHashIndexMetadata, CacheEntryRole::kOtherBlock},
          {CacheDumpBlockType::kMetaIndex, CacheEntryRole::kOtherBlock},
            {CacheDumpBlockType::kIndex, CacheEntryRole::kIndexBlock},
              {CacheDumpBlockType::kHeader, CacheEntryRole::kOtherBlock},
                {CacheDumpBlockType::kFooter, CacheEntryRole::kOtherBlock},
                {CacheDumpBlockTyp}};
*/
/*
std::unordered_map<CacheDumpBlockType, BlockType>
    CacheDumpBlockToCacheBlockMap = {
        {CacheDumpBlockType::kData, BlockType::kData},
        {CacheDumpBlockType::kFilter, BlockType::kFilter},
        {CacheDumpBlockType::kProperties, BlockType::kProperties},
        {CacheDumpBlockType::kCompressionDictionary,
         BlockType::kCompressionDictionary},
        {CacheDumpBlockType::kRangeDeletion, BlockType::kRangeDeletion},
        {CacheDumpBlockType::kHashIndexPrefixes, BlockType::kHashIndexPrefixes},
        {CacheDumpBlockType::kHashIndexMetadata, BlockType::kHashIndexMetadata},
        {CacheDumpBlockType::kMetaIndex, BlockType::kMetaIndex},
        {CacheDumpBlockType::kIndex, BlockType::kIndex},
        {CacheDumpBlockType::kDeprecatedFilterBlock, BlockType::kFilter},
        {CacheDumpBlockType::kFilterMetaBlock, BlockType::kFilter},
        {CacheDumpBlockType::kBlockTypeMax, BlockType::kInvalid}};
*/
/*
std::unordered_map<CacheEntryRole, CacheDumpBlockType>
    CacheEntryRoleToCacheDumpBlockType = {
        {CacheEntryRole::kDataBlock, CacheDumpBlockType::kData},
        {CacheEntryRole::kDeprecatedFilterBlock,
         CacheDumpBlockType::kDeprecatedFilterBlock},
        {CacheEntryRole::kFilterBlock, CacheDumpBlockType::kFilter},
        {CacheEntryRole::kFilterMetaBlock,
         CacheDumpBlockType::kFilterMetaBlock},
        {CacheEntryRole::kIndexBlock, CacheDumpBlockType::kIndex},
};
*/

struct DataContent {
  uint64_t timestamp;
  CacheDumpBlockType type;
  Slice key;
  size_t value_len;
  uint32_t value_checksum;
  void* value;

  void reset() {
    timestamp = 0;
    type = CacheDumpBlockType::kBlockTypeMax;
    key.clear();
    value_len = 0;
    value_checksum = 0;
    value = nullptr;
  }
};

class CacheDumperHelper {
 public:
  static void EncodeBlock(const DataContent& data_content, std::string* data) {
    assert(data);
    std::string encoded_data;
    PutFixed64(&encoded_data, data_content.timestamp);
    encoded_data.push_back(data_content.type);
    PutLengthPrefixedSlice(&encoded_data, data_content.key);
    PutFixed32(&encoded_data, static_cast<uint32_t>(data_content.value_len));
    PutFixed32(&encoded_data, data_content.value_checksum);
    fprintf(stdout, "key size: %d, block_size: %d\n",
            static_cast<int>(data_content.key.size()),
            static_cast<int>(data_content.value_len));
    PutLengthPrefixedSlice(&encoded_data, Slice((char*)data_content.value,
                                                data_content.value_len));

    uint32_t total_size = static_cast<uint32_t>(encoded_data.size());
    PutFixed32(data, total_size);
    data->append(encoded_data);
  }

  static Status DecodeBlock(const std::string& encoded_data,
                            DataContent* data_content) {
    assert(data_content != nullptr);
    Slice encoded_slice = Slice(encoded_data);

    // Decode timestamp
    if (!GetFixed64(&encoded_slice, &data_content->timestamp)) {
      return Status::Incomplete("Decode dumped block string failed");
    }
    // Decode the block type
    data_content->type = static_cast<CacheDumpBlockType>(encoded_slice[0]);
    encoded_slice.remove_prefix(1);
    // Decode the key
    if (!GetLengthPrefixedSlice(&encoded_slice, &(data_content->key))) {
      return Status::Incomplete("Decode dumped block string failed");
    }
    // Decode the value size
    uint32_t value_len;
    if (!GetFixed32(&encoded_slice, &value_len)) {
      return Status::Incomplete("Decode dumped block string failed");
    }
    data_content->value_len = static_cast<size_t>(value_len);
    // Decode the value checksum
    if (!GetFixed32(&encoded_slice, &(data_content->value_checksum))) {
      return Status::Incomplete("Decode dumped block string failed");
    }
    // Decode the block content and copy to the memory space whose pointer
    // will be managed by the cache finally.
    Slice block;
    if (!GetLengthPrefixedSlice(&encoded_slice, &block)) {
      return Status::Incomplete("Decode dumped block string failed");
    }
    char* v_b = new char[data_content->value_len];
    memcpy(v_b, block.data(), data_content->value_len);
    data_content->value = (void*)v_b;
    fprintf(stdout, "key size: %d, block_size: %d\n",
            static_cast<int>(data_content->key.size()),
            static_cast<int>(data_content->value_len));
    return Status::OK();
  }
};

class CacheDumper {
 public:
  CacheDumper(Env* env, const CacheDumpOptions& dump_options,
              const std::shared_ptr<Cache> cache, const std::string& db_id,
              SystemClock* clock)
      : env_(env),
        options_(dump_options),
        cache_(cache),
        db_id_(db_id),
        clock_(clock),
        prefix_size_(0) {}
  ~CacheDumper() {}

  void SetCacheKeyFilter(const std::set<std::string>& filter_set,
                         size_t prefix_size) {
    prefix_filter_ = filter_set;
    prefix_size_ = prefix_size;
  }

  Status Prepare() {
    // First, we prepare the file writer
    if (env_ == nullptr) {
      return IOStatus::IOError("Env is null");
    }
    Status s = WritableFileWriter::Create(
        env_->GetFileSystem(), options_.dump_file_path, FileOptions(),
        &file_writer_, nullptr);
    if (!s.ok()) {
      return s;
    }

    // Then, we copy the Cache Deleter Role Map as its member.
    role_map_ = CopyCacheDeleterRoleMap();
    return s;
  }

  std::function<void(const Slice&, void*, size_t, Cache::DeleterFn)>
  DumpOneBlockCallBack() {
    return [&](const Slice& key, void* value, size_t /*charge*/,
               Cache::DeleterFn deleter) {
      auto e = role_map_.find(deleter);
      CacheEntryRole role;
      CacheDumpBlockType type;
      if (e == role_map_.end()) {
        role = CacheEntryRole::kMisc;
      } else {
        role = e->second;
      }
      bool filter_out = false;
      std::string prefix = key.ToString().substr(0, prefix_size_);
      (void)prefix;
      // TODO: the filter of the keys
      /*
      if (prefix_filter_.size() > 0 &&
          prefix_filter_.find(prefix) == prefix_filter_.end()) {
        filter_out = true;
      }
      */
      const char* block_start = nullptr;
      size_t block_len = 0;
      switch (role) {
        case CacheEntryRole::kDataBlock:
          type = CacheDumpBlockType::kData;
          block_start = (static_cast<Block*>(value))->data();
          block_len = (static_cast<Block*>(value))->size();
          fprintf(stdout, "kDataBlock");
          break;
        case CacheEntryRole::kDeprecatedFilterBlock:
          type = CacheDumpBlockType::kDeprecatedFilterBlock;
          block_start = (static_cast<BlockContents*>(value))->data.data();
          block_len = (static_cast<BlockContents*>(value))->data.size();
          fprintf(stdout, "kDeprecatedFilterBlock");
          break;
        case CacheEntryRole::kFilterBlock:
          type = CacheDumpBlockType::kFilter;
          block_start = (static_cast<ParsedFullFilterBlock*>(value))
                            ->GetBlockContentsData()
                            .data();
          block_len = (static_cast<ParsedFullFilterBlock*>(value))
                          ->GetBlockContentsData()
                          .size();
          fprintf(stdout, "kFilterBlock");
          break;
        case CacheEntryRole::kFilterMetaBlock:
          type = CacheDumpBlockType::kFilterMetaBlock;
          block_start = (static_cast<Block*>(value))->data();
          block_len = (static_cast<Block*>(value))->size();
          fprintf(stdout, "kFilterMetaBlock");
          break;
        case CacheEntryRole::kIndexBlock:
          type = CacheDumpBlockType::kIndex;
          block_start = (static_cast<Block*>(value))->data();
          block_len = (static_cast<Block*>(value))->size();
          fprintf(stdout, "kIndexBlock");
          break;
        case CacheEntryRole::kMisc:
          fprintf(stdout, "kMisc");
          filter_out = true;
          break;
        case CacheEntryRole::kOtherBlock:
          fprintf(stdout, "kOtherBlock");
          filter_out = true;
          break;
        case CacheEntryRole::kWriteBuffer:
          fprintf(stdout, "kWriteBuffer");
          filter_out = true;
          break;
        default:
          fprintf(stdout, "out");
          filter_out = true;
      }
      if (!filter_out && block_start != nullptr) {
        char buffer[block_len];
        memcpy(buffer, block_start, block_len);
        WriteCacheBlock(type, key, (void*)buffer, block_len);
      }
    };
  }

  IOStatus Run() {
    assert(cache_ != nullptr);
    IOStatus io_s = WriteHeader();
    if (!io_s.ok()) {
      return io_s;
    }

    cache_->ApplyToAllEntries(DumpOneBlockCallBack(), {});

    io_s = WriteFooter();
    return io_s;
  }

 private:
  IOStatus WriteRawBlock(uint64_t timestamp, CacheDumpBlockType type,
                         const Slice& key, void* value, size_t len,
                         uint32_t checksum) {
    DataContent data_content;
    data_content.timestamp = timestamp;
    data_content.key = key;
    data_content.type = type;
    data_content.value_len = len;
    data_content.value = value;
    data_content.value_checksum = checksum;
    std::string encoded_data;
    CacheDumperHelper::EncodeBlock(data_content, &encoded_data);

    assert(file_writer_ != nullptr);
    return file_writer_->Append(Slice(encoded_data));
  }

  IOStatus WriteHeader() {
    std::string header_key = "header_key:" + db_id_;
    std::ostringstream s;
    s << kTraceMagic << "\t"
      << "Cache dump format version: " << 0 << "." << 1 << "\t"
      << "RocksDB Version: " << kMajorVersion << "." << kMinorVersion << "\t"
      << "Format: timestamp type cache_key cache_value_crc32c_checksum "
         "cache_value\n";
    std::string header_value(s.str());
    CacheDumpBlockType type = CacheDumpBlockType::kHeader;
    uint64_t timestamp = clock_->NowMicros();
    uint32_t header_checksum =
        crc32c::Value(header_value.c_str(), header_value.size());
    return WriteRawBlock(timestamp, type, Slice(header_key),
                         (void*)header_value.c_str(), header_value.size(),
                         header_checksum);
  }

  IOStatus WriteCacheBlock(const CacheDumpBlockType type, const Slice& key,
                           void* value, size_t len) {
    uint64_t timestamp = clock_->NowMicros();
    uint32_t value_checksum = crc32c::Value((char*)value, len);
    return WriteRawBlock(timestamp, type, key, value, len, value_checksum);
  }

  IOStatus WriteFooter() {
    std::string footer_key = "footer_key:" + db_id_;
    std::ostringstream s;
    std::string footer_value("cache dump completed");
    CacheDumpBlockType type = CacheDumpBlockType::kFooter;
    uint64_t timestamp = clock_->NowMicros();
    uint32_t footer_checksum =
        crc32c::Value(footer_value.c_str(), footer_value.size());
    return WriteRawBlock(timestamp, type, Slice(footer_key),
                         (void*)footer_value.c_str(), footer_value.size(),
                         footer_checksum);
  }

  Env* env_;
  CacheDumpOptions options_;
  std::shared_ptr<Cache> cache_;
  std::string db_id_;
  SystemClock* clock_;
  std::unique_ptr<WritableFileWriter> file_writer_;
  std::unordered_map<Cache::DeleterFn, CacheEntryRole> role_map_;
  std::set<std::string> prefix_filter_;
  size_t prefix_size_;
};

class CacheDumpedLoader {
 public:
  CacheDumpedLoader(Env* env, const BlockBasedTableOptions& toptions,
                    const CacheDumpOptions& dump_options,
                    const std::shared_ptr<Cache> cache,
                    const std::string& db_id)
      : env_(env),
        toptions_(toptions),
        options_(dump_options),
        cache_(cache),
        db_id_(db_id) {}
  ~CacheDumpedLoader() {
    Close().PermitUncheckedError();
    delete[] buffer_;
  }

  Status PrepareFileReader() {
    if (env_ == nullptr) {
      return IOStatus::IOError("Env is null");
    }
    std::unique_ptr<RandomAccessFileReader> file_reader;
    Status s = RandomAccessFileReader::Create(
        env_->GetFileSystem(), options_.dump_file_path, FileOptions(),
        &file_reader, nullptr);
    if (!s.ok()) {
      return s;
    }
    file_reader_.reset(file_reader.release());

    reader_offset_ = 0;
    buffer_ = new char[kBufferSize];
    return Status::OK();
  }

  IOStatus Run() {
    IOStatus io_s;
    DataContent data_content;
    std::string data;
    io_s = ReadHeader(&data, &data_content);
    if (!io_s.ok()) {
      return io_s;
    }
    // TODO, may need some verification here like db_id_ and others
    io_s = VerifyDB(data_content.key.ToString());
    if (!io_s.ok()) {
      return io_s;
    }
    while (io_s.ok() && data_content.type != CacheDumpBlockType::kFooter) {
      data_content.reset();
      data.clear();
      io_s = ReadCacheBlock(&data, &data_content);
      if (!io_s.ok()) {
        break;
      }
      MemoryAllocator* memory_allocator = GetMemoryAllocator(toptions_);
      CacheAllocationPtr heap_buf =
          AllocateBlock(data_content.value_len, memory_allocator);
      memcpy(heap_buf.get(), data_content.value, data_content.value_len);
      BlockContents* raw_block_contents = nullptr;
      *raw_block_contents =
          BlockContents(std::move(heap_buf), data_content.value_len);
      // TODO: do we need the statistic for the dumped cache load?
      Statistics* statistics = nullptr;

      if (data_content.type == CacheDumpBlockType::kDeprecatedFilterBlock) {
        std::unique_ptr<BlockContents> block_holder;
        block_holder.reset(BlocklikeTraits<BlockContents>::Create(
            std::move(*raw_block_contents), 0, statistics, false,
            toptions_.filter_policy.get()));
        Cache::Handle* cache_handle = nullptr;
        cache_->Insert(data_content.key, block_holder.get(),
                       BlocklikeTraits<BlockContents>::GetCacheItemHelper(
                           BlockType::kFilter),
                       data_content.value_len, &cache_handle,
                       Cache::Priority::LOW);
        fprintf(stdout, "Loaded filter BlockContent kFilter\n");
      } else if (data_content.type == CacheDumpBlockType::kFilter) {
        std::unique_ptr<ParsedFullFilterBlock> block_holder;
        block_holder.reset(BlocklikeTraits<ParsedFullFilterBlock>::Create(
            std::move(*raw_block_contents), toptions_.read_amp_bytes_per_bit,
            statistics, false, toptions_.filter_policy.get()));
        Cache::Handle* cache_handle = nullptr;
        cache_->Insert(
            data_content.key, block_holder.get(),
            BlocklikeTraits<ParsedFullFilterBlock>::GetCacheItemHelper(
                BlockType::kFilter),
            data_content.value_len, &cache_handle, Cache::Priority::LOW);
        fprintf(stdout, "Loaded filter ParsedFullFilterBlock kFilter\n");
      } else if (data_content.type == CacheDumpBlockType::kData) {
        std::unique_ptr<Block> block_holder;
        block_holder.reset(BlocklikeTraits<Block>::Create(
            std::move(*raw_block_contents), toptions_.read_amp_bytes_per_bit,
            statistics, false, toptions_.filter_policy.get()));
        Cache::Handle* cache_handle = nullptr;
        cache_->Insert(
            data_content.key, block_holder.get(),
            BlocklikeTraits<Block>::GetCacheItemHelper(BlockType::kData),
            data_content.value_len, &cache_handle, Cache::Priority::LOW);
        fprintf(stdout, "Loaded Block kData\n");
      } else if (data_content.type == CacheDumpBlockType::kIndex) {
        std::unique_ptr<Block> block_holder;
        block_holder.reset(BlocklikeTraits<Block>::Create(
            std::move(*raw_block_contents), 0, statistics, false,
            toptions_.filter_policy.get()));
        Cache::Handle* cache_handle = nullptr;
        cache_->Insert(
            data_content.key, block_holder.get(),
            BlocklikeTraits<Block>::GetCacheItemHelper(BlockType::kIndex),
            data_content.value_len, &cache_handle, Cache::Priority::LOW);
        fprintf(stdout, "Loaded Block kIndex\n");
      } else if (data_content.type == CacheDumpBlockType::kFilterMetaBlock) {
        std::unique_ptr<Block> block_holder;
        block_holder.reset(BlocklikeTraits<Block>::Create(
            std::move(*raw_block_contents), toptions_.read_amp_bytes_per_bit,
            statistics, false, toptions_.filter_policy.get()));
        Cache::Handle* cache_handle = nullptr;
        cache_->Insert(
            data_content.key, block_holder.get(),
            BlocklikeTraits<Block>::GetCacheItemHelper(BlockType::kFilter),
            data_content.value_len, &cache_handle, Cache::Priority::LOW);
        fprintf(stdout, "Loaded Block kFilter\n");
      } else if (data_content.type == CacheDumpBlockType::kFooter) {
        fprintf(stdout, "Get to Footer\n");
        break;
      } else {
        // might be other types not supported, skip it
        fprintf(stdout, "Not Supported Type\n");
        continue;
      }
    }
    if (data_content.type == CacheDumpBlockType::kFooter) {
      return IOStatus::OK();
    } else {
      return io_s;
    }
  }

 private:
  IOStatus ReadRawBlock(std::string* data) {
    assert(file_reader_ != nullptr);

    // First, read out the size of the data of this block (including key, value
    // and others. The size is uint32_t, 4 bytes)
    IOStatus io_s = file_reader_->Read(IOOptions(), reader_offset_, 4,
                                       &reader_result_, buffer_, nullptr);
    if (!io_s.ok()) {
      return io_s;
    }
    if (reader_result_.size() == 0) {
      return IOStatus::IOError("Read 4 bytes failed");
    }
    if (reader_result_.size() < 4) {
      return IOStatus::Corruption("Corrupted cache dump file");
    }
    uint32_t data_len = DecodeFixed32(buffer_);
    reader_offset_ += 4;

    // Read the block data
    unsigned int bytes_to_read = data_len;
    unsigned int to_read =
        bytes_to_read > kBufferSize ? kBufferSize : bytes_to_read;
    while (to_read > 0) {
      io_s = file_reader_->Read(IOOptions(), reader_offset_, to_read,
                                &reader_result_, buffer_, nullptr);
      if (!io_s.ok()) {
        return io_s;
      }
      if (reader_result_.size() < to_read) {
        return IOStatus::Corruption("Corrupted cache dump file.");
      }
      data->append(reader_result_.data(), reader_result_.size());
      reader_offset_ += to_read;
      bytes_to_read -= to_read;
      to_read = bytes_to_read > kBufferSize ? kBufferSize : bytes_to_read;
    }
    return IOStatus::OK();
  }

  IOStatus ReadHeader(std::string* data, DataContent* data_content) {
    IOStatus io_s = ReadRawBlock(data);
    if (!io_s.ok()) {
      return io_s;
    }
    CacheDumperHelper::DecodeBlock((*data), data_content);
    uint32_t value_checksum =
        crc32c::Value((char*)data_content->value, data_content->value_len);
    if (value_checksum != data_content->value_checksum) {
      fprintf(stdout, "loaded header corrupted\n");
      return IOStatus::Corruption("reloaded header corrupted!");
    }
    return IOStatus::OK();
  }

  IOStatus ReadCacheBlock(std::string* data, DataContent* data_content) {
    IOStatus io_s = ReadRawBlock(data);
    if (!io_s.ok()) {
      return io_s;
    }
    CacheDumperHelper::DecodeBlock((*data), data_content);
    uint32_t value_checksum =
        crc32c::Value((char*)data_content->value, data_content->value_len);
    if (value_checksum != data_content->value_checksum) {
      fprintf(stdout, "loaded block corrupted\n");
      return IOStatus::Corruption("reloaded block corrupted!");
    }
    return IOStatus::OK();
  }

  IOStatus VerifyDB(const std::string& db_id) {
    if (db_id_ == db_id) {
      return IOStatus::OK();
    } else {
      return IOStatus::OK();
    }
  }

  Status Close() {
    file_reader_.reset();
    return Status::OK();
  }

  Env* env_;
  const BlockBasedTableOptions& toptions_;
  CacheDumpOptions options_;
  std::shared_ptr<Cache> cache_;
  std::string db_id_;
  std::unique_ptr<RandomAccessFileReader> file_reader_;
  Slice reader_result_;
  size_t reader_offset_;
  char* buffer_;

  static const unsigned int kBufferSize = 1024;
};

}  // namespace ROCKSDB_NAMESPACE
