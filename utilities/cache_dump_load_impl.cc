//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/cache_dump_load_impl.h"

#include "cache/cache_entry_roles.h"
#include "file/writable_file_writer.h"
#include "port/lang.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "table/format.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

Status CacheDumperImpl::Prepare() {
  if (env_ == nullptr) {
    return Status::InvalidArgument("Env is null");
  }
  if (cache_ == nullptr) {
    return Status::InvalidArgument("Cache is null");
  }
  if (writer_ == nullptr) {
    return Status::InvalidArgument("CacheDumpWriter is null");
  }
  // We copy the Cache Deleter Role Map as its member.
  role_map_ = CopyCacheDeleterRoleMap();
  // Set the system clock
  clock_ = env_->GetSystemClock().get();
  // Set the sequence number
  sequence_num_ = 0;
  return Status::OK();
}

IOStatus CacheDumperImpl::Run() {
  assert(cache_ != nullptr);
  IOStatus io_s = WriteHeader();
  if (!io_s.ok()) {
    return io_s;
  }

  cache_->ApplyToAllEntries(DumpOneBlockCallBack(), {});

  io_s = WriteFooter();
  writer_->Close();
  return io_s;
}

std::function<void(const Slice&, void*, size_t, Cache::DeleterFn)>
CacheDumperImpl::DumpOneBlockCallBack() {
  return [&](const Slice& key, void* value, size_t /*charge*/,
             Cache::DeleterFn deleter) {
    auto e = role_map_.find(deleter);
    CacheEntryRole role;
    CacheDumpUnitType type;
    if (e == role_map_.end()) {
      role = CacheEntryRole::kMisc;
    } else {
      role = e->second;
    }
    bool filter_out = false;

    if (options_.prefix_set.size() != 0) {
      size_t prefix_size = options_.prefix_set.begin()->size();
      Slice key_prefix(key.data(), prefix_size);
      std::string prefix = key_prefix.ToString();
      if (options_.prefix_set.find(prefix) == options_.prefix_set.end()) {
        filter_out = true;
      }
    }

    const char* block_start = nullptr;
    size_t block_len = 0;
    switch (role) {
      case CacheEntryRole::kDataBlock:
        type = CacheDumpUnitType::kData;
        block_start = (static_cast<Block*>(value))->data();
        block_len = (static_cast<Block*>(value))->size();
        break;
      case CacheEntryRole::kDeprecatedFilterBlock:
        type = CacheDumpUnitType::kDeprecatedFilterBlock;
        block_start = (static_cast<BlockContents*>(value))->data.data();
        block_len = (static_cast<BlockContents*>(value))->data.size();
        break;
      case CacheEntryRole::kFilterBlock:
        type = CacheDumpUnitType::kFilter;
        block_start = (static_cast<ParsedFullFilterBlock*>(value))
                          ->GetBlockContentsData()
                          .data();
        block_len = (static_cast<ParsedFullFilterBlock*>(value))
                        ->GetBlockContentsData()
                        .size();
        break;
      case CacheEntryRole::kFilterMetaBlock:
        type = CacheDumpUnitType::kFilterMetaBlock;
        block_start = (static_cast<Block*>(value))->data();
        block_len = (static_cast<Block*>(value))->size();
        break;
      case CacheEntryRole::kIndexBlock:
        type = CacheDumpUnitType::kIndex;
        block_start = (static_cast<Block*>(value))->data();
        block_len = (static_cast<Block*>(value))->size();
        break;
      case CacheEntryRole::kMisc:
        filter_out = true;
        break;
      case CacheEntryRole::kOtherBlock:
        filter_out = true;
        break;
      case CacheEntryRole::kWriteBuffer:
        filter_out = true;
        break;
      default:
        filter_out = true;
    }
    if (!filter_out && block_start != nullptr) {
      char buffer[block_len];
      memcpy(buffer, block_start, block_len);
      WriteCacheBlock(type, key, (void*)buffer, block_len);
    }
  };
}

IOStatus CacheDumperImpl::WriteRawBlock(uint64_t timestamp,
                                        CacheDumpUnitType type,
                                        const Slice& key, void* value,
                                        size_t len, uint32_t checksum) {
  DumpUnit dump_unit;
  dump_unit.timestamp = timestamp;
  dump_unit.key = key;
  dump_unit.type = type;
  dump_unit.value_len = len;
  dump_unit.value = value;
  dump_unit.value_checksum = checksum;
  std::string encoded_data;
  CacheDumperHelper::EncodeDumpUnit(dump_unit, &encoded_data);

  DumpUnitMeta unit_meta;
  unit_meta.sequence_num = sequence_num_;
  sequence_num_++;
  unit_meta.dump_unit_checksum =
      crc32c::Value(encoded_data.c_str(), encoded_data.size());
  unit_meta.dump_unit_size = static_cast<uint64_t>(encoded_data.size());
  std::string encoded_meta;
  CacheDumperHelper::EncodeDumpUnitMeta(unit_meta, &encoded_meta);

  assert(writer_ != nullptr);
  IOStatus io_s = writer_->Write(Slice(encoded_meta));
  if (!io_s.ok()) {
    return io_s;
  }
  return writer_->Write(Slice(encoded_data));
}

IOStatus CacheDumperImpl::WriteHeader() {
  std::string header_key = "header";
  std::ostringstream s;
  s << kTraceMagic << "\t"
    << "Cache dump format version: " << 0 << "." << 1 << "\t"
    << "RocksDB Version: " << kMajorVersion << "." << kMinorVersion << "\t"
    << "Format: timestamp type cache_key cache_value_crc32c_checksum "
       "cache_value\n";
  std::string header_value(s.str());
  CacheDumpUnitType type = CacheDumpUnitType::kHeader;
  uint64_t timestamp = clock_->NowMicros();
  uint32_t header_checksum =
      crc32c::Value(header_value.c_str(), header_value.size());
  return WriteRawBlock(timestamp, type, Slice(header_key),
                       (void*)header_value.c_str(), header_value.size(),
                       header_checksum);
}

IOStatus CacheDumperImpl::WriteCacheBlock(const CacheDumpUnitType type,
                                          const Slice& key, void* value,
                                          size_t len) {
  uint64_t timestamp = clock_->NowMicros();
  uint32_t value_checksum = crc32c::Value((char*)value, len);
  return WriteRawBlock(timestamp, type, key, value, len, value_checksum);
}

IOStatus CacheDumperImpl::WriteFooter() {
  std::string footer_key = "footer";
  std::ostringstream s;
  std::string footer_value("cache dump completed");
  CacheDumpUnitType type = CacheDumpUnitType::kFooter;
  uint64_t timestamp = clock_->NowMicros();
  uint32_t footer_checksum =
      crc32c::Value(footer_value.c_str(), footer_value.size());
  return WriteRawBlock(timestamp, type, Slice(footer_key),
                       (void*)footer_value.c_str(), footer_value.size(),
                       footer_checksum);
}

Status CacheDumpedLoaderImpl::Prepare() {
  if (env_ == nullptr) {
    return Status::InvalidArgument("Env is null");
  }
  if (secondary_cache_ == nullptr) {
    return Status::InvalidArgument("Secondary Cache is null");
  }
  if (reader_ == nullptr) {
    return Status::InvalidArgument("CacheDumpReader is null");
  }
  // Then, we copy the Cache Deleter Role Map as its member.
  role_map_ = CopyCacheDeleterRoleMap();
  return Status::OK();
}

IOStatus CacheDumpedLoaderImpl::Run() {
  assert(secondary_cache_ != nullptr);
  IOStatus io_s;
  DumpUnit dump_unit;
  std::string data;
  io_s = ReadHeader(&data, &dump_unit);
  if (!io_s.ok()) {
    return io_s;
  }

  while (io_s.ok() && dump_unit.type != CacheDumpUnitType::kFooter) {
    dump_unit.reset();
    data.clear();
    io_s = ReadCacheBlock(&data, &dump_unit);
    if (!io_s.ok()) {
      break;
    }
    BlockContents raw_block_contents(
        Slice((char*)dump_unit.value, dump_unit.value_len));
    Cache::CacheItemHelper* helper = nullptr;
    Statistics* statistics = nullptr;
    void* block = nullptr;
    switch (dump_unit.type) {
      case CacheDumpUnitType::kDeprecatedFilterBlock: {
        helper = BlocklikeTraits<BlockContents>::GetCacheItemHelper(
            BlockType::kFilter);
        std::unique_ptr<BlockContents> block_holder;
        block_holder.reset(BlocklikeTraits<BlockContents>::Create(
            std::move(raw_block_contents), 0, statistics, false,
            toptions_.filter_policy.get()));
        block = (void*)block_holder.release();
        break;
      }
      case CacheDumpUnitType::kFilter: {
        helper = BlocklikeTraits<ParsedFullFilterBlock>::GetCacheItemHelper(
            BlockType::kFilter);
        std::unique_ptr<ParsedFullFilterBlock> block_holder;
        block_holder.reset(BlocklikeTraits<ParsedFullFilterBlock>::Create(
            std::move(raw_block_contents), toptions_.read_amp_bytes_per_bit,
            statistics, false, toptions_.filter_policy.get()));
        block = (void*)block_holder.release();
        break;
      }
      case CacheDumpUnitType::kData: {
        helper = BlocklikeTraits<Block>::GetCacheItemHelper(BlockType::kData);
        std::unique_ptr<Block> block_holder;
        block_holder.reset(BlocklikeTraits<Block>::Create(
            std::move(raw_block_contents), toptions_.read_amp_bytes_per_bit,
            statistics, false, toptions_.filter_policy.get()));
        block = (void*)block_holder.release();
        break;
      }
      case CacheDumpUnitType::kIndex: {
        helper = BlocklikeTraits<Block>::GetCacheItemHelper(BlockType::kIndex);
        std::unique_ptr<Block> block_holder;
        block_holder.reset(BlocklikeTraits<Block>::Create(
            std::move(raw_block_contents), 0, statistics, false,
            toptions_.filter_policy.get()));
        block = (void*)block_holder.release();
        break;
      }
      case CacheDumpUnitType::kFilterMetaBlock: {
        helper = BlocklikeTraits<Block>::GetCacheItemHelper(BlockType::kFilter);
        std::unique_ptr<Block> block_holder;
        block_holder.reset(BlocklikeTraits<Block>::Create(
            std::move(raw_block_contents), toptions_.read_amp_bytes_per_bit,
            statistics, false, toptions_.filter_policy.get()));
        block = (void*)block_holder.release();
        break;
      }
      case CacheDumpUnitType::kFooter:
        break;
      default:
        continue;
    }
    if (helper != nullptr) {
      secondary_cache_->Insert(dump_unit.key, block, helper);
    }
  }
  if (dump_unit.type == CacheDumpUnitType::kFooter) {
    return IOStatus::OK();
  } else {
    return io_s;
  }
}

IOStatus CacheDumpedLoaderImpl::ReadDumpUnitMeta(std::string* data,
                                                 DumpUnitMeta* unit_meta) {
  assert(reader_ != nullptr);
  assert(data != nullptr);
  assert(unit_meta != nullptr);
  IOStatus io_s = reader_->Read(kDumpUnitMetaSize, data);
  if (!io_s.ok()) {
    return io_s;
  }
  return status_to_io_status(
      CacheDumperHelper::DecodeDumpUnitMeta(*data, unit_meta));
}

IOStatus CacheDumpedLoaderImpl::ReadDumpUnit(size_t len, std::string* data,
                                             DumpUnit* unit) {
  assert(reader_ != nullptr);
  assert(data != nullptr);
  assert(unit != nullptr);
  IOStatus io_s = reader_->Read(len, data);
  if (!io_s.ok()) {
    return io_s;
  }
  Slice block;
  return status_to_io_status(CacheDumperHelper::DecodeDumpUnit(*data, unit));
}

IOStatus CacheDumpedLoaderImpl::ReadHeader(std::string* data,
                                           DumpUnit* dump_unit) {
  DumpUnitMeta header_meta;
  std::string meta_string;
  IOStatus io_s = ReadDumpUnitMeta(&meta_string, &header_meta);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = ReadDumpUnit(header_meta.dump_unit_size, data, dump_unit);
  if (!io_s.ok()) {
    return io_s;
  }
  uint32_t unit_checksum = crc32c::Value(data->c_str(), data->size());
  if (unit_checksum != header_meta.dump_unit_checksum) {
    return IOStatus::Corruption("Read header unit corrupted!");
  }
  return io_s;
}

IOStatus CacheDumpedLoaderImpl::ReadCacheBlock(std::string* data,
                                               DumpUnit* dump_unit) {
  DumpUnitMeta unit_meta;
  std::string unit_string;
  IOStatus io_s = ReadDumpUnitMeta(&unit_string, &unit_meta);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = ReadDumpUnit(unit_meta.dump_unit_size, data, dump_unit);
  if (!io_s.ok()) {
    return io_s;
  }
  uint32_t unit_checksum = crc32c::Value(data->c_str(), data->size());
  if (unit_checksum != unit_meta.dump_unit_checksum) {
    return IOStatus::Corruption(
        "Checksum does not match! Read dumped unit corrupted!");
  }
  return io_s;
}

}  // namespace ROCKSDB_NAMESPACE
