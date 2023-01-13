//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <unordered_map>

#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/utilities/cache_dump_load.h"
#include "table/block_based/block.h"
#include "table/block_based/block_type.h"
#include "table/block_based/cachable_entry.h"
#include "table/block_based/parsed_full_filter_block.h"
#include "table/block_based/reader_common.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

// the read buffer size of for the default CacheDumpReader
const unsigned int kDumpReaderBufferSize = 1024;  // 1KB
static const unsigned int kSizePrefixLen = 4;

enum CacheDumpUnitType : unsigned char {
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
  kDeprecatedFilterBlock = 12,  // OBSOLETE / DEPRECATED
  kFilterMetaBlock = 13,
  kBlockTypeMax,
};

// The metadata of a dump unit. After it is serilized, its size is fixed 16
// bytes.
struct DumpUnitMeta {
  // sequence number is a monotonically increasing number to indicate the order
  // of the blocks being written. Header is 0.
  uint32_t sequence_num;
  // The Crc32c checksum of its dump unit.
  uint32_t dump_unit_checksum;
  // The dump unit size after the dump unit is serilized to a string.
  uint64_t dump_unit_size;

  void reset() {
    sequence_num = 0;
    dump_unit_checksum = 0;
    dump_unit_size = 0;
  }
};

// The data structure to hold a block and its information.
struct DumpUnit {
  // The timestamp when the block is identified, copied, and dumped from block
  // cache
  uint64_t timestamp;
  // The type of the block
  CacheDumpUnitType type;
  // The key of this block when the block is referenced by this Cache
  Slice key;
  // The block size
  size_t value_len;
  // The Crc32c checksum of the block
  uint32_t value_checksum;
  // Pointer to the block. Note that, in the dump process, it points to a memory
  // buffer copied from cache block. The buffer is freed when we process the
  // next block. In the load process, we use an std::string to store the
  // serialized dump_unit read from the reader. So it points to the memory
  // address of the begin of the block in this string.
  void* value;

  DumpUnit() { reset(); }

  void reset() {
    timestamp = 0;
    type = CacheDumpUnitType::kBlockTypeMax;
    key.clear();
    value_len = 0;
    value_checksum = 0;
    value = nullptr;
  }
};

// The default implementation of the Cache Dumper
class CacheDumperImpl : public CacheDumper {
 public:
  CacheDumperImpl(const CacheDumpOptions& dump_options,
                  const std::shared_ptr<Cache>& cache,
                  std::unique_ptr<CacheDumpWriter>&& writer)
      : options_(dump_options), cache_(cache), writer_(std::move(writer)) {}
  ~CacheDumperImpl() { writer_.reset(); }
  Status SetDumpFilter(std::vector<DB*> db_list) override;
  IOStatus DumpCacheEntriesToWriter() override;

 private:
  IOStatus WriteBlock(CacheDumpUnitType type, const Slice& key,
                      const Slice& value);
  IOStatus WriteHeader();
  IOStatus WriteFooter();
  bool ShouldFilterOut(const Slice& key);
  std::function<void(const Slice&, Cache::ObjectPtr, size_t,
                     const Cache::CacheItemHelper*)>
  DumpOneBlockCallBack(std::string& buf);

  CacheDumpOptions options_;
  std::shared_ptr<Cache> cache_;
  std::unique_ptr<CacheDumpWriter> writer_;
  SystemClock* clock_;
  uint32_t sequence_num_;
  // The cache key prefix filter. Currently, we use db_session_id as the prefix,
  // so using std::set to store the prefixes as filter is enough. Further
  // improvement can be applied like BloomFilter or others to speedup the
  // filtering.
  std::set<std::string> prefix_filter_;
};

// The default implementation of CacheDumpedLoader
class CacheDumpedLoaderImpl : public CacheDumpedLoader {
 public:
  CacheDumpedLoaderImpl(const CacheDumpOptions& dump_options,
                        const BlockBasedTableOptions& /*toptions*/,
                        const std::shared_ptr<SecondaryCache>& secondary_cache,
                        std::unique_ptr<CacheDumpReader>&& reader)
      : options_(dump_options),
        secondary_cache_(secondary_cache),
        reader_(std::move(reader)) {}
  ~CacheDumpedLoaderImpl() {}
  IOStatus RestoreCacheEntriesToSecondaryCache() override;

 private:
  IOStatus ReadDumpUnitMeta(std::string* data, DumpUnitMeta* unit_meta);
  IOStatus ReadDumpUnit(size_t len, std::string* data, DumpUnit* unit);
  IOStatus ReadHeader(std::string* data, DumpUnit* dump_unit);
  IOStatus ReadCacheBlock(std::string* data, DumpUnit* dump_unit);

  CacheDumpOptions options_;
  std::shared_ptr<SecondaryCache> secondary_cache_;
  std::unique_ptr<CacheDumpReader> reader_;
};

// The default implementation of CacheDumpWriter. We write the blocks to a file
// sequentially.
class ToFileCacheDumpWriter : public CacheDumpWriter {
 public:
  explicit ToFileCacheDumpWriter(
      std::unique_ptr<WritableFileWriter>&& file_writer)
      : file_writer_(std::move(file_writer)) {}

  ~ToFileCacheDumpWriter() { Close().PermitUncheckedError(); }

  // Write the serialized metadata to the file
  virtual IOStatus WriteMetadata(const Slice& metadata) override {
    assert(file_writer_ != nullptr);
    std::string prefix;
    PutFixed32(&prefix, static_cast<uint32_t>(metadata.size()));
    IOStatus io_s = file_writer_->Append(Slice(prefix));
    if (!io_s.ok()) {
      return io_s;
    }
    io_s = file_writer_->Append(metadata);
    return io_s;
  }

  // Write the serialized data to the file
  virtual IOStatus WritePacket(const Slice& data) override {
    assert(file_writer_ != nullptr);
    std::string prefix;
    PutFixed32(&prefix, static_cast<uint32_t>(data.size()));
    IOStatus io_s = file_writer_->Append(Slice(prefix));
    if (!io_s.ok()) {
      return io_s;
    }
    io_s = file_writer_->Append(data);
    return io_s;
  }

  // Reset the writer
  virtual IOStatus Close() override {
    file_writer_.reset();
    return IOStatus::OK();
  }

 private:
  std::unique_ptr<WritableFileWriter> file_writer_;
};

// The default implementation of CacheDumpReader. It is implemented based on
// RandomAccessFileReader. Note that, we keep an internal variable to remember
// the current offset.
class FromFileCacheDumpReader : public CacheDumpReader {
 public:
  explicit FromFileCacheDumpReader(
      std::unique_ptr<RandomAccessFileReader>&& reader)
      : file_reader_(std::move(reader)),
        offset_(0),
        buffer_(new char[kDumpReaderBufferSize]) {}

  ~FromFileCacheDumpReader() { delete[] buffer_; }

  virtual IOStatus ReadMetadata(std::string* metadata) override {
    uint32_t metadata_len = 0;
    IOStatus io_s = ReadSizePrefix(&metadata_len);
    if (!io_s.ok()) {
      return io_s;
    }
    return Read(metadata_len, metadata);
  }

  virtual IOStatus ReadPacket(std::string* data) override {
    uint32_t data_len = 0;
    IOStatus io_s = ReadSizePrefix(&data_len);
    if (!io_s.ok()) {
      return io_s;
    }
    return Read(data_len, data);
  }

 private:
  IOStatus ReadSizePrefix(uint32_t* len) {
    std::string prefix;
    IOStatus io_s = Read(kSizePrefixLen, &prefix);
    if (!io_s.ok()) {
      return io_s;
    }
    Slice encoded_slice(prefix);
    if (!GetFixed32(&encoded_slice, len)) {
      return IOStatus::Corruption("Decode size prefix string failed");
    }
    return IOStatus::OK();
  }

  IOStatus Read(size_t len, std::string* data) {
    assert(file_reader_ != nullptr);
    IOStatus io_s;

    unsigned int bytes_to_read = static_cast<unsigned int>(len);
    unsigned int to_read = bytes_to_read > kDumpReaderBufferSize
                               ? kDumpReaderBufferSize
                               : bytes_to_read;

    while (to_read > 0) {
      io_s = file_reader_->Read(IOOptions(), offset_, to_read, &result_,
                                buffer_, nullptr,
                                Env::IO_TOTAL /* rate_limiter_priority */);
      if (!io_s.ok()) {
        return io_s;
      }
      if (result_.size() < to_read) {
        return IOStatus::Corruption("Corrupted cache dump file.");
      }
      data->append(result_.data(), result_.size());

      offset_ += to_read;
      bytes_to_read -= to_read;
      to_read = bytes_to_read > kDumpReaderBufferSize ? kDumpReaderBufferSize
                                                      : bytes_to_read;
    }
    return io_s;
  }
  std::unique_ptr<RandomAccessFileReader> file_reader_;
  Slice result_;
  size_t offset_;
  char* buffer_;
};

// The cache dump and load helper class
class CacheDumperHelper {
 public:
  // serialize the dump_unit_meta to a string, it is fixed 16 bytes size.
  static void EncodeDumpUnitMeta(const DumpUnitMeta& meta, std::string* data) {
    assert(data);
    PutFixed32(data, static_cast<uint32_t>(meta.sequence_num));
    PutFixed32(data, static_cast<uint32_t>(meta.dump_unit_checksum));
    PutFixed64(data, meta.dump_unit_size);
  }

  // Serialize the dump_unit to a string.
  static void EncodeDumpUnit(const DumpUnit& dump_unit, std::string* data) {
    assert(data);
    PutFixed64(data, dump_unit.timestamp);
    data->push_back(dump_unit.type);
    PutLengthPrefixedSlice(data, dump_unit.key);
    PutFixed32(data, static_cast<uint32_t>(dump_unit.value_len));
    PutFixed32(data, dump_unit.value_checksum);
    PutLengthPrefixedSlice(data,
                           Slice((char*)dump_unit.value, dump_unit.value_len));
  }

  // Deserialize the dump_unit_meta from a string
  static Status DecodeDumpUnitMeta(const std::string& encoded_data,
                                   DumpUnitMeta* unit_meta) {
    assert(unit_meta != nullptr);
    Slice encoded_slice = Slice(encoded_data);
    if (!GetFixed32(&encoded_slice, &(unit_meta->sequence_num))) {
      return Status::Incomplete("Decode dumped unit meta sequence_num failed");
    }
    if (!GetFixed32(&encoded_slice, &(unit_meta->dump_unit_checksum))) {
      return Status::Incomplete(
          "Decode dumped unit meta dump_unit_checksum failed");
    }
    if (!GetFixed64(&encoded_slice, &(unit_meta->dump_unit_size))) {
      return Status::Incomplete(
          "Decode dumped unit meta dump_unit_size failed");
    }
    return Status::OK();
  }

  // Deserialize the dump_unit from a string.
  static Status DecodeDumpUnit(const std::string& encoded_data,
                               DumpUnit* dump_unit) {
    assert(dump_unit != nullptr);
    Slice encoded_slice = Slice(encoded_data);

    // Decode timestamp
    if (!GetFixed64(&encoded_slice, &dump_unit->timestamp)) {
      return Status::Incomplete("Decode dumped unit string failed");
    }
    // Decode the block type
    dump_unit->type = static_cast<CacheDumpUnitType>(encoded_slice[0]);
    encoded_slice.remove_prefix(1);
    // Decode the key
    if (!GetLengthPrefixedSlice(&encoded_slice, &(dump_unit->key))) {
      return Status::Incomplete("Decode dumped unit string failed");
    }
    // Decode the value size
    uint32_t value_len;
    if (!GetFixed32(&encoded_slice, &value_len)) {
      return Status::Incomplete("Decode dumped unit string failed");
    }
    dump_unit->value_len = static_cast<size_t>(value_len);
    // Decode the value checksum
    if (!GetFixed32(&encoded_slice, &(dump_unit->value_checksum))) {
      return Status::Incomplete("Decode dumped unit string failed");
    }
    // Decode the block content and copy to the memory space whose pointer
    // will be managed by the cache finally.
    Slice block;
    if (!GetLengthPrefixedSlice(&encoded_slice, &block)) {
      return Status::Incomplete("Decode dumped unit string failed");
    }
    dump_unit->value = (void*)block.data();
    assert(block.size() == dump_unit->value_len);
    return Status::OK();
  }
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
