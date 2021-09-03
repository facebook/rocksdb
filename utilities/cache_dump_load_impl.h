//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <unordered_map>

#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/utilities/cache_dump_load.h"
#include "table/block_based/block.h"
#include "table/block_based/block_like_traits.h"
#include "table/block_based/block_type.h"
#include "table/block_based/cachable_entry.h"
#include "table/block_based/parsed_full_filter_block.h"
#include "table/block_based/reader_common.h"

namespace ROCKSDB_NAMESPACE {

const unsigned int kDumpReaderBufferSize = 1024;  // 1KB
const unsigned int kDumpUnitMetaSize = 16;

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
  kDeprecatedFilterBlock = 12,
  kFilterMetaBlock = 13,
  kBlockTypeMax,
};

struct DumpUnitMeta {
  uint32_t sequence_num;
  uint32_t dump_unit_checksum;
  uint64_t dump_unit_size;
};

struct DumpUnit {
  uint64_t timestamp;
  CacheDumpUnitType type;
  Slice key;
  size_t value_len;
  uint32_t value_checksum;
  void* value;

  void reset() {
    timestamp = 0;
    type = CacheDumpUnitType::kBlockTypeMax;
    key.clear();
    value_len = 0;
    value_checksum = 0;
    value = nullptr;
  }
};

class CacheDumperImpl : public CacheDumper {
 public:
  CacheDumperImpl(const CacheDumpOptions& dump_options,
                  const std::shared_ptr<Cache>& cache,
                  std::unique_ptr<CacheDumpWriter>&& writer)
      : env_(dump_options.env),
        options_(dump_options),
        cache_(cache),
        writer_(std::move(writer)) {}
  ~CacheDumperImpl() { writer_.reset(); }
  Status Prepare() override;
  IOStatus Run() override;

 private:
  IOStatus WriteRawBlock(uint64_t timestamp, CacheDumpUnitType type,
                         const Slice& key, void* value, size_t len,
                         uint32_t checksum);

  IOStatus WriteHeader();

  IOStatus WriteCacheBlock(const CacheDumpUnitType type, const Slice& key,
                           void* value, size_t len);

  IOStatus WriteFooter();
  std::function<void(const Slice&, void*, size_t, Cache::DeleterFn)>
  DumpOneBlockCallBack();

  Env* env_;
  CacheDumpOptions options_;
  std::shared_ptr<Cache> cache_;
  std::unique_ptr<CacheDumpWriter> writer_;
  std::unordered_map<Cache::DeleterFn, CacheEntryRole> role_map_;
  SystemClock* clock_;
  uint32_t sequence_num_;
};

class CacheDumpedLoaderImpl : public CacheDumpedLoader {
 public:
  CacheDumpedLoaderImpl(const CacheDumpOptions& dump_options,
                        const BlockBasedTableOptions& toptions,
                        const std::shared_ptr<SecondaryCache>& secondary_cache,
                        std::unique_ptr<CacheDumpReader>&& reader)
      : env_(dump_options.env),
        options_(dump_options),
        toptions_(toptions),
        secondary_cache_(secondary_cache),
        reader_(std::move(reader)) {}
  ~CacheDumpedLoaderImpl() {}
  Status Prepare() override;
  IOStatus Run() override;

 private:
  IOStatus ReadDumpUnitMeta(std::string* data, DumpUnitMeta* unit_meta);
  IOStatus ReadDumpUnit(size_t len, std::string* data, DumpUnit* unit);
  IOStatus ReadHeader(std::string* data, DumpUnit* dump_unit);
  IOStatus ReadCacheBlock(std::string* data, DumpUnit* dump_unit);

  Env* env_;
  CacheDumpOptions options_;
  const BlockBasedTableOptions& toptions_;
  std::shared_ptr<SecondaryCache> secondary_cache_;
  std::unique_ptr<CacheDumpReader> reader_;
  std::unordered_map<Cache::DeleterFn, CacheEntryRole> role_map_;
};

class DefaultCacheDumpWriter : public CacheDumpWriter {
 public:
  explicit DefaultCacheDumpWriter(
      std::unique_ptr<WritableFileWriter>&& file_writer)
      : file_writer_(std::move(file_writer)) {}

  ~DefaultCacheDumpWriter() { Close().PermitUncheckedError(); }

  virtual IOStatus Write(const Slice& data) override {
    assert(file_writer_ != nullptr);
    return file_writer_->Append(data);
  }

  virtual IOStatus Close() override {
    file_writer_.reset();
    return IOStatus::OK();
  }

  virtual uint64_t GetFileSize() override {
    assert(file_writer_ != nullptr);
    return file_writer_->GetFileSize();
  }

 private:
  std::unique_ptr<WritableFileWriter> file_writer_;
};

class DefaultCacheDumpReader : public CacheDumpReader {
 public:
  explicit DefaultCacheDumpReader(
      std::unique_ptr<RandomAccessFileReader>&& reader)
      : file_reader_(std::move(reader)),
        offset_(0),
        buffer_(new char[kDumpReaderBufferSize]) {}

  ~DefaultCacheDumpReader() {
    Close().PermitUncheckedError();
    delete[] buffer_;
  }

  virtual IOStatus Read(size_t len, std::string* data) override {
    assert(file_reader_ != nullptr);
    IOStatus io_s;

    unsigned int bytes_to_read = static_cast<unsigned int>(len);
    unsigned int to_read = bytes_to_read > kDumpReaderBufferSize
                               ? kDumpReaderBufferSize
                               : bytes_to_read;

    while (to_read > 0) {
      io_s = file_reader_->Read(IOOptions(), offset_, to_read, &result_,
                                buffer_, nullptr);
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

  virtual size_t GetOffset() const override { return offset_; }

  virtual IOStatus Close() override {
    file_reader_.reset();
    return IOStatus::OK();
  }

 private:
  std::unique_ptr<RandomAccessFileReader> file_reader_;
  Slice result_;
  size_t offset_;
  char* buffer_;
};

class CacheDumperHelper {
 public:
  static void EncodeDumpUnitMeta(const DumpUnitMeta& meta, std::string* data) {
    assert(data);
    PutFixed32(data, static_cast<uint32_t>(meta.sequence_num));
    PutFixed32(data, static_cast<uint32_t>(meta.dump_unit_checksum));
    PutFixed64(data, meta.dump_unit_size);
  }

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
