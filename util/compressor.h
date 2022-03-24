// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#pragma once

#include <functional>
#include <mutex>

#include "memory/memory_allocator_impl.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/customizable.h"
#include "rocksdb/utilities/object_registry.h"
#include "table/block_based/block_type.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
class CompressionInfo;
class UncompressionInfo;
class Compressor;
class MemoryAllocator;

struct CompressionOptions;

// A Compressor-specific processed dictionary.
// Compressor implementations should extend this class
// for Compression and Uncompression-specific dictionary data.
// For example, this class can be extended to hold the ZSTD digested
// compression-related dictionaries.
class ProcessedDict {
 public:
  virtual ~ProcessedDict() = default;
  // Returns a pointer to the processed dictionary data
  virtual void* Data() const { return nullptr; }
  // Returns the size of the  processed dictionary data
  virtual size_t Size() const { return 0; }
};

// Holds dictionary and related data, like ZSTD's digested compression
// dictionary.
class CompressionDict {
 private:
  // Raw dictionary
  std::string dict_;
  // Processed dictionary
  std::unique_ptr<ProcessedDict> processed_;

 public:
  explicit CompressionDict(const std::string& dict,
                           std::unique_ptr<ProcessedDict>&& processed = nullptr)
      : dict_(std::move(dict)), processed_(std::move(processed)) {}

  Slice GetRawDict() const { return dict_; }
  void* GetProcessedDict() const {
    if (processed_) {
      return processed_->Data();
    } else {
      return nullptr;
    }
  }

  static const CompressionDict& GetEmptyDict() {
    static CompressionDict empty_dict{};
    return empty_dict;
  }

  CompressionDict() = default;
  // Disable copy/move
  CompressionDict(const CompressionDict&) = delete;
  CompressionDict& operator=(const CompressionDict&) = delete;
  CompressionDict(CompressionDict&&) = delete;
  CompressionDict& operator=(CompressionDict&&) = delete;
};

// Holds dictionary and related data, like ZSTD's digested uncompression
// dictionary.
class UncompressionDict {
 private:
  // Block containing the data for the compression dictionary in case the
  // constructor that takes a string parameter is used.
  std::string dict_;

  // Block containing the data for the compression dictionary in case the
  // constructor that takes a Slice parameter is used and the passed in
  // CacheAllocationPtr is not nullptr.
  CacheAllocationPtr allocation_;

 public:
  // Slice pointing to the compression dictionary data. Can point to
  // dict_, allocation_, or some other memory location, depending on how
  // the object was constructed.
  Slice slice_;

 private:
  std::unique_ptr<ProcessedDict> processed_;

 public:
  explicit UncompressionDict(
      const std::string& dict,
      std::unique_ptr<ProcessedDict>&& processed = nullptr);
  UncompressionDict(const Slice& slice, CacheAllocationPtr&& allocation,
                    std::unique_ptr<ProcessedDict>&& processed = nullptr);
  UncompressionDict(UncompressionDict&& rhs) noexcept;
  UncompressionDict& operator=(UncompressionDict&& rhs);

  // The object is self-contained if the string constructor is used, or the
  // Slice constructor is invoked with a non-null allocation. Otherwise, it
  // is the caller's responsibility to ensure that the underlying storage
  // outlives this object.
  bool own_bytes() const { return !dict_.empty() || allocation_; }
  const Slice& GetRawDict() const { return slice_; }
  const void* GetProcessedDict() const {
    if (processed_) {
      return processed_->Data();
    } else {
      return nullptr;
    }
  }

  // For TypedCacheInterface
  const Slice& ContentSlice() const { return slice_; }
  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kOtherBlock;
  static constexpr BlockType kBlockType = BlockType::kCompressionDictionary;

  static const UncompressionDict& GetEmptyDict() {
    static UncompressionDict empty_dict{};
    return empty_dict;
  }
  size_t ApproximateMemoryUsage() const;

  UncompressionDict() = default;
  // Disable copy
  explicit UncompressionDict(const CompressionDict&) = delete;
  UncompressionDict& operator=(const CompressionDict&) = delete;
};

// Interface for each compression algorithm to implement.
class Compressor : public Customizable {
 public:
  virtual ~Compressor() = default;

  // Type required by Customizable
  static const char* Type() { return "Compressor"; }

  // Creates and configures a Compressor from the input options.
  // If an existing Compressor is found that matches the input, it
  // is returned.  Otherwise, a new one is created.
  static Status CreateFromString(const ConfigOptions& opts,
                                 const std::string& value,
                                 std::shared_ptr<Compressor>* compressor);

  // Returns the IDs of the Compressors supported by this installation.
  // Only Compressors that are available in this binary are returned.
  static std::vector<std::string> GetSupported();

  // Returns the IDs of the Compressors supported by this installation that
  // support compression dictionaries.
  // Only Compressors that are available in this binary are returned.
  static std::vector<std::string> GetDictSupported();

  // Get the numeric type associated with this compressor
  virtual CompressionType GetCompressionType() const = 0;

  // Whether the compressor is supported.
  // For example, a compressor can implement this method to verify its
  // dependencies or environment settings.
  virtual bool Supported() const { return true; }

  // Whether the compressor supports dictionary compression.
  virtual bool DictCompressionSupported() const { return false; }

  // Compress data.
  // @param info Pointer to CompressionInfo object (containing dictionary,
  // version, etc).
  // @param input Buffer containing data to compress.
  // @param output Compressed data.
  // Returns OK if compression completed correctly.
  // Returns other status in case of error (e.g., Corruption).
  virtual Status Compress(const CompressionInfo& info, const Slice& input,
                          std::string* output) = 0;

  // Uncompress data.
  // @param info Pointer to UnompressionInfo object (containing dictionary,
  // version, etc).
  // @param input Buffer containing data to uncompress.
  // @param input_length Length of the input data.
  // @param output Buffer containing uncompressed data.
  // @param output_length Length of the output data.
  // Returns OK if uncompression completed correctly.
  // Returns other status in case of error (e.g., Corruption).
  virtual Status Uncompress(const UncompressionInfo& info, const char* input,
                            size_t input_length, char** output,
                            size_t* output_length) = 0;

  // Create a dictionary for compression using buffered data blocks.
  // @param data_block_buffers Buffered data blocks
  // @dict Pointer to the generated dictionary
  // Returns OK if the dictionary was generated correctly.
  // Returns other status in case of error.
  virtual Status CreateDict(std::vector<std::string>& data_block_buffers,
                            std::unique_ptr<CompressionDict>* dict);

  // Returns a new compression dictionary from the input dict.
  // Classes which have a ProcessedDict should override this method.
  virtual std::unique_ptr<CompressionDict> NewCompressionDict(
      const std::string& dict) {
    return std::make_unique<CompressionDict>(dict);
  }

  // Returns a new uncompression dictionary from the input dict.
  // Classes which have a ProcessedDict should override this method.
  virtual UncompressionDict* NewUncompressionDict(const std::string& dict) {
    return new UncompressionDict(dict);
  }

  // Returns a new uncompression dictionary from the input.
  // Classes which have a ProcessedDict should override this method.
  virtual UncompressionDict* NewUncompressionDict(
      const Slice& slice, CacheAllocationPtr&& allocation) {
    return new UncompressionDict(slice, std::move(allocation));
  }

  // Whether dictionary compression is enabled for this compressor.
  // If the compressor does not support dictionary compression
  // (DictCompressionSupported returns false), then this method must always
  // return false.
  virtual bool IsDictEnabled() const;

  // Equivalent of max_dict_buffer_bytes in CompressionOptions.
  // As options are set for each compressor, this function returns the value for
  // that option.
  virtual uint64_t GetMaxDictBufferBytes() const { return 0; }

  // Equivalent of parallel_threads in CompressionOptions.
  // As options are set for each compressor, this function returns the value for
  // that option.
  virtual uint32_t GetParallelThreads() const { return 1; }

  // Equivalent of max_compressed_bytes_per_kb  in CompressionOptions.
  // As options are set for each compressor, this function returns the value for
  // that option.
  virtual int GetMaxCompressedBytesPerKb() const { return 1024 * 7 / 8; }

 protected:
  static std::mutex mutex_;
  static std::unordered_map<std::string, std::vector<std::weak_ptr<Compressor>>>
      compressors_;

  // Sample data blocks to create a dictionary.
  // @param data_block_buffers Buffered data blocks to sample from.
  // @param compression_dict_samples Pointer to string to which sampled blocks
  // are appended.
  // @param compression_dict_sample_lens Vector of sample lengths. For each
  // sample added to compression_dict_samples, store the corresponding sample
  // length in this vector.
  virtual void SampleDict(std::vector<std::string>& data_block_buffers,
                          std::string* compression_dict_samples,
                          std::vector<size_t>* compression_dict_sample_lens);

  // Train a dictionary from data samples.
  // @param compression_dict_samples String containing the sampled data (it
  // should be populated by the SampleDict method).
  // @param compression_dict_sample_lens Length of each sample included in
  // compression_dict_samples (it should be populated by the SampleDict
  // method).
  virtual std::string TrainDict(
      const std::string& compression_dict_samples,
      const std::vector<size_t>& compression_dict_sample_lens);

 private:
  // Equivalent of max_dict_bytes in CompressionOptions.
  // As options are set for each compressor, this function returns the value for
  // that option.
  virtual uint32_t GetMaxDictBytes() const { return 0; }

  // Equivalent of max_zstd_train_bytes in CompressionOptions.
  // As options are set for each compressor, this function returns the value for
  // that option.
  virtual uint32_t GetMaxTrainBytes() const { return 0; }

  // Equivalent of use_zstd_dict_trainer in CompressionOptions.
  // As options are set for each compressor, this function returns the value for
  // that option.
  virtual bool UseDictTrainer() const { return true; }

  // Equivalent of level in CompressionOptions.
  // As options are set for each compressor, this function returns the value for
  // that option.
  virtual int GetLevel() const {
    return CompressionOptions::kDefaultCompressionLevel;
  }
};

// A BuiltinCompressor.  Instances of this class are based on the
// CompressionType enumerator.  Builtins should extend this class and if they
// are supported, implement the Compress and Uncompress methods appropriately.
// BuiltinCompressors use CompressionOptions to store their configuration but
// only configure/compare the values that are used by the specific
// implementation
class BuiltinCompressor : public Compressor {
 public:
  BuiltinCompressor();
  // Get Compressor instance given its numeric type.  Any instance of that type
  // will match, regardless of the ComprssionOptions.
  //
  // Note that a Compressor can be created for CompressorTypes that are not
  // supported (based on platform and libraries)
  static std::shared_ptr<Compressor> GetCompressor(CompressionType type);

  // Gets a Compressor instance for the input type with the corresponding
  // options. If one already exists that matches, it will be returned.
  // Otherwise, a new one will be created.
  //
  // Note that a Compressor can be created for CompressorTypes that are not
  // supported (based on platform and libraries)
  static std::shared_ptr<Compressor> GetCompressor(
      CompressionType type, const CompressionOptions& options);

  // Returns true if the input type is supported by this installation.
  static bool TypeSupported(CompressionType type);

  // Returns true if the input type supports dictionary compression.
  static bool TypeSupportsDict(CompressionType type);

  // Converts the input string to its corresponding CompressionType.
  // Supports strings in both class (e.g. "Snappy") and enum (e.g
  // "kSnappyCompression") formats Returns true if the conversion was successful
  // and false otherwise.
  static bool StringToType(const std::string& s, CompressionType* type);

  // Converts the input CompressionType to its "class" representation.
  // For example, kSnappyCompression would convert to "Snappy".
  static std::string TypeToString(CompressionType type);

  // Converts the input type into a string.
  // If as_class is true, returns the class representation (e.g. "Snappy").
  // If as_class is false, returns the enum representation(e.g.
  // "kSnappyCompression") Returns true if the CompressionType could be
  // converted to a string and false otherwise.
  static bool TypeToString(CompressionType type, bool as_class,
                           std::string* result);

  static const char* kClassName() { return "BuiltinCompressor"; }
  bool IsInstanceOf(const std::string& id) const override;
  const void* GetOptionsPtr(const std::string& name) const override;

  // Default implementation that returns NotSupported.
  // Implementations should override this method when they are enabld.
  Status Compress(const CompressionInfo& /*info*/, const Slice& /*input*/,
                  std::string* /*output*/) override {
    return Status::NotSupported("Compaction library not available ", GetId());
  }

  // Default implementation that returns NotSupported.
  // Implementations should override this method when they are enabld.
  Status Uncompress(const UncompressionInfo& /*info*/, const char* /*input*/,
                    size_t /*input_length*/, char** /*output*/,
                    size_t* /*output_length*/) override {
    return Status::NotSupported("Compaction library not available ", GetId());
  }

  uint32_t GetParallelThreads() const override {
    return compression_opts_.parallel_threads;
  }

  int GetMaxCompressedBytesPerKb() const override {
    return compression_opts_.max_compressed_bytes_per_kb;
  }

 protected:
  // Method to match the input CompressionOptions to those for this Builtin.
  // Only compares the values from opts that are required by this
  // implementation.
  virtual bool MatchesOptions(const CompressionOptions& opts) const;
  CompressionOptions compression_opts_;
};

// A BuiltinCompressor that supports a Compression Dictionary
class BuiltinDictCompressor : public BuiltinCompressor {
 public:
  BuiltinDictCompressor();
  static const char* kClassName() { return "BuiltinDictCompressor"; }
  bool IsInstanceOf(const std::string& id) const override;

 protected:
  uint64_t GetMaxDictBufferBytes() const override {
    return compression_opts_.max_dict_buffer_bytes;
  }
  uint32_t GetMaxDictBytes() const override {
    return compression_opts_.max_dict_bytes;
  }

  uint32_t GetMaxTrainBytes() const override {
    return compression_opts_.zstd_max_train_bytes;
  }

  bool UseDictTrainer() const override {
    return compression_opts_.use_zstd_dict_trainer;
  }

  int GetLevel() const override { return compression_opts_.level; }

  bool MatchesOptions(const CompressionOptions& opts) const override;
};

// Class with Options to be passed to Compressor::Compress
class CompressionInfo {
  const CompressionDict& dict_;
  const uint32_t compress_format_version_ = 2;
  const uint64_t sample_for_compression_ = 0;

 public:
  CompressionInfo(uint64_t _sample_for_compression = 0)
      : dict_(CompressionDict::GetEmptyDict()),
        sample_for_compression_(_sample_for_compression) {}

  explicit CompressionInfo(const CompressionDict& _dict,
                           uint32_t _compress_format_version = 2,
                           uint64_t _sample_for_compression = 0)
      : dict_(_dict),
        compress_format_version_(_compress_format_version),
        sample_for_compression_(_sample_for_compression) {}

  const CompressionDict& dict() const { return dict_; }
  uint32_t CompressFormatVersion() const { return compress_format_version_; }
  uint64_t SampleForCompression() const { return sample_for_compression_; }

  inline bool CompressData(Compressor* compressor, const Slice& raw,
                           std::string* compressed) const {
    bool ret = false;

    // Will return compressed block contents if (1) the compression method is
    // supported in this platform and (2) the compression rate is "good enough".
    if (compressor == nullptr) {
      ret = false;
    } else {
      Status s = compressor->Compress(*this, raw, compressed);
      ret = s.ok();
    }

    TEST_SYNC_POINT_CALLBACK("CompressData:TamperWithReturnValue",
                             static_cast<void*>(&ret));

    return ret;
  }
};

// Class with Options to be passed to Compressor::Compress
class UncompressionInfo {
  const UncompressionDict& dict_;
  const uint32_t compress_format_version_ = 2;
  MemoryAllocator* allocator_ = nullptr;

 public:
  UncompressionInfo() : dict_(UncompressionDict::GetEmptyDict()) {}
  explicit UncompressionInfo(const UncompressionDict& _dict,
                             uint32_t _compress_format_version = 2,
                             MemoryAllocator* _allocator = nullptr)
      : dict_(_dict),
        compress_format_version_(_compress_format_version),
        allocator_(_allocator) {}

  const UncompressionDict& dict() const { return dict_; }
  uint32_t CompressFormatVersion() const { return compress_format_version_; }
  MemoryAllocator* GetMemoryAllocator() const { return allocator_; }

  inline CacheAllocationPtr UncompressData(
      Compressor* compressor, const char* compressed, size_t compressed_size,
      size_t* uncompressed_size, Status* uncompress_status = nullptr) const {
    if (compressor == nullptr) {
      return CacheAllocationPtr();
    }

    char* uncompressed_data;
    Status s = compressor->Uncompress(*this, compressed, compressed_size,
                                      &uncompressed_data, uncompressed_size);
    if (uncompress_status != nullptr) {
      *uncompress_status = s;
    }
    if (!s.ok()) {
      return CacheAllocationPtr();
    }
    CacheAllocationPtr ubuf(uncompressed_data, allocator_);
    return ubuf;
  }
};

}  // namespace ROCKSDB_NAMESPACE
