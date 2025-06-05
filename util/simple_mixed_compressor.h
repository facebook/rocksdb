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
#include <random>

#include "compression.h"
#include "options/options_helper.h"
#include "rocksdb/advanced_compression.h"

namespace ROCKSDB_NAMESPACE {

class MultiCompressorWrapper : public Compressor {
 public:
  explicit MultiCompressorWrapper(const CompressionOptions& opts,
                                  CompressionType type,
                                  CompressionDict&& dict = {}) {
    assert(type != kNoCompression);
    assert(type == kZSTD);
    auto builtInManager = GetDefaultBuiltinCompressionManager();
    const auto& compressions = GetSupportedCompressions();
    for (auto type_ : compressions) {
      if (type_ == kNoCompression) {  // Avoid no compression
        continue;
      }
      compressors_.push_back(builtInManager->GetCompressor(opts, type_));
    }
    (void)dict;
    (void)type;
  }
  size_t GetMaxSampleSizeIfWantDict(CacheEntryRole block_type) const override {
    return compressors_.back()->GetMaxSampleSizeIfWantDict(block_type);
  }

  Slice GetSerializedDict() const override {
    return compressors_.back()->GetSerializedDict();
  }

  CompressionType GetPreferredCompressionType() const override { return kZSTD; }

  ManagedWorkingArea ObtainWorkingArea() override {
    return compressors_.back()->ObtainWorkingArea();
  }
  virtual std::unique_ptr<Compressor> MaybeCloneSpecialized(
      CacheEntryRole block_type, DictSampleArgs&& dict_samples) override {
    return compressors_.back()->MaybeCloneSpecialized(block_type,
                                                      std::move(dict_samples));
  }

 protected:
  std::vector<std::unique_ptr<Compressor>> compressors_;

 private:
  mutable std::mutex mutex_;  // Protects access to current_index_
};
struct SimpleMixedCompressor : public MultiCompressorWrapper {
  using MultiCompressorWrapper::MultiCompressorWrapper;
  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override {
    const auto& compressions = GetSupportedCompressions();
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(
        1, (int)compressions.size() - 2);  // avoiding no compression and zstd
    auto selected = dis(gen);
    auto& compressor = compressors_[selected % compressors_.size()];
    // fprintf(stdout, "[MultiCompressorWrapper] selected compressor
    // typeint:%d\n",
    //         selected);
    Status status = compressor->CompressBlock(
        uncompressed_data, compressed_output, out_compression_type, wa);
    return status;
  }
};

class SimpleMixedCompressionManager : public CompressionManagerWrapper {
  using CompressionManagerWrapper::CompressionManagerWrapper;
  const char* Name() const override { return wrapped_->Name(); }
  std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext& context, const CompressionOptions& opts,
      CompressionType preferred) override {
    assert(preferred == kZSTD);
    (void)context;
    return std::make_unique<SimpleMixedCompressor>(opts, preferred);
  }
};

struct RoundRobinCompressor : public MultiCompressorWrapper {
  using MultiCompressorWrapper::MultiCompressorWrapper;
  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override {
    const auto& compressions = GetSupportedCompressions();
    auto counter = block_counter.FetchAddRelaxed(1);
    auto sel_idx = counter % (compressions.size() - 1);
    auto& compressor = compressors_[sel_idx];
    // auto type = compressions[sel_idx];
    // fprintf(stdout,
    //         "[CompressorWrapper] selected compression algo: %s typeint:%d\n",
    //         std::to_string(type).c_str(), type);
    return compressor->CompressBlock(uncompressed_data, compressed_output,
                                     out_compression_type, wa);
  }
  static RelaxedAtomic<uint64_t> block_counter;
};
RelaxedAtomic<uint64_t> RoundRobinCompressor::block_counter{0};

class RoundRobinManager : public CompressionManagerWrapper {
  using CompressionManagerWrapper::CompressionManagerWrapper;
  const char* Name() const override { return wrapped_->Name(); }
  std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext& context, const CompressionOptions& opts,
      CompressionType preferred) override {
    assert(preferred == kZSTD);
    (void)context;
    // fprintf(stdout,
    //         "[CompressorWrapper] selected compression algo: %s typeint:%d\n",
    // void)context;
    return std::make_unique<RoundRobinCompressor>(opts, preferred);
  }
};

}  // namespace ROCKSDB_NAMESPACE
