//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Creates mixed compressor wrapper which uses multiple compression algorithm
// within same SST file.

#pragma once
#include <memory>
#include <vector>

#include "compression.h"
#include "rocksdb/advanced_compression.h"

namespace ROCKSDB_NAMESPACE {

class MultiCompressorWrapper : public Compressor {
 public:
  explicit MultiCompressorWrapper(const CompressionOptions& opts,
                                  CompressionDict&& dict = {});

  size_t GetMaxSampleSizeIfWantDict(CacheEntryRole block_type) const override;
  Slice GetSerializedDict() const override;
  CompressionType GetPreferredCompressionType() const override;
  ManagedWorkingArea ObtainWorkingArea() override;
  std::unique_ptr<Compressor> MaybeCloneSpecialized(
      CacheEntryRole block_type, DictSampleArgs&& dict_samples) override;

 protected:
  std::vector<std::unique_ptr<Compressor>> compressors_;
};

struct RandomMixedCompressor : public MultiCompressorWrapper {
  using MultiCompressorWrapper::MultiCompressorWrapper;
  const char* Name() const override;
  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override;
};

class RandomMixedCompressionManager : public CompressionManagerWrapper {
  using CompressionManagerWrapper::CompressionManagerWrapper;
  const char* Name() const override;
  std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext& context, const CompressionOptions& opts,
      CompressionType preferred) override;
};

struct RoundRobinCompressor : public MultiCompressorWrapper {
  using MultiCompressorWrapper::MultiCompressorWrapper;
  const char* Name() const override;
  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override;
  static RelaxedAtomic<uint64_t> block_counter;
};

class RoundRobinManager : public CompressionManagerWrapper {
  using CompressionManagerWrapper::CompressionManagerWrapper;
  const char* Name() const override;
  std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext& context, const CompressionOptions& opts,
      CompressionType preferred) override;
};

}  // namespace ROCKSDB_NAMESPACE
