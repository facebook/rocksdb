//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Creates mixed compressor wrapper which uses multiple compression algorithm
// within same SST file.

#pragma once
#include <memory>
#include <mutex>
#include <vector>

#include "compression.h"
#include "random.h"
#include "rocksdb/advanced_compression.h"

namespace ROCKSDB_NAMESPACE {

class AutoSkipCompressorWrapper : public Compressor {
 public:
  explicit AutoSkipCompressorWrapper(const CompressionOptions& opts,
                                     CompressionType type,
                                     CompressionDict&& dict = {});

  size_t GetMaxSampleSizeIfWantDict(CacheEntryRole block_type) const override;
  Slice GetSerializedDict() const override;
  CompressionType GetPreferredCompressionType() const override;
  ManagedWorkingArea ObtainWorkingArea() override;
  std::unique_ptr<Compressor> MaybeCloneSpecialized(
      CacheEntryRole block_type, DictSampleArgs&& dict_samples) override;

  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override;

  // Tracking methods
  size_t GetRejectedCount() const;
  size_t GetCompressedCount() const;
  size_t GetBypassedCount() const;
  double GetPredRejectionRatio() const;
  bool RecordUpdatePred(std::string* compressed_output,
                        CompressionType* out_compression_type);

 private:
  std::vector<std::unique_ptr<Compressor>> compressors_;
  int pred_rejection_percentage_;
  int min_exploration_percentage_;
  size_t attempted_compression_count_;
  size_t rejected_count_;
  size_t compressed_count_;
  size_t bypassed_count_;
  mutable std::mutex mutex_;
  Random rnd_;
};

class AutoSkipCompressorManager : public CompressionManagerWrapper {
  using CompressionManagerWrapper::CompressionManagerWrapper;
  const char* Name() const override;
  std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext& context, const CompressionOptions& opts,
      CompressionType preferred) override;
};

}  // namespace ROCKSDB_NAMESPACE
