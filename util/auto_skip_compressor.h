//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Creates auto skip compressor wrapper which intelligently decides bypassing
// compression based on past data

#pragma once
#include <memory>

#include "rocksdb/advanced_compression.h"

namespace ROCKSDB_NAMESPACE {
// Predict rejection probability using a moving window approach
// This class is not thread safe
class CompressionRejectionProbabilityPredictor {
 public:
  CompressionRejectionProbabilityPredictor(int window_size)
      : pred_rejection_prob_percentage_(0),
        rejected_count_(0),
        compressed_count_(0),
        window_size_(window_size) {}
  int Predict() const;
  bool Record(Slice uncompressed_block_data, std::string* compressed_output,
              const CompressionOptions& opts);
  size_t attempted_compression_count() const;

 protected:
  int pred_rejection_prob_percentage_;
  size_t rejected_count_;
  size_t compressed_count_;
  size_t window_size_;
};

class AutoSkipCompressorWrapper : public CompressorWrapper {
 public:
  const char* Name() const override;
  explicit AutoSkipCompressorWrapper(std::unique_ptr<Compressor> compressor,
                                     const CompressionOptions& opts);

  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override;

 private:
  Status CompressBlockAndRecord(Slice uncompressed_data,
                                std::string* compressed_output,
                                CompressionType* out_compression_type,
                                ManagedWorkingArea* wa);
  static constexpr int kExplorationPercentage = 10;
  static constexpr int kProbabilityCutOff = 50;
  const CompressionOptions kOpts;
  std::shared_ptr<CompressionRejectionProbabilityPredictor> predictor_;
};

class AutoSkipCompressorManager : public CompressionManagerWrapper {
  using CompressionManagerWrapper::CompressionManagerWrapper;
  const char* Name() const override;
  std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext& context, const CompressionOptions& opts,
      CompressionType preferred) override;
};

}  // namespace ROCKSDB_NAMESPACE
