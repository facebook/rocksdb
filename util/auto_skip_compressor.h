//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Creates auto skip compressor wrapper which uses multiple compression
// algorithm within same SST file.

#pragma once
#include <memory>
#include <mutex>

#include "rocksdb/advanced_compression.h"
#include "util/compression.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {
// Predicts Rejection Ratio for a window size algorithm
class CompressionRejectionProbabilityPredictor {
 public:
  CompressionRejectionProbabilityPredictor(int window_size)
      : pred_rejection_percentage_(0),
        rejected_count_(0),
        compressed_count_(0),
        window_size_(window_size),
        attempted_compression_count_(0) {}
  int Predict() const { return pred_rejection_percentage_; }
  inline void TEST_SetPrediction(int pred_rejection) {
    pred_rejection_percentage_ = pred_rejection;
  }
  bool Record(Slice uncompressed_block_data, std::string* compressed_output,
              const CompressionOptions& opts);

 protected:
  int pred_rejection_percentage_ = 0;
  size_t rejected_count_ = 0;
  size_t compressed_count_ = 0;
  const size_t window_size_;
  size_t attempted_compression_count_ = 0;
};
class AutoSkipCompressorWrapper : public CompressorWrapper {
 public:
  explicit AutoSkipCompressorWrapper(std::unique_ptr<Compressor> compressor,
                                     const CompressionOptions& opts);

  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override;

  // Tracking methods
  void TEST_SetMinExplorationPercentage(int min_exploration_percentage);
  int TEST_GetMinExplorationPercentage() const;

 private:
  int min_exploration_percentage_;
  const CompressionOptions& opts_;
  Random rnd_;
  mutable std::mutex mutex_;
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
