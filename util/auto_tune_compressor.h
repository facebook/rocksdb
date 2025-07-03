//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Defines auto skip compressor wrapper which intelligently decides bypassing
// compression based on past data
// Defines CostAwareCompressor which currently tries to predict the cpu and io
// cost of the compression

#pragma once
#include <memory>

#include "rocksdb/advanced_compression.h"

namespace ROCKSDB_NAMESPACE {
// Auto Skip Compression Components
// Predict rejection probability using a moving window approach
class CompressionRejectionProbabilityPredictor {
 public:
  explicit CompressionRejectionProbabilityPredictor(int window_size)
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

class AutoSkipWorkingArea : public Compressor::WorkingArea {
 public:
  explicit AutoSkipWorkingArea(Compressor::ManagedWorkingArea&& wa)
      : wrapped(std::move(wa)),
        predictor(
            std::make_shared<CompressionRejectionProbabilityPredictor>(10)) {}
  ~AutoSkipWorkingArea() {}
  AutoSkipWorkingArea(const AutoSkipWorkingArea&) = delete;
  AutoSkipWorkingArea& operator=(const AutoSkipWorkingArea&) = delete;
  AutoSkipWorkingArea(AutoSkipWorkingArea&& other) noexcept
      : wrapped(std::move(other.wrapped)),
        predictor(std::move(other.predictor)) {}

  AutoSkipWorkingArea& operator=(AutoSkipWorkingArea&& other) noexcept {
    if (this != &other) {
      wrapped = std::move(other.wrapped);
      predictor = std::move(other.predictor);
    }
    return *this;
  }
  Compressor::ManagedWorkingArea wrapped;
  std::shared_ptr<CompressionRejectionProbabilityPredictor> predictor;
};
class AutoSkipCompressorWrapper : public CompressorWrapper {
 public:
  const char* Name() const override;
  explicit AutoSkipCompressorWrapper(std::unique_ptr<Compressor> compressor,
                                     const CompressionOptions& opts);

  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override;
  ManagedWorkingArea ObtainWorkingArea() override;
  void ReleaseWorkingArea(WorkingArea* wa) override;

 private:
  Status CompressBlockAndRecord(Slice uncompressed_data,
                                std::string* compressed_output,
                                CompressionType* out_compression_type,
                                AutoSkipWorkingArea* wa);
  static constexpr int kExplorationPercentage = 10;
  static constexpr int kProbabilityCutOff = 50;
  const CompressionOptions opts_;
};

class AutoSkipCompressorManager : public CompressionManagerWrapper {
  using CompressionManagerWrapper::CompressionManagerWrapper;
  const char* Name() const override;
  std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext& context, const CompressionOptions& opts,
      CompressionType preferred) override;
};
// Cost Aware Components
template <typename T>
class WindowAveragePredictor {
 public:
  explicit WindowAveragePredictor(int window_size)
      : sum_(0), prediction_(0), count_(0), kWindowSize(window_size) {}
  T Predict() { return prediction_; }
  bool Record(T data) {
    sum_ += data;
    count_++;
    if (count_ >= kWindowSize) {
      prediction_ = sum_ / count_;
      sum_ = 0;
      count_ = 0;
    }
    return true;
  }
  void SetPrediction(T prediction) { prediction_ = prediction; }

 private:
  T sum_;
  T prediction_;
  int count_;
  const int kWindowSize;
};

using IOCostPredictor = WindowAveragePredictor<size_t>;
using CPUUtilPredictor = WindowAveragePredictor<uint64_t>;

struct IOCPUCostPredictor {
  explicit IOCPUCostPredictor(int window_size)
      : IOPredictor(window_size), CPUPredictor(window_size) {}
  IOCostPredictor IOPredictor;
  CPUUtilPredictor CPUPredictor;
};
class CostAwareWorkingArea : public Compressor::WorkingArea {
 public:
  explicit CostAwareWorkingArea(Compressor::ManagedWorkingArea&& wa)
      : wrapped_(std::move(wa)) {}
  ~CostAwareWorkingArea() {}
  CostAwareWorkingArea(const CostAwareWorkingArea&) = delete;
  CostAwareWorkingArea& operator=(const CostAwareWorkingArea&) = delete;
  CostAwareWorkingArea(CostAwareWorkingArea&& other) noexcept
      : wrapped_(std::move(other.wrapped_)) {}

  CostAwareWorkingArea& operator=(CostAwareWorkingArea&& other) noexcept {
    if (this != &other) {
      wrapped_ = std::move(other.wrapped_);
      cost_predictors_ = std::move(other.cost_predictors_);
    }
    return *this;
  }
  Compressor::ManagedWorkingArea wrapped_;
  std::vector<std::vector<IOCPUCostPredictor*>> cost_predictors_;
};

class CostAwareCompressor : public Compressor {
 public:
  explicit CostAwareCompressor(const CompressionOptions& opts);
  const char* Name() const override;
  size_t GetMaxSampleSizeIfWantDict(CacheEntryRole block_type) const override;
  Slice GetSerializedDict() const override;
  CompressionType GetPreferredCompressionType() const override;
  ManagedWorkingArea ObtainWorkingArea() override;
  std::unique_ptr<Compressor> MaybeCloneSpecialized(
      CacheEntryRole block_type, DictSampleArgs&& dict_samples) override;

  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override;
  void ReleaseWorkingArea(WorkingArea* wa) override;

 private:
  Status CompressBlockAndRecord(size_t choosen_compression_type,
                                size_t compresion_level_ptr,
                                Slice uncompressed_data,
                                std::string* compressed_output,
                                CompressionType* out_compression_type,
                                CostAwareWorkingArea* wa);
  static constexpr int kExplorationPercentage = 10;
  static constexpr int kProbabilityCutOff = 50;
  // This is the vector containing the list of compression levels that
  // CostAwareCompressor will use create compressor and predicts the cost
  // The vector contains list of compression level for compression algorithm in
  // the order defined by enum CompressionType
  static const std::vector<std::vector<int>> kCompressionLevels;
  const CompressionOptions opts_;
  std::vector<std::vector<std::unique_ptr<Compressor>>> allcompressors_;
  std::vector<std::pair<size_t, size_t>> allcompressors_index_;
};

class CostAwareCompressorManager : public CompressionManagerWrapper {
  using CompressionManagerWrapper::CompressionManagerWrapper;
  const char* Name() const override;
  std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext& context, const CompressionOptions& opts,
      CompressionType preferred) override;
};

}  // namespace ROCKSDB_NAMESPACE
