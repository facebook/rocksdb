//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "util/auto_tune_compressor.h"

#include "options/options_helper.h"
#include "rocksdb/advanced_compression.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/stop_watch.h"
namespace ROCKSDB_NAMESPACE {

int CompressionRejectionProbabilityPredictor::Predict() const {
  return pred_rejection_prob_percentage_;
}

size_t CompressionRejectionProbabilityPredictor::attempted_compression_count()
    const {
  return rejected_count_ + compressed_count_;
}

bool CompressionRejectionProbabilityPredictor::Record(
    Slice uncompressed_block_data, std::string* compressed_output,
    const CompressionOptions& opts) {
  if (compressed_output->size() >
      (static_cast<uint64_t>(opts.max_compressed_bytes_per_kb) *
       uncompressed_block_data.size()) >>
      10) {
    rejected_count_++;
  } else {
    compressed_count_++;
  }
  auto attempted = attempted_compression_count();
  if (attempted >= window_size_) {
    pred_rejection_prob_percentage_ =
        static_cast<int>(rejected_count_ * 100 / attempted);
    compressed_count_ = 0;
    rejected_count_ = 0;
    assert(attempted_compression_count() == 0);
  }
  return true;
}

AutoSkipCompressorWrapper::AutoSkipCompressorWrapper(
    std::unique_ptr<Compressor> compressor, const CompressionOptions& opts)
    : CompressorWrapper::CompressorWrapper(std::move(compressor)),
      kOpts(opts) {}

const char* AutoSkipCompressorWrapper::Name() const {
  return "AutoSkipCompressorWrapper";
}

Status AutoSkipCompressorWrapper::CompressBlock(
    Slice uncompressed_data, std::string* compressed_output,
    CompressionType* out_compression_type, ManagedWorkingArea* wa) {
  // Check if the managed working area is provided or owned by this object.
  // If not, bypass auto-skip logic since the working area lacks a predictor to
  // record or make necessary decisions to compress or bypass compression of the
  // block
  if (wa == nullptr || wa->owner() != this) {
    return wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                   out_compression_type, wa);
  }
  bool exploration =
      Random::GetTLSInstance()->PercentTrue(kExplorationPercentage);
  TEST_SYNC_POINT_CALLBACK(
      "AutoSkipCompressorWrapper::CompressBlock::exploitOrExplore",
      &exploration);
  auto autoskip_wa = static_cast<AutoSkipWorkingArea*>(wa->get());
  if (exploration) {
    return CompressBlockAndRecord(uncompressed_data, compressed_output,
                                  out_compression_type, autoskip_wa);
  } else {
    auto predictor_ptr = autoskip_wa->predictor;
    auto prediction = predictor_ptr->Predict();
    if (prediction <= kProbabilityCutOff) {
      // decide to compress
      return CompressBlockAndRecord(uncompressed_data, compressed_output,
                                    out_compression_type, autoskip_wa);
    } else {
      // decide to bypass compression
      *out_compression_type = kNoCompression;
      return Status::OK();
    }
  }
  return Status::OK();
}

Compressor::ManagedWorkingArea AutoSkipCompressorWrapper::ObtainWorkingArea() {
  auto wrap_wa = wrapped_->ObtainWorkingArea();
  return ManagedWorkingArea(new AutoSkipWorkingArea(std::move(wrap_wa)), this);
}
void AutoSkipCompressorWrapper::ReleaseWorkingArea(WorkingArea* wa) {
  delete static_cast<AutoSkipWorkingArea*>(wa);
}

Status AutoSkipCompressorWrapper::CompressBlockAndRecord(
    Slice uncompressed_data, std::string* compressed_output,
    CompressionType* out_compression_type, AutoSkipWorkingArea* wa) {
  Status status = wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                          out_compression_type, &(wa->wrapped));
  // determine if it was rejected or compressed
  auto predictor_ptr = wa->predictor;
  predictor_ptr->Record(uncompressed_data, compressed_output, kOpts);
  return status;
}

const char* AutoSkipCompressorManager::Name() const {
  // should have returned "AutoSkipCompressorManager" but we currently have an
  // error so for now returning name of the wrapped container
  return wrapped_->Name();
}

std::unique_ptr<Compressor> AutoSkipCompressorManager::GetCompressorForSST(
    const FilterBuildingContext& context, const CompressionOptions& opts,
    CompressionType preferred) {
  assert(GetSupportedCompressions().size() > 1);
  assert(preferred != kNoCompression);
  return std::make_unique<AutoSkipCompressorWrapper>(
      wrapped_->GetCompressorForSST(context, opts, preferred), opts);
}

CostAwareCompressor::CostAwareCompressor(const CompressionOptions& opts)
    : kOpts(opts) {
  // Creates compressor supporting all the compression types and levels as per
  // the compression levels set in vector compression_levels_
  auto builtInManager = GetBuiltinV2CompressionManager();
  const auto& compressions = GetSupportedCompressions();
  for (size_t i = 0; i < compression_levels_.size(); i++) {
    CompressionType type_ = static_cast<CompressionType>(i + 1);
    if (type_ == kNoCompression) {
      continue;
    }
    if (compression_levels_[type_ - 1].size() == 0) {
      allcompressors_.emplace_back();
      continue;
    } else {
      // if the compression type is not supported, then skip and remove
      // compression levels from the supported compression level list
      if (std::find(compressions.begin(), compressions.end(), type_) ==
          compressions.end()) {
        compression_levels_[i].clear();
        allcompressors_.emplace_back();
        continue;
      }
      std::vector<std::unique_ptr<Compressor>> compressors_diff_levels;
      for (size_t j = 0; j < compression_levels_[type_ - 1].size(); j++) {
        auto level = compression_levels_[type_ - 1][j];
        CompressionOptions new_opts = opts;
        new_opts.level = level;
        compressors_diff_levels.push_back(
            builtInManager->GetCompressor(new_opts, type_));
        allcompressors_index_.emplace_back(i, j);
      }
      allcompressors_.push_back(std::move(compressors_diff_levels));
    }
  }
}

const char* CostAwareCompressor::Name() const { return "CostAwareCompressor"; }

Status CostAwareCompressor::CompressBlock(Slice uncompressed_data,
                                          std::string* compressed_output,
                                          CompressionType* out_compression_type,
                                          ManagedWorkingArea* wa) {
  // Check if the managed working area is provided or owned by this object.
  // If not, bypass compressor logic since the working area lacks a predictor
  if (allcompressors_.size() == 0) {
    return Status::NotSupported("No compression type supported");
  }
  if (wa == nullptr || wa->owner() != this) {
    // We just go with the last compressor created. If Zstd is supported will be
    // highest compression level of Zstd
    auto choosen_compression = allcompressors_index_.back();
    size_t choosen_compression_type = choosen_compression.first;
    size_t compression_level_ptr = choosen_compression.second;
    return allcompressors_[choosen_compression_type][compression_level_ptr]
        ->CompressBlock(uncompressed_data, compressed_output,
                        out_compression_type, wa);
  }
  auto local_wa = static_cast<CostAwareWorkingArea*>(wa->get());
  auto choosen_index = allcompressors_index_[Random::GetTLSInstance()->Uniform(
      static_cast<int>(allcompressors_index_.size()))];
  TEST_SYNC_POINT_CALLBACK(
      "CostAwareCompressor::CompressBlock::SelectCompressionTypeAndLevel",
      &choosen_index);
  size_t choosen_compression_type = choosen_index.first;
  size_t compresion_level_ptr = choosen_index.second;
  return CompressBlockAndRecord(choosen_compression_type, compresion_level_ptr,
                                uncompressed_data, compressed_output,
                                out_compression_type, local_wa);
}

Compressor::ManagedWorkingArea CostAwareCompressor::ObtainWorkingArea() {
  auto wrap_wa = allcompressors_.back().back()->ObtainWorkingArea();
  auto wa = new CostAwareWorkingArea(std::move(wrap_wa));
  // Create cost predictors for each compression type and level
  wa->cost_predictors.reserve(compression_levels_.size());
  for (size_t i = 0; i < compression_levels_.size(); i++) {
    CompressionType type_ = static_cast<CompressionType>(i + 1);
    if (compression_levels_[type_ - 1].size() == 0) {
      wa->cost_predictors.emplace_back();
      continue;
    } else {
      std::vector<IOCPUCostPredictor*> predictors_diff_levels;
      predictors_diff_levels.reserve(compression_levels_[type_ - 1].size());
      for (size_t j = 0; j < compression_levels_[type_ - 1].size(); j++) {
        predictors_diff_levels.emplace_back(new IOCPUCostPredictor(10));
      }
      wa->cost_predictors.emplace_back(std::move(predictors_diff_levels));
    }
  }
  return ManagedWorkingArea(wa, this);
}
void CostAwareCompressor::ReleaseWorkingArea(WorkingArea* wa) {
  // remove all created cost predictors
  for (auto& prdictors_diff_levels :
       static_cast<CostAwareWorkingArea*>(wa)->cost_predictors) {
    for (auto& predictor : prdictors_diff_levels) {
      delete predictor;
    }
  }
  delete static_cast<CostAwareWorkingArea*>(wa);
}

Status CostAwareCompressor::CompressBlockAndRecord(
    size_t choosen_compression_type, size_t compression_level_ptr,
    Slice uncompressed_data, std::string* compressed_output,
    CompressionType* out_compression_type, CostAwareWorkingArea* wa) {
  assert(choosen_compression_type < allcompressors_.size());
  assert(compresion_level_ptr <
         allcompressors_[choosen_compression_type].size());
  StopWatchNano timer(Env::Default()->GetSystemClock().get(), true);
  Status status =
      allcompressors_[choosen_compression_type][compression_level_ptr]
          ->CompressBlock(uncompressed_data, compressed_output,
                          out_compression_type, &(wa->wrapped));
  std::pair<size_t, size_t> measured_data(timer.ElapsedCPUNanos(),
                                          compressed_output->size());
  auto predictor =
      wa->cost_predictors[choosen_compression_type][compression_level_ptr];
  TEST_SYNC_POINT_CALLBACK(
      "CostAwareCompressor::CompressBlockAndRecord::"
      "SetCompressionTimeOutputSize",
      &measured_data);
  auto output_length = measured_data.second;
  auto cpu_time = measured_data.first;
  predictor->CPUPredictor.Record(cpu_time);
  predictor->IOPredictor.Record(output_length);
  TEST_SYNC_POINT_CALLBACK(
      "CostAwareCompressor::CompressBlockAndRecord::GetPredictor",
      &(wa->cost_predictors[choosen_compression_type][compression_level_ptr]));
  return status;
}

std::shared_ptr<CompressionManagerWrapper> CreateAutoSkipCompressionManager(
    std::shared_ptr<CompressionManager> wrapped) {
  return std::make_shared<AutoSkipCompressorManager>(
      wrapped == nullptr ? GetBuiltinV2CompressionManager() : wrapped);
}
const char* CostAwareCompressorManager::Name() const {
  // should have returned "CostAwareCompressorManager" but we currently have an
  // error so for now returning name of the wrapped container
  return wrapped_->Name();
}

std::unique_ptr<Compressor> CostAwareCompressorManager::GetCompressorForSST(
    const FilterBuildingContext& context, const CompressionOptions& opts,
    CompressionType preferred) {
  assert(GetSupportedCompressions().size() > 1);
  assert(preferred != kNoCompression);
  (void)context;
  (void)preferred;
  return std::make_unique<CostAwareCompressor>(opts);
}

std::shared_ptr<CompressionManagerWrapper> CreateCostAwareCompressionManager(
    std::shared_ptr<CompressionManager> wrapped) {
  return std::make_shared<CostAwareCompressorManager>(
      wrapped == nullptr ? GetBuiltinV2CompressionManager() : wrapped);
}
}  // namespace ROCKSDB_NAMESPACE
