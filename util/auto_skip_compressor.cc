//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Creates mixed compressor wrapper which uses multiple compression algorithm
// within same SST file.

#include "auto_skip_compressor.h"

#include <options/options_helper.h>

#include "random.h"
#include "rocksdb/advanced_compression.h"
namespace ROCKSDB_NAMESPACE {

bool RejectionRatioPredictor::Record(Slice uncompressed_block_data,
                                     std::string* compressed_output,
                                     const CompressionOptions& opts) {
  if (compressed_output->size() >
      (static_cast<uint64_t>(opts.max_compressed_bytes_per_kb) *
       uncompressed_block_data.size()) >>
      10) {
    // Prefer to keep uncompressed
    rejected_count_++;
  } else {
    compressed_count_++;
  }
  // block_nased_table_builder line no 1379
  /**
  auto compression_attempted = !compressed_output->empty();
  if (*out_compression_type == kNoCompression && compression_attempted) {
    rejected_count_++;
  } else if (*out_compression_type == kNoCompression) {
    bypassed_count_++;
  } else {
    compressed_count_++;
  }
    **/
  return true;
}

// WindowBasedRejectionPredictor Implementation
// WindowBasedRejectionPredictor::WindowBasedRejectionPredictor(int window_size)
// : window_size_(window_size) {}

int WindowBasedRejectionPredictor::Predict() const {
  // Implement window-based prediction logic
  return RejectionRatioPredictor::Predict();
}

// void WindowBasedRejectionPredictor::SetPrediction(
//     int pred_rejection) {  // Implement window-based prediction setting logic
//   RejectionRatioPredictor::SetPrediction(pred_rejection);
// }

bool WindowBasedRejectionPredictor::Record(Slice uncompressed_block_data,
                                           std::string* compressed_output,
                                           const CompressionOptions& opts) {
  auto status = RejectionRatioPredictor::Record(uncompressed_block_data,
                                                compressed_output, opts);
  attempted_compression_count_++;
  if (attempted_compression_count_ >= window_size_) {
    pred_rejection_percentage_ = static_cast<int>(
        rejected_count_ * 100 / (compressed_count_ + rejected_count_));
    fprintf(stdout,
            "[WindowBasedRejectionPredictor::Record] changed "
            "pred_rejection_percentage_: %d\n",
            pred_rejection_percentage_);
    attempted_compression_count_ = 0;
    compressed_count_ = 0;
    rejected_count_ = 0;
  }
  return status;
}
/**
bool WindowBasedRejectionPredictor::Record(
  std::string* compressed_output, CompressionType* out_compression_type) {
// Implement window-based recording logic
auto status =
    RejectionRatioPredictor::Record(compressed_output, out_compression_type);
attempted_compression_count_++;
if (attempted_compression_count_ >= window_size_) {
  pred_rejection_percentage_ = static_cast<int>(
      rejected_count_ * 100 / (compressed_count_ + rejected_count_));
  fprintf(stdout,
          "[WindowBasedRejectionPredictor::Record] changed "
          "pred_rejection_percentage_: %d\n",
          pred_rejection_percentage_);
  attempted_compression_count_ = 0;
  compressed_count_ = 0;
  rejected_count_ = 0;
  bypassed_count_ = 0;
}
return status;
}
**/
AutoSkipCompressorWrapper::AutoSkipCompressorWrapper(
    const CompressionOptions& opts, CompressionType type,
    CompressionDict&& dict)
    : min_exploration_percentage_(10),
      opts_(opts),
      type_(type),
      rnd_(331),
      model_(std::make_shared<WindowBasedRejectionPredictor>(100)) {
  assert(type != kNoCompression);
  auto builtInManager = GetDefaultBuiltinCompressionManager();
  const auto& compressions = GetSupportedCompressions();
  for (auto algo : compressions) {
    if (algo == kNoCompression) {
      continue;
    }
    compressors_.push_back(builtInManager->GetCompressor(opts, algo));
  }
  fprintf(stdout,
          "[AutoSkipCompressorWrapper::AutoSkipCompressorWrapper] Created new "
          "compressor wrapper\n");
  (void)dict;
}

size_t AutoSkipCompressorWrapper::GetMaxSampleSizeIfWantDict(
    CacheEntryRole block_type) const {
  return compressors_.back()->GetMaxSampleSizeIfWantDict(block_type);
}

Slice AutoSkipCompressorWrapper::GetSerializedDict() const {
  return compressors_.back()->GetSerializedDict();
}

CompressionType AutoSkipCompressorWrapper::GetPreferredCompressionType() const {
  return kZSTD;
}

Compressor::ManagedWorkingArea AutoSkipCompressorWrapper::ObtainWorkingArea() {
  return compressors_.back()->ObtainWorkingArea();
}

std::unique_ptr<Compressor> AutoSkipCompressorWrapper::MaybeCloneSpecialized(
    CacheEntryRole block_type, DictSampleArgs&& dict_samples) {
  return compressors_.back()->MaybeCloneSpecialized(block_type,
                                                    std::move(dict_samples));
}

Status AutoSkipCompressorWrapper::CompressBlock(
    Slice uncompressed_data, std::string* compressed_output,
    CompressionType* out_compression_type, ManagedWorkingArea* wa) {
  std::lock_guard<std::mutex> lock(mutex_);
  // exploration with pred_rejection_percentage
  auto& compressor = compressors_.back();
  if (rnd_.PercentTrue(min_exploration_percentage_)) {
    // fprintf(
    //     stdout,
    //     "[AutoSkipCompressorWrapper::CompressBlock] selected:
    //     exploration\n");
    Status status = compressor->CompressBlock(
        uncompressed_data, compressed_output, out_compression_type, wa);
    // check the value of the out_compression_type and compressed_output to
    // determine if it was rejected or compressed
    model_->Record(uncompressed_data, compressed_output, opts_);
    return status;
  } else {
    auto prediction = model_->Predict();
    // fprintf(stdout,
    //         "[AutoSkipCompressorWrapper::CompressBlock] selected: exploit "
    //         "pred_rejection: %d\n",
    //         prediction);
    if (prediction < 50) {
      // decide to compress
      // check the value of the out_compression_type and compressed_output to
      Status status = compressor->CompressBlock(
          uncompressed_data, compressed_output, out_compression_type, wa);
      // determine if it was rejected or compressed
      model_->Record(uncompressed_data, compressed_output, opts_);
      return status;
    } else {
      // bypassed compression
      *compressed_output = "";
      *out_compression_type = kNoCompression;
      return Status::OK();
    }
  }
  return Status::OK();
}

const char* AutoSkipCompressorManager::Name() const {
  // should have returned "AutoSkipCompressorManager" but we currently have an
  // error so return return "AutoSkipCompressorManager";
  return wrapped_->Name();
}

std::unique_ptr<Compressor> AutoSkipCompressorManager::GetCompressorForSST(
    const FilterBuildingContext& context, const CompressionOptions& opts,
    CompressionType preferred) {
  assert(preferred == kZSTD);
  (void)context;
  return std::make_unique<AutoSkipCompressorWrapper>(opts, preferred);
}

void AutoSkipCompressorWrapper::SetMinExplorationPercentage(
    int min_exploration_percentage) {
  min_exploration_percentage_ = min_exploration_percentage;
}
int AutoSkipCompressorWrapper::GetMinExplorationPercentage() const {
  return min_exploration_percentage_;
}
}  // namespace ROCKSDB_NAMESPACE
