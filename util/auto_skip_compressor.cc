//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Creates mixed compressor wrapper which uses multiple compression algorithm
// within same SST file.

#include "util/auto_skip_compressor.h"

#include <options/options_helper.h>
#include <stdio.h>

#include "rocksdb/advanced_compression.h"
#include "util/random.h"
namespace ROCKSDB_NAMESPACE {

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
  attempted_compression_count_++;
  if (attempted_compression_count_ >= kWindowSize) {
    pred_rejection_percentage_ = static_cast<int>(
        rejected_count_ * 100 / (compressed_count_ + rejected_count_));
    // fprintf(stdout,
    //         "[CompressionRejectionProbabilityPredictor::Record] changed "
    //         "pred_rejection_percentage_: %d\n",
    //         pred_rejection_percentage_);
    attempted_compression_count_ = 0;
    compressed_count_ = 0;
    rejected_count_ = 0;
  }
  return true;
}
AutoSkipCompressorWrapper::AutoSkipCompressorWrapper(
    std::unique_ptr<Compressor> compressor, const CompressionOptions& opts)
    : CompressorWrapper::CompressorWrapper(std::move(compressor)),
      kExplorationPercentage(10),
      opts_(opts),
      rnd_(331),
      predictor_(
          std::make_shared<CompressionRejectionProbabilityPredictor>(100)) {}

Status AutoSkipCompressorWrapper::CompressBlock(
    Slice uncompressed_data, std::string* compressed_output,
    CompressionType* out_compression_type, ManagedWorkingArea* wa) {
  const int kProbabilityCutOff = 50;
  std::lock_guard<std::mutex> lock(mutex_);
  if (rnd_.PercentTrue(kExplorationPercentage)) {
    // fprintf(
    //     stdout,
    //     "[AutoSkipCompressorWrapper::CompressBlock] selected:
    //     exploration\n");
    Status status = wrapped_->CompressBlock(
        uncompressed_data, compressed_output, out_compression_type, wa);
    // check the value of the out_compression_type and compressed_output to
    // determine if it was rejected or compressed
    predictor_->Record(uncompressed_data, compressed_output, opts_);
    return status;
  } else {
    auto prediction = predictor_->Predict();
    // fprintf(stdout,
    //         "[AutoSkipCompressorWrapper::CompressBlock] selected: exploit "
    //         "pred_rejection: %d\n",
    //         prediction);
    if (prediction < kProbabilityCutOff) {
      // decide to compress
      Status status = wrapped_->CompressBlock(
          uncompressed_data, compressed_output, out_compression_type, wa);
      // determine if it was rejected or compressed
      predictor_->Record(uncompressed_data, compressed_output, opts_);
      return status;
    } else {
      // bypassed compression
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
  return std::make_unique<AutoSkipCompressorWrapper>(
      wrapped_->GetCompressorForSST(context, opts, preferred), opts);
}

void AutoSkipCompressorWrapper::TEST_SetMinExplorationPercentage(
    int exploration_percentage) {
  kExplorationPercentage = exploration_percentage;
}
int AutoSkipCompressorWrapper::TEST_GetMinExplorationPercentage() const {
  return kExplorationPercentage;
}
}  // namespace ROCKSDB_NAMESPACE
