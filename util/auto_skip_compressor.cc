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

AutoSkipCompressorWrapper::AutoSkipCompressorWrapper(
    const CompressionOptions& opts, CompressionType type,
    CompressionDict&& dict)
    : rejected_count_(0), compressed_count_(0), bypassed_count_(0), rnd_(331) {
  assert(type != kNoCompression);
  auto builtInManager = GetDefaultBuiltinCompressionManager();
  const auto& compressions = GetSupportedCompressions();
  for (auto type_ : compressions) {
    if (type_ == kNoCompression) {
      continue;
    }
    compressors_.push_back(builtInManager->GetCompressor(opts, type_));
  }
  (void)dict;
  (void)type;
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
  auto selected = kZSTD - 1;
  // exploration with pred_rejection_percentage
  if (rnd_.PercentTrue(min_exploration_percentage_)) {
    fprintf(
        stdout,
        "[AutoSkipCompressorWrapper::CompressBlock] selected: exploration\n");
    auto& compressor = compressors_[selected];
    Status status = compressor->CompressBlock(
        uncompressed_data, compressed_output, out_compression_type, wa);
    // check the value of the out_compression_type and compressed_output to
    // determine if it was rejected or compressed
    RecordUpdatePred(compressed_output, out_compression_type);
    return status;
  } else {
    fprintf(stdout,
            "[AutoSkipCompressorWrapper::CompressBlock] selected: exploit "
            "pred_rejection: %d\n",
            pred_rejection_percentage_);
    if (pred_rejection_percentage_ >= 50) {
      // decide to compress
      // check the value of the out_compression_type and compressed_output to
      auto& compressor = compressors_[selected];
      Status status = compressor->CompressBlock(
          uncompressed_data, compressed_output, out_compression_type, wa);
      // determine if it was rejected or compressed
      RecordUpdatePred(compressed_output, out_compression_type);
      return status;
    } else {
      // bypassed compression
      *compressed_output = "blah";
      *out_compression_type = kNoCompression;
      return Status::OK();
    }
  }
}

bool AutoSkipCompressorWrapper::RecordUpdatePred(
    std::string* compressed_output, CompressionType* out_compression_type) {
  if (attempted_compression_count_ >= 100) {
    // make prediction
    pred_rejection_percentage_ =
        rejected_count_ * 100 / attempted_compression_count_;
    attempted_compression_count_ = 0;
    compressed_count_ = 0;
    rejected_count_ = 0;
    bypassed_count_ = 0;
  }
  if (*out_compression_type == kNoCompression && compressed_output->empty()) {
    rejected_count_++;
  } else if (*out_compression_type == kNoCompression) {
    compressed_count_++;
  } else {
    bypassed_count_++;
  }
  attempted_compression_count_++;
  return true;
}
size_t AutoSkipCompressorWrapper::GetRejectedCount() const {
  return rejected_count_;
}

size_t AutoSkipCompressorWrapper::GetCompressedCount() const {
  return compressed_count_;
}

size_t AutoSkipCompressorWrapper::GetBypassedCount() const {
  return bypassed_count_;
}

double AutoSkipCompressorWrapper::GetPredRejectionRatio() const {
  return static_cast<double>(rejected_count_) /
         (rejected_count_ + compressed_count_ + bypassed_count_);
}

const char* AutoSkipCompressorManager::Name() const {
  return "AutoSkipCompressorManager";
}

std::unique_ptr<Compressor> AutoSkipCompressorManager::GetCompressorForSST(
    const FilterBuildingContext& context, const CompressionOptions& opts,
    CompressionType preferred) {
  assert(preferred == kZSTD);
  (void)context;
  return std::make_unique<AutoSkipCompressorWrapper>(opts, preferred);
}

}  // namespace ROCKSDB_NAMESPACE
