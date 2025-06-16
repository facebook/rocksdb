//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Creates mixed compressor wrapper which uses multiple compression algorithm
// within same SST file.

#include "simple_mixed_compressor.h"

#include <options/options_helper.h>

#include "random.h"
#include "rocksdb/advanced_compression.h"
namespace ROCKSDB_NAMESPACE {

// MultiCompressorWrapper implementation
MultiCompressorWrapper::MultiCompressorWrapper(const CompressionOptions& opts,
                                               CompressionType type,
                                               CompressionDict&& dict) {
  assert(type != kNoCompression);
  assert(type == kZSTD);
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

size_t MultiCompressorWrapper::GetMaxSampleSizeIfWantDict(
    CacheEntryRole block_type) const {
  return compressors_.back()->GetMaxSampleSizeIfWantDict(block_type);
}

Slice MultiCompressorWrapper::GetSerializedDict() const {
  return compressors_.back()->GetSerializedDict();
}

CompressionType MultiCompressorWrapper::GetPreferredCompressionType() const {
  return kZSTD;
}

Compressor::ManagedWorkingArea MultiCompressorWrapper::ObtainWorkingArea() {
  return compressors_.back()->ObtainWorkingArea();
}

std::unique_ptr<Compressor> MultiCompressorWrapper::MaybeCloneSpecialized(
    CacheEntryRole block_type, DictSampleArgs&& dict_samples) {
  return compressors_.back()->MaybeCloneSpecialized(block_type,
                                                    std::move(dict_samples));
}

Status RandomMixedCompressor::CompressBlock(
    Slice uncompressed_data, std::string* compressed_output,
    CompressionType* out_compression_type, ManagedWorkingArea* wa) {
  auto selected =
      Random::GetTLSInstance()->Uniform(static_cast<int>(compressors_.size()));
  auto& compressor = compressors_[selected];
  return compressor->CompressBlock(uncompressed_data, compressed_output,
                                   out_compression_type, wa);
}

const char* RandomMixedCompressionManager::Name() const {
  return wrapped_->Name();
  // return "RandomMixedCompressionManager";
}

std::unique_ptr<Compressor> RandomMixedCompressionManager::GetCompressorForSST(
    const FilterBuildingContext& context, const CompressionOptions& opts,
    CompressionType preferred) {
  assert(preferred == kZSTD);
  (void)context;
  return std::make_unique<RandomMixedCompressor>(opts, preferred);
}

// RoundRobinCompressor implementation
Status RoundRobinCompressor::CompressBlock(
    Slice uncompressed_data, std::string* compressed_output,
    CompressionType* out_compression_type, ManagedWorkingArea* wa) {
  auto counter = block_counter.FetchAddRelaxed(1);
  auto sel_idx = counter % (compressors_.size());
  auto& compressor = compressors_[sel_idx];
  return compressor->CompressBlock(uncompressed_data, compressed_output,
                                   out_compression_type, wa);
}

RelaxedAtomic<uint64_t> RoundRobinCompressor::block_counter{0};

// RoundRobinManager implementation
const char* RoundRobinManager::Name() const {
  // return "RoundRobinManager";
  return wrapped_->Name();
}

std::unique_ptr<Compressor> RoundRobinManager::GetCompressorForSST(
    const FilterBuildingContext& context, const CompressionOptions& opts,
    CompressionType preferred) {
  assert(preferred == kZSTD);
  (void)context;
  return std::make_unique<RoundRobinCompressor>(opts, preferred);
}

}  // namespace ROCKSDB_NAMESPACE
