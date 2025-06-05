// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#pragma once
#include <memory>
#include <mutex>
#include <vector>

#include "compression.h"
#include "rocksdb/advanced_compression.h"

namespace ROCKSDB_NAMESPACE {

class MultiCompressorWrapper : public Compressor {
 public:
  explicit MultiCompressorWrapper(const CompressionOptions& opts,
                                  CompressionType type,
                                  CompressionDict&& dict = {});

  size_t GetMaxSampleSizeIfWantDict(CacheEntryRole block_type) const override;
  Slice GetSerializedDict() const override;
  CompressionType GetPreferredCompressionType() const override;
  ManagedWorkingArea ObtainWorkingArea() override;
  std::unique_ptr<Compressor> MaybeCloneSpecialized(
      CacheEntryRole block_type, DictSampleArgs&& dict_samples) override;

 protected:
  std::vector<std::unique_ptr<Compressor>> compressors_;

 private:
  mutable std::mutex mutex_;
};

struct SimpleMixedCompressor : public MultiCompressorWrapper {
  using MultiCompressorWrapper::MultiCompressorWrapper;
  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override;
};

class SimpleMixedCompressionManager : public CompressionManagerWrapper {
  using CompressionManagerWrapper::CompressionManagerWrapper;
  const char* Name() const override;
  std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext& context, const CompressionOptions& opts,
      CompressionType preferred) override;
};

struct RoundRobinCompressor : public MultiCompressorWrapper {
  using MultiCompressorWrapper::MultiCompressorWrapper;
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
