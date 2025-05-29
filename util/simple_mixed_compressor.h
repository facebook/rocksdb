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
#include <random>

#include "compression.h"
#include "options/options_helper.h"
#include "rocksdb/advanced_compression.h"

namespace ROCKSDB_NAMESPACE {
struct SimpleMixedCompressor : public CompressorWrapper {
  using CompressorWrapper::CompressorWrapper;

  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa, bool forced) override {
    // CompressionContext* ctx = nullptr;
    // if (wa != nullptr) {
    //   ctx = static_cast<CompressionContext*>(wa->get());
    // }
    const auto& compressions = GetSupportedCompressions();
    // select compression algo in random
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(
        1, compressions.size() - 2);  // avoiding no compression and zstd
    auto selected = dis(gen);
    auto type = compressions[selected];
    fprintf(stdout, "selected compression algo: %s typeint: %d\n",
            std::to_string(type).c_str(), type);
    *out_compression_type = type;
    forced = true;
    wrapped_->CompressBlock(uncompressed_data, compressed_output,
                            out_compression_type, wa, forced);
    return Status::OK();
  }
};

class SimpleMixedCompressionManager : public CompressionManagerWrapper {
  using CompressionManagerWrapper::CompressionManagerWrapper;
  const char* Name() const override { return wrapped_->Name(); }
  std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext& context, const CompressionOptions& opts,
      CompressionType preferred) override {
    return std::make_unique<SimpleMixedCompressor>(
        wrapped_->GetCompressorForSST(context, opts, preferred));
  }
};
}  // namespace ROCKSDB_NAMESPACE
