//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class DbStressCustomCompressionManager : public CompressionManager {
 public:
  const char* Name() const override {
    return "DbStressCustomCompressionManager";
  }
  const char* CompatibilityName() const override { return "DbStressCustom1"; }

  bool SupportsCompressionType(CompressionType type) const override {
    return default_->SupportsCompressionType(type) ||
           type == kCustomCompressionAA || type == kCustomCompressionAB ||
           type == kCustomCompressionAC;
  }

  std::unique_ptr<Compressor> GetCompressor(const CompressionOptions& opts,
                                            CompressionType type) override {
    // db_stress never specifies a custom type, so we randomly use them anyway
    // when this compression manager is used.
    std::array<CompressionType, 4> choices = {
        type, kCustomCompressionAA, kCustomCompressionAB, kCustomCompressionAC};
    type = choices[Random::GetTLSInstance()->Uniform(4)];
    switch (static_cast<unsigned char>(type)) {
      case kCustomCompressionAA:
        return std::make_unique<
            test::CompressorCustomAlg<kCustomCompressionAA>>();
      case kCustomCompressionAB:
        return std::make_unique<
            test::CompressorCustomAlg<kCustomCompressionAB>>();
      case kCustomCompressionAC:
        return std::make_unique<
            test::CompressorCustomAlg<kCustomCompressionAC>>();
      // Also support built-in compression algorithms
      default:
        return GetBuiltinV2CompressionManager()->GetCompressor(opts, type);
    }
  }

  std::shared_ptr<Decompressor> GetDecompressor() override {
    return std::make_shared<test::DecompressorCustomAlg>();
  }

  std::shared_ptr<Decompressor> GetDecompressorForTypes(
      const CompressionType* types_begin,
      const CompressionType* types_end) override {
    auto decomp = std::make_shared<test::DecompressorCustomAlg>();
    decomp->SetAllowedTypes(types_begin, types_end);
    return decomp;
  }

  static void Register();

 protected:
  std::shared_ptr<CompressionManager> default_ =
      GetBuiltinV2CompressionManager();
};

}  // namespace ROCKSDB_NAMESPACE
