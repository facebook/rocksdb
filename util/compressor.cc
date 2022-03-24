// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#include "options/cf_options.h"
#include "rocksdb/utilities/customizable_util.h"
#include "util/compression.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
UncompressionDict::UncompressionDict(const std::string& dict,
                                     std::unique_ptr<ProcessedDict>&& processed)
    : dict_(std::move(dict)), slice_(dict_), processed_(std::move(processed)) {}

UncompressionDict::UncompressionDict(const Slice& slice,
                                     CacheAllocationPtr&& allocation,
                                     std::unique_ptr<ProcessedDict>&& processed)
    : allocation_(std::move(allocation)),
      slice_(std::move(slice)),
      processed_(std::move(processed)) {}

UncompressionDict::UncompressionDict(UncompressionDict&& rhs) noexcept
    : dict_(std::move(rhs.dict_)),
      allocation_(std::move(rhs.allocation_)),
      slice_(std::move(rhs.slice_)),
      processed_(std::move(rhs.processed_)) {}

UncompressionDict& UncompressionDict::operator=(UncompressionDict&& rhs) {
  if (this == &rhs) {
    return *this;
  }

  dict_ = std::move(rhs.dict_);
  allocation_ = std::move(rhs.allocation_);
  slice_ = std::move(rhs.slice_);
  processed_ = std::move(rhs.processed_);
  return *this;
}

size_t UncompressionDict::ApproximateMemoryUsage() const {
  size_t usage = sizeof(UncompressionDict);
  usage += dict_.size();
  if (allocation_) {
    auto allocator = allocation_.get_deleter().allocator;
    if (allocator) {
      usage += allocator->UsableSize(allocation_.get(), slice_.size());
    } else {
      usage += slice_.size();
    }
  }
  if (processed_) {
    usage += processed_->Size();
  }
  return usage;
}

// Map built-in Compressor names to constants in CompressionType
static std::unordered_map<std::string, CompressionType> builtin_compressors{
    {NoCompressor::kClassName(), kNoCompression},
    {SnappyCompressor::kClassName(), kSnappyCompression},
    {ZlibCompressor::kClassName(), kZlibCompression},
    {BZip2Compressor::kClassName(), kBZip2Compression},
    {LZ4Compressor::kClassName(), kLZ4Compression},
    {LZ4HCCompressor::kClassName(), kLZ4HCCompression},
    {XpressCompressor::kClassName(), kXpressCompression},
    {ZSTDCompressor::kClassName(), kZSTD},
    {ZSTDNotFinalCompressor::kClassName(), kZSTDNotFinalCompression}};

std::mutex Compressor::mutex_;
std::unordered_map<std::string, std::vector<std::weak_ptr<Compressor>>>
    Compressor::compressors_;

template <typename T>
bool CreateIfMatches(const std::string& id, std::shared_ptr<Compressor>* c) {
  if (id == T::kClassName() || id == T::kNickName()) {
    c->reset(new T());
    return true;
  } else {
    return false;
  }
}

static Status NewCompressor(const ConfigOptions& /*config_options*/,
                            const std::string& id,
                            std::shared_ptr<Compressor>* result) {
  if (CreateIfMatches<NoCompressor>(id, result) ||
      CreateIfMatches<SnappyCompressor>(id, result) ||
      CreateIfMatches<ZlibCompressor>(id, result) ||
      CreateIfMatches<BZip2Compressor>(id, result) ||
      CreateIfMatches<LZ4Compressor>(id, result) ||
      CreateIfMatches<LZ4HCCompressor>(id, result) ||
      CreateIfMatches<XpressCompressor>(id, result) ||
      CreateIfMatches<ZSTDCompressor>(id, result) ||
      CreateIfMatches<ZSTDNotFinalCompressor>(id, result)) {
    return Status::OK();
  } else {
    return Status::NotSupported("Cannot find compressor ", id);
  }
}

Status Compressor::CreateFromString(const ConfigOptions& config_options,
                                    const std::string& value,
                                    std::shared_ptr<Compressor>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(config_options, result->get(),
                                              value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else if (value.empty() || value == kNullptrString) {
    result->reset();
    return Status::OK();
  } else if (id.empty()) {
    return Status::NotSupported("Cannot reset object ", value);
  } else {
    // For the builtins, always try to create based on the class name,
    // not the nickname
    CompressionType type;
    if (BuiltinCompressor::StringToType(id, &type)) {
      id = BuiltinCompressor::TypeToString(type);
    }
    std::unique_lock<std::mutex> lock(mutex_);
    auto vit = compressors_.find(id);
    std::shared_ptr<Compressor> compressor;
    // There are three case.
    // 1 - When there are no existing compressors of this type
    //     In this case, we create a new compressor and add it to the collection
    // 2 - When the compressor has no options
    //     In this case, we return the first valid compressor, creating/adding
    //     a new one if none found
    // 3 - When the compressor has options
    //     In this case, we create a new compressor and see if it matches
    //     and existing one.  If so, we return the existing one.
    //     If not, we create a new one and add it to the lsit
    if (!opt_map.empty() || vit == compressors_.end() || vit->second.empty()) {
      // Either there are none in the list or there are options.  Create one
      status = NewCompressor(config_options, id, &compressor);
      if (status.ok()) {
        status = Customizable::ConfigureNewObject(config_options,
                                                  compressor.get(), opt_map);
      }
    }
    if (vit != compressors_.end() && !vit->second.empty()) {
      if (opt_map.empty()) {
        // Case 2: There are no options
        for (const auto& cit : vit->second) {
          auto other = cit.lock();
          if (other) {
            // Found a valid one.  Return it
            *result = other;
            return Status::OK();
          }
        }
        // Looped through all of them and did not find any.  Create one
        status = NewCompressor(config_options, id, &compressor);
      } else if (status.ok()) {
        // Case 3: Compressor has options
        for (const auto& cit : vit->second) {
          std::string mismatch;
          auto other = cit.lock();
          if (other && other->AreEquivalent(config_options, compressor.get(),
                                            &mismatch)) {
            // Found a matching one.  Return it
            *result = other;
            return Status::OK();
          }
        }
      }
    }
    if (status.ok()) {
      compressors_[id].push_back(compressor);
      *result = compressor;
    }
  }
  return status;
}

std::shared_ptr<Compressor> BuiltinCompressor::GetCompressor(
    CompressionType type) {
  ConfigOptions config_options;
  config_options.ignore_unknown_options = false;
  config_options.ignore_unsupported_options = false;
  config_options.invoke_prepare_options = false;
  std::string id;
  // Simple case of looking for any compressor that matches the type
  // Convert the type to an ID and then go through the create w/o options case
  if (BuiltinCompressor::TypeToString(type, true, &id)) {
    std::shared_ptr<Compressor> result;
    Status s = Compressor::CreateFromString(config_options, id, &result);
    if (s.ok()) {
      return result;
    }
  }
  return nullptr;
}

std::shared_ptr<Compressor> BuiltinCompressor::GetCompressor(
    CompressionType type, const CompressionOptions& opts) {
  std::string id;
  if (BuiltinCompressor::TypeToString(type, true, &id)) {
    // Looking for any compressor with specific options.
    // Find the ones of the proper type and compare their options to the
    // requested. If they match, return the existing one.  If not, create a new
    // one
    std::unique_lock<std::mutex> lock(mutex_);
    auto vit = compressors_.find(id);
    if (vit != compressors_.end()) {
      for (const auto& cit : vit->second) {
        std::shared_ptr<Compressor> c = cit.lock();
        if (c) {
          auto bc = c->CheckedCast<BuiltinCompressor>();
          if (bc != nullptr && bc->MatchesOptions(opts)) {
            return c;
          }
        }
      }
    }
    // We did not find an appropriate compressor in the list.  Create a new one
    std::shared_ptr<Compressor> builtin;
    ConfigOptions config_options;
    config_options.ignore_unknown_options = false;
    config_options.ignore_unsupported_options = false;
    config_options.invoke_prepare_options = false;
    Status s = NewCompressor(config_options, id, &builtin);
    if (s.ok()) {
      auto bc_opts = builtin->GetOptions<CompressionOptions>();
      assert(bc_opts != nullptr);
      if (bc_opts != nullptr) {
        *bc_opts = opts;
      }
      compressors_[id].push_back(builtin);
      return builtin;
    }
  }
  return nullptr;
}

void Compressor::SampleDict(std::vector<std::string>& data_block_buffers,
                            std::string* compression_dict_samples,
                            std::vector<size_t>* compression_dict_sample_lens) {
  uint32_t max_dict_bytes = GetMaxDictBytes();
  uint32_t max_train_bytes = GetMaxTrainBytes();

  const size_t kSampleBytes =
      max_train_bytes > 0 ? max_train_bytes : max_dict_bytes;
  const size_t kNumBlocksBuffered = data_block_buffers.size();

  // Abstract algebra teaches us that a finite cyclic group (such as the
  // additive group of integers modulo N) can be generated by a number that is
  // coprime with N. Since N is variable (number of buffered data blocks), we
  // must then pick a prime number in order to guarantee coprimeness with any N.
  //
  // One downside of this approach is the spread will be poor when
  // `kPrimeGeneratorRemainder` is close to zero or close to
  // `kNumBlocksBuffered`.
  //
  // Picked a random number between one and one trillion and then chose the
  // next prime number greater than or equal to it.
  const uint64_t kPrimeGenerator = 545055921143ull;
  // Can avoid repeated division by just adding the remainder repeatedly.
  const size_t kPrimeGeneratorRemainder = static_cast<size_t>(
      kPrimeGenerator % static_cast<uint64_t>(kNumBlocksBuffered));
  const size_t kInitSampleIdx = kNumBlocksBuffered / 2;

  size_t buffer_idx = kInitSampleIdx;
  for (size_t i = 0; i < kNumBlocksBuffered &&
                     compression_dict_samples->size() < kSampleBytes;
       ++i) {
    size_t copy_len = std::min(kSampleBytes - compression_dict_samples->size(),
                               data_block_buffers[buffer_idx].size());
    compression_dict_samples->append(data_block_buffers[buffer_idx], 0,
                                     copy_len);
    compression_dict_sample_lens->emplace_back(copy_len);

    buffer_idx += kPrimeGeneratorRemainder;
    if (buffer_idx >= kNumBlocksBuffered) {
      buffer_idx -= kNumBlocksBuffered;
    }
  }
}

std::string Compressor::TrainDict(
    const std::string& compression_dict_samples,
    const std::vector<size_t>& compression_dict_sample_lens) {
  uint32_t max_dict_bytes = GetMaxDictBytes();
  uint32_t max_train_bytes = GetMaxTrainBytes();
  int level = GetLevel();
  bool use_dict_trainer = UseDictTrainer();

  // final data block flushed, now we can generate dictionary from the samples.
  // OK if compression_dict_samples is empty, we'll just get empty dictionary.
  if (max_train_bytes > 0) {
    if (use_dict_trainer && ZSTD_TrainDictionarySupported()) {
      return ZSTD_TrainDictionary(compression_dict_samples,
                                  compression_dict_sample_lens, max_dict_bytes);
    } else if (ZSTD_FinalizeDictionarySupported()) {
      return ZSTD_FinalizeDictionary(compression_dict_samples,
                                     compression_dict_sample_lens,
                                     max_dict_bytes, level);
    } else {
      return compression_dict_samples;
    }
  } else {
    return compression_dict_samples;
  }
}

Status Compressor::CreateDict(
    std::vector<std::string>& data_block_buffers,
    std::unique_ptr<CompressionDict>* compression_dict) {
  if (!DictCompressionSupported()) {
    return Status::NotSupported();
  }

  std::string compression_dict_samples;
  std::vector<size_t> compression_dict_sample_lens;
  SampleDict(data_block_buffers, &compression_dict_samples,
             &compression_dict_sample_lens);

  std::string dict =
      TrainDict(compression_dict_samples, compression_dict_sample_lens);

  *compression_dict = NewCompressionDict(dict);
  return Status::OK();
}

bool Compressor::IsDictEnabled() const {
  return DictCompressionSupported() && (GetMaxDictBytes() > 0);
}

}  // namespace ROCKSDB_NAMESPACE
