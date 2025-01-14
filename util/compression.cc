// Copyright (c) 2022-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/compression.h"

#include "rocksdb/configurable.h"
#include "rocksdb/utilities/options_type.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {
#define PARALLEL_THREADS_TYPE_INFO                         \
  {"parallel_threads",                                     \
   {offsetof(struct CompressionOptions, parallel_threads), \
    OptionType::kUInt32T, OptionVerificationType::kNormal, \
    OptionTypeFlags::kMutable}},
#define DICTIONARY_TYPE_INFO                                               \
  {"max_dict_bytes",                                                       \
   {offsetof(struct CompressionOptions, max_dict_bytes), OptionType::kInt, \
    OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},          \
      {"max_train_bytes",                                                  \
       {offsetof(struct CompressionOptions, zstd_max_train_bytes),         \
        OptionType::kUInt32T, OptionVerificationType::kNormal,             \
        OptionTypeFlags::kMutable}},                                       \
      {"max_dict_buffer_bytes",                                            \
       {offsetof(struct CompressionOptions, max_dict_buffer_bytes),        \
        OptionType::kUInt64T, OptionVerificationType::kNormal,             \
        OptionTypeFlags::kMutable}},                                       \
      {"use_zstd_dict_trainer",                                            \
       {offsetof(struct CompressionOptions, use_zstd_dict_trainer),        \
        OptionType::kBoolean, OptionVerificationType::kNormal,             \
        OptionTypeFlags::kMutable}},
#define LEVEL_TYPE_INFO                                           \
  {"level",                                                       \
   {offsetof(struct CompressionOptions, level), OptionType::kInt, \
    OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
#define WINDOW_BITS_TYPE_INFO                                           \
  {"window_bits",                                                       \
   {offsetof(struct CompressionOptions, window_bits), OptionType::kInt, \
    OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
#define STRATEGY_TYPE_INFO                                           \
  {"strategy",                                                       \
   {offsetof(struct CompressionOptions, strategy), OptionType::kInt, \
    OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
#define CHECKSUM_INFO                                                    \
  {"checksum",                                                           \
   {offsetof(struct CompressionOptions, checksum), OptionType::kBoolean, \
    OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},

static std::unordered_map<std::string, OptionTypeInfo>
    compressor_parallel_type_info = {PARALLEL_THREADS_TYPE_INFO};

static std::unordered_map<std::string, OptionTypeInfo>
    compressor_level_type_info = {LEVEL_TYPE_INFO};

static std::unordered_map<std::string, OptionTypeInfo>
    compressor_dict_type_info = {DICTIONARY_TYPE_INFO};

static std::unordered_map<std::string, OptionTypeInfo>
    compressor_strategy_type_info = {STRATEGY_TYPE_INFO};

static std::unordered_map<std::string, OptionTypeInfo>
    compressor_window_type_info = {WINDOW_BITS_TYPE_INFO};

static std::unordered_map<std::string, OptionTypeInfo>
    compressor_checksum_type_info = {CHECKSUM_INFO};

static std::unordered_map<CompressionType, std::pair<std::string, std::string>>
    builtin_compression_types = {
        {kNoCompression,
         std::make_pair(NoCompressor::kClassName(), NoCompressor::kNickName())},
        {kSnappyCompression, std::make_pair(SnappyCompressor::kClassName(),
                                            SnappyCompressor::kNickName())},
        {kZlibCompression, std::make_pair(ZlibCompressor::kClassName(),
                                          ZlibCompressor::kNickName())},
        {kBZip2Compression, std::make_pair(BZip2Compressor::kClassName(),
                                           BZip2Compressor::kNickName())},
        {kLZ4Compression, std::make_pair(LZ4Compressor::kClassName(),
                                         LZ4Compressor::kNickName())},
        {kLZ4HCCompression, std::make_pair(LZ4HCCompressor::kClassName(),
                                           LZ4HCCompressor::kNickName())},
        {kXpressCompression, std::make_pair(XpressCompressor::kClassName(),
                                            XpressCompressor::kNickName())},
        {kZSTD, std::make_pair(ZSTDCompressor::kClassName(),
                               ZSTDCompressor::kNickName())},
        {kZSTDNotFinalCompression,
         std::make_pair(ZSTDNotFinalCompressor::kClassName(),
                        ZSTDNotFinalCompressor::kNickName())},
        {kDisableCompressionOption,
         std::make_pair("DisableOption", "kDisableCompressionOption")},
};

std::vector<std::string> Compressor::GetSupported() {
  std::vector<std::string> supported;
  for (const auto& cit : builtin_compression_types) {
    if (BuiltinCompressor::TypeSupported(cit.first)) {
      supported.push_back(cit.second.first);
    }
  }
  return supported;
}

std::vector<std::string> Compressor::GetDictSupported() {
  std::vector<std::string> supported;
  for (const auto& cit : builtin_compression_types) {
    if (BuiltinCompressor::TypeSupported(cit.first) &&
        BuiltinCompressor::TypeSupportsDict(cit.first)) {
      supported.push_back(cit.second.first);
    }
  }
  return supported;
}

std::vector<CompressionType> GetSupportedCompressions() {
  std::vector<CompressionType> supported;
  for (const auto& cit : builtin_compression_types) {
    if (BuiltinCompressor::TypeSupported(cit.first)) {
      supported.push_back(cit.first);
    }
  }
  return supported;
}

BuiltinCompressor::BuiltinCompressor() {
  RegisterOptions(&compression_opts_, &compressor_parallel_type_info);
}

bool BuiltinCompressor::TypeToString(CompressionType type, bool as_class,
                                     std::string* result) {
  const auto cit = builtin_compression_types.find(type);
  if (cit != builtin_compression_types.end()) {
    *result = (as_class) ? cit->second.first : cit->second.second;
    return true;
  } else {
    *result = "";
    return false;
  }
}

std::string BuiltinCompressor::TypeToString(CompressionType type) {
  std::string result;
  bool okay __attribute__((__unused__));
  okay = TypeToString(type, true, &result);
  assert(okay);
  return result;
}

bool BuiltinCompressor::StringToType(const std::string& id,
                                     CompressionType* type) {
  for (const auto& cit : builtin_compression_types) {
    if (id == cit.second.first || id == cit.second.second) {
      *type = cit.first;
      return true;
    }
  }
  return false;
}

bool BuiltinCompressor::TypeSupported(CompressionType type) {
  switch (type) {
    case kNoCompression:
      return true;
    case kSnappyCompression:
      return Snappy_Supported();
    case kZlibCompression:
      return Zlib_Supported();
    case kBZip2Compression:
      return BZip2_Supported();
    case kLZ4Compression:
      return LZ4_Supported();
    case kLZ4HCCompression:
      return LZ4_Supported();
    case kXpressCompression:
      return XPRESS_Supported();
    case kZSTDNotFinalCompression:
      return ZSTDNotFinal_Supported();
    case kZSTD:
      return ZSTD_Supported();
    default:
      return false;
  }
}

bool BuiltinCompressor::TypeSupportsDict(CompressionType type) {
  switch (type) {
    case kNoCompression:
      return false;
    case kSnappyCompression:
      return false;
    case kZlibCompression:
      return Zlib_Supported();
    case kBZip2Compression:
      return false;
    case kLZ4Compression:
    case kLZ4HCCompression:
#if LZ4_VERSION_NUMBER >= 10400  // r124+
      return LZ4_Supported();
#else
      return false;
#endif
    case kXpressCompression:
      return false;
    case kZSTDNotFinalCompression:
#if ZSTD_VERSION_NUMBER >= 500  // v0.5.0+
      return ZSTDNotFinal_Supported();
#else
      return false;
#endif
    case kZSTD:
#if ZSTD_VERSION_NUMBER >= 500  // v0.5.0+
      return ZSTD_Supported();
#else
      return false;
#endif
    default:
      return false;
  }
}

bool BuiltinCompressor::MatchesOptions(const CompressionOptions& opts) const {
  return compression_opts_.parallel_threads == opts.parallel_threads;
}

bool BuiltinCompressor::IsInstanceOf(const std::string& id) const {
  if (id == kClassName()) {
    return true;
  } else {
    return Compressor::IsInstanceOf(id);
  }
}

const void* BuiltinCompressor::GetOptionsPtr(const std::string& name) const {
  if (name == CompressionOptions::kName()) {
    return &compression_opts_;
  } else {
    return Compressor::GetOptionsPtr(name);
  }
}

BuiltinDictCompressor::BuiltinDictCompressor() {
  RegisterOptions("dictionary", &compression_opts_, &compressor_dict_type_info);
}

bool BuiltinDictCompressor::IsInstanceOf(const std::string& id) const {
  if (id == kClassName()) {
    return true;
  } else {
    return BuiltinCompressor::IsInstanceOf(id);
  }
}

bool BuiltinDictCompressor::MatchesOptions(
    const CompressionOptions& opts) const {
  if (compression_opts_.max_dict_bytes != opts.max_dict_bytes ||
      compression_opts_.zstd_max_train_bytes != opts.zstd_max_train_bytes ||
      compression_opts_.max_dict_buffer_bytes != opts.max_dict_buffer_bytes ||
      compression_opts_.use_zstd_dict_trainer != opts.use_zstd_dict_trainer) {
    return false;
  } else {
    return BuiltinCompressor::MatchesOptions(opts);
  }
}

BZip2Compressor::BZip2Compressor() {
  // No additional options
}

#ifdef BZIP2
Status BZip2Compressor::Compress(const CompressionInfo& info,
                                 const Slice& input, std::string* output) {
  bool success = BZip2_Compress(info, info.CompressFormatVersion(),
                                input.data(), input.size(), output);
  if (!success) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status BZip2Compressor::Uncompress(const UncompressionInfo& info,
                                   const char* input, size_t input_length,
                                   char** output, size_t* output_length) {
  *output = BZip2_Uncompress(info, input, input_length, output_length);
  if (!*output) {
    return Status::Corruption();
  }
  return Status::OK();
}

#endif  // BZIP2

LZ4Compressor::LZ4Compressor() {
  RegisterOptions("level", &compression_opts_, &compressor_level_type_info);
}

bool LZ4Compressor::MatchesOptions(const CompressionOptions& opts) const {
  if (compression_opts_.level != opts.level) {
    return false;
  } else {
    return BuiltinDictCompressor::MatchesOptions(opts);
  }
}

bool LZ4Compressor::DictCompressionSupported() const {
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  return true;
#else
  return false;
#endif
}

#ifdef LZ4
Status LZ4Compressor::Compress(const CompressionInfo& info, const Slice& input,
                               std::string* output) {
  bool success =
      LZ4_Compress(info, compression_opts_, input.data(), input.size(), output);
  if (UNLIKELY(!success)) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status LZ4Compressor::Uncompress(const UncompressionInfo& info,
                                 const char* input, size_t input_length,
                                 char** output, size_t* output_length) {
  *output = LZ4_Uncompress(info, input, input_length, output_length);
  if (UNLIKELY(!*output)) {
    return Status::Corruption();
  }
  return Status::OK();
}
#endif  // LZ4

LZ4HCCompressor::LZ4HCCompressor() {
  RegisterOptions("level", &compression_opts_, &compressor_level_type_info);
}

bool LZ4HCCompressor::MatchesOptions(const CompressionOptions& opts) const {
  if (compression_opts_.level != opts.level) {
    return false;
  } else {
    return BuiltinDictCompressor::MatchesOptions(opts);
  }
}

bool LZ4HCCompressor::DictCompressionSupported() const {
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  return true;
#else
  return false;
#endif
}

#ifdef LZ4
Status LZ4HCCompressor::Compress(const CompressionInfo& info,
                                 const Slice& input, std::string* output) {
  bool success = LZ4HC_Compress(info, compression_opts_, input.data(),
                                input.size(), output);
  if (UNLIKELY(!success)) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status LZ4HCCompressor::Uncompress(const UncompressionInfo& info,
                                   const char* input, size_t input_length,
                                   char** output, size_t* output_length) {
  *output = LZ4_Uncompress(info, input, input_length, output_length);
  if (UNLIKELY(!*output)) {
    return Status::Corruption();
  }
  return Status::OK();
}
#endif  // LZ4

SnappyCompressor::SnappyCompressor() {
  // No additional options
}

#ifdef SNAPPY
Status SnappyCompressor::Compress(const CompressionInfo& info,
                                  const Slice& input, std::string* output) {
  bool success = Snappy_Compress(info, input.data(), input.size(), output);
  if (!success) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status SnappyCompressor::Uncompress(const UncompressionInfo& info,
                                    const char* input, size_t input_length,
                                    char** output, size_t* output_length) {
  *output = Snappy_Uncompress(input, input_length, output_length,
                              info.GetMemoryAllocator());
  if (!*output) {
    return Status::Corruption();
  } else {
    return Status::OK();
  }
}
#endif  // SNAPPY

XpressCompressor::XpressCompressor() {
  // No additional options
}

#ifdef XPRESS
Status XpressCompressor::Compress(const CompressionInfo& /*info*/,
                                  const Slice& input, std::string* output) {
  bool success = XPRESS_Compress(input.data(), input.size(), output);
  if (!success) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status XpressCompressor::Uncompress(const UncompressionInfo& /*info*/,
                                    const char* input, size_t input_length,
                                    char** output, size_t* output_length) {
  // XPRESS allocates memory internally, thus no support for custom allocator.
  *output = XPRESS_Uncompress(input, input_length, output_length);
  if (!*output) {
    return Status::Corruption();
  }
  return Status::OK();
}
#endif  // XPRESS

ZlibCompressor::ZlibCompressor() {
  RegisterOptions("level", &compression_opts_, &compressor_level_type_info);
  RegisterOptions("window", &compression_opts_, &compressor_window_type_info);
  RegisterOptions("strategy", &compression_opts_,
                  &compressor_strategy_type_info);
}

bool ZlibCompressor::MatchesOptions(const CompressionOptions& opts) const {
  if (compression_opts_.level != opts.level) {
    return false;
  } else if (compression_opts_.window_bits != opts.window_bits ||
             compression_opts_.strategy != opts.strategy) {
    return false;
  } else {
    return BuiltinDictCompressor::MatchesOptions(opts);
  }
}
#ifdef ZLIB
Status ZlibCompressor::Compress(const CompressionInfo& info, const Slice& input,
                                std::string* output) {
  bool success = Zlib_Compress(info, compression_opts_, input.data(),
                               input.size(), output);
  if (!success) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status ZlibCompressor::Uncompress(const UncompressionInfo& info,
                                  const char* input, size_t input_length,
                                  char** output, size_t* output_length) {
  *output = Zlib_Uncompress(info, input, input_length, output_length);
  if (!*output) {
    return Status::Corruption();
  }
  return Status::OK();
}
#endif  // ZLIB

ZSTDCompressor::ZSTDCompressor() {
  RegisterOptions("level", &compression_opts_, &compressor_level_type_info);
  RegisterOptions("checksum", &compression_opts_,
                  &compressor_checksum_type_info);
}

bool ZSTDCompressor::MatchesOptions(const CompressionOptions& opts) const {
  if (compression_opts_.level != opts.level) {
    return false;
  } else {
    return BuiltinDictCompressor::MatchesOptions(opts);
  }
}

bool ZSTDCompressor::DictCompressionSupported() const {
#if ZSTD_VERSION_NUMBER >= 500  // v0.5.0+
  return true;
#else
  return false;
#endif
}

#ifdef ZSTD
thread_local ZSTDContext ZSTDCompressor::zstd_context_;

Status ZSTDCompressor::Compress(const CompressionInfo& info, const Slice& input,
                                std::string* output) {
  auto length = input.size();
  if (length > std::numeric_limits<uint32_t>::max()) {
    // Can't compress more than 4GB
    return Status::Corruption("ZSTD:  Cannot compress more than 4GB");
  }

  size_t output_header_len = compression::PutDecompressedSizeInfo(
      output, static_cast<uint32_t>(length));

  size_t compressBound = ZSTD_compressBound(length);
  output->resize(static_cast<size_t>(output_header_len + compressBound));
  size_t outlen = 0;
#if ZSTD_VERSION_NUMBER >= 500  // v0.5.0+
  ZSTD_CCtx* context = zstd_context_.GetCompressionContext(
      this, compression_opts_.level, compression_opts_.checksum);
  assert(context != nullptr);
#ifdef ZSTD_ADVANCED
  if (info.dict().GetProcessedDict() != nullptr) {
    ZSTD_CCtx_refCDict(
        context, reinterpret_cast<ZSTD_CDict*>(info.dict().GetProcessedDict()));
  } else {
    ZSTD_CCtx_loadDictionary(context, info.dict().GetRawDict().data(),
                             info.dict().GetRawDict().size());
  }

  // Compression level is set in `contex` during CreateNativeContext()
  outlen = ZSTD_compress2(context, &(*output)[output_header_len], compressBound,
                          input.data(), length);
#else                           // ZSTD_ADVANCED
#if ZSTD_VERSION_NUMBER >= 700  // v0.7.0+
  if (info.dict().GetProcessedDict() != nullptr) {
    outlen = ZSTD_compress_usingCDict(
        context, &(*output)[output_header_len], compressBound, input.data(),
        length, reinterpret_cast<ZSTD_CDict*>(info.dict().GetProcessedDict()));
  }
#endif                          // ZSTD_VERSION_NUMBER >= 700
  // TODO (cbi): error handling for compression.
  if (outlen == 0) {
    int level;
    if (info.options().level == CompressionOptions::kDefaultCompressionLevel) {
      // 3 is the value of ZSTD_CLEVEL_DEFAULT (not exposed publicly), see
      // https://github.com/facebook/zstd/issues/1148
      level = 3;
    } else {
      level = info.options().level;
    }
    outlen = ZSTD_compress_usingDict(context, &(*output)[output_header_len],
                                     compressBound, input.data(), length,
                                     info.dict().GetRawDict().data(),
                                     info.dict().GetRawDict().size(), level);
  }
#endif                          // ZSTD_ADVANCED
#else                           // up to v0.4.x
  outlen = ZSTD_compress(&(*output)[output_header_len], compressBound,
                         input.data(), length, level);
#endif                          // ZSTD_VERSION_NUMBER >= 500
  if (outlen == 0) {
    return Status::Corruption();
  } else {
    output->resize(output_header_len + outlen);
    return Status::OK();
  }
}

Status ZSTDCompressor::Uncompress(const UncompressionInfo& info,
                                  const char* input, size_t input_length,
                                  char** uncompressed,
                                  size_t* uncompressed_length) {
  static const char* const kErrorDecodeOutputSize =
      "Cannot decode output size.";
  static const char* const kErrorOutputLenMismatch =
      "Decompressed size does not match header.";
  uint32_t output_len = 0;
  if (!compression::GetDecompressedSizeInfo(&input, &input_length,
                                            &output_len)) {
    return Status::Corruption(kErrorDecodeOutputSize);
  }

  auto output = Allocate(output_len, info.GetMemoryAllocator());
  size_t actual_output_length = 0;
#if ZSTD_VERSION_NUMBER >= 500  // v0.5.0+
  ZSTD_DCtx* context = zstd_context_.GetUncompressionContext(this);
  assert(context != nullptr);
#ifdef ROCKSDB_ZSTD_DDICT
  if (info.dict().GetProcessedDict() != nullptr) {
    actual_output_length = ZSTD_decompress_usingDDict(
        context, output, output_len, input, input_length,
        reinterpret_cast<const ZSTD_DDict*>(info.dict().GetProcessedDict()));
  } else {
#endif  // ROCKSDB_ZSTD_DDICT
    actual_output_length = ZSTD_decompress_usingDict(
        context, output, output_len, input, input_length,
        info.dict().GetRawDict().data(), info.dict().GetRawDict().size());
#ifdef ROCKSDB_ZSTD_DDICT
  }
#endif  // ROCKSDB_ZSTD_DDICT
#else   // up to v0.4.x
  actual_output_length =
      ZSTD_decompress(output, output_len, input, input_length);
#endif  // ZSTD_VERSION_NUMBER >= 500
  if (ZSTD_isError(actual_output_length)) {
    Deallocate(output, info.GetMemoryAllocator());
    return Status::Corruption(ZSTD_getErrorName(actual_output_length));
  } else if (actual_output_length != output_len) {
    Deallocate(output, info.GetMemoryAllocator());
    return Status::Corruption(kErrorOutputLenMismatch);
  }

  *uncompressed_length = actual_output_length;
  *uncompressed = output;
  return Status::OK();
}
#endif  // ZSTD

#if ZSTD_VERSION_NUMBER >= 700
class ZSTDCompressionDict : public ProcessedDict {
 private:
  ZSTD_CDict* cdict_ = nullptr;

 public:
  ZSTDCompressionDict(const std::string& dict, int level) {
    if (!dict.empty()) {
      if (level == CompressionOptions::kDefaultCompressionLevel) {
        // 3 is the value of ZSTD_CLEVEL_DEFAULT (not exposed publicly), see
        // https://github.com/facebook/zstd/issues/1148
        // TODO(cbi): ZSTD_CLEVEL_DEFAULT is exposed after
        //  https://github.com/facebook/zstd/pull/1174. Use
        //  ZSTD_CLEVEL_DEFAULT instead of hardcoding 3.
        level = 3;
      }
      // Should be safe (but slower) if below call fails as we'll use the
      // raw dictionary to compress.
      cdict_ = ZSTD_createCDict(dict.data(), dict.size(), level);
      assert(cdict_ != nullptr);
    }
  }

  ~ZSTDCompressionDict() override {
    if (cdict_ != nullptr) {
      auto res = ZSTD_freeCDict(cdict_);
      assert(res == 0);  // Last I checked they can't fail
      (void)res;         // prevent unused var warning
    }
  }
  void* Data() const override { return cdict_; }
};

std::unique_ptr<CompressionDict> ZSTDCompressor::NewCompressionDict(
    const std::string& dict) {
  std::unique_ptr<ProcessedDict> zstd_cdict(
      new ZSTDCompressionDict(dict, compression_opts_.level));
  return std::make_unique<CompressionDict>(dict, std::move(zstd_cdict));
}

class ZSTDUncompressionDict : public ProcessedDict {
 private:
  ZSTD_DDict* ddict_ = nullptr;

 public:
  explicit ZSTDUncompressionDict(const Slice& slice) {
    if (!slice.empty()) {
#ifdef ROCKSDB_ZSTD_DDICT
      ddict_ = ZSTD_createDDict_byReference(slice.data(), slice.size());
      assert(ddict_ != nullptr);
#else
      //**TODO: Should this use ZSTD_CreateDDict?
      // ddict_ = ZSTD_createDDict(slice.data(), slice.size());
      // assert(ddict_ != nullptr);
#endif
    }
  }

  ~ZSTDUncompressionDict() override {
    if (ddict_ != nullptr) {
      auto res = ZSTD_freeDDict(ddict_);
      assert(res == 0);  // Last I checked they can't fail
      (void)res;         // prevent unused var warning
    }
  }
  void* Data() const override { return ddict_; }
#ifdef ROCKSDB_ZSTD_DDICT
  size_t Size() const override { return ZSTD_sizeof_DDict(ddict_); }
#endif
};

UncompressionDict* ZSTDCompressor::NewUncompressionDict(
    const std::string& dict) {
  std::unique_ptr<ProcessedDict> processed(new ZSTDUncompressionDict(dict));
  return new UncompressionDict(dict, std::move(processed));
}

UncompressionDict* ZSTDCompressor::NewUncompressionDict(
    const Slice& slice, CacheAllocationPtr&& allocation) {
  std::unique_ptr<ProcessedDict> processed(new ZSTDUncompressionDict(slice));
  return new UncompressionDict(slice, std::move(allocation),
                               std::move(processed));
}
#endif  // ZSTD_VERSION_NUMBER >= 700

StreamingCompress* StreamingCompress::Create(CompressionType compression_type,
                                             const CompressionOptions& opts,
                                             uint32_t compress_format_version,
                                             size_t max_output_len) {
  switch (compression_type) {
    case kZSTD: {
      if (!ZSTD_Streaming_Supported()) {
        return nullptr;
      }
      return new ZSTDStreamingCompress(opts, compress_format_version,
                                       max_output_len);
    }
    default:
      return nullptr;
  }
}

StreamingUncompress* StreamingUncompress::Create(
    CompressionType compression_type, uint32_t compress_format_version,
    size_t max_output_len) {
  switch (compression_type) {
    case kZSTD: {
      if (!ZSTD_Streaming_Supported()) {
        return nullptr;
      }
      return new ZSTDStreamingUncompress(compress_format_version,
                                         max_output_len);
    }
    default:
      return nullptr;
  }
}

int ZSTDStreamingCompress::Compress(const char* input, size_t input_size,
                                    char* output, size_t* output_pos) {
  assert(input != nullptr && output != nullptr && output_pos != nullptr);
  *output_pos = 0;
  // Don't need to compress an empty input
  if (input_size == 0) {
    return 0;
  }
#ifndef ZSTD_ADVANCED
  (void)input;
  (void)input_size;
  (void)output;
  return -1;
#else
  if (input_buffer_.src == nullptr || input_buffer_.src != input) {
    // New input
    // Catch errors where the previous input was not fully decompressed.
    assert(input_buffer_.pos == input_buffer_.size);
    input_buffer_ = {input, input_size, /*pos=*/0};
  } else if (input_buffer_.src == input) {
    // Same input, not fully compressed.
  }
  ZSTD_outBuffer output_buffer = {output, max_output_len_, /*pos=*/0};
  const size_t remaining =
      ZSTD_compressStream2(cctx_, &output_buffer, &input_buffer_, ZSTD_e_end);
  if (ZSTD_isError(remaining)) {
    // Failure
    Reset();
    return -1;
  }
  // Success
  *output_pos = output_buffer.pos;
  return (int)remaining;
#endif
}

void ZSTDStreamingCompress::Reset() {
#ifdef ZSTD_ADVANCED
  ZSTD_CCtx_reset(cctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
#endif
}

int ZSTDStreamingUncompress::Uncompress(const char* input, size_t input_size,
                                        char* output, size_t* output_pos) {
  assert(output != nullptr && output_pos != nullptr);
  *output_pos = 0;
  // Don't need to uncompress an empty input
  if (input_size == 0) {
    return 0;
  }
#ifdef ZSTD_ADVANCED
  if (input) {
    // New input
    input_buffer_ = {input, input_size, /*pos=*/0};
  }
  ZSTD_outBuffer output_buffer = {output, max_output_len_, /*pos=*/0};
  size_t ret = ZSTD_decompressStream(dctx_, &output_buffer, &input_buffer_);
  if (ZSTD_isError(ret)) {
    Reset();
    return -1;
  }
  *output_pos = output_buffer.pos;
  return (int)(input_buffer_.size - input_buffer_.pos);
#else
  (void)input;
  (void)input_size;
  (void)output;
  return -1;
#endif
}

void ZSTDStreamingUncompress::Reset() {
#ifdef ZSTD_ADVANCED
  ZSTD_DCtx_reset(dctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
#endif
}

}  // namespace ROCKSDB_NAMESPACE
