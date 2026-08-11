//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/slice.h"

#include <algorithm>
#include <cstdio>

#include "port/likely.h"
#include "rocksdb/convenience.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/cast_util.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class FixedPrefixTransform : public SliceTransform {
 private:
  size_t prefix_len_;
  std::string id_;

 public:
  explicit FixedPrefixTransform(size_t prefix_len) : prefix_len_(prefix_len) {
    id_ = std::string(kClassName()) + "." + std::to_string(prefix_len_);
  }

  static const char* kClassName() { return "rocksdb.FixedPrefix"; }
  static const char* kNickName() { return "fixed"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

  bool IsInstanceOf(const std::string& name) const override {
    if (name == id_) {
      return true;
    } else if (StartsWith(name, kNickName())) {
      std::string alt_id =
          std::string(kNickName()) + ":" + std::to_string(prefix_len_);
      if (name == alt_id) {
        return true;
      }
    }
    return SliceTransform::IsInstanceOf(name);
  }

  std::string GetId() const override { return id_; }

  Slice Transform(const Slice& src) const override {
    assert(InDomain(src));
    return Slice(src.data(), prefix_len_);
  }

  bool InDomain(const Slice& src) const override {
    return (src.size() >= prefix_len_);
  }

  bool FullLengthEnabled(size_t* len) const override {
    *len = prefix_len_;
    return true;
  }

  bool SameResultWhenAppended(const Slice& prefix) const override {
    return InDomain(prefix);
  }
};

class CappedPrefixTransform : public SliceTransform {
 private:
  size_t cap_len_;
  std::string id_;

 public:
  explicit CappedPrefixTransform(size_t cap_len) : cap_len_(cap_len) {
    id_ = std::string(kClassName()) + "." + std::to_string(cap_len_);
  }

  static const char* kClassName() { return "rocksdb.CappedPrefix"; }
  static const char* kNickName() { return "capped"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }
  std::string GetId() const override { return id_; }

  bool IsInstanceOf(const std::string& name) const override {
    if (name == id_) {
      return true;
    } else if (StartsWith(name, kNickName())) {
      std::string alt_id =
          std::string(kNickName()) + ":" + std::to_string(cap_len_);
      if (name == alt_id) {
        return true;
      }
    }
    return SliceTransform::IsInstanceOf(name);
  }

  Slice Transform(const Slice& src) const override {
    assert(InDomain(src));
    return Slice(src.data(), std::min(cap_len_, src.size()));
  }

  bool InDomain(const Slice& /*src*/) const override { return true; }

  bool FullLengthEnabled(size_t* len) const override {
    *len = cap_len_;
    return true;
  }

  bool SameResultWhenAppended(const Slice& prefix) const override {
    return prefix.size() >= cap_len_;
  }
};

class NoopTransform : public SliceTransform {
 public:
  explicit NoopTransform() = default;

  static const char* kClassName() { return "rocksdb.Noop"; }
  const char* Name() const override { return kClassName(); }

  Slice Transform(const Slice& src) const override { return src; }

  bool InDomain(const Slice& /*src*/) const override { return true; }

  bool SameResultWhenAppended(const Slice& /*prefix*/) const override {
    return false;
  }
};

}  // end namespace

const SliceTransform* NewFixedPrefixTransform(size_t prefix_len) {
  return new FixedPrefixTransform(prefix_len);
}

const SliceTransform* NewCappedPrefixTransform(size_t cap_len) {
  return new CappedPrefixTransform(cap_len);
}

const SliceTransform* NewNoopTransform() { return new NoopTransform; }

static int RegisterBuiltinSliceTransform(ObjectLibrary& library,
                                         const std::string& /*arg*/) {
  // For the builtin transforms, the format is typically
  // [Name].[0-9]+ or [NickName]:[0-9]+
  library.AddFactory<const SliceTransform>(
      NoopTransform::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<const SliceTransform>* guard,
         std::string* /*errmsg*/) {
        guard->reset(NewNoopTransform());
        return guard->get();
      });
  library.AddFactory<const SliceTransform>(
      ObjectLibrary::PatternEntry(FixedPrefixTransform::kNickName(), false)
          .AddNumber(":"),
      [](const std::string& uri, std::unique_ptr<const SliceTransform>* guard,
         std::string* /*errmsg*/) {
        auto colon = uri.find(':');
        auto len = ParseSizeT(uri.substr(colon + 1));
        guard->reset(NewFixedPrefixTransform(len));
        return guard->get();
      });
  library.AddFactory<const SliceTransform>(
      ObjectLibrary::PatternEntry(FixedPrefixTransform::kClassName(), false)
          .AddNumber("."),
      [](const std::string& uri, std::unique_ptr<const SliceTransform>* guard,
         std::string* /*errmsg*/) {
        auto len = ParseSizeT(
            uri.substr(strlen(FixedPrefixTransform::kClassName()) + 1));
        guard->reset(NewFixedPrefixTransform(len));
        return guard->get();
      });
  library.AddFactory<const SliceTransform>(
      ObjectLibrary::PatternEntry(CappedPrefixTransform::kNickName(), false)
          .AddNumber(":"),
      [](const std::string& uri, std::unique_ptr<const SliceTransform>* guard,
         std::string* /*errmsg*/) {
        auto colon = uri.find(':');
        auto len = ParseSizeT(uri.substr(colon + 1));
        guard->reset(NewCappedPrefixTransform(len));
        return guard->get();
      });
  library.AddFactory<const SliceTransform>(
      ObjectLibrary::PatternEntry(CappedPrefixTransform::kClassName(), false)
          .AddNumber("."),
      [](const std::string& uri, std::unique_ptr<const SliceTransform>* guard,
         std::string* /*errmsg*/) {
        auto len = ParseSizeT(
            uri.substr(strlen(CappedPrefixTransform::kClassName()) + 1));
        guard->reset(NewCappedPrefixTransform(len));
        return guard->get();
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}

Status SliceTransform::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<const SliceTransform>* result) {
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterBuiltinSliceTransform(*(ObjectLibrary::Default().get()), "");
  });
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(config_options, result->get(),
                                              value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else if (id.empty() && opt_map.empty()) {
    result->reset();
  } else {
    status = config_options.registry->NewSharedObject(id, result);
    if (config_options.ignore_unsupported_options && status.IsNotSupported()) {
      return Status::OK();
    } else if (status.ok()) {
      SliceTransform* transform = const_cast<SliceTransform*>(result->get());
      status =
          Customizable::ConfigureNewObject(config_options, transform, opt_map);
    }
  }
  return status;
}

std::string SliceTransform::AsString() const {
  if (HasRegisteredOptions()) {
    ConfigOptions opts;
    opts.delimiter = ";";
    return ToString(opts);
  }
  return GetId();
}

Slice::Slice(const SliceParts& parts, std::string* buf) {
  size_t length = 0;
  for (int i = 0; i < parts.num_parts; ++i) {
    length += parts.parts[i].size();
  }
  buf->reserve(length);

  for (int i = 0; i < parts.num_parts; ++i) {
    buf->append(parts.parts[i].data(), parts.parts[i].size());
  }
  data_ = buf->data();
  size_ = buf->size();
}

// Return a string that contains the copy of the referenced data.
std::string Slice::ToString(bool hex) const {
  if (!hex) {
    return {data_, size_};
  }
  static constexpr char kHexChars[] = "0123456789ABCDEF";
  if constexpr (sizeof(void*) == 4) {
    if (UNLIKELY(size_ > SIZE_MAX / 2)) {
      // On 32-bit platforms, size_ > ~2GB could cause 2 * size_
      // to overflow, practically unreachable on 64-bit.
      assert(false);
      return {};
    }
  }
  std::string result(2 * size_, '\0');
  char* p = result.data();
  const unsigned char* src = lossless_cast<const unsigned char*>(data_);
  const unsigned char* end = src + size_;

  while (src != end) {
    const unsigned char c = *src++;
    *p++ = kHexChars[c >> 4];
    *p++ = kHexChars[c & 0xF];
  }
  return result;
}

namespace {
// clang-format off
constexpr uint8_t kHexLookup[256] = {
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255,  // 0-15
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255,  // 16-31
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255,  // 32-47
      0,  1,  2,  3,  4,  5,  6,  7,   8,  9,255,255,255,255,255,255,  // 48-63 ('0'-'9')
    255, 10, 11, 12, 13, 14, 15,255, 255,255,255,255,255,255,255,255,  // 64-79 ('A'-'F')
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255,  // 80-95
    255, 10, 11, 12, 13, 14, 15,255, 255,255,255,255,255,255,255,255,  // 96-111 ('a'-'f')
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255,  // 112-127
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255,  // 128-143
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255,  // 144-159
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255,  // 160-175
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255,  // 176-191
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255,  // 192-207
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255,  // 208-223
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255,  // 224-239
    255,255,255,255,255,255,255,255, 255,255,255,255,255,255,255,255   // 240-255
};
// clang-format on
}  // namespace

bool Slice::DecodeHex(std::string* result) const {
  if (!result) {
    return false;
  }
  const size_t len = size_;
  if (len % 2 != 0) {
    return false;
  }

  const size_t target_len = len / 2;
  result->resize(target_len);

  char* dst = result->data();
  const char* src = data_;

  for (size_t i = 0; i < target_len; ++i) {
    uint8_t h1 = kHexLookup[lossless_cast<uint8_t>(*src++)];
    uint8_t h2 = kHexLookup[lossless_cast<uint8_t>(*src++)];
    // Single branch check using bitwise OR
    if ((h1 | h2) >= 16) {
      result->resize(i);
      return false;
    }
    *dst++ = lossless_cast<char>(static_cast<uint8_t>((h1 << 4) | h2));
  }
  return true;
}

PinnableSlice::PinnableSlice(PinnableSlice&& other) : PinnableSlice() {
  *this = std::move(other);
}

PinnableSlice& PinnableSlice::operator=(PinnableSlice&& other) {
  if (this != &other) {
    Cleanable::Reset();
    Cleanable::operator=(std::move(other));
    size_ = other.size_;
    pinned_ = other.pinned_;
    if (pinned_) {
      data_ = other.data_;
      // When it's pinned, buf should no longer be of use.
    } else {
      if (other.buf_ == &other.self_space_) {
        self_space_ = std::move(other.self_space_);
        buf_ = &self_space_;
        data_ = buf_->data();
      } else {
        buf_ = other.buf_;
        data_ = other.data_;
      }
    }
    other.self_space_.clear();
    other.buf_ = &other.self_space_;
    other.pinned_ = false;
    other.PinSelf();
  }
  return *this;
}

}  // namespace ROCKSDB_NAMESPACE
