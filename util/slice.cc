//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/slice.h"

#include <stdio.h>

#include <algorithm>

#include "options/customizable_helper.h"
#include "rocksdb/convenience.h"
#include "rocksdb/slice_transform.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class FixedPrefixTransform : public SliceTransform {
 private:
  size_t prefix_len_;

 public:
  explicit FixedPrefixTransform(size_t prefix_len) : prefix_len_(prefix_len) {}

  static const char* kClassName() { return "rocksdb.FixedPrefix"; }
  const char* Name() const override { return kClassName(); }
  std::string GetId() const override {
    return std::string(Name()) + "." + ROCKSDB_NAMESPACE::ToString(prefix_len_);
  }

  Slice Transform(const Slice& src) const override {
    assert(InDomain(src));
    return Slice(src.data(), prefix_len_);
  }

  bool InDomain(const Slice& src) const override {
    return (src.size() >= prefix_len_);
  }

  bool InRange(const Slice& dst) const override {
    return (dst.size() == prefix_len_);
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

 public:
  explicit CappedPrefixTransform(size_t cap_len) : cap_len_(cap_len) {}

  static const char* kClassName() { return "rocksdb.CappedPrefix"; }
  const char* Name() const override { return kClassName(); }
  std::string GetId() const override {
    return std::string(Name()) + "." + ROCKSDB_NAMESPACE::ToString(cap_len_);
  }

  Slice Transform(const Slice& src) const override {
    assert(InDomain(src));
    return Slice(src.data(), std::min(cap_len_, src.size()));
  }

  bool InDomain(const Slice& /*src*/) const override { return true; }

  bool InRange(const Slice& dst) const override {
    return (dst.size() <= cap_len_);
  }

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
  explicit NoopTransform() { }

  static const char* kClassName() { return "rocksdb.Noop"; }
  const char* Name() const override { return kClassName(); }

  Slice Transform(const Slice& src) const override { return src; }

  bool InDomain(const Slice& /*src*/) const override { return true; }

  bool InRange(const Slice& /*dst*/) const override { return true; }

  bool SameResultWhenAppended(const Slice& /*prefix*/) const override {
    return false;
  }
};

static bool ParseSliceTransformHelper(
    const std::string& kFixedPrefixName, const std::string& kCappedPrefixName,
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform) {
  const char* no_op_name = "rocksdb.Noop";
  size_t no_op_length = strlen(no_op_name);
  auto& pe_value = value;
  if (value.empty() || value == kNullptrString) {
    slice_transform->reset();
  } else if (pe_value.size() > kFixedPrefixName.size() &&
             pe_value.compare(0, kFixedPrefixName.size(), kFixedPrefixName) ==
                 0) {
    int prefix_length = ParseInt(trim(value.substr(kFixedPrefixName.size())));
    slice_transform->reset(new FixedPrefixTransform(prefix_length));
  } else if (pe_value.size() > kCappedPrefixName.size() &&
             pe_value.compare(0, kCappedPrefixName.size(), kCappedPrefixName) ==
                 0) {
    int prefix_length =
        ParseInt(trim(pe_value.substr(kCappedPrefixName.size())));
    slice_transform->reset(new CappedPrefixTransform(prefix_length));
  } else if (pe_value.size() == no_op_length &&
             pe_value.compare(0, no_op_length, no_op_name) == 0) {
    slice_transform->reset(new NoopTransform());
  } else {
    return false;
  }

  return true;
}

static bool ParseSliceTransform(
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform) {
  // While we normally don't convert the string representation of a
  // pointer-typed option into its instance, here we do so for backward
  // compatibility as we allow this action in SetOption().

  // TODO(yhchiang): A possible better place for these serialization /
  // deserialization is inside the class definition of pointer-typed
  // option itself, but this requires a bigger change of public API.
  bool result =
      ParseSliceTransformHelper("fixed:", "capped:", value, slice_transform);
  if (result) {
    return result;
  }
  result = ParseSliceTransformHelper(
      "rocksdb.FixedPrefix.", "rocksdb.CappedPrefix.", value, slice_transform);
  if (result) {
    return result;
  }
  // TODO(yhchiang): we can further support other default
  //                 SliceTransforms here.
  return false;
}
}  // end namespace

const SliceTransform* NewFixedPrefixTransform(size_t prefix_len) {
  return new FixedPrefixTransform(prefix_len);
}

const SliceTransform* NewCappedPrefixTransform(size_t cap_len) {
  return new CappedPrefixTransform(cap_len);
}

const SliceTransform* NewNoopTransform() { return new NoopTransform; }

Status SliceTransform::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<const SliceTransform>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status =
      ConfigurableHelper::GetOptionsMap(value, result->get(), &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else if (opt_map.empty() && ParseSliceTransform(id, result)) {
    return Status::OK();
  } else {
    std::string curr_opts;
#ifndef ROCKSDB_LITE
    if (result->get() != nullptr && result->get()->GetId() == id) {
      // Try to get the existing options, ignoring any errors
      ConfigOptions embedded = config_options;
      embedded.delimiter = ";";
      (*result)->GetOptionString(embedded, &curr_opts).PermitUncheckedError();
    }
    status = config_options.registry->NewSharedObject(id, result);
#else
    status = Status::NotSupported("Cannot load object in LITE mode ", id);
#endif  // ROCKSDB_LITE
    if (!status.ok()) {
      if (config_options.ignore_unsupported_options &&
          status.IsNotSupported()) {
        return Status::OK();
      } else {
        return status;
      }
    } else if (!curr_opts.empty() || !opt_map.empty()) {
      SliceTransform* transform = const_cast<SliceTransform*>(result->get());
      status = ConfigurableHelper::ConfigureNewObject(config_options, transform,
                                                      id, curr_opts, opt_map);
    }
  }
  return status;
}

std::string SliceTransform::AsString() const {
#ifndef ROCKSDB_LITE
  ConfigOptions config_options;
  config_options.delimiter = ";";
  return ToString(config_options);
#else
  return GetId();
#endif // ROCKSDB_LITE
}

// 2 small internal utility functions, for efficient hex conversions
// and no need for snprintf, toupper etc...
// Originally from wdt/util/EncryptionUtils.cpp - for ToString(true)/DecodeHex:
char toHex(unsigned char v) {
  if (v <= 9) {
    return '0' + v;
  }
  return 'A' + v - 10;
}
// most of the code is for validation/error check
int fromHex(char c) {
  // toupper:
  if (c >= 'a' && c <= 'f') {
    c -= ('a' - 'A');  // aka 0x20
  }
  // validation
  if (c < '0' || (c > '9' && (c < 'A' || c > 'F'))) {
    return -1;  // invalid not 0-9A-F hex char
  }
  if (c <= '9') {
    return c - '0';
  }
  return c - 'A' + 10;
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
  std::string result;  // RVO/NRVO/move
  if (hex) {
    result.reserve(2 * size_);
    for (size_t i = 0; i < size_; ++i) {
      unsigned char c = data_[i];
      result.push_back(toHex(c >> 4));
      result.push_back(toHex(c & 0xf));
    }
    return result;
  } else {
    result.assign(data_, size_);
    return result;
  }
}

// Originally from rocksdb/utilities/ldb_cmd.h
bool Slice::DecodeHex(std::string* result) const {
  std::string::size_type len = size_;
  if (len % 2) {
    // Hex string must be even number of hex digits to get complete bytes back
    return false;
  }
  if (!result) {
    return false;
  }
  result->clear();
  result->reserve(len / 2);

  for (size_t i = 0; i < len;) {
    int h1 = fromHex(data_[i++]);
    if (h1 < 0) {
      return false;
    }
    int h2 = fromHex(data_[i++]);
    if (h2 < 0) {
      return false;
    }
    result->push_back(static_cast<char>((h1 << 4) | h2));
  }
  return true;
}

PinnableSlice::PinnableSlice(PinnableSlice&& other) {
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
