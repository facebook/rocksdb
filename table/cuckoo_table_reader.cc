//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE
#include "table/cuckoo_table_reader.h"

#include <string>
#include "table/meta_blocks.h"
#include "util/coding.h"

namespace rocksdb {

extern const uint64_t kCuckooTableMagicNumber;

CuckooTableReader::CuckooTableReader(
    const Options& options,
    std::unique_ptr<RandomAccessFile>&& file,
    uint64_t file_size,
    uint64_t (*GetSliceHashPtr)(const Slice&, uint32_t, uint64_t))
    : file_(std::move(file)),
      file_size_(file_size),
      GetSliceHash(GetSliceHashPtr) {
  if (!options.allow_mmap_reads) {
    status_ = Status::InvalidArgument("File is not mmaped");
  }
  TableProperties* props = nullptr;
  status_ = ReadTableProperties(file_.get(), file_size, kCuckooTableMagicNumber,
      options.env, options.info_log.get(), &props);
  if (!status_.ok()) {
    return;
  }
  table_props_.reset(props);
  auto& user_props = props->user_collected_properties;
  auto hash_funs = user_props.find(CuckooTablePropertyNames::kNumHashTable);
  if (hash_funs == user_props.end()) {
    status_ = Status::InvalidArgument("Number of hash functions not found");
    return;
  }
  num_hash_fun_ = *reinterpret_cast<const uint32_t*>(hash_funs->second.data());
  auto unused_key = user_props.find(CuckooTablePropertyNames::kEmptyKey);
  if (unused_key == user_props.end()) {
    status_ = Status::InvalidArgument("Empty bucket value not found");
    return;
  }
  unused_key_ = unused_key->second;

  key_length_ = props->fixed_key_len;
  auto value_length = user_props.find(CuckooTablePropertyNames::kValueLength);
  if (value_length == user_props.end()) {
    status_ = Status::InvalidArgument("Value length not found");
    return;
  }
  value_length_ = *reinterpret_cast<const uint32_t*>(
      value_length->second.data());
  bucket_length_ = key_length_ + value_length_;

  auto num_buckets = user_props.find(CuckooTablePropertyNames::kMaxNumBuckets);
  if (num_buckets == user_props.end()) {
    status_ = Status::InvalidArgument("Num buckets not found");
    return;
  }
  num_buckets_ = *reinterpret_cast<const uint64_t*>(num_buckets->second.data());
  auto is_last_level = user_props.find(CuckooTablePropertyNames::kIsLastLevel);
  if (is_last_level == user_props.end()) {
    status_ = Status::InvalidArgument("Is last level not found");
    return;
  }
  is_last_level_ = *reinterpret_cast<const bool*>(is_last_level->second.data());
  status_ = file_->Read(0, file_size, &file_data_, nullptr);
}

Status CuckooTableReader::Get(
    const ReadOptions& readOptions, const Slice& key, void* handle_context,
    bool (*result_handler)(void* arg, const ParsedInternalKey& k,
                           const Slice& v),
    void (*mark_key_may_exist_handler)(void* handle_context)) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    return Status::Corruption("Unable to parse key into inernal key.");
  }
  for (uint32_t hash_cnt = 0; hash_cnt < num_hash_fun_; ++hash_cnt) {
    uint64_t hash_val = GetSliceHash(ikey.user_key, hash_cnt, num_buckets_);
    assert(hash_val < num_buckets_);
    uint64_t offset = hash_val * bucket_length_;
    const char* bucket = &file_data_.data()[offset];
    if (unused_key_.compare(0, key_length_, bucket, key_length_) == 0) {
      return Status::OK();
    }
    // Here, we compare only the user key part as we support only one entry
    // per user key and we don't support sanpshot.
    if (ikey.user_key.compare(Slice(bucket, ikey.user_key.size())) == 0) {
      Slice value = Slice(&bucket[key_length_], value_length_);
      result_handler(handle_context, ikey, value);
      // We don't support merge operations. So, we return here.
      return Status::OK();
    }
  }
  return Status::OK();
}

Iterator* CuckooTableReader::NewIterator(const ReadOptions&, Arena* arena) {
  // TODO(rbs): Implement this as this will be used in compaction.
  return nullptr;
}
}  // namespace rocksdb
#endif
