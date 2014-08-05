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

#include <algorithm>
#include <limits>
#include <string>
#include <utility>
#include <vector>
#include "rocksdb/iterator.h"
#include "table/meta_blocks.h"
#include "util/arena.h"
#include "util/coding.h"

namespace rocksdb {

extern const uint64_t kCuckooTableMagicNumber;

CuckooTableReader::CuckooTableReader(
    const Options& options,
    std::unique_ptr<RandomAccessFile>&& file,
    uint64_t file_size,
    uint64_t (*get_slice_hash)(const Slice&, uint32_t, uint64_t))
    : file_(std::move(file)),
      get_slice_hash_(get_slice_hash) {
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
    uint64_t hash_val = get_slice_hash_(ikey.user_key, hash_cnt, num_buckets_);
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

class CuckooTableIterator : public Iterator {
 public:
  explicit CuckooTableIterator(CuckooTableReader* reader);
  ~CuckooTableIterator() {}
  bool Valid() const override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Seek(const Slice& target) override;
  void Next() override;
  void Prev() override;
  Slice key() const override;
  Slice value() const override;
  Status status() const override { return status_; }
  void LoadKeysFromReader();

 private:
  struct {
    bool operator()(const std::pair<Slice, uint32_t>& first,
        const std::pair<Slice, uint32_t>& second) const {
      return first.first.compare(second.first) < 0;
    }
  } CompareKeys;
  void PrepareKVAtCurrIdx();
  CuckooTableReader* reader_;
  Status status_;
  // Contains a map of keys to bucket_id sorted in key order.
  // We assume byte-wise comparison for key ordering.
  std::vector<std::pair<Slice, uint32_t>> key_to_bucket_id_;
  // We assume that the number of items can be stored in uint32 (4 Billion).
  uint32_t curr_key_idx_;
  Slice curr_value_;
  IterKey curr_key_;
  // No copying allowed
  CuckooTableIterator(const CuckooTableIterator&) = delete;
  void operator=(const Iterator&) = delete;
};

CuckooTableIterator::CuckooTableIterator(CuckooTableReader* reader)
  : reader_(reader),
    curr_key_idx_(std::numeric_limits<int32_t>::max()) {
  key_to_bucket_id_.clear();
  curr_value_.clear();
  curr_key_.Clear();
}

void CuckooTableIterator::LoadKeysFromReader() {
  key_to_bucket_id_.reserve(reader_->GetTableProperties()->num_entries);
  for (uint32_t bucket_id = 0; bucket_id < reader_->num_buckets_; bucket_id++) {
    Slice read_key;
    status_ = reader_->file_->Read(bucket_id * reader_->bucket_length_,
        reader_->key_length_, &read_key, nullptr);
    if (read_key != Slice(reader_->unused_key_)) {
      key_to_bucket_id_.push_back(std::make_pair(read_key, bucket_id));
    }
  }
  assert(key_to_bucket_id_.size() ==
      reader_->GetTableProperties()->num_entries);
  std::sort(key_to_bucket_id_.begin(), key_to_bucket_id_.end(), CompareKeys);
  curr_key_idx_ = key_to_bucket_id_.size();
}

void CuckooTableIterator::SeekToFirst() {
  curr_key_idx_ = 0;
  PrepareKVAtCurrIdx();
}

void CuckooTableIterator::SeekToLast() {
  curr_key_idx_ = key_to_bucket_id_.size() - 1;
  PrepareKVAtCurrIdx();
}

void CuckooTableIterator::Seek(const Slice& target) {
  // We assume that the target is an internal key. If this is last level file,
  // we need to take only the user key part to seek.
  Slice target_to_search = reader_->is_last_level_ ?
    ExtractUserKey(target) : target;
  auto seek_it = std::lower_bound(key_to_bucket_id_.begin(),
      key_to_bucket_id_.end(),
      std::make_pair(target_to_search, 0),
      CompareKeys);
  curr_key_idx_ = std::distance(key_to_bucket_id_.begin(), seek_it);
  PrepareKVAtCurrIdx();
}

bool CuckooTableIterator::Valid() const {
  return curr_key_idx_ < key_to_bucket_id_.size();
}

void CuckooTableIterator::PrepareKVAtCurrIdx() {
  if (!Valid()) {
    curr_value_.clear();
    curr_key_.Clear();
    return;
  }
  uint64_t offset = ((uint64_t) key_to_bucket_id_[curr_key_idx_].second
      * reader_->bucket_length_) + reader_->key_length_;
  status_ = reader_->file_->Read(offset, reader_->value_length_,
      &curr_value_, nullptr);
  if (reader_->is_last_level_) {
    // Always return internal key.
    curr_key_.SetInternalKey(
        key_to_bucket_id_[curr_key_idx_].first, 0, kTypeValue);
  }
}

void CuckooTableIterator::Next() {
  if (!Valid()) {
    curr_value_.clear();
    curr_key_.Clear();
    return;
  }
  ++curr_key_idx_;
  PrepareKVAtCurrIdx();
}

void CuckooTableIterator::Prev() {
  if (curr_key_idx_ == 0) {
    curr_key_idx_ = key_to_bucket_id_.size();
  }
  if (!Valid()) {
    curr_value_.clear();
    curr_key_.Clear();
    return;
  }
  --curr_key_idx_;
  PrepareKVAtCurrIdx();
}

Slice CuckooTableIterator::key() const {
  assert(Valid());
  if (reader_->is_last_level_) {
    return curr_key_.GetKey();
  } else {
    return key_to_bucket_id_[curr_key_idx_].first;
  }
}

Slice CuckooTableIterator::value() const {
  assert(Valid());
  return curr_value_;
}

Iterator* CuckooTableReader::NewIterator(const ReadOptions&, Arena* arena) {
  if (!status().ok()) {
    return NewErrorIterator(
        Status::Corruption("CuckooTableReader status is not okay."));
  }
  CuckooTableIterator* iter;
  if (arena == nullptr) {
    iter = new CuckooTableIterator(this);
  } else {
    auto iter_mem = arena->AllocateAligned(sizeof(CuckooTableIterator));
    iter = new (iter_mem) CuckooTableIterator(this);
  }
  if (iter->status().ok()) {
    iter->LoadKeysFromReader();
  }
  return iter;
}

size_t CuckooTableReader::ApproximateMemoryUsage() const { return 0; }

}  // namespace rocksdb
#endif
