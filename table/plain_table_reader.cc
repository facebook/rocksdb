// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/plain_table_reader.h"

#include <unordered_map>

#include "db/dbformat.h"

#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"

#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"

#include "util/coding.h"
#include "util/hash.h"
#include "util/histogram.h"
#include "util/perf_context_imp.h"
#include "util/stop_watch.h"

namespace std {
template<>
struct hash<rocksdb::Slice> {
public:
  std::size_t operator()(rocksdb::Slice const& s) const {
    return rocksdb::Hash(s.data(), s.size(), 397);
  }
};
}

namespace rocksdb {

PlainTableReader::PlainTableReader(const EnvOptions& storage_options,
                                   uint64_t file_size, int user_key_size,
                                   int key_prefix_len) :
    soptions_(storage_options), file_size_(file_size),
    user_key_size_(user_key_size), key_prefix_len_(key_prefix_len) {
  hash_table_ = nullptr;
}

PlainTableReader::~PlainTableReader() {
  if (hash_table_ != nullptr) {
    delete[] hash_table_;
  }
}

Status PlainTableReader::Open(const Options& options,
                              const EnvOptions& soptions,
                              unique_ptr<RandomAccessFile> && file,
                              uint64_t file_size,
                              unique_ptr<TableReader>* table_reader,
                              const int user_key_size,
                              const int key_prefix_len) {
  assert(options.allow_mmap_reads);

  PlainTableReader* t = new PlainTableReader(soptions, file_size,
                                             user_key_size,
                                             key_prefix_len);
  t->file_ = std::move(file);
  t->options_ = options;
  Status s = t->PopulateIndex(file_size);
  if (!s.ok()) {
    delete t;
    return s;
  }
  table_reader->reset(t);
  return s;
}

void PlainTableReader::SetupForCompaction() {
}

bool PlainTableReader::PrefixMayMatch(const Slice& internal_prefix) {
  return true;
}

Iterator* PlainTableReader::NewIterator(const ReadOptions& options) {
  return new PlainTableIterator(this);
}

Status PlainTableReader::PopulateIndex(uint64_t file_size) {
  Slice key_slice;
  Slice key_prefix_slice;
  Slice key_suffix_slice;
  Slice value_slice;
  Slice tmp_slice;
  Slice prev_key_prefix_slice;
  uint64_t pos = 0;
  uint64_t data_offset_for_cur_prefix = 0;
  int count_prefix = 0;
  bool first = true;
  std::string prefix_sub_index;
  HistogramImpl keys_per_prefix_hist;
  std::unordered_map<Slice, uint64_t> tmp_index;

  while (pos < file_size) {
    uint64_t key_offset = pos;
    pos = Next(pos, &key_slice, &value_slice, &tmp_slice);
    key_prefix_slice = Slice(key_slice.data(), key_prefix_len_);

    if (first || prev_key_prefix_slice != key_prefix_slice) {
      if (!first) {
        if (count_prefix < 8 || key_prefix_len_ == user_key_size_) {
          tmp_index[prev_key_prefix_slice] = data_offset_for_cur_prefix;
        } else {
          tmp_index[prev_key_prefix_slice] = sub_index_.length()
              | kSubIndexMask;
          PutFixed32(&sub_index_, (count_prefix - 1) / 8 + 1);
          sub_index_.append(prefix_sub_index);
        }
        prefix_sub_index.clear();
        data_offset_for_cur_prefix = key_offset;
        keys_per_prefix_hist.Add(count_prefix);
      }
      prev_key_prefix_slice = key_prefix_slice;
      count_prefix = 1;
    } else {
      count_prefix++;
    }
    if (key_prefix_len_ < user_key_size_ && count_prefix % 8 == 1) {
      prefix_sub_index.append(key_slice.data() + key_prefix_len_,
                              user_key_size_ - key_prefix_len_);
      PutFixed64(&prefix_sub_index, key_offset);
    }

    first = false;
  }
  keys_per_prefix_hist.Add(count_prefix);
  if (count_prefix <= 2 || key_prefix_len_ == user_key_size_) {
    tmp_index[prev_key_prefix_slice] = data_offset_for_cur_prefix;
  } else {
    tmp_index[prev_key_prefix_slice] = sub_index_.length() | kSubIndexMask;
    PutFixed32(&sub_index_, (count_prefix - 1) / 8 + 1);
    sub_index_.append(prefix_sub_index);
  }

  if (hash_table_ != nullptr) {
    delete[] hash_table_;
  }
  // Make the hash table 3/5 full
  hash_table_size_ = tmp_index.size() * 1.66;
  hash_table_ = new char[GetHashTableRecordLen() * hash_table_size_];
  for (int i = 0; i < hash_table_size_; i++) {
    memcpy(GetHashTableBucketPtr(i) + key_prefix_len_, &file_size_,
           kOffsetLen);
  }

  for (auto it = tmp_index.begin(); it != tmp_index.end(); ++it) {
    int bucket = GetHashTableBucket(it->first);
    uint64_t* hash_value;
    while (true) {
      GetHashValue(bucket, &hash_value);
      if (*hash_value == file_size_) {
        break;
      }
      bucket = (bucket + 1) % hash_table_size_;
    }

    char* bucket_ptr = GetHashTableBucketPtr(bucket);
    memcpy(bucket_ptr, it->first.data(), key_prefix_len_);
    memcpy(bucket_ptr + key_prefix_len_, &it->second, kOffsetLen);
  }

  Log(options_.info_log, "Number of prefixes: %d, suffix_map length %ld",
      hash_table_size_, sub_index_.length());
  Log(options_.info_log, "Number of Keys per prefix Histogram: %s",
      keys_per_prefix_hist.ToString().c_str());

  return Status::OK();
}

inline int PlainTableReader::GetHashTableBucket(Slice key) {
  return rocksdb::Hash(key.data(), key_prefix_len_, 397) % hash_table_size_;
}

inline void PlainTableReader::GetHashValue(int bucket, uint64_t** ret_value) {
  *ret_value = (uint64_t*) (GetHashTableBucketPtr(bucket) + key_prefix_len_);
}

Status PlainTableReader::GetOffset(const Slice& target, uint64_t* offset) {
  Status s;

  int bucket = GetHashTableBucket(target);
  uint64_t* found_value;
  Slice hash_key;
  while (true) {
    GetHashValue(bucket, &found_value);
    if (*found_value == file_size_) {
      break;
    }
    GetHashKey(bucket, &hash_key);
    if (target.starts_with(hash_key)) {
      break;
    }
    bucket = (bucket + 1) % hash_table_size_;
  }

  if (*found_value == file_size_ || (*found_value & kSubIndexMask) == 0) {
    *offset = *found_value;
    return Status::OK();
  }

  uint32_t low = 0;
  uint64_t prefix_index_offset = *found_value ^ kSubIndexMask;
  uint32_t high = DecodeFixed32(sub_index_.data() + prefix_index_offset);
  uint64_t base_offset = prefix_index_offset + 4;
  char* mid_key_str = new char[target.size()];
  memcpy(mid_key_str, target.data(), target.size());
  Slice mid_key = Slice(mid_key_str, target.size());

  // The key is between (low, high). Do a binary search between it.
  while (high - low > 1) {
    uint32_t mid = (high + low) / 2;
    const char* base = sub_index_.data() + base_offset
        + (user_key_size_ - key_prefix_len_ + kOffsetLen) * mid;
    memcpy(mid_key_str + key_prefix_len_, base,
           user_key_size_ - key_prefix_len_);

    int cmp_result = options_.comparator->Compare(target, mid_key);
    if (cmp_result > 0) {
      low = mid;
    } else {
      if (cmp_result == 0) {
        // Happen to have found the exact key or target is smaller than the
        // first key after base_offset.
        *offset = DecodeFixed64(base + user_key_size_ - key_prefix_len_);
        delete[] mid_key_str;
        return s;
      } else {
        high = mid;
      }
    }
  }

  const char* base = sub_index_.data() + base_offset
      + (user_key_size_ - key_prefix_len_ + kOffsetLen) * low;
  *offset = DecodeFixed64(base + user_key_size_ - key_prefix_len_);

  delete[] mid_key_str;
  return s;
}

uint64_t PlainTableReader::Next(uint64_t offset, Slice* key, Slice* value,
                                Slice* tmp_slice) {
  if (offset >= file_size_) {
    return file_size_;
  }
  int internal_key_size = GetInternalKeyLength();

  Status s = file_->Read(offset, internal_key_size, key, nullptr);
  offset += internal_key_size;

  s = file_->Read(offset, 4, tmp_slice, nullptr);
  offset += 4;
  uint32_t value_size = DecodeFixed32(tmp_slice->data());

  s = file_->Read(offset, value_size, value, nullptr);
  offset += value_size;

  return offset;
}

Status PlainTableReader::Get(
    const ReadOptions& ro, const Slice& target, void* arg,
    bool (*saver)(void*, const Slice&, const Slice&, bool),
    void (*mark_key_may_exist)(void*)) {
  uint64_t offset;
  Status s = GetOffset(target, &offset);
  if (!s.ok()) {
    return s;
  }
  Slice found_key;
  Slice found_value;
  Slice tmp_slice;
  while (offset < file_size_) {
    offset = Next(offset, &found_key, &found_value, &tmp_slice);
    if (options_.comparator->Compare(found_key, target) >= 0
        && !(*saver)(arg, found_key, found_value, true)) {
      break;
    }
  }
  return s;
}

bool PlainTableReader::TEST_KeyInCache(const ReadOptions& options,
                                       const Slice& key) {
  return false;
}

uint64_t PlainTableReader::ApproximateOffsetOf(const Slice& key) {
  return 0;
}

PlainTableIterator::PlainTableIterator(PlainTableReader* table) :
    table_(table) {
  SeekToFirst();
}

PlainTableIterator::~PlainTableIterator() {
}

bool PlainTableIterator::Valid() const {
  return offset_ < table_->file_size_ && offset_ >= 0;
}

void PlainTableIterator::SeekToFirst() {
  next_offset_ = 0;
  Next();
}

void PlainTableIterator::SeekToLast() {
  assert(false);
}

void PlainTableIterator::Seek(const Slice& target) {
  Status s = table_->GetOffset(target, &next_offset_);
  if (!s.ok()) {
    status_ = s;
  }
  if (next_offset_ < table_->file_size_) {
    for (Next();
        Valid() && table_->options_.comparator->Compare(key(), target) < 0;
        Next()) {
    }
  }
}

void PlainTableIterator::Next() {
  offset_ = next_offset_;
  Slice tmp_slice;
  next_offset_ = table_->Next(next_offset_, &key_, &value_, &tmp_slice);
}

void PlainTableIterator::Prev() {
  assert(false);
}

Slice PlainTableIterator::key() const {
  return key_;
}

Slice PlainTableIterator::value() const {
  return value_;
}

Status PlainTableIterator::status() const {
  return status_;
}

}  // namespace rocksdb
