// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/plain_table_reader.h"

#include <unordered_map>
#include <map>

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
#include "util/murmurhash.h"
#include "util/perf_context_imp.h"
#include "util/stop_watch.h"


namespace std {
template<>
struct hash<rocksdb::Slice> {
public:
  std::size_t operator()(rocksdb::Slice const& s) const {
    return MurmurHash(s.data(), s.size(), 397);
  }
};

class slice_comparator {
public:
  bool operator()(rocksdb::Slice const& s1, rocksdb::Slice const& s2) {
    return s1.compare(s2) < 0;
  }
};
}

namespace rocksdb {

static uint32_t getBucketId(Slice const& s, size_t prefix_len,
                            uint32_t num_buckets) {
  return MurmurHash(s.data(), prefix_len, 397) % num_buckets;
}

PlainTableReader::PlainTableReader(const EnvOptions& storage_options,
                                   uint64_t file_size, int user_key_size,
                                   int key_prefix_len, int bloom_bits_per_key,
                                   double hash_table_ratio) :
    hash_table_size_(0), soptions_(storage_options), file_size_(file_size),
    user_key_size_(user_key_size), key_prefix_len_(key_prefix_len),
    hash_table_ratio_(hash_table_ratio) {
  if (bloom_bits_per_key > 0) {
    filter_policy_ = NewBloomFilterPolicy(bloom_bits_per_key);
  } else {
    filter_policy_ = nullptr;
  }
  hash_table_ = nullptr;
  data_start_offset_ = 0;
  data_end_offset_ = file_size;
}

PlainTableReader::~PlainTableReader() {
  if (hash_table_ != nullptr) {
    delete[] hash_table_;
  }
  if (filter_policy_ != nullptr) {
    delete filter_policy_;
  }
}

Status PlainTableReader::Open(const Options& options,
                              const EnvOptions& soptions,
                              unique_ptr<RandomAccessFile> && file,
                              uint64_t file_size,
                              unique_ptr<TableReader>* table_reader,
                              const int user_key_size,
                              const int key_prefix_len,
                              const int bloom_num_bits,
                              double hash_table_ratio) {
  assert(options.allow_mmap_reads);

  if (file_size > 2147483646) {
    return Status::NotSupported("File is too large for PlainTableReader!");
  }

  PlainTableReader* t = new PlainTableReader(soptions, file_size,
                                             user_key_size,
                                             key_prefix_len,
                                             bloom_num_bits,
                                             hash_table_ratio);
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
  // Get mmapped memory to file_data_.
  Status s = file_->Read(0, file_size_, &file_data_, nullptr);
  if (!s.ok()) {
    return s;
  }
  version_ = DecodeFixed32(file_data_.data());
  version_ ^= 0x80000000;
  assert(version_ == 1);
  data_start_offset_ = 4;
  data_end_offset_ = file_size;

  Slice key_slice;
  Slice key_prefix_slice;
  Slice key_suffix_slice;
  Slice value_slice;
  Slice prev_key_prefix_slice;
  uint32_t pos = data_start_offset_;
  int key_index_within_prefix = 0;
  bool first = true;
  std::string prefix_sub_index;
  HistogramImpl keys_per_prefix_hist;
  // Need map to be ordered to make sure sub indexes generated
  // are in order.
  std::map<Slice, std::string, std::slice_comparator> prefix2map;

  while (pos < file_size) {
    uint32_t key_offset = pos;
    status_ = Next(pos, &key_slice, &value_slice, pos);
    key_prefix_slice = Slice(key_slice.data(), key_prefix_len_);

    if (first || prev_key_prefix_slice != key_prefix_slice) {
      if (!first) {
        keys_per_prefix_hist.Add(key_index_within_prefix);
      }
      key_index_within_prefix = 0;
      prev_key_prefix_slice = key_prefix_slice;
    }

    if (key_index_within_prefix++ % 8 == 0) {
      // Add an index key for every 8 keys
      std::string& prefix_index = prefix2map[key_prefix_slice];
      PutFixed32(&prefix_index, key_offset);
    }
    first = false;
  }
  keys_per_prefix_hist.Add(key_index_within_prefix);
  if (hash_table_ != nullptr) {
    delete[] hash_table_;
  }
  std::vector<Slice> filter_entries(0); // for creating bloom filter;
  if (filter_policy_ != nullptr) {
    filter_entries.reserve(prefix2map.size());
  }
  double hash_table_size_multipier =
      (hash_table_ratio_ > 1.0) ? 1.0 : 1.0 / hash_table_ratio_;
  hash_table_size_ = prefix2map.size() * hash_table_size_multipier + 1;
  hash_table_ = new uint32_t[hash_table_size_];
  std::vector<std::string> hash2map(hash_table_size_);

  size_t sub_index_size_needed = 0;
  for (auto& p: prefix2map) {
    auto& sub_index = hash2map[getBucketId(p.first, key_prefix_len_,
                                           hash_table_size_)];
    if (sub_index.length() > 0 || p.second.length() > kOffsetLen) {
      if (sub_index.length() <= kOffsetLen) {
        sub_index_size_needed += sub_index.length() + 4;
      }
      sub_index_size_needed += p.second.length();
    }
    sub_index.append(p.second);
    if (filter_policy_ != nullptr) {
      filter_entries.push_back(p.first);
    }
  }

  sub_index_.clear();
  Log(options_.info_log, "Reserving %zu bytes for sub index",
      sub_index_size_needed);
  sub_index_.reserve(sub_index_size_needed);
  for (int i = 0; i < hash_table_size_; i++) {
    uint32_t num_keys_for_bucket = hash2map[i].length() / kOffsetLen;
    switch (num_keys_for_bucket) {
    case 0:
      // No key for bucket
      hash_table_[i] = data_end_offset_;
      break;
    case 1:
      // point directly to the file offset
      hash_table_[i] = DecodeFixed32(hash2map[i].data());
      break;
    default:
      // point to index block
      hash_table_[i] = sub_index_.length() | kSubIndexMask;
      PutFixed32(&sub_index_, num_keys_for_bucket);
      sub_index_.append(hash2map[i]);
    }
  }
  if (filter_policy_ != nullptr) {
    filter_str_.clear();
    filter_policy_->CreateFilter(&filter_entries[0], filter_entries.size(),
                                 &filter_str_);
    filter_slice_ = Slice(filter_str_.data(), filter_str_.size());
  }

  Log(options_.info_log, "hash table size: %d, suffix_map length %zu",
      hash_table_size_, sub_index_.length());
  Log(options_.info_log, "Number of Keys per prefix Histogram: %s",
      keys_per_prefix_hist.ToString().c_str());

  return Status::OK();
}

uint32_t PlainTableReader::GetOffset(const Slice& target,
                                     bool& prefix_matched) {
  prefix_matched = false;
  int bucket = getBucketId(target, key_prefix_len_, hash_table_size_);
  uint32_t bucket_value = hash_table_[bucket];
  if (bucket_value == data_end_offset_) {
    return data_end_offset_;
  } else if ((bucket_value & kSubIndexMask) == 0) {
    // point directly to the file
    return bucket_value;
  }
  // point to sub-index, need to do a binary search

  uint32_t low = 0;
  uint64_t prefix_index_offset = bucket_value ^ kSubIndexMask;
  uint32_t upper_bound = DecodeFixed32(sub_index_.data() + prefix_index_offset);
  uint32_t high = upper_bound;
  uint64_t base_offset = prefix_index_offset + 4;
  Slice mid_key;

  // The key is between [low, high). Do a binary search between it.
  while (high - low > 1) {
    uint32_t mid = (high + low) / 2;
    const char* index_offset = sub_index_.data() + base_offset
        + kOffsetLen * mid;
    uint32_t file_offset = DecodeFixed32(index_offset);
    mid_key = Slice(file_data_.data() + file_offset, user_key_size_);

    int cmp_result = options_.comparator->Compare(target, mid_key);
    if (cmp_result > 0) {
      low = mid;
    } else {
      if (cmp_result == 0) {
        // Happen to have found the exact key or target is smaller than the
        // first key after base_offset.
        prefix_matched = true;
        return file_offset;
      } else {
        high = mid;
      }
    }
  }

  // The key is between low and low+1 (if exists). Both of them can have the
  // correct prefix. Need to rule out at least one, to avoid to miss the
  // correct one.
  uint32_t low_key_offset = DecodeFixed32(
      sub_index_.data() + base_offset + kOffsetLen * low);
  if (low + 1 < upper_bound) {
    if (Slice(file_data_.data() + low_key_offset, key_prefix_len_)
        == Slice(target.data(), key_prefix_len_)) {
      prefix_matched = true;
    } else {
      prefix_matched = false;
      return DecodeFixed32(
          sub_index_.data() + base_offset + kOffsetLen * (low + 1));
    }
  } else {
    prefix_matched = false;
  }
  return low_key_offset;
}

bool PlainTableReader::MayHavePrefix(const Slice& target_prefix) {
  return filter_policy_ == nullptr
      || filter_policy_->KeyMayMatch(target_prefix, filter_slice_);
}


Status PlainTableReader::Next(uint32_t offset, Slice* key, Slice* value,
                              uint32_t& next_offset) {
  if (offset == data_end_offset_) {
    next_offset = data_end_offset_;
    return Status::OK();
  }

  if (offset > data_end_offset_) {
    return Status::Corruption("Offset is out of file size");
  }

  int internal_key_size = GetInternalKeyLength();
  if (offset + internal_key_size >= data_end_offset_) {
    return Status::Corruption("Un able to read the next key");
  }

  const char* key_ptr =  file_data_.data() + offset;
  *key = Slice(key_ptr, internal_key_size);

  uint32_t value_size;
  const char* value_ptr = GetVarint32Ptr(key_ptr + internal_key_size,
                                         file_data_.data() + data_end_offset_,
                                         &value_size);
  if (value_ptr == nullptr) {
    return Status::Corruption("Error reading value length.");
  }
  next_offset = offset + (value_ptr - key_ptr) + value_size;
  if (next_offset > data_end_offset_) {
    return Status::Corruption("Reach end of file when reading value");
  }
  *value = Slice(value_ptr, value_size);

  return Status::OK();
}

Status PlainTableReader::Get(
    const ReadOptions& ro, const Slice& target, void* arg,
    bool (*saver)(void*, const Slice&, const Slice&, bool),
    void (*mark_key_may_exist)(void*)) {
  // Check bloom filter first.
  if (!MayHavePrefix(Slice(target.data(), key_prefix_len_))) {
    return Status::OK();
  }

  uint32_t offset;
  bool prefix_match;
  offset = GetOffset(target, prefix_match);
  Slice found_key;
  Slice found_value;
  while (offset < data_end_offset_) {
    Status s = Next(offset, &found_key, &found_value, offset);
    if (!s.ok()) {
      return s;
    }
    if (!prefix_match) {
      // Need to verify prefix for the first key found if it is not yet
      // checked.
      if (!target.starts_with(Slice(found_key.data(), key_prefix_len_))) {
        break;
      }
      prefix_match = true;
    }
    if (options_.comparator->Compare(found_key, target) >= 0
        && !(*saver)(arg, found_key, found_value, true)) {
      break;
    }
  }
  return Status::OK();
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
  return offset_ < table_->data_end_offset_
      && offset_ >= table_->data_start_offset_;
}

void PlainTableIterator::SeekToFirst() {
  next_offset_ = table_->data_start_offset_;
  Next();
}

void PlainTableIterator::SeekToLast() {
  assert(false);
}

void PlainTableIterator::Seek(const Slice& target) {
  if (!table_->MayHavePrefix(Slice(target.data(), table_->key_prefix_len_))) {
    offset_ = next_offset_ = table_->data_end_offset_;
    return;
  }
  bool prefix_match;
  next_offset_ = table_->GetOffset(target, prefix_match);

  if (next_offset_ < table_-> data_end_offset_) {
    for (Next(); status_.ok() && Valid(); Next()) {
      if (!prefix_match) {
        // Need to verify the first key's prefix
        if (!target.starts_with(Slice(key().data(), table_->key_prefix_len_))) {
          offset_ = next_offset_ = table_->data_end_offset_;
          break;
        }
        prefix_match = true;
      }
      if (table_->options_.comparator->Compare(key(), target) >= 0) {
        break;
      }
    }
  } else {
    offset_ = table_->data_end_offset_;
  }
}

void PlainTableIterator::Next() {
  offset_ = next_offset_;
  Slice tmp_slice;
  status_ = table_->Next(next_offset_, &key_, &value_, next_offset_);
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
