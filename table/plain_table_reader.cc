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
#include "rocksdb/plain_table_factory.h"

#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/two_level_iterator.h"

#include "util/coding.h"
#include "util/dynamic_bloom.h"
#include "util/hash.h"
#include "util/histogram.h"
#include "util/murmurhash.h"
#include "util/perf_context_imp.h"
#include "util/stop_watch.h"


namespace rocksdb {

extern const uint64_t kPlainTableMagicNumber;

static uint32_t GetSliceHash(Slice const& s) {
  return Hash(s.data(), s.size(), 397) ;
}
static uint32_t getBucketIdFromHash(uint32_t hash, uint32_t num_buckets) {
  return hash % num_buckets;
}

PlainTableReader::PlainTableReader(const EnvOptions& storage_options,
                                   uint64_t file_size, int bloom_bits_per_key,
                                   double hash_table_ratio,
                                   const TableProperties& table_properties) :
    hash_table_size_(0), soptions_(storage_options), file_size_(file_size),
    hash_table_ratio_(hash_table_ratio),
    bloom_bits_per_key_(bloom_bits_per_key),
    table_properties_(table_properties), data_start_offset_(0),
    data_end_offset_(table_properties_.data_size),
    user_key_len_(table_properties.fixed_key_len) {
  hash_table_ = nullptr;
  bloom_ = nullptr;
  sub_index_ = nullptr;
}

PlainTableReader::~PlainTableReader() {
  delete[] hash_table_;
  delete[] sub_index_;
  delete bloom_;
}

Status PlainTableReader::Open(const Options& options,
                              const EnvOptions& soptions,
                              unique_ptr<RandomAccessFile> && file,
                              uint64_t file_size,
                              unique_ptr<TableReader>* table_reader,
                              const int bloom_num_bits,
                              double hash_table_ratio) {
  assert(options.allow_mmap_reads);

  if (file_size > 2147483646) {
    return Status::NotSupported("File is too large for PlainTableReader!");
  }

  TableProperties table_properties;
  auto s = ReadTableProperties(
      file.get(),
      file_size,
      kPlainTableMagicNumber,
      options.env,
      options.info_log.get(),
      &table_properties
  );
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<PlainTableReader> new_reader(new PlainTableReader(
      soptions,
      file_size,
      bloom_num_bits,
      hash_table_ratio,
      table_properties
  ));
  new_reader->file_ = std::move(file);
  new_reader->options_ = options;

  // -- Populate Index
  s = new_reader->PopulateIndex();
  if (!s.ok()) {
    return s;
  }

  *table_reader = std::move(new_reader);
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

struct PlainTableReader::IndexRecord {
  uint32_t hash; // hash of the prefix
  uint32_t offset; // offset of a row
  IndexRecord* next;
};

// Helper class to track all the index records
class PlainTableReader::IndexRecordList {
public:
  explicit IndexRecordList(size_t num_records_per_group) :
      num_records_per_group_(num_records_per_group),
      current_group_(nullptr),
      num_records_in_current_group_(num_records_per_group) {
  }

  ~IndexRecordList() {
    for (size_t i = 0; i < groups_.size(); i++) {
      delete[] groups_[i];
    }
  }

  void AddRecord(murmur_t hash, uint32_t offset) {
    if (num_records_in_current_group_ == num_records_per_group_) {
      current_group_ = AllocateNewGroup();
      num_records_in_current_group_ = 0;
    }
    auto& new_record = current_group_[num_records_in_current_group_];
    new_record.hash = hash;
    new_record.offset = offset;
    new_record.next = nullptr;
    num_records_in_current_group_++;
  }

  size_t GetNumRecords() {
    return (groups_.size() - 1) * num_records_per_group_
        + num_records_in_current_group_;
  }
  IndexRecord* At(size_t index) {
    return &(groups_[index / num_records_per_group_]
                    [index % num_records_per_group_]);
  }

  IndexRecord* AllocateNewGroup() {
    IndexRecord* result = new IndexRecord[num_records_per_group_];
    groups_.push_back(result);
    return result;
  }
private:
  const size_t num_records_per_group_;
  IndexRecord* current_group_;
  // List of arrays allocated
  std::vector<IndexRecord*> groups_;
  size_t num_records_in_current_group_;
};

int PlainTableReader::PopulateIndexRecordList(
    IndexRecordList& record_list) {
  Slice key_slice;
  Slice key_prefix_slice;
  Slice key_suffix_slice;
  Slice value_slice;
  Slice prev_key_prefix_slice;
  uint32_t prev_key_prefix_hash = 0;
  uint32_t pos = data_start_offset_;
  int key_index_within_prefix = 0;
  bool first = true;
  std::string prefix_sub_index;
  HistogramImpl keys_per_prefix_hist;
  // Need map to be ordered to make sure sub indexes generated
  // are in order.

  int num_prefixes = 0;

  while (pos < data_end_offset_) {
    uint32_t key_offset = pos;
    status_ = Next(pos, &key_slice, &value_slice, pos);
    key_prefix_slice = GetPrefix(key_slice);

    if (first || prev_key_prefix_slice != key_prefix_slice) {
      num_prefixes++;
      if (!first) {
        keys_per_prefix_hist.Add(key_index_within_prefix);
      }
      key_index_within_prefix = 0;
      prev_key_prefix_slice = key_prefix_slice;
      prev_key_prefix_hash = GetSliceHash(key_prefix_slice);
    }

    if (key_index_within_prefix++ % 16 == 0) {
      // Add an index key for every 16 keys
      record_list.AddRecord(prev_key_prefix_hash, key_offset);
    }
    first = false;
  }
  keys_per_prefix_hist.Add(key_index_within_prefix);
  Log(options_.info_log, "Number of Keys per prefix Histogram: %s",
      keys_per_prefix_hist.ToString().c_str());

  return num_prefixes;
}

void PlainTableReader::Allocate(int num_prefixes) {
  if (hash_table_ != nullptr) {
    delete[] hash_table_;
  }
  if (bloom_bits_per_key_ > 0) {
    bloom_ = new DynamicBloom(num_prefixes * bloom_bits_per_key_);
  }
  double hash_table_size_multipier =
      (hash_table_ratio_ > 1.0) ? 1.0 : 1.0 / hash_table_ratio_;
  hash_table_size_ = num_prefixes * hash_table_size_multipier + 1;
  hash_table_ = new uint32_t[hash_table_size_];
}

size_t PlainTableReader::BucketizeIndexesAndFillBloom(
    IndexRecordList& record_list, int num_prefixes,
    std::vector<IndexRecord*>& hash2offsets,
    std::vector<uint32_t>& bucket_count) {
  size_t sub_index_size_needed = 0;
  bool first = true;
  uint32_t prev_hash = 0;
  size_t num_records = record_list.GetNumRecords();
  for (size_t i = 0; i < num_records; i++) {
    IndexRecord* index_record = record_list.At(i);
    uint32_t cur_hash = index_record->hash;
    if (first || prev_hash != cur_hash) {
      prev_hash = cur_hash;
      first = false;
      if (bloom_) {
        bloom_->AddHash(cur_hash);
      }
    }
    uint32_t bucket = getBucketIdFromHash(cur_hash, hash_table_size_);
    IndexRecord* prev_bucket_head = hash2offsets[bucket];
    index_record->next = prev_bucket_head;
    hash2offsets[bucket] = index_record;
    if (bucket_count[bucket] > 0) {
      if (bucket_count[bucket] == 1) {
        sub_index_size_needed += kOffsetLen + 1;
      }
      if (bucket_count[bucket] == 127) {
        // Need more than one byte for length
        sub_index_size_needed++;
      }
      sub_index_size_needed += kOffsetLen;
    }
    bucket_count[bucket]++;
  }
  return sub_index_size_needed;
}

void PlainTableReader::FillIndexes(size_t sub_index_size_needed,
                                   std::vector<IndexRecord*>& hash2offsets,
                                   std::vector<uint32_t>& bucket_count) {
  Log(options_.info_log, "Reserving %zu bytes for sub index",
      sub_index_size_needed);
  // 4 bytes buffer for variable length size
  size_t buffer_size = 64;
  size_t buffer_used = 0;
  sub_index_size_needed += buffer_size;
  sub_index_ = new char[sub_index_size_needed];
  size_t sub_index_offset = 0;
  char* prev_ptr;
  char* cur_ptr;
  uint32_t* sub_index_ptr;
  IndexRecord* record;
  for (int i = 0; i < hash_table_size_; i++) {
    uint32_t num_keys_for_bucket = bucket_count[i];
    switch (num_keys_for_bucket) {
    case 0:
      // No key for bucket
      hash_table_[i] = data_end_offset_;
      break;
    case 1:
      // point directly to the file offset
      hash_table_[i] = hash2offsets[i]->offset;
      break;
    default:
      // point to second level indexes.
      hash_table_[i] = sub_index_offset | kSubIndexMask;
      prev_ptr = sub_index_ + sub_index_offset;
      cur_ptr = EncodeVarint32(prev_ptr, num_keys_for_bucket);
      sub_index_offset += cur_ptr - prev_ptr;
      if (cur_ptr - prev_ptr > 2
          || (cur_ptr - prev_ptr == 2 && num_keys_for_bucket <= 127)) {
        // Need to resize sub_index. Exponentially grow buffer.
        buffer_used += cur_ptr - prev_ptr - 1;
        if (buffer_used + 4 > buffer_size) {
          Log(options_.info_log, "Recalculate suffix_map length to %zu",
              sub_index_size_needed);

          sub_index_size_needed += buffer_size;
          buffer_size *= 2;
          char* new_sub_index = new char[sub_index_size_needed];
          memcpy(new_sub_index, sub_index_, sub_index_offset);
          delete[] sub_index_;
          sub_index_ = new_sub_index;
        }
      }
      sub_index_ptr = (uint32_t*) (sub_index_ + sub_index_offset);
      record = hash2offsets[i];
      int j;
      for (j = num_keys_for_bucket - 1;
          j >= 0 && record; j--, record = record->next) {
        sub_index_ptr[j] = record->offset;
      }
      assert(j == -1 && record == nullptr);
      sub_index_offset += kOffsetLen * num_keys_for_bucket;
      break;
    }
  }

  Log(options_.info_log, "hash table size: %d, suffix_map length %zu",
      hash_table_size_, sub_index_size_needed);
}

// PopulateIndex() builds index of keys.
// hash_table_ contains buckets size of hash_table_size_, each is a 32-bit
// integer. The lower 31 bits contain an offset value (explained below) and
// the first bit of the integer indicates type of the offset:
//
// 0 indicates that the bucket contains only one prefix (no conflict when
//   hashing this prefix), whose first row starts from this offset of the file.
// 1 indicates that the bucket contains more than one prefixes, or there
//   are too many rows for one prefix so we need a binary search for it. In
//   this case, the offset indicates the offset of sub_index_ holding the
//   binary search indexes of keys for those rows. Those binary search indexes
//   are organized in this way:
//
// The first 4 bytes, indicates how many indexes (N) are stored after it. After
// it, there are N 32-bit integers, each points of an offset of the file, which
// points to starting of a row. Those offsets need to be guaranteed to be in
// ascending order so the keys they are pointing to are also in ascending order
// to make sure we can use them to do binary searches.
Status PlainTableReader::PopulateIndex() {
  // Get mmapped memory to file_data_.
  Status s = file_->Read(0, file_size_, &file_data_, nullptr);
  if (!s.ok()) {
    return s;
  }

  IndexRecordList record_list(256);
  // First, read the whole file, for every 16 rows for a prefix (starting from
  // the first one), generate a record of (hash, offset) and append it to
  // IndexRecordList, which is a data structure created to store them.
  int num_prefixes = PopulateIndexRecordList(record_list);
  // Calculated hash table and bloom filter size and allocate memory for indexes
  // and bloom filter based on the number of prefixes.
  Allocate(num_prefixes);

  // Bucketize all the index records to a temp data structure, in which for
  // each bucket, we generate a linked list of IndexRecord, in reversed order.
  std::vector<IndexRecord*> hash2offsets(hash_table_size_, nullptr);
  std::vector<uint32_t> bucket_count(hash_table_size_, 0);
  size_t sub_index_size_needed = BucketizeIndexesAndFillBloom(record_list,
                                                              num_prefixes,
                                                              hash2offsets,
                                                              bucket_count);
  // From the temp data structure, populate indexes.
  FillIndexes(sub_index_size_needed, hash2offsets, bucket_count);

  return Status::OK();
}

Status PlainTableReader::GetOffset(const Slice& target, const Slice& prefix,
                                   uint32_t prefix_hash, bool& prefix_matched,
                                   uint32_t& ret_offset) {
  prefix_matched = false;
  int bucket = getBucketIdFromHash(prefix_hash, hash_table_size_);
  uint32_t bucket_value = hash_table_[bucket];
  if (bucket_value == data_end_offset_) {
    ret_offset = data_end_offset_;
    return Status::OK();
  } else if ((bucket_value & kSubIndexMask) == 0) {
    // point directly to the file
    ret_offset = bucket_value;
    return Status::OK();
  }

  // point to sub-index, need to do a binary search
  uint32_t low = 0;
  uint64_t prefix_index_offset = bucket_value ^ kSubIndexMask;

  const char* index_ptr = sub_index_ + prefix_index_offset;
  uint32_t upper_bound;
  const uint32_t* base_ptr = (const uint32_t*) GetVarint32Ptr(index_ptr,
                                                              index_ptr + 4,
                                                              &upper_bound);
  uint32_t high = upper_bound;
  Slice mid_key;

  // The key is between [low, high). Do a binary search between it.
  while (high - low > 1) {
    uint32_t mid = (high + low) / 2;
    uint32_t file_offset = base_ptr[mid];
    size_t tmp;
    Status s = ReadKey(file_data_.data() + file_offset, &mid_key, tmp);
    if (!s.ok()) {
      return s;
    }
    int cmp_result = options_.comparator->Compare(target, mid_key);
    if (cmp_result > 0) {
      low = mid;
    } else {
      if (cmp_result == 0) {
        // Happen to have found the exact key or target is smaller than the
        // first key after base_offset.
        prefix_matched = true;
        ret_offset = file_offset;
        return Status::OK();
      } else {
        high = mid;
      }
    }
  }
  // Both of the key at the position low or low+1 could share the same
  // prefix as target. We need to rule out one of them to avoid to go
  // to the wrong prefix.
  Slice low_key;
  size_t tmp;
  uint32_t low_key_offset = base_ptr[low];
  Status s = ReadKey(file_data_.data() + low_key_offset, &low_key, tmp);
  if (GetPrefix(low_key) == prefix) {
    prefix_matched = true;
    ret_offset = low_key_offset;
  } else if (low + 1 < upper_bound) {
    // There is possible a next prefix, return it
    prefix_matched = false;
    ret_offset = base_ptr[low + 1];
  } else {
    // target is larger than a key of the last prefix in this bucket
    // but with a different prefix. Key does not exist.
    ret_offset = data_end_offset_;
  }
  return Status::OK();
}

bool PlainTableReader::MayHavePrefix(uint32_t hash) {
  return bloom_ == nullptr || bloom_->MayContainHash(hash);
}

Status PlainTableReader::ReadKey(const char* row_ptr, Slice* key,
                                 size_t& bytes_read) {
  const char* key_ptr;
  bytes_read = 0;
  size_t internal_key_size;
  if (IsFixedLength()) {
    internal_key_size = GetFixedInternalKeyLength();
    key_ptr = row_ptr;
  } else {
    uint32_t key_size;
    key_ptr = GetVarint32Ptr(row_ptr, file_data_.data() + data_end_offset_,
                             &key_size);
    internal_key_size = (size_t) key_size;
    bytes_read = key_ptr - row_ptr;
  }
  if (row_ptr + internal_key_size >= file_data_.data() + data_end_offset_) {
    return Status::Corruption("Unable to read the next key");
  }
  *key = Slice(key_ptr, internal_key_size);
  bytes_read += internal_key_size;
  return Status::OK();
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

  const char* row_ptr = file_data_.data() + offset;
  size_t bytes_for_key;
  Status s = ReadKey(row_ptr, key, bytes_for_key);
  uint32_t value_size;
  const char* value_ptr = GetVarint32Ptr(row_ptr + bytes_for_key,
                                         file_data_.data() + data_end_offset_,
                                         &value_size);
  if (value_ptr == nullptr) {
    return Status::Corruption("Error reading value length.");
  }
  next_offset = offset + (value_ptr - row_ptr) + value_size;
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
  Slice prefix_slice = GetPrefix(target);
  uint32_t prefix_hash = GetSliceHash(prefix_slice);
  if (!MayHavePrefix(prefix_hash)) {
    return Status::OK();
  }
  uint32_t offset;
  bool prefix_match;
  Status s = GetOffset(target, prefix_slice, prefix_hash, prefix_match, offset);
  if (!s.ok()) {
    return s;
  }
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
      if (GetPrefix(found_key) != prefix_slice) {
        return Status::OK();
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
  next_offset_ = offset_ = table_->data_end_offset_;
}

PlainTableIterator::~PlainTableIterator() {
}

bool PlainTableIterator::Valid() const {
  return offset_ < table_->data_end_offset_
      && offset_ >= table_->data_start_offset_;
}

void PlainTableIterator::SeekToFirst() {
  next_offset_ = table_->data_start_offset_;
  if (next_offset_ >= table_->data_end_offset_) {
    next_offset_ = offset_ = table_->data_end_offset_;
  } else {
    Next();
  }
}

void PlainTableIterator::SeekToLast() {
  assert(false);
}

void PlainTableIterator::Seek(const Slice& target) {
  Slice prefix_slice =  table_->GetPrefix(target);
  uint32_t prefix_hash = GetSliceHash(prefix_slice);
  if (!table_->MayHavePrefix(prefix_hash)) {
    offset_ = next_offset_ = table_->data_end_offset_;
    return;
  }
  bool prefix_match;
  status_ = table_->GetOffset(target, prefix_slice, prefix_hash, prefix_match,
                              next_offset_);
  if (!status_.ok()) {
    offset_ = next_offset_ = table_->data_end_offset_;
    return;
  }

  if (next_offset_ < table_-> data_end_offset_) {
    for (Next(); status_.ok() && Valid(); Next()) {
      if (!prefix_match) {
        // Need to verify the first key's prefix
        if (table_->GetPrefix(key()) != prefix_slice) {
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
