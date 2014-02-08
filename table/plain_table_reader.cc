// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/plain_table_reader.h"

#include <string>

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
#include "table/meta_blocks.h"
#include "table/two_level_iterator.h"
#include "table/plain_table_factory.h"

#include "util/coding.h"
#include "util/dynamic_bloom.h"
#include "util/hash.h"
#include "util/histogram.h"
#include "util/murmurhash.h"
#include "util/perf_context_imp.h"
#include "util/stop_watch.h"


namespace rocksdb {

namespace {

inline uint32_t GetSliceHash(Slice const& s) {
  return Hash(s.data(), s.size(), 397) ;
}

inline uint32_t GetBucketIdFromHash(uint32_t hash, uint32_t num_buckets) {
  return hash % num_buckets;
}

}  // namespace

// Iterator to iterate IndexedTable
class PlainTableIterator : public Iterator {
 public:
  explicit PlainTableIterator(PlainTableReader* table);
  ~PlainTableIterator();

  bool Valid() const;

  void SeekToFirst();

  void SeekToLast();

  void Seek(const Slice& target);

  void Next();

  void Prev();

  Slice key() const;

  Slice value() const;

  Status status() const;

 private:
  PlainTableReader* table_;
  uint32_t offset_;
  uint32_t next_offset_;
  Slice key_;
  Slice value_;
  Status status_;
  std::string tmp_str_;
  // No copying allowed
  PlainTableIterator(const PlainTableIterator&) = delete;
  void operator=(const Iterator&) = delete;
};

extern const uint64_t kPlainTableMagicNumber;
PlainTableReader::PlainTableReader(const EnvOptions& storage_options,
                                   const InternalKeyComparator& icomparator,
                                   uint64_t file_size, int bloom_bits_per_key,
                                   double hash_table_ratio,
                                   const TableProperties* table_properties)
    : soptions_(storage_options),
      internal_comparator_(icomparator),
      file_size_(file_size),
      kHashTableRatio(hash_table_ratio),
      kBloomBitsPerKey(bloom_bits_per_key),
      table_properties_(table_properties),
      data_end_offset_(table_properties_->data_size),
      user_key_len_(table_properties->fixed_key_len) {}

PlainTableReader::~PlainTableReader() {
  delete[] hash_table_;
  delete[] sub_index_;
  delete bloom_;
}

Status PlainTableReader::Open(const Options& options,
                              const EnvOptions& soptions,
                              const InternalKeyComparator& internal_comparator,
                              unique_ptr<RandomAccessFile>&& file,
                              uint64_t file_size,
                              unique_ptr<TableReader>* table_reader,
                              const int bloom_bits_per_key,
                              double hash_table_ratio) {
  assert(options.allow_mmap_reads);

  if (file_size > kMaxFileSize) {
    return Status::NotSupported("File is too large for PlainTableReader!");
  }

  TableProperties* props = nullptr;
  auto s = ReadTableProperties(file.get(), file_size, kPlainTableMagicNumber,
                               options.env, options.info_log.get(), &props);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<PlainTableReader> new_reader(
      new PlainTableReader(soptions, internal_comparator, file_size,
                           bloom_bits_per_key, hash_table_ratio, props));
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
  explicit IndexRecordList(size_t num_records_per_group)
      : kNumRecordsPerGroup(num_records_per_group),
        current_group_(nullptr),
        num_records_in_current_group_(num_records_per_group) {}

  ~IndexRecordList() {
    for (size_t i = 0; i < groups_.size(); i++) {
      delete[] groups_[i];
    }
  }

  void AddRecord(murmur_t hash, uint32_t offset) {
    if (num_records_in_current_group_ == kNumRecordsPerGroup) {
      current_group_ = AllocateNewGroup();
      num_records_in_current_group_ = 0;
    }
    auto& new_record = current_group_[num_records_in_current_group_++];
    new_record.hash = hash;
    new_record.offset = offset;
    new_record.next = nullptr;
  }

  size_t GetNumRecords() const {
    return (groups_.size() - 1) * kNumRecordsPerGroup +
           num_records_in_current_group_;
  }
  IndexRecord* At(size_t index) {
    return &(groups_[index / kNumRecordsPerGroup][index % kNumRecordsPerGroup]);
  }

 private:
  IndexRecord* AllocateNewGroup() {
    IndexRecord* result = new IndexRecord[kNumRecordsPerGroup];
    groups_.push_back(result);
    return result;
  }

  const size_t kNumRecordsPerGroup;
  IndexRecord* current_group_;
  // List of arrays allocated
  std::vector<IndexRecord*> groups_;
  size_t num_records_in_current_group_;
};

int PlainTableReader::PopulateIndexRecordList(IndexRecordList* record_list) {
  Slice prev_key_prefix_slice;
  uint32_t prev_key_prefix_hash = 0;
  uint32_t pos = data_start_offset_;
  int key_index_within_prefix = 0;
  bool is_first_record = true;
  HistogramImpl keys_per_prefix_hist;
  // Need map to be ordered to make sure sub indexes generated
  // are in order.

  int num_prefixes = 0;
  while (pos < data_end_offset_) {
    uint32_t key_offset = pos;
    ParsedInternalKey key;
    Slice value_slice;
    status_ = Next(pos, &key, &value_slice, pos);
    Slice key_prefix_slice = GetPrefix(key);

    if (is_first_record || prev_key_prefix_slice != key_prefix_slice) {
      ++num_prefixes;
      if (!is_first_record) {
        keys_per_prefix_hist.Add(key_index_within_prefix);
      }
      key_index_within_prefix = 0;
      prev_key_prefix_slice = key_prefix_slice;
      prev_key_prefix_hash = GetSliceHash(key_prefix_slice);
    }

    if (key_index_within_prefix++ % kIndexIntervalForSamePrefixKeys == 0) {
      // Add an index key for every kIndexIntervalForSamePrefixKeys keys
      record_list->AddRecord(prev_key_prefix_hash, key_offset);
    }
    is_first_record = false;
  }

  keys_per_prefix_hist.Add(key_index_within_prefix);
  Log(options_.info_log, "Number of Keys per prefix Histogram: %s",
      keys_per_prefix_hist.ToString().c_str());

  return num_prefixes;
}

void PlainTableReader::AllocateIndexAndBloom(int num_prefixes) {
  delete[] hash_table_;

  if (kBloomBitsPerKey > 0) {
    bloom_ = new DynamicBloom(num_prefixes * kBloomBitsPerKey);
  }
  double hash_table_size_multipier =
      (kHashTableRatio > 1.0) ? 1.0 : 1.0 / kHashTableRatio;
  hash_table_size_ = num_prefixes * hash_table_size_multipier + 1;
  hash_table_ = new uint32_t[hash_table_size_];
}

size_t PlainTableReader::BucketizeIndexesAndFillBloom(
    IndexRecordList& record_list, int num_prefixes,
    std::vector<IndexRecord*>* hash_to_offsets,
    std::vector<uint32_t>* bucket_count) {
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
    uint32_t bucket = GetBucketIdFromHash(cur_hash, hash_table_size_);
    IndexRecord* prev_bucket_head = (*hash_to_offsets)[bucket];
    index_record->next = prev_bucket_head;
    (*hash_to_offsets)[bucket] = index_record;
    auto& item_count = (*bucket_count)[bucket];
    if (item_count > 0) {
      if (item_count == 1) {
        sub_index_size_needed += kOffsetLen + 1;
      }
      if (item_count == 127) {
        // Need more than one byte for length
        sub_index_size_needed++;
      }
      sub_index_size_needed += kOffsetLen;
    }
    item_count++;
  }
  return sub_index_size_needed;
}

void PlainTableReader::FillIndexes(
    size_t sub_index_size_needed,
    const std::vector<IndexRecord*>& hash_to_offsets,
    const std::vector<uint32_t>& bucket_count) {
  Log(options_.info_log, "Reserving %zu bytes for sub index",
      sub_index_size_needed);
  // 8 bytes buffer for variable length size
  size_t buffer_size = 8 * 8;
  size_t buffer_used = 0;
  sub_index_size_needed += buffer_size;
  sub_index_ = new char[sub_index_size_needed];
  size_t sub_index_offset = 0;
  char* prev_ptr;
  char* cur_ptr;
  uint32_t* sub_index_ptr;
  for (int i = 0; i < hash_table_size_; i++) {
    uint32_t num_keys_for_bucket = bucket_count[i];
    switch (num_keys_for_bucket) {
    case 0:
      // No key for bucket
      hash_table_[i] = data_end_offset_;
      break;
    case 1:
      // point directly to the file offset
      hash_table_[i] = hash_to_offsets[i]->offset;
      break;
    default:
      // point to second level indexes.
      hash_table_[i] = sub_index_offset | kSubIndexMask;
      prev_ptr = sub_index_ + sub_index_offset;
      cur_ptr = EncodeVarint32(prev_ptr, num_keys_for_bucket);
      sub_index_offset += (cur_ptr - prev_ptr);
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
      IndexRecord* record = hash_to_offsets[i];
      int j;
      for (j = num_keys_for_bucket - 1; j >= 0 && record;
           j--, record = record->next) {
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

Status PlainTableReader::PopulateIndex() {
  // Get mmapped memory to file_data_.
  Status s = file_->Read(0, file_size_, &file_data_, nullptr);
  if (!s.ok()) {
    return s;
  }

  IndexRecordList record_list(kRecordsPerGroup);
  // First, read the whole file, for every kIndexIntervalForSamePrefixKeys rows
  // for a prefix (starting from the first one), generate a record of (hash,
  // offset) and append it to IndexRecordList, which is a data structure created
  // to store them.
  int num_prefixes = PopulateIndexRecordList(&record_list);
  // Calculated hash table and bloom filter size and allocate memory for indexes
  // and bloom filter based on the number of prefixes.
  AllocateIndexAndBloom(num_prefixes);

  // Bucketize all the index records to a temp data structure, in which for
  // each bucket, we generate a linked list of IndexRecord, in reversed order.
  std::vector<IndexRecord*> hash_to_offsets(hash_table_size_, nullptr);
  std::vector<uint32_t> bucket_count(hash_table_size_, 0);
  size_t sub_index_size_needed = BucketizeIndexesAndFillBloom(
      record_list, num_prefixes, &hash_to_offsets, &bucket_count);
  // From the temp data structure, populate indexes.
  FillIndexes(sub_index_size_needed, hash_to_offsets, bucket_count);

  return Status::OK();
}

Status PlainTableReader::GetOffset(const Slice& target, const Slice& prefix,
                                   uint32_t prefix_hash, bool& prefix_matched,
                                   uint32_t& ret_offset) {
  prefix_matched = false;
  int bucket = GetBucketIdFromHash(prefix_hash, hash_table_size_);
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
  uint32_t upper_bound = 0;
  const uint32_t* base_ptr = (const uint32_t*) GetVarint32Ptr(index_ptr,
                                                              index_ptr + 4,
                                                              &upper_bound);
  uint32_t high = upper_bound;
  ParsedInternalKey mid_key;
  ParsedInternalKey parsed_target;
  if (!ParseInternalKey(target, &parsed_target)) {
    return Status::Corruption(Slice());
  }

  // The key is between [low, high). Do a binary search between it.
  while (high - low > 1) {
    uint32_t mid = (high + low) / 2;
    uint32_t file_offset = base_ptr[mid];
    size_t tmp;
    Status s = ReadKey(file_data_.data() + file_offset, &mid_key, tmp);
    if (!s.ok()) {
      return s;
    }
    int cmp_result = internal_comparator_.Compare(mid_key, parsed_target);
    if (cmp_result < 0) {
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
  ParsedInternalKey low_key;
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

Slice PlainTableReader::GetPrefix(const ParsedInternalKey& target) {
  return options_.prefix_extractor->Transform(target.user_key);
}

Status PlainTableReader::ReadKey(const char* row_ptr, ParsedInternalKey* key,
                                 size_t& bytes_read) {
  const char* key_ptr = nullptr;
  bytes_read = 0;
  size_t user_key_size = 0;
  if (IsFixedLength()) {
    user_key_size = user_key_len_;
    key_ptr = row_ptr;
  } else {
    uint32_t tmp_size = 0;
    key_ptr = GetVarint32Ptr(row_ptr, file_data_.data() + data_end_offset_,
                             &tmp_size);
    if (key_ptr == nullptr) {
      return Status::Corruption("Unable to read the next key");
    }
    user_key_size = (size_t)tmp_size;
    bytes_read = key_ptr - row_ptr;
  }
  if (key_ptr + user_key_size + 1 >= file_data_.data() + data_end_offset_) {
    return Status::Corruption("Unable to read the next key");
  }

  if (*(key_ptr + user_key_size) == PlainTableFactory::kValueTypeSeqId0) {
    // Special encoding for the row with seqID=0
    key->user_key = Slice(key_ptr, user_key_size);
    key->sequence = 0;
    key->type = kTypeValue;
    bytes_read += user_key_size + 1;
  } else {
    if (row_ptr + user_key_size + 8 >= file_data_.data() + data_end_offset_) {
      return Status::Corruption("Unable to read the next key");
    }
    if (!ParseInternalKey(Slice(key_ptr, user_key_size + 8), key)) {
      return Status::Corruption(Slice());
    }
    bytes_read += user_key_size + 8;
  }

  return Status::OK();
}

Status PlainTableReader::Next(uint32_t offset, ParsedInternalKey* key,
                              Slice* value, uint32_t& next_offset) {
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

Status PlainTableReader::Get(const ReadOptions& ro, const Slice& target,
                             void* arg,
                             bool (*saver)(void*, const ParsedInternalKey&,
                                           const Slice&, bool),
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
  ParsedInternalKey found_key;
  ParsedInternalKey parsed_target;
  if (!ParseInternalKey(target, &parsed_target)) {
    return Status::Corruption(Slice());
  }

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
    if (internal_comparator_.Compare(found_key, parsed_target) >= 0) {
      if (!(*saver)(arg, found_key, found_value, true)) {
        break;
      }
    }
  }
  return Status::OK();
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
      if (table_->internal_comparator_.Compare(key(), target) >= 0) {
        break;
      }
    }
  } else {
    offset_ = table_->data_end_offset_;
  }
}

void PlainTableIterator::Next() {
  offset_ = next_offset_;
  if (offset_ < table_->data_end_offset_) {
    Slice tmp_slice;
    ParsedInternalKey parsed_key;
    status_ = table_->Next(next_offset_, &parsed_key, &value_, next_offset_);
    if (status_.ok()) {
      // Make a copy in this case. TODO optimize.
      tmp_str_.clear();
      AppendInternalKey(&tmp_str_, parsed_key);
      key_ = Slice(tmp_str_);
    } else {
      offset_ = next_offset_ = table_->data_end_offset_;
    }
  }
}

void PlainTableIterator::Prev() {
  assert(false);
}

Slice PlainTableIterator::key() const {
  assert(Valid());
  return key_;
}

Slice PlainTableIterator::value() const {
  assert(Valid());
  return value_;
}

Status PlainTableIterator::status() const {
  return status_;
}

}  // namespace rocksdb
