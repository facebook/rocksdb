// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE
#include "table/plain_table_reader.h"

#include <string>
#include <vector>

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

#include "util/arena.h"
#include "util/coding.h"
#include "util/dynamic_bloom.h"
#include "util/hash.h"
#include "util/histogram.h"
#include "util/murmurhash.h"
#include "util/perf_context_imp.h"
#include "util/stop_watch.h"


namespace rocksdb {

namespace {

inline uint32_t GetSliceHash(const Slice& s) {
  return Hash(s.data(), s.size(), 397) ;
}

inline uint32_t GetBucketIdFromHash(uint32_t hash, uint32_t num_buckets) {
  return hash % num_buckets;
}

// Safely getting a uint32_t element from a char array, where, starting from
// `base`, every 4 bytes are considered as an fixed 32 bit integer.
inline uint32_t GetFixed32Element(const char* base, size_t offset) {
  return DecodeFixed32(base + offset * sizeof(uint32_t));
}

}  // namespace

// Iterator to iterate IndexedTable
class PlainTableIterator : public Iterator {
 public:
  explicit PlainTableIterator(PlainTableReader* table, bool use_prefix_seek);
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
  bool use_prefix_seek_;
  uint32_t offset_;
  uint32_t next_offset_;
  IterKey key_;
  Slice value_;
  Status status_;
  // No copying allowed
  PlainTableIterator(const PlainTableIterator&) = delete;
  void operator=(const Iterator&) = delete;
};

extern const uint64_t kPlainTableMagicNumber;
PlainTableReader::PlainTableReader(
    const Options& options, unique_ptr<RandomAccessFile>&& file,
    const EnvOptions& storage_options, const InternalKeyComparator& icomparator,
    uint64_t file_size, int bloom_bits_per_key, double hash_table_ratio,
    size_t index_sparseness, const TableProperties* table_properties,
    size_t huge_page_tlb_size)
    : options_(options),
      soptions_(storage_options),
      file_(std::move(file)),
      internal_comparator_(icomparator),
      file_size_(file_size),
      kHashTableRatio(hash_table_ratio),
      kBloomBitsPerKey(bloom_bits_per_key),
      kIndexIntervalForSamePrefixKeys(index_sparseness),
      table_properties_(nullptr),
      data_end_offset_(table_properties->data_size),
      user_key_len_(table_properties->fixed_key_len),
      huge_page_tlb_size_(huge_page_tlb_size) {
  assert(kHashTableRatio >= 0.0);
}

PlainTableReader::~PlainTableReader() {
}

Status PlainTableReader::Open(const Options& options,
                              const EnvOptions& soptions,
                              const InternalKeyComparator& internal_comparator,
                              unique_ptr<RandomAccessFile>&& file,
                              uint64_t file_size,
                              unique_ptr<TableReader>* table_reader,
                              const int bloom_bits_per_key,
                              double hash_table_ratio, size_t index_sparseness,
                              size_t huge_page_tlb_size) {
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

  std::unique_ptr<PlainTableReader> new_reader(new PlainTableReader(
      options, std::move(file), soptions, internal_comparator, file_size,
      bloom_bits_per_key, hash_table_ratio, index_sparseness, props,
      huge_page_tlb_size));

  // -- Populate Index
  s = new_reader->PopulateIndex(props);
  if (!s.ok()) {
    return s;
  }

  *table_reader = std::move(new_reader);
  return s;
}

void PlainTableReader::SetupForCompaction() {
}

Iterator* PlainTableReader::NewIterator(const ReadOptions& options) {
  return new PlainTableIterator(this, options_.prefix_extractor != nullptr);
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

  // Each group in `groups_` contains fix-sized records (determined by
  // kNumRecordsPerGroup). Which can help us minimize the cost if resizing
  // occurs.
  const size_t kNumRecordsPerGroup;
  IndexRecord* current_group_;
  // List of arrays allocated
  std::vector<IndexRecord*> groups_;
  size_t num_records_in_current_group_;
};

Status PlainTableReader::PopulateIndexRecordList(IndexRecordList* record_list,
                                                 int* num_prefixes) const {
  Slice prev_key_prefix_slice;
  uint32_t prev_key_prefix_hash = 0;
  uint32_t pos = data_start_offset_;
  int num_keys_per_prefix = 0;
  bool is_first_record = true;
  HistogramImpl keys_per_prefix_hist;
  // Need map to be ordered to make sure sub indexes generated
  // are in order.

  *num_prefixes = 0;
  while (pos < data_end_offset_) {
    uint32_t key_offset = pos;
    ParsedInternalKey key;
    Slice value_slice;
    Status s = Next(&pos, &key, &value_slice);
    if (!s.ok()) {
      return s;
    }
    if (bloom_) {
      // total order mode and bloom filter is enabled.
      bloom_->AddHash(GetSliceHash(key.user_key));
    }
    Slice key_prefix_slice = GetPrefix(key);

    if (is_first_record || prev_key_prefix_slice != key_prefix_slice) {
      ++(*num_prefixes);
      if (!is_first_record) {
        keys_per_prefix_hist.Add(num_keys_per_prefix);
      }
      num_keys_per_prefix = 0;
      prev_key_prefix_slice = key_prefix_slice;
      prev_key_prefix_hash = GetSliceHash(key_prefix_slice);
    }

    if (kIndexIntervalForSamePrefixKeys == 0 ||
        num_keys_per_prefix++ % kIndexIntervalForSamePrefixKeys == 0) {
      // Add an index key for every kIndexIntervalForSamePrefixKeys keys
      record_list->AddRecord(prev_key_prefix_hash, key_offset);
    }
    is_first_record = false;
  }

  keys_per_prefix_hist.Add(num_keys_per_prefix);
  Log(options_.info_log, "Number of Keys per prefix Histogram: %s",
      keys_per_prefix_hist.ToString().c_str());

  return Status::OK();
}

void PlainTableReader::AllocateIndexAndBloom(int num_prefixes) {
  if (options_.prefix_extractor.get() != nullptr) {
    uint32_t bloom_total_bits = num_prefixes * kBloomBitsPerKey;
    if (bloom_total_bits > 0) {
      bloom_.reset(new DynamicBloom(bloom_total_bits, options_.bloom_locality,
                                    6, nullptr, huge_page_tlb_size_));
    }
  }

  if (options_.prefix_extractor.get() == nullptr || kHashTableRatio <= 0) {
    // Fall back to pure binary search if the user fails to specify a prefix
    // extractor.
    index_size_ = 1;
  } else {
    double hash_table_size_multipier = 1.0 / kHashTableRatio;
    index_size_ = num_prefixes * hash_table_size_multipier + 1;
  }
}

size_t PlainTableReader::BucketizeIndexesAndFillBloom(
    IndexRecordList* record_list, std::vector<IndexRecord*>* hash_to_offsets,
    std::vector<uint32_t>* entries_per_bucket) {
  bool first = true;
  uint32_t prev_hash = 0;
  size_t num_records = record_list->GetNumRecords();
  for (size_t i = 0; i < num_records; i++) {
    IndexRecord* index_record = record_list->At(i);
    uint32_t cur_hash = index_record->hash;
    if (first || prev_hash != cur_hash) {
      prev_hash = cur_hash;
      first = false;
      if (bloom_ && !IsTotalOrderMode()) {
        bloom_->AddHash(cur_hash);
      }
    }
    uint32_t bucket = GetBucketIdFromHash(cur_hash, index_size_);
    IndexRecord* prev_bucket_head = (*hash_to_offsets)[bucket];
    index_record->next = prev_bucket_head;
    (*hash_to_offsets)[bucket] = index_record;
    (*entries_per_bucket)[bucket]++;
  }
  size_t sub_index_size = 0;
  for (auto entry_count : *entries_per_bucket) {
    if (entry_count <= 1) {
      continue;
    }
    // Only buckets with more than 1 entry will have subindex.
    sub_index_size += VarintLength(entry_count);
    // total bytes needed to store these entries' in-file offsets.
    sub_index_size += entry_count * kOffsetLen;
  }
  return sub_index_size;
}

void PlainTableReader::FillIndexes(
    const size_t kSubIndexSize,
    const std::vector<IndexRecord*>& hash_to_offsets,
    const std::vector<uint32_t>& entries_per_bucket) {
  Log(options_.info_log, "Reserving %zu bytes for plain table's sub_index",
      kSubIndexSize);
  auto total_allocate_size = sizeof(uint32_t) * index_size_ + kSubIndexSize;
  char* allocated =
      arena_.AllocateAligned(total_allocate_size, huge_page_tlb_size_);
  index_ = reinterpret_cast<uint32_t*>(allocated);
  sub_index_ = allocated + sizeof(uint32_t) * index_size_;

  size_t sub_index_offset = 0;
  for (int i = 0; i < index_size_; i++) {
    uint32_t num_keys_for_bucket = entries_per_bucket[i];
    switch (num_keys_for_bucket) {
    case 0:
      // No key for bucket
      index_[i] = data_end_offset_;
      break;
    case 1:
      // point directly to the file offset
      index_[i] = hash_to_offsets[i]->offset;
      break;
    default:
      // point to second level indexes.
      index_[i] = sub_index_offset | kSubIndexMask;
      char* prev_ptr = &sub_index_[sub_index_offset];
      char* cur_ptr = EncodeVarint32(prev_ptr, num_keys_for_bucket);
      sub_index_offset += (cur_ptr - prev_ptr);
      char* sub_index_pos = &sub_index_[sub_index_offset];
      IndexRecord* record = hash_to_offsets[i];
      int j;
      for (j = num_keys_for_bucket - 1; j >= 0 && record;
           j--, record = record->next) {
        EncodeFixed32(sub_index_pos + j * sizeof(uint32_t), record->offset);
      }
      assert(j == -1 && record == nullptr);
      sub_index_offset += kOffsetLen * num_keys_for_bucket;
      assert(sub_index_offset <= kSubIndexSize);
      break;
    }
  }
  assert(sub_index_offset == kSubIndexSize);

  Log(options_.info_log, "hash table size: %d, suffix_map length %zu",
      index_size_, kSubIndexSize);
}

Status PlainTableReader::PopulateIndex(TableProperties* props) {
  assert(props != nullptr);
  table_properties_.reset(props);

  // options.prefix_extractor is requried for a hash-based look-up.
  if (options_.prefix_extractor.get() == nullptr && kHashTableRatio != 0) {
    return Status::NotSupported(
        "PlainTable requires a prefix extractor enable prefix hash mode.");
  }

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
  int num_prefixes;

  // Allocate bloom filter here for total order mode.
  if (IsTotalOrderMode()) {
    uint32_t num_bloom_bits = table_properties_->num_entries * kBloomBitsPerKey;
    if (num_bloom_bits > 0) {
      bloom_.reset(new DynamicBloom(num_bloom_bits, options_.bloom_locality, 6,
                                    nullptr, huge_page_tlb_size_));
    }
  }

  s = PopulateIndexRecordList(&record_list, &num_prefixes);
  if (!s.ok()) {
    return s;
  }
  // Calculated hash table and bloom filter size and allocate memory for indexes
  // and bloom filter based on the number of prefixes.
  AllocateIndexAndBloom(num_prefixes);

  // Bucketize all the index records to a temp data structure, in which for
  // each bucket, we generate a linked list of IndexRecord, in reversed order.
  std::vector<IndexRecord*> hash_to_offsets(index_size_, nullptr);
  std::vector<uint32_t> entries_per_bucket(index_size_, 0);
  size_t sub_index_size_needed = BucketizeIndexesAndFillBloom(
      &record_list, &hash_to_offsets, &entries_per_bucket);
  // From the temp data structure, populate indexes.
  FillIndexes(sub_index_size_needed, hash_to_offsets, entries_per_bucket);

  // Fill two table properties.
  // TODO(sdong): after we have the feature of storing index in file, this
  // properties need to be populated to index_size instead.
  props->user_collected_properties["plain_table_hash_table_size"] =
      std::to_string(index_size_ * 4U);
  props->user_collected_properties["plain_table_sub_index_size"] =
      std::to_string(sub_index_size_needed);

  return Status::OK();
}

Status PlainTableReader::GetOffset(const Slice& target, const Slice& prefix,
                                   uint32_t prefix_hash, bool& prefix_matched,
                                   uint32_t* offset) const {
  prefix_matched = false;
  int bucket = GetBucketIdFromHash(prefix_hash, index_size_);
  uint32_t bucket_value = index_[bucket];
  if (bucket_value == data_end_offset_) {
    *offset = data_end_offset_;
    return Status::OK();
  } else if ((bucket_value & kSubIndexMask) == 0) {
    // point directly to the file
    *offset = bucket_value;
    return Status::OK();
  }

  // point to sub-index, need to do a binary search
  uint32_t low = 0;
  uint64_t prefix_index_offset = bucket_value ^ kSubIndexMask;

  const char* index_ptr = &sub_index_[prefix_index_offset];
  uint32_t upper_bound = 0;
  const char* base_ptr = GetVarint32Ptr(index_ptr, index_ptr + 4, &upper_bound);
  uint32_t high = upper_bound;
  ParsedInternalKey mid_key;
  ParsedInternalKey parsed_target;
  if (!ParseInternalKey(target, &parsed_target)) {
    return Status::Corruption(Slice());
  }

  // The key is between [low, high). Do a binary search between it.
  while (high - low > 1) {
    uint32_t mid = (high + low) / 2;
    uint32_t file_offset = GetFixed32Element(base_ptr, mid);
    size_t tmp;
    Status s = ReadKey(file_data_.data() + file_offset, &mid_key, &tmp);
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
        *offset = file_offset;
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
  uint32_t low_key_offset = GetFixed32Element(base_ptr, low);
  Status s = ReadKey(file_data_.data() + low_key_offset, &low_key, &tmp);
  if (GetPrefix(low_key) == prefix) {
    prefix_matched = true;
    *offset = low_key_offset;
  } else if (low + 1 < upper_bound) {
    // There is possible a next prefix, return it
    prefix_matched = false;
    *offset = GetFixed32Element(base_ptr, low + 1);
  } else {
    // target is larger than a key of the last prefix in this bucket
    // but with a different prefix. Key does not exist.
    *offset = data_end_offset_;
  }
  return Status::OK();
}

bool PlainTableReader::MatchBloom(uint32_t hash) const {
  return bloom_.get() == nullptr || bloom_->MayContainHash(hash);
}

Slice PlainTableReader::GetPrefix(const ParsedInternalKey& target) const {
  return GetPrefixFromUserKey(target.user_key);
}

Status PlainTableReader::ReadKey(const char* start, ParsedInternalKey* key,
                                 size_t* bytes_read) const {
  const char* key_ptr = nullptr;
  *bytes_read = 0;
  size_t user_key_size = 0;
  if (IsFixedLength()) {
    user_key_size = user_key_len_;
    key_ptr = start;
  } else {
    uint32_t tmp_size = 0;
    key_ptr =
        GetVarint32Ptr(start, file_data_.data() + data_end_offset_, &tmp_size);
    if (key_ptr == nullptr) {
      return Status::Corruption(
          "Unexpected EOF when reading the next key's size");
    }
    user_key_size = (size_t)tmp_size;
    *bytes_read = key_ptr - start;
  }
  if (key_ptr + user_key_size + 1 >= file_data_.data() + data_end_offset_) {
    return Status::Corruption("Unexpected EOF when reading the next key");
  }

  if (*(key_ptr + user_key_size) == PlainTableFactory::kValueTypeSeqId0) {
    // Special encoding for the row with seqID=0
    key->user_key = Slice(key_ptr, user_key_size);
    key->sequence = 0;
    key->type = kTypeValue;
    *bytes_read += user_key_size + 1;
  } else {
    if (start + user_key_size + 8 >= file_data_.data() + data_end_offset_) {
      return Status::Corruption(
          "Unexpected EOF when reading internal bytes of the next key");
    }
    if (!ParseInternalKey(Slice(key_ptr, user_key_size + 8), key)) {
      return Status::Corruption(
          Slice("Incorrect value type found when reading the next key"));
    }
    *bytes_read += user_key_size + 8;
  }

  return Status::OK();
}

Status PlainTableReader::Next(uint32_t* offset, ParsedInternalKey* key,
                              Slice* value) const {
  if (*offset == data_end_offset_) {
    *offset = data_end_offset_;
    return Status::OK();
  }

  if (*offset > data_end_offset_) {
    return Status::Corruption("Offset is out of file size");
  }

  const char* start = file_data_.data() + *offset;
  size_t bytes_for_key;
  Status s = ReadKey(start, key, &bytes_for_key);
  if (!s.ok()) {
    return s;
  }
  uint32_t value_size;
  const char* value_ptr = GetVarint32Ptr(
      start + bytes_for_key, file_data_.data() + data_end_offset_, &value_size);
  if (value_ptr == nullptr) {
    return Status::Corruption(
        "Unexpected EOF when reading the next value's size.");
  }
  *offset = *offset + (value_ptr - start) + value_size;
  if (*offset > data_end_offset_) {
    return Status::Corruption("Unexpected EOF when reading the next value. ");
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
  Slice prefix_slice;
  uint32_t prefix_hash;
  if (IsTotalOrderMode()) {
    // Match whole user key for bloom filter check.
    if (!MatchBloom(GetSliceHash(GetUserKey(target)))) {
      return Status::OK();
    }
    // in total order mode, there is only one bucket 0, and we always use empty
    // prefix.
    prefix_slice = Slice();
    prefix_hash = 0;
  } else {
    prefix_slice = GetPrefix(target);
    prefix_hash = GetSliceHash(prefix_slice);
    if (!MatchBloom(prefix_hash)) {
      return Status::OK();
    }
  }
  uint32_t offset;
  bool prefix_match;
  Status s =
      GetOffset(target, prefix_slice, prefix_hash, prefix_match, &offset);
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
    Status s = Next(&offset, &found_key, &found_value);
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

PlainTableIterator::PlainTableIterator(PlainTableReader* table,
                                       bool use_prefix_seek)
    : table_(table), use_prefix_seek_(use_prefix_seek) {
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
  status_ = Status::NotSupported("SeekToLast() is not supported in PlainTable");
}

void PlainTableIterator::Seek(const Slice& target) {
  // If the user doesn't set prefix seek option and we are not able to do a
  // total Seek(). assert failure.
  if (!use_prefix_seek_ && table_->index_size_ > 1) {
    assert(false);
    status_ = Status::NotSupported(
        "PlainTable cannot issue non-prefix seek unless in total order mode.");
    offset_ = next_offset_ = table_->data_end_offset_;
    return;
  }

  Slice prefix_slice = table_->GetPrefix(target);
  uint32_t prefix_hash = 0;
  // Bloom filter is ignored in total-order mode.
  if (!table_->IsTotalOrderMode()) {
    prefix_hash = GetSliceHash(prefix_slice);
    if (!table_->MatchBloom(prefix_hash)) {
      offset_ = next_offset_ = table_->data_end_offset_;
      return;
    }
  }
  bool prefix_match;
  status_ = table_->GetOffset(target, prefix_slice, prefix_hash, prefix_match,
                              &next_offset_);
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
    status_ = table_->Next(&next_offset_, &parsed_key, &value_);
    if (status_.ok()) {
      // Make a copy in this case. TODO optimize.
      key_.SetInternalKey(parsed_key);
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
  return key_.GetKey();
}

Slice PlainTableIterator::value() const {
  assert(Valid());
  return value_;
}

Status PlainTableIterator::status() const {
  return status_;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
