// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <unordered_map>
#include <memory>
#include <vector>
#include <string>
#include <stdint.h>

#include "db/dbformat.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "table/table_reader.h"
#include "table/plain_table_factory.h"

namespace rocksdb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;
class TableReader;
class DynamicBloom;
class InternalKeyComparator;

using std::unique_ptr;
using std::unordered_map;
extern const uint32_t kPlainTableVariableLength;

// Based on following output file format shown in plain_table_factory.h
// When opening the output file, IndexedTableReader creates a hash table
// from key prefixes to offset of the output file. IndexedTable will decide
// whether it points to the data offset of the first key with the key prefix
// or the offset of it. If there are too many keys share this prefix, it will
// create a binary search-able index from the suffix to offset on disk.
//
// The implementation of IndexedTableReader requires output file is mmaped
class PlainTableReader: public TableReader {
 public:
  static Status Open(const Options& options, const EnvOptions& soptions,
                     const InternalKeyComparator& internal_comparator,
                     unique_ptr<RandomAccessFile>&& file, uint64_t file_size,
                     unique_ptr<TableReader>* table,
                     const int bloom_bits_per_key, double hash_table_ratio);

  bool PrefixMayMatch(const Slice& internal_prefix);

  Iterator* NewIterator(const ReadOptions&);

  Status Get(const ReadOptions&, const Slice& key, void* arg,
             bool (*result_handler)(void* arg, const ParsedInternalKey& k,
                                    const Slice& v, bool),
             void (*mark_key_may_exist)(void*) = nullptr);

  uint64_t ApproximateOffsetOf(const Slice& key);

  void SetupForCompaction();

  std::shared_ptr<const TableProperties> GetTableProperties() const {
    return table_properties_;
  }

  PlainTableReader(const EnvOptions& storage_options,
                   const InternalKeyComparator& internal_comparator,
                   uint64_t file_size, int bloom_num_bits,
                   double hash_table_ratio,
                   const TableProperties* table_properties);
  ~PlainTableReader();

 private:
  struct IndexRecord;
  class IndexRecordList;

  uint32_t* hash_table_ = nullptr;
  int hash_table_size_ = 0;
  char* sub_index_ = nullptr;

  Options options_;
  const EnvOptions& soptions_;
  const InternalKeyComparator internal_comparator_;
  Status status_;
  unique_ptr<RandomAccessFile> file_;

  Slice file_data_;
  uint32_t version_;
  uint32_t file_size_;

  const double kHashTableRatio;
  const int kBloomBitsPerKey;
  DynamicBloom* bloom_ = nullptr;

  std::shared_ptr<const TableProperties> table_properties_;
  const uint32_t data_start_offset_ = 0;
  const uint32_t data_end_offset_;
  const size_t user_key_len_;

  static const size_t kNumInternalBytes = 8;
  static const uint32_t kSubIndexMask = 0x80000000;
  static const size_t kOffsetLen = sizeof(uint32_t);
  static const uint64_t kMaxFileSize = 1u << 31;
  static const size_t kRecordsPerGroup = 256;
  // To speed up the search for keys with same prefix, we'll add index key for
  // every N keys, where the "N" is determined by
  // kIndexIntervalForSamePrefixKeys
  static const size_t kIndexIntervalForSamePrefixKeys = 16;

  bool IsFixedLength() const {
    return user_key_len_ != kPlainTableVariableLength;
  }

  size_t GetFixedInternalKeyLength() const {
    return user_key_len_ + kNumInternalBytes;
  }

  friend class TableCache;
  friend class PlainTableIterator;

  // Internal helper function to generate an IndexRecordList object from all
  // the rows, which contains index records as a list.
  int PopulateIndexRecordList(IndexRecordList* record_list);

  // Internal helper function to allocate memory for indexes and bloom filters
  void AllocateIndexAndBloom(int num_prefixes);

  // Internal helper function to bucket index record list to hash buckets.
  // hash_to_offsets is sized of of hash_table_size_, each contains a linked
  // list
  // of offsets for the hash, in reversed order.
  // bucket_count is sized of hash_table_size_. The value is how many index
  // records are there in hash_to_offsets for the same bucket.
  size_t BucketizeIndexesAndFillBloom(
      IndexRecordList& record_list, int num_prefixes,
      std::vector<IndexRecord*>* hash_to_offsets,
      std::vector<uint32_t>* bucket_count);

  // Internal helper class to fill the indexes and bloom filters to internal
  // data structures. hash_to_offsets and bucket_count are bucketized indexes
  // and counts generated by BucketizeIndexesAndFillBloom().
  void FillIndexes(size_t sub_index_size_needed,
                   const std::vector<IndexRecord*>& hash_to_offsets,
                   const std::vector<uint32_t>& bucket_count);

  // PopulateIndex() builds index of keys. It must be called before any query
  // to the table.
  //
  // hash_table_ contains buckets size of hash_table_size_, each is a 32-bit
  // integer. The lower 31 bits contain an offset value (explained below) and
  // the first bit of the integer indicates type of the offset.
  //
  // +--------------+------------------------------------------------------+
  // | Flag (1 bit) | Offset to binary search buffer or file (31 bits)     +
  // +--------------+------------------------------------------------------+
  //
  // Explanation for the "flag bit":
  //
  // 0 indicates that the bucket contains only one prefix (no conflict when
  //   hashing this prefix), whose first row starts from this offset of the
  // file.
  // 1 indicates that the bucket contains more than one prefixes, or there
  //   are too many rows for one prefix so we need a binary search for it. In
  //   this case, the offset indicates the offset of sub_index_ holding the
  //   binary search indexes of keys for those rows. Those binary search indexes
  //   are organized in this way:
  //
  // The first 4 bytes, indicate how many indexes (N) are stored after it. After
  // it, there are N 32-bit integers, each points of an offset of the file,
  // which
  // points to starting of a row. Those offsets need to be guaranteed to be in
  // ascending order so the keys they are pointing to are also in ascending
  // order
  // to make sure we can use them to do binary searches. Below is visual
  // presentation of a bucket.
  //
  // <begin>
  //   number_of_records:  varint32
  //   record 1 file offset:  fixedint32
  //   record 2 file offset:  fixedint32
  //    ....
  //   record N file offset:  fixedint32
  // <end>
  Status PopulateIndex();

  // Check bloom filter to see whether it might contain this prefix.
  // The hash of the prefix is given, since it can be reused for index lookup
  // too.
  bool MayHavePrefix(uint32_t hash);

  Status ReadKey(const char* row_ptr, ParsedInternalKey* key,
                 size_t& bytes_read);
  // Read the key and value at offset to key and value.
  // tmp_slice is a tmp slice.
  // return next_offset as the offset for the next key.
  Status Next(uint32_t offset, ParsedInternalKey* key, Slice* value,
              uint32_t& next_offset);
  // Get file offset for key target.
  // return value prefix_matched is set to true if the offset is confirmed
  // for a key with the same prefix as target.
  Status GetOffset(const Slice& target, const Slice& prefix,
                   uint32_t prefix_hash, bool& prefix_matched,
                   uint32_t& ret_offset);

  Slice GetPrefix(const Slice& target) {
    assert(target.size() >= 8);  // target is internal key
    return options_.prefix_extractor->Transform(
        Slice(target.data(), target.size() - 8));
  }

  Slice GetPrefix(const ParsedInternalKey& target);

  // No copying allowed
  explicit PlainTableReader(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;
};
}  // namespace rocksdb
