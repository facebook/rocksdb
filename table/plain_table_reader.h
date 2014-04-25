// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE
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
#include "util/arena.h"

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
                     const int bloom_bits_per_key, double hash_table_ratio,
                     size_t index_sparseness, size_t huge_page_tlb_size);

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

  PlainTableReader(const Options& options, unique_ptr<RandomAccessFile>&& file,
                   const EnvOptions& storage_options,
                   const InternalKeyComparator& internal_comparator,
                   uint64_t file_size, int bloom_num_bits,
                   double hash_table_ratio, size_t index_sparseness,
                   const TableProperties* table_properties,
                   size_t huge_page_tlb_size);
  virtual ~PlainTableReader();

 protected:
  // Check bloom filter to see whether it might contain this prefix.
  // The hash of the prefix is given, since it can be reused for index lookup
  // too.
  virtual bool MatchBloom(uint32_t hash) const;

  // PopulateIndex() builds index of keys. It must be called before any query
  // to the table.
  //
  // props: the table properties object that need to be stored. Ownership of
  //        the object will be passed.
  //
  // index_ contains buckets size of index_size_, each is a
  // 32-bit integer. The lower 31 bits contain an offset value (explained below)
  // and the first bit of the integer indicates type of the offset.
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
  Status PopulateIndex(TableProperties* props);

 private:
  struct IndexRecord;
  class IndexRecordList;

  // Plain table maintains an index and a sub index.
  // index is implemented by a hash table.
  // subindex is a big of memory array.
  // For more details about the in-memory index, please refer to:
  // https://github.com/facebook/rocksdb/wiki/PlainTable-Format
  // #wiki-in-memory-index-format
  uint32_t* index_;
  int index_size_ = 0;
  char* sub_index_;

  Options options_;
  const EnvOptions& soptions_;
  unique_ptr<RandomAccessFile> file_;

  const InternalKeyComparator internal_comparator_;
  // represents plain table's current status.
  Status status_;

  Slice file_data_;
  uint32_t file_size_;

  const double kHashTableRatio;
  const int kBloomBitsPerKey;
  // To speed up the search for keys with same prefix, we'll add index key for
  // every N keys, where the "N" is determined by
  // kIndexIntervalForSamePrefixKeys
  const size_t kIndexIntervalForSamePrefixKeys = 16;
  // Bloom filter is used to rule out non-existent key
  unique_ptr<DynamicBloom> bloom_;
  Arena arena_;

  std::shared_ptr<const TableProperties> table_properties_;
  // data_start_offset_ and data_end_offset_ defines the range of the
  // sst file that stores data.
  const uint32_t data_start_offset_ = 0;
  const uint32_t data_end_offset_;
  const size_t user_key_len_;
  const size_t huge_page_tlb_size_;

  static const size_t kNumInternalBytes = 8;
  static const uint32_t kSubIndexMask = 0x80000000;
  static const size_t kOffsetLen = sizeof(uint32_t);
  static const uint64_t kMaxFileSize = 1u << 31;
  static const size_t kRecordsPerGroup = 256;

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
  // If bloom_ is not null, all the keys' full-key hash will be added to the
  // bloom filter.
  Status PopulateIndexRecordList(IndexRecordList* record_list,
                                 int* num_prefixes) const;

  // Internal helper function to allocate memory for indexes and bloom filters
  void AllocateIndexAndBloom(int num_prefixes);

  // Internal helper function to bucket index record list to hash buckets.
  // bucket_header is a vector of size hash_table_size_, with each entry
  // containing a linklist of IndexRecord hashed to the same bucket, in reverse
  // order.
  // of offsets for the hash, in reversed order.
  // entries_per_bucket is sized of index_size_. The value is how many index
  // records are there in bucket_headers for the same bucket.
  size_t BucketizeIndexesAndFillBloom(
      IndexRecordList* record_list, std::vector<IndexRecord*>* bucket_headers,
      std::vector<uint32_t>* entries_per_bucket);

  // Internal helper class to fill the indexes and bloom filters to internal
  // data structures. bucket_headers and entries_per_bucket are bucketized
  // indexes and counts generated by BucketizeIndexesAndFillBloom().
  void FillIndexes(const size_t kSubIndexSize,
                   const std::vector<IndexRecord*>& bucket_headers,
                   const std::vector<uint32_t>& entries_per_bucket);

  // Read a plain table key from the position `start`. The read content
  // will be written to `key` and the size of read bytes will be populated
  // in `bytes_read`.
  Status ReadKey(const char* row_ptr, ParsedInternalKey* key,
                 size_t* bytes_read) const;
  // Read the key and value at `offset` to parameters `key` and `value`.
  // On success, `offset` will be updated as the offset for the next key.
  Status Next(uint32_t* offset, ParsedInternalKey* key, Slice* value) const;
  // Get file offset for key target.
  // return value prefix_matched is set to true if the offset is confirmed
  // for a key with the same prefix as target.
  Status GetOffset(const Slice& target, const Slice& prefix,
                   uint32_t prefix_hash, bool& prefix_matched,
                   uint32_t* offset) const;

  Slice GetUserKey(const Slice& key) const {
    return Slice(key.data(), key.size() - 8);
  }

  Slice GetPrefix(const Slice& target) const {
    assert(target.size() >= 8);  // target is internal key
    return GetPrefixFromUserKey(GetUserKey(target));
  }

  inline Slice GetPrefix(const ParsedInternalKey& target) const;

  Slice GetPrefixFromUserKey(const Slice& user_key) const {
    if (!IsTotalOrderMode()) {
      return options_.prefix_extractor->Transform(user_key);
    } else {
      // Use empty slice as prefix if prefix_extractor is not set. In that case,
      // it falls back to pure binary search and total iterator seek is
      // supported.
      return Slice();
    }
  }

  bool IsTotalOrderMode() const {
    return (options_.prefix_extractor.get() == nullptr);
  }

  // No copying allowed
  explicit PlainTableReader(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;
};
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
