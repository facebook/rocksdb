//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE
#include "table/cuckoo_table_builder.h"

#include <assert.h>
#include <algorithm>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/env.h"
#include "rocksdb/table.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "util/autovector.h"
#include "util/random.h"

namespace rocksdb {
const std::string CuckooTablePropertyNames::kEmptyBucket =
      "rocksdb.cuckoo.bucket.empty.bucket";
const std::string CuckooTablePropertyNames::kNumHashTable =
      "rocksdb.cuckoo.hash.num";
const std::string CuckooTablePropertyNames::kMaxNumBuckets =
      "rocksdb.cuckoo.bucket.maxnum";

// Obtained by running echo rocksdb.table.cuckoo | sha1sum
extern const uint64_t kCuckooTableMagicNumber = 0x926789d0c5f17873ull;

CuckooTableBuilder::CuckooTableBuilder(
    WritableFile* file, unsigned int fixed_key_length,
    unsigned int fixed_value_length, double hash_table_ratio,
    unsigned int file_size, unsigned int max_num_hash_table,
    unsigned int max_search_depth,
    unsigned int (*GetSliceHashPtr)(const Slice&, unsigned int,
      unsigned int))
    : num_hash_table_(std::min((unsigned int) 4, max_num_hash_table)),
      file_(file),
      key_length_(fixed_key_length),
      value_length_(fixed_value_length),
      bucket_size_(fixed_key_length + fixed_value_length),
      hash_table_ratio_(hash_table_ratio),
      max_num_buckets_(file_size / bucket_size_),
      max_num_hash_table_(max_num_hash_table),
      max_search_depth_(max_search_depth),
      buckets_(max_num_buckets_),
      GetSliceHash(GetSliceHashPtr) {
  // The bucket_size is currently not optimized for last level.
  // In last level, the bucket will not contain full key.
  // TODO(rbs): Find how we can determine if last level or not
  // before we start adding entries into the table.
  properties_.num_entries = 0;
  // Data is in a huge block.
  properties_.num_data_blocks = 1;
  properties_.index_size = 0;
  properties_.filter_size = 0;
}

CuckooTableBuilder::~CuckooTableBuilder() {
}

void CuckooTableBuilder::Add(const Slice& key, const Slice& value) {
  if (NumEntries() == max_num_buckets_) {
    status_ = Status::Corruption("Hash Table is full.");
    return;
  }
  unsigned int bucket_id;
  bool bucket_found = false;
  autovector<unsigned int> hash_vals;
  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    status_ = Status::Corruption("Unable to parse key into inernal key.");
    return;
  }
  Slice user_key = ikey.user_key;
  for (unsigned int hash_cnt = 0; hash_cnt < num_hash_table_; ++hash_cnt) {
    unsigned int hash_val = GetSliceHash(user_key, hash_cnt, max_num_buckets_);
    if (buckets_[hash_val].is_empty) {
      bucket_id = hash_val;
      bucket_found = true;
      break;
    } else {
      if (user_key.compare(ExtractUserKey(buckets_[hash_val].key)) == 0) {
        status_ = Status::Corruption("Same key is being inserted again.");
        return;
      }
      hash_vals.push_back(hash_val);
    }
  }
  while (!bucket_found && !MakeSpaceForKey(key, &bucket_id, hash_vals)) {
    // Rehash by increashing number of hash tables.
    if (num_hash_table_ >= max_num_hash_table_) {
      status_ = Status::Corruption("Too many collissions. Unable to hash.");
      return;
    }
    // We don't really need to rehash the entire table because old hashes are
    // still valid and we only increased the number of hash functions.
    unsigned int old_num_hash = num_hash_table_;
    num_hash_table_ = std::min(num_hash_table_ + 1, max_num_hash_table_);
    for (unsigned int i = old_num_hash; i < num_hash_table_; i++) {
      unsigned int hash_val = GetSliceHash(user_key, i, max_num_buckets_);
      if (buckets_[hash_val].is_empty) {
        bucket_found = true;
        bucket_id = hash_val;
        break;
      } else {
        hash_vals.push_back(hash_val);
      }
    }
  }
  buckets_[bucket_id].key = key;
  buckets_[bucket_id].value = value;
  buckets_[bucket_id].is_empty = false;

  if (ikey.sequence != 0) {
    // This is not a last level file.
    is_last_level_file_ = false;
  }
  properties_.num_entries++;

  // We assume that the keys are inserted in sorted order. To identify an
  // unused key, which will be used in filling empty buckets in the table,
  // we try to find gaps between successive keys inserted. This is done by
  // maintaining the previous key and comparing it with next key.
  if (unused_user_key_.empty()) {
    if (prev_key_.empty()) {
      prev_key_ = user_key.ToString();
      return;
    }
    std::string new_user_key = prev_key_;
    new_user_key.back()++;
    // We ignore carry-overs and check that it is larger than previous key.
    if ((new_user_key > prev_key_) &&
        (new_user_key < user_key.ToString())) {
      unused_user_key_ = new_user_key;
    } else {
      prev_key_ = user_key.ToString();
    }
  }
}

Status CuckooTableBuilder::status() const { return status_; }

Status CuckooTableBuilder::Finish() {
  assert(!closed_);
  closed_ = true;

  if (unused_user_key_.empty()) {
    if (prev_key_.empty()) {
      return Status::Corruption("Unable to find unused key");
    }
    std::string new_user_key = prev_key_;
    new_user_key.back()++;
    // We ignore carry-overs and check that it is larger than previous key.
    if (new_user_key > prev_key_) {
      unused_user_key_ = new_user_key;
    } else {
      return Status::Corruption("Unable to find unused key");
    }
  }
  std::string unused_bucket;
  if (is_last_level_file_) {
    unused_bucket = unused_user_key_;
  } else {
    ParsedInternalKey ikey(unused_user_key_, 0, kTypeValue);
    AppendInternalKey(&unused_bucket, ikey);
  }
  properties_.fixed_key_len = unused_bucket.size();
  unsigned int bucket_size = unused_bucket.size() + value_length_;
  // Resize to bucket size.
  unused_bucket.resize(bucket_size, 'a');

  // Write the table.
  for (auto& bucket : buckets_) {
    Status s;
    if (bucket.is_empty) {
      s = file_->Append(Slice(unused_bucket));
    } else {
      if (is_last_level_file_) {
        Slice user_key = ExtractUserKey(bucket.key);
        s = file_->Append(user_key);
        if (s.ok()) {
          s = file_->Append(bucket.value);
        }
      } else {
        s = file_->Append(bucket.key);
        if (s.ok()) {
          s = file_->Append(bucket.value);
        }
      }
    }
    if (!s.ok()) {
      return s;
    }
  }

  unsigned int offset = buckets_.size() * bucket_size;
  properties_.user_collected_properties[
    CuckooTablePropertyNames::kEmptyBucket] = unused_bucket;
  properties_.user_collected_properties[
    CuckooTablePropertyNames::kNumHashTable] = std::to_string(num_hash_table_);
  PutVarint32(&properties_.user_collected_properties[
    CuckooTablePropertyNames::kMaxNumBuckets], max_num_buckets_);

  // Write meta blocks.
  MetaIndexBuilder meta_index_builer;
  PropertyBlockBuilder property_block_builder;

  property_block_builder.AddTableProperty(properties_);
  property_block_builder.Add(properties_.user_collected_properties);
  Slice property_block = property_block_builder.Finish();
  BlockHandle property_block_handle;
  property_block_handle.set_offset(offset);
  property_block_handle.set_size(property_block.size());
  Status s = file_->Append(property_block);
  offset += property_block.size();
  if (!s.ok()) {
    return s;
  }

  meta_index_builer.Add(kPropertiesBlock, property_block_handle);
  Slice meta_index_block = meta_index_builer.Finish();

  BlockHandle meta_index_block_handle;
  meta_index_block_handle.set_offset(offset);
  meta_index_block_handle.set_size(meta_index_block.size());
  s = file_->Append(meta_index_block);
  if (!s.ok()) {
    return s;
  }

  Footer footer(kCuckooTableMagicNumber);
  footer.set_metaindex_handle(meta_index_block_handle);
  footer.set_index_handle(BlockHandle::NullBlockHandle());
  std::string footer_encoding;
  footer.EncodeTo(&footer_encoding);
  s = file_->Append(footer_encoding);
  return s;
}

void CuckooTableBuilder::Abandon() {
  assert(!closed_);
  closed_ = true;
}

uint64_t CuckooTableBuilder::NumEntries() const {
  return properties_.num_entries;
}

uint64_t CuckooTableBuilder::FileSize() const {
  if (closed_) {
    return file_->GetFileSize();
  } else {
    // This is not the actual size of the file as we need to account for
    // hash table ratio. This returns the size of filled buckets in the table
    // scaled up by a factor of 1/hash table ratio.
    return (properties_.num_entries * bucket_size_) / hash_table_ratio_;
  }
}

bool CuckooTableBuilder::MakeSpaceForKey(const Slice& key,
    unsigned int *bucket_id, autovector<unsigned int> hash_vals) {
  struct CuckooNode {
    unsigned int bucket_id;
    unsigned int depth;
    int parent_pos;
    CuckooNode(unsigned int bucket_id, unsigned int depth, int parent_pos)
      : bucket_id(bucket_id), depth(depth), parent_pos(parent_pos) {}
  };
  // This is BFS search tree that is stored simply as a vector.
  // Each node stores the index of parent node in the vector.
  std::vector<CuckooNode> tree;
  // This is a very bad way to keep track of visited nodes.
  // TODO(rbs): Change this by adding a 'GetKeyPathId' field to the bucket
  // and use it to track visited nodes.
  std::vector<bool> buckets_visited(max_num_buckets_, false);
  for (unsigned int hash_cnt = 0; hash_cnt < num_hash_table_; ++hash_cnt) {
    unsigned int bucket_id = hash_vals[hash_cnt];
    buckets_visited[bucket_id] = true;
    tree.push_back(CuckooNode(bucket_id, 0, -1));
  }
  bool null_found = false;
  unsigned int curr_pos = 0;
  while (!null_found && curr_pos < tree.size()) {
    CuckooNode& curr_node = tree[curr_pos];
    if (curr_node.depth >= max_search_depth_) {
      break;
    }
    CuckooBucket& curr_bucket = buckets_[curr_node.bucket_id];
    for (unsigned int hash_cnt = 0; hash_cnt < num_hash_table_; ++hash_cnt) {
      unsigned int child_bucket_id = GetSliceHash(
          ExtractUserKey(curr_bucket.key), hash_cnt, max_num_buckets_);
      if (child_bucket_id == curr_node.bucket_id) {
        continue;
      }
      if (buckets_visited[child_bucket_id]) {
        continue;
      }
      buckets_visited[child_bucket_id] = true;
      tree.push_back(CuckooNode(child_bucket_id, curr_node.depth + 1,
            curr_pos));
      if (buckets_[child_bucket_id].is_empty) {
        null_found = true;
        break;
      }
    }
    ++curr_pos;
  }

  if (null_found) {
    int bucket_to_replace_pos = tree.size()-1;
    while (bucket_to_replace_pos >= 0) {
      CuckooNode& curr_node = tree[bucket_to_replace_pos];
      if (curr_node.parent_pos != -1) {
        buckets_[curr_node.bucket_id] = buckets_[curr_node.parent_pos];
        bucket_to_replace_pos = curr_node.parent_pos;
      } else {
        *bucket_id = curr_node.bucket_id;
        return true;
      }
    }
    return true;
  } else {
    return false;
  }
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
