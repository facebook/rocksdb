//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
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
const std::string CuckooTablePropertyNames::kEmptyKey =
      "rocksdb.cuckoo.bucket.empty.key";
const std::string CuckooTablePropertyNames::kNumHashTable =
      "rocksdb.cuckoo.hash.num";
const std::string CuckooTablePropertyNames::kMaxNumBuckets =
      "rocksdb.cuckoo.bucket.maxnum";
const std::string CuckooTablePropertyNames::kValueLength =
      "rocksdb.cuckoo.value.length";
const std::string CuckooTablePropertyNames::kIsLastLevel =
      "rocksdb.cuckoo.file.islastlevel";

// Obtained by running echo rocksdb.table.cuckoo | sha1sum
extern const uint64_t kCuckooTableMagicNumber = 0x926789d0c5f17873ull;

CuckooTableBuilder::CuckooTableBuilder(
    WritableFile* file, uint32_t fixed_key_length,
    uint32_t fixed_value_length, double hash_table_ratio,
    uint64_t file_size, uint32_t max_num_hash_table,
    uint32_t max_search_depth, bool is_last_level,
    uint64_t (*GetSliceHashPtr)(const Slice&, uint32_t, uint64_t))
    : num_hash_table_(2),
      file_(file),
      value_length_(fixed_value_length),
      // 8 is the difference between sizes of user key and InternalKey.
      bucket_size_(fixed_key_length +
          fixed_value_length - (is_last_level ? 8 : 0)),
      hash_table_ratio_(hash_table_ratio),
      max_num_buckets_(file_size / bucket_size_),
      max_num_hash_table_(max_num_hash_table),
      max_search_depth_(max_search_depth),
      is_last_level_file_(is_last_level),
      buckets_(max_num_buckets_),
      make_space_for_key_call_id_(0),
      GetSliceHash(GetSliceHashPtr) {
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
  uint64_t bucket_id;
  bool bucket_found = false;
  autovector<uint64_t> hash_vals;
  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    status_ = Status::Corruption("Unable to parse key into inernal key.");
    return;
  }
  Slice user_key = ikey.user_key;
  for (uint32_t hash_cnt = 0; hash_cnt < num_hash_table_; ++hash_cnt) {
    uint64_t hash_val = GetSliceHash(user_key, hash_cnt, max_num_buckets_);
    if (buckets_[hash_val].is_empty) {
      bucket_id = hash_val;
      bucket_found = true;
      break;
    } else {
      if (user_key.compare(
            is_last_level_file_ ? Slice(buckets_[hash_val].key)
            : ExtractUserKey(Slice(buckets_[hash_val].key))) == 0) {
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
    uint64_t hash_val = GetSliceHash(user_key,
        num_hash_table_, max_num_buckets_);
    ++num_hash_table_;
    if (buckets_[hash_val].is_empty) {
      bucket_found = true;
      bucket_id = hash_val;
      break;
    } else {
      hash_vals.push_back(hash_val);
    }
  }
  if (is_last_level_file_) {
    buckets_[bucket_id].key.assign(user_key.data(), user_key.size());
  } else {
    buckets_[bucket_id].key.assign(key.data(), key.size());
  }
  buckets_[bucket_id].value.assign(value.data(), value.size());
  buckets_[bucket_id].is_empty = false;

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
    // Try to find the key next to prev_key_ by handling carryovers.
    std::string new_user_key = prev_key_;
    int curr_pos = new_user_key.size() - 1;
    while (curr_pos >= 0) {
      ++new_user_key[curr_pos];
      if (new_user_key > prev_key_) {
        unused_user_key_ = new_user_key;
        break;
      }
      --curr_pos;
    }
    if (curr_pos < 0) {
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
  properties_.user_collected_properties[
        CuckooTablePropertyNames::kValueLength].assign(
        reinterpret_cast<const char*>(&value_length_), sizeof(value_length_));

  unused_bucket.resize(bucket_size_, 'a');
  // Write the table.
  uint32_t num_added = 0;
  for (auto& bucket : buckets_) {
    Status s;
    if (bucket.is_empty) {
      s = file_->Append(Slice(unused_bucket));
    } else {
      ++num_added;
      s = file_->Append(Slice(bucket.key));
      if (s.ok()) {
        s = file_->Append(Slice(bucket.value));
      }
    }
    if (!s.ok()) {
      return s;
    }
  }
  assert(num_added == NumEntries());

  uint64_t offset = buckets_.size() * bucket_size_;
  unused_bucket.resize(properties_.fixed_key_len);
  properties_.user_collected_properties[
    CuckooTablePropertyNames::kEmptyKey] = unused_bucket;
  properties_.user_collected_properties[
    CuckooTablePropertyNames::kNumHashTable].assign(
        reinterpret_cast<char*>(&num_hash_table_), sizeof(num_hash_table_));
  properties_.user_collected_properties[
    CuckooTablePropertyNames::kMaxNumBuckets].assign(
        reinterpret_cast<const char*>(&max_num_buckets_),
        sizeof(max_num_buckets_));
  properties_.user_collected_properties[
    CuckooTablePropertyNames::kIsLastLevel].assign(
        reinterpret_cast<const char*>(&is_last_level_file_),
        sizeof(is_last_level_file_));

  // Write meta blocks.
  MetaIndexBuilder meta_index_builder;
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

  meta_index_builder.Add(kPropertiesBlock, property_block_handle);
  Slice meta_index_block = meta_index_builder.Finish();

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

// This method is invoked when there is no place to insert the target key.
// It searches for a set of elements that can be moved to accommodate target
// key. The search is a BFS graph traversal with first level (hash_vals)
// being all the buckets target key could go to.
// Then, from each node (curr_node), we find all the buckets that curr_node
// could go to. They form the children of curr_node in the tree.
// We continue the traversal until we find an empty bucket, in which case, we
// move all elements along the path from first level to this empty bucket, to
// make space for target key which is inserted at first level (*bucket_id).
// If tree depth exceedes max depth, we return false indicating failure.
bool CuckooTableBuilder::MakeSpaceForKey(const Slice& key,
    uint64_t *bucket_id, autovector<uint64_t> hash_vals) {
  struct CuckooNode {
    uint64_t bucket_id;
    uint32_t depth;
    uint32_t parent_pos;
    CuckooNode(uint64_t bucket_id, uint32_t depth, int parent_pos)
      : bucket_id(bucket_id), depth(depth), parent_pos(parent_pos) {}
  };
  // This is BFS search tree that is stored simply as a vector.
  // Each node stores the index of parent node in the vector.
  std::vector<CuckooNode> tree;
  // We want to identify already visited buckets in the current method call so
  // that we don't add same buckets again for exploration in the tree.
  // We do this by maintaining a count of current method call, which acts as a
  // unique id for this invocation of the method. We store this number into
  // the nodes that we explore in current method call.
  // It is unlikely for the increment operation to overflow because the maximum
  // number of times this will be called is <= max_num_hash_table_ +
  // max_num_buckets_.
  ++make_space_for_key_call_id_;
  for (uint32_t hash_cnt = 0; hash_cnt < num_hash_table_; ++hash_cnt) {
    uint64_t bucket_id = hash_vals[hash_cnt];
    buckets_[bucket_id].make_space_for_key_call_id =
      make_space_for_key_call_id_;
    tree.push_back(CuckooNode(bucket_id, 0, 0));
  }
  bool null_found = false;
  uint32_t curr_pos = 0;
  while (!null_found && curr_pos < tree.size()) {
    CuckooNode& curr_node = tree[curr_pos];
    if (curr_node.depth >= max_search_depth_) {
      break;
    }
    CuckooBucket& curr_bucket = buckets_[curr_node.bucket_id];
    for (uint32_t hash_cnt = 0; hash_cnt < num_hash_table_; ++hash_cnt) {
      uint64_t child_bucket_id = GetSliceHash(
          is_last_level_file_ ? curr_bucket.key
          : ExtractUserKey(Slice(curr_bucket.key)),
          hash_cnt, max_num_buckets_);
      if (buckets_[child_bucket_id].make_space_for_key_call_id ==
          make_space_for_key_call_id_) {
        continue;
      }
      buckets_[child_bucket_id].make_space_for_key_call_id =
        make_space_for_key_call_id_;
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
    // There is an empty node in tree.back(). Now, traverse the path from this
    // empty node to top of the tree and at every node in the path, replace
    // child with the parent. Stop when first level is reached in the tree
    // (happens when 0 <= bucket_to_replace_pos < num_hash_table_) and return
    // this location in first level for target key to be inserted.
    uint32_t bucket_to_replace_pos = tree.size()-1;
    while (bucket_to_replace_pos >= num_hash_table_) {
      CuckooNode& curr_node = tree[bucket_to_replace_pos];
      buckets_[curr_node.bucket_id] =
        buckets_[tree[curr_node.parent_pos].bucket_id];
      bucket_to_replace_pos = curr_node.parent_pos;
    }
    *bucket_id = tree[bucket_to_replace_pos].bucket_id;
  }
  return null_found;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
