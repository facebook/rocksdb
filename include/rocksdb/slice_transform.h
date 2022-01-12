// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Class for specifying user-defined functions which perform a
// transformation on a slice.  It is not required that every slice
// belong to the domain and/or range of a function.  Subclasses should
// define InDomain and InRange to determine which slices are in either
// of these sets respectively.

#pragma once

#include <memory>
#include <string>

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
struct ConfigOptions;

// A SliceTransform is a generic pluggable way of transforming one string
// to another. Its primary use-case is in configuring rocksdb
// to store prefix blooms by setting prefix_extractor in
// ColumnFamilyOptions.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class SliceTransform : public Customizable {
 public:
  virtual ~SliceTransform(){};

  // Return the name of this transformation.
  virtual const char* Name() const override = 0;
  static const char* Type() { return "SliceTransform"; }

  // Creates and configures a new SliceTransform from the input options and id.
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& id,
                                 std::shared_ptr<const SliceTransform>* result);

  // Returns a string representation of this SliceTransform, representing the ID
  // and any additional properties
  std::string AsString() const;

  // Extract a prefix from a specified key. This method is called when
  // a key is inserted into the db, and the returned slice is used to
  // create a bloom filter.
  virtual Slice Transform(const Slice& key) const = 0;

  // Determine whether the specified key is compatible with the logic
  // specified in the Transform method. This method is invoked for every
  // key that is inserted into the db. If this method returns true,
  // then Transform is called to translate the key to its prefix and
  // that returned prefix is inserted into the bloom filter. If this
  // method returns false, then the call to Transform is skipped and
  // no prefix is inserted into the bloom filters.
  //
  // For example, if the Transform method operates on a fixed length
  // prefix of size 4, then an invocation to InDomain("abc") returns
  // false because the specified key length(3) is shorter than the
  // prefix size of 4.
  //
  // Wiki documentation here:
  // https://github.com/facebook/rocksdb/wiki/Prefix-Seek
  //
  virtual bool InDomain(const Slice& key) const = 0;

  // This is currently not used and remains here for backward compatibility.
  virtual bool InRange(const Slice& /*dst*/) const { return false; }

  // Some SliceTransform will have a full length which can be used to
  // determine if two keys are consecutive. Can be disabled by always
  // returning 0
  virtual bool FullLengthEnabled(size_t* /*len*/) const { return false; }

  // Transform(s)=Transform(`prefix`) for any s with `prefix` as a prefix.
  //
  // This function is not used by RocksDB, but for users. If users pass
  // Options by string to RocksDB, they might not know what prefix extractor
  // they are using. This function is to help users can determine:
  //   if they want to iterate all keys prefixing `prefix`, whether it is
  //   safe to use prefix bloom filter and seek to key `prefix`.
  // If this function returns true, this means a user can Seek() to a prefix
  // using the bloom filter. Otherwise, user needs to skip the bloom filter
  // by setting ReadOptions.total_order_seek = true.
  //
  // Here is an example: Suppose we implement a slice transform that returns
  // the first part of the string after splitting it using delimiter ",":
  // 1. SameResultWhenAppended("abc,") should return true. If applying prefix
  //    bloom filter using it, all slices matching "abc:.*" will be extracted
  //    to "abc,", so any SST file or memtable containing any of those key
  //    will not be filtered out.
  // 2. SameResultWhenAppended("abc") should return false. A user will not
  //    guaranteed to see all the keys matching "abc.*" if a user seek to "abc"
  //    against a DB with the same setting. If one SST file only contains
  //    "abcd,e", the file can be filtered out and the key will be invisible.
  //
  // i.e., an implementation always returning false is safe.
  virtual bool SameResultWhenAppended(const Slice& /*prefix*/) const {
    return false;
  }
};

// The prefix is the first `prefix_len` bytes of the key, and keys shorter
// then `prefix_len` are not InDomain.
extern const SliceTransform* NewFixedPrefixTransform(size_t prefix_len);

// The prefix is the first min(length(key),`cap_len`) bytes of the key, and
// all keys are InDomain.
extern const SliceTransform* NewCappedPrefixTransform(size_t cap_len);

// Prefix is equal to key. All keys are InDomain.
extern const SliceTransform* NewNoopTransform();

}  // namespace ROCKSDB_NAMESPACE
