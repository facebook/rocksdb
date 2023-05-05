// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

// The general interface for comparing two Slices are defined for both of
// Comparator and some internal data structures.
class CompareInterface {
 public:
  virtual ~CompareInterface() {}

  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  // Note that Compare(a, b) also compares timestamp if timestamp size is
  // non-zero. For the same user key with different timestamps, larger (newer)
  // timestamp comes first.
  virtual int Compare(const Slice& a, const Slice& b) const = 0;
};

// A Comparator object provides a total order across slices that are
// used as keys in an sstable or a database.  A Comparator implementation
// must be thread-safe since rocksdb may invoke its methods concurrently
// from multiple threads.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class Comparator : public Customizable, public CompareInterface {
 public:
  Comparator() : timestamp_size_(0) {}

  Comparator(size_t ts_sz) : timestamp_size_(ts_sz) {}

  Comparator(const Comparator& orig) : timestamp_size_(orig.timestamp_size_) {}

  Comparator& operator=(const Comparator& rhs) {
    if (this != &rhs) {
      timestamp_size_ = rhs.timestamp_size_;
    }
    return *this;
  }

  ~Comparator() override {}

  static Status CreateFromString(const ConfigOptions& opts,
                                 const std::string& id,
                                 const Comparator** comp);
  static const char* Type() { return "Comparator"; }

  // The name of the comparator.  Used to check for comparator
  // mismatches (i.e., a DB created with one comparator is
  // accessed using a different comparator.
  //
  // The client of this package should switch to a new name whenever
  // the comparator implementation changes in a way that will cause
  // the relative ordering of any two keys to change.
  //
  // Names starting with "rocksdb." are reserved and should not be used
  // by any clients of this package.
  const char* Name() const override = 0;

  // Compares two slices for equality. The following invariant should always
  // hold (and is the default implementation):
  //   Equal(a, b) iff Compare(a, b) == 0
  // Overwrite only if equality comparisons can be done more efficiently than
  // three-way comparisons.
  virtual bool Equal(const Slice& a, const Slice& b) const {
    return Compare(a, b) == 0;
  }

  // Advanced functions: these are used to reduce the space requirements
  // for internal data structures like index blocks.

  // If *start < limit, changes *start to a short string in [start,limit).
  // Simple comparator implementations may return with *start unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const = 0;

  // Changes *key to a short string >= *key.
  // Simple comparator implementations may return with *key unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  virtual void FindShortSuccessor(std::string* key) const = 0;

  // given two keys, determine if t is the successor of s
  // BUG: only return true if no other keys starting with `t` are ordered
  // before `t`. Otherwise, the auto_prefix_mode can omit entries within
  // iterator bounds that have same prefix as upper bound but different
  // prefix from seek key.
  virtual bool IsSameLengthImmediateSuccessor(const Slice& /*s*/,
                                              const Slice& /*t*/) const {
    return false;
  }

  // return true if two keys with different byte sequences can be regarded
  // as equal by this comparator.
  // The major use case is to determine if DataBlockHashIndex is compatible
  // with the customized comparator.
  virtual bool CanKeysWithDifferentByteContentsBeEqual() const { return true; }

  // if it is a wrapped comparator, may return the root one.
  // return itself it is not wrapped.
  virtual const Comparator* GetRootComparator() const { return this; }

  inline size_t timestamp_size() const { return timestamp_size_; }

  // Compare the non-timestamp portion of keys containing timestamps.
  int CompareWithoutTimestamp(const Slice& a, const Slice& b) const {
    return CompareWithoutTimestamp(a, /*a_has_ts=*/true, b, /*b_has_ts=*/true);
  }

  // For two events e1 and e2 whose timestamps are t1 and t2 respectively,
  // Returns value:
  // < 0  iff t1 < t2
  // == 0 iff t1 == t2
  // > 0  iff t1 > t2
  // Note that an all-zero byte array will be the smallest (oldest) timestamp
  // of the same length, and a byte array with all bits 1 will be the largest.
  // In the future, we can extend Comparator so that subclasses can specify
  // both largest and smallest timestamps.
  virtual int CompareTimestamp(const Slice& /*ts1*/,
                               const Slice& /*ts2*/) const {
    return 0;
  }

  // Compare the non-timestamp portion of keys that may or may not have
  // timestamps, as specified by `a_has_ts` and `b_has_ts`. This default
  // implementation is only appropriate for comparators not using the user
  // timestamp feature. (timestamp_size_ always == 0)
  virtual int CompareWithoutTimestamp(const Slice& a, bool /*a_has_ts*/,
                                      const Slice& b, bool /*b_has_ts*/) const {
    return Compare(a, b);
  }

  virtual bool EqualWithoutTimestamp(const Slice& a, const Slice& b) const {
    return 0 ==
           CompareWithoutTimestamp(a, /*a_has_ts=*/true, b, /*b_has_ts=*/true);
  }

  // Tests key or prefix `a` for ordering with or containment in `prefix`.
  // Returning 0 => `a` is contained in `prefix`.
  // Returning < 0 => `a` is ordered before `prefix` (and all keys contained
  // in that prefix).
  // Returning > 0 => `a` is ordered after `prefix` (and all keys contained
  // in that prefix).
  // This function must form a partial order; specifically,
  //  * CompareToPrefix(a, false, a) == 0
  //  * If CompareToPrefix(a, false, b) < 0
  //    then CompareToPrefix(b, false, a) >= 0
  //  * If CompareToPrefix(a, false, b) < 0 and
  //       CompareToPrefix(b, false, c) < 0,
  //    then CompareToPrefix(a, false, c) < 0
  //
  // Note that two prefixes are equivalent iff
  // CompareToPrefix(a, false, b) == 0 && CompareToPrefix(b, false, a) == 0.
  // (One does not imply the other as in a strict order).
  //
  // RocksDB logic also requires that even if prefix bounds are not generally
  // supported, this function must return 0 if `prefix` is empty. See
  // SliceBound::kMin/kMax. A suitable implementation for "not supported" is
  // to return 0 always (every key matches every prefix, and all prefixes are
  // equivalent).
  //
  // The default implementation uses Slice::starts_with for prefix containment
  // so is appropriate for lexicographic orderings such as byte-wise orderings,
  // reverse or standard, where keys with different bytes cannot be equal.
  // It also uses `prefix` as a key (no user timestamp) passed to
  // CompareWithoutTimestamp(), so prefixes must be in the domain of comparable
  // keys with the default implementation of this function. The key may
  // contain several parsed fields, and this is implementation will work as
  // long as the order of earlier fields supercedes the order of later fields,
  // and the provided prefixes parse unambiguously to a prefix of fields. For
  // example, prefixes could end at a fixed length boundary between fields, or
  // end with a field delimiter character.
  virtual int CompareToPrefix(const Slice& a, bool a_has_ts,
                              const Slice& prefix) const;

 private:
  size_t timestamp_size_;
};

// Return a builtin comparator that uses lexicographic byte-wise ordering.
// The result remains owned by this module so must not be deleted.
extern const Comparator* BytewiseComparator();

// Return a builtin comparator that uses reverse lexicographic byte-wise
// ordering.
extern const Comparator* ReverseBytewiseComparator();

// SliceBound is a general, convenient way of specifying upper and lower
// bounds for ranges of keys. With a comparator, all keys are either strictly
// greater than or strictly less than a SliceBound (see OrderedBeforeBound()).
// This means that inclusivity vs. exclusivity of the bound is a part of and
// selectable in constructing the SliceBound. SliceBound also supports
// prefix-based upper and lower bounds under certain assumptions detailed
// below.
//
// You can think of SliceBound as generalizing the total order given by a
// comparator to include various conceptual points between actual keys. A
// total order on these various bounds is provided to support various
// operations (see CompareBounds). Here's an example of that ordering
// relationship between various kinds of SliceBounds and actual keys, using
// standard bytewise comparator:
//
// BeforePrefix("") // == SliceBound::kMin
// ...
// BeforePrefix("f")
// ...
// BeforePrefix("foo")
// BeforeKey("foo")
// +------------ Simple key "foo" (not a SliceBound) ------------+
// | ...
// | BeforeKeyWithTs("foo" + 1234)
// | Key "foo" with timestamp 1234 (not a SliceBound)
// | AfterKeyWithTs("foo" + 1234)
// | BeforeKeyWithTs("foo" + 1233)
// | Key "foo" with timestamp 1233 (not a SliceBound)
// | ...
// +-------------------------------------------------------------+
// AfterKey("foo")
// ...
// BeforePrefix("food")
// ...
// BeforeKey("food")
// Simple key "food" (not a SliceBound)
// AfterKey("food")
// ...
// AfterPrefix("food")
// ...
// AfterPrefix("foo")
// BeforePrefix("fop")
// ...
// AfterPrefix("")  // == SliceBound::kMax
//
// Notice that when using the User Timestamp feature, it is not possible to
// order a simple key such as "foo" against a timestamped bound such as
// AfterKeyWithTs("foo" + 1234). This is a documented limitation on
// OrderedBeforeBound(), but does not affect CompareBounds() because it only
// deals with SliceBounds.
//
// Prefixes for SliceBound are never considered to contain a user timestamp,
// nor any part of a user timestamp. Other than that, prefix bounds are rather
// simple for byte-wise orderings. A range from BeforePrefix("asdf") to
// AfterPrefix("asdf") exactly includes all keys starting with "asdf", whether
// using standard byte-wise or reverse byte-wise ordering. And that is just
// a subrange of BeforePrefix("as") to AfterPrefix("as"). Another range
// including the same set of keys under the standard bytewise comparator is
// AfterPrefix("ar") to BeforePrefix("at"), though this is not as intuitive
// and can cost extra in constructing strings to back the distinct Slices.
//
// Prefix bounds can generalize to other comparators with support from
// Comparator::CompareToPrefix() (see that function). The comparator might have
// a limited domain of keys (with and without timestamps); for example, it might
// assume a fixed-size key with or without an assertion. Prefixes recognized for
// bounds can have their own domain, which may or may not overlap with the
// domain of keys (without timestamps). For example, allowed prefixes might
// be a smaller fixed size, to cooperate with a prefix_extractor. Only
// CompareToPrefix() must handle comparing prefixes, and the left-operand
// of CompareToPrefix() must also handle keys in addition to prefixes.
// This establishes the relative ordering of keys against prefixes and
// inclusion of key ranges under prefix bounds.
//
// In addition to the domain handling above, the following properties must be
// satisfied for prefix bounds to be valid (to create a generalized total
// ordering):
//  * The comparator must give a total order on keys without timestamps, and
//    a total order on keys with timestamps.
//  * CompareToPrefix() must give a partial order on prefixes.
//  * For any x, x_has_ts, y, y_has_ts, and p in the appropriate domains, if
//    either
//    * CompareToPrefix(x, x_has_ts, prefix) < 0 and
//      CompareToPrefix(y, y_has_ts, prefix) >= 0, or
//    * CompareToPrefix(x, x_has_ts, prefix) <= 0 and
//      CompareToPrefix(y, y_has_ts, prefix) > 0
//    then
//    * CompareMaybeTimestamp(x, x_has_ts, y, y_has_ts) < 0
//
// This last property means the order of the prefixes has to be related to the
// order of keys, and that prefixes must encompass a contiguous range of keys,
// so include every key greater than the prefix's lower bound ("before") and
// less than the prefix's upper bound ("after"). But notably, some
// non-requirements contrast somewhat with prefix_extractor requirements:
//  * The prefix does not have to be the first nor last key in its range (nor
//    even in the domain of keys)
//  * The prefix does not have to satisfy Slice::starts_with().
//  * A key could be in the range of many distinct prefixes, even of the same
//    Slice size.
struct SliceBound {
  enum Mode {
    // Bit fields for constructing meaningful modes
    kAfterBit = 1 << 0,
    kUserTimestampBit = 1 << 1,
    kPrefixBit = 1 << 2,

    // Meaningful modes
    kBeforeKey = 0,
    kAfterKey = kAfterBit,

    kBeforeKeyWithTs = kUserTimestampBit,
    kAfterKeyWithTs = kUserTimestampBit | kAfterBit,

    kBeforePrefix = kPrefixBit,
    kAfterPrefix = kPrefixBit | kAfterBit,
  };

  // Bytes for the key or prefix
  Slice key_like;
  // What kind of SliceBound this is
  Mode mode;

  // A SliceBound ordered before everything else (that's not equivalent to it).
  // == BeforePrefix("")
  static const SliceBound kMin;
  // A SliceBound ordered after everything else (that's not equivalent to it).
  // == AfterPrefix("")
  static const SliceBound kMax;
};

// Returns true if key `a`, possibly with timestamp (`a_has_ts`), is ordered
// before bound `b` according to comparator comp. Returns false if ordered
// after. NOTE: a key cannot be "equal to" a SliceBound because bounds are
// logically between keys (or beyond either extreme).
bool OrderedBeforeBound(const Slice& a, bool a_has_ts, const SliceBound& b,
                        const Comparator& comp);

// Compares two SliceBounds according to comparator comp. This can be used to
// check for overlap in ranges. For example, if there exists a key k, k_has_ts
// for which
//  * OrderedBeforeBound(k, k_has_ts, a, comp) == false, and
//  * OrderedBeforeBound(k, k_has_ts, b, comp) == true
// then this function must return < 0 to indicate that 'a' is ordered before
// 'b'. However, just because 'a' is ordered before 'b' does not necessarily
// mean that there exists a key between the two bounds, but bounds are usually
// constructed in ways that minimize reports of overlap without any keys
// falling into that overlap. For example, in the default bytewise comparator,
// AfterPrefix("foo1") is ordered before BeforePrefix("foo2"), so the two
// prefix ranges are not reported as overlapping. See discussion on SliceBound.
int CompareBounds(const SliceBound& a, const SliceBound& b,
                  const Comparator& comp);

}  // namespace ROCKSDB_NAMESPACE
