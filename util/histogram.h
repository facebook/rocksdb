// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_HISTOGRAM_H_
#define STORAGE_LEVELDB_UTIL_HISTOGRAM_H_

#include "rocksdb/statistics.h"

#include <cassert>
#include <string>
#include <vector>
#include <map>

namespace leveldb {

class HistogramBucketMapper {
 public:

  HistogramBucketMapper();

  // converts a value to the bucket index.
  const size_t IndexForValue(const uint64_t value) const;
  // number of buckets required.

  const size_t BucketCount() const {
    return bucketValues_.size();
  }

  uint64_t LastValue() const {
    return maxBucketValue_;
  }

  uint64_t FirstValue() const {
    return minBucketValue_;
  }

  uint64_t BucketLimit(const uint64_t bucketNumber) const {
    assert(bucketNumber < BucketCount());
    return bucketValues_[bucketNumber];
  }

 private:
  const std::vector<uint64_t> bucketValues_;
  const uint64_t maxBucketValue_;
  const uint64_t minBucketValue_;
  std::map<uint64_t, uint64_t> valueIndexMap_;
};

class HistogramImpl {
 public:
  HistogramImpl();
  virtual ~HistogramImpl() {}
  virtual void Clear();
  virtual void Add(uint64_t value);
  void Merge(const HistogramImpl& other);

  virtual std::string ToString() const;

  virtual double Median() const;
  virtual double Percentile(double p) const;
  virtual double Average() const;
  virtual double StandardDeviation() const;
  virtual void Data(HistogramData * const data) const;

 private:
  double min_;
  double max_;
  double num_;
  double sum_;
  double sum_squares_;
  std::vector<uint64_t> buckets_;

};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_HISTOGRAM_H_
