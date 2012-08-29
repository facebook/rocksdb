// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
#define STORAGE_LEVELDB_INCLUDE_STATISTICS_H_

namespace leveldb {

// Analyze the performance of a db
class Statistics {
 public: 
  // Create an Statistics object with default values for all fields.
  Statistics() : numFileOpens_(0), numFileCloses_(0),
                 numFileErrors_(0) {}

  virtual void incNumFileOpens() = 0;
  virtual void incNumFileCloses() = 0;
  virtual void incNumFileErrors() = 0;

  virtual long getNumFileOpens() { return numFileOpens_;}
  virtual long getNumFileCloses() { return numFileCloses_;}
  virtual long getNumFileErrors() { return numFileErrors_;}
  virtual ~Statistics() {}

 protected:
  long numFileOpens_;
  long numFileCloses_;
  long numFileErrors_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
