// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#ifndef STORAGE_LEVELDB_UTIL_STORAGE_OPTIONS_H_
#define STORAGE_LEVELDB_UTIL_STORAGE_OPTIONS_H_

#include <string>
#include <stdint.h>
#include "leveldb/env.h"
#include "leveldb/options.h"

namespace leveldb {

// Environment Options that are used to read files from storage
class StorageOptions : public EnvOptions {
 public:
  /* implicit */ StorageOptions(const Options& opt) :
    data_in_os_(opt.allow_os_buffer),
    fs_readahead_(opt.allow_readahead),
    readahead_compactions_(opt.allow_readahead_compactions),
    use_mmap_reads_(opt.allow_mmap_reads),
    use_mmap_writes_(opt.allow_mmap_writes) {
  }

  // copy constructor with readaheads set to readahead_compactions_
  StorageOptions(const StorageOptions& opt) {
    data_in_os_ = opt.UseOsBuffer();
    fs_readahead_ = opt.UseReadaheadCompactions();
    readahead_compactions_ = opt.UseReadaheadCompactions();
    use_mmap_reads_ = opt.UseMmapReads();
    use_mmap_writes_ = opt.UseMmapWrites();
  }

  // constructor with default options
  StorageOptions() {
    Options opt;
    data_in_os_ = opt.allow_os_buffer;
    fs_readahead_ = opt.allow_readahead;
    readahead_compactions_ = fs_readahead_;
    use_mmap_reads_ = opt.allow_mmap_reads;
    use_mmap_writes_ = opt.allow_mmap_writes;
  }

  virtual ~StorageOptions() {}

  bool UseOsBuffer() const { return data_in_os_; };
  bool UseReadahead() const { return fs_readahead_; };
  bool UseMmapReads() const { return use_mmap_reads_; }
  bool UseMmapWrites() const { return use_mmap_writes_; }
  bool UseReadaheadCompactions() const { return readahead_compactions_;}

 private:
  bool data_in_os_;
  bool fs_readahead_;
  bool readahead_compactions_;
  bool use_mmap_reads_;
  bool use_mmap_writes_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_STORAGE_OPTIONS_H_
