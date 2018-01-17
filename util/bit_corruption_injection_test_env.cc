//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This custom env injects bit flips into file reads.

#include "util/bit_corruption_injection_test_env.h"
#include <functional>
#include <utility>

namespace rocksdb {

// A wrapper around SequentialFile that will inject bit errors.
class CorruptedSequentialFile : public SequentialFile {
 public:
  explicit CorruptedSequentialFile(uint64_t uber,
                            unique_ptr<SequentialFile>&& f)
    : UBER_(uber),
      target_(std::move(f)) {
        assert(target_ != nullptr);
  }
  // Here is where we will inject bit errors randomly
  Status Read(size_t n, Slice* result, char* scratch) {
    // First do the read from the underlying SequentialFile
    // We can't use result/scratch to call because they are const
    std::unique_ptr<char[]> tempScratch(new char[n]);
    rocksdb::Slice tempResult;
    Status s = target_->Read(n, &tempResult, tempScratch.get());
    if (!s.ok()) {
      return s;
    }
    // Use the uber to flip bits
    Random r((uint32_t)Env::Default()->NowMicros());
    for (size_t i = 0; i < tempResult.size(); ++i) {
      bool flipThis = r.OneIn(UBER_);
      if (flipThis) {
        // This should flip top bit
        scratch[i] = (tempScratch[i] ^ (char) 0x80);
      } else {
        scratch[i] = tempScratch[i];
      }
    }
    *result = Slice(scratch, tempResult.size());
    return s;
  }

  Status Skip(uint64_t n) {
    return target_->Skip(n);
  }

 private:
  // It doesn't really matter how many bits flip, because a ChecksumException
  // will happen even if 1 bit is flipped.
  uint64_t UBER_; // 1 in UBER_ *bytes* are corrupted.
  unique_ptr<SequentialFile> target_;
};

// A wrapper around RandomAccessFile that will inject bit errors.
class CorruptedRandomAccessFile : public RandomAccessFile {
 public:
   explicit CorruptedRandomAccessFile(uint64_t uber,
                             unique_ptr<RandomAccessFile>&& f)
       : UBER_(uber),
         target_(std::move(f)) {
           assert(target_ != nullptr);
   }

  // Here is where we will inject bit errors randomly
  Status Read(uint64_t offset, size_t n,
    Slice* result, char* scratch) const {
    // First do the read from the underlying RandomAccessFile
    // We can't use result/scratch to call because they are const
    std::unique_ptr<char[]> tempScratch(new char[n]);
    rocksdb::Slice tempResult;
    Status s = target_->Read(offset, n, &tempResult, tempScratch.get());
    if (!s.ok()) {
      return s;
    }
    // Use the uber to flip bits
    Random r((uint32_t)Env::Default()->NowMicros());
    for (size_t i = 0; i < tempResult.size(); ++i) {
      bool flipThis = r.OneIn(UBER_);
      if (flipThis) {
        // This should flip top bit
        scratch[i] = (tempScratch[i] ^ (char) 0x80);
      } else {
        scratch[i] = tempScratch[i];
      }
    }
    *result = Slice(scratch, tempResult.size());
    return s;
  }

 private:
  uint64_t UBER_;
  unique_ptr<RandomAccessFile> target_;
};

std::vector<std::string> initExcludedFiles() {
  std::vector<std::string> excludedFiles;
  excludedFiles.push_back("CURRENT");
  excludedFiles.push_back("MANIFEST");
  excludedFiles.push_back("IDENTITY");
  return excludedFiles;
}

std::vector<std::string>
  BitCorruptionInjectionTestEnv::excludedFiles_ = initExcludedFiles();

Status BitCorruptionInjectionTestEnv::NewSequentialFile(
  const std::string& f, unique_ptr<SequentialFile>* r,
  const EnvOptions& options) {
  Status s;
  s = EnvWrapper::NewSequentialFile(f, r, options);
  if (!s.ok()) {
    return s;
  }
  for (std::vector<std::string>::iterator it = excludedFiles_.begin();
    it < excludedFiles_.end(); it++) {
      if (f.compare(*it) == 0) {
        // Then it's a special metadata file and should be ignored
        return s;
      }
  }
  CorruptedSequentialFile* retMe = new CorruptedSequentialFile(UBER_,
    std::move(*r));
  r->reset(retMe);
  return s;
}

Status BitCorruptionInjectionTestEnv::NewRandomAccessFile(
  const std::string& f, unique_ptr<RandomAccessFile>* r,
  const EnvOptions& options) {
  Status s;
  s = EnvWrapper::NewRandomAccessFile(f, r, options);
  if (!s.ok()) {
    return s;
  }
  for (std::vector<std::string>::iterator it = excludedFiles_.begin();
    it < excludedFiles_.end(); it++) {
      if (f.compare(*it) == 0) {
        // Then it's a special metadata file and should be ignored
        return s;
      }
  }
  CorruptedRandomAccessFile* retMe = new CorruptedRandomAccessFile(UBER_,
    std::move(*r));
  r->reset(retMe);
  return s;
}
}  // namespace rocksdb
