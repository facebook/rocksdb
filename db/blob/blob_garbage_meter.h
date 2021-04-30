//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileGarbage;

class BlobGarbageMeter {
  class BlobStats {
   public:
    void Add(uint64_t bytes) {
      ++count_;
      bytes_ += bytes;
    }

    uint64_t GetCount() const { return count_; }
    uint64_t GetBytes() const { return bytes_; }

   private:
    uint64_t count_ = 0;
    uint64_t bytes_ = 0;
  };

  class BlobInOutFlow {
   public:
    void AddInFlow(uint64_t bytes) { in_flow_.Add(bytes); }
    void AddOutFlow(uint64_t bytes) { out_flow_.Add(bytes); }

    bool IsValid() const {
      return in_flow_.GetCount() >= out_flow_.GetCount() &&
             in_flow_.GetBytes() >= out_flow_.GetBytes();
    }
    bool HasGarbage() const {
      assert(IsValid());
      return in_flow_.GetCount() > out_flow_.GetCount();
    }
    uint64_t GarbageCount() const {
      assert(IsValid());
      assert(HasGarbage());
      return in_flow_.GetCount() - out_flow_.GetCount();
    }
    uint64_t GarbageBytes() const {
      assert(IsValid());
      assert(HasGarbage());
      return in_flow_.GetBytes() - out_flow_.GetBytes();
    }

   private:
    BlobStats in_flow_;
    BlobStats out_flow_;
  };

 public:
  void AddInFlow(uint64_t blob_file_number, uint64_t bytes) {
    flows_[blob_file_number].AddInFlow(bytes);
  }

  void AddOutFlow(uint64_t blob_file_number, uint64_t bytes) {
    flows_[blob_file_number].AddOutFlow(bytes);
  }

 private:
  std::unordered_map<uint64_t, BlobInOutFlow> flows_;
};

}  // namespace ROCKSDB_NAMESPACE
