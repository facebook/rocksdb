//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include "util/allocator.h"
#include "util/arena.h"
 
namespace rocksdb {
 
class Logger;

class SimpleConcurrentArena : public Allocator {
 public:
  // No copying allowed
  SimpleConcurrentArena(const SimpleConcurrentArena&) = delete;
  void operator=(const SimpleConcurrentArena&) = delete;

  explicit SimpleConcurrentArena(bool disableConcurrency, size_t block_size = Arena::kMinBlockSize, size_t huge_page_size = 0);
  ~SimpleConcurrentArena();

  char* Allocate(size_t bytes) override;

  char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                        Logger* logger = nullptr) override;

  size_t ApproximateMemoryUsage() const ;  

  size_t MemoryAllocatedBytes() const ;

  size_t AllocatedAndUnused() const ;

  // If an allocation is too big, we'll allocate an irregular block with the
  // same size of that allocation.
  size_t IrregularBlockNum() const ;

  size_t BlockSize() const override ;

 private: 
 
 	struct InternalDesc
	{
		Arena* pArena;
		size_t approximateMemoryUsage;
		size_t memoryAllocatedBytes;
		size_t allocatedAndUnused;
		size_t irregularBlockNum;
		port::Mutex* pMutex;
	};

	const bool disableConcurrency_;
	const int nConcurrentAreans_;
	InternalDesc* const pDescriptors_;
 
};

}  // namespace rocksdb
