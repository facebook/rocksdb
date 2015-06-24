//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <thread>
#include "port/port.h"
#include "util/concurrent_arena.h"
#include "util/mutexlock.h"
  
namespace rocksdb {

	static const size_t kMaxBlockSizeOfConcurrentArena = 2 * 1024 * 1024; // 2 MB
	static const int    kMaxNumberOfConcurrentArenas   = 16;

	static size_t OptimizeBlockSizeForConcurrentArena(size_t block_size)
	{
	   if(block_size <= kMaxBlockSizeOfConcurrentArena)
		  return block_size;
	   else
		  return kMaxBlockSizeOfConcurrentArena;
	}

	static int NumberOfConcurrentArenas()
	{
		int numProcessors = port::GetNumberOfProcessors();
		
		if(numProcessors <= kMaxNumberOfConcurrentArenas)
		   return numProcessors;
		else 
		   return kMaxNumberOfConcurrentArenas;
	}

	SimpleConcurrentArena::SimpleConcurrentArena(bool disableConcurrency, size_t block_size, size_t huge_page_size) :
		disableConcurrency_(disableConcurrency),
		nConcurrentAreans_(disableConcurrency ? 1 : NumberOfConcurrentArenas()), 
		pDescriptors_(new InternalDesc[nConcurrentAreans_])
	{
	    assert(nConcurrentAreans_>=1);
		for (int i = 0; i < nConcurrentAreans_; i++)
		{
			Arena* p = new Arena(OptimizeBlockSizeForConcurrentArena(block_size),huge_page_size);
			pDescriptors_[i].pArena = p;
			pDescriptors_[i].approximateMemoryUsage = p->ApproximateMemoryUsage();;
			pDescriptors_[i].memoryAllocatedBytes = p->MemoryAllocatedBytes();
			pDescriptors_[i].allocatedAndUnused = p->AllocatedAndUnused();
			pDescriptors_[i].irregularBlockNum = p->IrregularBlockNum();

			if (disableConcurrency_)
				pDescriptors_[i].pMutex = nullptr;
			else
				pDescriptors_[i].pMutex = new port::Mutex();
		}
	}



	SimpleConcurrentArena::~SimpleConcurrentArena()
	{
		for (int i = 0; i < nConcurrentAreans_; i++)
		{
			delete pDescriptors_[i].pArena;
			if (pDescriptors_[i].pMutex != nullptr) delete pDescriptors_[i].pMutex;
		}
		
		delete pDescriptors_;

	}

	char* SimpleConcurrentArena::Allocate(size_t bytes)
	{
		int i = 0;
		if (nConcurrentAreans_ > 1)
		{
			i = port::GetCurrentProcessor() % nConcurrentAreans_; 
		}

		if (disableConcurrency_)
		{
			return pDescriptors_[i].pArena->Allocate(bytes);
		}
		else
		{
			MutexLock l(pDescriptors_[i].pMutex);
			return pDescriptors_[i].pArena->Allocate(bytes);
		}
	}

	char* SimpleConcurrentArena::AllocateAligned(size_t bytes, size_t huge_page_size, Logger* logger)
	{
		int i = 0;
		if (nConcurrentAreans_ > 1)
		{
			i = port::GetCurrentProcessor() % nConcurrentAreans_; 
		}

		if (disableConcurrency_)
		{
			return pDescriptors_[i].pArena->AllocateAligned(bytes, huge_page_size, logger);
		}
		else
		{
			MutexLock l(pDescriptors_[i].pMutex);
			return pDescriptors_[i].pArena->AllocateAligned(bytes, huge_page_size, logger);
		}

	}

	size_t SimpleConcurrentArena::ApproximateMemoryUsage() const
	{
		size_t r = 0;
		for (int i = 0; i < nConcurrentAreans_; i++)
		{
			r += pDescriptors_[i].pArena->ApproximateMemoryUsage();
		}

		return r;
	}

	size_t SimpleConcurrentArena::MemoryAllocatedBytes() const
	{
		size_t r = 0;
		for (int i = 0; i < nConcurrentAreans_; i++)
		{
			r += pDescriptors_[i].pArena->MemoryAllocatedBytes();
		}

		return r;
	}

	size_t SimpleConcurrentArena::AllocatedAndUnused() const
	{
		size_t r = 0;
		for (int i = 0; i < nConcurrentAreans_; i++)
		{
			r += pDescriptors_[i].pArena->AllocatedAndUnused();
		}

		return r;

	}

	size_t SimpleConcurrentArena::IrregularBlockNum() const
	{
		size_t r = 0;
		for (int i = 0; i < nConcurrentAreans_; i++)
		{
			r += pDescriptors_[i].pArena->IrregularBlockNum();
		}

		return r;
	}

	size_t SimpleConcurrentArena::BlockSize() const
	{
		return pDescriptors_[0].pArena->BlockSize();
	}
	
}