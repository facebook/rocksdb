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
