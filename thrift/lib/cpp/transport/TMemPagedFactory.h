/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef _THRIFT_TRANSPORT_TMEMPAGEDFACTORY_H_
#define _THRIFT_TRANSPORT_TMEMPAGEDFACTORY_H_ 1

#include "external/google_base/spinlock.h"

namespace apache { namespace thrift { namespace transport {

/*
 * struct keeps information about memory usage for specific page
 */
struct FixedSizeMemoryPage {
  FixedSizeMemoryPage* next_; //!< chain hook
  uint8_t buffer_[]; //!< pointer to the buffer
};

/**
 * Fixed size memory pages factory
 * Thread safe
 *
 */
class FixedSizeMemoryPageFactory {
 public:
  /**
   * Constructor
   */
  FixedSizeMemoryPageFactory(size_t pageSize, //!< page size in bytes
                             size_t maxMemoryUsage, //!< max memory usage
                             size_t cacheMemorySize); //!< max memory in cache
  /**
   * Destructor
   */
  ~FixedSizeMemoryPageFactory();

  /**
   * Releases all unused memory
   */
  void releaseMemory();

  /**
   * Requests for a page allocation
   */
  FixedSizeMemoryPage* getPage(bool throwOnError = true);

  /**
   * Returns current page size
   */
  size_t getPageSize() const;

  /**
   * Returns page back to factory
   */
  void returnPage(FixedSizeMemoryPage* page);
 private:
  const size_t pageSize_;
  const size_t maxMemoryUsage_;
  const size_t cacheMemorySize_;
  size_t numAllocatedPages_;
  size_t numCachedPages_;
  FixedSizeMemoryPage* cachedPages_;
  mutable facebook::SpinLock lock_;
};


}}} // apache::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TMEMPAGEDFACTORY_H_
