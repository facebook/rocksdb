// Copyright 2008-present Facebook. All Rights Reserved.
#ifndef STORAGE_LEVELDB_ITER_HEAP_H_
#define STORAGE_LEVELDB_ITER_HEAP_H_

#include <queue>

#include "rocksdb/comparator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

// Return the max of two keys.
class MaxIteratorComparator {
 public:
  MaxIteratorComparator(const Comparator* comparator) :
    comparator_(comparator) {}

  bool operator()(IteratorWrapper* a, IteratorWrapper* b) {
    return comparator_->Compare(a->key(), b->key()) <= 0;
  }
 private:
  const Comparator* comparator_;
};

// Return the max of two keys.
class MinIteratorComparator {
 public:
  // if maxHeap is set comparator returns the max value.
  // else returns the min Value.
  // Can use to create a minHeap or a maxHeap.
  MinIteratorComparator(const Comparator* comparator) :
    comparator_(comparator) {}

  bool operator()(IteratorWrapper* a, IteratorWrapper* b) {
    return comparator_->Compare(a->key(), b->key()) > 0;
  }
 private:
  const Comparator* comparator_;
};

typedef std::priority_queue<
          IteratorWrapper*,
          std::vector<IteratorWrapper*>,
          MaxIteratorComparator> MaxIterHeap;

typedef std::priority_queue<
          IteratorWrapper*,
          std::vector<IteratorWrapper*>,
          MinIteratorComparator> MinIterHeap;

// Return's a new MaxHeap of IteratorWrapper's using the provided Comparator.
MaxIterHeap NewMaxIterHeap(const Comparator* comparator) {
  return MaxIterHeap(MaxIteratorComparator(comparator));
}

// Return's a new MinHeap of IteratorWrapper's using the provided Comparator.
MinIterHeap NewMinIterHeap(const Comparator* comparator) {
  return MinIterHeap(MinIteratorComparator(comparator));
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_ITER_HEAP_H_
