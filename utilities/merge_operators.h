#ifndef MERGE_OPERATORS_H
#define MERGE_OPERATORS_H

#include <memory>

#include "leveldb/merge_operator.h"

namespace leveldb {

class MergeOperators {
 public:
  static std::shared_ptr<leveldb::MergeOperator> CreatePutOperator();
  static std::shared_ptr<leveldb::MergeOperator> CreateUInt64AddOperator();
};

}

#endif
