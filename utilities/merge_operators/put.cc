#include <memory>
#include "leveldb/slice.h"
#include "leveldb/merge_operator.h"
#include "utilities/merge_operators.h"

using namespace leveldb;

namespace { // anonymous namespace

// A merge operator that mimics Put semantics
class PutOperator : public MergeOperator {
 public:
  virtual void Merge(const Slice& key,
                     const Slice* existing_value,
                     const Slice& value,
                     std::string* new_value,
                     Logger* logger) const override {
    // put basically only looks at the current value
    new_value->assign(value.data(), value.size());
  }

  virtual const char* Name() const override {
    return "PutOperator";
  }
};

} // end of anonymous namespace

namespace leveldb {

std::shared_ptr<MergeOperator> MergeOperators::CreatePutOperator() {
  return std::make_shared<PutOperator>();
}

}
