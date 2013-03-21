#include <memory>
#include "leveldb/env.h"
#include "leveldb/merge_operator.h"
#include "leveldb/slice.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"


using namespace leveldb;

namespace { // anonymous namespace

// A 'model' merge operator with uint64 addition semantics
class UInt64AddOperator : public MergeOperator {
 public:
  virtual void Merge(const Slice& key,
                     const Slice* existing_value,
                     const Slice& value,
                     std::string* new_value,
                     Logger* logger) const override {
    // assuming 0 if no existing value
    uint64_t existing = 0;
    if (existing_value) {
      if (existing_value->size() == sizeof(uint64_t)) {
        existing = DecodeFixed64(existing_value->data());
      } else {
        // if existing_value is corrupted, treat it as 0
        Log(logger, "existing value corruption, size: %zu > %zu",
            existing_value->size(), sizeof(uint64_t));
        existing = 0;
      }
    }

    uint64_t operand;
    if (value.size()  == sizeof(uint64_t)) {
      operand = DecodeFixed64(value.data());
    } else {
      // if operand is corrupted, treat it as 0
      Log(logger, "operand value corruption, size: %zu > %zu",
          value.size(), sizeof(uint64_t));
      operand = 0;
    }

    new_value->resize(sizeof(uint64_t));
    EncodeFixed64(&(*new_value)[0], existing + operand);

    return;
  }

  virtual const char* Name() const override {
    return "UInt64AddOperator";
  }
};

}

namespace leveldb {

std::shared_ptr<MergeOperator> MergeOperators::CreateUInt64AddOperator() {
  return std::make_shared<UInt64AddOperator>();
}

}
