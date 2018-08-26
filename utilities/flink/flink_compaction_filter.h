// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>
#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"
#include <chrono>
#include <functional>
#include <utility>

namespace rocksdb {
namespace flink {

#define BITS_PER_BYTE static_cast<size_t>(8)
#define TIMESTAMP_BYTE_SIZE static_cast<size_t>(8)
#define FLINK_MAP_STATE_NULL_BYTE_OFFSET static_cast<int>(1)
#define JAVA_MAX_LONG static_cast<int64_t>(0x7fffffffffffffff)

class TimeProvider {
public:
  virtual ~TimeProvider() = default;

  virtual int64_t CurrentTimestamp() const {
    using namespace std::chrono;
    return duration_cast< milliseconds >(system_clock::now().time_since_epoch()).count();
  }
};

/**
 * Compaction filter for removing expired Flink state entries with ttl.
 */
class FlinkCompactionFilter : public CompactionFilter {
public:
 enum StateType {
     // WARNING!!! Do not change the order of enum entries as it is important for jni translation
     Value,
     List,
     Map
 };
 explicit FlinkCompactionFilter(StateType state_type, int64_t ttl) :
   state_type_(state_type), ttl_(ttl), time_provider_(new TimeProvider()) {}
 explicit FlinkCompactionFilter(StateType state_type, int64_t ttl, TimeProvider* time_provider) :
   state_type_(state_type), ttl_(ttl), time_provider_(time_provider) {}

 const char* Name() const override;
 Decision FilterV2(int level, const Slice& key, ValueType value_type,
                   const Slice& existing_value, std::string* new_value,
                   std::string* skip_until) const override;

  ~FlinkCompactionFilter() override { delete time_provider_; }

private:
  inline Decision Decide(const char* ts_bytes, std::size_t offset) const;

  inline int64_t DeserializeTimestamp(const char *src, std::size_t offset) const {
    uint64_t result = 0;
    for (unsigned long i = 0; i < sizeof(uint64_t); i++) {
      result |= static_cast<uint64_t>(static_cast<unsigned char>(src[offset + i]))
          << ((sizeof(int64_t) - 1 - i) * BITS_PER_BYTE);
    }
    return static_cast<int64_t>(result);
  }

  StateType state_type_;
  int64_t ttl_;
  TimeProvider* time_provider_;
};

}  // namespace flink
}  // namespace rocksdb
