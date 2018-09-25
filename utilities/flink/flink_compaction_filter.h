// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <atomic>
#include <string>
#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"
#include <chrono>
#include <functional>
#include <utility>
#include <include/rocksdb/env.h>

namespace rocksdb {
namespace flink {

#define BITS_PER_BYTE static_cast<size_t>(8)
#define TIMESTAMP_BYTE_SIZE static_cast<size_t>(8)
#define JAVA_MAX_LONG static_cast<int64_t>(0x7fffffffffffffff)

/**
 * Compaction filter for removing expired Flink state entries with ttl.
 */
class FlinkCompactionFilter : public CompactionFilter {
public:
  enum StateType {
    // WARNING!!! Do not change the order of enum entries as it is important for jni translation
    Value,
    List,
    Map,
    Disabled
  };

  struct Config {
    StateType state_type_;
    std::size_t timestamp_offset_;
    int64_t ttl_;
    bool useSystemTime_;
  };

  explicit FlinkCompactionFilter() : logger_(nullptr) {};
  explicit FlinkCompactionFilter(Logger* logger) : logger_(logger) {};

  const char* Name() const override;
  Decision FilterV2(int level, const Slice& key, ValueType value_type,
                    const Slice& existing_value, std::string* new_value,
                    std::string* skip_until) const override;

  void Configure(Config* config) {
      Config* old_config = config_;
      config_ = config;
      delete old_config;
  }

  void SetCurrentTimestamp(int64_t current_timestamp) {
      current_timestamp_ = current_timestamp;
  }

  ~FlinkCompactionFilter() override {
      Config* config = config_;
      delete config;
      delete logger_;
  }

private:
  inline Decision Decide(const char* ts_bytes, std::size_t timestamp_offset, int64_t ttl, bool useSystemTime) const;

  inline int64_t DeserializeTimestamp(const char *src, std::size_t offset) const {
    uint64_t result = 0;
    for (unsigned long i = 0; i < sizeof(uint64_t); i++) {
      result |= static_cast<uint64_t>(static_cast<unsigned char>(src[offset + i]))
          << ((sizeof(int64_t) - 1 - i) * BITS_PER_BYTE);
    }
    return static_cast<int64_t>(result);
  }

  inline int64_t CurrentTimestamp(bool useSystemTime) const {
    using namespace std::chrono;
    int64_t current_timestamp;
    if (useSystemTime) {
        current_timestamp = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    } else {
        current_timestamp = current_timestamp_;
    }
    return current_timestamp;
  }

  std::atomic<Config*> config_ = { new Config{Disabled, 0, std::numeric_limits<int64_t>::max(), true} };
  std::atomic<std::int64_t> current_timestamp_ = { std::numeric_limits<int64_t>::min() };
  Logger* logger_;
};

}  // namespace flink
}  // namespace rocksdb
