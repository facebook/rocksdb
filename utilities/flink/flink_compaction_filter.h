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
#define JAVA_MAX_SIZE static_cast<std::size_t>(0x7fffffff)

/**
 * Compaction filter for removing expired Flink state entries with ttl.
 *
 * Note: this compaction filter is a special implementation, designed for usage only in Apache Flink project.
 */
class FlinkCompactionFilter : public CompactionFilter {
public:
  enum StateType {
    // WARNING!!! Do not change the order of enum entries as it is important for jni translation
    Disabled,
    Value,
    List
  };

  class ListElementIter {
  public:
    virtual ~ListElementIter() = default;
    virtual void SetListBytes(const Slice& list) const = 0;
    virtual std::size_t NextOffset(std::size_t current_offset) const = 0;
  };

  class FixedListElementIter : public ListElementIter {
  public:
    explicit FixedListElementIter(std::size_t fixed_size) : fixed_size_(fixed_size) {}
    void SetListBytes(const Slice& /* list */) const override {};
    inline std::size_t NextOffset(std::size_t current_offset) const override {
        return current_offset + fixed_size_;
    };
  private:
      std::size_t fixed_size_;
  };

  struct Config {
    StateType state_type_;
    std::size_t timestamp_offset_;
    int64_t ttl_;
    bool useSystemTime_;
    ListElementIter* list_element_iter_;
  };

  explicit FlinkCompactionFilter() : logger_(nullptr) {};
  explicit FlinkCompactionFilter(std::shared_ptr<Logger> logger) : logger_(std::move(logger)) {};

  const char* Name() const override;
  Decision FilterV2(int level, const Slice& key, ValueType value_type,
                    const Slice& existing_value, std::string* new_value,
                    std::string* skip_until) const override;

  bool IgnoreSnapshots() const override { return true; }

  void Configure(Config* config) {
      Config* old_config = config_;
      config_ = config;
      delete old_config->list_element_iter_;
      delete old_config;
  }

  void SetCurrentTimestamp(int64_t current_timestamp) {
      current_timestamp_ = current_timestamp;
  }

  ~FlinkCompactionFilter() override {
      Config* config = config_;
      delete config->list_element_iter_;
      delete config;
  }

private:
  Decision ListDecide(const Slice& existing_value, const Config& config, std::string* new_value) const;

  inline std::size_t ListNextOffset(std::size_t offset, ListElementIter* list_element_iter_) const;

  inline void SetUnexpiredListValue(
          const Slice& existing_value, std::size_t offset, std::string* new_value) const;

  inline Decision Decide(const char* ts_bytes, const Config& config, std::size_t timestamp_offset) const;

  inline int64_t DeserializeTimestamp(const char *src, std::size_t offset) const;

  inline int64_t CurrentTimestamp(bool useSystemTime) const;

  std::atomic<Config*> config_ = { new Config{Disabled, 0, std::numeric_limits<int64_t>::max(), true, nullptr} };
  std::atomic<std::int64_t> current_timestamp_ = { std::numeric_limits<int64_t>::min() };
  std::shared_ptr<Logger> logger_;
};

}  // namespace flink
}  // namespace rocksdb
