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

static const std::size_t BITS_PER_BYTE = static_cast<std::size_t>(8);
static const std::size_t TIMESTAMP_BYTE_SIZE = static_cast<std::size_t>(8);
static const int64_t JAVA_MIN_LONG = static_cast<int64_t>(0x8000000000000000);
static const int64_t JAVA_MAX_LONG = static_cast<int64_t>(0x7fffffffffffffff);
static const std::size_t JAVA_MAX_SIZE = static_cast<std::size_t>(0x7fffffff);

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

  // Provides current timestamp to check expiration, it must thread safe.
  class TimeProvider {
  public:
    virtual ~TimeProvider() = default;
    virtual int64_t CurrentTimestamp() const = 0;
  };

  // accepts serialized list state and checks elements for expiration starting from the head
  // stops upon discovery of unexpired element and returns its offset
  // or returns offset greater or equal to list byte length.
  class ListElementFilter {
  public:
    virtual ~ListElementFilter() = default;
    virtual std::size_t NextUnexpiredOffset(const Slice& list, int64_t ttl, int64_t current_timestamp) const = 0;
  };

  // this filter can operate directly on list state bytes
  // because the byte length of list element and last acess timestamp position are known.
  class FixedListElementFilter : public ListElementFilter {
  public:
    explicit FixedListElementFilter(std::size_t fixed_size, std::size_t timestamp_offset, std::shared_ptr<Logger> logger) :
            fixed_size_(fixed_size), timestamp_offset_(timestamp_offset), logger_(std::move(logger)) {}
    std::size_t NextUnexpiredOffset(const Slice& list, int64_t ttl, int64_t current_timestamp) const override;
  private:
      std::size_t fixed_size_;
      std::size_t timestamp_offset_;
      std::shared_ptr<Logger> logger_;
  };

  // Factory is needed to create one filter per filter/thread
  // and avoid concurrent access to the filter state
  class ListElementFilterFactory {
  public:
    virtual ~ListElementFilterFactory() = default;
    virtual ListElementFilter* CreateListElementFilter(std::shared_ptr<Logger> logger) const = 0;
  };

  class FixedListElementFilterFactory : public ListElementFilterFactory {
  public:
    explicit FixedListElementFilterFactory(std::size_t fixed_size, std::size_t timestamp_offset) :
            fixed_size_(fixed_size), timestamp_offset_(timestamp_offset) {}
    FixedListElementFilter* CreateListElementFilter(std::shared_ptr<Logger> logger) const override {
        return new FixedListElementFilter(fixed_size_, timestamp_offset_, logger);
    };
  private:
    std::size_t fixed_size_;
    std::size_t timestamp_offset_;
  };

  struct Config {
    StateType state_type_;
    std::size_t timestamp_offset_;
    int64_t ttl_;
    // Number of state entries to process by compaction filter before updating current timestamp.
    int64_t query_time_after_num_entries_;
    std::unique_ptr<ListElementFilterFactory> list_element_filter_factory_;
  };

  // Allows to configure at once all FlinkCompactionFilters created by the factory.
  // The ConfigHolder holds the shared Config.
  class ConfigHolder {
  public:
    explicit ConfigHolder();
    ~ConfigHolder();
    bool Configure(Config* config);
    Config* GetConfig();
  private:
    std::atomic<Config*> config_;
  };

  explicit FlinkCompactionFilter(std::shared_ptr<ConfigHolder> config_holder,
                                 std::unique_ptr<TimeProvider> time_provider);

  explicit FlinkCompactionFilter(std::shared_ptr<ConfigHolder> config_holder,
                                 std::unique_ptr<TimeProvider> time_provider,
                                 std::shared_ptr<Logger> logger);

  const char* Name() const override;
  Decision FilterV2(int level, const Slice& key, ValueType value_type,
                    const Slice& existing_value, std::string* new_value,
                    std::string* skip_until) const override;

  bool IgnoreSnapshots() const override { return true; }

private:
  inline void InitConfigIfNotYet() const;

  Decision ListDecide(const Slice& existing_value, std::string* new_value) const;

  inline std::size_t ListNextUnexpiredOffset(const Slice &existing_value, std::size_t offset, int64_t ttl) const;

  inline void SetUnexpiredListValue(
          const Slice& existing_value, std::size_t offset, std::string* new_value) const;

  inline void CreateListElementFilterIfNull() const {
    if (!list_element_filter_ && config_cached_->list_element_filter_factory_) {
      const_cast<FlinkCompactionFilter*>(this)->list_element_filter_ =
              std::unique_ptr<ListElementFilter>(config_cached_->list_element_filter_factory_->CreateListElementFilter(logger_));
    }
  }

  inline void UpdateCurrentTimestampIfStale() const {
    bool is_stale = record_counter_ >= config_cached_->query_time_after_num_entries_;
    if (is_stale) {
      const_cast<FlinkCompactionFilter*>(this)->record_counter_ = 0;
      const_cast<FlinkCompactionFilter*>(this)->current_timestamp_ = time_provider_->CurrentTimestamp();
    }
    const_cast<FlinkCompactionFilter*>(this)->record_counter_ = record_counter_ + 1;
  }

  std::shared_ptr<ConfigHolder> config_holder_;
  std::unique_ptr<TimeProvider> time_provider_;
  std::shared_ptr<Logger> logger_;
  Config* config_cached_;
  std::unique_ptr<ListElementFilter> list_element_filter_;
  int64_t current_timestamp_ = std::numeric_limits<int64_t>::max();
  int64_t record_counter_ = std::numeric_limits<int64_t>::max();
};

static const FlinkCompactionFilter::Config DISABLED_CONFIG = FlinkCompactionFilter::Config{
    FlinkCompactionFilter::StateType::Disabled, 0, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max(), nullptr};

}  // namespace flink
}  // namespace rocksdb
