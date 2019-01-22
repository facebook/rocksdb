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

  // Factory is needed to create one iterator per filter/thread
  // and avoid concurrent access to list state bytes between methods SetListBytes and NextOffset
  class ListElementIterFactory {
  public:
    virtual ~ListElementIterFactory() = default;
    virtual ListElementIter* CreateListElementIter() const = 0;
  };

  class FixedListElementIterFactory : public ListElementIterFactory {
  public:
    explicit FixedListElementIterFactory(std::size_t fixed_size) : fixed_size_(fixed_size) {}
    FixedListElementIter* CreateListElementIter() const override {
        return new FixedListElementIter(fixed_size_);
    };
  private:
    std::size_t fixed_size_;
  };

  struct Config {
    StateType state_type_;
    std::size_t timestamp_offset_;
    int64_t ttl_;
    bool useSystemTime_;
    ListElementIterFactory* list_element_iter_factory_;
  };

  // Allows to configure at once all FlinkCompactionFilters created by the factory
  // which holds the shared ConfigHolder.
  class ConfigHolder {
  public:
    ~ConfigHolder() {
      Config* config = config_.load();
      if (config != &DISABLED_CONFIG) {
        delete config->list_element_iter_factory_;
        delete config;
      }
    }

    // at the moment Flink configures filters (can be already created) only once when user creates state
    // otherwise it can lead to ListElementIter leak in Config
    // or race between its delete in Configure() and usage in FilterV2()
    void Configure(Config* config) {
      assert(GetConfig() == &DISABLED_CONFIG);
      config_ = config;
    }

    void SetCurrentTimestamp(int64_t current_timestamp) {
      current_timestamp_ = current_timestamp;
    }

    Config* GetConfig() {
        return config_.load();
    }

    std::int64_t GetCurrentTimestamp() {
      return current_timestamp_.load();
    }

  private:
    Config DISABLED_CONFIG = Config{Disabled, 0, std::numeric_limits<int64_t>::max(), true, nullptr};
    std::atomic<Config*> config_ = { &DISABLED_CONFIG };
    std::atomic<std::int64_t> current_timestamp_ = { std::numeric_limits<int64_t>::min() };
  };

  explicit FlinkCompactionFilter(std::shared_ptr<ConfigHolder> config_holder) :
          config_holder_(std::move(config_holder)), logger_(nullptr) {};
  explicit FlinkCompactionFilter(std::shared_ptr<ConfigHolder> config_holder, std::shared_ptr<Logger> logger) :
          config_holder_(std::move(config_holder)), logger_(std::move(logger)) {};

  ~FlinkCompactionFilter() override {
    delete list_element_iter_;
  }

  const char* Name() const override;
  Decision FilterV2(int level, const Slice& key, ValueType value_type,
                    const Slice& existing_value, std::string* new_value,
                    std::string* skip_until) const override;

  bool IgnoreSnapshots() const override { return true; }

private:
  Decision ListDecide(const Slice& existing_value, const Config* config, std::string* new_value) const;

  inline std::size_t ListNextOffset(std::size_t offset) const;

  inline void SetUnexpiredListValue(
          const Slice& existing_value, std::size_t offset, std::string* new_value) const;

  inline Decision Decide(const char* ts_bytes, const Config* config, std::size_t timestamp_offset) const;

  inline int64_t DeserializeTimestamp(const char *src, std::size_t offset) const;

  inline int64_t CurrentTimestamp(bool useSystemTime) const;

  inline void CreateListElementIterIfNull(ListElementIterFactory* list_element_iter_factory) const {
    if (!list_element_iter_ && list_element_iter_factory) {
      const_cast<FlinkCompactionFilter*>(this)->list_element_iter_ = list_element_iter_factory->CreateListElementIter();
    }
  }

  std::shared_ptr<ConfigHolder> config_holder_;
  std::shared_ptr<Logger> logger_;
  ListElementIter* list_element_iter_ = nullptr;
};

}  // namespace flink
}  // namespace rocksdb
