//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This class implements a custom SecondaryCache that randomly injects an
// error status into Inserts/Lookups based on a specified probability.
//
#include "rocksdb/secondary_cache.h"
#include "util/random.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

class FaultInjectionSecondaryCacheHandle : public SecondaryCacheResultHandle {
 public:
  FaultInjectionSecondaryCacheHandle(
      std::unique_ptr<SecondaryCacheResultHandle>&& base,
      SecondaryCache* cache)
    : base_(std::move(base)), cache_(cache) {}
  ~FaultInjectionSecondaryCacheHandle() override {}

  bool IsReady() override { return base_->IsReady(); }

  void Wait() override;

  void* Value() override { return base_->Value(); }

  size_t Size() override { return base_->Size(); }

 private:
  std::unique_ptr<SecondaryCacheResultHandle> base_;
  SecondaryCache* cache_;
};

class FaultInjectionSecondaryCache : public SecondaryCache {
 public:
  explicit FaultInjectionSecondaryCache(
      const std::shared_ptr<SecondaryCache>& base, uint32_t seed,
      int prob)
    : base_(base), seed_(seed), prob_(prob),
      thread_local_error_(new ThreadLocalPtr(DeleteThreadLocalErrorContext)) {}

  virtual ~FaultInjectionSecondaryCache() {}

  const char* Name() const override { return "FaultInjectionSecondaryCache"; }

  Status Insert(const Slice& key, void* value,
                const Cache::CacheItemHelper* helper) override;

  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CreateCallback& create_cb,
      bool wait) override;

  void Erase(const Slice& /*key*/) override;

  void WaitAll(std::vector<SecondaryCacheResultHandle*> handles) override;

  std::string GetPrintableOptions() const override { return ""; }

  void EnableErrorInjection(uint64_t prob);

 private:
  class ResultHandle : public SecondaryCacheResultHandle {
   public:
    ResultHandle(FaultInjectionSecondaryCache* cache,
        std::unique_ptr<SecondaryCacheResultHandle>&& base)
      : cache_(cache), base_(std::move(base)), value_(nullptr), size_(0) {}

    ~ResultHandle() override {}

    bool IsReady() override;

    void Wait() override;

    void* Value() override;

    size_t Size() override;

    static void WaitAll(FaultInjectionSecondaryCache* cache,
        std::vector<SecondaryCacheResultHandle*> handles);

   private:
    static void UpdateHandleValue(ResultHandle* handle);

    FaultInjectionSecondaryCache* cache_;
    std::unique_ptr<SecondaryCacheResultHandle> base_;
    void* value_;
    size_t size_;
  };

  static void DeleteThreadLocalErrorContext(void *p) {
    ErrorContext* ctx = static_cast<ErrorContext*>(p);
    delete ctx;
  }

  const std::shared_ptr<SecondaryCache> base_;
  uint32_t seed_;
  int prob_;

  struct ErrorContext {
    Random rand;

    ErrorContext(uint32_t seed) : rand(seed) {}
  };
  std::unique_ptr<ThreadLocalPtr> thread_local_error_;

  ErrorContext* GetErrorContext();
};

} // ROCKSDB_NAMESPACE
