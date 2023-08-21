//
// Created by mymai on 2023/6/9.
//

#include "lfu_cache.h"
#include "db/db_test_util.h"

namespace ROCKSDB_NAMESPACE {

namespace lfu_cache {

class LFUCacheTest : public testing::Test {
 public:
  LFUCacheTest() {

  }

  ~LFUCacheTest() {
    delete cache_;
  }

  void NewCache(size_t capcacity) {
    cache_ = new LFUCacheShard(capcacity);
  }

  void Insert(const Slice& key, Cache::ObjectPtr value) {
    EXPECT_OK(cache_->Insert(key, 0, value, nullptr, 1, nullptr,
                             Cache::Priority::BOTTOM));
  }

  bool Lookup(const Slice& key) {

    auto handle = cache_->Lookup(key, 0, nullptr, nullptr, Cache::Priority::BOTTOM, nullptr);

    if (handle) {
      cache_->Release(handle, false, true);
      return true;
    }

    return false;

  }

  bool LookupWithoutRelease(const Slice& key, LFUHandle*& lfuHandle) {
    auto handle = cache_->Lookup(key, 0, nullptr, nullptr, Cache::Priority::BOTTOM, nullptr);
    lfuHandle = handle;
    return handle != nullptr ? true : false;
  }

  bool Release(LFUHandle* handle, bool erase_if_last_ref = false) {
    return cache_->Release(handle, false, erase_if_last_ref);
  }

  void SetStrictCapacityLimit(bool strict_capacity_limit) {
    cache_->SetStrictCapacityLimit(strict_capacity_limit);
  }

 private:
  LFUCacheShard* cache_ = nullptr;

};

// case1: basic insert and lookup test
TEST_F(LFUCacheTest, BasicTest) {
  NewCache(5);

  Slice key("abc");
  Slice* value = new Slice("value1");
  Insert(key, value);
  assert(Lookup(key));

}

// case2: the size of input is more than cache capacity
TEST_F(LFUCacheTest, SizeMoreThanCapacityEvictTest) {

  NewCache(5);
  SetStrictCapacityLimit(true);

  Slice keya("keya");
  Slice* valuea = new Slice("valuea");
  Insert(keya, valuea);

  Slice keyb("keyb");
  Slice* valueb = new Slice("valueb");
  Insert(keyb, valueb);

  Slice keyc("keyc");
  Slice* valuec = new Slice("valuec");
  Insert(keyc,valuec);

  Slice keyd("keyd");
  Slice* valued = new Slice("valued");
  Insert(keyd, valued);

  Slice keye("keye");
  Slice* valuee = new Slice("valuee");
  Insert(keye, valuee);

  Slice keyf("keyf");
  Slice* valuef = new Slice("valuef");
  Insert(keyf, valuef);

  Slice keyg("keyg");
  Slice* valueg = new Slice("valueg");
  Insert(keyg, valueg);

  assert(!Lookup(keya));
  assert(!Lookup(keyb));
  assert(Lookup(keyc));
  assert(Lookup(keyd));
  assert(Lookup(keye));
  assert(Lookup(keyf));
  assert(Lookup(keyg));

  delete valuea;
  delete valueb;
  delete valuec;
  delete valued;
  delete valuee;
  delete valuef;
  delete valueg;

}

// case3: insert and lookup many times and element was evicted because of rules
TEST_F(LFUCacheTest, EvictPromotionTest) {
  NewCache(5);
  SetStrictCapacityLimit(true);

  Slice keya("keya");

  std::string valuea("valuea");
  Insert(keya, &valuea);

  Slice keyb("keyb");
  std::string valueb("valueb");
  Insert(keyb, &valueb);

  Slice keyc("keyc");
  std::string valuec("valuec");
  Insert(keyc, &valuec);

  Slice keyd("keyd");
  std::string valued("valued");
  Insert(keyd, &valued);

  Slice keye("keye");
  std::string valuee("valuee");
  Insert(keye, &valuee);

  LFUHandle* handlea = nullptr;
  assert(LookupWithoutRelease(keya, handlea));

  LFUHandle* handleb = nullptr;
  assert(LookupWithoutRelease(keyb, handleb));

  LFUHandle* handlec = nullptr;
  assert(LookupWithoutRelease(keyc, handlec));

  Slice keyf("keyf");
  std::string valuef("valuef");
  Insert(keyf, &valuef);

  Slice keyg("keyg");
  std::string valueg("valueg");
  Insert(keyg, &valueg);

  assert(Lookup(keya));
  assert(Lookup(keyb));
  assert(Lookup(keyc));
  assert(Lookup(keyf));
  assert(Lookup(keyg));
  assert(!Lookup(keyd));
  assert(!Lookup(keye));
  Release(handlea, true);
  Release(handleb, true);
  Release(handlec, true);


}

}

}

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}