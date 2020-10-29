//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/lru_cache.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class LRUHandleTableTest : public testing::Test {};

TEST(LRUHandleTableTest, HandleTableTest) {
  LRUHandleTable ht;

  for (uint32_t i = 0; i < ht.length_; ++i) {
    ASSERT_NE(ht.list_[i], nullptr);
    ASSERT_EQ(ht.list_[i]->next_hash, nullptr);
    ASSERT_EQ(ht.list_[i]->prev_hash, nullptr);
  }

  const uint32_t count = 10;
  Slice keys[count] = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
  ASSERT_NE(keys[0], keys[1]);
  LRUHandle* hs[count];
  for (uint32_t i = 0; i < count; ++i) {
    Slice* key = &keys[i];
    LRUHandle* h = reinterpret_cast<LRUHandle*>(
        malloc(sizeof(LRUHandle) - 1 + key->size()));
    h->value = nullptr;
    h->deleter = nullptr;
    h->charge = 1;
    h->key_length = key->size();
    h->hash = 1;  // make them in a same hash table linked-list
    h->refs = 0;
    h->prev = h->next = nullptr;
    h->prev_hash = h->next_hash = nullptr;
    h->SetInCache(true);
    memcpy(h->key_data, key->data(), key->size());

    LRUHandle* old = ht.Insert(h);
    ASSERT_EQ(ht.elems_, i + 1);
    ASSERT_EQ(old, nullptr);  // there is no entry with the same key and hash
    hs[i] = h;
  }
  ASSERT_EQ(ht.elems_, count);
  LRUHandle* h = ht.Lookup(Slice(std::to_string(count - 1)), 1);
  LRUHandle* head = ht.list_[1 & (ht.length_ - 1)];
  ASSERT_EQ(head, h->prev_hash);
  ASSERT_EQ(head->next_hash, h);
  uint32_t index = count - 1;
  while (h != nullptr) {
    ASSERT_EQ(hs[index], h) << index;
    h = h->next_hash;
    if (h != nullptr) {
      ASSERT_EQ(hs[index], h->prev_hash);
    }
    --index;
  }

  for (uint32_t i = 0; i < count; ++i) {
    Slice* key = &keys[i];
    LRUHandle* h1 = reinterpret_cast<LRUHandle*>(
        malloc(sizeof(LRUHandle) - 1 + key->size()));
    h1->value = nullptr;
    h1->deleter = nullptr;
    h1->charge = 1;
    h1->key_length = key->size();
    h1->hash = 1;  // make them in a same hash table linked-list
    h1->refs = 0;
    h1->prev = h1->next = nullptr;
    h1->prev_hash = h1->next_hash = nullptr;
    h1->SetInCache(true);
    memcpy(h1->key_data, key->data(), key->size());

    ASSERT_EQ(ht.Insert(h1),
              hs[i]);  // there is an entry with the same key and hash
    ASSERT_EQ(ht.elems_, count);
    free(hs[i]);
    hs[i] = h1;
  }
  ASSERT_EQ(ht.elems_, count);

  for (uint32_t i = 0; i < count; ++i) {
    ASSERT_EQ(ht.Lookup(keys[i], 1), hs[i]);
  }

  LRUHandle* old = ht.Remove(Slice("9"), 1);  // first in hash table linked-list
  ASSERT_EQ(old, hs[9]);
  ASSERT_EQ(old->prev_hash, head);
  ASSERT_EQ(old->next_hash, hs[8]);  // hs[8] is the new first node
  ASSERT_EQ(head->next_hash, hs[8]);
  ASSERT_EQ(hs[8]->prev_hash, head);

  old = ht.Remove(Slice("0"), 1);  // last in hash table linked-list
  ASSERT_EQ(old, hs[0]);
  ASSERT_EQ(old->prev_hash, hs[1]);  // hs[1] is the new last node
  ASSERT_EQ(old->prev_hash->next_hash, nullptr);

  old = ht.Remove(Slice("5"), 1);  // middle in hash table linked-list
  ASSERT_EQ(old, hs[5]);
  ASSERT_EQ(old->prev_hash, hs[6]);
  ASSERT_EQ(old->next_hash, hs[4]);
  ASSERT_EQ(hs[6]->next_hash, hs[4]);
  ASSERT_EQ(hs[4]->prev_hash, hs[6]);

  ht.Remove(hs[4]);  // middle in hash table linked-list
  ASSERT_EQ(hs[6]->next_hash, hs[3]);
  ASSERT_EQ(hs[3]->prev_hash, hs[6]);

  ASSERT_EQ(ht.elems_, count - 4);

  for (uint32_t i = 0; i < count; ++i) {
    free(hs[i]);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
