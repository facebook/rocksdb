//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/hash_map.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace rocksdb {

class HashMapTest : public testing::Test {};

TEST_F(HashMapTest, InsertErase) {
  enum Action { INSERT, EMPLACE, OPERATOR };
  for (Action a : {INSERT, EMPLACE, OPERATOR}) {
    HashMapRB<std::string, std::string> map;
    ASSERT_EQ(map.size(), 0);

    // Insert phase
    for (size_t i = 0; i < 1000; ++i) {
      auto it = map.end();
      auto key = ToString(i);
      if (a == INSERT) {
        auto ret = map.insert(std::make_pair(key, "a"));

        it = ret.first;
        ASSERT_TRUE(ret.second);
      } else if (a == EMPLACE) {
        auto ret = map.emplace(key, "a");

        it = ret.first;
        ASSERT_TRUE(ret.second);
      } else if (a == OPERATOR) {
        auto& ret = map[key];
        // std::string value should be empty
        ASSERT_TRUE(ret.empty());
        ret = "a";

        it = map.find(key);
        ASSERT_NE(it, map.end());
      }

      ASSERT_EQ(it->first, key);
      ASSERT_EQ(it->second, "a");
    }

    size_t size = map.size();
    ASSERT_EQ(size, 1000);

    // Delete phase
    for (size_t i = 0; i < 1000; ++i) {
      auto it = map.end();
      auto key = ToString(i);
      if (a == INSERT) {
        auto ret = map.insert(std::make_pair(key, "a"));

        it = ret.first;
        ASSERT_FALSE(ret.second);
      } else if (a == EMPLACE) {
        auto ret = map.emplace(key, "a");

        it = ret.first;
        ASSERT_FALSE(ret.second);
      } else if (a == OPERATOR) {
        auto& ret = map[key];
        ASSERT_EQ(ret, "a");

        it = map.find(key);
        ASSERT_NE(it, map.end());
      }

      ASSERT_EQ(it->first, key);
      ASSERT_EQ(it->second, "a");

      ASSERT_EQ(it, map.find(key));
      map.erase(it);
      ASSERT_EQ(--size, map.size());

      ASSERT_EQ(map.find(key), map.end());
    }

    ASSERT_EQ(map.size(), 0);
  }
}

namespace {
void AssertEqual(const HashMapRB<std::string, std::string>& a,
                 const HashMapRB<std::string, std::string>& b) {
  ASSERT_EQ(a.size(), b.size());
  auto it1 = a.begin();
  auto it2 = b.begin();
  for (; it1 != a.end(); ++it1, ++it2) {
    ASSERT_EQ(*it1, *it2);
  }
  ASSERT_EQ(it1, a.end());
  ASSERT_EQ(it2, b.end());
}
}  // namespace

TEST_F(HashMapTest, CopyAndAssignment) {
  HashMapRB<std::string, std::string> map;
  for (size_t i = 0; i < 1000; ++i) {
    map[ToString(i)] = "a";
  }

  for (bool copy : {true, false}) {
    if (copy) {
      HashMapRB<std::string, std::string> other(map);
      AssertEqual(map, other);
    } else {
      HashMapRB<std::string, std::string> other;
      other = map;
      AssertEqual(map, other);
    }
  }
}

TEST_F(HashMapTest, Iterators) {
  HashMapRB<std::string, std::string> map;
  for (size_t i = 0; i < 1000; ++i) {
    map[ToString(i)] = "a";
  }

  ASSERT_LE(map.begin(), map.end());

  std::vector<decltype(map)::value_type> vec;
  std::copy(map.begin(), map.end(), std::back_inserter(vec));

  ASSERT_EQ(vec.size(), map.size());

  // Check increment/decrement operator.
  size_t idx = 0;
  for (auto it = map.begin(); it != map.end(); ++it) {
    ASSERT_EQ(*it, vec[idx++]);
  }

  for (auto it = map.end(); it != map.begin();) {
    ASSERT_EQ(*(--it), vec[--idx]);
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
