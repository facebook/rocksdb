//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "memtable/skiplist.h"

#include <cstdlib>
#include <set>
#include <unordered_set>

#include "memory/arena.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "util/hash.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

using Key = uint64_t;

struct TestComparator {
  int operator()(const Key& a, const Key& b) const {
    if (a < b) {
      return -1;
    } else if (a > b) {
      return +1;
    } else {
      return 0;
    }
  }
};

class SkipTest : public testing::Test {};

TEST_F(SkipTest, Empty) {
  Arena arena;
  TestComparator cmp;
  SkipList<Key, TestComparator> list(cmp, &arena);
  ASSERT_TRUE(!list.Contains(10));

  SkipList<Key, TestComparator>::Iterator iter(&list);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToFirst();
  ASSERT_TRUE(!iter.Valid());
  iter.Seek(100);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekForPrev(100);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToLast();
  ASSERT_TRUE(!iter.Valid());
}

TEST_F(SkipTest, InsertAndLookup) {
  const int N = 2000;
  const int R = 5000;
  Random rnd(1000);
  std::set<Key> keys;
  Arena arena;
  TestComparator cmp;
  SkipList<Key, TestComparator> list(cmp, &arena);
  for (int i = 0; i < N; i++) {
    Key key = rnd.Next() % R;
    if (keys.insert(key).second) {
      list.Insert(key);
    }
  }

  for (int i = 0; i < R; i++) {
    if (list.Contains(i)) {
      ASSERT_EQ(keys.count(i), 1U);
    } else {
      ASSERT_EQ(keys.count(i), 0U);
    }
  }

  // Simple iterator tests
  {
    SkipList<Key, TestComparator>::Iterator iter(&list);
    ASSERT_TRUE(!iter.Valid());

    iter.Seek(0);
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), iter.key());

    iter.SeekForPrev(R - 1);
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), iter.key());

    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), iter.key());

    iter.SeekToLast();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), iter.key());
  }

  // Forward iteration test
  for (int i = 0; i < R; i++) {
    SkipList<Key, TestComparator>::Iterator iter(&list);
    iter.Seek(i);

    // Compare against model iterator
    std::set<Key>::iterator model_iter = keys.lower_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.end()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      } else {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, iter.key());
        ++model_iter;
        iter.Next();
      }
    }
  }

  // Backward iteration test
  for (int i = 0; i < R; i++) {
    SkipList<Key, TestComparator>::Iterator iter(&list);
    iter.SeekForPrev(i);

    // Compare against model iterator
    std::set<Key>::iterator model_iter = keys.upper_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.begin()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      } else {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*--model_iter, iter.key());
        iter.Prev();
      }
    }
  }
}

TEST_F(SkipTest, RemoveTest) {
  Arena arena;
  TestComparator cmp;
  SkipList<Key, TestComparator> list(cmp, &arena);
  list.Insert(42);
  list.Insert(43);
  list.Insert(44);
  list.Insert(45);
  list.Insert(46);
  list.Insert(47);
  list.Insert(51);
  list.Insert(52);

  ASSERT_TRUE(list.Contains(44));
  ASSERT_FALSE(list.Contains(48));

  SkipList<Key, TestComparator>::Iterator iter(&list);
  iter.Seek(48);
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(51, iter.key());
  iter.Remove();
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(52, iter.key());
  iter.Remove();
  ASSERT_FALSE(iter.Valid());
  iter.Seek(48);
  ASSERT_FALSE(iter.Valid());

  ASSERT_TRUE(list.Remove(46));
  ASSERT_TRUE(list.Remove(44));
  ASSERT_FALSE(list.Remove(41));

  iter.SeekToFirst();
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(42, iter.key());
  iter.Next();
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(43, iter.key());
  iter.Next();
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(45, iter.key());
  iter.Next();
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(47, iter.key());
  iter.Next();
  ASSERT_FALSE(iter.Valid());
}

TEST_F(SkipTest, InsertOptimizationBreaksRemove) {
  Arena arena;
  TestComparator cmp;
  SkipList<Key, TestComparator> list(cmp, &arena);

  list.Insert(101);
  {
    SkipList<Key, TestComparator>::Iterator iter(&list);
    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    iter.Remove();
  }
  list.Insert(201);
  {
    SkipList<Key, TestComparator>::Iterator iter(&list);
    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(201, iter.key());
  }

  auto counts = list.GetInsertCounts();
  ASSERT_EQ(1, counts.fast);
  ASSERT_EQ(1, counts.slow);
}

TEST_F(SkipTest, InsertOptimizationAndRemove) {
  Arena arena;
  TestComparator cmp;
  SkipList<Key, TestComparator> list(cmp, &arena);

  for (int i = 0; i < 100000; i += 10) {
    list.Insert(i);
  }
  auto counts = list.GetInsertCounts();
  ASSERT_EQ(10000, counts.fast);
  ASSERT_EQ(0, counts.slow);

  SkipList<Key, TestComparator>::Iterator iter(&list);
  iter.SeekToFirst();
  for (int i = 0; i < 5000; i++) {
    ASSERT_TRUE(iter.Valid());
    iter.Remove();
  }

  for (int i = 0; i < 50000; i += 10) {
    list.Insert(i);
  }
  counts = list.GetInsertCounts();
  ASSERT_EQ(14999, counts.fast);
  ASSERT_EQ(1, counts.slow);

  for (int i = 50000; i < 100000; i += 10) {
    for (int j = 1; j < 10; j++) {
      list.Insert(i + j);
    }
  }
  counts = list.GetInsertCounts();
  ASSERT_EQ(54999, counts.fast);
  ASSERT_EQ(5001, counts.slow);
}

TEST_F(SkipTest, BulkRemoveTest) {
  const int32_t LIMIT = 1000000;

  const int iterations[] = {100, 200, 300, 400, 500, 600, 700, 800, 900};
  for (int iteration : iterations) {
    Arena arena;
    TestComparator cmp;
    SkipList<Key, TestComparator> list(cmp, &arena);

    std::unordered_set<Key> known;
    std::unordered_set<Key> removed;

    srand(iteration);

    for (int i = 0; i < LIMIT; i++) {
      int k = rand() % LIMIT;
      bool exists = (known.find(k) != known.end());
      if (!exists) {
        list.Insert(k);
        known.insert(k);
      }
    }

    for (int i = 0; i < LIMIT / 2; i++) {
      int k = rand() % LIMIT;
      bool exists = (known.find(k) != known.end());
      if (exists) {
        list.Remove(k);
        known.erase(k);
        removed.insert(k);
      }
    }

    for (auto it = known.cbegin(); it != known.cend(); ++it) {
      ASSERT_TRUE(list.Contains(*it));
    }
    for (auto it = removed.cbegin(); it != removed.cend(); ++it) {
      ASSERT_FALSE(list.Contains(*it));
    }

    std::unique_ptr<SkipList<Key, TestComparator>::Iterator> iter(nullptr);
    iter.reset(new SkipList<Key, TestComparator>::Iterator(&list));

    size_t expected = known.size();
    iter->SeekToFirst();
    for (size_t i = 0; i < expected; i++) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(known.find(iter->key()) != known.end());
      ASSERT_TRUE(removed.find(iter->key()) == removed.end());
      known.erase(iter->key());
      iter->Remove();
    }
    // Check we have reached the end
    ASSERT_FALSE(iter->Valid());
    ASSERT_EQ(0, known.size());

    // Check there's nothing when we try again
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());

    // Probabilistic based on the random number generator,
    // but highly unlikely to be outside range
    auto counts = list.GetInsertCounts();
    ASSERT_TRUE(counts.fast > 5 && counts.fast < 50);
    ASSERT_TRUE(counts.slow > 500000 && counts.slow < 1000000);
  }
}

// We want to make sure that with a single writer and multiple
// concurrent readers (with no synchronization other than when a
// reader's iterator is created), the reader always observes all the
// data that was present in the skip list when the iterator was
// constructor.  Because insertions are happening concurrently, we may
// also observe new values that were inserted since the iterator was
// constructed, but we should never miss any values that were present
// at iterator construction time.
//
// We generate multi-part keys:
//     <key,gen,hash>
// where:
//     key is in range [0..K-1]
//     gen is a generation number for key
//     hash is hash(key,gen)
//
// The insertion code picks a random key, sets gen to be 1 + the last
// generation number inserted for that key, and sets hash to Hash(key,gen).
//
// At the beginning of a read, we snapshot the last inserted
// generation number for each key.  We then iterate, including random
// calls to Next() and Seek().  For every key we encounter, we
// check that it is either expected given the initial snapshot or has
// been concurrently added since the iterator started.
class ConcurrentTest {
 private:
  static const uint32_t K = 4;

  static uint64_t key(Key key) { return (key >> 40); }
  static uint64_t gen(Key key) { return (key >> 8) & 0xffffffffu; }
  static uint64_t hash(Key key) { return key & 0xff; }

  static uint64_t HashNumbers(uint64_t k, uint64_t g) {
    uint64_t data[2] = {k, g};
    return Hash(reinterpret_cast<char*>(data), sizeof(data), 0);
  }

  static Key MakeKey(uint64_t k, uint64_t g) {
    assert(sizeof(Key) == sizeof(uint64_t));
    assert(k <= K);  // We sometimes pass K to seek to the end of the skiplist
    assert(g <= 0xffffffffu);
    return ((k << 40) | (g << 8) | (HashNumbers(k, g) & 0xff));
  }

  static bool IsValidKey(Key k) {
    return hash(k) == (HashNumbers(key(k), gen(k)) & 0xff);
  }

  static Key RandomTarget(Random* rnd) {
    switch (rnd->Next() % 10) {
      case 0:
        // Seek to beginning
        return MakeKey(0, 0);
      case 1:
        // Seek to end
        return MakeKey(K, 0);
      default:
        // Seek to middle
        return MakeKey(rnd->Next() % K, 0);
    }
  }

  // Per-key generation
  struct State {
    std::atomic<int> generation[K];
    void Set(int k, int v) {
      generation[k].store(v, std::memory_order_release);
    }
    int Get(int k) { return generation[k].load(std::memory_order_acquire); }

    State() {
      for (unsigned int k = 0; k < K; k++) {
        Set(k, 0);
      }
    }
  };

  // Current state of the test
  State current_;

  Arena arena_;

  // SkipList is not protected by mu_.  We just use a single writer
  // thread to modify it.
  SkipList<Key, TestComparator> list_;

 public:
  ConcurrentTest() : list_(TestComparator(), &arena_) {}

  // REQUIRES: External synchronization
  void WriteStep(Random* rnd) {
    const uint32_t k = rnd->Next() % K;
    const int g = current_.Get(k) + 1;
    const Key new_key = MakeKey(k, g);
    list_.Insert(new_key);
    current_.Set(k, g);
  }

  void ReadStep(Random* rnd) {
    // Remember the initial committed state of the skiplist.
    State initial_state;
    for (unsigned int k = 0; k < K; k++) {
      initial_state.Set(k, current_.Get(k));
    }

    Key pos = RandomTarget(rnd);
    SkipList<Key, TestComparator>::Iterator iter(&list_);
    iter.Seek(pos);
    while (true) {
      Key current;
      if (!iter.Valid()) {
        current = MakeKey(K, 0);
      } else {
        current = iter.key();
        ASSERT_TRUE(IsValidKey(current)) << current;
      }
      ASSERT_LE(pos, current) << "should not go backwards";

      // Verify that everything in [pos,current) was not present in
      // initial_state.
      while (pos < current) {
        ASSERT_LT(key(pos), K) << pos;

        // Note that generation 0 is never inserted, so it is ok if
        // <*,0,*> is missing.
        ASSERT_TRUE((gen(pos) == 0U) ||
                    (gen(pos) > static_cast<uint64_t>(initial_state.Get(
                                    static_cast<int>(key(pos))))))
            << "key: " << key(pos) << "; gen: " << gen(pos)
            << "; initgen: " << initial_state.Get(static_cast<int>(key(pos)));

        // Advance to next key in the valid key space
        if (key(pos) < key(current)) {
          pos = MakeKey(key(pos) + 1, 0);
        } else {
          pos = MakeKey(key(pos), gen(pos) + 1);
        }
      }

      if (!iter.Valid()) {
        break;
      }

      if (rnd->Next() % 2) {
        iter.Next();
        pos = MakeKey(key(pos), gen(pos) + 1);
      } else {
        Key new_target = RandomTarget(rnd);
        if (new_target > pos) {
          pos = new_target;
          iter.Seek(new_target);
        }
      }
    }
  }
};
const uint32_t ConcurrentTest::K;

// Simple test that does single-threaded testing of the ConcurrentTest
// scaffolding.
TEST_F(SkipTest, ConcurrentWithoutThreads) {
  ConcurrentTest test;
  Random rnd(test::RandomSeed());
  for (int i = 0; i < 10000; i++) {
    test.ReadStep(&rnd);
    test.WriteStep(&rnd);
  }
}

class TestState {
 public:
  ConcurrentTest t_;
  int seed_;
  std::atomic<bool> quit_flag_;

  enum ReaderState { STARTING, RUNNING, DONE };

  explicit TestState(int s)
      : seed_(s), quit_flag_(false), state_(STARTING), state_cv_(&mu_) {}

  void Wait(ReaderState s) {
    mu_.Lock();
    while (state_ != s) {
      state_cv_.Wait();
    }
    mu_.Unlock();
  }

  void Change(ReaderState s) {
    mu_.Lock();
    state_ = s;
    state_cv_.Signal();
    mu_.Unlock();
  }

 private:
  port::Mutex mu_;
  ReaderState state_;
  port::CondVar state_cv_;
};

static void ConcurrentReader(void* arg) {
  TestState* state = reinterpret_cast<TestState*>(arg);
  Random rnd(state->seed_);
  int64_t reads = 0;
  state->Change(TestState::RUNNING);
  while (!state->quit_flag_.load(std::memory_order_acquire)) {
    state->t_.ReadStep(&rnd);
    ++reads;
  }
  state->Change(TestState::DONE);
}

static void RunConcurrent(int run) {
  const int seed = test::RandomSeed() + (run * 100);
  Random rnd(seed);
  const int N = 1000;
  const int kSize = 1000;
  for (int i = 0; i < N; i++) {
    if ((i % 100) == 0) {
      fprintf(stderr, "Run %d of %d\n", i, N);
    }
    TestState state(seed + 1);
    Env::Default()->SetBackgroundThreads(1);
    Env::Default()->Schedule(ConcurrentReader, &state);
    state.Wait(TestState::RUNNING);
    for (int k = 0; k < kSize; k++) {
      state.t_.WriteStep(&rnd);
    }
    state.quit_flag_.store(true, std::memory_order_release);
    state.Wait(TestState::DONE);
  }
}

TEST_F(SkipTest, Concurrent1) { RunConcurrent(1); }
TEST_F(SkipTest, Concurrent2) { RunConcurrent(2); }
TEST_F(SkipTest, Concurrent3) { RunConcurrent(3); }
TEST_F(SkipTest, Concurrent4) { RunConcurrent(4); }
TEST_F(SkipTest, Concurrent5) { RunConcurrent(5); }

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
