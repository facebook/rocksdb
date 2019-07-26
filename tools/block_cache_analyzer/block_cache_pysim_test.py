#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import random

from block_cache_pysim import (
    HashTable,
    LFUPolicy,
    LinUCBCache,
    LRUPolicy,
    MRUPolicy,
    ThompsonSamplingCache,
    TraceRecord,
    kSampleSize,
)


def test_hash_table():
    print("Test hash table")
    table = HashTable()
    data_size = 10000
    for i in range(data_size):
        table.insert("k{}".format(i), i, "v{}".format(i))
    for i in range(data_size):
        assert table.lookup("k{}".format(i), i) is not None
    for i in range(data_size):
        table.delete("k{}".format(i), i)
    for i in range(data_size):
        assert table.lookup("k{}".format(i), i) is None

    truth_map = {}
    n = 1000000
    records = 100
    for i in range(n):
        key_id = random.randint(0, records)
        key = "k{}".format(key_id)
        value = "v{}".format(key_id)
        action = random.randint(0, 2)
        # print "{}:{}:{}".format(action, key, value)
        assert len(truth_map) == table.elements, "{} {} {}".format(
            len(truth_map), table.elements, i
        )
        if action == 0:
            table.insert(key, key_id, value)
            truth_map[key] = value
        elif action == 1:
            if key in truth_map:
                assert table.lookup(key, key_id) is not None
                assert truth_map[key] == table.lookup(key, key_id)
            else:
                assert table.lookup(key, key_id) is None
        else:
            table.delete(key, key_id)
            if key in truth_map:
                del truth_map[key]
    print("Test hash table: Success")


def assert_metrics(cache, expected_value):
    assert cache.used_size == expected_value[0], "Expected {}, Actual {}".format(
        expected_value[0], cache.used_size
    )
    assert (
        cache.miss_ratio_stats.num_accesses == expected_value[1]
    ), "Expected {}, Actual {}".format(
        expected_value[1], cache.miss_ratio_stats.num_accesses
    )
    assert (
        cache.miss_ratio_stats.num_misses == expected_value[2]
    ), "Expected {}, Actual {}".format(
        expected_value[2], cache.miss_ratio_stats.num_misses
    )
    assert cache.table.elements == len(expected_value[3]) + len(
        expected_value[4]
    ), "Expected {}, Actual {}".format(
        len(expected_value[3]) + len(expected_value[4]), cache.table.elements
    )
    for expeceted_k in expected_value[3]:
        val = cache.table.lookup("b{}".format(expeceted_k), expeceted_k)
        assert val is not None
        assert val.value_size == 1
    for expeceted_k in expected_value[4]:
        val = cache.table.lookup("g{}".format(expeceted_k), expeceted_k)
        assert val is not None
        assert val.value_size == 1


# Access k1, k1, k2, k3, k3, k3, k4
def test_cache(policies, expected_value):
    cache = ThompsonSamplingCache(3, False, policies)
    k1 = TraceRecord(
        access_time=0,
        block_id=1,
        block_type=1,
        block_size=1,
        cf_id=0,
        cf_name="",
        level=0,
        fd=0,
        caller=1,
        no_insert=0,
        get_id=1,
        key_id=1,
        kv_size=5,
        is_hit=1,
    )
    k2 = TraceRecord(
        access_time=1,
        block_id=2,
        block_type=1,
        block_size=1,
        cf_id=0,
        cf_name="",
        level=0,
        fd=0,
        caller=1,
        no_insert=0,
        get_id=1,
        key_id=1,
        kv_size=5,
        is_hit=1,
    )
    k3 = TraceRecord(
        access_time=2,
        block_id=3,
        block_type=1,
        block_size=1,
        cf_id=0,
        cf_name="",
        level=0,
        fd=0,
        caller=1,
        no_insert=0,
        get_id=1,
        key_id=1,
        kv_size=5,
        is_hit=1,
    )
    k4 = TraceRecord(
        access_time=3,
        block_id=4,
        block_type=1,
        block_size=1,
        cf_id=0,
        cf_name="",
        level=0,
        fd=0,
        caller=1,
        no_insert=0,
        get_id=1,
        key_id=1,
        kv_size=5,
        is_hit=1,
    )
    sequence = [k1, k1, k2, k3, k3, k3]
    index = 0
    expected_values = []
    # Access k1, miss.
    expected_values.append([1, 1, 1, [1], []])
    # Access k1, hit.
    expected_values.append([1, 2, 1, [1], []])
    # Access k2, miss.
    expected_values.append([2, 3, 2, [1, 2], []])
    # Access k3, miss.
    expected_values.append([3, 4, 3, [1, 2, 3], []])
    # Access k3, hit.
    expected_values.append([3, 5, 3, [1, 2, 3], []])
    # Access k3, hit.
    expected_values.append([3, 6, 3, [1, 2, 3], []])
    for access in sequence:
        cache.access(access)
        assert_metrics(cache, expected_values[index])
        index += 1
    cache.access(k4)
    assert_metrics(cache, expected_value)


def test_lru_cache():
    print("Test LRU cache")
    policies = []
    policies.append(LRUPolicy())
    # Access k4, miss. evict k1
    test_cache(policies, [3, 7, 4, [2, 3, 4], []])
    print("Test LRU cache: Success")


def test_mru_cache():
    print("Test MRU cache")
    policies = []
    policies.append(MRUPolicy())
    # Access k4, miss. evict k3
    test_cache(policies, [3, 7, 4, [1, 2, 4], []])
    print("Test MRU cache: Success")


def test_lfu_cache():
    print("Test LFU cache")
    policies = []
    policies.append(LFUPolicy())
    # Access k4, miss. evict k2
    test_cache(policies, [3, 7, 4, [1, 3, 4], []])
    print("Test LFU cache: Success")


def test_mix(cache):
    print("Test Mix {} cache".format(cache.cache_name()))
    n = 100000
    records = 199
    for i in range(n):
        key_id = random.randint(0, records)
        vs = random.randint(0, 10)
        k = TraceRecord(
            access_time=i,
            block_id=key_id,
            block_type=1,
            block_size=vs,
            cf_id=0,
            cf_name="",
            level=0,
            fd=0,
            caller=1,
            no_insert=0,
            get_id=key_id,
            key_id=key_id,
            kv_size=5,
            is_hit=1,
        )
        cache.access(k)
    assert cache.miss_ratio_stats.miss_ratio() > 0
    print("Test Mix {} cache: Success".format(cache.cache_name()))


def test_hybrid(cache):
    print("Test {} cache".format(cache.cache_name()))
    k = TraceRecord(
        access_time=0,
        block_id=1,
        block_type=1,
        block_size=1,
        cf_id=0,
        cf_name="",
        level=0,
        fd=0,
        caller=1,
        no_insert=0,
        get_id=1,  # the first get request.
        key_id=1,
        kv_size=0,  # no size.
        is_hit=1,
    )
    cache.access(k)  # Expect a miss.
    # used size, num accesses, num misses, hash table size, blocks, get keys.
    assert_metrics(cache, [1, 1, 1, [1], []])
    k.access_time += 1
    k.kv_size = 1
    k.block_id = 2
    cache.access(k)  # k should be inserted.
    assert_metrics(cache, [3, 2, 2, [1, 2], [1]])
    k.access_time += 1
    k.block_id = 3
    cache.access(k)  # k should not be inserted again.
    assert_metrics(cache, [4, 3, 3, [1, 2, 3], [1]])
    # A second get request referencing the same key.
    k.access_time += 1
    k.get_id = 2
    k.block_id = 4
    k.kv_size = 0
    cache.access(k)  # k should observe a hit. No block access.
    assert_metrics(cache, [4, 4, 3, [1, 2, 3], [1]])

    # A third get request searches three files, three different keys.
    # And the second key observes a hit.
    k.access_time += 1
    k.kv_size = 1
    k.get_id = 3
    k.block_id = 3
    k.key_id = 2
    cache.access(k)  # k should observe a miss. block 3 observes a hit.
    assert_metrics(cache, [5, 5, 3, [1, 2, 3], [1, 2]])

    k.access_time += 1
    k.kv_size = 1
    k.get_id = 3
    k.block_id = 4
    k.kv_size = 1
    k.key_id = 1
    cache.access(k)  # k1 should observe a hit.
    assert_metrics(cache, [5, 6, 3, [1, 2, 3], [1, 2]])

    k.access_time += 1
    k.kv_size = 1
    k.get_id = 3
    k.block_id = 4
    k.kv_size = 1
    k.key_id = 3
    # k3 should observe a miss.
    # However, as the get already complete, we should not access k3 any more.
    cache.access(k)
    assert_metrics(cache, [5, 7, 3, [1, 2, 3], [1, 2]])

    # A fourth get request searches one file and two blocks. One row key.
    k.access_time += 1
    k.get_id = 4
    k.block_id = 5
    k.key_id = 4
    k.kv_size = 1
    cache.access(k)
    assert_metrics(cache, [7, 8, 4, [1, 2, 3, 5], [1, 2, 4]])

    # A bunch of insertions which evict cached row keys.
    for i in range(6, 100):
        k.access_time += 1
        k.get_id = 0
        k.block_id = i
        cache.access(k)

    k.get_id = 4
    k.block_id = 100  # A different block.
    k.key_id = 4  # Same row key and should not be inserted again.
    k.kv_size = 1
    cache.access(k)
    assert_metrics(cache, [16, 103, 99, [i for i in range(101 - kSampleSize, 101)], []])
    print("Test {} cache: Success".format(cache.cache_name()))


if __name__ == "__main__":
    policies = []
    policies.append(MRUPolicy())
    policies.append(LRUPolicy())
    policies.append(LFUPolicy())
    test_hash_table()
    test_lru_cache()
    test_mru_cache()
    test_lfu_cache()
    test_mix(ThompsonSamplingCache(100, False, policies))
    test_mix(ThompsonSamplingCache(100, True, policies))
    test_mix(LinUCBCache(100, False, policies))
    test_mix(LinUCBCache(100, True, policies))
    test_hybrid(ThompsonSamplingCache(kSampleSize, True, [LRUPolicy()]))
    test_hybrid(LinUCBCache(kSampleSize, True, [LRUPolicy()]))
