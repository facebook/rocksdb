#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import os
import random
import sys

from block_cache_pysim import (
    ARCCache,
    CacheEntry,
    create_cache,
    GDSizeCache,
    HashTable,
    HyperbolicPolicy,
    kMicrosInSecond,
    kSampleSize,
    LFUPolicy,
    LinUCBCache,
    LRUCache,
    LRUPolicy,
    MRUPolicy,
    OPTCache,
    OPTCacheEntry,
    run,
    ThompsonSamplingCache,
    TraceCache,
    TraceRecord,
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
        v = random.randint(0, records)
        key = "k{}".format(key_id)
        value = CacheEntry(v, v, v, v, v, v, v)
        action = random.randint(0, 10)
        assert len(truth_map) == table.elements, "{} {} {}".format(
            len(truth_map), table.elements, i
        )
        if action <= 8:
            if key in truth_map:
                assert table.lookup(key, key_id) is not None
                assert truth_map[key].value_size == table.lookup(key, key_id).value_size
            else:
                assert table.lookup(key, key_id) is None
            table.insert(key, key_id, value)
            truth_map[key] = value
        else:
            deleted = table.delete(key, key_id)
            if deleted:
                assert key in truth_map
            if key in truth_map:
                del truth_map[key]

    # Check all keys are unique in the sample set.
    for _i in range(10):
        samples = table.random_sample(kSampleSize)
        unique_keys = {}
        for sample in samples:
            unique_keys[sample.key] = True
        assert len(samples) == len(unique_keys)

    assert len(table) == len(truth_map)
    for key in truth_map:
        assert table.lookup(key, int(key[1:])) is not None
        assert truth_map[key].value_size == table.lookup(key, int(key[1:])).value_size
    print("Test hash table: Success")


def assert_metrics(cache, expected_value, expected_value_size=1, custom_hashtable=True):
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
    assert len(cache.table) == len(expected_value[3]) + len(
        expected_value[4]
    ), "Expected {}, Actual {}".format(
        len(expected_value[3]) + len(expected_value[4]), cache.table.elements
    )
    for expeceted_k in expected_value[3]:
        if custom_hashtable:
            val = cache.table.lookup("b{}".format(expeceted_k), expeceted_k)
        else:
            val = cache.table["b{}".format(expeceted_k)]
        assert val is not None, "Expected {} Actual: Not Exist {}, Table: {}".format(
            expeceted_k, expected_value, cache.table
        )
        assert val.value_size == expected_value_size
    for expeceted_k in expected_value[4]:
        if custom_hashtable:
            val = cache.table.lookup("g0-{}".format(expeceted_k), expeceted_k)
        else:
            val = cache.table["g0-{}".format(expeceted_k)]
        assert val is not None
        assert val.value_size == expected_value_size


# Access k1, k1, k2, k3, k3, k3, k4
# When k4 is inserted,
#   LRU should evict k1.
#   LFU should evict k2.
#   MRU should evict k3.
def test_cache(cache, expected_value, custom_hashtable=True):
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
        referenced_key_exist_in_block=1,
        num_keys_in_block=0,
        table_id=0,
        seq_number=0,
        block_key_size=0,
        key_size=0,
        block_offset_in_file=0,
        next_access_seq_no=0,
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
        referenced_key_exist_in_block=1,
        num_keys_in_block=0,
        table_id=0,
        seq_number=0,
        block_key_size=0,
        key_size=0,
        block_offset_in_file=0,
        next_access_seq_no=0,
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
        referenced_key_exist_in_block=1,
        num_keys_in_block=0,
        table_id=0,
        seq_number=0,
        block_key_size=0,
        key_size=0,
        block_offset_in_file=0,
        next_access_seq_no=0,
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
        referenced_key_exist_in_block=1,
        num_keys_in_block=0,
        table_id=0,
        seq_number=0,
        block_key_size=0,
        key_size=0,
        block_offset_in_file=0,
        next_access_seq_no=0,
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
    access_time = 0
    for access in sequence:
        access.access_time = access_time
        cache.access(access)
        assert_metrics(
            cache,
            expected_values[index],
            expected_value_size=1,
            custom_hashtable=custom_hashtable,
        )
        access_time += 1
        index += 1
    k4.access_time = access_time
    cache.access(k4)
    assert_metrics(
        cache, expected_value, expected_value_size=1, custom_hashtable=custom_hashtable
    )


def test_lru_cache(cache, custom_hashtable):
    print("Test LRU cache")
    # Access k4, miss. evict k1
    test_cache(cache, [3, 7, 4, [2, 3, 4], []], custom_hashtable)
    print("Test LRU cache: Success")


def test_mru_cache():
    print("Test MRU cache")
    policies = []
    policies.append(MRUPolicy())
    # Access k4, miss. evict k3
    test_cache(
        ThompsonSamplingCache(3, False, policies, cost_class_label=None),
        [3, 7, 4, [1, 2, 4], []],
    )
    print("Test MRU cache: Success")


def test_lfu_cache():
    print("Test LFU cache")
    policies = []
    policies.append(LFUPolicy())
    # Access k4, miss. evict k2
    test_cache(
        ThompsonSamplingCache(3, False, policies, cost_class_label=None),
        [3, 7, 4, [1, 3, 4], []],
    )
    print("Test LFU cache: Success")


def test_mix(cache):
    print("Test Mix {} cache".format(cache.cache_name()))
    n = 100000
    records = 100
    block_size_table = {}
    trace_num_misses = 0
    for i in range(n):
        key_id = random.randint(0, records)
        vs = random.randint(0, 10)
        now = i * kMicrosInSecond
        block_size = vs
        if key_id in block_size_table:
            block_size = block_size_table[key_id]
        else:
            block_size_table[key_id] = block_size
        is_hit = key_id % 2
        if is_hit == 0:
            trace_num_misses += 1
        k = TraceRecord(
            access_time=now,
            block_id=key_id,
            block_type=1,
            block_size=block_size,
            cf_id=0,
            cf_name="",
            level=0,
            fd=0,
            caller=1,
            no_insert=0,
            get_id=key_id,
            key_id=key_id,
            kv_size=5,
            is_hit=is_hit,
            referenced_key_exist_in_block=1,
            num_keys_in_block=0,
            table_id=0,
            seq_number=0,
            block_key_size=0,
            key_size=0,
            block_offset_in_file=0,
            next_access_seq_no=vs,
        )
        cache.access(k)
    assert cache.miss_ratio_stats.miss_ratio() > 0
    if cache.cache_name() == "Trace":
        assert cache.miss_ratio_stats.num_accesses == n
        assert cache.miss_ratio_stats.num_misses == trace_num_misses
    else:
        assert cache.used_size <= cache.cache_size
        all_values = cache.table.values()
        cached_size = 0
        for value in all_values:
            cached_size += value.value_size
        assert cached_size == cache.used_size, "Expeced {} Actual {}".format(
            cache.used_size, cached_size
        )
    print("Test Mix {} cache: Success".format(cache.cache_name()))


def test_end_to_end():
    print("Test All caches")
    n = 100000
    nblocks = 1000
    block_size = 16 * 1024
    ncfs = 7
    nlevels = 6
    nfds = 100000
    trace_file_path = "test_trace"
    # All blocks are of the same size so that OPT must achieve the lowest miss
    # ratio.
    with open(trace_file_path, "w+") as trace_file:
        access_records = ""
        for i in range(n):
            key_id = random.randint(0, nblocks)
            cf_id = random.randint(0, ncfs)
            level = random.randint(0, nlevels)
            fd = random.randint(0, nfds)
            now = i * kMicrosInSecond
            access_record = ""
            access_record += "{},".format(now)
            access_record += "{},".format(key_id)
            access_record += "{},".format(9)  # block type
            access_record += "{},".format(block_size)  # block size
            access_record += "{},".format(cf_id)
            access_record += "cf_{},".format(cf_id)
            access_record += "{},".format(level)
            access_record += "{},".format(fd)
            access_record += "{},".format(key_id % 3)  # caller
            access_record += "{},".format(0)  # no insert
            access_record += "{},".format(i)  # get_id
            access_record += "{},".format(i)  # key_id
            access_record += "{},".format(100)  # kv_size
            access_record += "{},".format(1)  # is_hit
            access_record += "{},".format(1)  # referenced_key_exist_in_block
            access_record += "{},".format(10)  # num_keys_in_block
            access_record += "{},".format(1)  # table_id
            access_record += "{},".format(0)  # seq_number
            access_record += "{},".format(10)  # block key size
            access_record += "{},".format(20)  # key size
            access_record += "{},".format(0)  # block offset
            access_record = access_record[:-1]
            access_records += access_record + "\n"
        trace_file.write(access_records)

    print("Test All caches: Start testing caches")
    cache_size = block_size * nblocks / 10
    downsample_size = 1
    cache_ms = {}
    for cache_type in [
        "ts",
        "opt",
        "lru",
        "pylru",
        "linucb",
        "gdsize",
        "pyccbt",
        "pycctbbt",
    ]:
        cache = create_cache(cache_type, cache_size, downsample_size)
        run(trace_file_path, cache_type, cache, 0, -1, "all")
        cache_ms[cache_type] = cache
        assert cache.miss_ratio_stats.num_accesses == n

    for cache_type in cache_ms:
        cache = cache_ms[cache_type]
        ms = cache.miss_ratio_stats.miss_ratio()
        assert ms <= 100.0 and ms >= 0.0
        # OPT should perform the best.
        assert cache_ms["opt"].miss_ratio_stats.miss_ratio() <= ms
        assert cache.used_size <= cache.cache_size
        all_values = cache.table.values()
        cached_size = 0
        for value in all_values:
            cached_size += value.value_size
        assert cached_size == cache.used_size, "Expeced {} Actual {}".format(
            cache.used_size, cached_size
        )
        print("Test All {}: Success".format(cache.cache_name()))

    os.remove(trace_file_path)
    print("Test All: Success")


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
        referenced_key_exist_in_block=1,
        num_keys_in_block=0,
        table_id=0,
        seq_number=0,
        block_key_size=0,
        key_size=0,
        block_offset_in_file=0,
        next_access_seq_no=0,
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
    assert_metrics(
        cache, [kSampleSize, 103, 99, [i for i in range(101 - kSampleSize, 101)], []]
    )
    print("Test {} cache: Success".format(cache.cache_name()))


def test_opt_cache():
    print("Test OPT cache")
    cache = OPTCache(3)
    # seq:         0,  1,  2,  3,  4,  5,  6,  7,  8
    # key:         k1, k2, k3, k4, k5, k6, k7, k1, k8
    # next_access: 7,  19, 18, M,  M,  17, 16, 25, M
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
        referenced_key_exist_in_block=1,
        num_keys_in_block=0,
        table_id=0,
        seq_number=0,
        block_key_size=0,
        key_size=0,
        block_offset_in_file=0,
        next_access_seq_no=7,
    )
    cache.access(k)
    assert_metrics(
        cache, [1, 1, 1, [1], []], expected_value_size=1, custom_hashtable=False
    )
    k.access_time += 1
    k.block_id = 2
    k.next_access_seq_no = 19
    cache.access(k)
    assert_metrics(
        cache, [2, 2, 2, [1, 2], []], expected_value_size=1, custom_hashtable=False
    )
    k.access_time += 1
    k.block_id = 3
    k.next_access_seq_no = 18
    cache.access(k)
    assert_metrics(
        cache, [3, 3, 3, [1, 2, 3], []], expected_value_size=1, custom_hashtable=False
    )
    k.access_time += 1
    k.block_id = 4
    k.next_access_seq_no = sys.maxsize  # Never accessed again.
    cache.access(k)
    # Evict 2 since its next access 19 is the furthest in the future.
    assert_metrics(
        cache, [3, 4, 4, [1, 3, 4], []], expected_value_size=1, custom_hashtable=False
    )
    k.access_time += 1
    k.block_id = 5
    k.next_access_seq_no = sys.maxsize  # Never accessed again.
    cache.access(k)
    # Evict 4 since its next access MAXINT is the furthest in the future.
    assert_metrics(
        cache, [3, 5, 5, [1, 3, 5], []], expected_value_size=1, custom_hashtable=False
    )
    k.access_time += 1
    k.block_id = 6
    k.next_access_seq_no = 17
    cache.access(k)
    # Evict 5 since its next access MAXINT is the furthest in the future.
    assert_metrics(
        cache, [3, 6, 6, [1, 3, 6], []], expected_value_size=1, custom_hashtable=False
    )
    k.access_time += 1
    k.block_id = 7
    k.next_access_seq_no = 16
    cache.access(k)
    # Evict 3 since its next access 18 is the furthest in the future.
    assert_metrics(
        cache, [3, 7, 7, [1, 6, 7], []], expected_value_size=1, custom_hashtable=False
    )
    k.access_time += 1
    k.block_id = 1
    k.next_access_seq_no = 25
    cache.access(k)
    assert_metrics(
        cache, [3, 8, 7, [1, 6, 7], []], expected_value_size=1, custom_hashtable=False
    )
    k.access_time += 1
    k.block_id = 8
    k.next_access_seq_no = sys.maxsize
    cache.access(k)
    # Evict 1 since its next access 25 is the furthest in the future.
    assert_metrics(
        cache, [3, 9, 8, [6, 7, 8], []], expected_value_size=1, custom_hashtable=False
    )

    # Insert a large kv pair to evict all keys.
    k.access_time += 1
    k.block_id = 10
    k.block_size = 3
    k.next_access_seq_no = sys.maxsize
    cache.access(k)
    assert_metrics(
        cache, [3, 10, 9, [10], []], expected_value_size=3, custom_hashtable=False
    )
    print("Test OPT cache: Success")


def test_trace_cache():
    print("Test trace cache")
    cache = TraceCache(0)
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
        get_id=1,
        key_id=1,
        kv_size=0,
        is_hit=1,
        referenced_key_exist_in_block=1,
        num_keys_in_block=0,
        table_id=0,
        seq_number=0,
        block_key_size=0,
        key_size=0,
        block_offset_in_file=0,
        next_access_seq_no=7,
    )
    cache.access(k)
    assert cache.miss_ratio_stats.num_accesses == 1
    assert cache.miss_ratio_stats.num_misses == 0
    k.is_hit = 0
    cache.access(k)
    assert cache.miss_ratio_stats.num_accesses == 2
    assert cache.miss_ratio_stats.num_misses == 1
    print("Test trace cache: Success")


if __name__ == "__main__":
    test_hash_table()
    test_trace_cache()
    test_opt_cache()
    test_lru_cache(
        ThompsonSamplingCache(
            3, enable_cache_row_key=0, policies=[LRUPolicy()], cost_class_label=None
        ),
        custom_hashtable=True,
    )
    test_lru_cache(LRUCache(3, enable_cache_row_key=0), custom_hashtable=False)
    test_mru_cache()
    test_lfu_cache()
    test_hybrid(
        ThompsonSamplingCache(
            kSampleSize,
            enable_cache_row_key=1,
            policies=[LRUPolicy()],
            cost_class_label=None,
        )
    )
    test_hybrid(
        LinUCBCache(
            kSampleSize,
            enable_cache_row_key=1,
            policies=[LRUPolicy()],
            cost_class_label=None,
        )
    )
    for cache_type in [
        "ts",
        "opt",
        "arc",
        "pylfu",
        "pymru",
        "trace",
        "pyhb",
        "lru",
        "pylru",
        "linucb",
        "gdsize",
        "pycctbbt",
        "pycctb",
        "pyccbt",
    ]:
        for enable_row_cache in [0, 1, 2]:
            cache_type_str = cache_type
            if cache_type != "opt" and cache_type != "trace":
                if enable_row_cache == 1:
                    cache_type_str += "_hybrid"
                elif enable_row_cache == 2:
                    cache_type_str += "_hybridn"
            test_mix(create_cache(cache_type_str, cache_size=100, downsample_size=1))
    test_end_to_end()
