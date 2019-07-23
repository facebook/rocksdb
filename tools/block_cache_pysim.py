import csv
import gc
import os
import random
import subprocess
import sys
import time
from os import path

import numpy as np


kSampleSize = 16  # The sample size used when performing eviction.
kMicrosInSecond = 1000000
kSecondsInMinute = 60
kSecondsInHour = 3600


class TraceRecord:
    """A trace record represents a block access."""

    def __init__(
        self,
        access_time,
        block_id,
        block_type,
        block_size,
        cf_id,
        level,
        fd,
        caller,
        no_insert,
        get_id,
        key_id,
        kv_size,
        is_hit,
    ):
        self.access_time = access_time
        self.block_id = block_id
        self.block_type = block_type
        self.block_size = block_size
        self.cf_id = cf_id
        self.level = level
        self.fd = fd
        self.caller = caller
        if no_insert == 1:
            self.no_insert = True
        else:
            self.no_insert = False
        self.get_id = get_id
        self.key_id = key_id
        self.kv_size = kv_size
        if is_hit == 1:
            self.is_hit = True
        else:
            self.is_hit = False


class CacheEntry:
    """A cache entry stored in the cache."""

    def __init__(self, value_size, cf_id, level, block_type, access_number):
        self.value_size = value_size
        self.last_access_number = access_number
        self.num_hits = 0
        self.cf_id = 0
        self.level = level
        self.block_type = block_type

    def __repr__(self):
        """Debug string."""
        return "s={},last={},hits={},cf={},l={},bt={}".format(
            self.value_size,
            self.last_access_number,
            self.num_hits,
            self.cf_id,
            self.level,
            self.block_type,
        )


class HashEntry:
    """A hash entry stored in a hash table."""

    def __init__(self, key, hash, value):
        self.key = key
        self.hash = hash
        self.value = value

    def __repr__(self):
        return "k={},h={},v=[{}]".format(self.key, self.hash, self.value)


class HashTable:
    """
    A custom implementation of hash table to support fast random sampling.
    It is closed hashing and uses chaining to resolve hash conflicts. It grows/shrinks the hash table upon insertion/deletion to support fast lookups and random samplings.
    """

    def __init__(self):
        self.table = [None] * 32
        self.elements = 0

    def random_sample(self, sample_size):
        """Randomly sample 'sample_size' hash entries from the table."""
        samples = []
        index = random.randint(0, len(self.table))
        pos = (index + 1) % len(self.table)
        searches = 0
        # Starting from index, adding hash entries to the sample list until
        # sample_size is met or we ran out of entries.
        while pos != index and len(samples) < sample_size:
            if self.table[pos] is not None:
                for i in range(len(self.table[pos])):
                    if self.table[pos][i] is None:
                        continue
                    samples.append(self.table[pos][i])
                    if len(samples) > sample_size:
                        break
            pos += 1
            pos = pos % len(self.table)
            searches += 1
        return samples

    def insert(self, key, hash, value):
        """Insert a hash entry in the table. Replace the old entry if it already exists."""
        self.grow()
        inserted = False
        index = hash % len(self.table)
        if self.table[index] is None:
            self.table[index] = []
        for i in range(len(self.table[index])):
            if self.table[index][i] is not None:
                if (
                    self.table[index][i].hash == hash
                    and self.table[index][i].key == key
                ):
                    # The entry already exists in the table.
                    self.table[index][i] = HashEntry(key, hash, value)
                    return
                continue
            self.table[index][i] = HashEntry(key, hash, value)
            inserted = True
            break
        if not inserted:
            self.table[index].append(HashEntry(key, hash, value))
        self.elements += 1

    def resize(self, new_size):
        if new_size == len(self.table):
            return
        if new_size == 0:
            return
        if self.elements < 100:
            return
        new_table = [None] * new_size
        # Copy 'self.table' to new_table.
        for i in range(len(self.table)):
            entries = self.table[i]
            if entries is None:
                continue
            for j in range(len(entries)):
                if entries[j] is None:
                    continue
                index = entries[j].hash % new_size
                if new_table[index] is None:
                    new_table[index] = []
                new_table[index].append(entries[j])
        self.table = new_table
        del new_table
        # Manually call python gc here to free the memory as 'self.table'
        # might be very large.
        gc.collect()

    def grow(self):
        if self.elements < len(self.table):
            return
        new_size = int(len(self.table) * 1.2)
        self.resize(new_size)

    def delete(self, key, hash):
        index = hash % len(self.table)
        entries = self.table[index]
        deleted = False
        if entries is None:
            return
        for i in range(len(entries)):
            if (
                entries[i] is not None
                and entries[i].hash == hash
                and entries[i].key == key
            ):
                entries[i] = None
                self.elements -= 1
                deleted = True
                break
        if deleted:
            self.shrink()

    def shrink(self):
        if self.elements * 2 >= len(self.table):
            return
        new_size = int(len(self.table) * 0.7)
        self.resize(new_size)

    def lookup(self, key, hash):
        index = hash % len(self.table)
        entries = self.table[index]
        if entries is None:
            return None
        for entry in entries:
            if entry is not None and entry.hash == hash and entry.key == key:
                return entry.value
        return None


class MissRatioStats:
    def __init__(self, time_unit):
        self.num_misses = 0
        self.num_accesses = 0
        self.time_unit = time_unit
        self.time_misses = {}
        self.time_accesses = {}

    def update_metrics(self, access_time, is_hit):
        access_time /= kMicrosInSecond * self.time_unit
        self.num_accesses += 1
        if access_time not in self.time_accesses:
            self.time_accesses[access_time] = 0
        self.time_accesses[access_time] += 1
        if not is_hit:
            self.num_misses += 1
            if access_time not in self.time_misses:
                self.time_misses[access_time] = 0
            self.time_misses[access_time] += 1

    def reset_counter(self):
        self.num_misses = 0
        self.num_accesses = 0

    def miss_ratio(self):
        return float(self.num_misses) * 100.0 / float(self.num_accesses)

    def write_miss_timeline(self, cache_type, cache_size, result_dir, start, end):
        start /= kMicrosInSecond * self.time_unit
        end /= kMicrosInSecond * self.time_unit
        header_file_path = "{}/header-ml-miss-timeline-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size
        )
        if not path.exists(header_file_path):
            header_file = open(header_file_path, "w+")
            header = "time"
            for time in range(start, end):
                header += ",{}".format(time)
            header_file.write(header + "\n")
            header_file.close()
        file_path = "{}/data-ml-miss-timeline-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size
        )
        file = open(file_path, "w+")
        row = "{}".format(cache_type)
        for time in range(start, end):
            row += ",{}".format(self.time_misses.get(time, 0))
        file.write(row + "\n")
        file.close()

    def write_miss_ratio_timeline(self, cache_type, cache_size, result_dir, start, end):
        start /= kMicrosInSecond * self.time_unit
        end /= kMicrosInSecond * self.time_unit
        header_file_path = "{}/header-ml-miss-ratio-timeline-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size
        )
        if not path.exists(header_file_path):
            header_file = open(header_file_path, "w+")
            header = "time"
            for time in range(start, end):
                header += ",{}".format(time)
            header_file.write(header + "\n")
            header_file.close()
        file_path = "{}/data-ml-miss-ratio-timeline-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size
        )
        file = open(file_path, "w+")
        row = "{}".format(cache_type)
        for time in range(start, end):
            naccesses = self.time_accesses.get(time, 0)
            miss_ratio = 0
            if naccesses > 0:
                miss_ratio = float(self.time_misses.get(time, 0) * 100.0) / float(
                    naccesses
                )
            row += ",{0:.2f}".format(miss_ratio)
        file.write(row + "\n")
        file.close()


class PolicyStats:
    def __init__(self, time_unit):
        self.time_selected_polices = {}
        self.time_accesses = {}
        self.policy_names = {}
        self.time_unit = time_unit
        self.policy_names[0] = "lru"
        self.policy_names[1] = "mru"
        self.policy_names[2] = "lfu"

    def update_metrics(self, access_time, selected_policy):
        access_time /= kMicrosInSecond * self.time_unit
        if access_time not in self.time_accesses:
            self.time_accesses[access_time] = 0
        self.time_accesses[access_time] += 1
        if access_time not in self.time_selected_polices:
            self.time_selected_polices[access_time] = {}
        policy_name = self.policy_names[selected_policy]
        if policy_name not in self.time_selected_polices[access_time]:
            self.time_selected_polices[access_time][policy_name] = 0
        self.time_selected_polices[access_time][policy_name] += 1

    def write_policy_timeline(self, cache_type, cache_size, result_dir, start, end):
        start /= kMicrosInSecond * self.time_unit
        end /= kMicrosInSecond * self.time_unit
        header_file_path = "{}/header-ml-policy-timeline-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size
        )
        if not path.exists(header_file_path):
            header_file = open(header_file_path, "w+")
            header = "time"
            for time in range(start, end):
                header += ",{}".format(time)
            header_file.write(header + "\n")
            header_file.close()
        file_path = "{}/data-ml-policy-timeline-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size
        )
        file = open(file_path, "w+")
        for policy in self.policy_names:
            policy_name = self.policy_names[policy]
            row = "{}-{}".format(cache_type, policy_name)
            for time in range(start, end):
                row += ",{}".format(
                    self.time_selected_polices.get(time, {}).get(policy_name, 0)
                )
            file.write(row + "\n")
        file.close()

    def write_policy_ratio_timeline(
        self, cache_type, cache_size, file_path, start, end
    ):
        start /= kMicrosInSecond * self.time_unit
        end /= kMicrosInSecond * self.time_unit
        header_file_path = "{}/header-ml-policy-ratio-timeline-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size
        )
        if not path.exists(header_file_path):
            header_file = open(header_file_path, "w+")
            header = "time"
            for time in range(start, end):
                header += ",{}".format(time)
            header_file.write(header + "\n")
            header_file.close()
        file_path = "{}/data-ml-policy-ratio-timeline-{}-{}-{}".format(
            result_dir, self.time_unit, cache_type, cache_size
        )
        file = open(file_path, "w+")
        for policy in self.policy_names:
            policy_name = self.policy_names[policy]
            row = "{}-{}".format(cache_type, policy_name)
            for time in range(start, end):
                naccesses = self.time_accesses.get(time, 0)
                ratio = 0
                if naccesses > 0:
                    ratio = float(
                        self.time_selected_polices.get(time, {}).get(policy_name, 0)
                        * 100.0
                    ) / float(naccesses)
                row += ",{0:.2f}".format(ratio)
            file.write(row + "\n")
        file.close()


class Policy(object):
    """A policy maintains a set of evicted keys. It returns a reward of one to itself if it has not evicted a missing key. Otherwise, it gives itself 0 reward."""

    def __init__(self):
        self.evicted_keys = {}

    def evict(self, key, max_size):
        self.evicted_keys[key] = 0

    def delete(self, key):
        self.evicted_keys.pop(key, None)

    def prioritize_samples(self, samples):
        raise NotImplementedError

    def generate_reward(self, key):
        if key in self.evicted_keys:
            return 0
        return 1


class LRUPolicy(Policy):
    def prioritize_samples(self, samples):
        return sorted(
            samples,
            cmp=lambda e1, e2: e1.value.last_access_number
            - e2.value.last_access_number,
        )


class MRUPolicy(Policy):
    def prioritize_samples(self, samples):
        return sorted(
            samples,
            cmp=lambda e1, e2: e2.value.last_access_number
            - e1.value.last_access_number,
        )


class LFUPolicy(Policy):
    def prioritize_samples(self, samples):
        return sorted(samples, cmp=lambda e1, e2: e1.value.num_hits - e2.value.num_hits)


class MLCache(object):
    def __init__(self, cache_size, enable_cache_row_key, policies):
        self.cache_size = cache_size
        self.used_size = 0
        self.miss_ratio_stats = MissRatioStats(kSecondsInMinute)
        self.policy_stats = PolicyStats(kSecondsInMinute)
        self.per_hour_miss_ratio_stats = MissRatioStats(kSecondsInHour)
        self.per_hour_policy_stats = PolicyStats(kSecondsInHour)
        self.cache = HashTable()
        self.enable_cache_row_key = enable_cache_row_key
        self.get_id_row_key_map = {}
        self.policies = policies

    def lookup(self, key, hash):
        value = self.cache.lookup(key, hash)
        if value is not None:
            value.last_access_number = self.miss_ratio_stats.num_accesses
            value.num_hits += 1
            return True
        return False

    def select_policy(self, trace_record, key):
        raise NotImplementedError

    def evict(self, policy_index, value_size):
        # Randomly sample n entries.
        samples = self.cache.random_sample(kSampleSize)
        samples = self.policies[policy_index].prioritize_samples(samples)
        for hash_entry in samples:
            self.used_size -= hash_entry.value.value_size
            self.cache.delete(hash_entry.key, hash_entry.hash)
            self.policies[policy_index].evict(
                key=hash_entry.key, max_size=self.cache.elements
            )
            if self.used_size + value_size <= self.cache_size:
                break

    def insert(self, trace_record, key, hash, value_size):
        if value_size > self.cache_size:
            return
        policy_index = self.select_policy(trace_record, key)
        self.policies[policy_index].delete(key)
        self.policy_stats.update_metrics(trace_record.access_time, policy_index)
        self.per_hour_policy_stats.update_metrics(
            trace_record.access_time, policy_index
        )
        while self.used_size + value_size > self.cache_size:
            self.evict(policy_index, value_size)
        self.cache.insert(
            key,
            hash,
            CacheEntry(
                value_size,
                trace_record.cf_id,
                trace_record.level,
                trace_record.block_type,
                self.miss_ratio_stats.num_accesses,
            ),
        )
        self.used_size += value_size

    def access_kv(self, trace_record, key, hash, value_size, no_insert):
        if self.lookup(key, hash):
            return True
        if not no_insert and value_size > 0:
            self.insert(trace_record, key, hash, value_size)
        return False

    def update_stats(self, access_time, is_hit):
        self.miss_ratio_stats.update_metrics(access_time, is_hit)
        self.per_hour_miss_ratio_stats.update_metrics(access_time, is_hit)

    def access(self, trace_record):
        assert self.used_size <= self.cache_size
        if (
            self.enable_cache_row_key
            and trace_record.caller == 1
            and trace_record.key_id != 0
        ):
            # This is a get request.
            if trace_record.get_id not in self.get_id_row_key_map:
                self.get_id_row_key_map[trace_record.get_id] = {}
            if trace_record.key_id not in self.get_id_row_key_map[trace_record.get_id]:
                # First time seen this key.
                is_hit = self.access_kv(
                    trace_record,
                    key="g{}".format(trace_record.key_id),
                    hash=trace_record.key_id,
                    value_size=trace_record.kv_size,
                    no_insert=False,
                )
                inserted = False
                if trace_record.kv_size > 0:
                    inserted = True
                self.get_id_row_key_map[trace_record.get_id][trace_record.key_id] = (
                    is_hit,
                    inserted,
                )
            if self.get_id_row_key_map[trace_record.get_id][trace_record.key_id]:
                # hit.
                self.update_stats(trace_record.access_time, is_hit=True)
                return
            # Access its blocks.
            is_hit = self.access_kv(
                trace_record,
                key="b{}".format(trace_record.block_id),
                hash=trace_record.block_id,
                value_size=trace_record.block_size,
                no_insert=trace_record.no_insert,
            )
            self.update_stats(trace_record.access_time, is_hit)
            if (
                trace_record.kv_size > 0
                and not self.get_id_row_key_map[trace_record.get_id][
                    trace_record.key_id
                ].inserted
            ):
                self.insert(
                    trace_record,
                    "g{}".format(trace_record.key_id),
                    trace_record.key_id,
                    trace_record.kv_size,
                    no_insert=False,
                )
                # Mark as inserted.
                self.get_id_row_key_map[trace_record.get_id][trace_record.key_id] = (
                    False,
                    True,
                )
            return
        # Access the block.
        is_hit = self.access_kv(
            trace_record,
            key="b{}".format(trace_record.block_id),
            hash=trace_record.block_id,
            value_size=trace_record.block_size,
            no_insert=trace_record.no_insert,
        )
        self.update_stats(trace_record.access_time, is_hit)


class ThompsonSamplingCache(MLCache):
    """
    An implementation of Thompson Sampling for the Bernoulli Bandit [1].
    [1] Daniel J. Russo, Benjamin Van Roy, Abbas Kazerouni, Ian Osband,
    and Zheng Wen. 2018. A Tutorial on Thompson Sampling. Found.
    Trends Mach. Learn. 11, 1 (July 2018), 1-96.
    DOI: https://doi.org/10.1561/2200000070
    """

    def __init__(self, cache_size, enable_cache_row_key, policies, init_a=1, init_b=1):
        super(ThompsonSamplingCache, self).__init__(
            cache_size, enable_cache_row_key, policies
        )
        self._as = {}
        self._bs = {}
        for i in range(len(policies)):
            self._as = [init_a] * len(self.policies)
            self._bs = [init_b] * len(self.policies)

    def select_policy(self, trace_record, key):
        samples = [
            np.random.beta(self._as[x], self._bs[x]) for x in range(len(self.policies))
        ]
        selected_policy = max(range(len(self.policies)), key=lambda x: samples[x])
        reward = self.policies[selected_policy].generate_reward(key)
        assert reward <= 1 and reward >= 0
        self._as[selected_policy] += reward
        self._bs[selected_policy] += 1 - reward
        return selected_policy


class LinUCBCache(MLCache):
    """
    An implementation of LinUCB with disjoint linear models [2].
    [2] Lihong Li, Wei Chu, John Langford, and Robert E. Schapire. 2010.
    A contextual-bandit approach to personalized news article recommendation.
    In Proceedings of the 19th international conference on World wide web
    (WWW '10). ACM, New York, NY, USA, 661-670.
    DOI=http://dx.doi.org/10.1145/1772690.1772758
    """
    def __init__(self, cache_size, enable_cache_row_key, policies, nfeatures):
        super(LinUCBCache, self).__init__(cache_size, enable_cache_row_key, policies)
        self.nfeatures = nfeatures
        self.th = np.zeros(
            (len(self.policies), self.nfeatures)
        )  # our real theta, what we will try to guess/
        self.eps = 0.2
        self.b = np.zeros_like(self.th)
        self.A = np.zeros((len(self.policies), self.nfeatures, self.nfeatures))
        self.A_inv = np.zeros((len(self.policies), self.nfeatures, self.nfeatures))
        for i in range(len(self.policies)):
            self.A[i] = np.identity(self.nfeatures)
        self.th_hat = np.zeros_like(
            self.th
        )  # our temporary feature vectors, our best current guesses
        self.p = np.zeros(len(self.policies))
        self.alph = 0.2

    def select_policy(self, trace_record, key):
        x_i = np.zeros(self.nfeatures)  # the current context vector
        x_i[0] = trace_record.block_type
        x_i[1] = trace_record.caller
        x_i[2] = trace_record.level
        x_i[3] = trace_record.cf_id
        p = np.zeros(len(self.policies))
        for a in range(len(self.policies)):
            self.th_hat[a] = self.A_inv[a].dot(self.b[a])
            ta = x_i.dot(self.A_inv[a]).dot(x_i)
            a_upper_ci = self.alph * np.sqrt(ta)
            a_mean = self.th_hat[a].dot(x_i)
            p[a] = a_mean + a_upper_ci
        p = p + (np.random.random(len(p)) * 0.000001)
        selected_policy = p.argmax()
        reward = self.policies[selected_policy].generate_reward(key)
        assert reward <= 1 and reward >= 0
        self.A[selected_policy] += np.outer(x_i, x_i)
        self.b[selected_policy] += reward * x_i
        self.A_inv[selected_policy] = np.linalg.inv(self.A[selected_policy])
        del x_i
        return selected_policy


def parse_cache_size(cs):
    cs = cs.replace("\n", "")
    if cs[-1] == "M":
        return int(cs[: len(cs) - 1]) * 1024 * 1024
    if cs[-1] == "G":
        return int(cs[: len(cs) - 1]) * 1024 * 1024 * 1024
    if cs[-1] == "T":
        return int(cs[: len(cs) - 1]) * 1024 * 1024 * 1024 * 1024
    return int(cs)


def create_cache(cache_type, cache_size, downsample_size):
    ml_config_caches = {}
    policies = []
    policies.append(LRUPolicy())
    policies.append(MRUPolicy())
    policies.append(LFUPolicy())
    linucb_nfeatures = 4
    cache_size = cache_size / downsample_size
    enable_cache_row_key = False
    if "hybrid" in cache_type:
        enable_cache_row_key = True
        cache_type = cache_type[:-7]
    if cache_type == "ts":
        return ThompsonSamplingCache(cache_size, enable_cache_row_key, policies)
    elif cache_type == "linucb":
        return LinUCBCache(cache_size, enable_cache_row_key, policies, linucb_nfeatures)
    else:
        print("Unknown cache type {}".format(cache_type))
        assert false
    return None


def parse_cache_config(cache_config_path, downsample_size):
    ml_config_caches = {}
    with open(cache_config_path, "r") as cache_config:
        for line in cache_config:
            ems = line.split(",")
            ml_config_caches[ems[0]] = []
            for i in range(3, len(ems)):
                cache_size = parse_cache_size(ems[i]) / downsample_size
                print("Cache config: {}, {}".format(ems[0], cache_size))
                ml_config_caches[ems[0]].append(
                    create_cache(cache_type, cache_size, downsample_size)
                )
    return ml_config_caches


def run_with_caches(trace_file_path, ml_config_caches, warmup_seconds):
    warmup_complete = False
    num = 0
    trace_start_time = 0
    trace_duration = 0
    start_time = time.time()
    time_interval = 1
    trace_miss_ratio_stats = MissRatioStats(kSecondsInMinute)
    with open(trace_file_path, "r") as trace_file:
        for line in trace_file:
            # if num > 100000:
            #     break
            num += 1
            if num % 1000000 == 0:
                gc.collect()
            ts = line.split(",")
            timestamp = int(ts[0])
            if trace_start_time == 0:
                trace_start_time = timestamp
            trace_duration = timestamp - trace_start_time
            if not warmup_complete and trace_duration > warmup_seconds * 1000000:
                for ml_config in ml_config_caches:
                    for cache in ml_config_caches[ml_config]:
                        cache.miss_ratio_stats.reset_counter()
                warmup_complete = True
            record = TraceRecord(
                access_time=int(ts[0]),
                block_id=int(ts[1]),
                block_type=int(ts[2]),
                block_size=int(ts[3]),
                cf_id=int(ts[4]),
                level=int(ts[5]),
                fd=int(ts[6]),
                caller=int(ts[7]),
                no_insert=int(ts[8]),
                get_id=int(ts[9]),
                key_id=int(ts[10]),
                kv_size=int(ts[11]),
                is_hit=int(ts[12]),
            )
            trace_miss_ratio_stats.update_metrics(
                record.access_time, is_hit=record.is_hit
            )
            for ml_config in ml_config_caches:
                for cache in ml_config_caches[ml_config]:
                    cache.access(record)
            del record
            if num % 100 != 0:
                continue
            now = time.time()
            if now - start_time > time_interval * 10:
                print(
                    "Take {} seconds to process {} trace records with trace duration of {} seconds. Throughput: {} records/second. Trace miss ratio {}".format(
                        now - start_time,
                        num,
                        trace_duration / 1000000,
                        num / (now - start_time),
                        trace_miss_ratio_stats.miss_ratio(),
                    )
                )
                time_interval += 1
                for ml_config in ml_config_caches:
                    for cache in ml_config_caches[ml_config]:
                        print(
                            "{},0,0,{},{},{}".format(
                                ml_config,
                                cache.cache_size,
                                cache.miss_ratio_stats.miss_ratio(),
                                cache.miss_ratio_stats.num_accesses,
                            )
                        )
    now = time.time()
    print(
        "Take {} seconds to process {} trace records with trace duration of {} seconds. Throughput: {} records/second. Trace miss ratio {}".format(
            now - start_time,
            num,
            trace_duration / 1000000,
            num / (now - start_time),
            trace_miss_ratio_stats.miss_ratio(),
        )
    )
    return trace_start_time, trace_duration


def run(trace_file_path, cache_config_path, downsample_size, warmup_seconds):
    ml_config_caches = parse_cache_config(cache_config_path, downsample_size)
    run_with_caches(trace_file_path, ml_config_caches, warmup_seconds)


def test_hash_table():
    cache = HashTable()
    data_size = 10000
    for i in range(data_size):
        cache.insert("k{}".format(i), i, "v{}".format(i))
    for i in range(data_size):
        assert cache.lookup("k{}".format(i), i) is not None
    for i in range(data_size):
        cache.delete("k{}".format(i), i)
    for i in range(data_size):
        assert cache.lookup("k{}".format(i), i) is None

    truth_map = {}
    n = 1000000
    records = 100
    for i in range(n):
        key_id = random.randint(0, records)
        key = "k{}".format(key_id)
        value = "v{}".format(key_id)
        action = random.randint(0, 2)
        # print "{}:{}:{}".format(action, key, value)
        assert len(truth_map) == cache.elements, "{} {} {}".format(
            len(truth_map), cache.elements, i
        )
        if action == 0:
            cache.insert(key, key_id, value)
            truth_map[key] = value
        elif action == 1:
            if key in truth_map:
                assert cache.lookup(key, key_id) is not None
                assert truth_map[key] == cache.lookup(key, key_id)
            else:
                assert cache.lookup(key, key_id) is None
        else:
            cache.delete(key, key_id)
            if key in truth_map:
                del truth_map[key]


def assert_metrics(cache, expected_value):
    assert cache.used_size == expected_value[0]
    assert cache.miss_ratio_stats.num_accesses == expected_value[1]
    assert cache.miss_ratio_stats.num_misses == expected_value[2]
    assert cache.cache.elements == len(expected_value[3])
    for expeceted_k in expected_value[3]:
        val = cache.cache.lookup("b{}".format(expeceted_k), expeceted_k)
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
    expected_values.append([1, 1, 1, [1]])
    # Access k1, hit.
    expected_values.append([1, 2, 1, [1]])
    # Access k2, miss.
    expected_values.append([2, 3, 2, [1, 2]])
    # Access k3, miss.
    expected_values.append([3, 4, 3, [1, 2, 3]])
    # Access k3, hit.
    expected_values.append([3, 5, 3, [1, 2, 3]])
    # Access k3, hit.
    expected_values.append([3, 6, 3, [1, 2, 3]])
    for access in sequence:
        cache.access(access)
        assert_metrics(cache, expected_values[index])
        index += 1
    cache.access(k4)
    assert_metrics(cache, expected_value)


def test_lru_cache():
    policies = []
    policies.append(LRUPolicy())
    # Access k4, miss. evict k1
    test_cache(policies, [3, 7, 4, [2, 3, 4]])


def test_mru_cache():
    policies = []
    policies.append(MRUPolicy())
    # Access k4, miss. evict k3
    test_cache(policies, [3, 7, 4, [1, 2, 4]])


def test_lfu_cache():
    policies = []
    policies.append(LFUPolicy())
    # Access k4, miss. evict k2
    test_cache(policies, [3, 7, 4, [1, 3, 4]])


def test_mix():
    policies = []
    policies.append(MRUPolicy())
    policies.append(LRUPolicy())
    policies.append(LFUPolicy())
    cache = ThompsonSamplingCache(100, False, policies)
    n = 1000000
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
            level=0,
            fd=0,
            caller=1,
            no_insert=0,
            get_id=1,
            key_id=1,
            kv_size=5,
            is_hit=1,
        )
        cache.access(k)
    assert cache.miss_ratio_stats.miss_ratio() > 0


def report_stats(
    cache, cache_type, cache_size, result_dir, trace_start_time, trace_end_time
):
    cache_label = "{}-{}".format(cache_type, cache_size)
    mrc_file = open("{}/data-ml-mrc-{}".format(result_dir, cache_label), "w+")
    mrc_file.write(
        "{},0,0,{},{},{}\n".format(
            cache_type,
            cache_size,
            cache.miss_ratio_stats.miss_ratio(),
            cache.miss_ratio_stats.num_accesses,
        )
    )
    mrc_file.close()
    cache.policy_stats.write_policy_timeline(
        cache_type, cache_size, result_dir, trace_start_time, trace_end_time
    )
    cache.policy_stats.write_policy_ratio_timeline(
        cache_type, cache_size, result_dir, trace_start_time, trace_end_time
    )
    cache.miss_ratio_stats.write_miss_timeline(
        cache_type, cache_size, result_dir, trace_start_time, trace_end_time
    )
    cache.miss_ratio_stats.write_miss_ratio_timeline(
        cache_type, cache_size, result_dir, trace_start_time, trace_end_time
    )

    cache.per_hour_policy_stats.write_policy_timeline(
        cache_type, cache_size, result_dir, trace_start_time, trace_end_time
    )
    cache.per_hour_policy_stats.write_policy_ratio_timeline(
        cache_type, cache_size, result_dir, trace_start_time, trace_end_time
    )
    cache.per_hour_miss_ratio_stats.write_miss_timeline(
        cache_type, cache_size, result_dir, trace_start_time, trace_end_time
    )
    cache.per_hour_miss_ratio_stats.write_miss_ratio_timeline(
        cache_type, cache_size, result_dir, trace_start_time, trace_end_time
    )


if __name__ == "__main__":
    # test_hash_table()
    # test_lru_cache()
    # test_mru_cache()
    # test_lfu_cache()
    # test_mix()
    print(sys.argv)
    cache_type = sys.argv[1]
    cache_size = parse_cache_size(sys.argv[2])
    downsample_size = int(sys.argv[3])
    warmup_seconds = int(sys.argv[4])
    trace_file_path = sys.argv[5]
    result_dir = sys.argv[6]
    ml_config_caches = {}
    cache = create_cache(cache_type, cache_size, downsample_size)
    ml_config_caches[cache_type] = [cache]
    trace_start_time, trace_duration = run_with_caches(
        trace_file_path, ml_config_caches, warmup_seconds
    )
    trace_end_time = trace_start_time + trace_duration
    report_stats(
        cache, cache_type, cache_size, result_dir, trace_start_time, trace_end_time
    )

    # file_path = sys.argv[1]
    # cache_config_path = sys.argv[2]
    # downsample_size = int(sys.argv[3])
    # warmup_seconds = int(sys.argv[4])
    # run(file_path, cache_config_path, downsample_size, warmup_seconds)
