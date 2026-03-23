RocksDB stores user data in two forms: recent data is temporarily stored in memory using memtable structures, whereas the rest of the data is stored directly on persistent storage (HD, SSD) as SST files. The process of transferring data from in-memory (memtables) to persistent storage (SST files at level 0) is called Flush.

# What is in-memory garbage ?

The temporary accumulation of data in memtables is sufficiently important for certain workload to generate what we refer to as garbage data. Garbage data is composed of entries that are no longer valid by the time the memtable they belong to is being flush to level 0. Garbage data is for example generated when K-V pairs are being overwritten or deleted within a relatively short amount of time, or when the time a KV entry has been kept in a memtable is greater than the TTL (Time to Live) of this very entry.
Examples:
* Entry being overwritten:
```
db->Put(K1,V1); // Writes {key: K1, value: V1}
[...]
db->Put(K1,V2); // Overwrite K1 with a new value: {key: K1, value: V2}.
                // The second operation generates garbage, because {K1, V1}
                // is still present on the memtable, but 

```
* Entry being inserted, then deleted:
```
db->Put(K1,V1); // Writes {key: K1, value: V1}
[...]
db->Delete(K1); // Deletes the first entry.
```

# How much in-memory garbage is typically generated in my workload(s), and should I use MemPurge? 

Each flush operation takes in an input `payload` from the memtables to flush, filters out the `garbage` bytes out of the input payload and eventually writes a `useful payload` to permanent storage. We define the garbage ratio as the ratio "garbage bytes"/"input payload bytes". You can estimate the garbage ratio of your workload by using two statistics counters, `memtable.payload.bytes.at.flush` and `memtable.garbage.bytes.at.flush`. The ratio "`memtable.garbage.bytes.at.flush` / `memtable.payload.bytes.at.flush` " is your in-memory garbage ratio. Typically, activating the MemPurge feature (in-memory garbage collection) becomes worthwhile for garbage ratios >20%.

# MemPurge Workflow

The MemPurge process (memtable garbage collection) is a background process that reroutes a standard flush operation happening when an immutable memtable is full. The MemPurge takes in a set of immutable memtables, filters out the garbage entries, and writes the 
`useful payload` bytes back into a new immutable memtable that will sit in memory waiting for the current mutable memtable to be made immutable and trigger a new flush. 
If the useful payload cannot be contained on a single immutable memtable, the mempurge process is aborted and a regular flush process takes place instead.

# How to activate the MemPurge feature

By default, the `mempurge` feature is deactivated. 
The MemPurge feature includes a decision module that assesses at each flush operation whether to mempurge or flush the set of immutable memtables. This evaluation is based on an *estimate* of the total amount of `useful payload` bytes contained in these memtables. This estimate of total useful payload bytes is based on sampled entries from these memtables. Eventually, we translate the total useful paylaod bytes into a percentage of memtable size (eg: if we have 30 MB of useful paylaod and a memtable is 40MB, then the amount of useful payload represents 75% of a memtable size). We compare this percentage with a `threshold`: if this percentage is `<threshold`, we mempurge the set of immutable memtables, else we flush them.
To activate the MemPurge feature, the API comes down to a single option flag, which is the aforementioned `threshold` : 
```
options.experimental_mempurge_threshold = 1.0; // Default value is 0.0 (no mempurge)
```

When activating the MemPurge feature, we recommend to use a threshold value of `1.0`. Therefore, when the estimated amount of useful payload is inferior or equal to `experimental_mempurge_threshold`% (`1.0`=>100%) of a memtable size, we mempurge, else we flush.
In practice, we don't see solid reasons why we would use a value different than `1.0` for `experimental_mempurge_threshold` when one wants to activate `MemPurge`.
Note that, as explained in the "MemPurge Workflow" section: regardless of the `threshold`, if the mempurge output (the actual useful payload, not to be confused with the "estimated useful payload from the sampling") cannot be contained on a single memtable, the mempurge process is aborted and rerouted to a regular flush. Therefore, an `experimental_mempurge_threshold` value of more than `1.0` is only really used to counter any consistent biases in the sampling. In other words, inputing a value `>1.0` for `experimental_mempurge_threshold` is only really useful when the useful payload is consistently overestimated and therefore the Decider leads to more flushes than it should.

# Important

Note that the current implementation of the MemPurge feature is not compatible with atomic flushes. When `options.atomic_flush = true`, the mempurge option is ignored and the priority is given to atomic flushes.
