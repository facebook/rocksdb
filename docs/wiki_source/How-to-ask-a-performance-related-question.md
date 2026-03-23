Performance is a strength of RocksDB. You are welcome to ask the questions if you see RocksDB is unexpectedly slow, or wonder whether there is any room of significant improvements. When you ask a performance-related question, providing more information will increase your chance of getting an answer sooner. In many cases, people can't tell much from a simple description of symptom. Answers to following questions are usually helpful:


1. Is your problem for Get(), iterator, or write?
2. either the answer of 1 is read or write, which metric(s) is unexpected to you? And what the value of the unexpected metric(s).
    1. throughput. For this problem, how many threads are you using?
    2. average latency.
    3. latency outlier.
3. What's your storage media?
    1. SSD
    2. single hard drive
    3. hard drive array
    4. ramfs/tmpfs
    5. HDFS or other remote storage?


More information about your configuration and DB state will be helpful:


* Set-up: do you use many RocksDB instances in single service, or one single one? How large is your expected DB size? How many column families do you use per DB instance?
* RocksDB release you are using.
* Build flags. If you are using *make*, the best and easiset way to provide the information is to share make_config.mk after running make. If you cannot provide full information, there are several information related to performance:
    * what's the platform? Linux, Windows, OS X, etc.
    * which allocator are you using? jemalloc, tcmalloc, glibc malloc, or others? If it is jemalloc, make_config.mk should show JEMALLOC=1. If it is tcmalloc, you can find it “-ltcmalloc” in PLATFORM_LDFLAGS.
    * is calculating CRC using SSE instruction supported and turned on? This information will be printed out in the header of the info log file, like this “Fast CRC32 supported: 1”
    * is fallocate supported and turned on?
* RocksDB Options you are using. You can provide your option file. Your option file is under your DB directory, naming as OPTIONS-xxxxx. Or you can also copy the header part of your information log file, which can be found under your DB directory, usually named as LOG, and LOG.old.xxxxx. The options files usually have RocksDB options printed out in the header part. If you cannot provide answer to the two, tell us some of your critical options will also be helpful (if you never set it, you can tell us default):
    * write buffer size
    * level0_file_num_compaction_trigger
    * target_file_size_base
    * compression
    * compaction style
    * If leveled compaction:
        * max_bytes_for_level_base
        * max_bytes_for_level_multiplier
        * level_compaction_dynamic_level_bytes
    * If universal compaction:
        * size_ratio
        * max_size_amplification_percent
    * block cache size
    * Bloom filter setting
    * Any uncommon options you set
* LSM-tree structure. You can generate a report of LSM-tree summary by calling DB::GetProperty() with property "rocksdb.stats". Another way to find the structure of LSM-tree is to use “ldb --manifest_dump” on your manifest file, which is MANIFEST-xxxxx file under the directory of your DB.
* Disk I/O stats while the problem happens. You can use the command you like. The command I usually use is “iostat -kxt 1”.
* Your workload characteristics:
    * key and value size
    * whether read and write are spiky
    * whether and how do you delete the data?
* If possible, share your information log files. By default, they are under your DB directory, named LOG and LOG.old.xxxxx.
* Hardware setting. We know there are cases where the concrete hardware setting can't be shared, but even if you can tell us the memory and number of cores are in which range, or order of magnitude, that will be helpful.
* If there is a way to easily reproduce the performance problem, reproduce instruction will be helpful.

