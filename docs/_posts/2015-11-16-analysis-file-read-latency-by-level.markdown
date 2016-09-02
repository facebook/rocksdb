---
title: Analysis File Read Latency by Level
layout: post
author: sdong
category: blog
---

In many use cases of RocksDB, people rely on OS page cache for caching compressed data. With this approach, verifying effective of the OS page caching is challenging, because file system is a black box to users.

As an example, a user can tune the DB as following: use level-based compaction, with L1 - L4 sizes to be 1GB, 10GB, 100GB and 1TB. And they reserve about 20GB memory as OS page cache, expecting level 0, 1 and 2 are mostly cached in memory, leaving only reads from level 3 and 4 requiring disk I/Os. However, in practice, it's not easy to verify whether OS page cache does exactly what we expect. For example, if we end up with doing 4 instead of 2 I/Os per query, it's not easy for users to figure out whether the it's because of efficiency of OS page cache or reading multiple blocks for a level. Analysis like it is especially important if users run RocksDB on hard drive disks, for the gap of latency between hard drives and memory is much higher than flash-based SSDs.

In order to make tuning easier, we added new instrumentation to help users analysis latency distribution of file reads in different levels. If users turn DB statistics on, we always keep track of distribution of file read latency for each level. Users can retrieve the information by querying DB property “rocksdb.stats” ( [https://github.com/facebook/rocksdb/blob/v3.13.1/include/rocksdb/db.h#L315-L316](https://github.com/facebook/rocksdb/blob/v3.13.1/include/rocksdb/db.h#L315-L316) ). It will also printed out as a part of compaction summary in info logs periodically.

The output looks like this:


```bash
** Level 0 read latency histogram (micros):
Count: 696 Average: 489.8118 StdDev: 222.40
Min: 3.0000 Median: 452.3077 Max: 1896.0000
Percentiles: P50: 452.31 P75: 641.30 P99: 1068.00 P99.9: 1860.80 P99.99: 1896.00
------------------------------------------------------
[ 2, 3 ) 1 0.144% 0.144%
[ 18, 20 ) 1 0.144% 0.287%
[ 45, 50 ) 5 0.718% 1.006%
[ 50, 60 ) 26 3.736% 4.741% #
[ 60, 70 ) 6 0.862% 5.603%
[ 90, 100 ) 1 0.144% 5.747%
[ 120, 140 ) 2 0.287% 6.034%
[ 140, 160 ) 1 0.144% 6.178%
[ 160, 180 ) 1 0.144% 6.322%
[ 200, 250 ) 9 1.293% 7.615%
[ 250, 300 ) 45 6.466% 14.080% #
[ 300, 350 ) 88 12.644% 26.724% ###
[ 350, 400 ) 88 12.644% 39.368% ###
[ 400, 450 ) 71 10.201% 49.569% ##
[ 450, 500 ) 65 9.339% 58.908% ##
[ 500, 600 ) 74 10.632% 69.540% ##
[ 600, 700 ) 92 13.218% 82.759% ###
[ 700, 800 ) 64 9.195% 91.954% ##
[ 800, 900 ) 35 5.029% 96.983% #
[ 900, 1000 ) 12 1.724% 98.707%
[ 1000, 1200 ) 6 0.862% 99.569%
[ 1200, 1400 ) 2 0.287% 99.856%
[ 1800, 2000 ) 1 0.144% 100.000%

** Level 1 read latency histogram (micros):
(......not pasted.....)

** Level 2 read latency histogram (micros):
(......not pasted.....)

** Level 3 read latency histogram (micros):
(......not pasted.....)

** Level 4 read latency histogram (micros):
(......not pasted.....)

** Level 5 read latency histogram (micros):
Count: 25583746 Average: 421.1326 StdDev: 385.11
Min: 1.0000 Median: 376.0011 Max: 202444.0000
Percentiles: P50: 376.00 P75: 438.00 P99: 1421.68 P99.9: 4164.43 P99.99: 9056.52
------------------------------------------------------
[ 0, 1 ) 2351 0.009% 0.009%
[ 1, 2 ) 6077 0.024% 0.033%
[ 2, 3 ) 8471 0.033% 0.066%
[ 3, 4 ) 788 0.003% 0.069%
[ 4, 5 ) 393 0.002% 0.071%
[ 5, 6 ) 786 0.003% 0.074%
[ 6, 7 ) 1709 0.007% 0.080%
[ 7, 8 ) 1769 0.007% 0.087%
[ 8, 9 ) 1573 0.006% 0.093%
[ 9, 10 ) 1495 0.006% 0.099%
[ 10, 12 ) 3043 0.012% 0.111%
[ 12, 14 ) 2259 0.009% 0.120%
[ 14, 16 ) 1233 0.005% 0.125%
[ 16, 18 ) 762 0.003% 0.128%
[ 18, 20 ) 451 0.002% 0.130%
[ 20, 25 ) 794 0.003% 0.133%
[ 25, 30 ) 1279 0.005% 0.138%
[ 30, 35 ) 1172 0.005% 0.142%
[ 35, 40 ) 1363 0.005% 0.148%
[ 40, 45 ) 409 0.002% 0.149%
[ 45, 50 ) 105 0.000% 0.150%
[ 50, 60 ) 80 0.000% 0.150%
[ 60, 70 ) 280 0.001% 0.151%
[ 70, 80 ) 1583 0.006% 0.157%
[ 80, 90 ) 4245 0.017% 0.174%
[ 90, 100 ) 6572 0.026% 0.200%
[ 100, 120 ) 9724 0.038% 0.238%
[ 120, 140 ) 3713 0.015% 0.252%
[ 140, 160 ) 2383 0.009% 0.261%
[ 160, 180 ) 18344 0.072% 0.333%
[ 180, 200 ) 51873 0.203% 0.536%
[ 200, 250 ) 631722 2.469% 3.005%
[ 250, 300 ) 2721970 10.639% 13.644% ##
[ 300, 350 ) 5909249 23.098% 36.742% #####
[ 350, 400 ) 6522507 25.495% 62.237% #####
[ 400, 450 ) 4296332 16.793% 79.030% ###
[ 450, 500 ) 2130323 8.327% 87.357% ##
[ 500, 600 ) 1553208 6.071% 93.428% #
[ 600, 700 ) 642129 2.510% 95.938% #
[ 700, 800 ) 372428 1.456% 97.394%
[ 800, 900 ) 187561 0.733% 98.127%
[ 900, 1000 ) 85858 0.336% 98.462%
[ 1000, 1200 ) 82730 0.323% 98.786%
[ 1200, 1400 ) 50691 0.198% 98.984%
[ 1400, 1600 ) 38026 0.149% 99.133%
[ 1600, 1800 ) 32991 0.129% 99.261%
[ 1800, 2000 ) 30200 0.118% 99.380%
[ 2000, 2500 ) 62195 0.243% 99.623%
[ 2500, 3000 ) 36684 0.143% 99.766%
[ 3000, 3500 ) 21317 0.083% 99.849%
[ 3500, 4000 ) 10216 0.040% 99.889%
[ 4000, 4500 ) 8351 0.033% 99.922%
[ 4500, 5000 ) 4152 0.016% 99.938%
[ 5000, 6000 ) 6328 0.025% 99.963%
[ 6000, 7000 ) 3253 0.013% 99.976%
[ 7000, 8000 ) 2082 0.008% 99.984%
[ 8000, 9000 ) 1546 0.006% 99.990%
[ 9000, 10000 ) 1055 0.004% 99.994%
[ 10000, 12000 ) 1566 0.006% 100.000%
[ 12000, 14000 ) 761 0.003% 100.003%
[ 14000, 16000 ) 462 0.002% 100.005%
[ 16000, 18000 ) 226 0.001% 100.006%
[ 18000, 20000 ) 126 0.000% 100.006%
[ 20000, 25000 ) 107 0.000% 100.007%
[ 25000, 30000 ) 43 0.000% 100.007%
[ 30000, 35000 ) 15 0.000% 100.007%
[ 35000, 40000 ) 14 0.000% 100.007%
[ 40000, 45000 ) 16 0.000% 100.007%
[ 45000, 50000 ) 1 0.000% 100.007%
[ 50000, 60000 ) 22 0.000% 100.007%
[ 60000, 70000 ) 10 0.000% 100.007%
[ 70000, 80000 ) 5 0.000% 100.007%
[ 80000, 90000 ) 14 0.000% 100.007%
[ 90000, 100000 ) 11 0.000% 100.007%
[ 100000, 120000 ) 33 0.000% 100.007%
[ 120000, 140000 ) 6 0.000% 100.007%
[ 140000, 160000 ) 3 0.000% 100.007%
[ 160000, 180000 ) 7 0.000% 100.007%
[ 200000, 250000 ) 2 0.000% 100.007%
```


In this example, you can see we only issued 696 reads from level 0 while issued 25 million reads from level 5. The latency distribution is also clearly shown among those reads. This will be helpful for users to analysis OS page cache efficiency.

Currently the read latency per level includes reads from data blocks, index blocks, as well as bloom filter blocks. We are also working on a feature to break down those three type of blocks.
