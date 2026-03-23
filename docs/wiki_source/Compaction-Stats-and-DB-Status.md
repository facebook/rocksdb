Where to find it?
You can find compaction stats in following ways:
1. RocksDB dump statistics to LOG file every `stats_dump_period_sec` seconds. This is 600 by default, which means that stats will be dumped every 10 minutes in LOG files.
2. You can get the same data in the application by calling `db->GetProperty("rocksdb.stats");`

In both ways, the outputs look like this:

    ** Compaction Stats **
    Level Files  Size(MB) Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) Comp(cnt) Avg(sec) Stall(sec) Stall(cnt) Avg(ms)     KeyIn   KeyDrop
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    L0      2/0        15   0.5     0.0     0.0      0.0      32.8     32.8       0.0   0.0      0.0     23.0    1457      4346    0.335       0.00          0    0.00             0        0
    L1     22/0       125   1.0   163.7    32.8    130.9     165.5     34.6       0.0   5.1     25.6     25.9    6549      1086    6.031       0.00          0    0.00    1287667342        0
    L2    227/0      1276   1.0   262.7    34.4    228.4     262.7     34.3       0.1   7.6     26.0     26.0   10344      4137    2.500       0.00          0    0.00    1023585700        0
    L3   1634/0     12794   1.0   259.7    31.7    228.1     254.1     26.1       1.5   8.0     20.8     20.4   12787      3758    3.403       0.00          0    0.00    1128138363        0
    L4   1819/0     15132   0.1     3.9     2.0      2.0       3.6      1.6      13.1   1.8     20.1     18.4     201       206    0.974       0.00          0    0.00      91486994        0
    Sum  3704/0     29342   0.0   690.1   100.8    589.3     718.7    129.4      14.8  21.9     22.5     23.5   31338     13533    2.316       0.00          0    0.00    3530878399        0
    Int     0/0         0   0.0     2.1     0.3      1.8       2.2      0.4       0.0  24.3     24.0     24.9      91        42    2.164       0.00          0    0.00      11718977        0
    Flush(GB): accumulative 32.786, interval 0.091
    Stalls(secs): 0.000 level0_slowdown, 0.000 level0_numfiles, 0.000 memtable_compaction, 0.000 leveln_slowdown_soft, 0.000 leveln_slowdown_hard
    Stalls(count): 0 level0_slowdown, 0 level0_numfiles, 0 memtable_compaction, 0 leveln_slowdown_soft, 0 leveln_slowdown_hard

    ** DB Stats **
    Uptime(secs): 128748.3 total, 300.1 interval
    Cumulative writes: 1288457363 writes, 14173030838 keys, 357293118 batches, 3.6 writes per batch, 3055.92 GB user ingest, stall micros: 7067721262
    Cumulative WAL: 1251702527 writes, 357293117 syncs, 3.50 writes per sync, 3055.92 GB written
    Interval writes: 3621943 writes, 39841373 keys, 1013611 batches, 3.6 writes per batch, 8797.4 MB user ingest, stall micros: 112418835
    Interval WAL: 3511027 writes, 1013611 syncs, 3.46 writes per sync, 8.59 MB written

### Compaction stats
Compaction stats for the compactions executed between levels N and N+1 are reported at level N+1 (compaction output). Here is the quick reference:
* Level - for leveled compaction the level of the LSM. For universal compaction all files are in L0. **Sum** has the values aggregated over all levels. **Int** is like **Sum** but limited to the data from the last reporting interval.
* Files - this has two values as (a/b). The first is the number of files in the level. The second is the number of files currently doing compaction for that level.
* Score: for levels other than L0 the score is (current level size) / (max level size). Values of 0 or 1 are okay, but any value greater than 1 means that level needs to be compacted. For L0 the score is computed from the current number of files and number of files that triggers a compaction.
* Read(GB): Total bytes read during compaction between levels N and N+1. This includes bytes read from level N and from level N+1
* Rn(GB): Bytes read from level N during compaction between levels N and N+1
* Rnp1(GB): Bytes read from level N+1 during compaction between levels N and N+1
* Write(GB): Total bytes written during compaction between levels N and N+1
* Wnew(GB):  New bytes written to level N+1, calculated as (total bytes written to N+1) - (bytes read from N+1 during compaction with level N)
* Moved(GB): Bytes moved to level N+1 during compaction. In this case there is no IO other than updating the manifest to indicate that a file which used to be in level X is now in level Y
* W-Amp: (total bytes written to level N+1) / (total bytes read from level N). This is the write amplification from compaction between levels N and N+1
* Rd(MB/s): The rate at which data is read during compaction between levels N and N+1. This is (Read(GB) * 1024) / duration where duration is the time for which compactions are in progress from level N to N+1.
* Wr(MB/s): The rate at which data is written during compaction. See Rd(MB/s).
* Rn(cnt): Total files read from level N during compaction between levels N and N+1
* Rnp1(cnt): Total files read from level N+1 during compaction between levels N and N+1
* Wnp1(cnt): Total files written to level N+1 during compaction between levels N and N+1
* Wnew(cnt): (Wnp1(cnt) - Rnp1(cnt)) -- Increase in file count as result of compaction between levels N and N+1
* Comp(sec): Total time spent doing compactions between levels N and N+1
* Comp(cnt): Total number of compactions between levels N and N+1
* Avg(sec): Average time per compaction between levels N and N+1
* Stall(sec): Total time writes were stalled because level N+1 was uncompacted (compaction score was high)
* Stall(cnt): Total number of writes stalled because level N+1 was uncompacted
* Avg(ms): Average time in milliseconds a write was stalled because level N+1 was uncompacted
* KeyIn: number of records compared during compaction
* KeyDrop: number of records dropped (not written out) during compaction

### General stats
After the per-level compaction stats, we also output some general stats. General stats are reported for both **cumulative** and **interval**. Cumulative stats report total values from RocksDB instance start. Interval stats report values since the last stats output.
* Uptime(secs): total -- number of seconds this instance has been running, interval -- number of seconds since the last stats dump.
* Cumulative/Interval writes: total -- number of Put calls; keys -- number of entries in the WriteBatches from the Put calls; batches -- number of group commits where each group commit makes persistent one or more Put calls (with concurrency there can be more than 1 Put call made persistent at one point in time); per batch -- average number of bytes in a single batch; ingest -- total bytes written into DB (not counting compactions); stall micros - number of microseconds writes have been stalled when compaction gets behind
* Cumulative/Interval WAL: writes -- number of writes logged in the WAL; syncs - number of times fsync or fdatasync has been used; writes per sync - ratio of writes to syncs; GB written - number of GB written to the WAL 
* Stalls: total count and seconds of each stall type since beginning of time: level0_slowdown -- Stall because of `level0_slowdown_writes_trigger`. level0_numfiles -- Stall because of `level0_stop_writes_trigger`. `memtable_compaction` -- Stall because all memtables were full, flush process couldn't keep up. `leveln_slowdown` -- Stall because of `soft_rate_limit` and `hard_rate_limit`