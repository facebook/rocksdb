## Overview

Repairer does best effort recovery to recover as much data as possible after a disaster without compromising consistency. It does not guarantee bringing the database to a time consistent state.
Note: Currently there is a limitation that un-flushed column families will be lost after repair. This would happen even if the DB is in healthy state.

## Usage

Note the CLI command uses default options for repairing your DB and only adds the column families found in the SST files. If you need to specify any options, e.g., custom comparator, have column family-specific options, or want to specify the exact set of column families, you should choose the programmatic way.

### Programmatic

For programmatic usage, call one of the `RepairDB` functions declared in `include/rocksdb/db.h`.

### CLI

For CLI usage, first build `ldb`, our admin CLI tool:

```
$ make clean && make ldb
```

Now use the `ldb`'s `repair` subcommand, specifying your DB. Note it prints info logs to stderr so you may wish to redirect. Here I run it on a DB in `./tmp` where I've deleted the MANIFEST file:

```
$ ./ldb repair --db=./tmp 2>./repair-log.txt
$ tail -2 ./repair-log.txt 
[WARN] [db/repair.cc:208] **** Repaired rocksdb ./tmp; recovered 1 files; 926bytes. Some data may have been lost. ****
```

Looks successful. MANIFEST file is back and DB is readable:

```
$ ls tmp/
000006.sst  CURRENT  IDENTITY  LOCK  LOG  LOG.old.1504116879407136  lost  MANIFEST-000001  MANIFEST-000003  OPTIONS-000005
$ ldb get a --db=./tmp
b
```

Notice the `lost/` directory. It holds files containing data that was potentially lost during recovery.

## Repair Process

Repair process is broken into 4 phase:
* Find files
* Convert logs to tables
* Extract metadata
* Write Descriptor

#### Find files

The repairer goes through all the files in the directory, and classifies them based on their file name. Any file that cannot be identified by name will be ignored.

#### Convert logs to table

Every log file that is active is replayed. All sections of the file where the checksum does not match is skipped over. We intentionally give preference to data consistency.

#### Extract metadata

We scan every table to compute

* smallest/largest for the table
* largest sequence number in the table

If we are unable to scan the file, then we ignore the table.

#### Write Descriptor

We generate descriptor contents:

* log number is set to zero
* next-file-number is set to 1 + largest file number we found
* last-sequence-number is set to largest sequence# found across all tables 
* compaction pointers are cleared
* every table file is added at level 0

#### Possible optimizations

1. Compute total size and use to pick appropriate max-level M
2. Sort tables by largest sequence# in the table
3. For each table: if it overlaps earlier table, place in level-0, else place in level-M.
4. We can provide options for time consistent recovery and unsafe recovery (ignore checksum failure when applicable)
5. Store per-table metadata (smallest, largest, largest-seq#, ...) in the table's meta section to speed up ScanTable.

## Limitations

If the column family is created recently and not persisted in sst files by a flush, then it will be dropped during the repair process. With this limitation repair would might even damage a healthy db if its column families are not flushed yet.