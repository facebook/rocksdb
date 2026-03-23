## Overview

RocksDB is file system and storage medium agnostic. File system operations are not atomic, and are susceptible to inconsistencies in the event of system failure. Even with journaling turned on, file systems do not guarantee consistency on unclean restart. POSIX file system does not support atomic batching of operations either. Hence, it is not possible to rely on metadata embedded in RocksDB datastore files to reconstruct the last consistent state of the RocksDB on restart. 

RocksDB has a built-in mechanism to overcome these limitations of POSIX file system by keeping a transactional log of RocksDB state changes using [Version Edit Records](https://github.com/facebook/rocksdb/wiki/MANIFEST#version-edit-record-types-and-layout) in the Manifest log files. MANIFEST is used to restore RocksDB to the latest known consistent state on a restart.    

## Terminology

* _**MANIFEST**_ refers to the system that keeps track of RocksDB state changes in a transactional log
* _**Manifest log**_ refers to an individual log file that contains RocksDB state snapshot/edits
* _**CURRENT**_ refers to the latest manifest log 

## How does it work ?

MANIFEST is a transactional log of the RocksDB state changes. MANIFEST consists of - manifest log files and pointer to the latest manifest file (CURRENT). Manifest logs are rolling log files named MANIFEST-(seq number). The sequence number is always increasing. CURRENT is a special file that points to the latest manifest log file.

On system (re)start, the latest manifest log contains the consistent state of RocksDB. Any subsequent change to RocksDB state is logged to the manifest log file. When a manifest log file exceeds a certain size, a new manifest log file is created with the snapshot of the RocksDB state. The latest manifest file pointer is updated and the file system is synced. Upon successful update to CURRENT file, the redundant manifest logs are purged. 

```
MANIFEST = { CURRENT, MANIFEST-<seq-no>* } 
CURRENT = File pointer to the latest manifest log
MANIFEST-<seq no> = Contains snapshot of RocksDB state and subsequent modifications
```

## Version Edit

A certain state of RocksDB at any given time is referred to as a **Version** (aka snapshot). Any modification to the Version is considered a **Version Edit**. A Version (or RocksDB state snapshot) is constructed by joining a sequence of version-edits. Essentially, a manifest log file is a sequence of version-edits.

```
version-edit      = Any RocksDB state change
version           = { version-edit* }
manifest-log-file = { version, version-edit* }
                  = { version-edit* }
```

## Version Edit Layout

Manifest log is a sequence of Version Edit records. The Version Edit record type is identified by the **edit identification number**. 

We use the following datatypes for encoding/decoding.

### Data Types

Simple data types
```
VarX   - Variable character encoding of intX
FixedX - Fixed character encoding of intX
```

Complex data types
```
String - Length prefixed string data
+-----------+--------------------+
| size (n)  | content of string  |
+-----------+--------------------+
|<- Var32 ->|<-- n            -->|
```

### Version Edit Record Format

Version Edit records have the following format. The decoder identifies the record type using the **record identification number**.
```
+-------------+------ ......... ----------+
| Record ID   | Variable size record data |
+-------------+------ .......... ---------+
<-- Var32 --->|<-- varies by type       -->
```

### Version Edit Record Types and Layout

There are a variety of edit records corresponding to different state changes of RocksDB.

Comparator edit record:
```
Captures the comparator name

+-------------+----------------+
| kComparator | data           |
+-------------+----------------+
<-- Var32 --->|<-- String   -->|
```

Log number edit record:
```
Latest WAL log file number

+-------------+----------------+
| kLogNumber  | log number     |
+-------------+----------------+
<-- Var32 --->|<-- Var64    -->|
```

Previous File Number edit record:
```
Previous manifest file number

+------------------+----------------+
| kPrevFileNumber  | log number     |
+------------------+----------------+
<-- Var32      --->|<-- Var64    -->|
```

Next File Number edit record:
```
Next manifest file number

+------------------+----------------+
| kNextFileNumber  | log number     |
+------------------+----------------+
<-- Var32      --->|<-- Var64    -->|
```

Last Sequence Number edit record:
```
Last sequence number of RocksDB

+------------------+----------------+
| kLastSequence    | log number     |
+------------------+----------------+
<-- Var32      --->|<-- Var64    -->|
```

Max Column Family edit record:
```
Adjust the maximum number of family columns allowed.

+---------------------+----------------+
| kMaxColumnFamily    | log number     |
+---------------------+----------------+
<-- Var32         --->|<-- Var32    -->|
```

Deleted File edit record:
```
Mark a file as deleted from database.

+-----------------+-------------+--------------+
| kDeletedFile    | level       | file number  |
+-----------------+-------------+--------------+
<-- Var32     --->|<-- Var32 -->|<-- Var64  -->|
```

New File edit record:

Mark a file as newly added to the database and provide RocksDB meta information.

* File edit record with compaction information
```
+--------------+-------------+--------------+------------+----------------+--------------+----------------+----------------+
| kNewFile4    | level       | file number  | file size  | smallest_key   | largest_key  | smallest_seqno | largest_seq_no |
+--------------+-------------+--------------+------------+----------------+--------------+----------------+----------------+
|<-- var32  -->|<-- var32 -->|<-- var64  -->|<-  var64 ->|<-- String   -->|<-- String -->|<-- var64    -->|<-- var64    -->|

+--------------+------------------+---------+------+----------------+--------------------+---------+------------+
|  CustomTag1  | Field 1 size n1  | field1  | ...  |  CustomTag(m)  | Field m size n(m)  | field(m)| kTerminate |
+--------------+------------------+---------+------+----------------+--------------------+---------+------------+
<-- var32   -->|<-- var32      -->|<- n1  ->|      |<-- var32   - ->|<--    var32     -->|<- n(m)->|<- var32 -->|

```
Several Optional customized fields can be written there.
The field has a special bit indicating that whether it can be safely ignored. This is for compatibility reason. A RocksDB older release may see a field it can't identify. Checking the bit, RocksDB knows whether it should stop opening the DB, or ignore the field.

Several optional customized fields are supported:
`kNeedCompaction`: Whether the file should be compacted to the next level.
`kMinLogNumberToKeepHack`: WAL file number that is still in need for recovery after this entry.
`kPathId`: The Path ID in which the file lives. This can't be ignored by an old release. 

* File edit record backward compatible
```
+--------------+-------------+--------------+------------+----------------+--------------+----------------+----------------+
| kNewFile2    | level       | file number  | file size  | smallest_key   | largest_key  | smallest_seqno | largest_seq_no |
+--------------+-------------+--------------+------------+----------------+--------------+----------------+----------------+
<-- var32   -->|<-- var32 -->|<-- var64  -->|<-  var64 ->|<-- String   -->|<-- String -->|<-- var64    -->|<-- var64    -->|
```

* File edit record with path information
```
+--------------+-------------+--------------+-------------+-------------+----------------+--------------+
| kNewFile3    | level       | file number  | Path ID     | file size   | smallest_key   | largest_key  |
+--------------+-------------+--------------+-------------+-------------+----------------+--------------+
|<-- var32  -->|<-- var32 -->|<-- var64  -->|<-- var32 -->|<-- var64 -->|<-- String   -->|<-- String -->|
+----------------+----------------+
| smallest_seqno | largest_seq_no |
+----------------+----------------+
<-- var64     -->|<-- var64    -->|
```   

Column family status edit record:
```
Note the status of column family feature (enabled/disabled)

+------------------+----------------+
| kColumnFamily    | 0/1            |
+------------------+----------------+
<-- Var32      --->|<-- Var32    -->|
```

Column family add edit record:
```
Add a column family

+---------------------+----------------+
| kColumnFamilyAdd    | cf name        |
+---------------------+----------------+
<-- Var32         --->|<-- String   -->|
```

Column family drop edit record:
```
Drop all column family

+---------------------+
| kColumnFamilyDrop   |
+---------------------+
<-- Var32         --->|
```

Record as part of an atomic group (since RocksDB 5.16):

There are cases in which 'all-or-nothing', multi-column-family version change is desirable. For example, [atomic flush](https://github.com/facebook/rocksdb/wiki/Atomic-flush) ensures either all or none of the column families get flushed successfully, [multiple column families external SST ingestion](https://github.com/facebook/rocksdb/wiki/Creating-and-Ingesting-SST-files) guarantees that either all or none of the column families ingest SSTs successfully. Since writing multiple version edits is not atomic, we need to take extra measure to achieve atomicity (not necessarily ***instantaneity*** from the user's perspective). Therefore we introduce a new record field `kInAtomicGroup` to indicate that this record is part of a group of Version Edits that follow the 'all-or-none' property. The format is as follows.
```
+-----------------+--------------------------------------------+
| kInAtomicGroup  | #remaining Version Edits in the same group |
+-----------------+--------------------------------------------+
|<--- Var32 ----->|<----------------- Var32 ------------------>|
```
During recovery, RocksDB buffers Version Edits of an atomic group without applying them until the last Version Edit of the atomic group is decoded successfully from the MANIFEST file. Then RocksDB applies all the Version Edits in this atomic group. RocksDB never applies partial atomic groups.

### Version Edit ignorable record types
We reserved a special bit in record type. If the bit is set, it can be safely ignored. And the safely ignorable record has a standard general format:
```
+---------+----------------+----------------+
|   kTag  | field length n |  fields ...    |
+--------------------------+----------------+
<- Var32->|<--  var32   -->|<---   n       >|
```
This is introduced in 6.0 and no customized ignorable record created yet.

The following types of Version Edits fall into the ignorable category.

DB ID edit record: introduced since RocksDB 6.5. If `options.write_dbid_to_manifest` is true, then RocksDB writes the DB ID edit record to the MANIFEST file, besides storing in the IDENTITY file.
```
+-----------+------------+
|   kDbId   |    db id   |
+-----------+------------+
|<- Var32 ->|<- String ->|
```