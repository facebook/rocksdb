## Overview

Write ahead log (WAL) serializes memtable operations to persistent medium as log files. In the event of a failure, WAL files can be used to recover the database to its consistent state, by reconstructing the memtable from the logs. When a memtable is flushed out to persistent medium safely, the corresponding WAL log(s) become obsolete and are archived. Eventually the archived logs are purged from disk after a certain period of time.

## WAL Manager

WAL files are generated with increasing sequence number in the WAL directory. In order to reconstruct the state of the database, these files are read in the order of sequence number. WAL manager provides the abstraction for reading the WAL files as a single unit. Internally, it opens and reads the files using Reader or Writer abstraction.

## Reader/Writer

Writer provides an abstraction for appending log records to a log file. The medium specific internal details are handled by WriteableFile interface. Similarly, Reader provides an abstraction for sequentially reading log records from the log file. The internal medium specific details are handled by SequentialFile interface.

## Log File Format

Log file consists of a sequence of variable length records. Records are grouped by `kBlockSize`(32k). If a certain record cannot fit into the leftover space, then the leftover space is padded with empty (null) data. The writer writes and the reader reads in chunks of `kBlockSize`.

```
       +-----+-------------+--+----+----------+------+-- ... ----+
 File  | r0  |        r1   |P | r2 |    r3    |  r4  |           |
       +-----+-------------+--+----+----------+------+-- ... ----+
       <--- kBlockSize ------>|<-- kBlockSize ------>|

  rn = variable size records
  P = Padding
```

### Record Format

The record layout format is as shown below. There are two kinds of record format, Legacy and Recyclable:

#### The Legacy Record Format
```
+---------+-----------+-----------+--- ... ---+
|CRC (4B) | Size (2B) | Type (1B) | Payload   |
+---------+-----------+-----------+--- ... ---+

CRC = 32bit hash computed over the payload using CRC
Size = Length of the payload data
Type = Type of record
       (kZeroType, kFullType, kFirstType, kLastType, kMiddleType )
       The type is used to group a bunch of records together to represent
       blocks that are larger than kBlockSize
Payload = Byte stream as long as specified by the payload size
```

#### The Recyclable Record Format

```
+---------+-----------+-----------+----------------+--- ... ---+
|CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
+---------+-----------+-----------+----------------+--- ... ---+
Same as above, with the addition of
Log number = 32bit log file number, so that we can distinguish between
records written by the most recent log writer vs a previous one.
```

#### Record Format Details For Legacy Format

The log file contents are a sequence of 32KB blocks.  The only exception is that the tail of the file may contain a partial block.


Each block consists of a sequence of records:

```
block := record* trailer?
record :=
  checksum: uint32	// crc32c of type and data[]
  length: uint16
  type: uint8		// One of FULL, FIRST, MIDDLE, LAST 
  data: uint8[length]
```

A record never starts within the last six bytes of a block (since it won't fit).  Any leftover bytes here form the trailer, which must consist entirely of zero bytes and must be skipped by readers.

> if exactly seven bytes are left in the current block, and a new non-zero length record is added, the writer must emit a FIRST record (which contains zero bytes of user data) to fill up the trailing seven bytes of the block and then emit all of the user data in subsequent blocks.




More types may be added in the future.  Some Readers may skip record types they do not understand, others may report that some data was skipped.

```
FULL == 1
FIRST == 2
MIDDLE == 3
LAST == 4
```

The `FULL` record contains the contents of an entire user record.

`FIRST`, `MIDDLE`, `LAST` are types used for user records that have been
split into multiple fragments (typically because of block boundaries).
`FIRST` is the type of the first fragment of a user record, `LAST` is the
type of the last fragment of a user record, and `MID` is the type of all
interior fragments of a user record.

Example: consider a sequence of user records:

```
   A: length 1000
   B: length 97270
   C: length 8000
```

`A` will be stored as a `FULL` record in the first block.

`B` will be split into three fragments: first fragment occupies the rest of the first block, second fragment occupies the entirety of the second block, and the third fragment occupies a prefix of the third block.  This will leave six bytes free in the third block, which will be left empty as the trailer.

`C` will be stored as a `FULL` record in the fourth block.

### Benefits

Some benefits over the `recordio` format:

1. We do not need any heuristics for resyncing - just go to next block boundary and scan.  If there is a corruption, skip to the next block.  As a side-benefit, we do not get confused when part of the contents of one log file are embedded as a record inside another log file.
1. Splitting at approximate boundaries (e.g., for `mapreduce`) is simple: find the next block boundary and skip records until we hit a `FULL` or `FIRST` record.
1. We do not need extra buffering for large records.

### Downsides

Some downsides compared to `recordio` format:

1. No packing of tiny records.  This could be fixed by adding a new record type, so it is a shortcoming of the current implementation, not necessarily the format.
1. No compression.  Again, this could be fixed by adding new record types.