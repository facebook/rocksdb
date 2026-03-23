# Ldb Tool
## Introduction
The ldb command line tool offers multiple data access and database admin commands. Some examples are listed below. For more information, please consult the help message displayed when running ldb without any arguments and the unit tests in tools/ldb_test.py.

## Example
Example data access sequence:

```bash
    $./ldb --db=/tmp/test_db --create_if_missing put a1 b1
    OK 


    $ ./ldb --db=/tmp/test_db get a1
    b1
 
    $ ./ldb --db=/tmp/test_db get a2
    Failed: NotFound:

    $ ./ldb --db=/tmp/test_db scan
    a1 ==> b1
 
    $ ./ldb --db=/tmp/test_db scan --hex
    0x6131 ==> 0x6231
 
    $ ./ldb --db=/tmp/test_db put --key_hex 0x6132 b2
    OK
 
    $ ./ldb --db=/tmp/test_db scan
    a1 ==> b1
    a2 ==> b2
 
    $ ./ldb --db=/tmp/test_db get --value_hex a2
    0x6232
 
    $ ./ldb --db=/tmp/test_db get --hex 0x6131
    0x6231
 
    $ ./ldb --db=/tmp/test_db batchput a3 b3 a4 b4
    OK
 
    $ ./ldb --db=/tmp/test_db scan
    a1 ==> b1
    a2 ==> b2
    a3 ==> b3
    a4 ==> b4
 
    $ ./ldb --db=/tmp/test_db batchput "multiple words key" "multiple words value"
    OK
 
    $ ./ldb --db=/tmp/test_db scan
    Created bg thread 0x7f4a1dbff700
    a1 ==> b1
    a2 ==> b2
    a3 ==> b3
    a4 ==> b4
    multiple words key : multiple words value
```

To dump an existing leveldb database in HEX:
```bash
$ ./ldb --db=/tmp/test_db dump --hex > /tmp/dbdump
```

To load the dumped HEX format data to a new leveldb database:
```bash
$ cat /tmp/dbdump | ./ldb --db=/tmp/test_db_new load --hex --compression_type=bzip2 --block_size=65536 --create_if_missing --disable_wal
```

To compact an existing leveldb database:
```bash
$ ./ldb --db=/tmp/test_db_new compact --compression_type=bzip2 --block_size=65536
```

You can specify command line `--column_family=<string>` for which column family your query will be against.

`--try_load_options` will try to load the options file in the DB to open the DB. It is a good idea to always try to have this option on when you operate the DB. If you open the DB with default options, it may mess up LSM-tree structure which can't be recovered automatically.

## Open as secondary
By default, `ldb` can only used against a DB that is offline. Operating the DB, even for read-only operations, might make changes to the DB directory, e.g. info logs. An option `--secondary_path=<secondary_path>` would open the DB as a [[Secondary instance|https://github.com/facebook/rocksdb/wiki/Read-only-and-Secondary-instances]], which can be used to open a running DB and/or to minimize impacts to the DB directory. This argument can be used for any ldb command, but since secondary instance not all operations can be done against secondary instance, some operations will fail. Besides write operations which would definitely fail with secondary instance, some read operations might also fail.

# SST dump tool
sst_dump tool can be used to gain insights about a specific SST file. There are multiple operations that sst_dump can execute on a SST file.

```
$ ./sst_dump
file or directory must be specified.

sst_dump --file=<data_dir_OR_sst_file> [--command=check|scan|raw]
    --file=<data_dir_OR_sst_file>
      Path to SST file or directory containing SST files

    --command=check|scan|raw|verify
        check: Iterate over entries in files but dont print anything except if an error is encounterd (default command)
        scan: Iterate over entries in files and print them to screen
        raw: Dump all the table contents to <file_name>_dump.txt
        verify: Iterate all the blocks in files verifying checksum to detect possible coruption but dont print anything except if a corruption is encountered
        recompress: reports the SST file size if recompressed with different
                    compression types

    --output_hex
      Can be combined with scan command to print the keys and values in Hex

    --from=<user_key>
      Key to start reading from when executing check|scan

    --to=<user_key>
      Key to stop reading at when executing check|scan

    --prefix=<user_key>
      Returns all keys with this prefix when executing check|scan
      Cannot be used in conjunction with --from

    --read_num=<num>
      Maximum number of entries to read when executing check|scan

    --verify_checksum
      Verify file checksum when executing check|scan

    --input_key_hex
      Can be combined with --from and --to to indicate that these values are encoded in Hex

    --show_properties
      Print table properties after iterating over the file when executing
      check|scan|raw

    --set_block_size=<block_size>
      Can be combined with --command=recompress to set the block size that will
      be used when trying different compression algorithms

    --compression_types=<comma-separated list of CompressionType members, e.g.,
      kSnappyCompression>
      Can be combined with --command=recompress to run recompression for this
      list of compression types

    --parse_internal_key=<0xKEY>
      Convenience option to parse an internal key on the command line. Dumps the
      internal key in hex format {'key' @ SN: type}
```

##### Dumping SST file blocks
```bash
./sst_dump --file=/path/to/sst/000829.sst --command=raw
``` 
This command will generate a txt file named /path/to/sst/000829_dump.txt.
This file will contain all index blocks and data blocks encoded in Hex. It will also contain information like table properties, footer details and meta index details.

##### Printing entries in SST file
```bash
./sst_dump --file=/path/to/sst/000829.sst --command=scan --read_num=5
```
This command will print the first 5 keys in the SST file to the screen. the output may look like this
```
'Key1' @ 5: 1 => Value1
'Key2' @ 2: 1 => Value2
'Key3' @ 4: 1 => Value3
'Key4' @ 3: 1 => Value4
'Key5' @ 1: 1 => Value5
```
The output can be interpreted like this
```
'<key>' @ <sequence number>: <type> => <value>
```
Please notice that if your key has non-ascii characters, it will be hard to print it on screen, in this case it's a good idea to use --output_hex like this
```bash
./sst_dump --file=/path/to/sst/000829.sst --command=scan --read_num=5 --output_hex
```

You can also specify where do you want to start reading from and where do you want to stop by using --from and --to like this
```bash
./sst_dump --file=/path/to/sst/000829.sst --command=scan --from="key2" --to="key4"
```

You can pass --from and --to using hexadecimal as well by using --input_key_hex
```bash
./sst_dump --file=/path/to/sst/000829.sst --command=scan --from="0x6B657932" --to="0x6B657934" --input_key_hex
```

##### Checking SST file
```bash
./sst_dump --file=/path/to/sst/000829.sst --command=check --verify_checksum
```
This command will Iterate over all entries in the SST file but wont print any thing except if it encountered a problem in the SST file. It will also verify the checksum.

##### Printing SST file properties
```bash
./sst_dump --file=/path/to/sst/000829.sst --show_properties
```
This command will read the SST file properties and print them, output may look like this
```
from [] to []
Process /path/to/sst/000829.sst
Sst file format: block-based
Table Properties:
------------------------------
  # data blocks: 26541
  # entries: 2283572
  raw key size: 264639191
  raw average key size: 115.888262
  raw value size: 26378342
  raw average value size: 11.551351
  data block size: 67110160
  index block size: 3620969
  filter block size: 0
  (estimated) table size: 70731129
  filter policy name: N/A
  # deleted keys: 571272
```

##### Trying different compression algorithms
sst_dump can be used to check the size of the file under different compression algorithms.
```bash
./sst_dump --file=/path/to/sst/000829.sst --show_compression_sizes
```
By using --show_compression_sizes sst_dump will recreate the SST file in memory using different compression algorithms and report the size, output may look like this
```
from [] to []
Process /path/to/sst/000829.sst
Sst file format: block-based
Block Size: 16384
Compression: kNoCompression Size: 103974700
Compression: kSnappyCompression Size: 103906223
Compression: kZlibCompression Size: 80602892
Compression: kBZip2Compression Size: 76250777
Compression: kLZ4Compression Size: 103905572
Compression: kLZ4HCCompression Size: 97234828
Compression: kZSTDNotFinalCompression Size: 79821573
```

These files are created in memory and they are generated with block size of 16KB, the block size can be change by using --set_block_size.