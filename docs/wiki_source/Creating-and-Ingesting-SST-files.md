RocksDB provide the user with APIs that can be used to create SST files that can be ingested later. This can be useful if you have a use case that needs to load the data quickly, but the process of creating the data can be done offline.

## Creating SST file
rocksdb::SstFileWriter can be used to create SST file. After creating a SstFileWriter object you can open a file, insert rows into it and finish.   

This is an example of how to create SST file in `/home/usr/file1.sst`

```cpp
Options options;

SstFileWriter sst_file_writer(EnvOptions(), options);
// Path to where we will write the SST file
std::string file_path = "/home/usr/file1.sst";

// Open the file for writing
Status s = sst_file_writer.Open(file_path);
if (!s.ok()) {
    printf("Error while opening file %s, Error: %s\n", file_path.c_str(),
           s.ToString().c_str());
    return 1;
}

// Insert rows into the SST file, note that inserted keys must be 
// strictly increasing (based on options.comparator)
for (...) {
  s = sst_file_writer.Put(key, value);
  if (!s.ok()) {
    printf("Error while adding Key: %s, Error: %s\n", key.c_str(),
           s.ToString().c_str());
    return 1;
  }
}

// Close the file
s = sst_file_writer.Finish();
if (!s.ok()) {
    printf("Error while finishing file %s, Error: %s\n", file_path.c_str(),
           s.ToString().c_str());
    return 1;
}
return 0;
```

Now we have our SST file located at `/home/usr/file1.sst`.

Please note that:  
*    Options passed to SstFileWriter will be used to figure out the table type, compression options, etc that will be used to create the SST file.
*    The Comparator that is passed to the SstFileWriter must be exactly the same as the Comparator used in the DB that this file will be ingested into.
*    Rows must be inserted in a strictly increasing order (one exception is range tombstones, which can be out-of-order and contain overlapping ranges). 

You can learn more about the SstFileWriter by checking [include/rocksdb/sst_file_writer.h](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/sst_file_writer.h)
## Ingesting SST files
Ingesting an SST files is simple, all you need to do is to call DB::IngestExternalFile() and pass the file paths as a vector of `std::string`
```cpp
IngestExternalFileOptions ifo;
// Ingest the 2 passed SST files into the DB
Status s = db_->IngestExternalFile({"/home/usr/file1.sst", "/home/usr/file2.sst"}, ifo);
if (!s.ok()) {
  printf("Error while adding file %s and %s, Error %s\n",
         file_path1.c_str(), file_path2.c_str(), s.ToString().c_str());
  return 1;
}
```

You can learn more by checking DB::IngestExternalFile() and DB::IngestExternalFiles() in [include/rocksdb/db.h](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/db.h). DB::IngestExternalFiles() ingests a collection of external SST files for **multiple** column families following the 'all-or-nothing' property. If the function returns Status::OK, then all files are ingested successfully for **all** column families of interest. If the function returns non-OK status, then none of the files are ingested into none of the column families.

## What happens when you ingest a file

**When you call DB::IngestExternalFile() We will**
- Copy or link the file into the DB directory
- block (not skip) writes to the DB because we have to keep a consistent db state so we have to make sure we can safely assign the right sequence number to all the keys in the file we are going to ingest
- If file key range overlap with memtable key range, flush memtable
- Assign the file to the best level possible in the LSM-tree
- Assign the file a global sequence number
- Resume writes to the DB

**We pick the lowest level in the LSM-Tree that satisfies these conditions**
- The file can fit in the level
- The file key range don't overlap with any keys in this layer and its upper levels
- The file don't overlap with the outputs of running compactions going to this level and its upper levels

**Global sequence number**

Files created using SstFileWriter have a special field in their metablock called global sequence number, when this field is used, all the keys inside this file start acting as if they have such sequence number. When we ingest a file, we assign a sequence number to all the keys in this file. Before RocksDB 5.16, RocksDB always updates this global sequence number field in the metablock of the SST file using a random write. From RocksDB 5.16, RocksDB enables user to choose whether to update this field via `IngestExternalFileOptions::write_global_seqno`. If this field is false during ingestion, then RocksDB uses the information in MANIFEST to deduce the global sequence number when accessing the file. This can be useful if the underlying file system does not support random write or if users wish to minimize sync operations. If backward compatibility is the concern, set this option to true so that external SST files ingested by RocksDB 5.16 or newer can be opened by RocksDB 5.15 or older.

## Ingestion Behind
Starting from 5.5, IngestExternalFile() will load a list of external SST files with ingestion behind supported, which means duplicate keys will be skipped if `ingest_behind==true`. In this mode we will always ingest in the bottom most level. Duplicate keys in the file being ingested to be skipped rather than overwriting existing data under that key. Currently, only universal compaction supports ingest-behind.

**Use case**

Back-fill of some historical data in the database without over-writing existing newer version of data. This option could only be used if the DB has been running with allow_ingest_behind=true since the dawn of time.
All files will be ingested at the bottommost level with `seqno=0`.


## Ingesting DB-Generated SST Files (Experimental)
Starting from RocksDB 9.5, RocksDB provides an [ingestion option](https://github.com/facebook/rocksdb/blob/feb1486e37a563fcac2014f4a1bc1c416c3d9a02/include/rocksdb/options.h#L2656) that allows users to ingest DB generated SST files (instead of being generated by SstFileWriter). The requirement is that the DB generated files should have no duplicated keys and all keys have sequence number 0. This can be achieved by running a manual compaction on a CF with no snapshot preserving sequence number/overwrites. Starting from RocksDB 10.6, this requirement is no longer needed and the feature has been extended to any DB generated file. The only requirement is that the files being ingested don't already overlap with existing files. 
