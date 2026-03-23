## Full File Checksum Background and Motivations
**SST integrity:** In current RocksDB, we calculate the checksum for block (e.g., data block) before they are flushed to file system and store the checksum in block trailer. When reading the blocks, the checksum is verified. It ensures the correctness of data block. However, to better protect the data in RocksDB, checksum for each SST file is needed, especially when the SST files are stored remotely or the SST file are moved or copied. File might be corrupted during the transmission or when it is stored in the storage.

**SST identity:** If a wrong SST file is transferred to a RocksDB SST file directory, all block checksum will match, but it doesn't contain the data we want. This usually can be caught by file name and file size mismatch because the chance that two different SST files share the same size is very small, but it may not be a good assumption to make. A full file checksum

SST file checksum can be used when: 1) SST files are copied to other places (e.g., backup, move, or replicate); 2) SST files are stored remotely, 3) ingesting external SST files to RocksDB, 4) verify the SST file when the whole file is read in DB (e.g., compaction). 

## Full File Checksum Design
1. where to generate: SST file checksum is generated when a SST file is generated in RocksDB (1. flush Memtable 2. compaction) via writeable_file_writer.
2. Flexibility
    1. options.file_checksum_gen_factory is for upper-layer applications to plugin a specific file checksum generator factory implementation. FileChecksumGenFactory creates a FileChecksumGenerator object for each SST file and it generates the file checksum for a certain file. The object IS NOT shared, so FileChecksumGenerator can store the intermediate data during checksum generating in the object and the implementation does not need to be thread safe. 
    2. Provide a default checksum generator (FileChecksumGenCrc32c) and factory (FileChecksumGenCrc32cFactory) for SST files (based on Crc32c) such that user can easily use it if they do not have their own requirement.
    3. The checksum value is std::string, any other checksum value type such as uint32, int, uint64 can be easily converted to a string type. checksum function name is also a string.
3. what should be stored
    1. the checksum value if self.
    2. the name of the checksum function: there are many different checksum functions. Therefore, the checksum value should be pair with its function name. Otherwise, either RocksDB or the application is not able to make meaningful checksum check.
4. where to store the checksums
    1. we store the checksum function name and checksum value in vstorage as part of FileMetadata.
    2. we store the checksum function name and checksum value in MANIFEST for persistency
5. Tools: Dump the checksum of all SST file from MANIFEST in a map (in ldb)

## How to Use Full File Checksum
In order to enable the full file checksum, user needs to initialize the Options.file_checksum_gen_factory. For example: 
```cpp
Options options;
FileChecksumGenCrc32cFactory* file_checksum_gen_factory = new FileChecksumGenCrc32cFactory();
options.file_checksum_gen_factory.reset(file_checksum_gen_factory);
ImmutableCFOptions ioptions(options);
......
```

To implement a customized checksum generator factory, the application needs to implement a checksum generator. For example:
```cpp
class FileChecksumGenCrc32c : public FileChecksumGenerator {
 public:
  FileChecksumGenCrc32c(const FileChecksumGenContext& /*context*/) {
    checksum_ = 0;
  }
  void Update(const char* data, size_t n) override {
    checksum_ = crc32c::Extend(checksum_, data, n);
  }
  void Finalize() override { checksum_str_ = Uint32ToString(checksum_); }
  std::string GetChecksum() const override { return checksum_str_; }
  const char* Name() const override { return "FileChecksumCrc32c"; }
 private:
  uint32_t checksum_;
  std::string checksum_str_;
};
```

And also the checksum generator factory, for example:

```cpp
class FileChecksumGenCrc32cFactory : public FileChecksumGenFactory {
 public:
  std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
      const FileChecksumGenContext& context) override {
    return std::unique_ptr<FileChecksumGenerator>(
        new FileChecksumGenCrc32c(context));
  }
  const char* Name() const override { return "FileChecksumGenCrc32cFactory"; }
};
```

When sst_file_checksum_func is intialized (!=nullptr), RocksDB generate the checksum value when creating the SST file.

In the current stage, we do not provide a public db interface to list or get the checksum value and checksum function name. However, there are two ways that user can get the checksum.

1. by calling `db->GetLiveFileMetadata(std::vector<LiveFileMetaData>)`, checksum value and checksum function name are included in the LiveFileMetadata. The checksum information is from vstorage in memory.
2. If the db is not running, or if user only has the Manifest file, we can use ldb tool to print a list of checksum with the file name. It will print a list of SST file wit checksum information as the following format:[file_number, checksum_function_name, checksum value]

```bash
./ldb --db=<db path> file_checksum_dump.
```
### File Checksum application in backup engine of RocksDB
We also compare the full file checksum in the RocksDB backup engine if it is possible. When we initialize the backup engine instance, if the SST file checksum is enabled, we are able to get the checksum function name and the checksum value of the SST files to be backuped. Then, we compare them with the checksum we generated. Note that, by default, we use crc32c as the checksum calculation method for the full files. It is also the default for backup full file checksum check.

### File ingestion with file checksum
Users may use the `DB::IngestExternalFiles` to ingest the external SST files to a DB. When DB enables the full file checksum, by default, we will calculate the checksum of each ingested SST file and store them in the DB for further use. To achieve better data protection and data integrity, user can also provide the checksum value of each SST file to be ingested and the corresponding checksum calculation method in the ingestion parameters. When running the ingestion, we use an option `ingestion_options_.verify_file_checksum` to control if we need to verify the provided checksum or not. Only when `verify_file_checksum == false` and the checksum for ingested files are provided, DB will use the provided checksum and does not generate the checksum for ingested files. Otherwise, the ingestion engine will also calculate the checksum of each ingested file with the checksum calculation method requested by the DB, compare them with the provided checksum method and checksum value. If one of them does not match, ingestion will be failed. 

The interface and data structure that we can provide the file checksum in file ingestion
```cpp
// It is valid that files_checksums and files_checksum_func_names are both
// empty (no checksum information is provided for ingestion). Otherwise,
// their sizes should be the same as external_files. The file order should
// be the same in three vectors and guaranteed by the caller.
struct IngestExternalFileArg {
  ColumnFamilyHandle* column_family = nullptr;
  std::vector<std::string> external_files;
  IngestExternalFileOptions options;
  std::vector<std::string> files_checksums;
  std::vector<std::string> files_checksum_func_names;
};
```

```cpp
  virtual Status IngestExternalFiles(
      const std::vector<IngestExternalFileArg>& args) = 0;
```

## The Next Step of Full File Checksum
We plan to work on following:
1. Work with some use cases to apply the full file checksum.
2. Implement WAL file checksum and store them in manifest too.

## File Write Checksum Handoff Background
In current RocksDB, we already have the full SST file checksum generated during Flush, Compaction, and SstFileWriter (see Full File Checksum wiki). Applications plugin the FileChecksumGenFactory to enable SST file checksum and it will create a file checksum generator for each SST file. For Flush and Compaction, the generated SST file checksum is stored in Manifest such that later on the applications can use it for file identity and integrity (e.g, move, copy, ingest) check or verification

However, it might be too late to find the file corruption that happens before SST/WAL/Manifest write (e.g., bit flip or other CPU and Memory related corruption) after the file is stored for a long time. Therefore, we need a mechanism to achieve checksum handoff during file write, which passes the data verification information together with data. To enable checksum handoff, the underlying storage system needs to support the checksum verification.

## File Write Checksum Handoff Design Details
1. who generates the checksum: the checksum is generated for each piece of data that will be written to the storage layer that is at FileSystem/Env WritableFile::Append(). Currently, we only support crc32c as the checksum type. Note that, the checksum is calculated after the rate limiter.
2. who brings the checksum information: A data structure called DataVerificationInfo is introduced to carry the checksum and potentially other data verification information with data.

```cpp
// A data structure brings the data verification information, which is
// used together with data being written to a file.
struct DataVerificationInfo {
  // checksum of the data being written.
  Slice checksum;
};
```
3. How it is passed to the lower layers: we introduce the new interface in File System and Env WritableFile class.
```cpp
  virtual IOStatus Append(const Slice& data, const IOOptions& options,
                          const DataVerificationInfo& /* verification_info */,
                          IODebugContext* dbg)
```
To achieve the checksum verification, the user-customized storage layer should be able to re-calculate the crc32c checksum and compare it with the one in the DataVerificationInfo. For example, in a distributed file system, the wrap layer will receive the data and checksum information from WritableFile::Append. the client-side can do the verification or the server-side will do the check before data is persisted. Once the mismatch is found, the error code will be returned to RocksDB and RocksDB can handle it.
4. The control mechanism: checksum handoff is turned off as default. Currently, we support three types of files: 1) SST file, 2) Manifest, 3) WAL. To enable checksum handoff for certain file type, add the file type to the options 
```cpp
FileTypeSet checksum_handoff_file_types;
```
For example, if  we want to enable checksum handoff for WAL and Manifest. The following should be set:
```cpp
Options options = CurrentOptions();
    options.checksum_handoff_file_types.Add(FileType::kWalFile);
    options.checksum_handoff_file_types.Add(FileType::kDescriptorFile);
```
5. Performance overhead: Checksum calculate and verification has CPU overhead. If the existing storage system already has the checksum calculation and verification mechanism, the checksum handoff may be able to move the calculation logic up to RocksDB and avoid the extra overhead. It depends on the storage layer implementation.

### Reducing checksum calculation overhead and improve protection coverage for WAL and Manifest
In the initial design, all the file writes before we call `FileSystem::FSWritableFile->Append()`, we will calculate the checksum of each data piece. This is done after the data is buffered in `WritableFileWriter` buffer and split small in the rate-limiter. Therefore, the checksum calculation has high overhead even the original data has the checksum. Also, we cannot cover the protection from the point when the original data is created due to buffering and rate-limiter.

In RocksDB, WAL and Manifest are written via the log record, each log record has its own crc32c checksum and record itself is aligned. Therefore, for WAL and Manifest checksum handoff, we introduce the `crc32c_combine` to use the existing crc32c checksum for handoff. First, for each `WritableFileWriter::Append`, we allow the caller pass in the crc32c checksum of the data. Then, we use `crc32c_combine` to directly generate the crc32c of the buffered data region. Finally, we do not let the rate-limiter to cut the buffered data into small pieces. Instead, we call rata-limiter multiple times until we get enough write quota. Finally, we write the buffered data and its combined crc32c checksum to the `FileSystem::FSWritableWriter->append()`, which is the new interface we introduced to achieve checksum handoff.

Note that, for SST file and other file writes, we are not able to use the same technique since the blocks are not aligned. Therefore, the crc32c checksum of SST files and other files (e.g., blob or info_log files) will still be calculated after we use rate-limiter in `WritableFileWriter`


## Future Work of File Write Checksum Handoff
1. The file data (e.g., data block of SST file and log record of WAL and Manifest file) is created before WritableFileWrite::Append is calculated. After Append is called, the original data may be memcpy to a write buffer and may be segmented by the rate-limiter, which happens before the checksum is calculated. In the next step, we hope to generate the checksum earlier, and also, if we can effectively use the existing checksum in the SST file or WAL/Manifest file, it will save the CPU overhead.
2. support other checksum types or user-customized checksum type for checksum handoff (similar to full file checksum plugin)