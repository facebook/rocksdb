//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/advanced_iterator.h"
#include "rocksdb/customizable.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/file_system.h"
#include "rocksdb/iterator_base.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class ExternalTableFactory;

// EXPERIMENTAL
// The interface defined in this file is subject to change at any time without
// warning!!

// This file defines an interface for plugging in an external table
// into RocksDB. The external table reader will be used instead of the
// BlockBasedTable to load and query sst files.
// The external table files can be created using an SstFileWriter. Eventually
// external tables will be allowed to be ingested into a RocksDB instance
// using the IngestExternalFIle() API.
//
// Initial support is for writing and querying the files using an
// SstFileWriter and SstFileReader. We will add support for ingestion of an
// external table into a limited RocksDB instance that only supports ingestion
// and not live writes in the near future. It'll be followed by support for
// replacing the column family by ingesting a new set of files. In all cases,
// the external table files will only be allowed in the bottommost level.
//
// The external table can support one or both of the following layouts -
// 1. Total order seek - All the keys in the files are in sorted order, and a
//    user can seek to the first, last, or any key in between and iterate
//    forwards or backwards till the end of the range. To support this mode,
//    the implementation needs to use the comparator passed in
//    ExternalTableOptions to enforce the key ordering. The prefix_extractor
//    in ExternalTableOptions and the ExternalTableReader interfaces can be
//    ignored.
// 2. Prefix seek - In this mode, the prefix_extractor is used to extract the
//    prefix from a key. All the keys sharing the same prefix are ordered in
//    ascending order according to the comparator. However, no specific
//    ordering is required across prefixes. Users can scan keys by seeking
//    to a specific key inside a prefix, and iterate forwards or backwards
//    within the prefix. The prefix_same_as_start flag in ReadOptions will
//    be true.
// 3. Both - If supporting both of the above, a user can seek inside a prefix
//    and iterate beyond the prefix. The prefix_same_as_start in ReadOptions
//    will be false. Additionally, the total_order_seek flag can be set to
//    true to seek to the first non-empty prefix (as determined by the key
//    order) if the seek prefix is empty.
//
// Many of the options in ReadOptions and WriteOptions may not be relevant to
// the external table implementation.
// TODO: Specify which options are relevant

class ExternalTableIterator : public IteratorBase {
 public:
  virtual ~ExternalTableIterator() {}

  // This can optionally be called to prepare the iterator for a series
  // of scans. The scan_opts parameter specifies the order of scans to
  // follow, as well as the limits for those scans. After calling this,
  // the caller will Seek() the iterator to successive start keys in scan_opts.
  //
  // If Prepare() is called again with a different scan_opts pointer, it
  // means the iterator will be reused for a new multi scan. If scan_opts
  // is null, then the previous Prepare() can be discarded.
  //
  // The caller guarantees the lifetime of scan_opts until its either cleared
  // or replaced by another Prepare().
  // TODO: Update the contract to trim the scan_opts range to only include
  // scans that potentially intersect the file key range.
  //
  // If the sequence of Seeks is interrupted by seeking to some other target
  // key, then the iterator is free to discard anything done during Prepare.
  virtual void Prepare(const ScanOptions scan_opts[], size_t num_opts) = 0;

  // Similar to Next(), except it also fills the result and returns whether
  // the iterator is on a valid key or not
  virtual bool NextAndGetResult(IterateResult* result) = 0;

  // Prepares the value if its lazily materialized. The implementation can
  // request that this be called by setting value_prepared to false in
  // IterateResult. Next() should always implicitly materialize the
  // value.
  bool PrepareValue() override = 0;

  // Return the current key's value
  virtual Slice value() const = 0;

  // Return the current position bounds check result - kInbound if the
  // position is a valid key, kOutOfBound if the key is out of bound (i.e
  // scan has terminated), or kUnknown if end of file.
  virtual IterBoundCheck UpperBoundCheckResult() = 0;
};

class ExternalTableReader {
 public:
  virtual ~ExternalTableReader() {}

  // Return an Iterator that can be used to scan the table file.
  // The read_options can optionally contain the upper bound
  // key (exclusive) of the scan in iterate_upper_bound.
  virtual ExternalTableIterator* NewIterator(
      const ReadOptions& read_options,
      const SliceTransform* prefix_extractor) = 0;

  // Point lookup the given key and return its value
  virtual Status Get(const ReadOptions& read_options, const Slice& key,
                     const SliceTransform* prefix_extractor,
                     std::string* value) = 0;

  // Point lookup the given vector of keys and return the values, as well
  // as status of each individual lookup in statuses.
  virtual void MultiGet(const ReadOptions& read_options,
                        const std::vector<Slice>& keys,
                        const SliceTransform* prefix_extractor,
                        std::vector<std::string>* values,
                        std::vector<Status>* statuses) = 0;

  // Return TableProperties for the file. At a minimum, the following
  // properties need to be returned -
  // comparator_name
  // num_entries
  // raw_key_size
  // raw_value_size
  virtual std::shared_ptr<const TableProperties> GetTableProperties() const = 0;

  virtual Status VerifyChecksum(const ReadOptions& /*ro*/) {
    return Status::NotSupported("VerifyChecksum() not supported");
  }
};

// A table builder interface that can be used by SstFileWriter to allow
// RocksDB users to write external table files. The sequence of operations
// to write an external table is as follows -
// 1. Add() is called one or more times to write all key-values to the table.
//    Its called in increasing key order, as determined by the comparator.
//    The input key is a user key, i.e sequence number and value type are
//    stripped out.
// 2. After every Add() operation, status() is called to check the current
//    status.
// 3. After the last key is added, Finish() is called to do whatever is
//    necessary to ensure the data is persisted in the table file.
// 4. If there is a failure midway for some reason, Abandon() is called
//    instead of Finish().
// 5. At the end, FileSize(), GetTableProperties(), and status() are called to
//    get the final size of the file, the table properties, and the final
//    status. GetFileChecksum() and GetFileChecksumFuncName() may also be
//    called to get checksum information about the whole file, but their
//    implementation is optional.
class ExternalTableBuilder {
 public:
  virtual ~ExternalTableBuilder() {}

  // Write a single KV to the table file. This is guaranteed to be called
  // in key order, and the write may be buffered and flushed at a later time.
  virtual void Add(const Slice& key, const Slice& value) = 0;

  // Return the current Status. This could return non-ok, for example, if
  // Add() fails for some reason.
  virtual Status status() const = 0;

  // Flush and close the table file
  virtual Status Finish() = 0;

  // Delete the partial file and release any allocated resources. Either this
  // or Finish() will be called, but not both.
  virtual void Abandon() = 0;

  // Return the size of the table file. Will be called at the end, after
  // Finish().
  virtual uint64_t FileSize() const = 0;

  //  As mentioned in earlier comments, the following table properties must be
  //  returned at a minimum -
  //  comparator_name
  //  num_entries
  //  raw_key_size
  //  raw_value_size
  virtual TableProperties GetTableProperties() const = 0;

  virtual std::string GetFileChecksum() const { return kUnknownFileChecksum; }

  virtual const char* GetFileChecksumFuncName() const {
    return kUnknownFileChecksumFuncName;
  }
};

struct ExternalTableOptions {
  const std::shared_ptr<const SliceTransform>& prefix_extractor;
  const Comparator* comparator;
  const std::shared_ptr<FileSystem>& fs;
  const FileOptions& file_options;

  ExternalTableOptions(
      const std::shared_ptr<const SliceTransform>& _prefix_extractor,
      const Comparator* _comparator, const std::shared_ptr<FileSystem>& _fs,
      const FileOptions& _file_options)
      : prefix_extractor(_prefix_extractor),
        comparator(_comparator),
        fs(_fs),
        file_options(_file_options) {}
};

struct ExternalTableBuilderOptions {
  const ReadOptions& read_options;
  const WriteOptions& write_options;
  const std::shared_ptr<const SliceTransform>& prefix_extractor;
  const Comparator* comparator;
  const std::string& column_family_name;
  const std::string db_id;
  const std::string db_session_id;
  const TableFileCreationReason reason;

  ExternalTableBuilderOptions(
      const ReadOptions& _read_options, const WriteOptions& _write_options,
      const std::shared_ptr<const SliceTransform>& _prefix_extractor,
      const Comparator* _comparator, const std::string& _column_family_name,
      const TableFileCreationReason _reason)
      : read_options(_read_options),
        write_options(_write_options),
        prefix_extractor(_prefix_extractor),
        comparator(_comparator),
        column_family_name(_column_family_name),
        reason(_reason) {}
};

class ExternalTableFactory : public Customizable {
 public:
  ~ExternalTableFactory() override {}

  const char* Name() const override { return "ExternalTableFactory"; }

  virtual Status NewTableReader(
      const ReadOptions& read_options, const std::string& file_path,
      const ExternalTableOptions& table_options,
      std::unique_ptr<ExternalTableReader>* table_reader) const = 0;

  // The table builder should use the file pointer to append to the file.
  // Do not sync or close the file after finishing. RocksDB will do that.
  virtual ExternalTableBuilder* NewTableBuilder(
      const ExternalTableBuilderOptions& builder_options,
      const std::string& file_path, FSWritableFile* file) const = 0;
};

// Allocate a TableFactory that wraps around an ExternalTableFactory. Use this
// to allocate and set in ColumnFamilyOptions::table_factory.
std::shared_ptr<TableFactory> NewExternalTableFactory(
    std::shared_ptr<ExternalTableFactory> inner_factory);

}  // namespace ROCKSDB_NAMESPACE
