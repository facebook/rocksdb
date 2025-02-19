//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/customizable.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class ExternalTableFactory;

// EXPERIMENTAL
// The interface defined in this file is subject to change at any time without
// warning!!

// This file defines an interface for plugging in an external table reader
// into RocksDB. The external table reader will be used instead of the
// BlockBasedTable to load and query sst files. As of now, creating the
// external table files using RocksDB is not supported, but will be added in
// the near future. The external table files can be created outside and
// RocksDB and ingested into a RocksDB instance using the IngestExternalFIle()
// API.
//
// Initial support is for loading and querying the files using an
// SstFileReader. We will add support for ingestion of an external table
// into a limited RocksDB instance that only supports ingestion and not live
// writes in the near future. It'll be followed by support for replacing the
// column family by ingesting a new set of files. In all cases, the external
// table files will only be allowed in the bottommost level.
//
// The external table reader can support one or both of the following layouts -
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
// Many of the options in ReadOptions may not be relevant to the external
// table implementation.
// TODO: Specify which options are relevant

class ExternalTableReader {
 public:
  virtual ~ExternalTableReader() {}

  // Return an Iterator that can be used to scan the table file.
  // The read_options can optionally contain the upper bound
  // key (exclusive) of the scan in iterate_upper_bound.
  virtual Iterator* NewIterator(const ReadOptions& read_options,
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

struct ExternalTableOptions {
  const std::shared_ptr<const SliceTransform>& prefix_extractor;
  const Comparator* comparator;

  ExternalTableOptions(
      const std::shared_ptr<const SliceTransform>& _prefix_extractor,
      const Comparator* _comparator)
      : prefix_extractor(_prefix_extractor), comparator(_comparator) {}
};

class ExternalTableFactory : public Customizable {
 public:
  ~ExternalTableFactory() override {}

  const char* Name() const override { return "ExternalTableFactory"; }

  virtual Status NewTableReader(
      const ReadOptions& read_options, const std::string& file_path,
      const ExternalTableOptions& table_options,
      std::unique_ptr<ExternalTableReader>* table_reader) = 0;
};

// Allocate a TableFactory that wraps around an ExternalTableFactory. Use this
// to allocate and set in ColumnFamilyOptions::table_factory.
std::shared_ptr<TableFactory> NewExternalTableFactory(
    std::shared_ptr<ExternalTableFactory> inner_factory);

}  // namespace ROCKSDB_NAMESPACE
