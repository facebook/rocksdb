// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <vector>
#include "db/dbformat.h"
#include "include/rocksdb/env.h"
#include "include/rocksdb/immutable_options.h"
#include "include/rocksdb/listener.h"
#include "include/rocksdb/options.h"
#include "include/rocksdb/status.h"
#include "table/block_based_table_reader.h"

namespace rocksdb {

struct ColDeclaration;
struct KVPairColDeclarations;

class ColumnAwareEncodingReader {
 public:
  explicit ColumnAwareEncodingReader(const std::string& file_name);

  void GetKVPairsFromDataBlocks(std::vector<KVPairBlock>* kv_pair_blocks);

  void EncodeBlocksToRowFormat(WritableFile* out_file,
                               CompressionType compression_type,
                               const std::vector<KVPairBlock>& kv_pair_blocks,
                               std::vector<std::string>* blocks);

  void DecodeBlocksFromRowFormat(WritableFile* out_file,
                                 const std::vector<std::string>* blocks);

  void DumpDataColumns(const std::string& filename,
                       const KVPairColDeclarations& kvp_col_declarations,
                       const std::vector<KVPairBlock>& kv_pair_blocks);

  Status EncodeBlocks(const KVPairColDeclarations& kvp_col_declarations,
                      WritableFile* out_file, CompressionType compression_type,
                      const std::vector<KVPairBlock>& kv_pair_blocks,
                      std::vector<std::string>* blocks, bool print_column_stat);

  void DecodeBlocks(const KVPairColDeclarations& kvp_col_declarations,
                    WritableFile* out_file,
                    const std::vector<std::string>* blocks);

  static void GetColDeclarationsPrimary(
      std::vector<ColDeclaration>** key_col_declarations,
      std::vector<ColDeclaration>** value_col_declarations,
      ColDeclaration** value_checksum_declaration);

  static void GetColDeclarationsSecondary(
      std::vector<ColDeclaration>** key_col_declarations,
      std::vector<ColDeclaration>** value_col_declarations,
      ColDeclaration** value_checksum_declaration);

 private:
  // Init the TableReader for the sst file
  void InitTableReader(const std::string& file_path);

  std::string file_name_;
  EnvOptions soptions_;

  Options options_;

  Status init_result_;
  std::unique_ptr<BlockBasedTable> table_reader_;
  std::unique_ptr<RandomAccessFileReader> file_;

  const ImmutableCFOptions ioptions_;
  InternalKeyComparator internal_comparator_;
  std::unique_ptr<TableProperties> table_properties_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
