//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <cstdio>
#include <cstdlib>

#ifndef ROCKSDB_LITE
#ifdef GFLAGS

#include <gflags/gflags.h>
#include <inttypes.h>
#include <vector>
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "table/block_based_table_builder.h"
#include "table/block_based_table_reader.h"
#include "table/format.h"
#include "tools/sst_dump_tool_imp.h"
#include "util/compression.h"
#include "util/stop_watch.h"
#include "utilities/col_buf_encoder.h"
#include "utilities/column_aware_encoding_util.h"

using GFLAGS::ParseCommandLineFlags;
DEFINE_string(encoded_file, "", "file to store encoded data blocks");
DEFINE_string(decoded_file, "",
              "file to store decoded data blocks after encoding");
DEFINE_string(format, "col", "Output Format. Can be 'row' or 'col'");
// TODO(jhli): option `col` should be removed and replaced by general
// column specifications.
DEFINE_string(index_type, "col", "Index type. Can be 'primary' or 'secondary'");
DEFINE_string(dump_file, "",
              "Dump data blocks separated by columns in human-readable format");
DEFINE_bool(decode, false, "Deocde blocks after they are encoded");
DEFINE_bool(stat, false,
            "Print column distribution statistics. Cannot decode in this mode");
DEFINE_string(compression_type, "kNoCompression",
              "The compression algorithm used to compress data blocks");

namespace rocksdb {

class ColumnAwareEncodingExp {
 public:
  static void Run(const std::string& sst_file) {
    bool decode = FLAGS_decode;
    if (FLAGS_decoded_file.size() > 0) {
      decode = true;
    }
    if (FLAGS_stat) {
      decode = false;
    }

    ColumnAwareEncodingReader reader(sst_file);
    std::vector<ColDeclaration>* key_col_declarations;
    std::vector<ColDeclaration>* value_col_declarations;
    ColDeclaration* value_checksum_declaration;
    if (FLAGS_index_type == "primary") {
      ColumnAwareEncodingReader::GetColDeclarationsPrimary(
          &key_col_declarations, &value_col_declarations,
          &value_checksum_declaration);
    } else {
      ColumnAwareEncodingReader::GetColDeclarationsSecondary(
          &key_col_declarations, &value_col_declarations,
          &value_checksum_declaration);
    }
    KVPairColDeclarations kvp_cd(key_col_declarations, value_col_declarations,
                                 value_checksum_declaration);

    if (!FLAGS_dump_file.empty()) {
      std::vector<KVPairBlock> kv_pair_blocks;
      reader.GetKVPairsFromDataBlocks(&kv_pair_blocks);
      reader.DumpDataColumns(FLAGS_dump_file, kvp_cd, kv_pair_blocks);
      return;
    }
    std::unordered_map<std::string, CompressionType> compressions = {
        {"kNoCompression", CompressionType::kNoCompression},
        {"kZlibCompression", CompressionType::kZlibCompression},
        {"kZSTD", CompressionType::kZSTD}};

    // Find Compression
    CompressionType compression_type = compressions[FLAGS_compression_type];
    EnvOptions env_options;
    if (CompressionTypeSupported(compression_type)) {
      fprintf(stdout, "[%s]\n", FLAGS_compression_type.c_str());
      unique_ptr<WritableFile> encoded_out_file;

      std::unique_ptr<Env> env(NewMemEnv(Env::Default()));
      if (!FLAGS_encoded_file.empty()) {
        env->NewWritableFile(FLAGS_encoded_file, &encoded_out_file,
                             env_options);
      }

      std::vector<KVPairBlock> kv_pair_blocks;
      reader.GetKVPairsFromDataBlocks(&kv_pair_blocks);

      std::vector<std::string> encoded_blocks;
      StopWatchNano sw(env.get(), true);
      if (FLAGS_format == "col") {
        reader.EncodeBlocks(kvp_cd, encoded_out_file.get(), compression_type,
                            kv_pair_blocks, &encoded_blocks, FLAGS_stat);
      } else {  // row format
        reader.EncodeBlocksToRowFormat(encoded_out_file.get(), compression_type,
                                       kv_pair_blocks, &encoded_blocks);
      }
      if (encoded_out_file != nullptr) {
        uint64_t size = 0;
        env->GetFileSize(FLAGS_encoded_file, &size);
        fprintf(stdout, "File size: %" PRIu64 "\n", size);
      }
      uint64_t encode_time = sw.ElapsedNanosSafe(false /* reset */);
      fprintf(stdout, "Encode time: %" PRIu64 "\n", encode_time);
      if (decode) {
        unique_ptr<WritableFile> decoded_out_file;
        if (!FLAGS_decoded_file.empty()) {
          env->NewWritableFile(FLAGS_decoded_file, &decoded_out_file,
                               env_options);
        }
        sw.Start();
        if (FLAGS_format == "col") {
          reader.DecodeBlocks(kvp_cd, decoded_out_file.get(), &encoded_blocks);
        } else {
          reader.DecodeBlocksFromRowFormat(decoded_out_file.get(),
                                           &encoded_blocks);
        }
        uint64_t decode_time = sw.ElapsedNanosSafe(true /* reset */);
        fprintf(stdout, "Decode time: %" PRIu64 "\n", decode_time);
      }
    } else {
      fprintf(stdout, "Unsupported compression type: %s.\n",
              FLAGS_compression_type.c_str());
    }
    delete key_col_declarations;
    delete value_col_declarations;
    delete value_checksum_declaration;
  }
};

}  // namespace rocksdb

int main(int argc, char** argv) {
  int arg_idx = ParseCommandLineFlags(&argc, &argv, true);
  if (arg_idx >= argc) {
    fprintf(stdout, "SST filename required.\n");
    exit(1);
  }
  std::string sst_file(argv[arg_idx]);
  if (FLAGS_format != "row" && FLAGS_format != "col") {
    fprintf(stderr, "Format must be 'row' or 'col'\n");
    exit(1);
  }
  if (FLAGS_index_type != "primary" && FLAGS_index_type != "secondary") {
    fprintf(stderr, "Format must be 'primary' or 'secondary'\n");
    exit(1);
  }
  rocksdb::ColumnAwareEncodingExp::Run(sst_file);
  return 0;
}

#else
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#endif  // GFLAGS
#else
int main(int argc, char** argv) {
  fprintf(stderr, "Not supported in lite mode.\n");
  return 1;
}
#endif  // ROCKSDB_LITE
