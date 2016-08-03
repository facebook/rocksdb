//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef ROCKSDB_LITE

#include "utilities/column_aware_encoding_util.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <algorithm>
#include <utility>
#include <vector>
#include "include/rocksdb/comparator.h"
#include "include/rocksdb/slice.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "table/block_based_table_builder.h"
#include "table/block_based_table_factory.h"
#include "table/format.h"
#include "table/table_reader.h"
#include "util/coding.h"
#include "utilities/col_buf_decoder.h"
#include "utilities/col_buf_encoder.h"

#include "port/port.h"

namespace rocksdb {

ColumnAwareEncodingReader::ColumnAwareEncodingReader(
    const std::string& file_path)
    : file_name_(file_path),
      ioptions_(options_),
      internal_comparator_(BytewiseComparator()) {
  InitTableReader(file_name_);
}

void ColumnAwareEncodingReader::InitTableReader(const std::string& file_path) {
  std::unique_ptr<RandomAccessFile> file;
  uint64_t file_size;
  options_.env->NewRandomAccessFile(file_path, &file, soptions_);
  options_.env->GetFileSize(file_path, &file_size);

  file_.reset(new RandomAccessFileReader(std::move(file)));

  options_.comparator = &internal_comparator_;
  options_.table_factory = std::make_shared<BlockBasedTableFactory>();
  shared_ptr<BlockBasedTableFactory> block_table_factory =
      std::dynamic_pointer_cast<BlockBasedTableFactory>(options_.table_factory);

  std::unique_ptr<TableReader> table_reader;
  block_table_factory->NewTableReader(
      TableReaderOptions(ioptions_, soptions_, internal_comparator_,
                         /*skip_filters=*/false),
      std::move(file_), file_size, &table_reader, /*enable_prefetch=*/false);

  table_reader_.reset(dynamic_cast<BlockBasedTable*>(table_reader.release()));
}

void ColumnAwareEncodingReader::GetKVPairsFromDataBlocks(
    std::vector<KVPairBlock>* kv_pair_blocks) {
  table_reader_->GetKVPairsFromDataBlocks(kv_pair_blocks);
}

void ColumnAwareEncodingReader::DecodeBlocks(
    const KVPairColDeclarations& kvp_col_declarations, WritableFile* out_file,
    const std::vector<std::string>* blocks) {
  char* decoded_content_base = new char[16384];
  Options options;
  ImmutableCFOptions ioptions(options);
  for (auto& block : *blocks) {
    KVPairColBufDecoders kvp_col_bufs(kvp_col_declarations);
    auto& key_col_bufs = kvp_col_bufs.key_col_bufs;
    auto& value_col_bufs = kvp_col_bufs.value_col_bufs;
    auto& value_checksum_buf = kvp_col_bufs.value_checksum_buf;

    auto& slice_final_with_bit = block;
    uint32_t format_version = 2;
    Slice compression_dict;
    BlockContents contents;
    const char* content_ptr;

    CompressionType type =
        (CompressionType)slice_final_with_bit[slice_final_with_bit.size() - 1];
    if (type != kNoCompression) {
      UncompressBlockContents(slice_final_with_bit.c_str(),
                              slice_final_with_bit.size() - 1, &contents,
                              format_version, compression_dict, ioptions);
      content_ptr = contents.data.data();
    } else {
      content_ptr = slice_final_with_bit.data();
    }

    size_t num_kv_pairs;
    const char* header_content_ptr = content_ptr;
    num_kv_pairs = DecodeFixed64(header_content_ptr);

    header_content_ptr += sizeof(size_t);
    size_t num_key_columns = key_col_bufs.size();
    size_t num_value_columns = value_col_bufs.size();
    std::vector<const char*> key_content_ptr(num_key_columns);
    std::vector<const char*> value_content_ptr(num_value_columns);
    const char* checksum_content_ptr;

    size_t num_columns = num_key_columns + num_value_columns;
    const char* col_content_ptr =
        header_content_ptr + sizeof(size_t) * num_columns;

    // Read headers
    for (size_t i = 0; i < num_key_columns; ++i) {
      key_content_ptr[i] = col_content_ptr;
      key_content_ptr[i] += key_col_bufs[i]->Init(key_content_ptr[i]);
      size_t offset;
      offset = DecodeFixed64(header_content_ptr);
      header_content_ptr += sizeof(size_t);
      col_content_ptr += offset;
    }
    for (size_t i = 0; i < num_value_columns; ++i) {
      value_content_ptr[i] = col_content_ptr;
      value_content_ptr[i] += value_col_bufs[i]->Init(value_content_ptr[i]);
      size_t offset;
      offset = DecodeFixed64(header_content_ptr);
      header_content_ptr += sizeof(size_t);
      col_content_ptr += offset;
    }
    checksum_content_ptr = col_content_ptr;
    checksum_content_ptr += value_checksum_buf->Init(checksum_content_ptr);

    // Decode block
    char* decoded_content = decoded_content_base;
    for (size_t j = 0; j < num_kv_pairs; ++j) {
      for (size_t i = 0; i < num_key_columns; ++i) {
        key_content_ptr[i] +=
            key_col_bufs[i]->Decode(key_content_ptr[i], &decoded_content);
      }
      for (size_t i = 0; i < num_value_columns; ++i) {
        value_content_ptr[i] +=
            value_col_bufs[i]->Decode(value_content_ptr[i], &decoded_content);
      }
      checksum_content_ptr +=
          value_checksum_buf->Decode(checksum_content_ptr, &decoded_content);
    }

    size_t offset = decoded_content - decoded_content_base;
    Slice output_content(decoded_content, offset);

    if (out_file != nullptr) {
      out_file->Append(output_content);
    }
  }
  delete[] decoded_content_base;
}

void ColumnAwareEncodingReader::DecodeBlocksFromRowFormat(
    WritableFile* out_file, const std::vector<std::string>* blocks) {
  Options options;
  ImmutableCFOptions ioptions(options);
  for (auto& block : *blocks) {
    auto& slice_final_with_bit = block;
    uint32_t format_version = 2;
    Slice compression_dict;
    BlockContents contents;
    std::string decoded_content;

    CompressionType type =
        (CompressionType)slice_final_with_bit[slice_final_with_bit.size() - 1];
    if (type != kNoCompression) {
      UncompressBlockContents(slice_final_with_bit.c_str(),
                              slice_final_with_bit.size() - 1, &contents,
                              format_version, compression_dict, ioptions);
      decoded_content = std::string(contents.data.data(), contents.data.size());
    } else {
      decoded_content = std::move(slice_final_with_bit);
    }

    if (out_file != nullptr) {
      out_file->Append(decoded_content);
    }
  }
}

void ColumnAwareEncodingReader::DumpDataColumns(
    const std::string& filename,
    const KVPairColDeclarations& kvp_col_declarations,
    const std::vector<KVPairBlock>& kv_pair_blocks) {
  KVPairColBufEncoders kvp_col_bufs(kvp_col_declarations);
  auto& key_col_bufs = kvp_col_bufs.key_col_bufs;
  auto& value_col_bufs = kvp_col_bufs.value_col_bufs;
  auto& value_checksum_buf = kvp_col_bufs.value_checksum_buf;

  FILE* fp = fopen(filename.c_str(), "w");
  size_t block_id = 1;
  for (auto& kv_pairs : kv_pair_blocks) {
    fprintf(fp, "---------------- Block: %-4" ROCKSDB_PRIszt " ----------------\n", block_id);
    for (auto& kv_pair : kv_pairs) {
      const auto& key = kv_pair.first;
      const auto& value = kv_pair.second;
      size_t value_offset = 0;

      const char* key_ptr = key.data();
      for (auto& buf : key_col_bufs) {
        size_t col_size = buf->Append(key_ptr);
        std::string tmp_buf(key_ptr, col_size);
        Slice col(tmp_buf);
        fprintf(fp, "%s ", col.ToString(true).c_str());
        key_ptr += col_size;
      }
      fprintf(fp, "|");

      const char* value_ptr = value.data();
      for (auto& buf : value_col_bufs) {
        size_t col_size = buf->Append(value_ptr);
        std::string tmp_buf(value_ptr, col_size);
        Slice col(tmp_buf);
        fprintf(fp, " %s", col.ToString(true).c_str());
        value_ptr += col_size;
        value_offset += col_size;
      }

      if (value_offset < value.size()) {
        size_t col_size = value_checksum_buf->Append(value_ptr);
        std::string tmp_buf(value_ptr, col_size);
        Slice col(tmp_buf);
        fprintf(fp, "|%s", col.ToString(true).c_str());
      } else {
        value_checksum_buf->Append(nullptr);
      }
      fprintf(fp, "\n");
    }
    block_id++;
  }
  fclose(fp);
}

namespace {

void CompressDataBlock(const std::string& output_content, Slice* slice_final,
                       CompressionType* type, std::string* compressed_output) {
  CompressionOptions compression_opts;
  uint32_t format_version = 2;  // hard-coded version
  Slice compression_dict;
  *slice_final =
      CompressBlock(output_content, compression_opts, type, format_version,
                    compression_dict, compressed_output);
}

}  // namespace

void ColumnAwareEncodingReader::EncodeBlocksToRowFormat(
    WritableFile* out_file, CompressionType compression_type,
    const std::vector<KVPairBlock>& kv_pair_blocks,
    std::vector<std::string>* blocks) {
  std::string output_content;
  for (auto& kv_pairs : kv_pair_blocks) {
    output_content.clear();
    std::string last_key;
    size_t counter = 0;
    const size_t block_restart_interval = 16;
    for (auto& kv_pair : kv_pairs) {
      const auto& key = kv_pair.first;
      const auto& value = kv_pair.second;

      Slice last_key_piece(last_key);
      size_t shared = 0;
      if (counter >= block_restart_interval) {
        counter = 0;
      } else {
        const size_t min_length = std::min(last_key_piece.size(), key.size());
        while ((shared < min_length) && last_key_piece[shared] == key[shared]) {
          shared++;
        }
      }
      const size_t non_shared = key.size() - shared;
      output_content.append(key.c_str() + shared, non_shared);
      output_content.append(value);

      last_key.resize(shared);
      last_key.append(key.data() + shared, non_shared);
      counter++;
    }
    Slice slice_final;
    auto type = compression_type;
    std::string compressed_output;
    CompressDataBlock(output_content, &slice_final, &type, &compressed_output);

    if (out_file != nullptr) {
      out_file->Append(slice_final);
    }

    // Add a bit in the end for decoding
    std::string slice_final_with_bit(slice_final.data(), slice_final.size());
    slice_final_with_bit.append(reinterpret_cast<char*>(&type), 1);
    blocks->push_back(
        std::string(slice_final_with_bit.data(), slice_final_with_bit.size()));
  }
}

Status ColumnAwareEncodingReader::EncodeBlocks(
    const KVPairColDeclarations& kvp_col_declarations, WritableFile* out_file,
    CompressionType compression_type,
    const std::vector<KVPairBlock>& kv_pair_blocks,
    std::vector<std::string>* blocks, bool print_column_stat) {
  std::vector<size_t> key_col_sizes(
      kvp_col_declarations.key_col_declarations->size(), 0);
  std::vector<size_t> value_col_sizes(
      kvp_col_declarations.value_col_declarations->size(), 0);
  size_t value_checksum_size = 0;

  for (auto& kv_pairs : kv_pair_blocks) {
    KVPairColBufEncoders kvp_col_bufs(kvp_col_declarations);
    auto& key_col_bufs = kvp_col_bufs.key_col_bufs;
    auto& value_col_bufs = kvp_col_bufs.value_col_bufs;
    auto& value_checksum_buf = kvp_col_bufs.value_checksum_buf;

    size_t num_kv_pairs = 0;
    for (auto& kv_pair : kv_pairs) {
      const auto& key = kv_pair.first;
      const auto& value = kv_pair.second;
      size_t value_offset = 0;
      num_kv_pairs++;

      const char* key_ptr = key.data();
      for (auto& buf : key_col_bufs) {
        size_t col_size = buf->Append(key_ptr);
        key_ptr += col_size;
      }

      const char* value_ptr = value.data();
      for (auto& buf : value_col_bufs) {
        size_t col_size = buf->Append(value_ptr);
        value_ptr += col_size;
        value_offset += col_size;
      }

      if (value_offset < value.size()) {
        value_checksum_buf->Append(value_ptr);
      } else {
        value_checksum_buf->Append(nullptr);
      }
    }

    kvp_col_bufs.Finish();
    // Get stats
    // Compress and write a block
    if (print_column_stat) {
      for (size_t i = 0; i < key_col_bufs.size(); ++i) {
        Slice slice_final;
        auto type = compression_type;
        std::string compressed_output;
        CompressDataBlock(key_col_bufs[i]->GetData(), &slice_final, &type,
                          &compressed_output);
        out_file->Append(slice_final);
        key_col_sizes[i] += slice_final.size();
      }
      for (size_t i = 0; i < value_col_bufs.size(); ++i) {
        Slice slice_final;
        auto type = compression_type;
        std::string compressed_output;
        CompressDataBlock(value_col_bufs[i]->GetData(), &slice_final, &type,
                          &compressed_output);
        out_file->Append(slice_final);
        value_col_sizes[i] += slice_final.size();
      }
      Slice slice_final;
      auto type = compression_type;
      std::string compressed_output;
      CompressDataBlock(value_checksum_buf->GetData(), &slice_final, &type,
                        &compressed_output);
      out_file->Append(slice_final);
      value_checksum_size += slice_final.size();
    } else {
      std::string output_content;
      // Write column sizes
      PutFixed64(&output_content, num_kv_pairs);
      for (auto& buf : key_col_bufs) {
        size_t size = buf->GetData().size();
        PutFixed64(&output_content, size);
      }
      for (auto& buf : value_col_bufs) {
        size_t size = buf->GetData().size();
        PutFixed64(&output_content, size);
      }
      // Write data
      for (auto& buf : key_col_bufs) {
        output_content.append(buf->GetData());
      }
      for (auto& buf : value_col_bufs) {
        output_content.append(buf->GetData());
      }
      output_content.append(value_checksum_buf->GetData());

      Slice slice_final;
      auto type = compression_type;
      std::string compressed_output;
      CompressDataBlock(output_content, &slice_final, &type,
                        &compressed_output);

      if (out_file != nullptr) {
        out_file->Append(slice_final);
      }

      // Add a bit in the end for decoding
      std::string slice_final_with_bit(slice_final.data(),
                                       slice_final.size() + 1);
      slice_final_with_bit[slice_final.size()] = static_cast<char>(type);
      blocks->push_back(std::string(slice_final_with_bit.data(),
                                    slice_final_with_bit.size()));
    }
  }

  if (print_column_stat) {
    size_t total_size = 0;
    for (size_t i = 0; i < key_col_sizes.size(); ++i)
      total_size += key_col_sizes[i];
    for (size_t i = 0; i < value_col_sizes.size(); ++i)
      total_size += value_col_sizes[i];
    total_size += value_checksum_size;

    for (size_t i = 0; i < key_col_sizes.size(); ++i)
      printf("Key col %" ROCKSDB_PRIszt " size: %" ROCKSDB_PRIszt " percentage %lf%%\n", i, key_col_sizes[i],
             100.0 * key_col_sizes[i] / total_size);
    for (size_t i = 0; i < value_col_sizes.size(); ++i)
      printf("Value col %" ROCKSDB_PRIszt " size: %" ROCKSDB_PRIszt " percentage %lf%%\n", i,
             value_col_sizes[i], 100.0 * value_col_sizes[i] / total_size);
    printf("Value checksum size: %" ROCKSDB_PRIszt " percentage %lf%%\n", value_checksum_size,
           100.0 * value_checksum_size / total_size);
  }
  return Status::OK();
}

void ColumnAwareEncodingReader::GetColDeclarationsPrimary(
    std::vector<ColDeclaration>** key_col_declarations,
    std::vector<ColDeclaration>** value_col_declarations,
    ColDeclaration** value_checksum_declaration) {
  *key_col_declarations = new std::vector<ColDeclaration>{
      ColDeclaration("FixedLength", ColCompressionType::kColRleVarint, 4, false,
                     true),
      ColDeclaration("FixedLength", ColCompressionType::kColRleDeltaVarint, 8,
                     false, true),
      ColDeclaration("FixedLength", ColCompressionType::kColDeltaVarint, 8,
                     false, true),
      ColDeclaration("FixedLength", ColCompressionType::kColDeltaVarint, 8,
                     false, true),
      ColDeclaration("FixedLength", ColCompressionType::kColRleVarint, 8)};

  *value_col_declarations = new std::vector<ColDeclaration>{
      ColDeclaration("FixedLength", ColCompressionType::kColRleVarint, 4),
      ColDeclaration("FixedLength", ColCompressionType::kColRleVarint, 4),
      ColDeclaration("FixedLength", ColCompressionType::kColRle, 1),
      ColDeclaration("VariableLength"),
      ColDeclaration("FixedLength", ColCompressionType::kColDeltaVarint, 4),
      ColDeclaration("FixedLength", ColCompressionType::kColRleVarint, 8)};
  *value_checksum_declaration = new ColDeclaration(
      "LongFixedLength", ColCompressionType::kColNoCompression, 9,
      true /* nullable */);
}

void ColumnAwareEncodingReader::GetColDeclarationsSecondary(
    std::vector<ColDeclaration>** key_col_declarations,
    std::vector<ColDeclaration>** value_col_declarations,
    ColDeclaration** value_checksum_declaration) {
  *key_col_declarations = new std::vector<ColDeclaration>{
      ColDeclaration("FixedLength", ColCompressionType::kColRleVarint, 4, false,
                     true),
      ColDeclaration("FixedLength", ColCompressionType::kColDeltaVarint, 8,
                     false, true),
      ColDeclaration("FixedLength", ColCompressionType::kColRleDeltaVarint, 8,
                     false, true),
      ColDeclaration("FixedLength", ColCompressionType::kColRle, 1),
      ColDeclaration("FixedLength", ColCompressionType::kColDeltaVarint, 4,
                     false, true),
      ColDeclaration("FixedLength", ColCompressionType::kColDeltaVarint, 8,
                     false, true),
      ColDeclaration("FixedLength", ColCompressionType::kColRleVarint, 8, false,
                     true),
      ColDeclaration("VariableChunk", ColCompressionType::kColNoCompression),
      ColDeclaration("FixedLength", ColCompressionType::kColRleVarint, 8)};
  *value_col_declarations = new std::vector<ColDeclaration>();
  *value_checksum_declaration = new ColDeclaration(
      "LongFixedLength", ColCompressionType::kColNoCompression, 9,
      true /* nullable */);
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
