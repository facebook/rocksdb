//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "rocksdb/sst_dump_tool.h"
#include "rocksdb/sst_file_reader.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "table/block.h"
#include "table/block_based_table_builder.h"
#include "table/block_based_table_factory.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/plain_table_factory.h"
#include "table/table_reader.h"
#include "util/compression.h"
#include "util/random.h"

#include "port/port.h"

namespace rocksdb {

static const std::vector<std::pair<CompressionType, const char*>>
    kCompressions = {
        {CompressionType::kNoCompression, "kNoCompression"},
        {CompressionType::kSnappyCompression, "kSnappyCompression"},
        {CompressionType::kZlibCompression, "kZlibCompression"},
        {CompressionType::kBZip2Compression, "kBZip2Compression"},
        {CompressionType::kLZ4Compression, "kLZ4Compression"},
        {CompressionType::kLZ4HCCompression, "kLZ4HCCompression"},
        {CompressionType::kXpressCompression, "kXpressCompression"},
        {CompressionType::kZSTD, "kZSTD"}};

namespace {

void print_help() {
  fprintf(stderr,
          R"(sst_dump --file=<data_dir_OR_sst_file> [--command=check|scan|raw]
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
)");
}

}  // namespace

int SSTDumpTool::Run(int argc, char** argv) {
  const char* dir_or_file = nullptr;
  uint64_t read_num = std::numeric_limits<uint64_t>::max();
  std::string command;

  char junk;
  uint64_t n;
  bool verify_checksum = false;
  bool output_hex = false;
  bool input_key_hex = false;
  bool has_from = false;
  bool has_to = false;
  bool use_from_as_prefix = false;
  bool show_properties = false;
  bool show_summary = false;
  bool set_block_size = false;
  std::string from_key;
  std::string to_key;
  std::string block_size_str;
  size_t block_size = 0;
  std::vector<std::pair<CompressionType, const char*>> compression_types;
  uint64_t total_num_files = 0;
  uint64_t total_num_data_blocks = 0;
  uint64_t total_data_block_size = 0;
  uint64_t total_index_block_size = 0;
  uint64_t total_filter_block_size = 0;
  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--file=", 7) == 0) {
      dir_or_file = argv[i] + 7;
    } else if (strcmp(argv[i], "--output_hex") == 0) {
      output_hex = true;
    } else if (strcmp(argv[i], "--input_key_hex") == 0) {
      input_key_hex = true;
    } else if (sscanf(argv[i],
               "--read_num=%lu%c",
               (unsigned long*)&n, &junk) == 1) {
      read_num = n;
    } else if (strcmp(argv[i], "--verify_checksum") == 0) {
      verify_checksum = true;
    } else if (strncmp(argv[i], "--command=", 10) == 0) {
      command = argv[i] + 10;
    } else if (strncmp(argv[i], "--from=", 7) == 0) {
      from_key = argv[i] + 7;
      has_from = true;
    } else if (strncmp(argv[i], "--to=", 5) == 0) {
      to_key = argv[i] + 5;
      has_to = true;
    } else if (strncmp(argv[i], "--prefix=", 9) == 0) {
      from_key = argv[i] + 9;
      use_from_as_prefix = true;
    } else if (strcmp(argv[i], "--show_properties") == 0) {
      show_properties = true;
    } else if (strcmp(argv[i], "--show_summary") == 0) {
      show_summary = true;
    } else if (strncmp(argv[i], "--set_block_size=", 17) == 0) {
      set_block_size = true;
      block_size_str = argv[i] + 17;
      std::istringstream iss(block_size_str);
      iss >> block_size;
      if (iss.fail()) {
        fprintf(stderr, "block size must be numeric\n");
        exit(1);
      }
    } else if (strncmp(argv[i], "--compression_types=", 20) == 0) {
      std::string compression_types_csv = argv[i] + 20;
      std::istringstream iss(compression_types_csv);
      std::string compression_type;
      while (std::getline(iss, compression_type, ',')) {
        auto iter = std::find_if(
            kCompressions.begin(), kCompressions.end(),
            [&compression_type](std::pair<CompressionType, const char*> curr) {
              return curr.second == compression_type;
            });
        if (iter == kCompressions.end()) {
          fprintf(stderr, "%s is not a valid CompressionType\n",
                  compression_type.c_str());
          exit(1);
        }
        compression_types.emplace_back(*iter);
      }
    } else if (strncmp(argv[i], "--parse_internal_key=", 21) == 0) {
      std::string in_key(argv[i] + 21);
      try {
        in_key = rocksdb::LDBCommand::HexToString(in_key);
      } catch (...) {
        std::cerr << "ERROR: Invalid key input '"
          << in_key
          << "' Use 0x{hex representation of internal rocksdb key}" << std::endl;
        return -1;
      }
      Slice sl_key = rocksdb::Slice(in_key);
      ParsedInternalKey ikey;
      int retc = 0;
      if (!ParseInternalKey(sl_key, &ikey)) {
        std::cerr << "Internal Key [" << sl_key.ToString(true /* in hex*/)
                  << "] parse error!\n";
        retc = -1;
      }
      fprintf(stdout, "key=%s\n", ikey.DebugString(true).c_str());
      return retc;
    } else {
      fprintf(stderr, "Unrecognized argument '%s'\n\n", argv[i]);
      print_help();
      exit(1);
    }
  }

  if (use_from_as_prefix && has_from) {
    fprintf(stderr, "Cannot specify --prefix and --from\n\n");
    exit(1);
  }

  if (input_key_hex) {
    if (has_from || use_from_as_prefix) {
      from_key = rocksdb::LDBCommand::HexToString(from_key);
    }
    if (has_to) {
      to_key = rocksdb::LDBCommand::HexToString(to_key);
    }
  }

  if (dir_or_file == nullptr) {
    fprintf(stderr, "file or directory must be specified.\n\n");
    print_help();
    exit(1);
  }

  std::vector<std::string> filenames;
  rocksdb::Env* env = rocksdb::Env::Default();
  rocksdb::Status st = env->GetChildren(dir_or_file, &filenames);
  bool dir = true;
  if (!st.ok()) {
    filenames.clear();
    filenames.push_back(dir_or_file);
    dir = false;
  }

  fprintf(stdout, "from [%s] to [%s]\n",
      rocksdb::Slice(from_key).ToString(true).c_str(),
      rocksdb::Slice(to_key).ToString(true).c_str());

  uint64_t total_read = 0;
  for (size_t i = 0; i < filenames.size(); i++) {
    std::string filename = filenames.at(i);
    if (filename.length() <= 4 ||
        filename.rfind(".sst") != filename.length() - 4) {
      // ignore
      continue;
    }
    if (dir) {
      filename = std::string(dir_or_file) + "/" + filename;
    }

    rocksdb::SstFileReader reader(
        filename, verify_checksum,
        (command == "scan" ? rocksdb::DefaultKvHandler(output_hex) : false),
        rocksdb::DefaultInfoHandler(), rocksdb::DefaultErrHandler());
    if (!reader.getStatus().ok()) {
      fprintf(stderr, "%s: %s\n", filename.c_str(),
              reader.getStatus().ToString().c_str());
      continue;
    }

    if (command == "recompress") {
      reader.ShowAllCompressionSizes(
          set_block_size ? block_size : 16384,
          compression_types.empty() ? kCompressions : compression_types);
      return 0;
    }

    if (command == "raw") {
      std::string out_filename = filename.substr(0, filename.length() - 4);
      out_filename.append("_dump.txt");

      st = reader.DumpTable(out_filename);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
        exit(1);
      } else {
        fprintf(stdout, "raw dump written to file %s\n", &out_filename[0]);
      }
      continue;
    }

    // scan all files in give file path.
    if (command == "" || command == "scan" || command == "check") {
      st = reader.ReadSequential(
          read_num > 0 ? (read_num - total_read) : read_num,
          has_from || use_from_as_prefix, from_key, has_to, to_key,
          use_from_as_prefix);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(),
            st.ToString().c_str());
      }
      total_read += reader.GetReadNumber();
      if (read_num > 0 && total_read > read_num) {
        break;
      }
    }

    if (command == "verify") {
      st = reader.VerifyChecksum();
      if (!st.ok()) {
        fprintf(stderr, "%s is corrupted: %s\n", filename.c_str(),
                st.ToString().c_str());
      } else {
        fprintf(stdout, "The file is ok\n");
      }
      continue;
    }

    if (show_properties || show_summary) {
      const rocksdb::TableProperties* table_properties;

      std::shared_ptr<const rocksdb::TableProperties>
          table_properties_from_reader;
      st = reader.ReadTableProperties(&table_properties_from_reader);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
        fprintf(stderr, "Try to use initial table properties\n");
        table_properties = reader.GetInitTableProperties();
      } else {
        table_properties = table_properties_from_reader.get();
      }
      if (table_properties != nullptr) {
        if (show_properties) {
          fprintf(stdout,
                  "Table Properties:\n"
                  "------------------------------\n"
                  "  %s",
                  table_properties->ToString("\n  ", ": ").c_str());
          fprintf(stdout, "# deleted keys: %" PRIu64 "\n",
                  rocksdb::GetDeletedKeys(
                      table_properties->user_collected_properties));

          bool property_present;
          uint64_t merge_operands = rocksdb::GetMergeOperands(
              table_properties->user_collected_properties, &property_present);
          if (property_present) {
            fprintf(stdout, "  # merge operands: %" PRIu64 "\n",
                    merge_operands);
          } else {
            fprintf(stdout, "  # merge operands: UNKNOWN\n");
          }
        }
        total_num_files += 1;
        total_num_data_blocks += table_properties->num_data_blocks;
        total_data_block_size += table_properties->data_size;
        total_index_block_size += table_properties->index_size;
        total_filter_block_size += table_properties->filter_size;
      }
      if (show_properties) {
        fprintf(stdout,
                "Raw user collected properties\n"
                "------------------------------\n");
        for (const auto& kv : table_properties->user_collected_properties) {
          std::string prop_name = kv.first;
          std::string prop_val = Slice(kv.second).ToString(true);
          fprintf(stdout, "  # %s: 0x%s\n", prop_name.c_str(),
                  prop_val.c_str());
        }
      }
    }
  }
  if (show_summary) {
    fprintf(stdout, "total number of files: %" PRIu64 "\n", total_num_files);
    fprintf(stdout, "total number of data blocks: %" PRIu64 "\n",
            total_num_data_blocks);
    fprintf(stdout, "total data block size: %" PRIu64 "\n",
            total_data_block_size);
    fprintf(stdout, "total index block size: %" PRIu64 "\n",
            total_index_block_size);
    fprintf(stdout, "total filter block size: %" PRIu64 "\n",
            total_filter_block_size);
  }
  return 0;
}
}  // namespace rocksdb

#endif  // ROCKSDB_LITE
