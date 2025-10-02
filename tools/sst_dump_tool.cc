//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "rocksdb/sst_dump_tool.h"

#include <cinttypes>
#include <iostream>

#include "db_stress_tool/db_stress_compression_manager.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/sst_file_dumper.h"

namespace ROCKSDB_NAMESPACE {

namespace {

void print_help(bool to_stderr) {
  std::string supported_compressions;
  for (CompressionType ct : GetSupportedCompressions()) {
    if (!supported_compressions.empty()) {
      supported_compressions += ", ";
    }
    std::string str;
    Status s = GetStringFromCompressionType(&str, ct);
    assert(s.ok());
    supported_compressions += str;
  }
  fprintf(
      to_stderr ? stderr : stdout,
      R"(sst_dump <db_dirs_OR_sst_files...> [--command=check|scan|raw|recompress|identify]
    --file=<db_dir_OR_sst_file>
      Path to SST file or directory containing SST files (old option syntax)

    --env_uri=<uri of underlying Env>
      URI of underlying Env, mutually exclusive with fs_uri

    --fs_uri=<uri of underlying FileSystem>
      URI of underlying FileSystem, mutually exclusive with env_uri

    --command=check|scan|raw|verify|identify
        check: Iterate over entries in files but don't print anything except if an error is encountered (default command)
               When read_num, from and to are not set, it compares the number of keys read with num_entries in table
               property and will report corruption if there is a mismatch.
        scan: Iterate over entries in files and print them to screen
        raw: Dump all the table contents to <file_name>_dump.txt
        verify: Iterate all the blocks in files verifying checksum to detect possible corruption but don't print anything except if a corruption is encountered
        recompress: reports the SST file size if recompressed with different
                    compression types
        identify: Reports a file is a valid SST file or lists all valid SST files under a directory

    --output_hex
      Can be combined with scan command to print the keys and values in Hex

    --decode_blob_index
      Decode blob indexes and print them in a human-readable format during scans.

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
      check|scan|raw|identify

    --block_size=<block_size>
      Can be combined with --command=recompress to set the block size that will
      be used when trying different compression algorithms

    --compression_types=<comma-separated list of CompressionType members, e.g.,
      kSnappyCompression or kCustomCompressionC4>
      Can be combined with --command=recompress to run recompression for this
      list of compression types
      Supported built-in compression types: %s

    --compression_manager=<compression manager string>
      Used with --command=recompress to specify a compression manager to use
      instead of the built-in compression manager, which may support a
      different set of compression types.

    --parse_internal_key=<0xKEY>
      Convenience option to parse an internal key on the command line. Dumps the
      internal key in hex format {'key' @ SN: type}

    --compression_level=<compression_level>
      Sets both --compression_level_from= and --compression_level_to=

    --compression_level_from=<compression_level>
      Compression level to start compressing when executing recompress. One compression type
      and compression_level_to must also be specified

    --compression_level_to=<compression_level>
      Compression level to stop compressing when executing recompress. One compression type
      and compression_level_from must also be specified

    --compression_max_dict_buffer_bytes=<int64_t>
      Limit on buffer size from which we collect samples for dictionary generation.

    --compression_max_dict_bytes=<uint32_t>
      Maximum size of dictionary used to prime the compression library

    --compression_parallel_threads=<uint32_t>
      Number of parallel threads to use with --command=recompress

    --compression_use_zstd_finalize_dict
      Use zstd's finalizeDictionary() API instead of zstd's dictionary trainer to generate dictionary.

    --compression_zstd_max_train_bytes=<uint32_t>
      Maximum size of training data passed to zstd's dictionary trainer

    --list_meta_blocks
      Print the list of all meta blocks in the file
)",
      supported_compressions.c_str());
}

// arg_name would include all prefix, e.g. "--my_arg="
// arg_val is the parses value.
// True if there is a match. False otherwise.
// Woud exit after printing errmsg if cannot be parsed.
bool ParseIntArg(const char* arg, const std::string arg_name,
                 const std::string err_msg, int64_t* arg_val) {
  if (strncmp(arg, arg_name.c_str(), arg_name.size()) == 0) {
    std::string input_str = arg + arg_name.size();
    std::istringstream iss(input_str);
    iss >> *arg_val;
    if (iss.fail()) {
      fprintf(stderr, "%s\n", err_msg.c_str());
      exit(1);
    }
    return true;
  }
  return false;
}
}  // namespace

int SSTDumpTool::Run(int argc, char const* const* argv, Options options) {
  std::string env_uri, fs_uri;
  enum DirVsFile {
    kUnknownDirVsFile,
    kDir,
    kFile,
  };
  std::vector<std::pair<const char*, DirVsFile>> dirs_or_files;
  uint64_t read_num = std::numeric_limits<uint64_t>::max();
  std::string command;

  char junk;
  uint64_t n;
  bool verify_checksum = false;
  bool output_hex = false;
  bool decode_blob_index = false;
  bool input_key_hex = false;
  bool has_from = false;
  bool has_to = false;
  bool use_from_as_prefix = false;
  bool show_properties = false;
  bool show_summary = false;
  bool list_meta_blocks = false;
  bool has_compression_level_from = false;
  bool has_compression_level_to = false;
  std::string from_key;
  std::string to_key;
  std::string block_size_str;
  std::string compression_level_from_str;
  std::string compression_level_to_str;
  size_t block_size = 16384;  // A popular choice for default
  size_t readahead_size = 2 * 1024 * 1024;
  std::vector<CompressionType> compression_types;
  std::shared_ptr<CompressionManager> compression_manager;
  uint64_t total_num_files = 0;
  uint64_t total_num_data_blocks = 0;
  uint64_t total_data_block_size = 0;
  uint64_t total_index_block_size = 0;
  uint64_t total_filter_block_size = 0;
  int32_t compress_level_from = CompressionOptions::kDefaultCompressionLevel;
  int32_t compress_level_to = CompressionOptions::kDefaultCompressionLevel;
  uint32_t compression_max_dict_bytes =
      ROCKSDB_NAMESPACE::CompressionOptions().max_dict_bytes;
  uint32_t compression_zstd_max_train_bytes =
      ROCKSDB_NAMESPACE::CompressionOptions().zstd_max_train_bytes;
  uint64_t compression_max_dict_buffer_bytes =
      ROCKSDB_NAMESPACE::CompressionOptions().max_dict_buffer_bytes;
  bool compression_use_zstd_finalize_dict =
      !ROCKSDB_NAMESPACE::CompressionOptions().use_zstd_dict_trainer;
  uint32_t compression_parallel_threads = 1;

  int64_t tmp_val;

  TEST_AllowUnsupportedFormatVersion() = true;
  DbStressCustomCompressionManager::Register();

  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--env_uri=", 10) == 0) {
      env_uri = argv[i] + 10;
    } else if (strncmp(argv[i], "--fs_uri=", 9) == 0) {
      fs_uri = argv[i] + 9;
    } else if (strncmp(argv[i], "--file=", 7) == 0) {
      dirs_or_files.emplace_back(argv[i] + 7, kUnknownDirVsFile);
    } else if (strcmp(argv[i], "--output_hex") == 0) {
      output_hex = true;
    } else if (strcmp(argv[i], "--decode_blob_index") == 0) {
      decode_blob_index = true;
    } else if (strcmp(argv[i], "--input_key_hex") == 0) {
      input_key_hex = true;
    } else if (sscanf(argv[i], "--read_num=%lu%c", (unsigned long*)&n, &junk) ==
               1) {
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
    } else if (ParseIntArg(argv[i], "--set_block_size=",
                           "block size must be numeric", &tmp_val) ||
               ParseIntArg(argv[i], "--block_size=",
                           "block size must be numeric", &tmp_val)) {
      block_size = static_cast<size_t>(tmp_val);
    } else if (ParseIntArg(argv[i], "--readahead_size=",
                           "readahead_size must be numeric", &tmp_val)) {
      readahead_size = static_cast<size_t>(tmp_val);
    } else if (strncmp(argv[i], "--compression_types=", 20) == 0) {
      std::string compression_types_csv = argv[i] + 20;
      std::istringstream iss(compression_types_csv);
      std::string compression_type;

      while (std::getline(iss, compression_type, ',')) {
        auto iter =
            OptionsHelper::compression_type_string_map.find(compression_type);
        if (iter == OptionsHelper::compression_type_string_map.end()) {
          fprintf(stderr, "%s is not a valid CompressionType\n",
                  compression_type.c_str());
          exit(1);
        }
        compression_types.emplace_back(iter->second);
      }
    } else if (strncmp(argv[i], "--compression_manager=", 22) == 0) {
      std::string compression_manager_str = argv[i] + 22;
      ConfigOptions config_options;
      config_options.ignore_unsupported_options = false;
      Status s = CompressionManager::CreateFromString(
          config_options, compression_manager_str, &compression_manager);
      if (!s.ok()) {
        fprintf(stderr, "Failed to create compression manager: %s\n",
                s.ToString().c_str());
        exit(1);
      }
      if (compression_manager == nullptr) {
        fprintf(stderr, "No compression manager created: %s\n",
                compression_manager_str.c_str());
        exit(1);
      }
      options.compression_manager = compression_manager;
      printf("Using compression manager: %s\n",
             compression_manager->GetId().c_str());
    } else if (strncmp(argv[i], "--parse_internal_key=", 21) == 0) {
      std::string in_key(argv[i] + 21);
      try {
        in_key = ROCKSDB_NAMESPACE::LDBCommand::HexToString(in_key);
      } catch (...) {
        std::cerr << "ERROR: Invalid key input '" << in_key
                  << "' Use 0x{hex representation of internal rocksdb key}"
                  << std::endl;
        return -1;
      }
      Slice sl_key = ROCKSDB_NAMESPACE::Slice(in_key);
      ParsedInternalKey ikey;
      int retc = 0;
      Status pik_status =
          ParseInternalKey(sl_key, &ikey, true /* log_err_key */);
      if (!pik_status.ok()) {
        std::cerr << pik_status.getState() << "\n";
        retc = -1;
      }
      fprintf(stdout, "key=%s\n", ikey.DebugString(true, true).c_str());
      return retc;
    } else if (ParseIntArg(argv[i], "--compression_level=",
                           "compression_level must be numeric", &tmp_val)) {
      has_compression_level_from = true;
      has_compression_level_to = true;
      compress_level_from = static_cast<int>(tmp_val);
      compress_level_to = static_cast<int>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_level_from=",
                           "compression_level_from must be numeric",
                           &tmp_val)) {
      has_compression_level_from = true;
      compress_level_from = static_cast<int>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_level_to=",
                           "compression_level_to must be numeric", &tmp_val)) {
      has_compression_level_to = true;
      compress_level_to = static_cast<int>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_max_dict_bytes=",
                           "compression_max_dict_bytes must be numeric",
                           &tmp_val)) {
      if (tmp_val < 0 || tmp_val > std::numeric_limits<uint32_t>::max()) {
        fprintf(stderr, "compression_max_dict_bytes must be a uint32_t: '%s'\n",
                argv[i]);
        print_help(/*to_stderr*/ true);
        return 1;
      }
      compression_max_dict_bytes = static_cast<uint32_t>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_parallel_threads=",
                           "compression_parallel_threads must be numeric",
                           &tmp_val)) {
      if (tmp_val < 0 || tmp_val > 100) {
        fprintf(stderr, "compression_parallel_threads out of range: '%s'\n",
                argv[i]);
        print_help(/*to_stderr*/ true);
        return 1;
      }
      compression_parallel_threads = static_cast<uint32_t>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_zstd_max_train_bytes=",
                           "compression_zstd_max_train_bytes must be numeric",
                           &tmp_val)) {
      if (tmp_val < 0 || tmp_val > std::numeric_limits<uint32_t>::max()) {
        fprintf(stderr,
                "compression_zstd_max_train_bytes must be a uint32_t: '%s'\n",
                argv[i]);
        print_help(/*to_stderr*/ true);
        return 1;
      }
      compression_zstd_max_train_bytes = static_cast<uint32_t>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_max_dict_buffer_bytes=",
                           "compression_max_dict_buffer_bytes must be numeric",
                           &tmp_val)) {
      if (tmp_val < 0) {
        fprintf(stderr,
                "compression_max_dict_buffer_bytes must be positive: '%s'\n",
                argv[i]);
        print_help(/*to_stderr*/ true);
        return 1;
      }
      compression_max_dict_buffer_bytes = static_cast<uint64_t>(tmp_val);
    } else if (strcmp(argv[i], "--compression_use_zstd_finalize_dict") == 0) {
      compression_use_zstd_finalize_dict = true;
    } else if (strcmp(argv[i], "--list_meta_blocks") == 0) {
      list_meta_blocks = true;
    } else if (strcmp(argv[i], "--help") == 0) {
      print_help(/*to_stderr*/ false);
      return 0;
    } else if (strcmp(argv[i], "--version") == 0) {
      printf("%s\n", GetRocksBuildInfoAsString("sst_dump").c_str());
      return 0;
    } else if (strcmp(argv[i], "--") == 0) {
      // Remaining args are dir-or-file
      for (++i; i < argc; ++i) {
        dirs_or_files.emplace_back(argv[i], kUnknownDirVsFile);
      }
    } else if (argv[i][0] == '-') {
      fprintf(stderr, "Unrecognized argument '%s'\n\n", argv[i]);
      print_help(/*to_stderr*/ true);
      return 1;
    } else {
      // Dir-or-file arg
      dirs_or_files.emplace_back(argv[i], kUnknownDirVsFile);
    }
  }

  if (has_compression_level_from ^ has_compression_level_to) {
    fprintf(stderr,
            "Specify both --compression_level_from and "
            "--compression_level_to.\n\n");
    exit(1);
  }

  if (use_from_as_prefix && has_from) {
    fprintf(stderr, "Cannot specify --prefix and --from\n\n");
    exit(1);
  }

  if (input_key_hex) {
    if (has_from || use_from_as_prefix) {
      from_key = ROCKSDB_NAMESPACE::LDBCommand::HexToString(from_key);
    }
    if (has_to) {
      to_key = ROCKSDB_NAMESPACE::LDBCommand::HexToString(to_key);
    }
  }

  if (dirs_or_files.empty()) {
    fprintf(stderr, "file or directory must be specified.\n\n");
    print_help(/*to_stderr*/ true);
    exit(1);
  }

  std::shared_ptr<ROCKSDB_NAMESPACE::Env> env_guard;

  // If caller of SSTDumpTool::Run(...) does not specify a different env other
  // than Env::Default(), then try to load custom env based on env_uri/fs_uri.
  // Otherwise, the caller is responsible for creating custom env.
  {
    ConfigOptions config_options;
    config_options.env = options.env;
    Status s = Env::CreateFromUri(config_options, env_uri, fs_uri, &options.env,
                                  &env_guard);
    if (!s.ok()) {
      fprintf(stderr, "CreateEnvFromUri: %s\n", s.ToString().c_str());
      exit(1);
    } else {
      fprintf(stdout, "options.env is %p\n", options.env);
    }
  }

  std::vector<std::string> filenames;
  ROCKSDB_NAMESPACE::Env* env = options.env;
  ROCKSDB_NAMESPACE::Status st;

  for (size_t i = 0; i < dirs_or_files.size(); ++i) {
    auto dir_or_file = dirs_or_files[i].first;
    std::vector<std::string> children;
    st = env->GetChildren(dirs_or_files[i].first, &children);
    if (!st.ok() || children.empty()) {
      // dir_or_file does not exist or does not contain children
      // Check its existence first
      Status s = env->FileExists(dir_or_file);
      // dir_or_file does not exist
      if (!s.ok()) {
        fprintf(stderr, "%s%s: No such file or directory\n",
                s.ToString().c_str(), dir_or_file);
        return 1;
      }
      // dir_or_file exists and is treated as a "file"
      // since it has no children
      // This is ok since later it will be checked
      // that whether it is a valid sst or not
      // (A directory "file" is not a valid sst)
      filenames.emplace_back(dir_or_file);
      dirs_or_files[i].second = kFile;
    } else {
      for (auto& child : children) {
        filenames.push_back(std::string{dir_or_file} + "/" + child);
      }
      dirs_or_files[i].second = kDir;
    }
  }

  uint64_t total_read = 0;
  // List of RocksDB SST file without corruption
  std::vector<std::string> valid_sst_files;
  for (size_t i = 0; i < filenames.size(); i++) {
    std::string filename = filenames.at(i);
    if (filename.length() <= 4 ||
        filename.rfind(".sst") != filename.length() - 4) {
      // ignore
      continue;
    }

    if (command == "verify") {
      verify_checksum = true;
    }

    // Update options for when simulating writing a table file
    {
      BlockBasedTableOptions bbto;
      if (options.table_factory->IsInstanceOf(
              TableFactory::kBlockBasedTableName()) &&
          options.table_factory->GetOptions<BlockBasedTableOptions>()) {
        bbto = *options.table_factory->GetOptions<BlockBasedTableOptions>();
      }
      bbto.block_size = block_size;
      // Maximize compression features available
      bbto.format_version = kLatestFormatVersion;
      options.table_factory = std::make_shared<BlockBasedTableFactory>(bbto);
    }
    options.compression_opts.max_dict_bytes = compression_max_dict_bytes;
    options.compression_opts.zstd_max_train_bytes =
        compression_zstd_max_train_bytes;
    options.compression_opts.max_dict_buffer_bytes =
        compression_max_dict_buffer_bytes;
    options.compression_opts.use_zstd_dict_trainer =
        !compression_use_zstd_finalize_dict;
    options.compression_opts.parallel_threads = compression_parallel_threads;

    ROCKSDB_NAMESPACE::SstFileDumper dumper(
        options, filename, Temperature::kUnknown, readahead_size,
        verify_checksum, output_hex, decode_blob_index);

    // Not a valid SST
    if (!dumper.getStatus().ok()) {
      fprintf(stderr, "%s: %s\n", filename.c_str(),
              dumper.getStatus().ToString().c_str());
      continue;
    } else {
      valid_sst_files.push_back(filename);
      // Print out from and to key information once
      // where there is at least one valid SST
      if (valid_sst_files.size() == 1) {
        // from_key and to_key are only used for "check", "scan", or ""
        if (command == "check" || command == "scan" || command == "") {
          fprintf(stdout, "from [%s] to [%s]\n",
                  ROCKSDB_NAMESPACE::Slice(from_key).ToString(true).c_str(),
                  ROCKSDB_NAMESPACE::Slice(to_key).ToString(true).c_str());
        }
      }
    }

    if (command == "recompress") {
      if (compression_types.empty()) {
        if (options.compression_manager != nullptr) {
          for (int c = 0; c < kDisableCompressionOption; ++c) {
            if (options.compression_manager->SupportsCompressionType(
                    static_cast<CompressionType>(c))) {
              compression_types.emplace_back(static_cast<CompressionType>(c));
            }
          }
        } else {
          compression_types = GetSupportedCompressions();
        }
      }
      st = dumper.ShowAllCompressionSizes(
          compression_types, compress_level_from, compress_level_to);
      if (!st.ok()) {
        fprintf(stderr, "Failed to recompress: %s\n", st.ToString().c_str());
        exit(1);
      }
      continue;
    }

    if (command == "raw") {
      std::string out_filename = filename.substr(0, filename.length() - 4);
      out_filename.append("_dump.txt");

      st = dumper.DumpTable(out_filename);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
        exit(1);
      } else {
        fprintf(stdout, "raw dump written to file %s\n", out_filename.data());
      }
      continue;
    }

    // scan all files in give file path.
    if (command == "" || command == "scan" || command == "check") {
      st = dumper.ReadSequential(
          command == "scan", read_num > 0 ? (read_num - total_read) : read_num,
          has_from || use_from_as_prefix, from_key, has_to, to_key,
          use_from_as_prefix);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
      }
      total_read += dumper.GetReadNumber();
      if (read_num > 0 && total_read > read_num) {
        break;
      }
    }

    if (command == "verify") {
      st = dumper.VerifyChecksum();
      if (!st.ok()) {
        fprintf(stderr, "%s is corrupted: %s\n", filename.c_str(),
                st.ToString().c_str());
      } else {
        fprintf(stdout, "The file is ok\n");
      }
      continue;
    }

    if (show_properties || show_summary) {
      const ROCKSDB_NAMESPACE::TableProperties* table_properties;

      std::shared_ptr<const ROCKSDB_NAMESPACE::TableProperties>
          table_properties_from_reader;
      st = dumper.ReadTableProperties(&table_properties_from_reader);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
        fprintf(stderr, "Try to use initial table properties\n");
        table_properties = dumper.GetInitTableProperties();
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
        }
        total_num_files += 1;
        total_num_data_blocks += table_properties->num_data_blocks;
        total_data_block_size += table_properties->data_size;
        total_index_block_size += table_properties->index_size;
        total_filter_block_size += table_properties->filter_size;
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
      } else {
        fprintf(stderr, "Reader unexpectedly returned null properties\n");
      }
    }

    BlockContents& meta_index_contents = dumper.GetMetaIndexContents();
    if (list_meta_blocks && meta_index_contents.data.size() > 0) {
      Block meta_index_block(std::move(meta_index_contents));
      std::unique_ptr<MetaBlockIter> meta_index_iter;
      meta_index_iter.reset(meta_index_block.NewMetaIterator());
      meta_index_iter->SeekToFirst();
      fprintf(stdout,
              "Meta Blocks:\n"
              "------------------------------\n");
      while (meta_index_iter->status().ok() && meta_index_iter->Valid()) {
        Slice v = meta_index_iter->value();
        BlockHandle handle;
        st = handle.DecodeFrom(&v);
        if (!st.ok()) {
          fprintf(stderr, "%s: Could not decode block handle - %s\n",
                  filename.c_str(), st.ToString().c_str());
        } else {
          fprintf(stdout, "  %s: %" PRIu64 " %" PRIu64 "\n",
                  meta_index_iter->key().ToString().c_str(), handle.offset(),
                  handle.size());
        }
        meta_index_iter->Next();
      }
    } else if (list_meta_blocks) {
      fprintf(stderr, "Could not read the meta index block\n");
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

  if (valid_sst_files.empty()) {
    // No valid SST files are found
    // Exit with an error state
    for (auto& e : dirs_or_files) {
      if (e.second == kDir) {
        fprintf(stdout, "------------------------------\n");
        fprintf(stderr, "No valid SST files found in %s\n", e.first);
      } else {
        assert(e.second == kFile);
        fprintf(stderr, "%s is not a valid SST file\n", e.first);
      }
    }
    return 1;
  } else {
    assert(!dirs_or_files.empty());
    if (command == "identify") {
      if (dirs_or_files.size() > 1 || dirs_or_files[0].second == kDir) {
        fprintf(stdout, "------------------------------\n");
        std::string single_dir_msg;
        if (dirs_or_files.size() == 1) {
          single_dir_msg += " found in ";
          single_dir_msg += dirs_or_files[0].first;
        }
        fprintf(stdout, "List of valid SST files%s:\n", single_dir_msg.c_str());
        for (const auto& f : valid_sst_files) {
          fprintf(stdout, "%s\n", f.c_str());
        }
        fprintf(stdout, "Number of valid SST files: %zu\n",
                valid_sst_files.size());
      } else {
        fprintf(stdout, "%s is a valid SST file\n", dirs_or_files[0].first);
      }
    }
    // At least one valid SST
    // exit with a success state
    return 0;
  }
}
}  // namespace ROCKSDB_NAMESPACE
