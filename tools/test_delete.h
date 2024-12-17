#pragma once
#include "rocksdb/range_delete_db.hpp"
#include <gflags/gflags.h>
#include <boost/dynamic_bitset.hpp>

DEFINE_string(db_path, "/home/wangfan/delete/global-range-delete/build/testdb", "dbpath");
DEFINE_string(mode, "default", "methods: default or grd");
//LSM
DEFINE_int32(buffer_size, 64, "Buffer size in MB");
DEFINE_int32(size_ratio, 10, "size_ratio");
DEFINE_int32(bpk_filter, 5, "Bits per key for rocksdb bloom filter");
//GRD
DEFINE_uint64(max_key, 10000000, "the upper bound of key space");
DEFINE_int32(bpk_rd_filter, 10, "Bits per key for range delete filter");
DEFINE_int32(rep_buffer_size, 64, "LSM RTree Buffer size in KB");
DEFINE_int32(rep_size_ratio, 10, "LSM RTree size_ratio");
DEFINE_int32(ksize, 24, "Size of key-value pair");
DEFINE_int32(kvsize, 128, "Size of key-value pair");
//Workload
DEFINE_string(workload, "prepare", "prepare or test");
DEFINE_uint64(prep_num, 100000, "#entries to prepare");
DEFINE_uint64(write_num, 100000, "#write operations");
DEFINE_uint64(read_num, 100000, "#read operations");
DEFINE_uint64(seek_num, 100000, "#range query operations");
DEFINE_uint64(seek_len, 10, "length of range query");
DEFINE_uint64(rdelete_num, 100000, "#range delete operations");
DEFINE_uint64(rdelete_len, 10, "length of delete range");

rocksdb::range_delete_db_opt get_default_options() {
  rocksdb::Options db_opts;
  db_opts.create_if_missing = true;
  db_opts.write_buffer_size = FLAGS_buffer_size << 20;  // MB
  // db_opts.target_file_size_base = 1024 * 1024 * 1024;
  db_opts.target_file_size_multiplier = FLAGS_size_ratio;
  db_opts.max_bytes_for_level_multiplier = FLAGS_size_ratio;
  db_opts.level_compaction_dynamic_level_bytes = false;
  db_opts.max_bytes_for_level_base = db_opts.write_buffer_size * db_opts.max_bytes_for_level_multiplier;
  db_opts.use_direct_reads = true;
  db_opts.use_direct_io_for_flush_and_compaction = true;
  auto table_options =
      db_opts.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->no_block_cache = true;
  // db_opts.statistics = rocksdb::CreateDBStatistics();

  rangedelete_filter::rd_filter_opt filter_opts;
  filter_opts.bit_per_key = FLAGS_bpk_rd_filter;
  filter_opts.num_keys = FLAGS_max_key / 100;
  filter_opts.num_blocks = filter_opts.num_keys / 10000  * FLAGS_bpk_rd_filter;
  filter_opts.min_key = 0;
  filter_opts.max_key = FLAGS_max_key;

  rangedelete_rep::rd_rep_opt rep_opts;
  rep_opts.buffer_cap = (FLAGS_rep_buffer_size << 10) / sizeof(rangedelete_rep::Rectangle); //size is 32
  rep_opts.T = FLAGS_rep_size_ratio;
  rep_opts.path = FLAGS_db_path + "/";

  rocksdb::range_delete_db_opt options;
  options.enable_global_rd = false;
  options.db_path = FLAGS_db_path;
  options.db_conf = db_opts;
  options.filter_conf = filter_opts;
  options.rep_conf = rep_opts;

  return options;
}
