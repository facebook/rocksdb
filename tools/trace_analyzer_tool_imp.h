//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/trace_analyzer_tool.h"

#include <stdio.h>
#include <fstream>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/write_batch.h"
#include "util/trace_replay.h"

namespace rocksdb {

class DBImpl;
class WriteBatch;
class AnalyzerOptions;
class TraceWriteHandler;

const int taTypeNum = 6;

enum TraceOperationType : uint32_t {
  taGet = 0,
  taPut = 1,
  taDelete = 2,
  taSingleDelete = 3,
  taRangeDelete = 4,
  taMerge = 5
};

struct TraceUnit {
  uint32_t type;
  std::string key;
  size_t value_size;
  uint64_t ts;
  uint32_t cf_id;
};

struct StatsUnit {
  uint64_t key_id;
  uint32_t cf_id;
  size_t value_size;
  uint64_t access_count;
};


class AnalyzerOptions {
 public:
  bool output_key_stats;
  bool output_access_count_stats;
  bool output_trace_unit;
  bool output_time_serial;
  bool output_prefix_cut;
  bool output_trace_sequence;
  bool output_io_stats;
  bool input_key_space;
  bool use_get;
  bool use_put;
  bool use_delete;
  bool use_single_delete;
  bool use_range_delete;
  bool use_merge;
  bool no_key;
  bool print_overall_stats;
  bool print_key_distribution;
  bool print_value_distribution;
  bool print_top_k_access;
  uint64_t  output_ignore_count;
  uint64_t start_time;
  int  value_interval;
  int top_k;
  int prefix_cut;
  std::string output_prefix;
  std::string key_space_dir;

  AnalyzerOptions();

  ~AnalyzerOptions();
};


struct TraceStats {
  uint32_t cf_id;
  std::string cf_name;
  uint64_t a_count;
  uint64_t akey_id;
  uint64_t a_key_size_sqsum;
  uint64_t a_key_size_sum;
  uint64_t a_key_mid;
  uint64_t a_value_size_sqsum;
  uint64_t a_value_size_sum;
  uint64_t a_value_mid;
  uint32_t a_peak_io;
  double a_ave_io;
  std::map<std::string, StatsUnit> a_key_stats;
  std::map<uint64_t, uint64_t> a_count_stats;
  std::map<uint64_t, uint64_t> a_key_size_stats;
  std::map<uint64_t, uint64_t> a_value_size_stats;
  std::map<uint32_t, uint32_t> a_io_stats;
  std::priority_queue<std::pair<uint64_t, std::string>,
                  std::vector<std::pair<uint64_t, std::string>>,
                  std::greater<std::pair<uint64_t, std::string>>> top_k_queue;
  std::list<TraceUnit> time_serial;

  FILE* time_serial_f;
  FILE* a_key_f;
  FILE* a_count_dist_f;
  FILE* a_prefix_cut_f;
  FILE* a_value_size_f;
  FILE* a_io_f;
  FILE* w_key_f;
  FILE* w_prefix_cut_f;

  TraceStats();
  ~TraceStats();
};


struct TypeUnit {
  std::string type_name;
  bool enabled;
  uint64_t total_keys;
  std::map<uint32_t, TraceStats> stats;
};

struct CfUnit {
  uint32_t cf_id;
  uint64_t w_count;  // total keys in this cf if we use the whole key space
  uint64_t a_count;  // the total keys in this cf that are accessed
  std::map<uint64_t, uint64_t> w_key_size_stats;  // whole key space key size
                                                  // statistic this cf
};

class TraceAnalyzer {
 public:
  TraceAnalyzer(std::string &trace_path, std::string &output_path,
                AnalyzerOptions _analyzer_opts);
  ~TraceAnalyzer();

  Status PrepareProcessing();

  Status StartProcessing();

  Status MakeStatistics();

  Status ReProcessing();

  Status EndProcessing();

  Status WriteTraceUnit(TraceUnit &unit);

  // The trace  processing functions for different type
  Status HandleGetCF(uint32_t column_family_id, const std::string& key,
                     const uint64_t& ts);
  Status HandlePutCF(uint32_t column_family_id, const Slice& key,
                     const Slice& value);
  Status HandleDeleteCF(uint32_t column_family_id, const Slice& key);
  Status HandleSingleDeleteCF(uint32_t column_family_id, const Slice& key);
  Status HandleDeleteRangeCF(uint32_t column_family_id, const Slice& begin_key,
                             const Slice& end_key);
  Status HandleMergeCF(uint32_t column_family_id, const Slice& key,
                       const Slice& value);

 private:
  rocksdb::Env* env_;
  unique_ptr<rocksdb::TraceReader> trace_reader_;
  size_t offset_;
  char *buffer_;
  uint64_t c_time_;
  std::string trace_name_;
  std::string output_path_;
  AnalyzerOptions analyzer_opts_;
  uint64_t total_requests_;
  uint64_t total_access_keys_;
  uint64_t total_gets_;
  uint64_t total_writes_;
  uint64_t begin_time_;
  uint64_t end_time_;
  FILE* trace_sequence_f;  // output the trace sequence for further process
  FILE* iops_f;            // output the requests per second
  std::ifstream wkey_input_f;
  std::vector<TypeUnit> ta_;  // The main statistic collecting data structure
  std::map<uint32_t, CfUnit> cfs_;  // All the cf_id appears in this trace;
  std::vector<uint32_t> io_peak_;
  std::vector<double> io_ave_;

  Status KeyStatsInsertion(const uint32_t& type, const uint32_t& cf_id,
                           const std::string& key, const size_t value_size,
                           const uint64_t ts);
  Status OpenStatsOutputFiles(const std::string& type, TraceStats& new_stats);
  FILE* CreateOutputFile(const std::string& type, const std::string& cf_name,
                         const std::string& ending);
  void CloseOutputFiles();

  void PrintGetStatistics();
  Status TraceUnitWriter(FILE *file_p, TraceUnit &unit);
  std::string MicrosdToDate(uint64_t time);

  Status WriteTraceSequence(const uint32_t& type, const uint32_t& cf_id,
                            const std::string& key, const size_t value_size,
                            const uint64_t ts);
  Status MakeStatisticIO();
};

// write bach handler to be used for WriteBache iterator
// when processing the write trace
class TraceWriteHandler : public WriteBatch::Handler {
 private:
  TraceAnalyzer* ta_ptr;
  std::string tmp_use;

 public:
  TraceWriteHandler() { ta_ptr = nullptr; }
  TraceWriteHandler(TraceAnalyzer* _ta_ptr) { ta_ptr = _ta_ptr; }
  ~TraceWriteHandler() {}

  virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                       const Slice& value) override {
    return ta_ptr->HandlePutCF(column_family_id, key, value);
  }
  virtual Status DeleteCF(uint32_t column_family_id,
                          const Slice& key) override {
    return ta_ptr->HandleDeleteCF(column_family_id, key);
  }
  virtual Status SingleDeleteCF(uint32_t column_family_id,
                                const Slice& key) override {
    return ta_ptr->HandleSingleDeleteCF(column_family_id, key);
  }
  virtual Status DeleteRangeCF(uint32_t column_family_id,
                               const Slice& begin_key,
                               const Slice& end_key) override {
    return ta_ptr->HandleDeleteRangeCF(column_family_id, begin_key, end_key);
  }
  virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
    return ta_ptr->HandleMergeCF(column_family_id, key, value);
  }
  virtual void LogData(const Slice& blob) override {
    tmp_use = blob.ToString();
  }
  virtual Status MarkBeginPrepare() override { return Status::OK(); }
  virtual Status MarkEndPrepare(const Slice& xid) override {
    tmp_use = xid.ToString();
    return Status::OK();
  }
  virtual Status MarkCommit(const Slice& xid) override {
    tmp_use = xid.ToString();
    return Status::OK();
  }
  virtual Status MarkRollback(const Slice& xid) override {
    tmp_use = xid.ToString();
    return Status::OK();
  }
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
