//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#ifdef GFLAGS
#ifdef NUMA
#include <numa.h>
#include <numaif.h>
#endif
#ifndef OS_WIN
#include <unistd.h>
#endif
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "rocksdb/write_batch.h"
#include "table/meta_blocks.h"
#include "table/plain_table_factory.h"
#include "table/table_reader.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/file_reader_writer.h"
#include "util/gflags_compat.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/trace_replay.h"
#include "utilities/trace_analyzer_tool_imp.h"

#ifdef OS_WIN
#include <io.h>  // open/close
#endif

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_string(trace_path, "", "The trace file path.");
DEFINE_string(output_dir, "", "The directory to store the output files.");
DEFINE_string(output_prefix, "", "The prefix used for all the output files.");
DEFINE_bool(output_key_stats, false,
            "Output the key access count statistics to file\n"
            "for accessed keys:\n"
            "format:[cf_id value_size access_keyid access_count]\n"
            "for the whole key space keys:\n"
            "format:[whole_key_space_keyid access_count]");
DEFINE_bool(output_access_count_stats, false,
            "Output the access count distribution statistics to file."
            "format:[access_count number_of_access_count]");
DEFINE_uint64(output_time_series, 0,
              "trace collect time, in microseconds"
              "Output the access time sequence of each key\n"
              "format:[type_id time_in_sec access_key_id].");
DEFINE_int32(output_prefix_cut, 0,
             "The number of bytes as prefix to cut the keys."
             "if it is enabled, it will generate the following:\n"
             "for accessed keys:\n"
             "format:[acessed_keyid access_count num_keys ave_access prefix]\n"
             "for whole key space keys:\n"
             "format:[start_keyid_in_whole_keyspace prefix]\n"
             "if 'output_qps_stats' is enabled, it will output:\n"
             "format:[time_in_sec IO_num], [prefix qps_of_this_second].");
DEFINE_bool(output_trace_sequence, false,
            "Out put the trace sequence for further processing"
            "including the type, cf_id, ts, value_sze, key. This file"
            "will be extremely large (similar size as the original trace)"
            "you can specify 'no_key' to reduce the size\n"
            "format:[type_id cf_id value_size time_in_micorsec <key>].");
DEFINE_bool(output_qps_stats, false,
            "Output the query per second(qps) statistics"
            "For the overall qps, it will contain all qps of each query type.\n"
            "For each cf and query, it will have its own qps output\n"
            "format:[query_count_in_this_second].");
DEFINE_bool(no_print, false, "Do not print out any result");
DEFINE_string(
    output_correlation, "",
    "intput format: [correlation pairs][.,.]\n"
    "Output the query correlations between the pairs of query types"
    "listed in the parameter, input should select the operations from:\n"
    "get, put, delete, single_delete, rangle_delete, merge. No space\n"
    "between the pairs separated by commar. Example: =[get,get]...");
DEFINE_string(key_space_dir, "",
              "<the directory stores full key space files>"
              "The key space files should be: <column family id>.txt");
DEFINE_bool(analyze_get, false, "Analyze the Get query.");
DEFINE_bool(analyze_put, false, "Analyze the Put query.");
DEFINE_bool(analyze_delete, false, "Analyze the Delete query.");
DEFINE_bool(analyze_single_delete, false, "Analyze the SingleDelete query.");
DEFINE_bool(analyze_range_delete, false, "Analyze the DeleteRange query.");
DEFINE_bool(analyze_merge, false, "Analyze the Merge query.");
DEFINE_bool(analyze_iterator, false,
            "Analyze the iterate query like seek() and seekForPre().");
DEFINE_bool(no_key, true,
            " Does not output the key to the result files to make smaller.");
DEFINE_bool(print_overall_stats, true,
            " Print the stats of the whole trace,"
            "like total requests, keys, and etc.");
DEFINE_bool(print_key_distribution, false, "Print the key size distribution.");
DEFINE_bool(output_value_distribution, false,
            "Print the value size distribution, only available for Put.");
DEFINE_int32(print_top_k_access, 1,
             "<top K of the variables to be printed>"
             "Print the top k accessed keys, top k accessed prefix"
             "and etc.");
DEFINE_int32(output_ignore_count, 0,
             "<threshold>, ignores the access count <= this value,"
             "it will shorter the output.");
DEFINE_int32(value_interval, 8,
             "To output the value distribution, we need to set the value"
             "intervals andmake the statistic of the value size distribution"
             "in different intervals. The default is 8.");

namespace rocksdb {

std::map<std::string, int> taOptToIndex = {
    {"get", 0},          {"put", 1},   {"delete", 2},  {"single_delete", 3},
    {"range_delete", 4}, {"merge", 5}, {"iterator", 6}};

std::map<int, std::string> taIndexToOpt = {
    {0, "get"},          {1, "put"},   {2, "delete"},  {3, "single_delete"},
    {4, "range_delete"}, {5, "merge"}, {6, "iterator"}};

// Transfer the Microsecond time to date time
std::string TraceAnalyzer::MicrosdToDate(uint64_t time_in) {
  time_t tx = static_cast<time_t>(time_in / 1000000);
  int rest = static_cast<int>(time_in % 1000000);
  std::string date_time(ctime(&tx));
  date_time.pop_back();
  date_time += " +: " + std::to_string(rest);
  return date_time;
}

namespace {

bool ReadOneLine(std::istringstream* iss, SequentialFile* seq_file,
                 std::string* output, bool* has_data, Status* result) {
  const int kBufferSize = 128;
  char buffer[kBufferSize + 1];
  Slice input_slice;

  std::string line;
  bool has_complete_line = false;
  while (!has_complete_line) {
    if (std::getline(*iss, line)) {
      has_complete_line = !iss->eof();
    } else {
      has_complete_line = false;
    }
    if (!has_complete_line) {
      // if we're not sure whether we have a complete line,
      // further read from the file.
      if (*has_data) {
        *result = seq_file->Read(kBufferSize, &input_slice, buffer);
      }
      if (input_slice.size() == 0) {
        // meaning we have read all the data
        *has_data = false;
        break;
      } else {
        iss->str(line + input_slice.ToString());
        // reset the internal state of iss so that we can keep reading it.
        iss->clear();
        *has_data = (input_slice.size() == kBufferSize);
        continue;
      }
    }
  }
  *output = line;
  return *has_data || has_complete_line;
}

uint64_t MultiplyCheckOverflow(uint64_t op1, uint64_t op2) {
  if (op1 == 0 || op2 == 0) {
    return 0;
  }
  if (port::kMaxUint64 / op1 < op2) {
    return op1;
  }
  return (op1 * op2);
}

void DecodeCFAndKey(std::string& buffer, uint32_t* cf_id, Slice* key) {
  Slice buf(buffer);
  GetFixed32(&buf, cf_id);
  GetLengthPrefixedSlice(&buf, key);
}

}  // namespace

// The default constructor of AnalyzerOptions
AnalyzerOptions::AnalyzerOptions()
    : corre_map(kTaTypeNum, std::vector<int>(kTaTypeNum, -1)) {}

AnalyzerOptions::~AnalyzerOptions() {}

void AnalyzerOptions::SparseCorreInput(const std::string& in_str) {
  std::string cur = in_str;
  if (cur.size() == 0) {
    FLAGS_output_correlation = "";
    return;
  }
  while (!cur.empty()) {
    if (cur.compare(0, 1, "[") != 0) {
      fprintf(stderr, "Invalid correlation input: %s\n", in_str.c_str());
      exit(1);
    }
    std::string opt1, opt2;
    std::size_t split = cur.find_first_of(",");
    if (split != std::string::npos) {
      opt1 = cur.substr(1, split - 1);
    } else {
      fprintf(stderr, "Invalid correlation input: %s\n", in_str.c_str());
      exit(1);
    }
    std::size_t end = cur.find_first_of("]");
    if (end != std::string::npos) {
      opt2 = cur.substr(split + 1, end - split - 1);
    } else {
      fprintf(stderr, "Invalid correlation input: %s\n", in_str.c_str());
      exit(1);
    }
    cur = cur.substr(end + 1);

    if (taOptToIndex.find(opt1) != taOptToIndex.end() &&
        taOptToIndex.find(opt2) != taOptToIndex.end()) {
      corre_list.push_back(
          std::make_pair(taOptToIndex[opt1], taOptToIndex[opt2]));
    } else {
      fprintf(stderr, "Invalid correlation input: %s\n", in_str.c_str());
      exit(1);
    }
  }

  int sequence = 0;
  for (auto& it : corre_list) {
    corre_map[it.first][it.second] = sequence;
    sequence++;
  }
  return;
}

// The trace statistic struct constructor
TraceStats::TraceStats() {
  cf_id = 0;
  cf_name = "0";
  a_count = 0;
  akey_id = 0;
  a_key_size_sqsum = 0;
  a_key_size_sum = 0;
  a_key_mid = 0;
  a_value_size_sqsum = 0;
  a_value_size_sum = 0;
  a_value_mid = 0;
  a_peak_qps = 0;
  a_ave_qps = 0.0;
}

TraceStats::~TraceStats() {}

// The trace analyzer constructor
TraceAnalyzer::TraceAnalyzer(std::string& trace_path, std::string& output_path,
                             AnalyzerOptions _analyzer_opts)
    : trace_name_(trace_path),
      output_path_(output_path),
      analyzer_opts_(_analyzer_opts) {
  rocksdb::EnvOptions env_options;
  env_ = rocksdb::Env::Default();
  offset_ = 0;
  c_time_ = 0;
  total_requests_ = 0;
  total_access_keys_ = 0;
  total_gets_ = 0;
  total_writes_ = 0;
  begin_time_ = 0;
  end_time_ = 0;
  ta_.resize(kTaTypeNum);
  ta_[0].type_name = "get";
  if (FLAGS_analyze_get) {
    ta_[0].enabled = true;
  } else {
    ta_[0].enabled = false;
  }
  ta_[1].type_name = "put";
  if (FLAGS_analyze_put) {
    ta_[1].enabled = true;
  } else {
    ta_[1].enabled = false;
  }
  ta_[2].type_name = "delete";
  if (FLAGS_analyze_delete) {
    ta_[2].enabled = true;
  } else {
    ta_[2].enabled = false;
  }
  ta_[3].type_name = "single_delete";
  if (FLAGS_analyze_single_delete) {
    ta_[3].enabled = true;
  } else {
    ta_[3].enabled = false;
  }
  ta_[4].type_name = "range_delete";
  if (FLAGS_analyze_range_delete) {
    ta_[4].enabled = true;
  } else {
    ta_[4].enabled = false;
  }
  ta_[5].type_name = "merge";
  if (FLAGS_analyze_merge) {
    ta_[5].enabled = true;
  } else {
    ta_[5].enabled = false;
  }
  ta_[6].type_name = "iterator";
  if (FLAGS_analyze_iterator) {
    ta_[6].enabled = true;
  } else {
    ta_[6].enabled = false;
  }
}

TraceAnalyzer::~TraceAnalyzer() {}

// Prepare the processing
// Initiate the global trace reader and writer here
Status TraceAnalyzer::PrepareProcessing() {
  Status s;
  // Prepare the trace reader
  s = NewFileTraceReader(env_, env_options_, trace_name_, &trace_reader_);
  if (!s.ok()) {
    return s;
  }

  // Prepare and open the trace sequence file writer if needed
  if (FLAGS_output_trace_sequence) {
    std::string trace_sequence_name;
    trace_sequence_name =
        output_path_ + "/" + FLAGS_output_prefix + "-trace_sequence.txt";
    s = env_->NewWritableFile(trace_sequence_name, &trace_sequence_f_,
                              env_options_);
    if (!s.ok()) {
      return s;
    }
  }

  // prepare the general QPS file writer
  if (FLAGS_output_qps_stats) {
    std::string qps_stats_name;
    qps_stats_name =
        output_path_ + "/" + FLAGS_output_prefix + "-qps_stats.txt";
    s = env_->NewWritableFile(qps_stats_name, &qps_f_, env_options_);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status TraceAnalyzer::ReadTraceHeader(Trace* header) {
  assert(header != nullptr);
  Status s = ReadTraceRecord(header);
  if (!s.ok()) {
    return s;
  }
  if (header->type != kTraceBegin) {
    return Status::Corruption("Corrupted trace file. Incorrect header.");
  }
  if (header->payload.substr(0, kTraceMagic.length()) != kTraceMagic) {
    return Status::Corruption("Corrupted trace file. Incorrect magic.");
  }

  return s;
}

Status TraceAnalyzer::ReadTraceFooter(Trace* footer) {
  assert(footer != nullptr);
  Status s = ReadTraceRecord(footer);
  if (!s.ok()) {
    return s;
  }
  if (footer->type != kTraceEnd) {
    return Status::Corruption("Corrupted trace file. Incorrect footer.");
  }
  return s;
}

Status TraceAnalyzer::ReadTraceRecord(Trace* trace) {
  assert(trace != nullptr);
  std::string encoded_trace;
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }

  Slice enc_slice = Slice(encoded_trace);
  GetFixed64(&enc_slice, &trace->ts);
  trace->type = static_cast<TraceType>(enc_slice[0]);
  enc_slice.remove_prefix(kTraceTypeSize + kTracePayloadLengthSize);
  trace->payload = enc_slice.ToString();
  return s;
}

// process the trace itself and redirect the trace content
// to different operation type handler. With different race
// format, this function can be changed
Status TraceAnalyzer::StartProcessing() {
  Status s;
  Trace header;
  s = ReadTraceHeader(&header);
  if (!s.ok()) {
    fprintf(stderr, "Cannot read the header\n");
    return s;
  }
  if (FLAGS_output_time_series == 0) {
    FLAGS_output_time_series = header.ts;
  }

  Trace trace;
  while (s.ok()) {
    trace.reset();
    s = ReadTraceRecord(&trace);
    if (!s.ok()) {
      break;
    }

    total_requests_++;
    end_time_ = trace.ts;
    if (trace.type == kTraceWrite) {
      total_writes_++;
      c_time_ = trace.ts;
      WriteBatch batch(trace.payload);
      if (batch.HasBeginPrepare() && !batch.HasCommit()) {
        continue;
      }
      TraceWriteHandler write_handler(this);
      s = batch.Iterate(&write_handler);
      if (!s.ok()) {
        fprintf(stderr, "Cannot process the write batch in the trace\n");
        exit(1);
      }
    } else if (trace.type == kTraceGet) {
      uint32_t cf_id = 0;
      Slice key;
      DecodeCFAndKey(trace.payload, &cf_id, &key);
      total_gets_++;

      s = HandleGetCF(cf_id, key.ToString(), trace.ts, 1);
      if (!s.ok()) {
        fprintf(stderr, "Cannot process the get in the trace\n");
        exit(1);
      }
    } else if (trace.type == kTraceIter) {
      // Not supported in the current trace_replay to collect iterator
      // Need to be refactored if trace_replay implemented tracing iteator
      uint32_t cf_id = 0;
      Slice key;
      DecodeCFAndKey(trace.payload, &cf_id, &key);
      s = HandleIterCF(cf_id, key.ToString(), trace.ts);
      if (!s.ok()) {
        fprintf(stderr, "Cannot process the iterator in the trace\n");
        exit(1);
      }
    } else if (trace.type == kTraceEnd) {
      break;
    }
  }
  if (s.IsIncomplete()) {
    // Fix it: Reaching eof returns Incomplete status at the moment.
    //
    return Status::OK();
  }
  return s;
}

// After the trace is processed by StartProcessing, the statistic data
// is stored in the map or other in memory data structures. To get the
// other statistic result such as key size distribution, value size
// distribution, these data structures are re-processed here.
Status TraceAnalyzer::MakeStatistics() {
  int ret;
  Status s;
  for (int type = 0; type < kTaTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    for (auto& i : ta_[type].stats) {
      i.second.akey_id = 0;
      for (auto& it : i.second.a_key_stats) {
        it.second.key_id = i.second.akey_id;
        i.second.akey_id++;
        if (it.second.access_count <=
            static_cast<uint64_t>(FLAGS_output_ignore_count)) {
          continue;
        }

        // Generate the key access count distribution data
        if (FLAGS_output_access_count_stats) {
          if (i.second.a_count_stats.find(it.second.access_count) ==
              i.second.a_count_stats.end()) {
            i.second.a_count_stats[it.second.access_count] = 1;
          } else {
            i.second.a_count_stats[it.second.access_count]++;
          }
        }

        // Generate the key size distribution data
        if (FLAGS_print_key_distribution) {
          if (i.second.a_key_size_stats.find(it.first.size()) ==
              i.second.a_key_size_stats.end()) {
            i.second.a_key_size_stats[it.first.size()] = 1;
          } else {
            i.second.a_key_size_stats[it.first.size()]++;
          }
        }

        if (!FLAGS_output_correlation.empty()) {
          s = MakeStatisticCorrelation(i.second, it.second);
        }
      }

      // Output the prefix cut or the whole content of the accessed key space
      if (FLAGS_output_key_stats || FLAGS_output_prefix_cut > 0) {
        s = MakeStatisticKeyStatsOrPrefix(i.second);
        if (!s.ok()) {
          return s;
        }
      }

      // output the access count distribution
      if (FLAGS_output_access_count_stats && i.second.a_count_dist_f) {
        for (auto& it : i.second.a_count_stats) {
          ret = sprintf(buffer_, "access_count: %" PRIu64 " num: %" PRIu64 "\n",
                        it.first, it.second);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = i.second.a_count_dist_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write access count distribution file failed\n");
            return s;
          }
        }
      }

      // find the medium of the key size
      uint64_t k_count = 0;
      for (auto& it : i.second.a_key_size_stats) {
        k_count += it.second;
        if (k_count >= i.second.a_key_mid) {
          i.second.a_key_mid = it.first;
          break;
        }
      }

      // output the value size distribution
      uint64_t v_begin = 0, v_end = 0, v_count = 0;
      bool get_mid = false;
      for (auto& it : i.second.a_value_size_stats) {
        v_begin = v_end;
        v_end = (it.first + 1) * FLAGS_value_interval;
        v_count += it.second;
        if (!get_mid && v_count >= i.second.a_count / 2) {
          i.second.a_value_mid = (v_begin + v_end) / 2;
          get_mid = true;
        }
        if (FLAGS_output_value_distribution && i.second.a_value_size_f) {
          ret = sprintf(buffer_,
                        "Number_of_value_size_between %" PRIu64 " and %" PRIu64
                        " is: %" PRIu64 "\n",
                        v_begin, v_end, it.second);
          if (ret < 0) {
            return Status::IOError("Format output failed");
          }
          std::string printout(buffer_);
          s = i.second.a_value_size_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write value size distribution file failed\n");
            return s;
          }
        }
      }
    }
  }

  // Make the QPS statistics
  if (FLAGS_output_qps_stats) {
    s = MakeStatisticQPS();
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

// Process the statistics of the key access and
// prefix of the accessed keys if required
Status TraceAnalyzer::MakeStatisticKeyStatsOrPrefix(TraceStats& stats) {
  int ret;
  Status s;
  std::string prefix = "0";
  uint64_t prefix_access = 0;
  uint64_t prefix_count = 0;
  uint64_t prefix_succ_access = 0;
  double prefix_ave_access = 0.0;
  stats.a_succ_count = 0;
  for (auto& it : stats.a_key_stats) {
    // write the key access statistic file
    if (!stats.a_key_f) {
      fprintf(stderr, "The accessed_key_stats file is not opend\n");
      exit(1);
    }
    stats.a_succ_count += it.second.succ_count;
    double succ_ratio =
        (static_cast<double>(it.second.succ_count)) / it.second.access_count;
    ret = sprintf(buffer_, "%u %zu %" PRIu64 " %" PRIu64 " %f\n",
                  it.second.cf_id, it.second.value_size, it.second.key_id,
                  it.second.access_count, succ_ratio);
    if (ret < 0) {
      return Status::IOError("Format output failed");
    }
    std::string printout(buffer_);
    s = stats.a_key_f->Append(printout);
    if (!s.ok()) {
      fprintf(stderr, "Write key access file failed\n");
      return s;
    }

    // write the prefix cut of the accessed keys
    if (FLAGS_output_prefix_cut > 0 && stats.a_prefix_cut_f) {
      if (it.first.compare(0, FLAGS_output_prefix_cut, prefix) != 0) {
        std::string prefix_out = rocksdb::LDBCommand::StringToHex(prefix);
        if (prefix_count == 0) {
          prefix_ave_access = 0.0;
        } else {
          prefix_ave_access =
              (static_cast<double>(prefix_access)) / prefix_count;
        }
        double prefix_succ_ratio =
            (static_cast<double>(prefix_succ_access)) / prefix_access;
        ret = sprintf(buffer_, "%" PRIu64 " %" PRIu64 " %" PRIu64 " %f %f %s\n",
                      it.second.key_id, prefix_access, prefix_count,
                      prefix_ave_access, prefix_succ_ratio, prefix_out.c_str());
        if (ret < 0) {
          return Status::IOError("Format output failed");
        }
        std::string pout(buffer_);
        s = stats.a_prefix_cut_f->Append(pout);
        if (!s.ok()) {
          fprintf(stderr, "Write accessed key prefix file failed\n");
          return s;
        }

        // make the top k statistic for the prefix
        if (static_cast<int32_t>(stats.top_k_prefix_access.size()) <
            FLAGS_print_top_k_access) {
          stats.top_k_prefix_access.push(
              std::make_pair(prefix_access, prefix_out));
        } else {
          if (prefix_access > stats.top_k_prefix_access.top().first) {
            stats.top_k_prefix_access.pop();
            stats.top_k_prefix_access.push(
                std::make_pair(prefix_access, prefix_out));
          }
        }

        if (static_cast<int32_t>(stats.top_k_prefix_ave.size()) <
            FLAGS_print_top_k_access) {
          stats.top_k_prefix_ave.push(
              std::make_pair(prefix_ave_access, prefix_out));
        } else {
          if (prefix_ave_access > stats.top_k_prefix_ave.top().first) {
            stats.top_k_prefix_ave.pop();
            stats.top_k_prefix_ave.push(
                std::make_pair(prefix_ave_access, prefix_out));
          }
        }

        prefix = it.first.substr(0, FLAGS_output_prefix_cut);
        prefix_access = 0;
        prefix_count = 0;
        prefix_succ_access = 0;
      }
      prefix_access += it.second.access_count;
      prefix_count += 1;
      prefix_succ_access += it.second.succ_count;
    }
  }
  return Status::OK();
}

// Process the statistics of different query type
// correlations
Status TraceAnalyzer::MakeStatisticCorrelation(TraceStats& stats,
                                               StatsUnit& unit) {
  if (stats.corre_output.size() != analyzer_opts_.corre_list.size()) {
    fprintf(stderr, "Cannot make the statistic of correlation\n");
    return Status::OK();
  }

  for (int i = 0; i < static_cast<int>(analyzer_opts_.corre_list.size()); i++) {
    stats.corre_output[i].first += unit.v_corre[i].count;
    stats.corre_output[i].second += unit.v_corre[i].total_ts;
  }
  return Status::OK();
}

// Process the statistics of QPS
Status TraceAnalyzer::MakeStatisticQPS() {
  uint32_t duration = (end_time_ - begin_time_) / 1000000;
  int ret;
  Status s;
  std::vector<std::vector<uint32_t>> type_qps(
      duration, std::vector<uint32_t>(kTaTypeNum + 1, 0));
  std::vector<uint64_t> qps_sum(kTaTypeNum + 1, 0);
  std::vector<uint32_t> qps_peak(kTaTypeNum + 1, 0);
  qps_ave_.resize(kTaTypeNum + 1);

  for (int type = 0; type < kTaTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    for (auto& i : ta_[type].stats) {
      uint32_t time_line = 0;
      uint64_t cf_qps_sum = 0;
      for (auto& time_it : i.second.a_qps_stats) {
        if (time_it.first >= duration) {
          continue;
        }
        type_qps[time_it.first][kTaTypeNum] += time_it.second;
        type_qps[time_it.first][type] += time_it.second;
        cf_qps_sum += time_it.second;
        if (time_it.second > i.second.a_peak_qps) {
          i.second.a_peak_qps = time_it.second;
        }
        if (i.second.a_qps_f) {
          while (time_line < time_it.first) {
            ret = sprintf(buffer_, "%u\n", 0);
            if (ret < 0) {
              return Status::IOError("Format the output failed");
            }
            std::string printout(buffer_);
            s = i.second.a_qps_f->Append(printout);
            if (!s.ok()) {
              fprintf(stderr, "Write QPS file failed\n");
              return s;
            }
            time_line++;
          }
          ret = sprintf(buffer_, "%u\n", time_it.second);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = i.second.a_qps_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write QPS file failed\n");
            return s;
          }
          if (time_line == time_it.first) {
            time_line++;
          }
        }

        // Process the top k IO peaks
        if (FLAGS_output_prefix_cut > 0) {
          if (static_cast<int32_t>(i.second.top_k_qps_sec.size()) <
              FLAGS_print_top_k_access) {
            i.second.top_k_qps_sec.push(
                std::make_pair(time_it.second, time_it.first));
          } else {
            if (i.second.top_k_qps_sec.size() > 0 &&
                i.second.top_k_qps_sec.top().first < time_it.second) {
              i.second.top_k_qps_sec.pop();
              i.second.top_k_qps_sec.push(
                  std::make_pair(time_it.second, time_it.first));
            }
          }
        }
      }
      i.second.a_ave_qps = (static_cast<double>(cf_qps_sum)) / duration;

      // output the prefix of top k access peak
      if (FLAGS_output_prefix_cut > 0 && i.second.a_top_qps_prefix_f) {
        while (!i.second.top_k_qps_sec.empty()) {
          ret = sprintf(buffer_, "At time: %u with QPS: %u\n",
                        i.second.top_k_qps_sec.top().second,
                        i.second.top_k_qps_sec.top().first);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = i.second.a_top_qps_prefix_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write prefix QPS top K file failed\n");
            return s;
          }
          uint32_t qps_time = i.second.top_k_qps_sec.top().second;
          i.second.top_k_qps_sec.pop();
          if (i.second.a_qps_prefix_stats.find(qps_time) !=
              i.second.a_qps_prefix_stats.end()) {
            for (auto& qps_prefix : i.second.a_qps_prefix_stats[qps_time]) {
              std::string qps_prefix_out =
                  rocksdb::LDBCommand::StringToHex(qps_prefix.first);
              ret = sprintf(buffer_, "The prefix: %s Access count: %u\n",
                            qps_prefix_out.c_str(), qps_prefix.second);
              if (ret < 0) {
                return Status::IOError("Format the output failed");
              }
              std::string pout(buffer_);
              s = i.second.a_top_qps_prefix_f->Append(pout);
              if (!s.ok()) {
                fprintf(stderr, "Write prefix QPS top K file failed\n");
                return s;
              }
            }
          }
        }
      }
    }
  }

  if (qps_f_) {
    for (uint32_t i = 0; i < duration; i++) {
      for (int type = 0; type <= kTaTypeNum; type++) {
        if (type < kTaTypeNum) {
          ret = sprintf(buffer_, "%u ", type_qps[i][type]);
        } else {
          ret = sprintf(buffer_, "%u\n", type_qps[i][type]);
        }
        if (ret < 0) {
          return Status::IOError("Format the output failed");
        }
        std::string printout(buffer_);
        s = qps_f_->Append(printout);
        if (!s.ok()) {
          return s;
        }
        qps_sum[type] += type_qps[i][type];
        if (type_qps[i][type] > qps_peak[type]) {
          qps_peak[type] = type_qps[i][type];
        }
      }
    }
  }

  qps_peak_ = qps_peak;
  for (int type = 0; type <= kTaTypeNum; type++) {
    qps_ave_[type] = (static_cast<double>(qps_sum[type])) / duration;
  }

  return Status::OK();
}

// In reprocessing, if we have the whole key space
// we can output the access count of all keys in a cf
// we can make some statistics of the whole key space
// also, we output the top k accessed keys here
Status TraceAnalyzer::ReProcessing() {
  int ret;
  Status s;
  for (auto& cf_it : cfs_) {
    uint32_t cf_id = cf_it.first;

    // output the time series;
    if (FLAGS_output_time_series > 0) {
      for (int i = 0; i < kTaTypeNum; i++) {
        if (!ta_[i].enabled || ta_[i].stats.find(cf_id) == ta_[i].stats.end()) {
          continue;
        }
        TraceStats& stats = ta_[i].stats[cf_id];
        if (!stats.time_series_f) {
          fprintf(stderr, "Cannot write time_series of '%s' in '%u'\n",
                  ta_[i].type_name.c_str(), cf_id);
          continue;
        }
        while (!stats.time_series.empty()) {
          uint64_t key_id = 0;
          auto found = stats.a_key_stats.find(stats.time_series.front().key);
          if (found != stats.a_key_stats.end()) {
            key_id = found->second.key_id;
          }
          ret = sprintf(buffer_, "%u %" PRIu64 " %" PRIu64 "\n",
                        stats.time_series.front().type,
                        stats.time_series.front().ts, key_id);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = stats.time_series_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write time series file failed\n");
            return s;
          }
          stats.time_series.pop_front();
        }
      }
    }

    // process the whole key space if needed
    if (!FLAGS_key_space_dir.empty()) {
      std::string whole_key_path =
          FLAGS_key_space_dir + "/" + std::to_string(cf_id) + ".txt";
      std::string input_key, get_key;
      std::vector<std::string> prefix(kTaTypeNum);
      std::istringstream iss;
      bool has_data = true;
      s = env_->NewSequentialFile(whole_key_path, &wkey_input_f_, env_options_);
      if (!s.ok()) {
        fprintf(stderr, "Cannot open the whole key space file of CF: %u\n",
                cf_id);
        wkey_input_f_.release();
      }
      if (wkey_input_f_) {
        for (cfs_[cf_id].w_count = 0;
             ReadOneLine(&iss, wkey_input_f_.get(), &get_key, &has_data, &s);
             ++cfs_[cf_id].w_count) {
          if (!s.ok()) {
            fprintf(stderr, "Read whole key space file failed\n");
            return s;
          }

          input_key = rocksdb::LDBCommand::HexToString(get_key);
          for (int i = 0; i < kTaTypeNum; i++) {
            if (!ta_[i].enabled) {
              continue;
            }
            TraceStats& stats = ta_[i].stats[cf_id];
            if (stats.w_key_f) {
              if (stats.a_key_stats.find(input_key) !=
                  stats.a_key_stats.end()) {
                ret = sprintf(buffer_, "%" PRIu64 " %" PRIu64 "\n",
                              cfs_[cf_id].w_count,
                              stats.a_key_stats[input_key].access_count);
                if (ret < 0) {
                  return Status::IOError("Format the output failed");
                }
                std::string printout(buffer_);
                s = stats.w_key_f->Append(printout);
                if (!s.ok()) {
                  fprintf(stderr, "Write whole key space access file failed\n");
                  return s;
                }
              }
            }

            // Output the prefix cut file of the whole key space
            if (FLAGS_output_prefix_cut > 0 && stats.w_prefix_cut_f) {
              if (input_key.compare(0, FLAGS_output_prefix_cut, prefix[i]) !=
                  0) {
                prefix[i] = input_key.substr(0, FLAGS_output_prefix_cut);
                std::string prefix_out =
                    rocksdb::LDBCommand::StringToHex(prefix[i]);
                ret = sprintf(buffer_, "%" PRIu64 " %s\n", cfs_[cf_id].w_count,
                              prefix_out.c_str());
                if (ret < 0) {
                  return Status::IOError("Format the output failed");
                }
                std::string printout(buffer_);
                s = stats.w_prefix_cut_f->Append(printout);
                if (!s.ok()) {
                  fprintf(stderr,
                          "Write whole key space prefix cut file failed\n");
                  return s;
                }
              }
            }
          }

          // Make the statistics fo the key size distribution
          if (FLAGS_print_key_distribution) {
            if (cfs_[cf_id].w_key_size_stats.find(input_key.size()) ==
                cfs_[cf_id].w_key_size_stats.end()) {
              cfs_[cf_id].w_key_size_stats[input_key.size()] = 1;
            } else {
              cfs_[cf_id].w_key_size_stats[input_key.size()]++;
            }
          }
        }
      }
    }

    // process the top k accessed keys
    if (FLAGS_print_top_k_access > 0) {
      for (int i = 0; i < kTaTypeNum; i++) {
        if (!ta_[i].enabled || ta_[i].stats.find(cf_id) == ta_[i].stats.end()) {
          continue;
        }
        TraceStats& stats = ta_[i].stats[cf_id];
        for (auto& it : stats.a_key_stats) {
          if (static_cast<int32_t>(stats.top_k_queue.size()) <
              FLAGS_print_top_k_access) {
            stats.top_k_queue.push(
                std::make_pair(it.second.access_count, it.first));
          } else {
            if (it.second.access_count > stats.top_k_queue.top().first) {
              stats.top_k_queue.pop();
              stats.top_k_queue.push(
                  std::make_pair(it.second.access_count, it.first));
            }
          }
        }
      }
    }
  }
  return Status::OK();
}

// End the processing, print the requested results
Status TraceAnalyzer::EndProcessing() {
  if (trace_sequence_f_) {
    trace_sequence_f_->Close();
  }
  if (FLAGS_no_print) {
    return Status::OK();
  }
  PrintGetStatistics();
  CloseOutputFiles();
  return Status::OK();
}

// Insert the corresponding key statistics to the correct type
// and correct CF, output the time-series file if needed
Status TraceAnalyzer::KeyStatsInsertion(const uint32_t& type,
                                        const uint32_t& cf_id,
                                        const std::string& key,
                                        const size_t value_size,
                                        const uint64_t ts) {
  Status s;
  StatsUnit unit;
  unit.key_id = 0;
  unit.cf_id = cf_id;
  unit.value_size = value_size;
  unit.access_count = 1;
  unit.latest_ts = ts;
  if (type != TraceOperationType::kGet || value_size > 0) {
    unit.succ_count = 1;
  } else {
    unit.succ_count = 0;
  }
  unit.v_corre.resize(analyzer_opts_.corre_list.size());
  for (int i = 0; i < (static_cast<int>(analyzer_opts_.corre_list.size()));
       i++) {
    unit.v_corre[i].count = 0;
    unit.v_corre[i].total_ts = 0;
  }
  std::string prefix;
  if (FLAGS_output_prefix_cut > 0) {
    prefix = key.substr(0, FLAGS_output_prefix_cut);
  }

  if (begin_time_ == 0) {
    begin_time_ = ts;
  }
  uint32_t time_in_sec;
  if (ts < begin_time_) {
    time_in_sec = 0;
  } else {
    time_in_sec = (ts - begin_time_) / 1000000;
  }

  uint64_t dist_value_size = value_size / FLAGS_value_interval;
  auto found_stats = ta_[type].stats.find(cf_id);
  if (found_stats == ta_[type].stats.end()) {
    ta_[type].stats[cf_id].cf_id = cf_id;
    ta_[type].stats[cf_id].cf_name = std::to_string(cf_id);
    ta_[type].stats[cf_id].a_count = 1;
    ta_[type].stats[cf_id].akey_id = 0;
    ta_[type].stats[cf_id].a_key_size_sqsum = MultiplyCheckOverflow(
        static_cast<uint64_t>(key.size()), static_cast<uint64_t>(key.size()));
    ta_[type].stats[cf_id].a_key_size_sum = key.size();
    ta_[type].stats[cf_id].a_value_size_sqsum = MultiplyCheckOverflow(
        static_cast<uint64_t>(value_size), static_cast<uint64_t>(value_size));
    ta_[type].stats[cf_id].a_value_size_sum = value_size;
    s = OpenStatsOutputFiles(ta_[type].type_name, ta_[type].stats[cf_id]);
    if (!FLAGS_output_correlation.empty()) {
      s = StatsUnitCorreUpdate(unit, type, ts, key);
    }
    ta_[type].stats[cf_id].a_key_stats[key] = unit;
    ta_[type].stats[cf_id].a_value_size_stats[dist_value_size] = 1;
    ta_[type].stats[cf_id].a_qps_stats[time_in_sec] = 1;
    ta_[type].stats[cf_id].corre_output.resize(
        analyzer_opts_.corre_list.size());
    if (FLAGS_output_prefix_cut > 0) {
      std::map<std::string, uint32_t> tmp_qps_map;
      tmp_qps_map[prefix] = 1;
      ta_[type].stats[cf_id].a_qps_prefix_stats[time_in_sec] = tmp_qps_map;
    }
  } else {
    found_stats->second.a_count++;
    found_stats->second.a_key_size_sqsum += MultiplyCheckOverflow(
        static_cast<uint64_t>(key.size()), static_cast<uint64_t>(key.size()));
    found_stats->second.a_key_size_sum += key.size();
    found_stats->second.a_value_size_sqsum += MultiplyCheckOverflow(
        static_cast<uint64_t>(value_size), static_cast<uint64_t>(value_size));
    found_stats->second.a_value_size_sum += value_size;
    auto found_key = found_stats->second.a_key_stats.find(key);
    if (found_key == found_stats->second.a_key_stats.end()) {
      found_stats->second.a_key_stats[key] = unit;
    } else {
      found_key->second.access_count++;
      if (type != TraceOperationType::kGet || value_size > 0) {
        found_key->second.succ_count++;
      }
      if (!FLAGS_output_correlation.empty()) {
        s = StatsUnitCorreUpdate(found_key->second, type, ts, key);
      }
    }

    auto found_value =
        found_stats->second.a_value_size_stats.find(dist_value_size);
    if (found_value == found_stats->second.a_value_size_stats.end()) {
      found_stats->second.a_value_size_stats[dist_value_size] = 1;
    } else {
      found_value->second++;
    }

    auto found_qps = found_stats->second.a_qps_stats.find(time_in_sec);
    if (found_qps == found_stats->second.a_qps_stats.end()) {
      found_stats->second.a_qps_stats[time_in_sec] = 1;
    } else {
      found_qps->second++;
    }

    if (FLAGS_output_prefix_cut > 0) {
      auto found_qps_prefix =
          found_stats->second.a_qps_prefix_stats.find(time_in_sec);
      if (found_qps_prefix == found_stats->second.a_qps_prefix_stats.end()) {
        std::map<std::string, uint32_t> tmp_qps_map;
        found_stats->second.a_qps_prefix_stats[time_in_sec] = tmp_qps_map;
      }
      if (found_stats->second.a_qps_prefix_stats[time_in_sec].find(prefix) ==
          found_stats->second.a_qps_prefix_stats[time_in_sec].end()) {
        found_stats->second.a_qps_prefix_stats[time_in_sec][prefix] = 1;
      } else {
        found_stats->second.a_qps_prefix_stats[time_in_sec][prefix]++;
      }
    }
  }

  if (cfs_.find(cf_id) == cfs_.end()) {
    CfUnit cf_unit;
    cf_unit.cf_id = cf_id;
    cf_unit.w_count = 0;
    cf_unit.a_count = 0;
    cfs_[cf_id] = cf_unit;
  }

  if (FLAGS_output_time_series > 0) {
    TraceUnit trace_u;
    trace_u.type = type;
    trace_u.key = key;
    trace_u.value_size = value_size;
    trace_u.ts = (ts - FLAGS_output_time_series) / 1000000;
    trace_u.cf_id = cf_id;
    ta_[type].stats[cf_id].time_series.push_back(trace_u);
  }

  return Status::OK();
}

// Update the correlation unit of each key if enabled
Status TraceAnalyzer::StatsUnitCorreUpdate(StatsUnit& unit,
                                           const uint32_t& type,
                                           const uint64_t& ts,
                                           const std::string& key) {
  if (type >= kTaTypeNum) {
    fprintf(stderr, "Unknown Type Id: %u\n", type);
    exit(1);
  }

  for (int i = 0; i < kTaTypeNum; i++) {
    if (analyzer_opts_.corre_map[i][type] < 0 ||
        ta_[i].stats.find(unit.cf_id) == ta_[i].stats.end() ||
        ta_[i].stats[unit.cf_id].a_key_stats.find(key) ==
            ta_[i].stats[unit.cf_id].a_key_stats.end() ||
        ta_[i].stats[unit.cf_id].a_key_stats[key].latest_ts == ts) {
      continue;
    }

    int corre_id = analyzer_opts_.corre_map[i][type];

    // after get the x-y operation time or x, update;
    unit.v_corre[corre_id].count++;
    unit.v_corre[corre_id].total_ts +=
        (ts - ta_[i].stats[unit.cf_id].a_key_stats[key].latest_ts);
  }

  unit.latest_ts = ts;
  return Status::OK();
}

// when a new trace stattistic is created, the file handler
// pointers should be initiated if needed according to
// the trace analyzer options
Status TraceAnalyzer::OpenStatsOutputFiles(const std::string& type,
                                           TraceStats& new_stats) {
  Status s;
  if (FLAGS_output_key_stats) {
    s = CreateOutputFile(type, new_stats.cf_name, "accessed_key_stats.txt",
                         &new_stats.a_key_f);
    if (!FLAGS_key_space_dir.empty()) {
      s = CreateOutputFile(type, new_stats.cf_name, "whole_key_stats.txt",
                           &new_stats.w_key_f);
    }
  }

  if (FLAGS_output_access_count_stats) {
    s = CreateOutputFile(type, new_stats.cf_name,
                         "accessed_key_count_distribution.txt",
                         &new_stats.a_count_dist_f);
  }

  if (FLAGS_output_prefix_cut > 0) {
    s = CreateOutputFile(type, new_stats.cf_name, "accessed_key_prefix_cut.txt",
                         &new_stats.a_prefix_cut_f);
    if (!FLAGS_key_space_dir.empty()) {
      s = CreateOutputFile(type, new_stats.cf_name, "whole_key_prefix_cut.txt",
                           &new_stats.w_prefix_cut_f);
    }

    if (FLAGS_output_qps_stats) {
      s = CreateOutputFile(type, new_stats.cf_name,
                           "accessed_top_k_qps_prefix_cut.txt",
                           &new_stats.a_top_qps_prefix_f);
    }
  }

  if (FLAGS_output_time_series > 0) {
    s = CreateOutputFile(type, new_stats.cf_name, "time_series.txt",
                         &new_stats.time_series_f);
  }

  if (FLAGS_output_value_distribution) {
    s = CreateOutputFile(type, new_stats.cf_name,
                         "accessed_value_size_distribution.txt",
                         &new_stats.a_value_size_f);
  }

  if (FLAGS_output_qps_stats) {
    s = CreateOutputFile(type, new_stats.cf_name, "qps_stats.txt",
                         &new_stats.a_qps_f);
  }

  return Status::OK();
}

// create the output path of the files to be opened
Status TraceAnalyzer::CreateOutputFile(
    const std::string& type, const std::string& cf_name,
    const std::string& ending, std::unique_ptr<rocksdb::WritableFile>* f_ptr) {
  std::string path;
  path = output_path_ + "/" + FLAGS_output_prefix + "-" + type + "-" + cf_name +
         "-" + ending;
  Status s;
  s = env_->NewWritableFile(path, f_ptr, env_options_);
  if (!s.ok()) {
    fprintf(stderr, "Cannot open file: %s\n", path.c_str());
    exit(1);
  }
  return Status::OK();
}

// Close the output files in the TraceStats if they are opened
void TraceAnalyzer::CloseOutputFiles() {
  for (int type = 0; type < kTaTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    for (auto& i : ta_[type].stats) {
      if (i.second.time_series_f) {
        i.second.time_series_f->Close();
      }

      if (i.second.a_key_f) {
        i.second.a_key_f->Close();
      }

      if (i.second.a_count_dist_f) {
        i.second.a_count_dist_f->Close();
      }

      if (i.second.a_prefix_cut_f) {
        i.second.a_prefix_cut_f->Close();
      }

      if (i.second.a_value_size_f) {
        i.second.a_value_size_f->Close();
      }

      if (i.second.a_qps_f) {
        i.second.a_qps_f->Close();
      }

      if (i.second.a_top_qps_prefix_f) {
        i.second.a_top_qps_prefix_f->Close();
      }

      if (i.second.w_key_f) {
        i.second.w_key_f->Close();
      }
      if (i.second.w_prefix_cut_f) {
        i.second.w_prefix_cut_f->Close();
      }
    }
  }
  return;
}

// Handle the Get request in the trace
Status TraceAnalyzer::HandleGetCF(uint32_t column_family_id,
                                  const std::string& key, const uint64_t& ts,
                                  const uint32_t& get_ret) {
  Status s;
  size_t value_size = 0;
  if (FLAGS_output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(TraceOperationType::kGet, column_family_id, key,
                           value_size, ts);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[TraceOperationType::kGet].enabled) {
    return Status::OK();
  }
  if (get_ret == 1) {
    value_size = 10;
  }
  s = KeyStatsInsertion(TraceOperationType::kGet, column_family_id, key,
                        value_size, ts);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Handle the Put request in the write batch of the trace
Status TraceAnalyzer::HandlePutCF(uint32_t column_family_id, const Slice& key,
                                  const Slice& value) {
  Status s;
  size_t value_size = value.ToString().size();
  if (FLAGS_output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(TraceOperationType::kPut, column_family_id,
                           key.ToString(), value_size, c_time_);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[TraceOperationType::kPut].enabled) {
    return Status::OK();
  }
  s = KeyStatsInsertion(TraceOperationType::kPut, column_family_id,
                        key.ToString(), value_size, c_time_);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Handle the Delete request in the write batch of the trace
Status TraceAnalyzer::HandleDeleteCF(uint32_t column_family_id,
                                     const Slice& key) {
  Status s;
  size_t value_size = 0;
  if (FLAGS_output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(TraceOperationType::kDelete, column_family_id,
                           key.ToString(), value_size, c_time_);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[TraceOperationType::kDelete].enabled) {
    return Status::OK();
  }
  s = KeyStatsInsertion(TraceOperationType::kDelete, column_family_id,
                        key.ToString(), value_size, c_time_);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Handle the SingleDelete request in the write batch of the trace
Status TraceAnalyzer::HandleSingleDeleteCF(uint32_t column_family_id,
                                           const Slice& key) {
  Status s;
  size_t value_size = 0;
  if (FLAGS_output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(TraceOperationType::kSingleDelete, column_family_id,
                           key.ToString(), value_size, c_time_);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[TraceOperationType::kSingleDelete].enabled) {
    return Status::OK();
  }
  s = KeyStatsInsertion(TraceOperationType::kSingleDelete, column_family_id,
                        key.ToString(), value_size, c_time_);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Handle the DeleteRange request in the write batch of the trace
Status TraceAnalyzer::HandleDeleteRangeCF(uint32_t column_family_id,
                                          const Slice& begin_key,
                                          const Slice& end_key) {
  Status s;
  size_t value_size = 0;
  if (FLAGS_output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(TraceOperationType::kRangeDelete, column_family_id,
                           begin_key.ToString(), value_size, c_time_);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[TraceOperationType::kRangeDelete].enabled) {
    return Status::OK();
  }
  s = KeyStatsInsertion(TraceOperationType::kRangeDelete, column_family_id,
                        begin_key.ToString(), value_size, c_time_);
  s = KeyStatsInsertion(TraceOperationType::kRangeDelete, column_family_id,
                        end_key.ToString(), value_size, c_time_);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Handle the Merge request in the write batch of the trace
Status TraceAnalyzer::HandleMergeCF(uint32_t column_family_id, const Slice& key,
                                    const Slice& value) {
  Status s;
  size_t value_size = value.ToString().size();
  if (FLAGS_output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(TraceOperationType::kMerge, column_family_id,
                           key.ToString(), value_size, c_time_);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[TraceOperationType::kMerge].enabled) {
    return Status::OK();
  }
  s = KeyStatsInsertion(TraceOperationType::kMerge, column_family_id,
                        key.ToString(), value_size, c_time_);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Handle the Iterator request in the trace
Status TraceAnalyzer::HandleIterCF(uint32_t column_family_id,
                                   const std::string& key, const uint64_t& ts) {
  Status s;
  size_t value_size = 0;
  if (FLAGS_output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(TraceOperationType::kIter, column_family_id, key,
                           value_size, ts);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[TraceOperationType::kIter].enabled) {
    return Status::OK();
  }
  s = KeyStatsInsertion(TraceOperationType::kIter, column_family_id, key,
                        value_size, ts);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Before the analyzer is closed, the requested general statistic results are
// printed out here. In current stage, these information are not output to
// the files.
// -----type
//          |__cf_id
//                |_statistics
void TraceAnalyzer::PrintGetStatistics() {
  for (int type = 0; type < kTaTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    ta_[type].total_keys = 0;
    ta_[type].total_access = 0;
    ta_[type].total_succ_access = 0;
    printf("\n################# Operation Type: %s #####################\n",
           ta_[type].type_name.c_str());
    if (qps_ave_.size() == kTaTypeNum + 1) {
      printf("Peak IO is: %u Average IO is: %f\n", qps_peak_[type],
             qps_ave_[type]);
    }
    for (auto& i : ta_[type].stats) {
      if (i.second.a_count == 0) {
        continue;
      }
      TraceStats& stats = i.second;

      uint64_t total_a_keys = static_cast<uint64_t>(stats.a_key_stats.size());
      double key_size_ave =
          (static_cast<double>(stats.a_key_size_sum)) / stats.a_count;
      double value_size_ave =
          (static_cast<double>(stats.a_value_size_sum)) / stats.a_count;
      double key_size_vari =
          sqrt((static_cast<double>(stats.a_key_size_sqsum)) / stats.a_count -
               key_size_ave * key_size_ave);
      double value_size_vari =
          sqrt((static_cast<double>(stats.a_value_size_sqsum)) / stats.a_count -
               value_size_ave * value_size_ave);
      if (value_size_ave == 0.0) {
        stats.a_value_mid = 0;
      }
      cfs_[stats.cf_id].a_count += total_a_keys;
      ta_[type].total_keys += total_a_keys;
      ta_[type].total_access += stats.a_count;
      ta_[type].total_succ_access += stats.a_succ_count;
      printf("*********************************************************\n");
      printf("colume family id: %u\n", stats.cf_id);
      printf("Total unique keys in this cf: %" PRIu64 "\n", total_a_keys);
      printf("Average key size: %f key size medium: %" PRIu64
             " Key size Variation: %f\n",
             key_size_ave, stats.a_key_mid, key_size_vari);
      if (type == kPut || type == kMerge) {
        printf("Average value size: %f Value size medium: %" PRIu64
               " Value size variation: %f\n",
               value_size_ave, stats.a_value_mid, value_size_vari);
      }
      printf("Peak QPS is: %u Average QPS is: %f\n", stats.a_peak_qps,
             stats.a_ave_qps);

      // print the top k accessed key and its access count
      if (FLAGS_print_top_k_access > 0) {
        printf("The Top %d keys that are accessed:\n",
               FLAGS_print_top_k_access);
        while (!stats.top_k_queue.empty()) {
          std::string hex_key =
              rocksdb::LDBCommand::StringToHex(stats.top_k_queue.top().second);
          printf("Access_count: %" PRIu64 " %s\n",
                 stats.top_k_queue.top().first, hex_key.c_str());
          stats.top_k_queue.pop();
        }
      }

      // print the top k access prefix range and
      // top k prefix range with highest average access per key
      if (FLAGS_output_prefix_cut > 0) {
        printf("The Top %d accessed prefix range:\n", FLAGS_print_top_k_access);
        while (!stats.top_k_prefix_access.empty()) {
          printf("Prefix: %s Access count: %" PRIu64 "\n",
                 stats.top_k_prefix_access.top().second.c_str(),
                 stats.top_k_prefix_access.top().first);
          stats.top_k_prefix_access.pop();
        }

        printf("The Top %d prefix with highest access per key:\n",
               FLAGS_print_top_k_access);
        while (!stats.top_k_prefix_ave.empty()) {
          printf("Prefix: %s access per key: %f\n",
                 stats.top_k_prefix_ave.top().second.c_str(),
                 stats.top_k_prefix_ave.top().first);
          stats.top_k_prefix_ave.pop();
        }
      }

      // print the key size distribution
      if (FLAGS_print_key_distribution) {
        printf("The key size distribution\n");
        for (auto& it : stats.a_key_size_stats) {
          printf("key_size %" PRIu64 " nums: %" PRIu64 "\n", it.first,
                 it.second);
        }
      }

      // print the operation correlations
      if (!FLAGS_output_correlation.empty()) {
        for (int corre = 0;
             corre < static_cast<int>(analyzer_opts_.corre_list.size());
             corre++) {
          printf("The correlation statistics of '%s' after '%s' is:",
                 taIndexToOpt[analyzer_opts_.corre_list[corre].second].c_str(),
                 taIndexToOpt[analyzer_opts_.corre_list[corre].first].c_str());
          double corre_ave =
              (static_cast<double>(stats.corre_output[corre].second)) /
              (stats.corre_output[corre].first * 1000);
          printf(" total numbers: %" PRIu64 " average time: %f(ms)\n",
                 stats.corre_output[corre].first, corre_ave);
        }
      }
    }
    printf("*********************************************************\n");
    printf("Total keys of '%s' is: %" PRIu64 "\n", ta_[type].type_name.c_str(),
           ta_[type].total_keys);
    printf("Total access is: %" PRIu64 "\n", ta_[type].total_access);
    total_access_keys_ += ta_[type].total_keys;
  }

  // Print the overall statistic information of the trace
  printf("\n*********************************************************\n");
  printf("*********************************************************\n");
  printf("The column family based statistics\n");
  for (auto& it : cfs_) {
    printf("The column family id: %u\n", it.first);
    printf("The whole key space key numbers: %" PRIu64 "\n", it.second.w_count);
    printf("The accessed key space key numbers: %" PRIu64 "\n",
           it.second.a_count);
  }

  if (FLAGS_print_overall_stats) {
    printf("\n*********************************************************\n");
    printf("*********************************************************\n");
    if (qps_peak_.size() == kTaTypeNum + 1) {
      printf("Average QPS per second: %f Peak QPS: %u\n", qps_ave_[kTaTypeNum],
             qps_peak_[kTaTypeNum]);
    }
    printf("Total_requests: %" PRIu64 " Total_accessed_keys: %" PRIu64
           " Total_gets: %" PRIu64 " Total_write_batch: %" PRIu64 "\n",
           total_requests_, total_access_keys_, total_gets_, total_writes_);
    for (int type = 0; type < kTaTypeNum; type++) {
      if (!ta_[type].enabled) {
        continue;
      }
      printf("Operation: '%s' has: %" PRIu64 "\n", ta_[type].type_name.c_str(),
             ta_[type].total_access);
    }
  }
}

// Write the trace sequence to file
Status TraceAnalyzer::WriteTraceSequence(const uint32_t& type,
                                         const uint32_t& cf_id,
                                         const std::string& key,
                                         const size_t value_size,
                                         const uint64_t ts) {
  std::string hex_key = rocksdb::LDBCommand::StringToHex(key);
  int ret;
  ret =
      sprintf(buffer_, "%u %u %zu %" PRIu64 "\n", type, cf_id, value_size, ts);
  if (ret < 0) {
    return Status::IOError("failed to write the file");
  }
  std::string printout(buffer_);
  if (!FLAGS_no_key) {
    printout = hex_key + " " + printout;
  }
  return trace_sequence_f_->Append(printout);
}

// The entrance function of Trace_Analyzer
int trace_analyzer_tool(int argc, char** argv) {
  std::string trace_path;
  std::string output_path;

  AnalyzerOptions analyzer_opts;

  ParseCommandLineFlags(&argc, &argv, true);

  if (!FLAGS_output_correlation.empty()) {
    analyzer_opts.SparseCorreInput(FLAGS_output_correlation);
  }

  TraceAnalyzer* analyzer =
      new TraceAnalyzer(FLAGS_trace_path, FLAGS_output_dir, analyzer_opts);

  if (analyzer == nullptr) {
    fprintf(stderr, "Cannot initiate the trace analyzer\n");
    exit(1);
  }

  rocksdb::Status s = analyzer->PrepareProcessing();
  if (!s.ok()) {
    fprintf(stderr, "Cannot initiate the trace reader\n");
    delete analyzer;
    exit(1);
  }

  s = analyzer->StartProcessing();
  if (!s.ok()) {
    fprintf(stderr, "Cannot processing the trace\n");
    delete analyzer;
    exit(1);
  }

  s = analyzer->MakeStatistics();
  if (!s.ok()) {
    analyzer->EndProcessing();
    fprintf(stderr, "Cannot make the statistics\n");
    delete analyzer;
    exit(1);
  }

  s = analyzer->ReProcessing();
  if (!s.ok()) {
    fprintf(stderr, "Cannot re-process the trace for more statistics\n");
    analyzer->EndProcessing();
    delete analyzer;
    exit(1);
  }

  s = analyzer->EndProcessing();
  if (!s.ok()) {
    fprintf(stderr, "Cannot close the trace analyzer\n");
    delete analyzer;
    exit(1);
  }

  delete analyzer;
  return 0;
}
}  // namespace rocksdb

#endif
