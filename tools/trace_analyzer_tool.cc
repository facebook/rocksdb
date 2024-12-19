//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#ifdef GFLAGS
#ifdef NUMA
#include <numa.h>
#endif
#ifndef OS_WIN
#include <unistd.h>
#endif
#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <stdexcept>

#include "db/db_impl/db_impl.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "env/composite_env_wrapper.h"
#include "file/line_file_reader.h"
#include "file/writable_file_writer.h"
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
#include "table/table_reader.h"
#include "tools/trace_analyzer_tool.h"
#include "trace_replay/trace_replay.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/gflags_compat.h"
#include "util/random.h"
#include "util/string_util.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

DEFINE_string(trace_path, "", "The trace file path.");
DEFINE_string(output_dir, "", "The directory to store the output files.");
DEFINE_string(output_prefix, "trace",
              "The prefix used for all the output files.");
DEFINE_bool(output_key_stats, false,
            "Output the key access count statistics to file\n"
            "for accessed keys:\n"
            "file name: <prefix>-<query_type>-<cf_id>-accessed_key_stats.txt\n"
            "Format:[cf_id value_size access_keyid access_count]\n"
            "for the whole key space keys:\n"
            "File name: <prefix>-<query_type>-<cf_id>-whole_key_stats.txt\n"
            "Format:[whole_key_space_keyid access_count]");
DEFINE_bool(output_access_count_stats, false,
            "Output the access count distribution statistics to file.\n"
            "File name:  <prefix>-<query_type>-<cf_id>-accessed_"
            "key_count_distribution.txt \n"
            "Format:[access_count number_of_access_count]");
DEFINE_bool(output_time_series, false,
            "Output the access time in second of each key, "
            "such that we can have the time series data of the queries \n"
            "File name: <prefix>-<query_type>-<cf_id>-time_series.txt\n"
            "Format:[type_id time_in_sec access_keyid].");
DEFINE_bool(try_process_corrupted_trace, false,
            "In default, trace_analyzer will exit if the trace file is "
            "corrupted due to the unexpected tracing cases. If this option "
            "is enabled, trace_analyzer will stop reading the trace file, "
            "and start analyzing the read-in data.");
DEFINE_int32(output_prefix_cut, 0,
             "The number of bytes as prefix to cut the keys.\n"
             "If it is enabled, it will generate the following:\n"
             "For accessed keys:\n"
             "File name: <prefix>-<query_type>-<cf_id>-"
             "accessed_key_prefix_cut.txt \n"
             "Format:[acessed_keyid access_count_of_prefix "
             "number_of_keys_in_prefix average_key_access "
             "prefix_succ_ratio prefix]\n"
             "For whole key space keys:\n"
             "File name: <prefix>-<query_type>-<cf_id>"
             "-whole_key_prefix_cut.txt\n"
             "Format:[start_keyid_in_whole_keyspace prefix]\n"
             "if 'output_qps_stats' and 'top_k' are enabled, it will output:\n"
             "File name: <prefix>-<query_type>-<cf_id>"
             "-accessed_top_k_qps_prefix_cut.txt\n"
             "Format:[the_top_ith_qps_time QPS], [prefix qps_of_this_second].");
DEFINE_bool(convert_to_human_readable_trace, false,
            "Convert the binary trace file to a human readable txt file "
            "for further processing. "
            "This file will be extremely large "
            "(similar size as the original binary trace file). "
            "You can specify 'no_key' to reduce the size, if key is not "
            "needed in the next step.\n"
            "File name: <prefix>_human_readable_trace.txt\n"
            "Format:[<key> type_id cf_id value_size time_in_micorsec].");
DEFINE_bool(output_qps_stats, false,
            "Output the query per second(qps) statistics \n"
            "For the overall qps, it will contain all qps of each query type. "
            "The time is started from the first trace record\n"
            "File name: <prefix>_qps_stats.txt\n"
            "Format: [qps_type_1 qps_type_2 ...... overall_qps]\n"
            "For each cf and query, it will have its own qps output.\n"
            "File name: <prefix>-<query_type>-<cf_id>_qps_stats.txt \n"
            "Format:[query_count_in_this_second].");
DEFINE_bool(no_print, false, "Do not print out any result");
DEFINE_string(
    print_correlation, "",
    "intput format: [correlation pairs][.,.]\n"
    "Output the query correlations between the pairs of query types "
    "listed in the parameter, input should select the operations from:\n"
    "get, put, delete, single_delete, rangle_delete, merge. No space "
    "between the pairs separated by commar. Example: =[get,get]... "
    "It will print out the number of pairs of 'A after B' and "
    "the average time interval between the two query.");
DEFINE_string(key_space_dir, "",
              "<the directory stores full key space files> \n"
              "The key space files should be: <column family id>.txt");
DEFINE_bool(analyze_get, false, "Analyze the Get query.");
DEFINE_bool(analyze_put, false, "Analyze the Put query.");
DEFINE_bool(analyze_delete, false, "Analyze the Delete query.");
DEFINE_bool(analyze_single_delete, false, "Analyze the SingleDelete query.");
DEFINE_bool(analyze_range_delete, false, "Analyze the DeleteRange query.");
DEFINE_bool(analyze_merge, false, "Analyze the Merge query.");
DEFINE_bool(analyze_iterator, false,
            " Analyze the iterate query like Seek() and SeekForPrev().");
DEFINE_bool(analyze_multiget, false,
            " Analyze the MultiGet query. NOTE: for"
            " MultiGet, we analyze each KV-pair read in one MultiGet query. "
            "Therefore, the total queries and QPS are calculated based on "
            "the number of KV-pairs being accessed not the number of MultiGet."
            "It can be improved in the future if needed");
DEFINE_bool(no_key, false,
            " Does not output the key to the result files to make smaller.");
DEFINE_bool(print_overall_stats, true,
            " Print the stats of the whole trace, "
            "like total requests, keys, and etc.");
DEFINE_bool(output_key_distribution, false, "Print the key size distribution.");
DEFINE_bool(
    output_value_distribution, false,
    "Out put the value size distribution, only available for Put and Merge.\n"
    "File name: <prefix>-<query_type>-<cf_id>"
    "-accessed_value_size_distribution.txt\n"
    "Format:[Number_of_value_size_between x and "
    "x+value_interval is: <the count>]");
DEFINE_int32(print_top_k_access, 1,
             "<top K of the variables to be printed> "
             "Print the top k accessed keys, top k accessed prefix "
             "and etc.");
DEFINE_int32(output_ignore_count, 0,
             "<threshold>, ignores the access count <= this value, "
             "it will shorter the output.");
DEFINE_int32(value_interval, 8,
             "To output the value distribution, we need to set the value "
             "intervals and make the statistic of the value size distribution "
             "in different intervals. The default is 8.");
DEFINE_double(sample_ratio, 1.0,
              "If the trace size is extremely huge or user want to sample "
              "the trace when analyzing, sample ratio can be set (0, 1.0]");

namespace ROCKSDB_NAMESPACE {

const size_t kShadowValueSize = 10;

std::map<std::string, int> taOptToIndex = {
    {"get", kGet},
    {"put", kPut},
    {"delete", kDelete},
    {"single_delete", kSingleDelete},
    {"range_delete", kRangeDelete},
    {"merge", kMerge},
    {"iterator_Seek", kIteratorSeek},
    {"iterator_SeekForPrev", kIteratorSeekForPrev},
    {"multiget", kMultiGet}};

std::map<int, std::string> taIndexToOpt = {
    {kGet, "get"},
    {kPut, "put"},
    {kDelete, "delete"},
    {kSingleDelete, "single_delete"},
    {kRangeDelete, "range_delete"},
    {kMerge, "merge"},
    {kIteratorSeek, "iterator_Seek"},
    {kIteratorSeekForPrev, "iterator_SeekForPrev"},
    {kMultiGet, "multiget"}};

namespace {

uint64_t MultiplyCheckOverflow(uint64_t op1, uint64_t op2) {
  if (op1 == 0 || op2 == 0) {
    return 0;
  }
  if (std::numeric_limits<uint64_t>::max() / op1 < op2) {
    return op1;
  }
  return (op1 * op2);
}

}  // namespace

// The default constructor of AnalyzerOptions
AnalyzerOptions::AnalyzerOptions()
    : correlation_map(kTaTypeNum, std::vector<int>(kTaTypeNum, -1)) {}

AnalyzerOptions::~AnalyzerOptions() = default;

void AnalyzerOptions::SparseCorrelationInput(const std::string& in_str) {
  std::string cur = in_str;
  if (cur.size() == 0) {
    return;
  }
  while (!cur.empty()) {
    if (cur.compare(0, 1, "[") != 0) {
      fprintf(stderr, "Invalid correlation input: %s\n", in_str.c_str());
      exit(1);
    }
    std::string opt1, opt2;
    std::size_t split = cur.find_first_of(',');
    if (split != std::string::npos) {
      opt1 = cur.substr(1, split - 1);
    } else {
      fprintf(stderr, "Invalid correlation input: %s\n", in_str.c_str());
      exit(1);
    }
    std::size_t end = cur.find_first_of(']');
    if (end != std::string::npos) {
      opt2 = cur.substr(split + 1, end - split - 1);
    } else {
      fprintf(stderr, "Invalid correlation input: %s\n", in_str.c_str());
      exit(1);
    }
    cur = cur.substr(end + 1);

    if (taOptToIndex.find(opt1) != taOptToIndex.end() &&
        taOptToIndex.find(opt2) != taOptToIndex.end()) {
      correlation_list.emplace_back(taOptToIndex[opt1], taOptToIndex[opt2]);
    } else {
      fprintf(stderr, "Invalid correlation input: %s\n", in_str.c_str());
      exit(1);
    }
  }

  int sequence = 0;
  for (auto& it : correlation_list) {
    correlation_map[it.first][it.second] = sequence;
    sequence++;
  }
}

// The trace statistic struct constructor
TraceStats::TraceStats() {
  cf_id = 0;
  cf_name = "0";
  a_count = 0;
  a_key_id = 0;
  a_key_size_sqsum = 0;
  a_key_size_sum = 0;
  a_key_mid = 0;
  a_value_size_sqsum = 0;
  a_value_size_sum = 0;
  a_value_mid = 0;
  a_peak_qps = 0;
  a_ave_qps = 0.0;
}

TraceStats::~TraceStats() = default;

// The trace analyzer constructor
TraceAnalyzer::TraceAnalyzer(std::string& trace_path, std::string& output_path,
                             AnalyzerOptions _analyzer_opts)
    : write_batch_ts_(0),
      trace_name_(trace_path),
      output_path_(output_path),
      analyzer_opts_(_analyzer_opts) {
  ROCKSDB_NAMESPACE::EnvOptions env_options;
  env_ = ROCKSDB_NAMESPACE::Env::Default();
  offset_ = 0;
  total_requests_ = 0;
  total_access_keys_ = 0;
  total_gets_ = 0;
  total_writes_ = 0;
  total_seeks_ = 0;
  total_seek_prevs_ = 0;
  total_multigets_ = 0;
  trace_create_time_ = 0;
  begin_time_ = 0;
  end_time_ = 0;
  time_series_start_ = 0;
  cur_time_sec_ = 0;
  if (FLAGS_sample_ratio > 1.0 || FLAGS_sample_ratio <= 0) {
    sample_max_ = 1;
  } else {
    sample_max_ = static_cast<uint32_t>(1.0 / FLAGS_sample_ratio);
  }

  ta_.resize(kTaTypeNum);
  ta_[kGet].type_name = "get";
  if (FLAGS_analyze_get) {
    ta_[kGet].enabled = true;
  } else {
    ta_[kGet].enabled = false;
  }
  ta_[kPut].type_name = "put";
  if (FLAGS_analyze_put) {
    ta_[kPut].enabled = true;
  } else {
    ta_[kPut].enabled = false;
  }
  ta_[kDelete].type_name = "delete";
  if (FLAGS_analyze_delete) {
    ta_[kDelete].enabled = true;
  } else {
    ta_[kDelete].enabled = false;
  }
  ta_[kSingleDelete].type_name = "single_delete";
  if (FLAGS_analyze_single_delete) {
    ta_[kSingleDelete].enabled = true;
  } else {
    ta_[kSingleDelete].enabled = false;
  }
  ta_[kRangeDelete].type_name = "range_delete";
  if (FLAGS_analyze_range_delete) {
    ta_[kRangeDelete].enabled = true;
  } else {
    ta_[kRangeDelete].enabled = false;
  }
  ta_[kMerge].type_name = "merge";
  if (FLAGS_analyze_merge) {
    ta_[kMerge].enabled = true;
  } else {
    ta_[kMerge].enabled = false;
  }
  ta_[kIteratorSeek].type_name = "iterator_Seek";
  if (FLAGS_analyze_iterator) {
    ta_[kIteratorSeek].enabled = true;
  } else {
    ta_[kIteratorSeek].enabled = false;
  }
  ta_[kIteratorSeekForPrev].type_name = "iterator_SeekForPrev";
  if (FLAGS_analyze_iterator) {
    ta_[kIteratorSeekForPrev].enabled = true;
  } else {
    ta_[kIteratorSeekForPrev].enabled = false;
  }
  ta_[kMultiGet].type_name = "multiget";
  if (FLAGS_analyze_multiget) {
    ta_[kMultiGet].enabled = true;
  } else {
    ta_[kMultiGet].enabled = false;
  }
  for (int i = 0; i < kTaTypeNum; i++) {
    ta_[i].sample_count = 0;
  }
}

TraceAnalyzer::~TraceAnalyzer() = default;

// Prepare the processing
// Initiate the global trace reader and writer here
Status TraceAnalyzer::PrepareProcessing() {
  Status s;
  // Prepare the trace reader
  if (trace_reader_ == nullptr) {
    s = NewFileTraceReader(env_, env_options_, trace_name_, &trace_reader_);
  } else {
    s = trace_reader_->Reset();
  }
  if (!s.ok()) {
    return s;
  }

  // Prepare and open the trace sequence file writer if needed
  if (FLAGS_convert_to_human_readable_trace) {
    std::string trace_sequence_name;
    trace_sequence_name =
        output_path_ + "/" + FLAGS_output_prefix + "-human_readable_trace.txt";
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

    qps_stats_name =
        output_path_ + "/" + FLAGS_output_prefix + "-cf_qps_stats.txt";
    s = env_->NewWritableFile(qps_stats_name, &cf_qps_f_, env_options_);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status TraceAnalyzer::ReadTraceHeader(Trace* header) {
  assert(header != nullptr);
  std::string encoded_trace;
  // Read the trace head
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }

  s = TracerHelper::DecodeTrace(encoded_trace, header);

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
  return TracerHelper::DecodeTrace(encoded_trace, trace);
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
  // Set the default trace file version as version 0.2
  int trace_file_version = 2;
  s = TracerHelper::ParseTraceHeader(header, &trace_file_version, &db_version_);
  if (!s.ok()) {
    return s;
  }
  trace_create_time_ = header.ts;
  if (FLAGS_output_time_series) {
    time_series_start_ = header.ts;
  }

  Trace trace;
  std::unique_ptr<TraceRecord> record;
  while (s.ok()) {
    trace.reset();
    s = ReadTraceRecord(&trace);
    if (!s.ok()) {
      break;
    }

    end_time_ = trace.ts;
    if (trace.type == kTraceEnd) {
      break;
    }
    // Do not count TraceEnd (if there is one)
    total_requests_++;

    s = TracerHelper::DecodeTraceRecord(&trace, trace_file_version, &record);
    if (s.IsNotSupported()) {
      continue;
    }
    if (!s.ok()) {
      return s;
    }
    s = record->Accept(this, nullptr);
    if (!s.ok()) {
      fprintf(stderr, "Cannot process the TraceRecord\n");
      return s;
    }
  }
  if (s.IsIncomplete()) {
    // Fix it: Reaching eof returns Incomplete status at the moment.
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
    for (auto& stat : ta_[type].stats) {
      stat.second.a_key_id = 0;
      for (auto& record : stat.second.a_key_stats) {
        record.second.key_id = stat.second.a_key_id;
        stat.second.a_key_id++;
        if (record.second.access_count <=
            static_cast<uint64_t>(FLAGS_output_ignore_count)) {
          continue;
        }

        // Generate the key access count distribution data
        if (FLAGS_output_access_count_stats) {
          if (stat.second.a_count_stats.find(record.second.access_count) ==
              stat.second.a_count_stats.end()) {
            stat.second.a_count_stats[record.second.access_count] = 1;
          } else {
            stat.second.a_count_stats[record.second.access_count]++;
          }
        }

        // Generate the key size distribution data
        if (FLAGS_output_key_distribution) {
          if (stat.second.a_key_size_stats.find(record.first.size()) ==
              stat.second.a_key_size_stats.end()) {
            stat.second.a_key_size_stats[record.first.size()] = 1;
          } else {
            stat.second.a_key_size_stats[record.first.size()]++;
          }
        }

        if (!FLAGS_print_correlation.empty()) {
          s = MakeStatisticCorrelation(stat.second, record.second);
          if (!s.ok()) {
            return s;
          }
        }
      }

      // Output the prefix cut or the whole content of the accessed key space
      if (FLAGS_output_key_stats || FLAGS_output_prefix_cut > 0) {
        s = MakeStatisticKeyStatsOrPrefix(stat.second);
        if (!s.ok()) {
          return s;
        }
      }

      // output the access count distribution
      if (FLAGS_output_access_count_stats && stat.second.a_count_dist_f) {
        for (auto& record : stat.second.a_count_stats) {
          ret = snprintf(buffer_, sizeof(buffer_),
                         "access_count: %" PRIu64 " num: %" PRIu64 "\n",
                         record.first, record.second);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = stat.second.a_count_dist_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write access count distribution file failed\n");
            return s;
          }
        }
      }

      // find the medium of the key size
      uint64_t k_count = 0;
      bool get_mid = false;
      for (auto& record : stat.second.a_key_size_stats) {
        k_count += record.second;
        if (!get_mid && k_count >= stat.second.a_key_mid) {
          stat.second.a_key_mid = record.first;
          get_mid = true;
        }
        if (FLAGS_output_key_distribution && stat.second.a_key_size_f) {
          ret = snprintf(buffer_, sizeof(buffer_), "%" PRIu64 " %" PRIu64 "\n",
                         record.first, record.second);
          if (ret < 0) {
            return Status::IOError("Format output failed");
          }
          std::string printout(buffer_);
          s = stat.second.a_key_size_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write key size distribution file failed\n");
            return s;
          }
        }
      }

      // output the value size distribution
      uint64_t v_begin = 0, v_end = 0, v_count = 0;
      get_mid = false;
      for (auto& record : stat.second.a_value_size_stats) {
        v_begin = v_end;
        v_end = (record.first + 1) * FLAGS_value_interval;
        v_count += record.second;
        if (!get_mid && v_count >= stat.second.a_count / 2) {
          stat.second.a_value_mid = (v_begin + v_end) / 2;
          get_mid = true;
        }
        if (FLAGS_output_value_distribution && stat.second.a_value_size_f &&
            (type == TraceOperationType::kPut ||
             type == TraceOperationType::kMerge)) {
          ret = snprintf(buffer_, sizeof(buffer_),
                         "Number_of_value_size_between %" PRIu64 " and %" PRIu64
                         " is: %" PRIu64 "\n",
                         v_begin, v_end, record.second);
          if (ret < 0) {
            return Status::IOError("Format output failed");
          }
          std::string printout(buffer_);
          s = stat.second.a_value_size_f->Append(printout);
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
  for (auto& record : stats.a_key_stats) {
    // write the key access statistic file
    if (!stats.a_key_f) {
      return Status::IOError("Failed to open accessed_key_stats file.");
    }
    stats.a_succ_count += record.second.succ_count;
    double succ_ratio = 0.0;
    if (record.second.access_count > 0) {
      succ_ratio = (static_cast<double>(record.second.succ_count)) /
                   record.second.access_count;
    }
    ret = snprintf(buffer_, sizeof(buffer_),
                   "%u %zu %" PRIu64 " %" PRIu64 " %f\n", record.second.cf_id,
                   record.second.value_size, record.second.key_id,
                   record.second.access_count, succ_ratio);
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
      if (record.first.compare(0, FLAGS_output_prefix_cut, prefix) != 0) {
        std::string prefix_out =
            ROCKSDB_NAMESPACE::LDBCommand::StringToHex(prefix);
        if (prefix_count == 0) {
          prefix_ave_access = 0.0;
        } else {
          prefix_ave_access =
              (static_cast<double>(prefix_access)) / prefix_count;
        }
        double prefix_succ_ratio = 0.0;
        if (prefix_access > 0) {
          prefix_succ_ratio =
              (static_cast<double>(prefix_succ_access)) / prefix_access;
        }
        ret =
            snprintf(buffer_, sizeof(buffer_),
                     "%" PRIu64 " %" PRIu64 " %" PRIu64 " %f %f %s\n",
                     record.second.key_id, prefix_access, prefix_count,
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

        prefix = record.first.substr(0, FLAGS_output_prefix_cut);
        prefix_access = 0;
        prefix_count = 0;
        prefix_succ_access = 0;
      }
      prefix_access += record.second.access_count;
      prefix_count += 1;
      prefix_succ_access += record.second.succ_count;
    }
  }
  return Status::OK();
}

// Process the statistics of different query type
// correlations
Status TraceAnalyzer::MakeStatisticCorrelation(TraceStats& stats,
                                               StatsUnit& unit) {
  if (stats.correlation_output.size() !=
      analyzer_opts_.correlation_list.size()) {
    return Status::Corruption("Cannot make the statistic of correlation.");
  }

  for (int i = 0; i < static_cast<int>(analyzer_opts_.correlation_list.size());
       i++) {
    if (i >= static_cast<int>(stats.correlation_output.size()) ||
        i >= static_cast<int>(unit.v_correlation.size())) {
      break;
    }
    stats.correlation_output[i].first += unit.v_correlation[i].count;
    stats.correlation_output[i].second += unit.v_correlation[i].total_ts;
  }
  return Status::OK();
}

// Process the statistics of QPS
Status TraceAnalyzer::MakeStatisticQPS() {
  if (begin_time_ == 0) {
    begin_time_ = trace_create_time_;
  }
  uint32_t duration =
      static_cast<uint32_t>((end_time_ - begin_time_) / 1000000);
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
    for (auto& stat : ta_[type].stats) {
      uint32_t time_line = 0;
      uint64_t cf_qps_sum = 0;
      for (auto& time_it : stat.second.a_qps_stats) {
        if (time_it.first >= duration) {
          continue;
        }
        type_qps[time_it.first][kTaTypeNum] += time_it.second;
        type_qps[time_it.first][type] += time_it.second;
        cf_qps_sum += time_it.second;
        if (time_it.second > stat.second.a_peak_qps) {
          stat.second.a_peak_qps = time_it.second;
        }
        if (stat.second.a_qps_f) {
          while (time_line < time_it.first) {
            ret = snprintf(buffer_, sizeof(buffer_), "%u\n", 0);
            if (ret < 0) {
              return Status::IOError("Format the output failed");
            }
            std::string printout(buffer_);
            s = stat.second.a_qps_f->Append(printout);
            if (!s.ok()) {
              fprintf(stderr, "Write QPS file failed\n");
              return s;
            }
            time_line++;
          }
          ret = snprintf(buffer_, sizeof(buffer_), "%u\n", time_it.second);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = stat.second.a_qps_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write QPS file failed\n");
            return s;
          }
          if (time_line == time_it.first) {
            time_line++;
          }
        }

        // Process the top k QPS peaks
        if (FLAGS_output_prefix_cut > 0) {
          if (static_cast<int32_t>(stat.second.top_k_qps_sec.size()) <
              FLAGS_print_top_k_access) {
            stat.second.top_k_qps_sec.push(
                std::make_pair(time_it.second, time_it.first));
          } else {
            if (stat.second.top_k_qps_sec.size() > 0 &&
                stat.second.top_k_qps_sec.top().first < time_it.second) {
              stat.second.top_k_qps_sec.pop();
              stat.second.top_k_qps_sec.push(
                  std::make_pair(time_it.second, time_it.first));
            }
          }
        }
      }
      if (duration == 0) {
        stat.second.a_ave_qps = 0;
      } else {
        stat.second.a_ave_qps = (static_cast<double>(cf_qps_sum)) / duration;
      }

      // Output the accessed unique key number change overtime
      if (stat.second.a_key_num_f) {
        uint64_t cur_uni_key =
            static_cast<uint64_t>(stat.second.a_key_stats.size());
        double cur_ratio = 0.0;
        uint64_t cur_num = 0;
        for (uint32_t i = 0; i < duration; i++) {
          auto find_time = stat.second.uni_key_num.find(i);
          if (find_time != stat.second.uni_key_num.end()) {
            cur_ratio = (static_cast<double>(find_time->second)) / cur_uni_key;
            cur_num = find_time->second;
          }
          ret = snprintf(buffer_, sizeof(buffer_), "%" PRIu64 " %.12f\n",
                         cur_num, cur_ratio);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = stat.second.a_key_num_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr,
                    "Write accessed unique key number change file failed\n");
            return s;
          }
        }
      }

      // output the prefix of top k access peak
      if (FLAGS_output_prefix_cut > 0 && stat.second.a_top_qps_prefix_f) {
        while (!stat.second.top_k_qps_sec.empty()) {
          ret = snprintf(buffer_, sizeof(buffer_), "At time: %u with QPS: %u\n",
                         stat.second.top_k_qps_sec.top().second,
                         stat.second.top_k_qps_sec.top().first);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = stat.second.a_top_qps_prefix_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write prefix QPS top K file failed\n");
            return s;
          }
          uint32_t qps_time = stat.second.top_k_qps_sec.top().second;
          stat.second.top_k_qps_sec.pop();
          if (stat.second.a_qps_prefix_stats.find(qps_time) !=
              stat.second.a_qps_prefix_stats.end()) {
            for (auto& qps_prefix : stat.second.a_qps_prefix_stats[qps_time]) {
              std::string qps_prefix_out =
                  ROCKSDB_NAMESPACE::LDBCommand::StringToHex(qps_prefix.first);
              ret = snprintf(buffer_, sizeof(buffer_),
                             "The prefix: %s Access count: %u\n",
                             qps_prefix_out.c_str(), qps_prefix.second);
              if (ret < 0) {
                return Status::IOError("Format the output failed");
              }
              std::string pout(buffer_);
              s = stat.second.a_top_qps_prefix_f->Append(pout);
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
          ret = snprintf(buffer_, sizeof(buffer_), "%u ", type_qps[i][type]);
        } else {
          ret = snprintf(buffer_, sizeof(buffer_), "%u\n", type_qps[i][type]);
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

  if (cf_qps_f_) {
    int cfs_size = static_cast<uint32_t>(cfs_.size());
    uint32_t v;
    for (uint32_t i = 0; i < duration; i++) {
      for (int cf = 0; cf < cfs_size; cf++) {
        if (cfs_[cf].cf_qps.find(i) != cfs_[cf].cf_qps.end()) {
          v = cfs_[cf].cf_qps[i];
        } else {
          v = 0;
        }
        if (cf < cfs_size - 1) {
          ret = snprintf(buffer_, sizeof(buffer_), "%u ", v);
        } else {
          ret = snprintf(buffer_, sizeof(buffer_), "%u\n", v);
        }
        if (ret < 0) {
          return Status::IOError("Format the output failed");
        }
        std::string printout(buffer_);
        s = cf_qps_f_->Append(printout);
        if (!s.ok()) {
          return s;
        }
      }
    }
  }

  qps_peak_ = qps_peak;
  for (int type = 0; type <= kTaTypeNum; type++) {
    if (duration == 0) {
      qps_ave_[type] = 0;
    } else {
      qps_ave_[type] = (static_cast<double>(qps_sum[type])) / duration;
    }
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
    if (FLAGS_output_time_series) {
      for (int type = 0; type < kTaTypeNum; type++) {
        if (!ta_[type].enabled ||
            ta_[type].stats.find(cf_id) == ta_[type].stats.end()) {
          continue;
        }
        TraceStats& stat = ta_[type].stats[cf_id];
        if (!stat.time_series_f) {
          fprintf(stderr, "Cannot write time_series of '%s' in '%u'\n",
                  ta_[type].type_name.c_str(), cf_id);
          continue;
        }
        while (!stat.time_series.empty()) {
          uint64_t key_id = 0;
          auto found = stat.a_key_stats.find(stat.time_series.front().key);
          if (found != stat.a_key_stats.end()) {
            key_id = found->second.key_id;
          }
          ret =
              snprintf(buffer_, sizeof(buffer_), "%u %" PRIu64 " %" PRIu64 "\n",
                       stat.time_series.front().type,
                       stat.time_series.front().ts, key_id);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = stat.time_series_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write time series file failed\n");
            return s;
          }
          stat.time_series.pop_front();
        }
      }
    }

    // process the whole key space if needed
    if (!FLAGS_key_space_dir.empty()) {
      std::string whole_key_path =
          FLAGS_key_space_dir + "/" + std::to_string(cf_id) + ".txt";
      std::string input_key, get_key;
      std::vector<std::string> prefix(kTaTypeNum);
      std::unique_ptr<FSSequentialFile> file;

      s = env_->GetFileSystem()->NewSequentialFile(
          whole_key_path, FileOptions(env_options_), &file, nullptr);
      if (!s.ok()) {
        fprintf(stderr, "Cannot open the whole key space file of CF: %u\n",
                cf_id);
        file.reset();
      }

      if (file) {
        size_t kTraceFileReadaheadSize = 2 * 1024 * 1024;
        LineFileReader lf_reader(
            std::move(file), whole_key_path,
            kTraceFileReadaheadSize /* filereadahead_size */);
        for (cfs_[cf_id].w_count = 0; lf_reader.ReadLine(
                 &get_key, Env::IO_TOTAL /* rate_limiter_priority */);
             ++cfs_[cf_id].w_count) {
          input_key = ROCKSDB_NAMESPACE::LDBCommand::HexToString(get_key);
          for (int type = 0; type < kTaTypeNum; type++) {
            if (!ta_[type].enabled) {
              continue;
            }
            TraceStats& stat = ta_[type].stats[cf_id];
            if (stat.w_key_f) {
              if (stat.a_key_stats.find(input_key) != stat.a_key_stats.end()) {
                ret = snprintf(buffer_, sizeof(buffer_),
                               "%" PRIu64 " %" PRIu64 "\n", cfs_[cf_id].w_count,
                               stat.a_key_stats[input_key].access_count);
                if (ret < 0) {
                  return Status::IOError("Format the output failed");
                }
                std::string printout(buffer_);
                s = stat.w_key_f->Append(printout);
                if (!s.ok()) {
                  fprintf(stderr, "Write whole key space access file failed\n");
                  return s;
                }
              }
            }

            // Output the prefix cut file of the whole key space
            if (FLAGS_output_prefix_cut > 0 && stat.w_prefix_cut_f) {
              if (input_key.compare(0, FLAGS_output_prefix_cut, prefix[type]) !=
                  0) {
                prefix[type] = input_key.substr(0, FLAGS_output_prefix_cut);
                std::string prefix_out =
                    ROCKSDB_NAMESPACE::LDBCommand::StringToHex(prefix[type]);
                ret = snprintf(buffer_, sizeof(buffer_), "%" PRIu64 " %s\n",
                               cfs_[cf_id].w_count, prefix_out.c_str());
                if (ret < 0) {
                  return Status::IOError("Format the output failed");
                }
                std::string printout(buffer_);
                s = stat.w_prefix_cut_f->Append(printout);
                if (!s.ok()) {
                  fprintf(stderr,
                          "Write whole key space prefix cut file failed\n");
                  return s;
                }
              }
            }
          }

          // Make the statistics fo the key size distribution
          if (FLAGS_output_key_distribution) {
            if (cfs_[cf_id].w_key_size_stats.find(input_key.size()) ==
                cfs_[cf_id].w_key_size_stats.end()) {
              cfs_[cf_id].w_key_size_stats[input_key.size()] = 1;
            } else {
              cfs_[cf_id].w_key_size_stats[input_key.size()]++;
            }
          }
        }
        s = lf_reader.GetStatus();
        if (!s.ok()) {
          fprintf(stderr, "Read whole key space file failed\n");
          return s;
        }
      }
    }

    // process the top k accessed keys
    if (FLAGS_print_top_k_access > 0) {
      for (int type = 0; type < kTaTypeNum; type++) {
        if (!ta_[type].enabled ||
            ta_[type].stats.find(cf_id) == ta_[type].stats.end()) {
          continue;
        }
        TraceStats& stat = ta_[type].stats[cf_id];
        for (auto& record : stat.a_key_stats) {
          if (static_cast<int32_t>(stat.top_k_queue.size()) <
              FLAGS_print_top_k_access) {
            stat.top_k_queue.push(
                std::make_pair(record.second.access_count, record.first));
          } else {
            if (record.second.access_count > stat.top_k_queue.top().first) {
              stat.top_k_queue.pop();
              stat.top_k_queue.push(
                  std::make_pair(record.second.access_count, record.first));
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
  Status s;
  if (trace_sequence_f_) {
    s = trace_sequence_f_->Close();
  }
  if (FLAGS_no_print) {
    return s;
  }
  PrintStatistics();
  if (s.ok()) {
    s = CloseOutputFiles();
  }
  return s;
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
  if ((type != TraceOperationType::kGet &&
       type != TraceOperationType::kMultiGet) ||
      value_size > 0) {
    unit.succ_count = 1;
  } else {
    unit.succ_count = 0;
  }
  unit.v_correlation.resize(analyzer_opts_.correlation_list.size());
  for (int i = 0;
       i < (static_cast<int>(analyzer_opts_.correlation_list.size())); i++) {
    unit.v_correlation[i].count = 0;
    unit.v_correlation[i].total_ts = 0;
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
    time_in_sec = static_cast<uint32_t>((ts - begin_time_) / 1000000);
  }

  uint64_t dist_value_size = value_size / FLAGS_value_interval;
  auto found_stats = ta_[type].stats.find(cf_id);
  if (found_stats == ta_[type].stats.end()) {
    ta_[type].stats[cf_id].cf_id = cf_id;
    ta_[type].stats[cf_id].cf_name = std::to_string(cf_id);
    ta_[type].stats[cf_id].a_count = 1;
    ta_[type].stats[cf_id].a_key_id = 0;
    ta_[type].stats[cf_id].a_key_size_sqsum = MultiplyCheckOverflow(
        static_cast<uint64_t>(key.size()), static_cast<uint64_t>(key.size()));
    ta_[type].stats[cf_id].a_key_size_sum = key.size();
    ta_[type].stats[cf_id].a_value_size_sqsum = MultiplyCheckOverflow(
        static_cast<uint64_t>(value_size), static_cast<uint64_t>(value_size));
    ta_[type].stats[cf_id].a_value_size_sum = value_size;
    s = OpenStatsOutputFiles(ta_[type].type_name, ta_[type].stats[cf_id]);
    if (!FLAGS_print_correlation.empty()) {
      s = StatsUnitCorrelationUpdate(unit, type, ts, key);
    }
    ta_[type].stats[cf_id].a_key_stats[key] = unit;
    ta_[type].stats[cf_id].a_value_size_stats[dist_value_size] = 1;
    ta_[type].stats[cf_id].a_qps_stats[time_in_sec] = 1;
    ta_[type].stats[cf_id].correlation_output.resize(
        analyzer_opts_.correlation_list.size());
    if (FLAGS_output_prefix_cut > 0) {
      std::map<std::string, uint32_t> tmp_qps_map;
      tmp_qps_map[prefix] = 1;
      ta_[type].stats[cf_id].a_qps_prefix_stats[time_in_sec] = tmp_qps_map;
    }
    if (time_in_sec != cur_time_sec_) {
      ta_[type].stats[cf_id].uni_key_num[cur_time_sec_] =
          static_cast<uint64_t>(ta_[type].stats[cf_id].a_key_stats.size());
      cur_time_sec_ = time_in_sec;
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
      if (!FLAGS_print_correlation.empty()) {
        s = StatsUnitCorrelationUpdate(found_key->second, type, ts, key);
      }
    }
    if (time_in_sec != cur_time_sec_) {
      found_stats->second.uni_key_num[cur_time_sec_] =
          static_cast<uint64_t>(found_stats->second.a_key_stats.size());
      cur_time_sec_ = time_in_sec;
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

  if (FLAGS_output_qps_stats) {
    cfs_[cf_id].cf_qps[time_in_sec]++;
  }

  if (FLAGS_output_time_series) {
    TraceUnit trace_u;
    trace_u.type = type;
    trace_u.key = key;
    trace_u.value_size = value_size;
    trace_u.ts = (ts - time_series_start_) / 1000000;
    trace_u.cf_id = cf_id;
    ta_[type].stats[cf_id].time_series.push_back(trace_u);
  }

  return s;
}

// Update the correlation unit of each key if enabled
Status TraceAnalyzer::StatsUnitCorrelationUpdate(StatsUnit& unit,
                                                 const uint32_t& type_second,
                                                 const uint64_t& ts,
                                                 const std::string& key) {
  if (type_second >= kTaTypeNum) {
    fprintf(stderr, "Unknown Type Id: %u\n", type_second);
    return Status::NotFound();
  }

  for (int type_first = 0; type_first < kTaTypeNum; type_first++) {
    if (type_first >= static_cast<int>(ta_.size()) ||
        type_first >= static_cast<int>(analyzer_opts_.correlation_map.size())) {
      break;
    }
    if (analyzer_opts_.correlation_map[type_first][type_second] < 0 ||
        ta_[type_first].stats.find(unit.cf_id) == ta_[type_first].stats.end() ||
        ta_[type_first].stats[unit.cf_id].a_key_stats.find(key) ==
            ta_[type_first].stats[unit.cf_id].a_key_stats.end() ||
        ta_[type_first].stats[unit.cf_id].a_key_stats[key].latest_ts == ts) {
      continue;
    }

    int correlation_id =
        analyzer_opts_.correlation_map[type_first][type_second];

    // after get the x-y operation time or x, update;
    if (correlation_id < 0 ||
        correlation_id >= static_cast<int>(unit.v_correlation.size())) {
      continue;
    }
    unit.v_correlation[correlation_id].count++;
    unit.v_correlation[correlation_id].total_ts +=
        (ts - ta_[type_first].stats[unit.cf_id].a_key_stats[key].latest_ts);
  }

  unit.latest_ts = ts;
  return Status::OK();
}

// when a new trace statistic is created, the file handler
// pointers should be initiated if needed according to
// the trace analyzer options
Status TraceAnalyzer::OpenStatsOutputFiles(const std::string& type,
                                           TraceStats& new_stats) {
  Status s;
  if (FLAGS_output_key_stats) {
    s = CreateOutputFile(type, new_stats.cf_name, "accessed_key_stats.txt",
                         &new_stats.a_key_f);
    s = CreateOutputFile(type, new_stats.cf_name,
                         "accessed_unique_key_num_change.txt",
                         &new_stats.a_key_num_f);
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

  if (FLAGS_output_time_series) {
    s = CreateOutputFile(type, new_stats.cf_name, "time_series.txt",
                         &new_stats.time_series_f);
  }

  if (FLAGS_output_value_distribution) {
    s = CreateOutputFile(type, new_stats.cf_name,
                         "accessed_value_size_distribution.txt",
                         &new_stats.a_value_size_f);
  }

  if (FLAGS_output_key_distribution) {
    s = CreateOutputFile(type, new_stats.cf_name,
                         "accessed_key_size_distribution.txt",
                         &new_stats.a_key_size_f);
  }

  if (FLAGS_output_qps_stats) {
    s = CreateOutputFile(type, new_stats.cf_name, "qps_stats.txt",
                         &new_stats.a_qps_f);
  }

  return s;
}

// create the output path of the files to be opened
Status TraceAnalyzer::CreateOutputFile(
    const std::string& type, const std::string& cf_name,
    const std::string& ending,
    std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile>* f_ptr) {
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
Status TraceAnalyzer::CloseOutputFiles() {
  Status s;
  for (int type = 0; type < kTaTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    for (auto& stat : ta_[type].stats) {
      if (s.ok() && stat.second.time_series_f) {
        s = stat.second.time_series_f->Close();
      }

      if (s.ok() && stat.second.a_key_f) {
        s = stat.second.a_key_f->Close();
      }

      if (s.ok() && stat.second.a_key_num_f) {
        s = stat.second.a_key_num_f->Close();
      }

      if (s.ok() && stat.second.a_count_dist_f) {
        s = stat.second.a_count_dist_f->Close();
      }

      if (s.ok() && stat.second.a_prefix_cut_f) {
        s = stat.second.a_prefix_cut_f->Close();
      }

      if (s.ok() && stat.second.a_value_size_f) {
        s = stat.second.a_value_size_f->Close();
      }

      if (s.ok() && stat.second.a_key_size_f) {
        s = stat.second.a_key_size_f->Close();
      }

      if (s.ok() && stat.second.a_qps_f) {
        s = stat.second.a_qps_f->Close();
      }

      if (s.ok() && stat.second.a_top_qps_prefix_f) {
        s = stat.second.a_top_qps_prefix_f->Close();
      }

      if (s.ok() && stat.second.w_key_f) {
        s = stat.second.w_key_f->Close();
      }
      if (s.ok() && stat.second.w_prefix_cut_f) {
        s = stat.second.w_prefix_cut_f->Close();
      }
    }
  }
  return s;
}

Status TraceAnalyzer::Handle(const WriteQueryTraceRecord& record,
                             std::unique_ptr<TraceRecordResult>* /*result*/) {
  total_writes_++;
  // Note that, if the write happens in a transaction,
  // 'Write' will be called twice, one for Prepare, one for
  // Commit. Thus, in the trace, for the same WriteBatch, there
  // will be two records if it is in a transaction. Here, we only
  // process the reord that is committed. If write is non-transaction,
  // HasBeginPrepare()==false, so we process it normally.
  WriteBatch batch(record.GetWriteBatchRep().ToString());
  if (batch.Count() == 0 || (batch.HasBeginPrepare() && !batch.HasCommit())) {
    return Status::OK();
  }
  write_batch_ts_ = record.GetTimestamp();

  // write_result_ will be updated in batch's handler during iteration.
  Status s = batch.Iterate(this);
  write_batch_ts_ = 0;
  if (!s.ok()) {
    fprintf(stderr, "Cannot process the write batch in the trace\n");
    return s;
  }

  return Status::OK();
}

Status TraceAnalyzer::Handle(const GetQueryTraceRecord& record,
                             std::unique_ptr<TraceRecordResult>* /*result*/) {
  total_gets_++;
  return OutputAnalysisResult(TraceOperationType::kGet, record.GetTimestamp(),
                              record.GetColumnFamilyID(),
                              std::move(record.GetKey()), 0);
}

Status TraceAnalyzer::Handle(const IteratorSeekQueryTraceRecord& record,
                             std::unique_ptr<TraceRecordResult>* /*result*/) {
  TraceOperationType op_type;
  if (record.GetSeekType() == IteratorSeekQueryTraceRecord::kSeek) {
    op_type = TraceOperationType::kIteratorSeek;
    total_seeks_++;
  } else {
    op_type = TraceOperationType::kIteratorSeekForPrev;
    total_seek_prevs_++;
  }

  // To do: shall we add lower/upper bounds?

  return OutputAnalysisResult(op_type, record.GetTimestamp(),
                              record.GetColumnFamilyID(),
                              std::move(record.GetKey()), 0);
}

Status TraceAnalyzer::Handle(const MultiGetQueryTraceRecord& record,
                             std::unique_ptr<TraceRecordResult>* /*result*/) {
  total_multigets_++;

  std::vector<uint32_t> cf_ids = record.GetColumnFamilyIDs();
  std::vector<Slice> keys = record.GetKeys();
  std::vector<size_t> value_sizes;

  // If the size does not match is not the error of tracing and anayzing, we
  // just report it to the user. The analyzing continues.
  if (cf_ids.size() > keys.size()) {
    printf("The CF ID vector size does not match the keys vector size!\n");
    // Make the sure the 2 vectors are of the same (smaller) size.
    cf_ids.resize(keys.size());
  } else if (cf_ids.size() < keys.size()) {
    printf("The CF ID vector size does not match the keys vector size!\n");
    // Make the sure the 2 vectors are of the same (smaller) size.
    keys.resize(cf_ids.size());
  }
  // Now the 2 vectors must be of the same size.
  value_sizes.resize(cf_ids.size(), 0);

  return OutputAnalysisResult(TraceOperationType::kMultiGet,
                              record.GetTimestamp(), std::move(cf_ids),
                              std::move(keys), std::move(value_sizes));
}

// Handle the Put request in the write batch of the trace
Status TraceAnalyzer::PutCF(uint32_t column_family_id, const Slice& key,
                            const Slice& value) {
  return OutputAnalysisResult(TraceOperationType::kPut, write_batch_ts_,
                              column_family_id, key, value.size());
}

Status TraceAnalyzer::PutEntityCF(uint32_t column_family_id, const Slice& key,
                                  const Slice& value) {
  return OutputAnalysisResult(TraceOperationType::kPutEntity, write_batch_ts_,
                              column_family_id, key, value.size());
}

// Handle the Delete request in the write batch of the trace
Status TraceAnalyzer::DeleteCF(uint32_t column_family_id, const Slice& key) {
  return OutputAnalysisResult(TraceOperationType::kDelete, write_batch_ts_,
                              column_family_id, key, 0);
}

// Handle the SingleDelete request in the write batch of the trace
Status TraceAnalyzer::SingleDeleteCF(uint32_t column_family_id,
                                     const Slice& key) {
  return OutputAnalysisResult(TraceOperationType::kSingleDelete,
                              write_batch_ts_, column_family_id, key, 0);
}

// Handle the DeleteRange request in the write batch of the trace
Status TraceAnalyzer::DeleteRangeCF(uint32_t column_family_id,
                                    const Slice& begin_key,
                                    const Slice& end_key) {
  return OutputAnalysisResult(TraceOperationType::kRangeDelete, write_batch_ts_,
                              {column_family_id, column_family_id},
                              {begin_key, end_key}, {0, 0});
}

// Handle the Merge request in the write batch of the trace
Status TraceAnalyzer::MergeCF(uint32_t column_family_id, const Slice& key,
                              const Slice& value) {
  return OutputAnalysisResult(TraceOperationType::kMerge, write_batch_ts_,
                              column_family_id, key, value.size());
}

Status TraceAnalyzer::OutputAnalysisResult(TraceOperationType op_type,
                                           uint64_t timestamp,
                                           std::vector<uint32_t> cf_ids,
                                           std::vector<Slice> keys,
                                           std::vector<size_t> value_sizes) {
  assert(!cf_ids.empty());
  assert(cf_ids.size() == keys.size());
  assert(cf_ids.size() == value_sizes.size());

  Status s;

  if (FLAGS_convert_to_human_readable_trace && trace_sequence_f_) {
    // DeleteRane only writes the begin_key.
    size_t cnt =
        op_type == TraceOperationType::kRangeDelete ? 1 : cf_ids.size();
    for (size_t i = 0; i < cnt; i++) {
      s = WriteTraceSequence(op_type, cf_ids[i], keys[i], value_sizes[i],
                             timestamp);
      if (!s.ok()) {
        return Status::Corruption("Failed to write the trace sequence to file");
      }
    }
  }

  if (ta_[op_type].sample_count >= sample_max_) {
    ta_[op_type].sample_count = 0;
  }
  if (ta_[op_type].sample_count > 0) {
    ta_[op_type].sample_count++;
    return Status::OK();
  }
  ta_[op_type].sample_count++;

  if (!ta_[op_type].enabled) {
    return Status::OK();
  }

  for (size_t i = 0; i < cf_ids.size(); i++) {
    // Get query does not have value part, just give a fixed value 10 for easy
    // calculation.
    s = KeyStatsInsertion(
        op_type, cf_ids[i], keys[i].ToString(),
        value_sizes[i] == 0 ? kShadowValueSize : value_sizes[i], timestamp);
    if (!s.ok()) {
      return Status::Corruption("Failed to insert key statistics");
    }
  }

  return Status::OK();
}

Status TraceAnalyzer::OutputAnalysisResult(TraceOperationType op_type,
                                           uint64_t timestamp, uint32_t cf_id,
                                           const Slice& key,
                                           size_t value_size) {
  return OutputAnalysisResult(
      op_type, timestamp, std::vector<uint32_t>({cf_id}),
      std::vector<Slice>({key}), std::vector<size_t>({value_size}));
}

// Before the analyzer is closed, the requested general statistic results are
// printed out here. In current stage, these information are not output to
// the files.
// -----type
//          |__cf_id
//                |_statistics
void TraceAnalyzer::PrintStatistics() {
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
      printf("Peak QPS is: %u Average QPS is: %f\n", qps_peak_[type],
             qps_ave_[type]);
    }
    for (auto& stat_it : ta_[type].stats) {
      if (stat_it.second.a_count == 0) {
        continue;
      }
      TraceStats& stat = stat_it.second;
      uint64_t total_a_keys = static_cast<uint64_t>(stat.a_key_stats.size());
      double key_size_ave = 0.0;
      double value_size_ave = 0.0;
      double key_size_vari = 0.0;
      double value_size_vari = 0.0;
      if (stat.a_count > 0) {
        key_size_ave =
            (static_cast<double>(stat.a_key_size_sum)) / stat.a_count;
        value_size_ave =
            (static_cast<double>(stat.a_value_size_sum)) / stat.a_count;
        key_size_vari = std::sqrt((static_cast<double>(stat.a_key_size_sqsum)) /
                                      stat.a_count -
                                  key_size_ave * key_size_ave);
        value_size_vari = std::sqrt(
            (static_cast<double>(stat.a_value_size_sqsum)) / stat.a_count -
            value_size_ave * value_size_ave);
      }
      if (value_size_ave == 0.0) {
        stat.a_value_mid = 0;
      }
      cfs_[stat.cf_id].a_count += total_a_keys;
      ta_[type].total_keys += total_a_keys;
      ta_[type].total_access += stat.a_count;
      ta_[type].total_succ_access += stat.a_succ_count;
      printf("*********************************************************\n");
      printf("colume family id: %u\n", stat.cf_id);
      printf("Total number of queries to this cf by %s: %" PRIu64 "\n",
             ta_[type].type_name.c_str(), stat.a_count);
      printf("Total unique keys in this cf: %" PRIu64 "\n", total_a_keys);
      printf("Average key size: %f key size medium: %" PRIu64
             " Key size Variation: %f\n",
             key_size_ave, stat.a_key_mid, key_size_vari);
      if (type == kPut || type == kMerge) {
        printf("Average value size: %f Value size medium: %" PRIu64
               " Value size variation: %f\n",
               value_size_ave, stat.a_value_mid, value_size_vari);
      }
      printf("Peak QPS is: %u Average QPS is: %f\n", stat.a_peak_qps,
             stat.a_ave_qps);

      // print the top k accessed key and its access count
      if (FLAGS_print_top_k_access > 0) {
        printf("The Top %d keys that are accessed:\n",
               FLAGS_print_top_k_access);
        while (!stat.top_k_queue.empty()) {
          std::string hex_key = ROCKSDB_NAMESPACE::LDBCommand::StringToHex(
              stat.top_k_queue.top().second);
          printf("Access_count: %" PRIu64 " %s\n", stat.top_k_queue.top().first,
                 hex_key.c_str());
          stat.top_k_queue.pop();
        }
      }

      // print the top k access prefix range and
      // top k prefix range with highest average access per key
      if (FLAGS_output_prefix_cut > 0) {
        printf("The Top %d accessed prefix range:\n", FLAGS_print_top_k_access);
        while (!stat.top_k_prefix_access.empty()) {
          printf("Prefix: %s Access count: %" PRIu64 "\n",
                 stat.top_k_prefix_access.top().second.c_str(),
                 stat.top_k_prefix_access.top().first);
          stat.top_k_prefix_access.pop();
        }

        printf("The Top %d prefix with highest access per key:\n",
               FLAGS_print_top_k_access);
        while (!stat.top_k_prefix_ave.empty()) {
          printf("Prefix: %s access per key: %f\n",
                 stat.top_k_prefix_ave.top().second.c_str(),
                 stat.top_k_prefix_ave.top().first);
          stat.top_k_prefix_ave.pop();
        }
      }

      // print the operation correlations
      if (!FLAGS_print_correlation.empty()) {
        for (int correlation = 0;
             correlation <
             static_cast<int>(analyzer_opts_.correlation_list.size());
             correlation++) {
          printf(
              "The correlation statistics of '%s' after '%s' is:",
              taIndexToOpt[analyzer_opts_.correlation_list[correlation].second]
                  .c_str(),
              taIndexToOpt[analyzer_opts_.correlation_list[correlation].first]
                  .c_str());
          double correlation_ave = 0.0;
          if (stat.correlation_output[correlation].first > 0) {
            correlation_ave =
                (static_cast<double>(
                    stat.correlation_output[correlation].second)) /
                (stat.correlation_output[correlation].first * 1000);
          }
          printf(" total numbers: %" PRIu64 " average time: %f(ms)\n",
                 stat.correlation_output[correlation].first, correlation_ave);
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
  for (auto& cf : cfs_) {
    printf("The column family id: %u\n", cf.first);
    printf("The whole key space key numbers: %" PRIu64 "\n", cf.second.w_count);
    printf("The accessed key space key numbers: %" PRIu64 "\n",
           cf.second.a_count);
  }

  if (FLAGS_print_overall_stats) {
    printf("\n*********************************************************\n");
    printf("*********************************************************\n");
    if (qps_peak_.size() == kTaTypeNum + 1) {
      printf("Average QPS per second: %f Peak QPS: %u\n", qps_ave_[kTaTypeNum],
             qps_peak_[kTaTypeNum]);
    }
    printf("The statistics related to query number need to times: %u\n",
           sample_max_);
    printf("Total_requests: %" PRIu64 " Total_accessed_keys: %" PRIu64
           " Total_gets: %" PRIu64 " Total_write_batches: %" PRIu64
           " Total_seeks: %" PRIu64 " Total_seek_for_prevs: %" PRIu64
           " Total_multigets: %" PRIu64 "\n",
           total_requests_, total_access_keys_, total_gets_, total_writes_,
           total_seeks_, total_seek_prevs_, total_multigets_);
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
                                         const Slice& key,
                                         const size_t value_size,
                                         const uint64_t ts) {
  std::string hex_key =
      ROCKSDB_NAMESPACE::LDBCommand::StringToHex(key.ToString());
  int ret;
  ret = snprintf(buffer_, sizeof(buffer_), "%u %u %zu %" PRIu64 "\n", type,
                 cf_id, value_size, ts);
  if (ret < 0) {
    return Status::IOError("failed to format the output");
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

  if (!FLAGS_print_correlation.empty()) {
    analyzer_opts.SparseCorrelationInput(FLAGS_print_correlation);
  }

  std::unique_ptr<TraceAnalyzer> analyzer(
      new TraceAnalyzer(FLAGS_trace_path, FLAGS_output_dir, analyzer_opts));

  if (!analyzer) {
    fprintf(stderr, "Cannot initiate the trace analyzer\n");
    exit(1);
  }

  ROCKSDB_NAMESPACE::Status s = analyzer->PrepareProcessing();
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.getState());
    fprintf(stderr, "Cannot initiate the trace reader\n");
    exit(1);
  }

  s = analyzer->StartProcessing();
  if (!s.ok() && !FLAGS_try_process_corrupted_trace) {
    fprintf(stderr, "%s\n", s.getState());
    fprintf(stderr, "Cannot process the trace\n");
    exit(1);
  }

  s = analyzer->MakeStatistics();
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.getState());
    analyzer->EndProcessing();
    fprintf(stderr, "Cannot make the statistics\n");
    exit(1);
  }

  s = analyzer->ReProcessing();
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.getState());
    fprintf(stderr, "Cannot re-process the trace for more statistics\n");
    analyzer->EndProcessing();
    exit(1);
  }

  s = analyzer->EndProcessing();
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.getState());
    fprintf(stderr, "Cannot close the trace analyzer\n");
    exit(1);
  }

  return 0;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // Endif of Gflag
