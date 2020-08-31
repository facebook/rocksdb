//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//    This source code is licensed under both the GPLv2 (found in the
//    COPYING file in the root directory) and Apache 2.0 License
//    (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#ifdef GFLAGS
#include "tools/io_tracer_parser_tool.h"

#include <cinttypes>
#include <cstdio>
#include <iomanip>
#include <memory>
#include <sstream>

#include "port/lang.h"
#include "trace_replay/io_tracer.h"
#include "util/gflags_compat.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

DEFINE_string(io_trace_file, "", "The IO trace file path.");
DEFINE_string(
    output_file, "",
    "The output file path for storing human readable format of IO trace "
    "records that are stored in IO trace file.");

namespace ROCKSDB_NAMESPACE {

IOTraceRecordParser::IOTraceRecordParser(const std::string& input_file,
                                         const std::string& output_file)
    : input_file_(input_file), output_file_(output_file) {}

IOTraceRecordParser::~IOTraceRecordParser() {
  if (trace_writer_) {
    trace_writer_->Flush();
    trace_writer_->Close();
  }
}

void IOTraceRecordParser::PrintHumanReadableHeader(
    const IOTraceHeader& header) {
  std::stringstream ss;
  ss << "Start Time: " << header.start_time
     << "\nRocksDB Major Version: " << header.rocksdb_major_version
     << "\nRocksDB Minor Version: " << header.rocksdb_minor_version << "\n";
  trace_writer_->Append(ss.str());
}

void IOTraceRecordParser::PrintHumanReadableIOTraceRecord(
    const IOTraceRecord& record) {
  std::stringstream ss;
  ss << "Access Time : " << std::setw(17) << std::left
     << record.access_timestamp << ", File Operation: " << std::setw(18)
     << std::left << record.file_operation.c_str()
     << ", Latency: " << std::setw(9) << std::left << record.latency
     << ", IO Status: " << record.io_status.c_str();

  switch (record.trace_type) {
    case TraceType::kIOGeneral:
      break;
    case TraceType::kIOFileNameAndFileSize:
      ss << ", File Size: " << record.file_size;
      FALLTHROUGH_INTENDED;
    case TraceType::kIOFileName: {
      if (!record.file_name.empty()) {
        ss << ", File Name: " << record.file_name.c_str();
      }
      break;
    }
    case TraceType::kIOLenAndOffset:
      ss << ", Offset: " << record.offset;
      FALLTHROUGH_INTENDED;
    case TraceType::kIOLen:
      ss << ", Length: " << record.len;
      break;
    default:
      assert(false);
  }
  ss << "\n";
  trace_writer_->Append(ss.str());
}

int IOTraceRecordParser::ReadIOTraceRecords() {
  Status status;
  Env* env(Env::Default());
  std::unique_ptr<TraceReader> trace_reader;
  std::unique_ptr<IOTraceReader> io_trace_reader;

  status = NewFileTraceReader(env, EnvOptions(), input_file_, &trace_reader);
  if (!status.ok()) {
    fprintf(stderr, "%s: %s\n", input_file_.c_str(), status.ToString().c_str());
    return 1;
  }
  io_trace_reader.reset(new IOTraceReader(std::move(trace_reader)));

  status = env->NewWritableFile(output_file_, &trace_writer_, EnvOptions());
  if (!status.ok()) {
    fprintf(stderr, "%s: %s\n", output_file_.c_str(),
            status.ToString().c_str());
    return 1;
  }

  // Read the header and dump it in a file.
  IOTraceHeader header;
  status = io_trace_reader->ReadHeader(&header);
  if (!status.ok()) {
    fprintf(stderr, "%s: %s\n", input_file_.c_str(), status.ToString().c_str());
    return 1;
  }
  PrintHumanReadableHeader(header);

  // Read the records one by one and print them in human readable format.
  while (status.ok()) {
    IOTraceRecord record;
    status = io_trace_reader->ReadIOOp(&record);
    if (!status.ok()) {
      break;
    }
    PrintHumanReadableIOTraceRecord(record);
  }
  return 0;
}

int io_tracer_parser(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_io_trace_file.empty()) {
    fprintf(stderr, "IO Trace file path is empty\n");
    return 1;
  }

  if (FLAGS_output_file.empty()) {
    fprintf(stderr, "Output file path is empty\n");
    return 1;
  }

  IOTraceRecordParser io_tracer_parser(FLAGS_io_trace_file, FLAGS_output_file);
  return io_tracer_parser.ReadIOTraceRecords();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
#endif  // ROCKSDB_LITE
