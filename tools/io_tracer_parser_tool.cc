//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//    This source code is licensed under both the GPLv2 (found in the
//    COPYING file in the root directory) and Apache 2.0 License
//    (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/io_tracer_parser_tool.h"

#include <cinttypes>
#include <cstdio>
#include <iomanip>
#include <memory>
#include <sstream>

#include "port/lang.h"
#include "rocksdb/env.h"
#include "trace_replay/io_tracer.h"

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
      ss << ", File Name: " << record.file_name.c_str();
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
    // No way to differentiate if end of record or record is corrupted.
    if (!status.ok()) {
      break;
    }
    PrintHumanReadableIOTraceRecord(record);
  }
  return 0;
}

int IOTracerParserTool::run(int argc, char const* const* argv) {
  std::string input_file;
  std::string output_file;
  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--input_file=", 13) == 0) {
      input_file = argv[i] + 13;
    } else if (strncmp(argv[i], "--output_file=", 14) == 0) {
      output_file = argv[i] + 14;
    } else {
      fprintf(stderr, "Unrecognized argument '%s'\n\n", argv[i]);
      return 1;
    }
  }

  if (input_file.empty()) {
    fprintf(stderr, "Specify IO trace file to be read with --input_file=\n\n");
    return 1;
  }

  if (output_file.empty()) {
    fprintf(stderr, "Specify IO trace file to be read with --output_file=\n\n");
    return 1;
  }

  std::unique_ptr<IOTraceRecordParser> io_tracer_parser;
  io_tracer_parser.reset(new IOTraceRecordParser(input_file, output_file));
  return io_tracer_parser->ReadIOTraceRecords();
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
