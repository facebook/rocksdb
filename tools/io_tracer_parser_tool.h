// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#pragma once

#include <memory>

#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct IOTraceHeader;
struct IOTraceRecord;

// IOTraceRecordParser class reads the IO trace file (in binary format) and
// dumps the human readable records in output_file_.
class IOTraceRecordParser {
 public:
  explicit IOTraceRecordParser(const std::string& input_file);

  // ReadIOTraceRecords reads the binary trace file records one by one and
  // invoke PrintHumanReadableIOTraceRecord to dump the records in output_file_.
  int ReadIOTraceRecords();

 private:
  void PrintHumanReadableHeader(const IOTraceHeader& header);
  void PrintHumanReadableIOTraceRecord(const IOTraceRecord& record);

  // Binary file that contains IO trace records.
  std::string input_file_;
};

int io_tracer_parser(int argc, char** argv);

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
