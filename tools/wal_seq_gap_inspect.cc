//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <cstdint>
#include <deque>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "db/log_reader.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "file/sequence_file_reader.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

namespace {

struct Reporter : public log::Reader::Reporter {
  void Corruption(size_t bytes, const Status& status,
                  uint64_t log_number = kMaxSequenceNumber) override {
    std::cerr << "corruption bytes=" << bytes << " log=" << log_number
              << " status=" << status.ToString() << "\n";
  }
};

struct RecordInfo {
  uint64_t log_number = 0;
  uint64_t offset = 0;
  SequenceNumber sequence = 0;
  uint32_t count = 0;
  size_t byte_size = 0;
};

std::optional<uint64_t> ParseWalNumber(const std::string& name) {
  uint64_t number = 0;
  FileType type = kTempFile;
  if (ParseFileName(name, &number, &type) && type == kWalFile) {
    return number;
  }
  return std::nullopt;
}

int Run(const std::string& wal_dir) {
  Env* env = Env::Default();
  const auto& fs = env->GetFileSystem();
  IOOptions io_opts;
  io_opts.do_not_recurse = true;

  std::vector<std::string> children;
  IOStatus io_s = fs->GetChildren(wal_dir, io_opts, &children, nullptr);
  if (!io_s.ok()) {
    std::cerr << "GetChildren failed: " << io_s.ToString() << "\n";
    return 1;
  }

  std::vector<std::pair<uint64_t, std::string>> wal_files;
  wal_files.reserve(children.size());
  for (const auto& child : children) {
    std::optional<uint64_t> number = ParseWalNumber(child);
    if (number.has_value()) {
      wal_files.emplace_back(*number, wal_dir + "/" + child);
    }
  }
  std::sort(wal_files.begin(), wal_files.end());

  if (wal_files.empty()) {
    std::cerr << "No WAL files under " << wal_dir << "\n";
    return 1;
  }

  FileOptions file_opts{DBOptions()};
  Reporter reporter;
  std::optional<SequenceNumber> prev_seq;
  std::optional<uint32_t> prev_count;
  std::deque<RecordInfo> history;

  for (const auto& [log_number, path] : wal_files) {
    std::unique_ptr<SequentialFileReader> reader_file;
    Status s = SequentialFileReader::Create(fs, path, file_opts, &reader_file,
                                            nullptr, nullptr);
    if (!s.ok()) {
      std::cerr << "Open WAL failed: " << path << " " << s.ToString() << "\n";
      return 1;
    }

    log::Reader reader(nullptr, std::move(reader_file), &reporter,
                       /*checksum=*/true, log_number);
    std::string scratch;
    Slice record;
    WriteBatch batch;

    while (reader.ReadRecord(&record, &scratch)) {
      if (record.size() < WriteBatchInternal::kHeader) {
        std::cerr << "Short record in " << path
                  << " offset=" << reader.LastRecordOffset() << "\n";
        return 1;
      }

      s = WriteBatchInternal::SetContents(&batch, record);
      if (!s.ok()) {
        std::cerr << "SetContents failed in " << path
                  << " offset=" << reader.LastRecordOffset() << " "
                  << s.ToString() << "\n";
        return 1;
      }

      RecordInfo info;
      info.log_number = log_number;
      info.offset = reader.LastRecordOffset();
      info.sequence = WriteBatchInternal::Sequence(&batch);
      info.count = WriteBatchInternal::Count(&batch);
      info.byte_size = WriteBatchInternal::ByteSize(&batch);

      if (prev_seq.has_value() && prev_count.has_value() &&
          *prev_seq + *prev_count != info.sequence) {
        std::cout << "Sequence discontinuity detected\n";
        std::cout << "expected=" << (*prev_seq + *prev_count)
                  << " actual=" << info.sequence << "\n";
        std::cout << "history:\n";
        for (const auto& h : history) {
          std::cout << "  log=" << h.log_number << " offset=" << h.offset
                    << " seq=" << h.sequence << " count=" << h.count
                    << " bytes=" << h.byte_size << "\n";
        }
        std::cout << "current:\n";
        std::cout << "  log=" << info.log_number << " offset=" << info.offset
                  << " seq=" << info.sequence << " count=" << info.count
                  << " bytes=" << info.byte_size << "\n";
        return 2;
      }

      if (history.size() == 8) {
        history.pop_front();
      }
      history.push_back(info);
      prev_seq = info.sequence;
      prev_count = info.count;
    }
  }

  std::cout << "No sequence discontinuity found\n";
  return 0;
}

}  // namespace

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: wal_seq_gap_inspect <wal_dir>\n";
    return 1;
  }
  return ROCKSDB_NAMESPACE::Run(argv[1]);
}
