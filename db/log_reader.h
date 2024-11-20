//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <memory>
#include <unordered_map>
#include <vector>

#include "db/log_format.h"
#include "file/sequence_file_reader.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/compression.h"
#include "util/hash_containers.h"
#include "util/udt_util.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {
class Logger;

namespace log {

/**
 * Reader is a general purpose log stream reader implementation. The actual job
 * of reading from the device is implemented by the SequentialFile interface.
 *
 * Please see Writer for details on the file and record layout.
 */
class Reader {
 public:
  // Interface for reporting errors.
  class Reporter {
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    virtual void Corruption(size_t bytes, const Status& status) = 0;

    virtual void OldLogRecord(size_t /*bytes*/) {}
  };

  // Create a reader that will return log records from "*file".
  // "*file" must remain live while this Reader is in use.
  //
  // If "reporter" is non-nullptr, it is notified whenever some data is
  // dropped due to a detected corruption.  "*reporter" must remain
  // live while this Reader is in use.
  //
  // If "checksum" is true, verify checksums if available.
  Reader(std::shared_ptr<Logger> info_log,
         std::unique_ptr<SequentialFileReader>&& file, Reporter* reporter,
         bool checksum, uint64_t log_num);
  // No copying allowed
  Reader(const Reader&) = delete;
  void operator=(const Reader&) = delete;

  virtual ~Reader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage. The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  // If record_checksum is not nullptr, then this function will calculate the
  // checksum of the record read and set record_checksum to it. The checksum is
  // calculated from the original buffers that contain the contents of the
  // record.
  virtual bool ReadRecord(Slice* record, std::string* scratch,
                          WALRecoveryMode wal_recovery_mode =
                              WALRecoveryMode::kTolerateCorruptedTailRecords,
                          uint64_t* record_checksum = nullptr);

  // Return the recorded user-defined timestamp size that have been read so
  // far. This only applies to WAL logs.
  const UnorderedMap<uint32_t, size_t>& GetRecordedTimestampSize() const {
    return recorded_cf_to_ts_sz_;
  }

  // Returns the physical offset of the last record returned by ReadRecord.
  //
  // Undefined before the first call to ReadRecord.
  uint64_t LastRecordOffset();

  // Returns the first physical offset after the last record returned by
  // ReadRecord, or zero before first call to ReadRecord. This can also be
  // thought of as the "current" position in processing the file bytes.
  uint64_t LastRecordEnd();

  // returns true if the reader has encountered an eof condition.
  bool IsEOF() { return eof_; }

  // returns true if the reader has encountered read error.
  bool hasReadError() const { return read_error_; }

  // when we know more data has been written to the file. we can use this
  // function to force the reader to look again in the file.
  // Also aligns the file position indicator to the start of the next block
  // by reading the rest of the data from the EOF position to the end of the
  // block that was partially read.
  virtual void UnmarkEOF();

  SequentialFileReader* file() { return file_.get(); }

  Reporter* GetReporter() const { return reporter_; }

  uint64_t GetLogNumber() const { return log_number_; }

  size_t GetReadOffset() const {
    return static_cast<size_t>(end_of_buffer_offset_);
  }

  bool IsCompressedAndEmptyFile() {
    return !first_record_read_ && compression_type_record_read_;
  }

 protected:
  std::shared_ptr<Logger> info_log_;
  const std::unique_ptr<SequentialFileReader> file_;
  Reporter* const reporter_;
  bool const checksum_;
  char* const backing_store_;

  // Internal state variables used for reading records
  Slice buffer_;
  bool eof_;         // Last Read() indicated EOF by returning < kBlockSize
  bool read_error_;  // Error occurred while reading from file

  // Offset of the file position indicator within the last block when an
  // EOF was detected.
  size_t eof_offset_;

  // Offset of the last record returned by ReadRecord.
  uint64_t last_record_offset_;
  // Offset of the first location past the end of buffer_.
  uint64_t end_of_buffer_offset_;

  // which log number this is
  uint64_t const log_number_;

  // Whether this is a recycled log file
  bool recycled_;

  // Whether the first record has been read or not.
  bool first_record_read_;
  // Type of compression used
  CompressionType compression_type_;
  // Track whether the compression type record has been read or not.
  bool compression_type_record_read_;
  StreamingUncompress* uncompress_;
  // Reusable uncompressed output buffer
  std::unique_ptr<char[]> uncompressed_buffer_;
  // Reusable uncompressed record
  std::string uncompressed_record_;
  // Used for stream hashing fragment content in ReadRecord()
  XXH3_state_t* hash_state_;
  // Used for stream hashing uncompressed buffer in ReadPhysicalRecord()
  XXH3_state_t* uncompress_hash_state_;

  // The recorded user-defined timestamp sizes that have been read so far. This
  // is only for WAL logs.
  UnorderedMap<uint32_t, size_t> recorded_cf_to_ts_sz_;

  // Extend record types with the following special values
  enum {
    kEof = kMaxRecordType + 1,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    kBadRecord = kMaxRecordType + 2,
    // Returned when we fail to read a valid header.
    kBadHeader = kMaxRecordType + 3,
    // Returned when we read an old record from a previous user of the log.
    kOldRecord = kMaxRecordType + 4,
    // Returned when we get a bad record length
    kBadRecordLen = kMaxRecordType + 5,
    // Returned when we get a bad record checksum
    kBadRecordChecksum = kMaxRecordType + 6,
  };

  // Return type, or one of the preceding special values
  // If WAL compressioned is enabled, fragment_checksum is the checksum of the
  // fragment computed from the orginal buffer containinng uncompressed
  // fragment.
  unsigned int ReadPhysicalRecord(Slice* result, size_t* drop_size,
                                  uint64_t* fragment_checksum = nullptr);

  // Read some more
  bool ReadMore(size_t* drop_size, int* error);

  void UnmarkEOFInternal();

  // Reports dropped bytes to the reporter.
  // buffer_ must be updated to remove the dropped bytes prior to invocation.
  void ReportCorruption(size_t bytes, const char* reason);
  void ReportDrop(size_t bytes, const Status& reason);
  void ReportOldLogRecord(size_t bytes);

  void InitCompression(const CompressionTypeRecord& compression_record);

  Status UpdateRecordedTimestampSize(
      const std::vector<std::pair<uint32_t, size_t>>& cf_to_ts_sz);
};

class FragmentBufferedReader : public Reader {
 public:
  FragmentBufferedReader(std::shared_ptr<Logger> info_log,
                         std::unique_ptr<SequentialFileReader>&& _file,
                         Reporter* reporter, bool checksum, uint64_t log_num)
      : Reader(info_log, std::move(_file), reporter, checksum, log_num),
        fragments_(),
        in_fragmented_record_(false) {}
  ~FragmentBufferedReader() override {}
  bool ReadRecord(Slice* record, std::string* scratch,
                  WALRecoveryMode wal_recovery_mode =
                      WALRecoveryMode::kTolerateCorruptedTailRecords,
                  uint64_t* record_checksum = nullptr) override;
  void UnmarkEOF() override;

 private:
  std::string fragments_;
  bool in_fragmented_record_;

  bool TryReadFragment(Slice* result, size_t* drop_size,
                       unsigned int* fragment_type_or_err);

  bool TryReadMore(size_t* drop_size, int* error);

  // No copy allowed
  FragmentBufferedReader(const FragmentBufferedReader&);
  void operator=(const FragmentBufferedReader&);
};

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE
