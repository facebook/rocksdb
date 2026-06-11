//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/sst_file_writer.h"

#include <limits>
#include <numeric>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "file/filename.h"
#include "file/writable_file_writer.h"
#include "rocksdb/file_system.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/sst_file_writer_collectors.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

const std::string ExternalSstFilePropertyNames::kVersion =
    "rocksdb.external_sst_file.version";
const std::string ExternalSstFilePropertyNames::kGlobalSeqno =
    "rocksdb.external_sst_file.global_seqno";

const size_t kFadviseTrigger = 1024 * 1024;  // 1MB

std::string GetParentDir(const std::string& file_path) {
  const size_t slash = file_path.find_last_of('/');
  if (slash == std::string::npos) {
    return ".";
  }
  if (slash == 0) {
    return "/";
  }
  return file_path.substr(0, slash);
}

struct SstFileWriter::Rep {
  Rep(const EnvOptions& _env_options, const Options& options,
      Env::IOPriority _io_priority, const Comparator* _user_comparator,
      ColumnFamilyHandle* _cfh, bool _invalidate_page_cache,
      std::string _db_session_id)
      : env_options(_env_options),
        ioptions(options),
        mutable_cf_options(options),
        io_priority(_io_priority),
        internal_comparator(_user_comparator),
        cfh(_cfh),
        invalidate_page_cache(_invalidate_page_cache),
        db_session_id(_db_session_id),
        ts_sz(_user_comparator->timestamp_size()),
        strip_timestamp(ts_sz > 0 &&
                        !ioptions.persist_user_defined_timestamps) {
    // TODO (hx235): pass in `WriteOptions` instead of `rate_limiter_priority`
    // during construction
    write_options.rate_limiter_priority = io_priority;
  }

  std::unique_ptr<WritableFileWriter> file_writer;
  std::unique_ptr<TableBuilder> builder;
  EnvOptions env_options;
  ImmutableOptions ioptions;
  MutableCFOptions mutable_cf_options;
  Env::IOPriority io_priority;
  WriteOptions write_options;
  InternalKeyComparator internal_comparator;
  ExternalSstFileInfo file_info;
  InternalKey ikey;
  std::string column_family_name;
  ColumnFamilyHandle* cfh;
  // If true, We will give the OS a hint that this file pages is not needed
  // every time we write 1MB to the file.
  bool invalidate_page_cache;
  // The size of the file during the last time we called Fadvise to remove
  // cached pages from page cache.
  uint64_t last_fadvise_size = 0;
  std::string db_session_id;
  uint64_t next_file_number = 1;
  size_t ts_sz;
  bool strip_timestamp;
  bool write_blob_files = false;
  bool opening_with_blob_files = false;
  SstFileWriterBlobOptions blob_options;
  std::string sst_file_dir;
  uint64_t next_blob_file_number = 0;
  uint64_t current_blob_file_number = 0;
  uint64_t current_blob_count = 0;
  uint64_t current_blob_bytes = 0;
  std::string current_blob_file_path;
  std::unique_ptr<BlobLogWriter> blob_writer;
  std::vector<ExternalBlobFileInfo> blob_file_infos;

  Status AddImpl(const Slice& user_key, const Slice& value,
                 ValueType value_type) {
    if (!builder) {
      return Status::InvalidArgument("File is not opened");
    }
    if (!builder->status().ok()) {
      return builder->status();
    }

    // user_key + kNumInternalBytes must fit in uint32_t (BlockBuilder
    // assumption). Also check value size.
    if (user_key.size() >
        size_t{std::numeric_limits<uint32_t>::max()} - kNumInternalBytes) {
      return Status::InvalidArgument("key is too large");
    }
    if (value.size() > size_t{std::numeric_limits<uint32_t>::max()}) {
      return Status::InvalidArgument("value is too large");
    }

    assert(user_key.size() >= ts_sz);
    if (strip_timestamp) {
      // In this mode, we expect users to always provide a min timestamp.
      if (internal_comparator.user_comparator()->CompareTimestamp(
              Slice(user_key.data() + user_key.size() - ts_sz, ts_sz),
              MinU64Ts()) != 0) {
        return Status::InvalidArgument(
            "persist_user_defined_timestamps flag is set to false, only "
            "minimum timestamp is accepted.");
      }
    }
    if (file_info.num_entries == 0) {
      file_info.smallest_key.assign(user_key.data(), user_key.size());
    } else {
      if (internal_comparator.user_comparator()->Compare(
              user_key, file_info.largest_key) <= 0) {
        // Make sure that keys are added in order
        return Status::InvalidArgument(
            "Keys must be added in strict ascending order.");
      }
    }

    assert(value_type == kTypeValue || value_type == kTypeMerge ||
           value_type == kTypeDeletion ||
           value_type == kTypeDeletionWithTimestamp ||
           value_type == kTypeWideColumnEntity);

    constexpr SequenceNumber sequence_number = 0;

    ikey.Set(user_key, sequence_number, value_type);

    builder->Add(ikey.Encode(), value);

    // update file info
    file_info.num_entries++;
    file_info.largest_key.assign(user_key.data(), user_key.size());
    file_info.file_size = builder->FileSize();

    InvalidatePageCache(false /* closing */).PermitUncheckedError();
    return builder->status();
  }

  Status Add(const Slice& user_key, const Slice& value, ValueType value_type) {
    if (internal_comparator.user_comparator()->timestamp_size() != 0) {
      return Status::InvalidArgument("Timestamp size mismatch");
    }

    return AddImpl(user_key, value, value_type);
  }

  Status CheckAddPreconditions(const Slice& user_key) const {
    if (!builder) {
      return Status::InvalidArgument("File is not opened");
    }
    if (!builder->status().ok()) {
      return builder->status();
    }
    if (internal_comparator.user_comparator()->timestamp_size() != 0) {
      return Status::InvalidArgument("Timestamp size mismatch");
    }

    assert(user_key.size() >= ts_sz);
    if (strip_timestamp &&
        internal_comparator.user_comparator()->CompareTimestamp(
            Slice(user_key.data() + user_key.size() - ts_sz, ts_sz),
            MinU64Ts()) != 0) {
      return Status::InvalidArgument(
          "persist_user_defined_timestamps flag is set to false, only "
          "minimum timestamp is accepted.");
    }
    if (file_info.num_entries != 0 &&
        internal_comparator.user_comparator()->Compare(
            user_key, file_info.largest_key) <= 0) {
      return Status::InvalidArgument(
          "Keys must be added in strict ascending order.");
    }

    return Status::OK();
  }

  Status Add(const Slice& user_key, const Slice& timestamp, const Slice& value,
             ValueType value_type) {
    const size_t timestamp_size = timestamp.size();

    if (internal_comparator.user_comparator()->timestamp_size() !=
        timestamp_size) {
      return Status::InvalidArgument("Timestamp size mismatch");
    }

    const size_t user_key_size = user_key.size();

    if (user_key.data() + user_key_size == timestamp.data()) {
      Slice user_key_with_ts(user_key.data(), user_key_size + timestamp_size);
      return AddImpl(user_key_with_ts, value, value_type);
    }

    std::string user_key_with_ts;
    user_key_with_ts.reserve(user_key_size + timestamp_size);
    user_key_with_ts.append(user_key.data(), user_key_size);
    user_key_with_ts.append(timestamp.data(), timestamp_size);

    return AddImpl(user_key_with_ts, value, value_type);
  }

  Status AddEntity(const Slice& user_key, const WideColumns& columns) {
    WideColumns sorted_columns(columns);
    WideColumnsHelper::SortColumns(sorted_columns);

    std::string entity;
    const Status s = WideColumnSerialization::Serialize(sorted_columns, entity);
    if (!s.ok()) {
      return s;
    }
    if (entity.size() > size_t{std::numeric_limits<uint32_t>::max()}) {
      return Status::InvalidArgument("wide column entity is too large");
    }
    return Add(user_key, entity, kTypeWideColumnEntity);
  }

  Status AddEntityWithBlobIndexes(
      const Slice& user_key, const WideColumns& columns,
      const std::vector<SstFileWriterBlobColumn>& blob_columns) {
    std::vector<size_t> order(columns.size());
    std::iota(order.begin(), order.end(), size_t{0});
    std::sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs) {
      return columns[lhs].name().compare(columns[rhs].name()) < 0;
    });

    WideColumns sorted_columns;
    sorted_columns.reserve(columns.size());
    std::vector<size_t> old_to_new(columns.size());
    for (size_t new_index = 0; new_index < order.size(); ++new_index) {
      const size_t old_index = order[new_index];
      old_to_new[old_index] = new_index;
      sorted_columns.push_back(columns[old_index]);
    }

    std::vector<std::pair<size_t, BlobIndex>> decoded_blob_columns;
    decoded_blob_columns.reserve(blob_columns.size());
    for (const SstFileWriterBlobColumn& blob_column : blob_columns) {
      if (blob_column.column_index >= old_to_new.size()) {
        return Status::InvalidArgument("Blob column index out of range");
      }
      BlobIndex blob_index;
      Status s = blob_index.DecodeFrom(blob_column.blob_index);
      if (!s.ok()) {
        return s;
      }
      decoded_blob_columns.emplace_back(old_to_new[blob_column.column_index],
                                        blob_index);
    }

    std::string entity;
    const Status s = WideColumnSerialization::SerializeV2(
        sorted_columns, decoded_blob_columns, entity);
    if (!s.ok()) {
      return s;
    }
    if (entity.size() > size_t{std::numeric_limits<uint32_t>::max()}) {
      return Status::InvalidArgument("wide column entity is too large");
    }
    return Add(user_key, entity, kTypeWideColumnEntity);
  }

  Status AddEntityWithBlobValues(
      const Slice& user_key, const WideColumns& columns,
      const std::vector<SstFileWriterBlobValue>& blob_columns) {
    if (!write_blob_files) {
      return Status::InvalidArgument(
          "SstFileWriter was not opened with blob file support");
    }
    Status s = CheckAddPreconditions(user_key);
    if (!s.ok()) {
      return s;
    }

    std::vector<size_t> order(columns.size());
    std::iota(order.begin(), order.end(), size_t{0});
    std::sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs) {
      return columns[lhs].name().compare(columns[rhs].name()) < 0;
    });

    WideColumns sorted_columns;
    sorted_columns.reserve(columns.size());
    std::vector<size_t> old_to_new(columns.size());
    for (size_t new_index = 0; new_index < order.size(); ++new_index) {
      const size_t old_index = order[new_index];
      old_to_new[old_index] = new_index;
      sorted_columns.push_back(columns[old_index]);
    }
    for (size_t i = 1; i < sorted_columns.size(); ++i) {
      if (sorted_columns[i - 1].name().compare(sorted_columns[i].name()) >= 0) {
        return Status::Corruption("Wide columns out of order");
      }
    }

    std::vector<bool> has_blob_value(columns.size(), false);
    for (const SstFileWriterBlobValue& blob_column : blob_columns) {
      if (blob_column.column_index >= old_to_new.size()) {
        return Status::InvalidArgument("Blob column index out of range");
      }
      if (has_blob_value[blob_column.column_index]) {
        return Status::InvalidArgument("Duplicate blob column index");
      }
      has_blob_value[blob_column.column_index] = true;
    }

    std::vector<std::pair<size_t, BlobIndex>> decoded_blob_columns;
    decoded_blob_columns.reserve(blob_columns.size());
    for (const SstFileWriterBlobValue& blob_column : blob_columns) {
      BlobIndex blob_index;
      s = AddBlobValue(user_key, blob_column.value, &blob_index);
      if (!s.ok()) {
        return s;
      }
      decoded_blob_columns.emplace_back(old_to_new[blob_column.column_index],
                                        blob_index);
    }

    std::string entity;
    s = WideColumnSerialization::SerializeV2(sorted_columns,
                                             decoded_blob_columns, entity);
    if (!s.ok()) {
      return s;
    }
    if (entity.size() > size_t{std::numeric_limits<uint32_t>::max()}) {
      return Status::InvalidArgument("wide column entity is too large");
    }
    return Add(user_key, entity, kTypeWideColumnEntity);
  }

  Status AddBlobValue(const Slice& user_key, const Slice& blob_value,
                      BlobIndex* blob_index) {
    assert(blob_index);

    Status s = OpenBlobFileIfNeeded();
    if (!s.ok()) {
      return s;
    }
    assert(blob_writer);

    uint64_t key_offset = 0;
    uint64_t blob_offset = 0;
    s = blob_writer->AddRecord(write_options, user_key, blob_value,
                               &key_offset, &blob_offset);
    if (!s.ok()) {
      return s;
    }

    ++current_blob_count;
    current_blob_bytes +=
        BlobLogRecord::kHeaderSize + user_key.size() + blob_value.size();

    std::string encoded_blob_index;
    BlobIndex::EncodeBlob(&encoded_blob_index, current_blob_file_number,
                          blob_offset, blob_value.size(), kNoCompression);
    Slice blob_index_slice(encoded_blob_index);
    s = blob_index->DecodeFrom(blob_index_slice);
    if (!s.ok()) {
      return s;
    }

    return CloseBlobFileIfNeeded();
  }

  Status OpenBlobFileIfNeeded() {
    if (blob_writer) {
      return Status::OK();
    }
    if (cfh == nullptr) {
      return Status::InvalidArgument(
          "SstFileWriter blob files require a column family handle");
    }
    if (mutable_cf_options.blob_compression_type != kNoCompression) {
      return Status::NotSupported(
          "SstFileWriter blob value columns require uncompressed blobs");
    }

    current_blob_file_number = next_blob_file_number++;
    const std::string blob_file_dir =
        blob_options.blob_file_dir.empty() ? sst_file_dir
                                           : blob_options.blob_file_dir;
    current_blob_file_path =
        BlobFileName(blob_file_dir, current_blob_file_number);

    std::unique_ptr<FSWritableFile> blob_file;
    FileOptions blob_file_options(env_options);
    blob_file_options.temperature = blob_options.blob_file_temperature;
    Status s = ioptions.env->GetFileSystem()->NewWritableFile(
        current_blob_file_path, blob_file_options, &blob_file, nullptr);
    if (!s.ok()) {
      return s;
    }

    blob_file->SetIOPriority(io_priority);
    blob_file->SetWriteLifeTimeHint(blob_file_options.write_hint);

    FileTypeSet checksum_handoff_file_types =
        ioptions.checksum_handoff_file_types;
    std::unique_ptr<WritableFileWriter> blob_file_writer(new WritableFileWriter(
        std::move(blob_file), current_blob_file_path, env_options,
        ioptions.clock, nullptr /* io_tracer */, ioptions.stats,
        Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS, ioptions.listeners,
        ioptions.file_checksum_gen_factory.get(),
        checksum_handoff_file_types.Contains(FileType::kBlobFile), false));

    constexpr bool do_flush = false;
    blob_writer.reset(new BlobLogWriter(
        std::move(blob_file_writer), ioptions.clock, ioptions.stats,
        current_blob_file_number, ioptions.use_fsync, do_flush));

    constexpr bool has_ttl = false;
    constexpr ExpirationRange expiration_range;
    BlobLogHeader header(cfh->GetID(), kNoCompression, has_ttl,
                         expiration_range);
    s = blob_writer->WriteHeader(write_options, header);
    if (!s.ok()) {
      blob_writer.reset();
      return s;
    }

    current_blob_count = 0;
    current_blob_bytes = 0;
    return Status::OK();
  }

  Status CloseBlobFileIfNeeded() {
    assert(blob_writer);

    const uint64_t target_file_size =
        blob_options.blob_file_size != 0 ? blob_options.blob_file_size
                                         : mutable_cf_options.blob_file_size;
    if (target_file_size == 0 ||
        blob_writer->file()->GetFileSize() < target_file_size) {
      return Status::OK();
    }

    return CloseBlobFile();
  }

  Status CloseBlobFile() {
    if (!blob_writer) {
      return Status::OK();
    }

    BlobLogFooter footer;
    footer.blob_count = current_blob_count;

    std::string checksum_method;
    std::string checksum_value;
    Status s = blob_writer->AppendFooter(write_options, footer,
                                         &checksum_method, &checksum_value);
    if (!s.ok()) {
      return s;
    }

    ExternalBlobFileInfo blob_file_info;
    blob_file_info.external_file_path = current_blob_file_path;
    blob_file_info.blob_file_number = current_blob_file_number;
    blob_file_info.total_blob_count = current_blob_count;
    blob_file_info.total_blob_bytes = current_blob_bytes;
    blob_file_info.checksum_method = std::move(checksum_method);
    blob_file_info.checksum_value = std::move(checksum_value);
    blob_file_info.file_temperature = blob_options.blob_file_temperature;
    blob_file_infos.emplace_back(std::move(blob_file_info));

    blob_writer.reset();
    current_blob_file_number = 0;
    current_blob_count = 0;
    current_blob_bytes = 0;
    current_blob_file_path.clear();
    return Status::OK();
  }

  void AbandonBlobFiles() {
    blob_writer.reset();
    Status s;
    for (const ExternalBlobFileInfo& blob_file_info : blob_file_infos) {
      s = ioptions.env->DeleteFile(blob_file_info.external_file_path);
      s.PermitUncheckedError();
    }
    if (!current_blob_file_path.empty()) {
      s = ioptions.env->DeleteFile(current_blob_file_path);
      s.PermitUncheckedError();
    }
    blob_file_infos.clear();
  }

  Status DeleteRangeImpl(const Slice& begin_key, const Slice& end_key) {
    if (!builder) {
      return Status::InvalidArgument("File is not opened");
    }
    // begin_key + kNumInternalBytes must fit in uint32_t (BlockBuilder
    // assumption). end_key is stored as the value in the range deletion
    // block, so it only needs to fit in uint32_t.
    if (begin_key.size() >
        size_t{std::numeric_limits<uint32_t>::max()} - kNumInternalBytes) {
      return Status::InvalidArgument("key is too large");
    }
    if (end_key.size() > size_t{std::numeric_limits<uint32_t>::max()}) {
      return Status::InvalidArgument("end key is too large");
    }
    int cmp = internal_comparator.user_comparator()->CompareWithoutTimestamp(
        begin_key, end_key);
    if (cmp > 0) {
      // It's an empty range where endpoints appear mistaken. Don't bother
      // applying it to the DB, and return an error to the user.
      return Status::InvalidArgument("end key comes before start key");
    } else if (cmp == 0) {
      // It's an empty range. Don't bother applying it to the DB.
      return Status::OK();
    }

    assert(begin_key.size() >= ts_sz);
    assert(end_key.size() >= ts_sz);
    Slice begin_key_ts =
        Slice(begin_key.data() + begin_key.size() - ts_sz, ts_sz);
    Slice end_key_ts = Slice(end_key.data() + end_key.size() - ts_sz, ts_sz);
    assert(begin_key_ts.compare(end_key_ts) == 0);
    if (strip_timestamp) {
      // In this mode, we expect users to always provide a min timestamp.
      if (internal_comparator.user_comparator()->CompareTimestamp(
              begin_key_ts, MinU64Ts()) != 0) {
        return Status::InvalidArgument(
            "persist_user_defined_timestamps flag is set to false, only "
            "minimum timestamp is accepted for start key.");
      }
      if (internal_comparator.user_comparator()->CompareTimestamp(
              end_key_ts, MinU64Ts()) != 0) {
        return Status::InvalidArgument(
            "persist_user_defined_timestamps flag is set to false, only "
            "minimum timestamp is accepted for end key.");
      }
    }

    RangeTombstone tombstone(begin_key, end_key, 0 /* Sequence Number */);
    if (file_info.num_range_del_entries == 0) {
      file_info.smallest_range_del_key.assign(tombstone.start_key_.data(),
                                              tombstone.start_key_.size());
      file_info.largest_range_del_key.assign(tombstone.end_key_.data(),
                                             tombstone.end_key_.size());
    } else {
      if (internal_comparator.user_comparator()->Compare(
              tombstone.start_key_, file_info.smallest_range_del_key) < 0) {
        file_info.smallest_range_del_key.assign(tombstone.start_key_.data(),
                                                tombstone.start_key_.size());
      }
      if (internal_comparator.user_comparator()->Compare(
              tombstone.end_key_, file_info.largest_range_del_key) > 0) {
        file_info.largest_range_del_key.assign(tombstone.end_key_.data(),
                                               tombstone.end_key_.size());
      }
    }

    auto ikey_and_end_key = tombstone.Serialize();
    builder->Add(ikey_and_end_key.first.Encode(), ikey_and_end_key.second);

    // update file info
    file_info.num_range_del_entries++;
    file_info.file_size = builder->FileSize();

    InvalidatePageCache(false /* closing */).PermitUncheckedError();
    return Status::OK();
  }

  Status DeleteRange(const Slice& begin_key, const Slice& end_key) {
    if (internal_comparator.user_comparator()->timestamp_size() != 0) {
      return Status::InvalidArgument("Timestamp size mismatch");
    }
    return DeleteRangeImpl(begin_key, end_key);
  }

  // begin_key and end_key should be users keys without timestamp.
  Status DeleteRange(const Slice& begin_key, const Slice& end_key,
                     const Slice& timestamp) {
    const size_t timestamp_size = timestamp.size();

    if (internal_comparator.user_comparator()->timestamp_size() !=
        timestamp_size) {
      return Status::InvalidArgument("Timestamp size mismatch");
    }

    const size_t begin_key_size = begin_key.size();
    const size_t end_key_size = end_key.size();
    if (begin_key.data() + begin_key_size == timestamp.data() ||
        end_key.data() + begin_key_size == timestamp.data()) {
      assert(memcmp(begin_key.data() + begin_key_size,
                    end_key.data() + end_key_size, timestamp_size) == 0);
      Slice begin_key_with_ts(begin_key.data(),
                              begin_key_size + timestamp_size);
      Slice end_key_with_ts(end_key.data(), end_key.size() + timestamp_size);
      return DeleteRangeImpl(begin_key_with_ts, end_key_with_ts);
    }
    std::string begin_key_with_ts;
    begin_key_with_ts.reserve(begin_key_size + timestamp_size);
    begin_key_with_ts.append(begin_key.data(), begin_key_size);
    begin_key_with_ts.append(timestamp.data(), timestamp_size);
    std::string end_key_with_ts;
    end_key_with_ts.reserve(end_key_size + timestamp_size);
    end_key_with_ts.append(end_key.data(), end_key_size);
    end_key_with_ts.append(timestamp.data(), timestamp_size);
    return DeleteRangeImpl(begin_key_with_ts, end_key_with_ts);
  }

  Status InvalidatePageCache(bool closing) {
    Status s = Status::OK();
    if (invalidate_page_cache == false) {
      // Fadvise disabled
      return s;
    }
    uint64_t bytes_since_last_fadvise = builder->FileSize() - last_fadvise_size;
    if (bytes_since_last_fadvise > kFadviseTrigger || closing) {
      TEST_SYNC_POINT_CALLBACK("SstFileWriter::Rep::InvalidatePageCache",
                               &(bytes_since_last_fadvise));
      // Tell the OS that we don't need this file in page cache
      s = file_writer->InvalidateCache(0, 0);
      if (s.IsNotSupported()) {
        // NotSupported is fine as it could be a file type that doesn't use page
        // cache.
        s = Status::OK();
      }
      last_fadvise_size = builder->FileSize();
    }
    return s;
  }
};

SstFileWriter::SstFileWriter(const EnvOptions& env_options,
                             const Options& options,
                             const Comparator* user_comparator,
                             ColumnFamilyHandle* column_family,
                             bool invalidate_page_cache,
                             Env::IOPriority io_priority)
    : rep_(new Rep(env_options, options, io_priority, user_comparator,
                   column_family, invalidate_page_cache,
                   DBImpl::GenerateDbSessionId(options.env))) {
  // SstFileWriter is used to create sst files that can be added to database
  // later. Therefore, no real db_id and db_session_id are associated with it.
  // Here we mimic the way db_session_id behaves by getting a db_session_id
  // for each SstFileWriter, and (later below) assign unique file numbers
  // in the table properties. The db_id is set to be "SST Writer" for clarity.

  rep_->file_info.file_size = 0;
}

SstFileWriter::~SstFileWriter() {
  if (rep_->builder) {
    // User did not call Finish() or Finish() failed, we need to
    // abandon the builder.
    rep_->builder->Abandon();
  }
  rep_->AbandonBlobFiles();
}

Status SstFileWriter::Open(const std::string& file_path, Temperature temp) {
  Rep* r = rep_.get();
  if (!r->opening_with_blob_files) {
    r->write_blob_files = false;
    r->blob_options = SstFileWriterBlobOptions();
    r->next_blob_file_number = 0;
    r->blob_file_infos.clear();
  }
  Status s;
  std::unique_ptr<FSWritableFile> sst_file;
  FileOptions cur_file_opts(r->env_options);
  cur_file_opts.temperature = temp;
  cur_file_opts.open_contract = FileOpenContract::kNoReopenForWrite |
                                FileOpenContract::kNoReadersWhileOpenForWrite;
  s = r->ioptions.env->GetFileSystem()->NewWritableFile(
      file_path, cur_file_opts, &sst_file, nullptr);
  if (!s.ok()) {
    return s;
  }

  sst_file->SetIOPriority(r->io_priority);
  r->sst_file_dir = GetParentDir(file_path);

  CompressionType compression_type;
  CompressionOptions compression_opts;
  if (r->mutable_cf_options.bottommost_compression !=
      kDisableCompressionOption) {
    compression_type = r->mutable_cf_options.bottommost_compression;
    if (r->mutable_cf_options.bottommost_compression_opts.enabled) {
      compression_opts = r->mutable_cf_options.bottommost_compression_opts;
    } else {
      compression_opts = r->mutable_cf_options.compression_opts;
    }
  } else if (!r->mutable_cf_options.compression_per_level.empty()) {
    // Use the compression of the last level if we have per level compression
    compression_type = *(r->mutable_cf_options.compression_per_level.rbegin());
    compression_opts = r->mutable_cf_options.compression_opts;
  } else {
    compression_type = r->mutable_cf_options.compression;
    compression_opts = r->mutable_cf_options.compression_opts;
  }

  InternalTblPropCollFactories internal_tbl_prop_coll_factories;

  // SstFileWriter properties collector to add SstFileWriter version.
  internal_tbl_prop_coll_factories.emplace_back(
      new SstFileWriterPropertiesCollectorFactory(2 /* version */,
                                                  0 /* global_seqno*/));

  // User collector factories
  auto user_collector_factories =
      r->ioptions.table_properties_collector_factories;
  for (size_t i = 0; i < user_collector_factories.size(); i++) {
    internal_tbl_prop_coll_factories.emplace_back(
        new UserKeyTablePropertiesCollectorFactory(
            user_collector_factories[i]));
  }
  int unknown_level = -1;
  uint32_t cf_id;

  if (r->cfh != nullptr) {
    // user explicitly specified that this file will be ingested into cfh,
    // we can persist this information in the file.
    cf_id = r->cfh->GetID();
    r->column_family_name = r->cfh->GetName();
  } else {
    r->column_family_name = "";
    cf_id = TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;
  }

  // TODO: it would be better to set oldest_key_time to be used for getting the
  //  approximate time of ingested keys.
  // TODO: plumb Env::IOActivity, Env::IOPriority
  TableBuilderOptions table_builder_options(
      r->ioptions, r->mutable_cf_options, ReadOptions(), r->write_options,
      r->internal_comparator, &internal_tbl_prop_coll_factories,
      compression_type, compression_opts, cf_id, r->column_family_name,
      unknown_level, kUnknownNewestKeyTime, false /* is_bottommost */,
      TableFileCreationReason::kMisc, 0 /* oldest_key_time */,
      0 /* file_creation_time */, "SST Writer" /* db_id */, r->db_session_id,
      0 /* target_file_size */, r->next_file_number);
  // External SST files used to each get a unique session id. Now for
  // slightly better uniqueness probability in constructing cache keys, we
  // assign fake file numbers to each file (into table properties) and keep
  // the same session id for the life of the SstFileWriter.
  r->next_file_number++;
  FileTypeSet tmp_set = r->ioptions.checksum_handoff_file_types;
  r->file_writer.reset(new WritableFileWriter(
      std::move(sst_file), file_path, r->env_options, r->ioptions.clock,
      nullptr /* io_tracer */, r->ioptions.stats, Histograms::SST_WRITE_MICROS,
      r->ioptions.listeners, r->ioptions.file_checksum_gen_factory.get(),
      tmp_set.Contains(FileType::kTableFile), false));

  // TODO(tec) : If table_factory is using compressed block cache, we will
  // be adding the external sst file blocks into it, which is wasteful.
  r->builder.reset(r->mutable_cf_options.table_factory->NewTableBuilder(
      table_builder_options, r->file_writer.get()));

  r->file_info = ExternalSstFileInfo();
  r->file_info.file_path = file_path;
  r->file_info.version = 2;
  return s;
}

Status SstFileWriter::OpenWithBlobFiles(
    const std::string& file_path, const SstFileWriterBlobOptions& blob_options,
    Temperature temp) {
  if (blob_options.starting_blob_file_number == 0) {
    return Status::InvalidArgument("Blob file numbers must be non-zero");
  }

  Rep* r = rep_.get();
  r->opening_with_blob_files = true;
  r->write_blob_files = true;
  r->blob_options = blob_options;
  r->next_blob_file_number = blob_options.starting_blob_file_number;
  Status s = Open(file_path, temp);
  r->opening_with_blob_files = false;
  if (!s.ok()) {
    r->write_blob_files = false;
    r->blob_options = SstFileWriterBlobOptions();
    r->next_blob_file_number = 0;
  }
  return s;
}

Status SstFileWriter::Put(const Slice& user_key, const Slice& value) {
  return rep_->Add(user_key, value, ValueType::kTypeValue);
}

Status SstFileWriter::Put(const Slice& user_key, const Slice& timestamp,
                          const Slice& value) {
  return rep_->Add(user_key, timestamp, value, ValueType::kTypeValue);
}

Status SstFileWriter::PutEntity(const Slice& user_key,
                                const WideColumns& columns) {
  return rep_->AddEntity(user_key, columns);
}

Status SstFileWriter::PutEntityWithBlobIndexes(
    const Slice& user_key, const WideColumns& columns,
    const std::vector<SstFileWriterBlobColumn>& blob_columns) {
  return rep_->AddEntityWithBlobIndexes(user_key, columns, blob_columns);
}

Status SstFileWriter::PutEntityWithBlobValues(
    const Slice& user_key, const WideColumns& columns,
    const std::vector<SstFileWriterBlobValue>& blob_columns) {
  return rep_->AddEntityWithBlobValues(user_key, columns, blob_columns);
}

Status SstFileWriter::Merge(const Slice& user_key, const Slice& value) {
  return rep_->Add(user_key, value, ValueType::kTypeMerge);
}

Status SstFileWriter::Delete(const Slice& user_key) {
  return rep_->Add(user_key, Slice(), ValueType::kTypeDeletion);
}

Status SstFileWriter::Delete(const Slice& user_key, const Slice& timestamp) {
  return rep_->Add(user_key, timestamp, Slice(),
                   ValueType::kTypeDeletionWithTimestamp);
}

Status SstFileWriter::DeleteRange(const Slice& begin_key,
                                  const Slice& end_key) {
  return rep_->DeleteRange(begin_key, end_key);
}

Status SstFileWriter::DeleteRange(const Slice& begin_key, const Slice& end_key,
                                  const Slice& timestamp) {
  return rep_->DeleteRange(begin_key, end_key, timestamp);
}

Status SstFileWriter::Finish(ExternalSstFileInfo* file_info) {
  return Finish(file_info, nullptr);
}

Status SstFileWriter::Finish(
    ExternalSstFileInfo* file_info,
    std::vector<ExternalBlobFileInfo>* blob_file_infos) {
  Rep* r = rep_.get();
  if (!r->builder) {
    return Status::InvalidArgument("File is not opened");
  }
  if (r->file_info.num_entries == 0 &&
      r->file_info.num_range_del_entries == 0) {
    r->builder->status().PermitUncheckedError();
    return Status::InvalidArgument("Cannot create sst file with no entries");
  }

  Status s = r->CloseBlobFile();
  if (s.ok()) {
    s = r->builder->Finish();
  }
  r->file_info.file_size = r->builder->FileSize();

  IOOptions opts;
  if (s.ok()) {
    s = WritableFileWriter::PrepareIOOptions(r->write_options, opts);
  }
  if (s.ok()) {
    s = r->file_writer->Sync(opts, r->ioptions.use_fsync);
    r->InvalidatePageCache(true /* closing */).PermitUncheckedError();
    if (s.ok()) {
      s = r->file_writer->Close(opts);
    }
  }
  if (s.ok()) {
    r->file_info.file_checksum = r->file_writer->GetFileChecksum();
    r->file_info.file_checksum_func_name =
        r->file_writer->GetFileChecksumFuncName();
  }
  if (!s.ok()) {
    Status status = r->ioptions.env->DeleteFile(r->file_info.file_path);
    // Silence ASSERT_STATUS_CHECKED warning, since DeleteFile may fail under
    // some error injection, and we can just ignore the failure
    status.PermitUncheckedError();
    r->AbandonBlobFiles();
  }

  if (file_info != nullptr) {
    *file_info = r->file_info;
    Slice smallest_key = r->file_info.smallest_key;
    Slice largest_key = r->file_info.largest_key;
    Slice smallest_range_del_key = r->file_info.smallest_range_del_key;
    Slice largest_range_del_key = r->file_info.largest_range_del_key;
    assert(smallest_key.empty() == largest_key.empty());
    assert(smallest_range_del_key.empty() == largest_range_del_key.empty());
    // Remove user-defined timestamps from external file metadata too when they
    // should not be persisted.
    if (r->strip_timestamp) {
      if (!smallest_key.empty()) {
        assert(smallest_key.size() >= r->ts_sz);
        assert(largest_key.size() >= r->ts_sz);
        file_info->smallest_key.resize(smallest_key.size() - r->ts_sz);
        file_info->largest_key.resize(largest_key.size() - r->ts_sz);
      }
      if (!smallest_range_del_key.empty()) {
        assert(smallest_range_del_key.size() >= r->ts_sz);
        assert(largest_range_del_key.size() >= r->ts_sz);
        file_info->smallest_range_del_key.resize(smallest_range_del_key.size() -
                                                 r->ts_sz);
        file_info->largest_range_del_key.resize(largest_range_del_key.size() -
                                                r->ts_sz);
      }
    }
  }

  if (blob_file_infos != nullptr) {
    *blob_file_infos = r->blob_file_infos;
  }

  r->builder.reset();
  r->blob_file_infos.clear();
  return s;
}

uint64_t SstFileWriter::FileSize() { return rep_->file_info.file_size; }

bool SstFileWriter::CreatedBySstFileWriter(const TableProperties& tp) {
  const auto& uprops = tp.user_collected_properties;
  return uprops.find(ExternalSstFilePropertyNames::kVersion) != uprops.end();
}

}  // namespace ROCKSDB_NAMESPACE
