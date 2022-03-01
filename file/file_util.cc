//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "file/file_util.h"

#include <string>
#include <algorithm>

#include "file/random_access_file_reader.h"
#include "file/sequence_file_reader.h"
#include "file/sst_file_manager_impl.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

// Utility function to copy a file up to a specified length
IOStatus CopyFile(FileSystem* fs, const std::string& source,
                  const std::string& destination, uint64_t size, bool use_fsync,
                  const std::shared_ptr<IOTracer>& io_tracer,
                  const Temperature temperature) {
  FileOptions soptions;
  IOStatus io_s;
  std::unique_ptr<SequentialFileReader> src_reader;
  std::unique_ptr<WritableFileWriter> dest_writer;

  {
    soptions.temperature = temperature;
    std::unique_ptr<FSSequentialFile> srcfile;
    io_s = fs->NewSequentialFile(source, soptions, &srcfile, nullptr);
    if (!io_s.ok()) {
      return io_s;
    }
    std::unique_ptr<FSWritableFile> destfile;
    io_s = fs->NewWritableFile(destination, soptions, &destfile, nullptr);
    if (!io_s.ok()) {
      return io_s;
    }

    if (size == 0) {
      // default argument means copy everything
      io_s = fs->GetFileSize(source, IOOptions(), &size, nullptr);
      if (!io_s.ok()) {
        return io_s;
      }
    }
    src_reader.reset(
        new SequentialFileReader(std::move(srcfile), source, io_tracer));
    dest_writer.reset(
        new WritableFileWriter(std::move(destfile), destination, soptions));
  }

  char buffer[4096];
  Slice slice;
  while (size > 0) {
    size_t bytes_to_read = std::min(sizeof(buffer), static_cast<size_t>(size));
    io_s = status_to_io_status(src_reader->Read(bytes_to_read, &slice, buffer));
    if (!io_s.ok()) {
      return io_s;
    }
    if (slice.size() == 0) {
      return IOStatus::Corruption("file too small");
    }
    io_s = dest_writer->Append(slice);
    if (!io_s.ok()) {
      return io_s;
    }
    size -= slice.size();
  }
  return dest_writer->Sync(use_fsync);
}

// Utility function to create a file with the provided contents
IOStatus CreateFile(FileSystem* fs, const std::string& destination,
                    const std::string& contents, bool use_fsync) {
  const EnvOptions soptions;
  IOStatus io_s;
  std::unique_ptr<WritableFileWriter> dest_writer;

  std::unique_ptr<FSWritableFile> destfile;
  io_s = fs->NewWritableFile(destination, soptions, &destfile, nullptr);
  if (!io_s.ok()) {
    return io_s;
  }
  dest_writer.reset(
      new WritableFileWriter(std::move(destfile), destination, soptions));
  io_s = dest_writer->Append(Slice(contents));
  if (!io_s.ok()) {
    return io_s;
  }
  return dest_writer->Sync(use_fsync);
}

Status DeleteDBFile(const ImmutableDBOptions* db_options,
                    const std::string& fname, const std::string& dir_to_sync,
                    const bool force_bg, const bool force_fg) {
#ifndef ROCKSDB_LITE
  SstFileManagerImpl* sfm =
      static_cast<SstFileManagerImpl*>(db_options->sst_file_manager.get());
  if (sfm && !force_fg) {
    return sfm->ScheduleFileDeletion(fname, dir_to_sync, force_bg);
  } else {
    return db_options->env->DeleteFile(fname);
  }
#else
  (void)dir_to_sync;
  (void)force_bg;
  (void)force_fg;
  // SstFileManager is not supported in ROCKSDB_LITE
  // Delete file immediately
  return db_options->env->DeleteFile(fname);
#endif
}

// requested_checksum_func_name brings the function name of the checksum
// generator in checksum_factory. Empty string is permitted, in which case the
// name of the generator created by the factory is unchecked. When
// `requested_checksum_func_name` is non-empty, however, the created generator's
// name must match it, otherwise an `InvalidArgument` error is returned.
IOStatus GenerateOneFileChecksum(
    FileSystem* fs, const std::string& file_path,
    FileChecksumGenFactory* checksum_factory,
    const std::string& requested_checksum_func_name, std::string* file_checksum,
    std::string* file_checksum_func_name,
    size_t verify_checksums_readahead_size, bool allow_mmap_reads,
    std::shared_ptr<IOTracer>& io_tracer, RateLimiter* rate_limiter,
    Env::IOPriority rate_limiter_priority) {
  if (checksum_factory == nullptr) {
    return IOStatus::InvalidArgument("Checksum factory is invalid");
  }
  assert(file_checksum != nullptr);
  assert(file_checksum_func_name != nullptr);

  FileChecksumGenContext gen_context;
  gen_context.requested_checksum_func_name = requested_checksum_func_name;
  gen_context.file_name = file_path;
  std::unique_ptr<FileChecksumGenerator> checksum_generator =
      checksum_factory->CreateFileChecksumGenerator(gen_context);
  if (checksum_generator == nullptr) {
    std::string msg =
        "Cannot get the file checksum generator based on the requested "
        "checksum function name: " +
        requested_checksum_func_name +
        " from checksum factory: " + checksum_factory->Name();
    return IOStatus::InvalidArgument(msg);
  } else {
    // For backward compatibility and use in file ingestion clients where there
    // is no stored checksum function name, `requested_checksum_func_name` can
    // be empty. If we give the requested checksum function name, we expect it
    // is the same name of the checksum generator.
    if (!requested_checksum_func_name.empty() &&
        checksum_generator->Name() != requested_checksum_func_name) {
      std::string msg = "Expected file checksum generator named '" +
                        requested_checksum_func_name +
                        "', while the factory created one "
                        "named '" +
                        checksum_generator->Name() + "'";
      return IOStatus::InvalidArgument(msg);
    }
  }

  uint64_t size;
  IOStatus io_s;
  std::unique_ptr<RandomAccessFileReader> reader;
  {
    std::unique_ptr<FSRandomAccessFile> r_file;
    io_s = fs->NewRandomAccessFile(file_path, FileOptions(), &r_file, nullptr);
    if (!io_s.ok()) {
      return io_s;
    }
    io_s = fs->GetFileSize(file_path, IOOptions(), &size, nullptr);
    if (!io_s.ok()) {
      return io_s;
    }
    reader.reset(new RandomAccessFileReader(std::move(r_file), file_path,
                                            nullptr /*Env*/, io_tracer, nullptr,
                                            0, nullptr, rate_limiter));
  }

  // Found that 256 KB readahead size provides the best performance, based on
  // experiments, for auto readahead. Experiment data is in PR #3282.
  size_t default_max_read_ahead_size = 256 * 1024;
  size_t readahead_size = (verify_checksums_readahead_size != 0)
                              ? verify_checksums_readahead_size
                              : default_max_read_ahead_size;

  FilePrefetchBuffer prefetch_buffer(readahead_size /* readahead_size */,
                                     readahead_size /* max_readahead_size */,
                                     !allow_mmap_reads /* enable */);

  Slice slice;
  uint64_t offset = 0;
  IOOptions opts;
  while (size > 0) {
    size_t bytes_to_read =
        static_cast<size_t>(std::min(uint64_t{readahead_size}, size));
    if (!prefetch_buffer.TryReadFromCache(
            opts, reader.get(), offset, bytes_to_read, &slice,
            nullptr /* status */, rate_limiter_priority,
            false /* for_compaction */)) {
      return IOStatus::Corruption("file read failed");
    }
    if (slice.size() == 0) {
      return IOStatus::Corruption("file too small");
    }
    checksum_generator->Update(slice.data(), slice.size());
    size -= slice.size();
    offset += slice.size();
  }
  checksum_generator->Finalize();
  *file_checksum = checksum_generator->GetChecksum();
  *file_checksum_func_name = checksum_generator->Name();
  return IOStatus::OK();
}

Status DestroyDir(Env* env, const std::string& dir) {
  Status s;
  if (env->FileExists(dir).IsNotFound()) {
    return s;
  }
  std::vector<std::string> files_in_dir;
  s = env->GetChildren(dir, &files_in_dir);
  if (s.ok()) {
    for (auto& file_in_dir : files_in_dir) {
      std::string path = dir + "/" + file_in_dir;
      bool is_dir = false;
      s = env->IsDirectory(path, &is_dir);
      if (s.ok()) {
        if (is_dir) {
          s = DestroyDir(env, path);
        } else {
          s = env->DeleteFile(path);
        }
      } else if (s.IsNotSupported()) {
        s = Status::OK();
      }
      if (!s.ok()) {
        // IsDirectory, etc. might not report NotFound
        if (s.IsNotFound() || env->FileExists(path).IsNotFound()) {
          // Allow files to be deleted externally
          s = Status::OK();
        } else {
          break;
        }
      }
    }
  }

  if (s.ok()) {
    s = env->DeleteDir(dir);
    // DeleteDir might or might not report NotFound
    if (!s.ok() && (s.IsNotFound() || env->FileExists(dir).IsNotFound())) {
      // Allow to be deleted externally
      s = Status::OK();
    }
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
