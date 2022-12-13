//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/env_encryption.h"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <iostream>

#include "env/composite_env_wrapper.h"
#include "env/env_encryption_ctr.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/convenience.h"
#include "rocksdb/io_status.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/options_type.h"
#include "util/aligned_buffer.h"
#include "util/coding.h"
#include "util/random.h"
#include "util/string_util.h"

#endif
namespace ROCKSDB_NAMESPACE {
#ifndef ROCKSDB_LITE
std::shared_ptr<EncryptionProvider> EncryptionProvider::NewCTRProvider(
    const std::shared_ptr<BlockCipher>& cipher) {
  return std::make_shared<CTREncryptionProvider>(cipher);
}

// Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
// written by this routine.  Sets "*result" to the data that was
// read (including if fewer than "n" bytes were successfully read).
// May set "*result" to point at data in "scratch[0..n-1]", so
// "scratch[0..n-1]" must be live when "*result" is used.
// If an error was encountered, returns a non-OK status.
//
// REQUIRES: External synchronization
IOStatus EncryptedSequentialFile::Read(size_t n, const IOOptions& options,
                                       Slice* result, char* scratch,
                                       IODebugContext* dbg) {
  assert(scratch);
  IOStatus io_s = file_->Read(n, options, result, scratch, dbg);
  if (!io_s.ok()) {
    return io_s;
  }
  {
    PERF_TIMER_GUARD(decrypt_data_nanos);
    io_s = status_to_io_status(
        stream_->Decrypt(offset_, (char*)result->data(), result->size()));
  }
  if (io_s.ok()) {
    offset_ += result->size();  // We've already ready data from disk, so update
                                // offset_ even if decryption fails.
  }
  return io_s;
}

// Skip "n" bytes from the file. This is guaranteed to be no
// slower that reading the same data, but may be faster.
//
// If end of file is reached, skipping will stop at the end of the
// file, and Skip will return OK.
//
// REQUIRES: External synchronization
IOStatus EncryptedSequentialFile::Skip(uint64_t n) {
  auto status = file_->Skip(n);
  if (!status.ok()) {
    return status;
  }
  offset_ += n;
  return status;
}

// Indicates the upper layers if the current SequentialFile implementation
// uses direct IO.
bool EncryptedSequentialFile::use_direct_io() const {
  return file_->use_direct_io();
}

// Use the returned alignment value to allocate
// aligned buffer for Direct I/O
size_t EncryptedSequentialFile::GetRequiredBufferAlignment() const {
  return file_->GetRequiredBufferAlignment();
}

// Remove any kind of caching of data from the offset to offset+length
// of this file. If the length is 0, then it refers to the end of file.
// If the system is not caching the file contents, then this is a noop.
IOStatus EncryptedSequentialFile::InvalidateCache(size_t offset,
                                                  size_t length) {
  return file_->InvalidateCache(offset + prefixLength_, length);
}

// Positioned Read for direct I/O
// If Direct I/O enabled, offset, n, and scratch should be properly aligned
IOStatus EncryptedSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                                 const IOOptions& options,
                                                 Slice* result, char* scratch,
                                                 IODebugContext* dbg) {
  assert(scratch);
  offset += prefixLength_;  // Skip prefix
  auto io_s = file_->PositionedRead(offset, n, options, result, scratch, dbg);
  if (!io_s.ok()) {
    return io_s;
  }
  offset_ = offset + result->size();
  {
    PERF_TIMER_GUARD(decrypt_data_nanos);
    io_s = status_to_io_status(
        stream_->Decrypt(offset, (char*)result->data(), result->size()));
  }
  return io_s;
}

// Read up to "n" bytes from the file starting at "offset".
// "scratch[0..n-1]" may be written by this routine.  Sets "*result"
// to the data that was read (including if fewer than "n" bytes were
// successfully read).  May set "*result" to point at data in
// "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
// "*result" is used.  If an error was encountered, returns a non-OK
// status.
//
// Safe for concurrent use by multiple threads.
// If Direct I/O enabled, offset, n, and scratch should be aligned properly.
IOStatus EncryptedRandomAccessFile::Read(uint64_t offset, size_t n,
                                         const IOOptions& options,
                                         Slice* result, char* scratch,
                                         IODebugContext* dbg) const {
  assert(scratch);
  offset += prefixLength_;
  auto io_s = file_->Read(offset, n, options, result, scratch, dbg);
  if (!io_s.ok()) {
    return io_s;
  }
  {
    PERF_TIMER_GUARD(decrypt_data_nanos);
    io_s = status_to_io_status(
        stream_->Decrypt(offset, (char*)result->data(), result->size()));
  }
  return io_s;
}

// Readahead the file starting from offset by n bytes for caching.
IOStatus EncryptedRandomAccessFile::Prefetch(uint64_t offset, size_t n,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  // return Status::OK();
  return file_->Prefetch(offset + prefixLength_, n, options, dbg);
}

// Tries to get an unique ID for this file that will be the same each time
// the file is opened (and will stay the same while the file is open).
// Furthermore, it tries to make this ID at most "max_size" bytes. If such an
// ID can be created this function returns the length of the ID and places it
// in "id"; otherwise, this function returns 0, in which case "id"
// may not have been modified.
//
// This function guarantees, for IDs from a given environment, two unique ids
// cannot be made equal to each other by adding arbitrary bytes to one of
// them. That is, no unique ID is the prefix of another.
//
// This function guarantees that the returned ID will not be interpretable as
// a single varint.
//
// Note: these IDs are only valid for the duration of the process.
size_t EncryptedRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return file_->GetUniqueId(id, max_size);
};

void EncryptedRandomAccessFile::Hint(AccessPattern pattern) {
  file_->Hint(pattern);
}

// Indicates the upper layers if the current RandomAccessFile implementation
// uses direct IO.
bool EncryptedRandomAccessFile::use_direct_io() const {
  return file_->use_direct_io();
}

// Use the returned alignment value to allocate
// aligned buffer for Direct I/O
size_t EncryptedRandomAccessFile::GetRequiredBufferAlignment() const {
  return file_->GetRequiredBufferAlignment();
}

// Remove any kind of caching of data from the offset to offset+length
// of this file. If the length is 0, then it refers to the end of file.
// If the system is not caching the file contents, then this is a noop.
IOStatus EncryptedRandomAccessFile::InvalidateCache(size_t offset,
                                                    size_t length) {
  return file_->InvalidateCache(offset + prefixLength_, length);
}

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
IOStatus EncryptedWritableFile::Append(const Slice& data,
                                       const IOOptions& options,
                                       IODebugContext* dbg) {
  AlignedBuffer buf;
  Slice dataToAppend(data);
  if (data.size() > 0) {
    auto offset = file_->GetFileSize(options, dbg);  // size including prefix
    // Encrypt in cloned buffer
    buf.Alignment(GetRequiredBufferAlignment());
    buf.AllocateNewBuffer(data.size());
    // TODO (sagar0): Modify AlignedBuffer.Append to allow doing a memmove
    // so that the next two lines can be replaced with buf.Append().
    memmove(buf.BufferStart(), data.data(), data.size());
    buf.Size(data.size());
    IOStatus io_s;
    {
      PERF_TIMER_GUARD(encrypt_data_nanos);
      io_s = status_to_io_status(
          stream_->Encrypt(offset, buf.BufferStart(), buf.CurrentSize()));
    }
    if (!io_s.ok()) {
      return io_s;
    }
    dataToAppend = Slice(buf.BufferStart(), buf.CurrentSize());
  }
  return file_->Append(dataToAppend, options, dbg);
}

IOStatus EncryptedWritableFile::PositionedAppend(const Slice& data,
                                                 uint64_t offset,
                                                 const IOOptions& options,
                                                 IODebugContext* dbg) {
  AlignedBuffer buf;
  Slice dataToAppend(data);
  offset += prefixLength_;
  if (data.size() > 0) {
    // Encrypt in cloned buffer
    buf.Alignment(GetRequiredBufferAlignment());
    buf.AllocateNewBuffer(data.size());
    memmove(buf.BufferStart(), data.data(), data.size());
    buf.Size(data.size());
    IOStatus io_s;
    {
      PERF_TIMER_GUARD(encrypt_data_nanos);
      io_s = status_to_io_status(
          stream_->Encrypt(offset, buf.BufferStart(), buf.CurrentSize()));
    }
    if (!io_s.ok()) {
      return io_s;
    }
    dataToAppend = Slice(buf.BufferStart(), buf.CurrentSize());
  }
  return file_->PositionedAppend(dataToAppend, offset, options, dbg);
}

// Indicates the upper layers if the current WritableFile implementation
// uses direct IO.
bool EncryptedWritableFile::use_direct_io() const {
  return file_->use_direct_io();
}

// true if Sync() and Fsync() are safe to call concurrently with Append()
// and Flush().
bool EncryptedWritableFile::IsSyncThreadSafe() const {
  return file_->IsSyncThreadSafe();
}

// Use the returned alignment value to allocate
// aligned buffer for Direct I/O
size_t EncryptedWritableFile::GetRequiredBufferAlignment() const {
  return file_->GetRequiredBufferAlignment();
}

/*
 * Get the size of valid data in the file.
 */
uint64_t EncryptedWritableFile::GetFileSize(const IOOptions& options,
                                            IODebugContext* dbg) {
  return file_->GetFileSize(options, dbg) - prefixLength_;
}

// Truncate is necessary to trim the file to the correct size
// before closing. It is not always possible to keep track of the file
// size due to whole pages writes. The behavior is undefined if called
// with other writes to follow.
IOStatus EncryptedWritableFile::Truncate(uint64_t size,
                                         const IOOptions& options,
                                         IODebugContext* dbg) {
  return file_->Truncate(size + prefixLength_, options, dbg);
}

// Remove any kind of caching of data from the offset to offset+length
// of this file. If the length is 0, then it refers to the end of file.
// If the system is not caching the file contents, then this is a noop.
// This call has no effect on dirty pages in the cache.
IOStatus EncryptedWritableFile::InvalidateCache(size_t offset, size_t length) {
  return file_->InvalidateCache(offset + prefixLength_, length);
}

// Sync a file range with disk.
// offset is the starting byte of the file range to be synchronized.
// nbytes specifies the length of the range to be synchronized.
// This asks the OS to initiate flushing the cached data to disk,
// without waiting for completion.
// Default implementation does nothing.
IOStatus EncryptedWritableFile::RangeSync(uint64_t offset, uint64_t nbytes,
                                          const IOOptions& options,
                                          IODebugContext* dbg) {
  return file_->RangeSync(offset + prefixLength_, nbytes, options, dbg);
}

// PrepareWrite performs any necessary preparation for a write
// before the write actually occurs.  This allows for pre-allocation
// of space on devices where it can result in less file
// fragmentation and/or less waste from over-zealous filesystem
// pre-allocation.
void EncryptedWritableFile::PrepareWrite(size_t offset, size_t len,
                                         const IOOptions& options,
                                         IODebugContext* dbg) {
  file_->PrepareWrite(offset + prefixLength_, len, options, dbg);
}

void EncryptedWritableFile::SetPreallocationBlockSize(size_t size) {
  // the size here doesn't need to include prefixLength_, as it's a
  // configuration will be use for `PrepareWrite()`.
  file_->SetPreallocationBlockSize(size);
}

void EncryptedWritableFile::GetPreallocationStatus(
    size_t* block_size, size_t* last_allocated_block) {
  file_->GetPreallocationStatus(block_size, last_allocated_block);
}

// Pre-allocates space for a file.
IOStatus EncryptedWritableFile::Allocate(uint64_t offset, uint64_t len,
                                         const IOOptions& options,
                                         IODebugContext* dbg) {
  return file_->Allocate(offset + prefixLength_, len, options, dbg);
}

IOStatus EncryptedWritableFile::Flush(const IOOptions& options,
                                      IODebugContext* dbg) {
  return file_->Flush(options, dbg);
}

IOStatus EncryptedWritableFile::Sync(const IOOptions& options,
                                     IODebugContext* dbg) {
  return file_->Sync(options, dbg);
}

IOStatus EncryptedWritableFile::Close(const IOOptions& options,
                                      IODebugContext* dbg) {
  return file_->Close(options, dbg);
}

// A file abstraction for random reading and writing.

// Indicates if the class makes use of direct I/O
// If false you must pass aligned buffer to Write()
bool EncryptedRandomRWFile::use_direct_io() const {
  return file_->use_direct_io();
}

// Use the returned alignment value to allocate
// aligned buffer for Direct I/O
size_t EncryptedRandomRWFile::GetRequiredBufferAlignment() const {
  return file_->GetRequiredBufferAlignment();
}

// Write bytes in `data` at  offset `offset`, Returns Status::OK() on success.
// Pass aligned buffer when use_direct_io() returns true.
IOStatus EncryptedRandomRWFile::Write(uint64_t offset, const Slice& data,
                                      const IOOptions& options,
                                      IODebugContext* dbg) {
  AlignedBuffer buf;
  Slice dataToWrite(data);
  offset += prefixLength_;
  if (data.size() > 0) {
    // Encrypt in cloned buffer
    buf.Alignment(GetRequiredBufferAlignment());
    buf.AllocateNewBuffer(data.size());
    memmove(buf.BufferStart(), data.data(), data.size());
    buf.Size(data.size());
    IOStatus io_s;
    {
      PERF_TIMER_GUARD(encrypt_data_nanos);
      io_s = status_to_io_status(
          stream_->Encrypt(offset, buf.BufferStart(), buf.CurrentSize()));
    }
    if (!io_s.ok()) {
      return io_s;
    }
    dataToWrite = Slice(buf.BufferStart(), buf.CurrentSize());
  }
  return file_->Write(offset, dataToWrite, options, dbg);
}

// Read up to `n` bytes starting from offset `offset` and store them in
// result, provided `scratch` size should be at least `n`.
// Returns Status::OK() on success.
IOStatus EncryptedRandomRWFile::Read(uint64_t offset, size_t n,
                                     const IOOptions& options, Slice* result,
                                     char* scratch, IODebugContext* dbg) const {
  assert(scratch);
  offset += prefixLength_;
  auto status = file_->Read(offset, n, options, result, scratch, dbg);
  if (!status.ok()) {
    return status;
  }
  {
    PERF_TIMER_GUARD(decrypt_data_nanos);
    status = status_to_io_status(
        stream_->Decrypt(offset, (char*)result->data(), result->size()));
  }
  return status;
}

IOStatus EncryptedRandomRWFile::Flush(const IOOptions& options,
                                      IODebugContext* dbg) {
  return file_->Flush(options, dbg);
}

IOStatus EncryptedRandomRWFile::Sync(const IOOptions& options,
                                     IODebugContext* dbg) {
  return file_->Sync(options, dbg);
}

IOStatus EncryptedRandomRWFile::Fsync(const IOOptions& options,
                                      IODebugContext* dbg) {
  return file_->Fsync(options, dbg);
}

IOStatus EncryptedRandomRWFile::Close(const IOOptions& options,
                                      IODebugContext* dbg) {
  return file_->Close(options, dbg);
}

namespace {
static std::unordered_map<std::string, OptionTypeInfo> encrypted_fs_type_info =
    {
        {"provider",
         OptionTypeInfo::AsCustomSharedPtr<EncryptionProvider>(
             0 /* No offset, whole struct*/, OptionVerificationType::kByName,
             OptionTypeFlags::kNone)},
};
// EncryptedFileSystemImpl implements an FileSystemWrapper that adds encryption
// to files stored on disk.
class EncryptedFileSystemImpl : public EncryptedFileSystem {
 public:
  const char* Name() const override {
    return EncryptedFileSystem::kClassName();
  }
  // Returns the raw encryption provider that should be used to write the input
  // encrypted file.  If there is no such provider, NotFound is returned.
  IOStatus GetWritableProvider(const std::string& /*fname*/,
                               EncryptionProvider** result) {
    if (provider_) {
      *result = provider_.get();
      return IOStatus::OK();
    } else {
      *result = nullptr;
      return IOStatus::NotFound("No WriteProvider specified");
    }
  }

  // Returns the raw encryption provider that should be used to read the input
  // encrypted file.  If there is no such provider, NotFound is returned.
  IOStatus GetReadableProvider(const std::string& /*fname*/,
                               EncryptionProvider** result) {
    if (provider_) {
      *result = provider_.get();
      return IOStatus::OK();
    } else {
      *result = nullptr;
      return IOStatus::NotFound("No Provider specified");
    }
  }

  // Creates a CipherStream for the underlying file/name using the options
  // If a writable provider is found and encryption is enabled, uses
  // this provider to create a cipher stream.
  // @param fname         Name of the writable file
  // @param underlying    The underlying "raw" file
  // @param options       Options for creating the file/cipher
  // @param prefix_length Returns the length of the encryption prefix used for
  // this file
  // @param stream        Returns the cipher stream to use for this file if it
  // should be encrypted
  // @return OK on success, non-OK on failure.
  template <class TypeFile>
  IOStatus CreateWritableCipherStream(
      const std::string& fname, const std::unique_ptr<TypeFile>& underlying,
      const FileOptions& options, size_t* prefix_length,
      std::unique_ptr<BlockAccessCipherStream>* stream, IODebugContext* dbg) {
    EncryptionProvider* provider = nullptr;
    *prefix_length = 0;
    IOStatus status = GetWritableProvider(fname, &provider);
    if (!status.ok()) {
      return status;
    } else if (provider != nullptr) {
      // Initialize & write prefix (if needed)
      AlignedBuffer buffer;
      Slice prefix;
      *prefix_length = provider->GetPrefixLength();
      if (*prefix_length > 0) {
        // Initialize prefix
        buffer.Alignment(underlying->GetRequiredBufferAlignment());
        buffer.AllocateNewBuffer(*prefix_length);
        status = status_to_io_status(provider->CreateNewPrefix(
            fname, buffer.BufferStart(), *prefix_length));
        if (status.ok()) {
          buffer.Size(*prefix_length);
          prefix = Slice(buffer.BufferStart(), buffer.CurrentSize());
          // Write prefix
          status = underlying->Append(prefix, options.io_options, dbg);
        }
        if (!status.ok()) {
          return status;
        }
      }
      // Create cipher stream
      status = status_to_io_status(
          provider->CreateCipherStream(fname, options, prefix, stream));
    }
    return status;
  }

  template <class TypeFile>
  IOStatus CreateWritableEncryptedFile(const std::string& fname,
                                       std::unique_ptr<TypeFile>& underlying,
                                       const FileOptions& options,
                                       std::unique_ptr<TypeFile>* result,
                                       IODebugContext* dbg) {
    // Create cipher stream
    std::unique_ptr<BlockAccessCipherStream> stream;
    size_t prefix_length;
    IOStatus status = CreateWritableCipherStream(fname, underlying, options,
                                                 &prefix_length, &stream, dbg);
    if (status.ok()) {
      if (stream) {
        result->reset(new EncryptedWritableFile(
            std::move(underlying), std::move(stream), prefix_length));
      } else {
        result->reset(underlying.release());
      }
    }
    return status;
  }

  // Creates a CipherStream for the underlying file/name using the options
  // If a writable provider is found and encryption is enabled, uses
  // this provider to create a cipher stream.
  // @param fname         Name of the writable file
  // @param underlying    The underlying "raw" file
  // @param options       Options for creating the file/cipher
  // @param prefix_length Returns the length of the encryption prefix used for
  // this file
  // @param stream        Returns the cipher stream to use for this file if it
  // should be encrypted
  // @return OK on success, non-OK on failure.
  template <class TypeFile>
  IOStatus CreateRandomWriteCipherStream(
      const std::string& fname, const std::unique_ptr<TypeFile>& underlying,
      const FileOptions& options, size_t* prefix_length,
      std::unique_ptr<BlockAccessCipherStream>* stream, IODebugContext* dbg) {
    EncryptionProvider* provider = nullptr;
    *prefix_length = 0;
    IOStatus io_s = GetWritableProvider(fname, &provider);
    if (!io_s.ok()) {
      return io_s;
    } else if (provider != nullptr) {
      // Initialize & write prefix (if needed)
      AlignedBuffer buffer;
      Slice prefix;
      *prefix_length = provider->GetPrefixLength();
      if (*prefix_length > 0) {
        // Initialize prefix
        buffer.Alignment(underlying->GetRequiredBufferAlignment());
        buffer.AllocateNewBuffer(*prefix_length);
        io_s = status_to_io_status(provider->CreateNewPrefix(
            fname, buffer.BufferStart(), *prefix_length));
        if (io_s.ok()) {
          buffer.Size(*prefix_length);
          prefix = Slice(buffer.BufferStart(), buffer.CurrentSize());
          // Write prefix
          io_s = underlying->Write(0, prefix, options.io_options, dbg);
        }
        if (!io_s.ok()) {
          return io_s;
        }
      }
      // Create cipher stream
      io_s = status_to_io_status(
          provider->CreateCipherStream(fname, options, prefix, stream));
    }
    return io_s;
  }

  // Creates a CipherStream for the underlying file/name using the options
  // If a readable provider is found and the file is encrypted, uses
  // this provider to create a cipher stream.
  // @param fname         Name of the writable file
  // @param underlying    The underlying "raw" file
  // @param options       Options for creating the file/cipher
  // @param prefix_length Returns the length of the encryption prefix used for
  // this file
  // @param stream        Returns the cipher stream to use for this file if it
  // is encrypted
  // @return OK on success, non-OK on failure.
  template <class TypeFile>
  IOStatus CreateSequentialCipherStream(
      const std::string& fname, const std::unique_ptr<TypeFile>& underlying,
      const FileOptions& options, size_t* prefix_length,
      std::unique_ptr<BlockAccessCipherStream>* stream, IODebugContext* dbg) {
    // Read prefix (if needed)
    AlignedBuffer buffer;
    Slice prefix;
    *prefix_length = provider_->GetPrefixLength();
    if (*prefix_length > 0) {
      // Read prefix
      buffer.Alignment(underlying->GetRequiredBufferAlignment());
      buffer.AllocateNewBuffer(*prefix_length);
      IOStatus status = underlying->Read(*prefix_length, options.io_options,
                                         &prefix, buffer.BufferStart(), dbg);
      if (!status.ok()) {
        return status;
      }
      buffer.Size(*prefix_length);
    }
    return status_to_io_status(
        provider_->CreateCipherStream(fname, options, prefix, stream));
  }

  // Creates a CipherStream for the underlying file/name using the options
  // If a readable provider is found and the file is encrypted, uses
  // this provider to create a cipher stream.
  // @param fname         Name of the writable file
  // @param underlying    The underlying "raw" file
  // @param options       Options for creating the file/cipher
  // @param prefix_length Returns the length of the encryption prefix used for
  // this file
  // @param stream        Returns the cipher stream to use for this file if it
  // is encrypted
  // @return OK on success, non-OK on failure.
  template <class TypeFile>
  IOStatus CreateRandomReadCipherStream(
      const std::string& fname, const std::unique_ptr<TypeFile>& underlying,
      const FileOptions& options, size_t* prefix_length,
      std::unique_ptr<BlockAccessCipherStream>* stream, IODebugContext* dbg) {
    // Read prefix (if needed)
    AlignedBuffer buffer;
    Slice prefix;
    *prefix_length = provider_->GetPrefixLength();
    if (*prefix_length > 0) {
      // Read prefix
      buffer.Alignment(underlying->GetRequiredBufferAlignment());
      buffer.AllocateNewBuffer(*prefix_length);
      IOStatus status = underlying->Read(0, *prefix_length, options.io_options,
                                         &prefix, buffer.BufferStart(), dbg);
      if (!status.ok()) {
        return status;
      }
      buffer.Size(*prefix_length);
    }
    return status_to_io_status(
        provider_->CreateCipherStream(fname, options, prefix, stream));
  }

 public:
  EncryptedFileSystemImpl(const std::shared_ptr<FileSystem>& base,
                          const std::shared_ptr<EncryptionProvider>& provider)
      : EncryptedFileSystem(base) {
    provider_ = provider;
    RegisterOptions("EncryptionProvider", &provider_, &encrypted_fs_type_info);
  }

  Status AddCipher(const std::string& descriptor, const char* cipher,
                   size_t len, bool for_write) override {
    return provider_->AddCipher(descriptor, cipher, len, for_write);
  }

  // NewSequentialFile opens a file for sequential reading.
  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override {
    result->reset();
    if (options.use_mmap_reads) {
      return IOStatus::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<FSSequentialFile> underlying;
    auto status =
        FileSystemWrapper::NewSequentialFile(fname, options, &underlying, dbg);
    if (!status.ok()) {
      return status;
    }
    uint64_t file_size;
    status = FileSystemWrapper::GetFileSize(fname, options.io_options,
                                            &file_size, dbg);
    if (!status.ok()) {
      return status;
    }
    if (!file_size) {
      *result = std::move(underlying);
      return status;
    }
    // Create cipher stream
    std::unique_ptr<BlockAccessCipherStream> stream;
    size_t prefix_length;
    status = CreateSequentialCipherStream(fname, underlying, options,
                                          &prefix_length, &stream, dbg);
    if (status.ok()) {
      result->reset(new EncryptedSequentialFile(
          std::move(underlying), std::move(stream), prefix_length));
    }
    return status;
  }

  // NewRandomAccessFile opens a file for random read access.
  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    result->reset();
    if (options.use_mmap_reads) {
      return IOStatus::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<FSRandomAccessFile> underlying;
    auto status = FileSystemWrapper::NewRandomAccessFile(fname, options,
                                                         &underlying, dbg);
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<BlockAccessCipherStream> stream;
    size_t prefix_length;
    status = CreateRandomReadCipherStream(fname, underlying, options,
                                          &prefix_length, &stream, dbg);
    if (status.ok()) {
      if (stream) {
        result->reset(new EncryptedRandomAccessFile(
            std::move(underlying), std::move(stream), prefix_length));
      } else {
        result->reset(underlying.release());
      }
    }
    return status;
  }

  // NewWritableFile opens a file for sequential writing.
  IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    result->reset();
    if (options.use_mmap_writes) {
      return IOStatus::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<FSWritableFile> underlying;
    IOStatus status =
        FileSystemWrapper::NewWritableFile(fname, options, &underlying, dbg);
    if (!status.ok()) {
      return status;
    }
    return CreateWritableEncryptedFile(fname, underlying, options, result, dbg);
  }

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override {
    result->reset();
    if (options.use_mmap_writes) {
      return IOStatus::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<FSWritableFile> underlying;
    IOStatus status =
        FileSystemWrapper::ReopenWritableFile(fname, options, &underlying, dbg);
    if (!status.ok()) {
      return status;
    }
    return CreateWritableEncryptedFile(fname, underlying, options, result, dbg);
  }

  // Reuse an existing file by renaming it and opening it as writable.
  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override {
    result->reset();
    if (options.use_mmap_writes) {
      return IOStatus::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<FSWritableFile> underlying;
    auto status = FileSystemWrapper::ReuseWritableFile(
        fname, old_fname, options, &underlying, dbg);
    if (!status.ok()) {
      return status;
    }
    return CreateWritableEncryptedFile(fname, underlying, options, result, dbg);
  }

  // Open `fname` for random read and write, if file doesn't exist the file
  // will be created.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  IOStatus NewRandomRWFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override {
    result->reset();
    if (options.use_mmap_reads || options.use_mmap_writes) {
      return IOStatus::InvalidArgument();
    }
    // Check file exists
    bool isNewFile = !FileExists(fname, options.io_options, dbg).ok();

    // Open file using underlying Env implementation
    std::unique_ptr<FSRandomRWFile> underlying;
    auto status =
        FileSystemWrapper::NewRandomRWFile(fname, options, &underlying, dbg);
    if (!status.ok()) {
      return status;
    }
    // Create cipher stream
    std::unique_ptr<BlockAccessCipherStream> stream;
    size_t prefix_length = 0;
    if (!isNewFile) {
      // File already exists, read prefix
      status = CreateRandomReadCipherStream(fname, underlying, options,
                                            &prefix_length, &stream, dbg);
    } else {
      status = CreateRandomWriteCipherStream(fname, underlying, options,
                                             &prefix_length, &stream, dbg);
    }
    if (status.ok()) {
      if (stream) {
        result->reset(new EncryptedRandomRWFile(
            std::move(underlying), std::move(stream), prefix_length));
      } else {
        result->reset(underlying.release());
      }
    }
    return status;
  }

  // Store in *result the attributes of the children of the specified
  // directory.
  // In case the implementation lists the directory prior to iterating the
  // files
  // and files are concurrently deleted, the deleted files will be omitted
  // from
  // result.
  // The name attributes are relative to "dir".
  // Original contents of *results are dropped.
  // Returns OK if "dir" exists and "*result" contains its children.
  //         NotFound if "dir" does not exist, the calling process does not
  //         have
  //                  permission to access "dir", or if "dir" is invalid.
  //         IOError if an IO Error was encountered
  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& options,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* dbg) override {
    auto status =
        FileSystemWrapper::GetChildrenFileAttributes(dir, options, result, dbg);
    if (!status.ok()) {
      return status;
    }
    for (auto it = std::begin(*result); it != std::end(*result); ++it) {
      // assert(it->size_bytes >= prefixLength);
      //  breaks env_basic_test when called on directory containing
      //  directories
      // which makes subtraction of prefixLength worrisome since
      // FileAttributes does not identify directories
      EncryptionProvider* provider;
      status = GetReadableProvider(it->name, &provider);
      if (!status.ok()) {
        return status;
      } else if (provider != nullptr) {
        it->size_bytes -= provider->GetPrefixLength();
      }
    }
    return IOStatus::OK();
  }

  // Store the size of fname in *file_size.
  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override {
    auto status =
        FileSystemWrapper::GetFileSize(fname, options, file_size, dbg);
    if (!status.ok() || !(*file_size)) {
      return status;
    }
    EncryptionProvider* provider;
    status = GetReadableProvider(fname, &provider);
    if (provider != nullptr && status.ok()) {
      size_t prefixLength = provider->GetPrefixLength();
      assert(*file_size >= prefixLength);
      *file_size -= prefixLength;
    }
    return status;
  }

 private:
  std::shared_ptr<EncryptionProvider> provider_;
};
}  // namespace

Status NewEncryptedFileSystemImpl(
    const std::shared_ptr<FileSystem>& base,
    const std::shared_ptr<EncryptionProvider>& provider,
    std::unique_ptr<FileSystem>* result) {
  result->reset(new EncryptedFileSystemImpl(base, provider));
  return Status::OK();
}

std::shared_ptr<FileSystem> NewEncryptedFS(
    const std::shared_ptr<FileSystem>& base,
    const std::shared_ptr<EncryptionProvider>& provider) {
  std::unique_ptr<FileSystem> efs;
  Status s = NewEncryptedFileSystemImpl(base, provider, &efs);
  if (s.ok()) {
    s = efs->PrepareOptions(ConfigOptions());
  }
  if (s.ok()) {
    std::shared_ptr<FileSystem> result(efs.release());
    return result;
  } else {
    return nullptr;
  }
}
// Returns an Env that encrypts data when stored on disk and decrypts data when
// read from disk.
Env* NewEncryptedEnv(Env* base_env,
                     const std::shared_ptr<EncryptionProvider>& provider) {
  return new CompositeEnvWrapper(
      base_env, NewEncryptedFS(base_env->GetFileSystem(), provider));
}

// Encrypt one or more (partial) blocks of data at the file offset.
// Length of data is given in dataSize.
Status BlockAccessCipherStream::Encrypt(uint64_t fileOffset, char* data,
                                        size_t dataSize) {
  // Calculate block index
  auto blockSize = BlockSize();
  uint64_t blockIndex = fileOffset / blockSize;
  size_t blockOffset = fileOffset % blockSize;
  std::unique_ptr<char[]> blockBuffer;

  std::string scratch;
  AllocateScratch(scratch);

  // Encrypt individual blocks.
  while (1) {
    char* block = data;
    size_t n = std::min(dataSize, blockSize - blockOffset);
    if (n != blockSize) {
      // We're not encrypting a full block.
      // Copy data to blockBuffer
      if (!blockBuffer.get()) {
        // Allocate buffer
        blockBuffer = std::unique_ptr<char[]>(new char[blockSize]);
      }
      block = blockBuffer.get();
      // Copy plain data to block buffer
      memmove(block + blockOffset, data, n);
    }
    auto status = EncryptBlock(blockIndex, block, (char*)scratch.data());
    if (!status.ok()) {
      return status;
    }
    if (block != data) {
      // Copy encrypted data back to `data`.
      memmove(data, block + blockOffset, n);
    }
    dataSize -= n;
    if (dataSize == 0) {
      return Status::OK();
    }
    data += n;
    blockOffset = 0;
    blockIndex++;
  }
}

// Decrypt one or more (partial) blocks of data at the file offset.
// Length of data is given in dataSize.
Status BlockAccessCipherStream::Decrypt(uint64_t fileOffset, char* data,
                                        size_t dataSize) {
  // Calculate block index
  auto blockSize = BlockSize();
  uint64_t blockIndex = fileOffset / blockSize;
  size_t blockOffset = fileOffset % blockSize;
  std::unique_ptr<char[]> blockBuffer;

  std::string scratch;
  AllocateScratch(scratch);

  // Decrypt individual blocks.
  while (1) {
    char* block = data;
    size_t n = std::min(dataSize, blockSize - blockOffset);
    if (n != blockSize) {
      // We're not decrypting a full block.
      // Copy data to blockBuffer
      if (!blockBuffer.get()) {
        // Allocate buffer
        blockBuffer = std::unique_ptr<char[]>(new char[blockSize]);
      }
      block = blockBuffer.get();
      // Copy encrypted data to block buffer
      memmove(block + blockOffset, data, n);
    }
    auto status = DecryptBlock(blockIndex, block, (char*)scratch.data());
    if (!status.ok()) {
      return status;
    }
    if (block != data) {
      // Copy decrypted data back to `data`.
      memmove(data, block + blockOffset, n);
    }

    // Simply decrementing dataSize by n could cause it to underflow,
    // which will very likely make it read over the original bounds later
    assert(dataSize >= n);
    if (dataSize < n) {
      return Status::Corruption("Cannot decrypt data at given offset");
    }

    dataSize -= n;
    if (dataSize == 0) {
      return Status::OK();
    }
    data += n;
    blockOffset = 0;
    blockIndex++;
  }
}

namespace {
static std::unordered_map<std::string, OptionTypeInfo>
    rot13_block_cipher_type_info = {
        {"block_size",
         {0 /* No offset, whole struct*/, OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
};
// Implements a BlockCipher using ROT13.
//
// Note: This is a sample implementation of BlockCipher,
// it is NOT considered safe and should NOT be used in production.
class ROT13BlockCipher : public BlockCipher {
 private:
  size_t blockSize_;

 public:
  explicit ROT13BlockCipher(size_t blockSize) : blockSize_(blockSize) {
    RegisterOptions("ROT13BlockCipherOptions", &blockSize_,
                    &rot13_block_cipher_type_info);
  }

  static const char* kClassName() { return "ROT13"; }
  const char* Name() const override { return kClassName(); }
  // BlockSize returns the size of each block supported by this cipher stream.
  size_t BlockSize() override { return blockSize_; }

  // Encrypt a block of data.
  // Length of data is equal to BlockSize().
  Status Encrypt(char* data) override {
    for (size_t i = 0; i < blockSize_; ++i) {
      data[i] += 13;
    }
    return Status::OK();
  }

  // Decrypt a block of data.
  // Length of data is equal to BlockSize().
  Status Decrypt(char* data) override { return Encrypt(data); }
};
static const std::unordered_map<std::string, OptionTypeInfo>
    ctr_encryption_provider_type_info = {
        {"cipher",
         OptionTypeInfo::AsCustomSharedPtr<BlockCipher>(
             0 /* No offset, whole struct*/, OptionVerificationType::kByName,
             OptionTypeFlags::kNone)},
};
}  // anonymous namespace

// Allocate scratch space which is passed to EncryptBlock/DecryptBlock.
void CTRCipherStream::AllocateScratch(std::string& scratch) {
  auto blockSize = cipher_->BlockSize();
  scratch.reserve(blockSize);
}

// Encrypt a block of data at the given block index.
// Length of data is equal to BlockSize();
Status CTRCipherStream::EncryptBlock(uint64_t blockIndex, char* data,
                                     char* scratch) {
  // Create nonce + counter
  auto blockSize = cipher_->BlockSize();
  memmove(scratch, iv_.data(), blockSize);
  EncodeFixed64(scratch, blockIndex + initialCounter_);

  // Encrypt nonce+counter
  auto status = cipher_->Encrypt(scratch);
  if (!status.ok()) {
    return status;
  }

  // XOR data with ciphertext.
  for (size_t i = 0; i < blockSize; i++) {
    data[i] = data[i] ^ scratch[i];
  }
  return Status::OK();
}

// Decrypt a block of data at the given block index.
// Length of data is equal to BlockSize();
Status CTRCipherStream::DecryptBlock(uint64_t blockIndex, char* data,
                                     char* scratch) {
  // For CTR decryption & encryption are the same
  return EncryptBlock(blockIndex, data, scratch);
}

CTREncryptionProvider::CTREncryptionProvider(
    const std::shared_ptr<BlockCipher>& c)
    : cipher_(c) {
  RegisterOptions("Cipher", &cipher_, &ctr_encryption_provider_type_info);
}

bool CTREncryptionProvider::IsInstanceOf(const std::string& name) const {
  // Special case for test purposes.
  if (name == "1://test" && cipher_ != nullptr) {
    return cipher_->IsInstanceOf(ROT13BlockCipher::kClassName());
  } else {
    return EncryptionProvider::IsInstanceOf(name);
  }
}

// GetPrefixLength returns the length of the prefix that is added to every file
// and used for storing encryption options.
// For optimal performance, the prefix length should be a multiple of
// the page size.
size_t CTREncryptionProvider::GetPrefixLength() const {
  return defaultPrefixLength;
}

Status CTREncryptionProvider::AddCipher(const std::string& /*descriptor*/,
                                        const char* cipher, size_t len,
                                        bool /*for_write*/) {
  if (cipher_) {
    return Status::NotSupported("Cannot add keys to CTREncryptionProvider");
  } else if (strcmp(ROT13BlockCipher::kClassName(), cipher) == 0) {
    cipher_.reset(new ROT13BlockCipher(len));
    return Status::OK();
  } else {
    return BlockCipher::CreateFromString(ConfigOptions(), std::string(cipher),
                                         &cipher_);
  }
}

// decodeCTRParameters decodes the initial counter & IV from the given
// (plain text) prefix.
static void decodeCTRParameters(const char* prefix, size_t blockSize,
                                uint64_t& initialCounter, Slice& iv) {
  // First block contains 64-bit initial counter
  initialCounter = DecodeFixed64(prefix);
  // Second block contains IV
  iv = Slice(prefix + blockSize, blockSize);
}

// CreateNewPrefix initialized an allocated block of prefix memory
// for a new file.
Status CTREncryptionProvider::CreateNewPrefix(const std::string& /*fname*/,
                                              char* prefix,
                                              size_t prefixLength) const {
  if (!cipher_) {
    return Status::InvalidArgument("Encryption Cipher is missing");
  }
  // Create & seed rnd.
  Random rnd((uint32_t)SystemClock::Default()->NowMicros());
  // Fill entire prefix block with random values.
  for (size_t i = 0; i < prefixLength; i++) {
    prefix[i] = rnd.Uniform(256) & 0xFF;
  }
  // Take random data to extract initial counter & IV
  auto blockSize = cipher_->BlockSize();
  uint64_t initialCounter;
  Slice prefixIV;
  decodeCTRParameters(prefix, blockSize, initialCounter, prefixIV);

  // Now populate the rest of the prefix, starting from the third block.
  PopulateSecretPrefixPart(prefix + (2 * blockSize),
                           prefixLength - (2 * blockSize), blockSize);

  // Encrypt the prefix, starting from block 2 (leave block 0, 1 with initial
  // counter & IV unencrypted)
  CTRCipherStream cipherStream(cipher_, prefixIV.data(), initialCounter);
  Status status;
  {
    PERF_TIMER_GUARD(encrypt_data_nanos);
    status = cipherStream.Encrypt(0, prefix + (2 * blockSize),
                                  prefixLength - (2 * blockSize));
  }
  if (!status.ok()) {
    return status;
  }
  return Status::OK();
}

// PopulateSecretPrefixPart initializes the data into a new prefix block
// in plain text.
// Returns the amount of space (starting from the start of the prefix)
// that has been initialized.
size_t CTREncryptionProvider::PopulateSecretPrefixPart(
    char* /*prefix*/, size_t /*prefixLength*/, size_t /*blockSize*/) const {
  // Nothing to do here, put in custom data in override when needed.
  return 0;
}

Status CTREncryptionProvider::CreateCipherStream(
    const std::string& fname, const EnvOptions& options, Slice& prefix,
    std::unique_ptr<BlockAccessCipherStream>* result) {
  if (!cipher_) {
    return Status::InvalidArgument("Encryption Cipher is missing");
  }
  // Read plain text part of prefix.
  auto blockSize = cipher_->BlockSize();
  uint64_t initialCounter;
  Slice iv;
  decodeCTRParameters(prefix.data(), blockSize, initialCounter, iv);

  // If the prefix is smaller than twice the block size, we would below read a
  // very large chunk of the file (and very likely read over the bounds)
  assert(prefix.size() >= 2 * blockSize);
  if (prefix.size() < 2 * blockSize) {
    return Status::Corruption("Unable to read from file " + fname +
                              ": read attempt would read beyond file bounds");
  }

  // Decrypt the encrypted part of the prefix, starting from block 2 (block 0, 1
  // with initial counter & IV are unencrypted)
  CTRCipherStream cipherStream(cipher_, iv.data(), initialCounter);
  Status status;
  {
    PERF_TIMER_GUARD(decrypt_data_nanos);
    status = cipherStream.Decrypt(0, (char*)prefix.data() + (2 * blockSize),
                                  prefix.size() - (2 * blockSize));
  }
  if (!status.ok()) {
    return status;
  }

  // Create cipher stream
  return CreateCipherStreamFromPrefix(fname, options, initialCounter, iv,
                                      prefix, result);
}

// CreateCipherStreamFromPrefix creates a block access cipher stream for a file
// given given name and options. The given prefix is already decrypted.
Status CTREncryptionProvider::CreateCipherStreamFromPrefix(
    const std::string& /*fname*/, const EnvOptions& /*options*/,
    uint64_t initialCounter, const Slice& iv, const Slice& /*prefix*/,
    std::unique_ptr<BlockAccessCipherStream>* result) {
  (*result) = std::unique_ptr<BlockAccessCipherStream>(
      new CTRCipherStream(cipher_, iv.data(), initialCounter));
  return Status::OK();
}

namespace {
static void RegisterEncryptionBuiltins() {
  static std::once_flag once;
  std::call_once(once, [&]() {
    auto lib = ObjectRegistry::Default()->AddLibrary("encryption");
    // Match "CTR" or "CTR://test"
    lib->AddFactory<EncryptionProvider>(
        ObjectLibrary::PatternEntry(CTREncryptionProvider::kClassName(), true)
            .AddSuffix("://test"),
        [](const std::string& uri, std::unique_ptr<EncryptionProvider>* guard,
           std::string* /*errmsg*/) {
          if (EndsWith(uri, "://test")) {
            std::shared_ptr<BlockCipher> cipher =
                std::make_shared<ROT13BlockCipher>(32);
            guard->reset(new CTREncryptionProvider(cipher));
          } else {
            guard->reset(new CTREncryptionProvider());
          }
          return guard->get();
        });

    lib->AddFactory<EncryptionProvider>(
        "1://test", [](const std::string& /*uri*/,
                       std::unique_ptr<EncryptionProvider>* guard,
                       std::string* /*errmsg*/) {
          std::shared_ptr<BlockCipher> cipher =
              std::make_shared<ROT13BlockCipher>(32);
          guard->reset(new CTREncryptionProvider(cipher));
          return guard->get();
        });

    // Match "ROT13" or "ROT13:[0-9]+"
    lib->AddFactory<BlockCipher>(
        ObjectLibrary::PatternEntry(ROT13BlockCipher::kClassName(), true)
            .AddNumber(":"),
        [](const std::string& uri, std::unique_ptr<BlockCipher>* guard,
           std::string* /* errmsg */) {
          size_t colon = uri.find(':');
          if (colon != std::string::npos) {
            size_t block_size = ParseSizeT(uri.substr(colon + 1));
            guard->reset(new ROT13BlockCipher(block_size));
          } else {
            guard->reset(new ROT13BlockCipher(32));
          }

          return guard->get();
        });
  });
}
}  // namespace

Status BlockCipher::CreateFromString(const ConfigOptions& config_options,
                                     const std::string& value,
                                     std::shared_ptr<BlockCipher>* result) {
  RegisterEncryptionBuiltins();
  return LoadSharedObject<BlockCipher>(config_options, value, nullptr, result);
}

Status EncryptionProvider::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<EncryptionProvider>* result) {
  RegisterEncryptionBuiltins();
  return LoadSharedObject<EncryptionProvider>(config_options, value, nullptr,
                                              result);
}

#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE
