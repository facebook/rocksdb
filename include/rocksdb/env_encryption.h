//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE)

#include <string>

#include "rocksdb/customizable.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class EncryptionProvider;

struct ConfigOptions;

// Returns an Env that encrypts data when stored on disk and decrypts data when
// read from disk.
Env* NewEncryptedEnv(Env* base_env,
                     const std::shared_ptr<EncryptionProvider>& provider);
std::shared_ptr<FileSystem> NewEncryptedFS(
    const std::shared_ptr<FileSystem>& base_fs,
    const std::shared_ptr<EncryptionProvider>& provider);

// BlockAccessCipherStream is the base class for any cipher stream that
// supports random access at block level (without requiring data from other
// blocks). E.g. CTR (Counter operation mode) supports this requirement.
class BlockAccessCipherStream {
 public:
  virtual ~BlockAccessCipherStream(){};

  // BlockSize returns the size of each block supported by this cipher stream.
  virtual size_t BlockSize() = 0;

  // Encrypt one or more (partial) blocks of data at the file offset.
  // Length of data is given in dataSize.
  virtual Status Encrypt(uint64_t fileOffset, char* data, size_t dataSize);

  // Decrypt one or more (partial) blocks of data at the file offset.
  // Length of data is given in dataSize.
  virtual Status Decrypt(uint64_t fileOffset, char* data, size_t dataSize);

 protected:
  // Allocate scratch space which is passed to EncryptBlock/DecryptBlock.
  virtual void AllocateScratch(std::string&) = 0;

  // Encrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual Status EncryptBlock(uint64_t blockIndex, char* data,
                              char* scratch) = 0;

  // Decrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual Status DecryptBlock(uint64_t blockIndex, char* data,
                              char* scratch) = 0;
};

// BlockCipher
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class BlockCipher : public Customizable {
 public:
  virtual ~BlockCipher(){};

  // Creates a new BlockCipher from the input config_options and value
  // The value describes the type of provider (and potentially optional
  // configuration parameters) used to create this provider.
  // For example, if the value is "ROT13", a ROT13BlockCipher is created.
  //
  // @param config_options  Options to control how this cipher is created
  //                        and initialized.
  // @param value  The value might be:
  //   - ROT13         Create a ROT13 Cipher
  //   - ROT13:nn      Create a ROT13 Cipher with block size of nn
  // @param result The new cipher object
  // @return OK if the cipher was successfully created
  // @return NotFound if an invalid name was specified in the value
  // @return InvalidArgument if either the options were not valid
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value,
                                 std::shared_ptr<BlockCipher>* result);

  static const char* Type() { return "BlockCipher"; }
  // Short-cut method to create a ROT13 BlockCipher.
  // This cipher is only suitable for test purposes and should not be used in
  // production!!!
  static std::shared_ptr<BlockCipher> NewROT13Cipher(size_t block_size);

  // BlockSize returns the size of each block supported by this cipher stream.
  virtual size_t BlockSize() = 0;

  // Encrypt a block of data.
  // Length of data is equal to BlockSize().
  virtual Status Encrypt(char* data) = 0;

  // Decrypt a block of data.
  // Length of data is equal to BlockSize().
  virtual Status Decrypt(char* data) = 0;
};

// The encryption provider is used to create a cipher stream for a specific
// file. The returned cipher stream will be used for actual
// encryption/decryption actions.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class EncryptionProvider : public Customizable {
 public:
  virtual ~EncryptionProvider(){};

  // Creates a new EncryptionProvider from the input config_options and value
  // The value describes the type of provider (and potentially optional
  // configuration parameters) used to create this provider.
  // For example, if the value is "CTR", a CTREncryptionProvider will be
  // created. If the value is ends with "://test" (e.g CTR://test"), the
  // provider will be initialized in "TEST" mode prior to being returned.
  //
  // @param config_options  Options to control how this provider is created
  //                        and initialized.
  // @param value  The value might be:
  //   - CTR         Create a CTR provider
  //   - CTR://test Create a CTR provider and initialize it for tests.
  // @param result The new provider object
  // @return OK if the provider was successfully created
  // @return NotFound if an invalid name was specified in the value
  // @return InvalidArgument if either the options were not valid
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value,
                                 std::shared_ptr<EncryptionProvider>* result);

  static const char* Type() { return "EncryptionProvider"; }

  // Short-cut method to create a CTR-provider
  static std::shared_ptr<EncryptionProvider> NewCTRProvider(
      const std::shared_ptr<BlockCipher>& cipher);

  // GetPrefixLength returns the length of the prefix that is added to every
  // file and used for storing encryption options. For optimal performance, the
  // prefix length should be a multiple of the page size.
  virtual size_t GetPrefixLength() const = 0;

  // CreateNewPrefix initialized an allocated block of prefix memory
  // for a new file.
  virtual Status CreateNewPrefix(const std::string& fname, char* prefix,
                                 size_t prefixLength) const = 0;

  // Method to add a new cipher key for use by the EncryptionProvider.
  // @param description  Descriptor for this key.
  // @param cipher       The cryptographic key to use
  // @param len          The length of the cipher key
  // @param for_write If true, this cipher should be used for writing files.
  //                  If false, this cipher should only be used for reading
  //                  files
  // @return OK if the cipher was successfully added to the provider, non-OK
  // otherwise
  virtual Status AddCipher(const std::string& descriptor, const char* cipher,
                           size_t len, bool for_write) = 0;

  // CreateCipherStream creates a block access cipher stream for a file given
  // given name and options.
  virtual Status CreateCipherStream(
      const std::string& fname, const EnvOptions& options, Slice& prefix,
      std::unique_ptr<BlockAccessCipherStream>* result) = 0;

  // Returns a string representing an encryption marker prefix for this
  // provider. If a marker is provided, this marker can be used to tell whether
  // or not a file is encrypted by this provider.  The maker will also be part
  // of any encryption prefix for this provider.
  virtual std::string GetMarker() const { return ""; }
};

class EncryptedSequentialFile : public FSSequentialFile {
 protected:
  std::unique_ptr<FSSequentialFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  uint64_t offset_;
  size_t prefixLength_;

 public:
  // Default ctor. Given underlying sequential file is supposed to be at
  // offset == prefixLength.
  EncryptedSequentialFile(std::unique_ptr<FSSequentialFile>&& f,
                          std::unique_ptr<BlockAccessCipherStream>&& s,
                          size_t prefixLength)
      : file_(std::move(f)),
        stream_(std::move(s)),
        offset_(prefixLength),
        prefixLength_(prefixLength) {}

  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // REQUIRES: External synchronization
  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  IOStatus Skip(uint64_t n) override;

  // Indicates the upper layers if the current SequentialFile implementation
  // uses direct IO.
  bool use_direct_io() const override;

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  size_t GetRequiredBufferAlignment() const override;

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  IOStatus InvalidateCache(size_t offset, size_t length) override;

  // Positioned Read for direct I/O
  // If Direct I/O enabled, offset, n, and scratch should be properly aligned
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;
};

// A file abstraction for randomly reading the contents of a file.
class EncryptedRandomAccessFile : public FSRandomAccessFile {
 protected:
  std::unique_ptr<FSRandomAccessFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  size_t prefixLength_;

 public:
  EncryptedRandomAccessFile(std::unique_ptr<FSRandomAccessFile>&& f,
                            std::unique_ptr<BlockAccessCipherStream>&& s,
                            size_t prefixLength)
      : file_(std::move(f)),
        stream_(std::move(s)),
        prefixLength_(prefixLength) {}

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
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  // Readahead the file starting from offset by n bytes for caching.
  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& options,
                    IODebugContext* dbg) override;

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
  size_t GetUniqueId(char* id, size_t max_size) const override;

  void Hint(AccessPattern pattern) override;

  // Indicates the upper layers if the current RandomAccessFile implementation
  // uses direct IO.
  bool use_direct_io() const override;

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  size_t GetRequiredBufferAlignment() const override;

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  IOStatus InvalidateCache(size_t offset, size_t length) override;
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class EncryptedWritableFile : public FSWritableFile {
 protected:
  std::unique_ptr<FSWritableFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  size_t prefixLength_;

 public:
  // Default ctor. Prefix is assumed to be written already.
  EncryptedWritableFile(std::unique_ptr<FSWritableFile>&& f,
                        std::unique_ptr<BlockAccessCipherStream>&& s,
                        size_t prefixLength)
      : file_(std::move(f)),
        stream_(std::move(s)),
        prefixLength_(prefixLength) {}

  using FSWritableFile::Append;
  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override;

  using FSWritableFile::PositionedAppend;
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override;

  // true if Sync() and Fsync() are safe to call concurrently with Append()
  // and Flush().
  bool IsSyncThreadSafe() const override;

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  bool use_direct_io() const override;

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  size_t GetRequiredBufferAlignment() const override;

  /*
   * Get the size of valid data in the file.
   */
  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override;

  // Truncate is necessary to trim the file to the correct size
  // before closing. It is not always possible to keep track of the file
  // size due to whole pages writes. The behavior is undefined if called
  // with other writes to follow.
  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override;

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  // This call has no effect on dirty pages in the cache.
  IOStatus InvalidateCache(size_t offset, size_t length) override;

  // Sync a file range with disk.
  // offset is the starting byte of the file range to be synchronized.
  // nbytes specifies the length of the range to be synchronized.
  // This asks the OS to initiate flushing the cached data to disk,
  // without waiting for completion.
  // Default implementation does nothing.
  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& options,
                     IODebugContext* dbg) override;

  // PrepareWrite performs any necessary preparation for a write
  // before the write actually occurs.  This allows for pre-allocation
  // of space on devices where it can result in less file
  // fragmentation and/or less waste from over-zealous filesystem
  // pre-allocation.
  void PrepareWrite(size_t offset, size_t len, const IOOptions& options,
                    IODebugContext* dbg) override;

  void SetPreallocationBlockSize(size_t size) override;

  void GetPreallocationStatus(size_t* block_size,
                              size_t* last_allocated_block) override;

  // Pre-allocates space for a file.
  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& options,
                    IODebugContext* dbg) override;

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;
};

// A file abstraction for random reading and writing.
class EncryptedRandomRWFile : public FSRandomRWFile {
 protected:
  std::unique_ptr<FSRandomRWFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  size_t prefixLength_;

 public:
  EncryptedRandomRWFile(std::unique_ptr<FSRandomRWFile>&& f,
                        std::unique_ptr<BlockAccessCipherStream>&& s,
                        size_t prefixLength)
      : file_(std::move(f)),
        stream_(std::move(s)),
        prefixLength_(prefixLength) {}

  // Indicates if the class makes use of direct I/O
  // If false you must pass aligned buffer to Write()
  bool use_direct_io() const override;

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  size_t GetRequiredBufferAlignment() const override;

  // Write bytes in `data` at  offset `offset`, Returns Status::OK() on success.
  // Pass aligned buffer when use_direct_io() returns true.
  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& options,
                 IODebugContext* dbg) override;

  // Read up to `n` bytes starting from offset `offset` and store them in
  // result, provided `scratch` size should be at least `n`.
  // Returns Status::OK() on success.
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;
};

class EncryptedFileSystem : public FileSystemWrapper {
 public:
  explicit EncryptedFileSystem(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}
  // Method to add a new cipher key for use by the EncryptionProvider.
  // @param description  Descriptor for this key.
  // @param cipher       The cryptographic key to use
  // @param len          The length of the cipher key
  // @param for_write If true, this cipher should be used for writing files.
  //                  If false, this cipher should only be used for reading
  //                  files
  // @return OK if the cipher was successfully added to the provider, non-OK
  // otherwise
  virtual Status AddCipher(const std::string& descriptor, const char* cipher,
                           size_t len, bool for_write) = 0;
  static const char* kClassName() { return "EncryptedFileSystem"; }
  bool IsInstanceOf(const std::string& name) const override {
    if (name == kClassName()) {
      return true;
    } else {
      return FileSystemWrapper::IsInstanceOf(name);
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE)
