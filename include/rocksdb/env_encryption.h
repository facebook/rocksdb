//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE)

#include <string>

#include "env.h"

namespace ROCKSDB_NAMESPACE {

class EncryptionProvider;

// Returns an Env that encrypts data when stored on disk and decrypts data when
// read from disk.
Env* NewEncryptedEnv(Env* base_env, EncryptionProvider* provider);

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
class BlockCipher {
 public:
  virtual ~BlockCipher(){};

  // BlockSize returns the size of each block supported by this cipher stream.
  virtual size_t BlockSize() = 0;

  // Encrypt a block of data.
  // Length of data is equal to BlockSize().
  virtual Status Encrypt(char* data) = 0;

  // Decrypt a block of data.
  // Length of data is equal to BlockSize().
  virtual Status Decrypt(char* data) = 0;
};

// Implements a BlockCipher using ROT13.
//
// Note: This is a sample implementation of BlockCipher,
// it is NOT considered safe and should NOT be used in production.
class ROT13BlockCipher : public BlockCipher {
 private:
  size_t blockSize_;

 public:
  ROT13BlockCipher(size_t blockSize) : blockSize_(blockSize) {}
  virtual ~ROT13BlockCipher(){};

  // BlockSize returns the size of each block supported by this cipher stream.
  virtual size_t BlockSize() override { return blockSize_; }

  // Encrypt a block of data.
  // Length of data is equal to BlockSize().
  virtual Status Encrypt(char* data) override;

  // Decrypt a block of data.
  // Length of data is equal to BlockSize().
  virtual Status Decrypt(char* data) override;
};

// CTRCipherStream implements BlockAccessCipherStream using an
// Counter operations mode.
// See https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation
//
// Note: This is a possible implementation of BlockAccessCipherStream,
// it is considered suitable for use.
class CTRCipherStream final : public BlockAccessCipherStream {
 private:
  BlockCipher& cipher_;
  std::string iv_;
  uint64_t initialCounter_;

 public:
  CTRCipherStream(BlockCipher& c, const char* iv, uint64_t initialCounter)
      : cipher_(c), iv_(iv, c.BlockSize()), initialCounter_(initialCounter){};
  virtual ~CTRCipherStream(){};

  // BlockSize returns the size of each block supported by this cipher stream.
  virtual size_t BlockSize() override { return cipher_.BlockSize(); }

 protected:
  // Allocate scratch space which is passed to EncryptBlock/DecryptBlock.
  virtual void AllocateScratch(std::string&) override;

  // Encrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual Status EncryptBlock(uint64_t blockIndex, char* data,
                              char* scratch) override;

  // Decrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual Status DecryptBlock(uint64_t blockIndex, char* data,
                              char* scratch) override;
};

// The encryption provider is used to create a cipher stream for a specific
// file. The returned cipher stream will be used for actual
// encryption/decryption actions.
class EncryptionProvider {
 public:
  virtual ~EncryptionProvider(){};

  // GetPrefixLength returns the length of the prefix that is added to every
  // file and used for storing encryption options. For optimal performance, the
  // prefix length should be a multiple of the page size.
  virtual size_t GetPrefixLength() = 0;

  // CreateNewPrefix initialized an allocated block of prefix memory
  // for a new file.
  virtual Status CreateNewPrefix(const std::string& fname, char* prefix,
                                 size_t prefixLength) = 0;

  // CreateCipherStream creates a block access cipher stream for a file given
  // given name and options.
  virtual Status CreateCipherStream(
      const std::string& fname, const EnvOptions& options, Slice& prefix,
      std::unique_ptr<BlockAccessCipherStream>* result) = 0;
};

// This encryption provider uses a CTR cipher stream, with a given block cipher
// and IV.
//
// Note: This is a possible implementation of EncryptionProvider,
// it is considered suitable for use, provided a safe BlockCipher is used.
class CTREncryptionProvider : public EncryptionProvider {
 private:
  BlockCipher& cipher_;

 protected:
  const static size_t defaultPrefixLength = 4096;

 public:
  CTREncryptionProvider(BlockCipher& c) : cipher_(c){};
  virtual ~CTREncryptionProvider() {}

  // GetPrefixLength returns the length of the prefix that is added to every
  // file
  // and used for storing encryption options.
  // For optimal performance, the prefix length should be a multiple of
  // the page size.
  virtual size_t GetPrefixLength() override;

  // CreateNewPrefix initialized an allocated block of prefix memory
  // for a new file.
  virtual Status CreateNewPrefix(const std::string& fname, char* prefix,
                                 size_t prefixLength) override;

  // CreateCipherStream creates a block access cipher stream for a file given
  // given name and options.
  virtual Status CreateCipherStream(
      const std::string& fname, const EnvOptions& options, Slice& prefix,
      std::unique_ptr<BlockAccessCipherStream>* result) override;

 protected:
  // PopulateSecretPrefixPart initializes the data into a new prefix block
  // that will be encrypted. This function will store the data in plain text.
  // It will be encrypted later (before written to disk).
  // Returns the amount of space (starting from the start of the prefix)
  // that has been initialized.
  virtual size_t PopulateSecretPrefixPart(char* prefix, size_t prefixLength,
                                          size_t blockSize);

  // CreateCipherStreamFromPrefix creates a block access cipher stream for a
  // file given
  // given name and options. The given prefix is already decrypted.
  virtual Status CreateCipherStreamFromPrefix(
      const std::string& fname, const EnvOptions& options,
      uint64_t initialCounter, const Slice& iv, const Slice& prefix,
      std::unique_ptr<BlockAccessCipherStream>* result);
};

class EncryptedSequentialFile : public SequentialFile {
 protected:
  std::unique_ptr<SequentialFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  uint64_t offset_;
  size_t prefixLength_;

 public:
  // Default ctor. Given underlying sequential file is supposed to be at
  // offset == prefixLength.
  EncryptedSequentialFile(std::unique_ptr<SequentialFile>&& f,
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
  virtual Status Read(size_t n, Slice* result, char* scratch) override;

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  virtual Status Skip(uint64_t n) override;

  // Indicates the upper layers if the current SequentialFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const override;

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const override;

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  virtual Status InvalidateCache(size_t offset, size_t length) override;

  // Positioned Read for direct I/O
  // If Direct I/O enabled, offset, n, and scratch should be properly aligned
  virtual Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                                char* scratch) override;
};

// A file abstraction for randomly reading the contents of a file.
class EncryptedRandomAccessFile : public RandomAccessFile {
 protected:
  std::unique_ptr<RandomAccessFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  size_t prefixLength_;

 public:
  EncryptedRandomAccessFile(std::unique_ptr<RandomAccessFile>&& f,
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
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override;

  // Readahead the file starting from offset by n bytes for caching.
  virtual Status Prefetch(uint64_t offset, size_t n) override;

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
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;

  virtual void Hint(AccessPattern pattern) override;

  // Indicates the upper layers if the current RandomAccessFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const override;

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const override;

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  virtual Status InvalidateCache(size_t offset, size_t length) override;
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class EncryptedWritableFile : public WritableFileWrapper {
 protected:
  std::unique_ptr<WritableFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  size_t prefixLength_;

 public:
  // Default ctor. Prefix is assumed to be written already.
  EncryptedWritableFile(std::unique_ptr<WritableFile>&& f,
                        std::unique_ptr<BlockAccessCipherStream>&& s,
                        size_t prefixLength)
      : WritableFileWrapper(f.get()),
        file_(std::move(f)),
        stream_(std::move(s)),
        prefixLength_(prefixLength) {}

  Status Append(const Slice& data) override;

  Status PositionedAppend(const Slice& data, uint64_t offset) override;

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const override;

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const override;

  /*
   * Get the size of valid data in the file.
   */
  virtual uint64_t GetFileSize() override;

  // Truncate is necessary to trim the file to the correct size
  // before closing. It is not always possible to keep track of the file
  // size due to whole pages writes. The behavior is undefined if called
  // with other writes to follow.
  virtual Status Truncate(uint64_t size) override;

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  // This call has no effect on dirty pages in the cache.
  virtual Status InvalidateCache(size_t offset, size_t length) override;

  // Sync a file range with disk.
  // offset is the starting byte of the file range to be synchronized.
  // nbytes specifies the length of the range to be synchronized.
  // This asks the OS to initiate flushing the cached data to disk,
  // without waiting for completion.
  // Default implementation does nothing.
  virtual Status RangeSync(uint64_t offset, uint64_t nbytes) override;

  // PrepareWrite performs any necessary preparation for a write
  // before the write actually occurs.  This allows for pre-allocation
  // of space on devices where it can result in less file
  // fragmentation and/or less waste from over-zealous filesystem
  // pre-allocation.
  virtual void PrepareWrite(size_t offset, size_t len) override;

  // Pre-allocates space for a file.
  virtual Status Allocate(uint64_t offset, uint64_t len) override;
};

// A file abstraction for random reading and writing.
class EncryptedRandomRWFile : public RandomRWFile {
 protected:
  std::unique_ptr<RandomRWFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  size_t prefixLength_;

 public:
  EncryptedRandomRWFile(std::unique_ptr<RandomRWFile>&& f,
                        std::unique_ptr<BlockAccessCipherStream>&& s,
                        size_t prefixLength)
      : file_(std::move(f)),
        stream_(std::move(s)),
        prefixLength_(prefixLength) {}

  // Indicates if the class makes use of direct I/O
  // If false you must pass aligned buffer to Write()
  virtual bool use_direct_io() const override;

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const override;

  // Write bytes in `data` at  offset `offset`, Returns Status::OK() on success.
  // Pass aligned buffer when use_direct_io() returns true.
  virtual Status Write(uint64_t offset, const Slice& data) override;

  // Read up to `n` bytes starting from offset `offset` and store them in
  // result, provided `scratch` size should be at least `n`.
  // Returns Status::OK() on success.
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override;

  virtual Status Flush() override;

  virtual Status Sync() override;

  virtual Status Fsync() override;

  virtual Status Close() override;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE)
