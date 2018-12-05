//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) 

#include <string>

#include "env.h"

namespace rocksdb {
class EncryptionProvider;
class BlockCipher;
struct DBOptions;
struct ColumnFamilyOptions;
  
// Returns an Env that encrypts data when stored on disk and decrypts data when 
// read from disk.
Env* NewEncryptedEnv(Env* base_env,
		     const std::shared_ptr<EncryptionProvider> & provider);

// BlockAccessCipherStream is the base class for any cipher stream that 
// supports random access at block level (without requiring data from other blocks).
// E.g. CTR (Counter operation mode) supports this requirement.
class BlockAccessCipherStream {
    public:
      virtual ~BlockAccessCipherStream() {};

      // BlockSize returns the size of each block supported by this cipher stream.
      virtual size_t BlockSize() = 0;

      // Encrypt one or more (partial) blocks of data at the file offset.
      // Length of data is given in dataSize.
      virtual Status Encrypt(uint64_t fileOffset, char *data, size_t dataSize);

      // Decrypt one or more (partial) blocks of data at the file offset.
      // Length of data is given in dataSize.
      virtual Status Decrypt(uint64_t fileOffset, char *data, size_t dataSize);

    protected:
      // Allocate scratch space which is passed to EncryptBlock/DecryptBlock.
      virtual void AllocateScratch(std::string&) = 0;

      // Encrypt a block of data at the given block index.
      // Length of data is equal to BlockSize();
      virtual Status EncryptBlock(uint64_t blockIndex, char *data, char* scratch) = 0;

      // Decrypt a block of data at the given block index.
      // Length of data is equal to BlockSize();
      virtual Status DecryptBlock(uint64_t blockIndex, char *data, char* scratch) = 0;
};

// BlockCipher 
  class BlockCipher : public Extension {
  public:
    static const std::string kType;
    public:
      virtual ~BlockCipher() {};
      static const std::string & Type() {
	return kType;
      }

      // BlockSize returns the size of each block supported by this cipher stream.
      virtual size_t BlockSize() = 0;

      // Encrypt a block of data.
      // Length of data is equal to BlockSize().
      virtual Status Encrypt(char *data) = 0;

      // Decrypt a block of data.
      // Length of data is equal to BlockSize().
      virtual Status Decrypt(char *data) = 0;
};


// The encryption provider is used to create a cipher stream for a specific file.
// The returned cipher stream will be used for actual encryption/decryption 
// actions.
class EncryptionProvider : public Extension {
public:
    static const std::string kType;
 public:
    virtual ~EncryptionProvider() {};
    static const std::string & Type() {
      return kType;
    }

    // GetPrefixLength returns the length of the prefix that is added to every file
    // and used for storing encryption options.
    // For optimal performance, the prefix length should be a multiple of 
    // the page size.
    virtual size_t GetPrefixLength() = 0;

    // CreateNewPrefix initialized an allocated block of prefix memory 
    // for a new file.
    virtual Status CreateNewPrefix(const std::string& fname, char *prefix, size_t prefixLength) = 0;

    // CreateCipherStream creates a block access cipher stream for a file given
    // given name and options.
    virtual Status CreateCipherStream(const std::string& fname, const EnvOptions& options,
      Slice& prefix, unique_ptr<BlockAccessCipherStream>* result) = 0;
};

}  // namespace rocksdb

#endif  // !defined(ROCKSDB_LITE)
