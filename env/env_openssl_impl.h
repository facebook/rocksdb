//  copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifdef ROCKSDB_OPENSSL_AES_CTR
#ifndef ROCKSDB_LITE

#include "openssl/aes.h"
#include "openssl/evp.h"
#include "rocksdb/env_openssl.h"

namespace ROCKSDB_NAMESPACE {

// following define block from page 70:
//  https://www.intel.com/content/dam/doc/white-paper/advanced-encryption-standard-new-instructions-set-paper.pdf
#if !defined(ALIGN16)
#if defined(__GNUC__)
#define ALIGN16 __attribute__((aligned(16)))
#else
#define ALIGN16 __declspec(align(16))
#endif
#endif

constexpr uint8_t kEncryptCodeVersion0{'0'};

typedef char EncryptMarker[8];
static EncryptMarker kEncryptMarker = "Encrypt";

// long term:  code_version could be used in a switch statement or factory
// prefix version 0 is 12 byte sha1 description hash, 128 bit (16 byte)
// nounce (assumed to be packed/byte aligned)
typedef struct {
  uint8_t key_description_[EVP_MAX_MD_SIZE];  // max md is 64
  uint8_t nonce_[AES_BLOCK_SIZE];             // block size is 16
} PrefixVersion0;

class AESBlockAccessCipherStream : public BlockAccessCipherStream {
 public:
  AESBlockAccessCipherStream(const AesCtrKey& key, uint8_t code_version,
                             const uint8_t nonce[])
      : key_(key), code_version_(code_version) {
    memcpy(&nonce_, nonce, AES_BLOCK_SIZE);
  }

  // BlockSize returns the size of each block supported by this cipher stream.
  size_t BlockSize() override { return AES_BLOCK_SIZE; };

  // Encrypt one or more (partial) blocks of data at the file offset.
  // Length of data is given in data_size.
  Status Encrypt(uint64_t file_offset, char* data, size_t data_size) override;

  // Decrypt one or more (partial) blocks of data at the file offset.
  // Length of data is given in data_size.
  Status Decrypt(uint64_t file_offset, char* data, size_t data_size) override;

  // helper routine to combine 128 bit nounce_ with offset
  static void BigEndianAdd128(uint8_t* buf, uint64_t value);

 protected:
  void AllocateScratch(std::string&) override{};

  Status EncryptBlock(uint64_t, char*, char*) override {
    return Status::NotSupported("Wrong EncryptionProvider assumed");
  };

  Status DecryptBlock(uint64_t, char*, char*) override {
    return Status::NotSupported("Wrong EncryptionProvider assumed");
  };

  AesCtrKey key_;
  uint8_t code_version_;
  uint8_t nonce_[AES_BLOCK_SIZE];
};

class EncryptedWritableFileV2 : public EncryptedWritableFile {
 public:
  // Default ctor. Prefix is assumed to be written already.
  EncryptedWritableFileV2(std::unique_ptr<WritableFile>&& f,
                          std::unique_ptr<BlockAccessCipherStream>&& s,
                          size_t prefix_length)
      : EncryptedWritableFile(std::move(f), std::move(s), prefix_length) {}

  Status Append(const Slice& data) override;

  Status PositionedAppend(const Slice& data, uint64_t offset) override;

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  bool use_direct_io() const override { return false; };
};

// A file abstraction for random reading and writing.
class EncryptedRandomRWFileV2 : public EncryptedRandomRWFile {
 protected:
 public:
  EncryptedRandomRWFileV2(std::unique_ptr<RandomRWFile>&& f,
                          std::unique_ptr<BlockAccessCipherStream>&& s,
                          size_t prefixLength)
      : EncryptedRandomRWFile(std::move(f), std::move(s), prefixLength) {}

  // Indicates if the class makes use of direct I/OF
  // If false you must pass aligned buffer to Write()
  bool use_direct_io() const override { return false; };

  // Write bytes in `data` at  offset `offset`, Returns Status::OK() on success.
  // Pass aligned buffer when use_direct_io() returns true.
  Status Write(uint64_t offset, const Slice& data) override;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
#endif  // ROCKSDB_OPENSSL_AES_CTR
