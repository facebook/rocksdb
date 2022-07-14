// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#pragma once
#ifndef ROCKSDB_LITE
#ifdef OPENSSL
#include <openssl/aes.h>
#include <openssl/evp.h>

#include <string>

#include "rocksdb/encryption.h"
#include "rocksdb/env_encryption.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace encryption {

#if OPENSSL_VERSION_NUMBER < 0x01010000f

#define InitCipherContext(ctx) \
  EVP_CIPHER_CTX ctx##_var;    \
  ctx = &ctx##_var;            \
  EVP_CIPHER_CTX_init(ctx);

// do nothing
#define FreeCipherContext(ctx)

#else

#define InitCipherContext(ctx)            \
  ctx = EVP_CIPHER_CTX_new();             \
  if (ctx != nullptr) {                   \
    if (EVP_CIPHER_CTX_reset(ctx) != 1) { \
      ctx = nullptr;                      \
    }                                     \
  }

#define FreeCipherContext(ctx) EVP_CIPHER_CTX_free(ctx);

#endif

// TODO: OpenSSL Lib does not export SM4_BLOCK_SIZE by now.
// Need to remove SM4_BLOCK_Size once Openssl lib support the definition.
// SM4 uses 128-bit block size as AES.
// Ref:
// https://github.com/openssl/openssl/blob/OpenSSL_1_1_1-stable/include/crypto/sm4.h#L24
#define SM4_BLOCK_SIZE 16

class AESCTRCipherStream : public BlockAccessCipherStream {
 public:
  AESCTRCipherStream(const EVP_CIPHER* cipher, const std::string& key,
                     uint64_t iv_high, uint64_t iv_low)
      : cipher_(cipher),
        key_(key),
        initial_iv_high_(iv_high),
        initial_iv_low_(iv_low) {}

  ~AESCTRCipherStream() = default;

  size_t BlockSize() override {
    // Openssl support SM4 after 1.1.1 release version.
#if OPENSSL_VERSION_NUMBER >= 0x1010100fL && !defined(OPENSSL_NO_SM4)
    if (EVP_CIPHER_nid(cipher_) == NID_sm4_ctr) {
      return SM4_BLOCK_SIZE;
    }
#endif
    return AES_BLOCK_SIZE;  // 16
  }

  Status Encrypt(uint64_t file_offset, char* data, size_t data_size) override {
    return Cipher(file_offset, data, data_size, true /*is_encrypt*/);
  }

  Status Decrypt(uint64_t file_offset, char* data, size_t data_size) override {
    return Cipher(file_offset, data, data_size, false /*is_encrypt*/);
  }

 protected:
  // Following methods required by BlockAccessCipherStream is unused.

  void AllocateScratch(std::string& /*scratch*/) override {
    // should not be called.
    assert(false);
  }

  Status EncryptBlock(uint64_t /*block_index*/, char* /*data*/,
                      char* /*scratch*/) override {
    return Status::NotSupported("EncryptBlock should not be called.");
  }

  Status DecryptBlock(uint64_t /*block_index*/, char* /*data*/,
                      char* /*scratch*/) override {
    return Status::NotSupported("DecryptBlock should not be called.");
  }

 private:
  Status Cipher(uint64_t file_offset, char* data, size_t data_size,
                bool is_encrypt);

  const EVP_CIPHER* cipher_;
  const std::string key_;
  const uint64_t initial_iv_high_;
  const uint64_t initial_iv_low_;
};

extern Status NewAESCTRCipherStream(
    EncryptionMethod method, const std::string& key, const std::string& iv,
    std::unique_ptr<AESCTRCipherStream>* result);

class AESEncryptionProvider : public EncryptionProvider {
 public:
  AESEncryptionProvider(KeyManager* key_manager) : key_manager_(key_manager) {}
  virtual ~AESEncryptionProvider() = default;

  const char* Name() const override { return "AESEncryptionProvider"; }

  size_t GetPrefixLength() const override { return 0; }

  Status CreateNewPrefix(const std::string& /*fname*/, char* /*prefix*/,
                         size_t /*prefix_length*/) const override {
    return Status::OK();
  }

  Status AddCipher(const std::string& /*descriptor*/, const char* /*cipher*/,
                   size_t /*len*/, bool /*for_write*/) override {
    return Status::NotSupported();
  }

  Status CreateCipherStream(
      const std::string& fname, const EnvOptions& options, Slice& prefix,
      std::unique_ptr<BlockAccessCipherStream>* result) override;

 private:
  KeyManager* key_manager_;
};

}  // namespace encryption
}  // namespace ROCKSDB_NAMESPACE

#endif  // OPENSSL
#endif  // !ROCKSDB_LITE
