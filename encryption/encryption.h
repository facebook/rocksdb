// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#pragma once
#ifndef ROCKSDB_LITE
#ifdef OPENSSL
#include <openssl/aes.h>
#include <openssl/evp.h>

#include <string>

#include "encryption/encryption.h"
#include "rocksdb/env_encryption.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace encryption {
class AESCTRCipherStream;

using evp_ctx_unique_ptr =
    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)>;

// The encryption method supported.
enum class EncryptionMethod : int {
  kUnknown = 0,
  kPlaintext = 1,
  kAES128_CTR = 2,
  kAES192_CTR = 3,
  kAES256_CTR = 4,
  // OpenSSL support SM4 after 1.1.1 release version.
  kSM4_CTR = 5,
};

// Get the key size of the encryption method.
size_t KeySize(EncryptionMethod method);

// Get the block size of the encryption method.
size_t BlockSize(EncryptionMethod method);

// Get the encryption method from string, case-insensitive.
EncryptionMethod EncryptionMethodStringToEnum(const std::string& method);

// Get the OpenSSL EVP_CIPHER according to the encryption method.
const EVP_CIPHER* GetEVPCipher(EncryptionMethod method);

// Get the last OpenSSL error message.
std::string GetOpenSSLErrors();

// Convert an OpenSSL error to an IOError Status.
#define OPENSSL_RET_NOT_OK(call, msg)                  \
  if (UNLIKELY((call) <= 0)) {                         \
    return Status::IOError((msg), GetOpenSSLErrors()); \
  }

Status NewAESCTRCipherStream(EncryptionMethod method,
                             const std::string& file_key,
                             const std::string& file_key_iv,
                             std::unique_ptr<AESCTRCipherStream>* result);

// The cipher stream for AES-CTR encryption.
class AESCTRCipherStream : public BlockAccessCipherStream {
 public:
  AESCTRCipherStream(const EncryptionMethod method, const std::string& file_key,
                     uint64_t iv_high, uint64_t iv_low)
      : method_(method),
        file_key_(file_key),
        initial_iv_high_(iv_high),
        initial_iv_low_(iv_low) {}

  ~AESCTRCipherStream() = default;

  size_t BlockSize() override;

  enum class EncryptType : int { kDecrypt = 0, kEncrypt = 1 };

  Status Encrypt(uint64_t file_offset, char* data, size_t data_size) override {
    return Cipher(file_offset, data, data_size, EncryptType::kEncrypt);
  }

  Status Decrypt(uint64_t file_offset, char* data, size_t data_size) override {
    return Cipher(file_offset, data, data_size, EncryptType::kDecrypt);
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
                EncryptType encrypt_type);

  const EncryptionMethod method_;
  const std::string file_key_;
  const uint64_t initial_iv_high_;
  const uint64_t initial_iv_low_;
};

// TODO(yingchun): Is it possible to derive from CTREncryptionProvider?
// The encryption provider for AES-CTR encryption.
class AESEncryptionProvider : public EncryptionProvider {
 public:
  AESEncryptionProvider(std::string instance_key, EncryptionMethod method)
      : instance_key_(std::move(instance_key)), method_(method) {}
  virtual ~AESEncryptionProvider() = default;

  static const char* kClassName() { return "AES"; }
  const char* Name() const override { return kClassName(); }
  bool IsInstanceOf(const std::string& name) const override;

  size_t GetPrefixLength() const override { return kDefaultPageSize; }

  Status CreateNewPrefix(const std::string& /*fname*/, char* prefix,
                         size_t prefix_length) const override;

  Status AddCipher(const std::string& /*descriptor*/, const char* /*cipher*/,
                   size_t /*len*/, bool /*for_write*/) override {
    return Status::NotSupported();
  }

  Status CreateCipherStream(
      const std::string& fname, const EnvOptions& options, Slice& prefix,
      std::unique_ptr<BlockAccessCipherStream>* result) override;

 private:
  struct FileEncryptionInfo {
    EncryptionMethod method = EncryptionMethod::kUnknown;
    std::string key;
    std::string iv;  // TODO(yingchun): not used yet
  };

  Status WriteEncryptionHeader(char* header_buf) const;
  Status ReadEncryptionHeader(Slice prefix,
                              FileEncryptionInfo* file_info) const;

  const std::string instance_key_;
  const EncryptionMethod method_;
};

}  // namespace encryption
}  // namespace ROCKSDB_NAMESPACE

#endif  // OPENSSL
#endif  // !ROCKSDB_LITE
