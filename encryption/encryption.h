#pragma once
#ifndef ROCKSDB_LITE
#ifdef OPENSSL
#include <openssl/aes.h>

#include "rocksdb/encryption.h"
#include "rocksdb/env_encryption.h"
#include "util/coding.h"

namespace rocksdb {
namespace encryption {

class AESBlockCipher final : public BlockCipher {
 public:
  virtual ~AESBlockCipher() = default;

  Status InitKey(const std::string& key);

  size_t BlockSize() override {
    return AES_BLOCK_SIZE;  // 16
  }

  Status Encrypt(char* data) override {
    AES_encrypt(reinterpret_cast<unsigned char*>(data),
                reinterpret_cast<unsigned char*>(data), &encrypt_key_);
    return Status::OK();
  }

  Status Decrypt(char* data) override {
    AES_decrypt(reinterpret_cast<unsigned char*>(data),
                reinterpret_cast<unsigned char*>(data), &decrypt_key_);
    return Status::OK();
  }

 private:
  AES_KEY encrypt_key_;
  AES_KEY decrypt_key_;
};

class AESCTRCipherStream : public BlockAccessCipherStream {
 public:
  static constexpr size_t kNonceSize = AES_BLOCK_SIZE - sizeof(uint64_t);  // 8

  AESCTRCipherStream(const std::string& iv)
      : nonce_(iv, 0, kNonceSize),
        initial_counter_(
            *reinterpret_cast<const uint64_t*>(iv.data() + kNonceSize)) {}

  size_t BlockSize() override {
    return AES_BLOCK_SIZE;  // 16
  }

  Status InitKey(const std::string& key) { return block_cipher_.InitKey(key); }

 protected:
  void AllocateScratch(std::string& scratch) override {
    scratch.reserve(BlockSize());
  }

  Status EncryptBlock(uint64_t block_index, char* data,
                      char* scratch) override {
    memcpy(scratch, nonce_.data(), kNonceSize);
    EncodeFixed64(scratch + kNonceSize, block_index + initial_counter_);
    Status s = block_cipher_.Encrypt(scratch);
    if (!s.ok()) {
      return s;
    }
    for (size_t i = 0; i < AES_BLOCK_SIZE; i++) {
      data[i] = data[i] ^ scratch[i];
    }
    return Status::OK();
  }

  Status DecryptBlock(uint64_t block_index, char* data,
                      char* scratch) override {
    return EncryptBlock(block_index, data, scratch);
  }

 private:
  AESBlockCipher block_cipher_;
  std::string nonce_;
  uint64_t initial_counter_;
};

extern Status NewAESCTRCipherStream(
    EncryptionMethod method, const std::string& key, const std::string& iv,
    std::unique_ptr<AESCTRCipherStream>* result);

class AESEncryptionProvider : public EncryptionProvider {
 public:
  AESEncryptionProvider(KeyManager* key_manager) : key_manager_(key_manager) {}
  virtual ~AESEncryptionProvider() = default;

  size_t GetPrefixLength() override { return 0; }

  Status CreateNewPrefix(const std::string& /*fname*/, char* /*prefix*/,
                         size_t /*prefix_length*/) override {
    return Status::OK();
  }

  Status CreateCipherStream(
      const std::string& fname, const EnvOptions& options, Slice& prefix,
      std::unique_ptr<BlockAccessCipherStream>* result) override;

 private:
  KeyManager* key_manager_;
};

}  // namespace encryption
}  // namespace rocksdb

#endif  // OPENSSL
#endif  // !ROCKSDB_LITE
