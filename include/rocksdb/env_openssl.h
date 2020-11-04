//  copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//
// env_encryption.cc copied to this file then modified.

#pragma once

#ifdef ROCKSDB_OPENSSL_AES_CTR
#ifndef ROCKSDB_LITE

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <algorithm>
#include <cctype>
#include <iostream>

#include "env.h"
#include "rocksdb/env_encryption.h"
#include "util/aligned_buffer.h"
#include "util/coding.h"
#include "util/library_loader.h"
#include "util/random.h"

#endif

namespace ROCKSDB_NAMESPACE {

#ifndef ROCKSDB_LITE

struct ShaDescription {
  uint8_t desc[EVP_MAX_MD_SIZE];
  bool valid;

  ShaDescription() : valid(false) { memset(desc, 0, EVP_MAX_MD_SIZE); }

  ShaDescription(const ShaDescription& rhs) { *this = rhs; }

  ShaDescription& operator=(const ShaDescription& rhs) {
    memcpy(desc, rhs.desc, sizeof(desc));
    valid = rhs.valid;
    return *this;
  }

  ShaDescription(uint8_t* desc_in, size_t desc_len) : valid(false) {
    memset(desc, 0, EVP_MAX_MD_SIZE);
    if (desc_len <= EVP_MAX_MD_SIZE) {
      memcpy(desc, desc_in, desc_len);
      valid = true;
    }
  }

  ShaDescription(const std::string& key_desc_str);

  // see AesCtrKey destructor below.  This data is not really
  //  essential to clear, but trying to set pattern for future work.
  // goal is to explicitly remove desc from memory once no longer needed
  ~ShaDescription() {
    memset(desc, 0, EVP_MAX_MD_SIZE);
    valid = false;
  }

  bool operator<(const ShaDescription& rhs) const {
    return memcmp(desc, rhs.desc, EVP_MAX_MD_SIZE) < 0;
  }

  bool operator==(const ShaDescription& rhs) const {
    return 0 == memcmp(desc, rhs.desc, EVP_MAX_MD_SIZE) && valid == rhs.valid;
  }

  bool IsValid() const { return valid; }
};

std::shared_ptr<ShaDescription> NewShaDescription(
    const std::string& key_desc_str);

struct AesCtrKey {
  uint8_t key[EVP_MAX_KEY_LENGTH];
  bool valid;

  AesCtrKey() : valid(false) { memset(key, 0, EVP_MAX_KEY_LENGTH); }

  AesCtrKey(const uint8_t* key_in, size_t key_len) : valid(false) {
    memset(key, 0, EVP_MAX_KEY_LENGTH);
    if (key_len <= EVP_MAX_KEY_LENGTH) {
      memcpy(key, key_in, key_len);
      valid = true;
    } else {
      valid = false;
    }
  }

  AesCtrKey(const std::string& hex_key_str);

  // see Writing Solid Code, 2nd edition
  //   Chapter 9, page 321, Managing Secrets in Memory ... bullet 4 "Scrub the
  //   memory"
  // Not saying this is essential or effective in initial implementation since
  // current
  //  usage model loads all keys at start and only deletes them at shutdown. But
  //  does establish presidence.
  // goal is to explicitly remove key from memory once no longer needed
  ~AesCtrKey() {
    memset(key, 0, EVP_MAX_KEY_LENGTH);
    valid = false;
  }

  bool operator==(const AesCtrKey& rhs) const {
    return (0 == memcmp(key, rhs.key, EVP_MAX_KEY_LENGTH)) &&
           (valid == rhs.valid);
  }

  bool IsValid() const { return valid; }
};

// code tests for 64 character hex string to yield 32 byte binary key
std::shared_ptr<AesCtrKey> NewAesCtrKey(const std::string& hex_key_str);

class OpenSSLEncryptionProvider : public EncryptionProvider {
 public:
  OpenSSLEncryptionProvider(){};

  OpenSSLEncryptionProvider(const EncryptionProvider&&) = delete;

  OpenSSLEncryptionProvider(const OpenSSLEncryptionProvider&&) = delete;

  const char* Name() const override { return kName(); }

  static const char* kName() { return "OpenSSLEncryptionProvider"; }

  size_t GetPrefixLength() const override { return 4096; }

  Status CreateNewPrefix(const std::string& /*fname*/, char* prefix,
                         size_t prefixLength) const override;

  Status AddCipher(const std::string& descriptor, const char* cipher,
                   size_t len, bool for_write) override;

  Status CreateCipherStream(
      const std::string& /*fname*/, const EnvOptions& /*options*/,
      Slice& /*prefix*/,
      std::unique_ptr<BlockAccessCipherStream>* /*result*/) override;

  std::string GetMarker() const override;

  bool Valid() const { return valid_; };

 protected:
  bool valid_{false};

  using WriteKey = std::pair<ShaDescription, AesCtrKey>;
  using ReadKeys = std::map<ShaDescription, AesCtrKey>;

  ReadKeys encrypt_read_;
  WriteKey encrypt_write_;
  mutable port::RWMutex key_lock_;

  // Optional method to initialize an EncryptionProvider in the TEST
  // environment.
  virtual Status TEST_Initialize() override {
    return AddCipher(
        "test key",
        "0102030405060708090A0B0C0D0E0F101112131415161718191a1b1c1d1e1f20", 64,
        true);
  }
};

// Status::NoSupported() if libcrypto unavailable
Status NewOpenSSLEncryptionProvider(
    std::shared_ptr<EncryptionProvider>* result);

#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_OPENSSL_AES_CTR
