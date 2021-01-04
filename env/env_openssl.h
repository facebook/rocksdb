//  copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifdef ROCKSDB_OPENSSL_AES_CTR
#ifndef ROCKSDB_LITE

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <algorithm>
#include <cctype>
#include <iostream>

#include "rocksdb/env.h"
#include "rocksdb/env_encryption.h"
#include "util/aligned_buffer.h"
#include "util/coding.h"
#include "util/random.h"

#endif

namespace ROCKSDB_NAMESPACE {

#ifndef ROCKSDB_LITE

// following define block from page 70:
//  https://www.intel.com/content/dam/doc/white-paper/advanced-encryption-standard-new-instructions-set-paper.pdf
#if !defined(ALIGN16)
#if defined(__GNUC__)
#define ALIGN16 __attribute__((aligned(16)))
#else
#define ALIGN16 __declspec(align(16))
#endif
#endif

class UnixLibCrypto {
 public:
  UnixLibCrypto();
  virtual ~UnixLibCrypto() = default;

  bool IsValid() const { return is_valid_; }

  // _new & _free are ssl 1.1, replacing 1.0 _create & _destroy
  using EVP_MD_CTX_new_t = EVP_MD_CTX *(*)(void);
  using EVP_DigestInit_ex_t = int (*)(EVP_MD_CTX *ctx, const EVP_MD *type,
                                      ENGINE *impl);
  using EVP_sha1_t = const EVP_MD *(*)(void);
  using EVP_DigestUpdate_t = int (*)(EVP_MD_CTX *ctx, const void *d,
                                     size_t cnt);
  using EVP_DigestFinal_ex_t = int (*)(EVP_MD_CTX *ctx, unsigned char *md,
                                       unsigned int *s);
  using EVP_MD_CTX_free_t = void (*)(EVP_MD_CTX *ctx);

  EVP_MD_CTX *EVP_MD_CTX_new() const { return ctx_new_(); };

  int EVP_DigestInit_ex(EVP_MD_CTX *ctx, const EVP_MD *type, ENGINE *impl) {
    return digest_init_(ctx, type, impl);
  }

  const EVP_MD *EVP_sha1() { return sha1_(); }

  int EVP_DigestUpdate(EVP_MD_CTX *ctx, const void *d, size_t cnt) {
    return digest_update_(ctx, d, cnt);
  }

  int EVP_DigestFinal_ex(EVP_MD_CTX *ctx, unsigned char *md, unsigned int *s) {
    return digest_final_(ctx, md, s);
  }

  void EVP_MD_CTX_free(EVP_MD_CTX *ctx) { ctx_free_(ctx); }

  EVP_MD_CTX_free_t EVP_MD_CTX_free_ptr() { return ctx_free_; }

  using RAND_bytes_t = int (*)(unsigned char *buf, int num);
  using RAND_poll_t = int (*)();

  int RAND_bytes(unsigned char *buf, int num) { return rand_bytes_(buf, num); }

  int RAND_poll() { return rand_poll_(); }

  using EVP_CIPHER_CTX_reset_t = int (*)(EVP_CIPHER_CTX *ctx);
  using EVP_CIPHER_CTX_new_t = EVP_CIPHER_CTX *(*)(void);
  using EVP_CIPHER_CTX_free_t = void (*)(EVP_CIPHER_CTX *ctx);
  using EVP_EncryptInit_ex_t = int (*)(EVP_CIPHER_CTX *ctx,
                                       const EVP_CIPHER *type, ENGINE *impl,
                                       const unsigned char *key,
                                       const unsigned char *iv);
  using EVP_aes_256_ctr_t = const EVP_CIPHER *(*)(void);
  using EVP_EncryptUpdate_t = int (*)(EVP_CIPHER_CTX *ctx, unsigned char *out,
                                      int *outl, const unsigned char *in,
                                      int inl);
  using EVP_EncryptFinal_ex_t = int (*)(EVP_CIPHER_CTX *ctx, unsigned char *out,
                                        int *outl);

  int EVP_CIPHER_CTX_reset(EVP_CIPHER_CTX *ctx) { return cipher_reset_(ctx); }

  EVP_CIPHER_CTX *EVP_CIPHER_CTX_new(void) const { return cipher_new_(); }

  void EVP_CIPHER_CTX_free(EVP_CIPHER_CTX *ctx) { cipher_free_(ctx); }

  EVP_CIPHER_CTX_free_t EVP_CIPHER_CTX_free_ptr() { return cipher_free_; }

  int EVP_EncryptInit_ex(EVP_CIPHER_CTX *ctx, const EVP_CIPHER *type,
                         ENGINE *impl, const unsigned char *key,
                         const unsigned char *iv) {
    return encrypt_init_(ctx, type, impl, key, iv);
  }

  const EVP_CIPHER *EVP_aes_256_ctr() { return aes_256_ctr_(); }

  int EVP_EncryptUpdate(EVP_CIPHER_CTX *ctx, unsigned char *out, int *outl,
                        const unsigned char *in, int inl) {
    return encrypt_update_(ctx, out, outl, in, inl);
  }

  int EVP_EncryptFinal_ex(EVP_CIPHER_CTX *ctx, unsigned char *out, int *outl) {
    return encrypt_final_(ctx, out, outl);
  }
  static const char *crypto_lib_name_;

 protected:
  EVP_MD_CTX_new_t ctx_new_;
  EVP_DigestInit_ex_t digest_init_;
  EVP_sha1_t sha1_;
  EVP_DigestUpdate_t digest_update_;
  EVP_DigestFinal_ex_t digest_final_;
  EVP_MD_CTX_free_t ctx_free_;

  RAND_bytes_t rand_bytes_;
  RAND_poll_t rand_poll_;

  EVP_CIPHER_CTX_reset_t cipher_reset_;
  EVP_CIPHER_CTX_new_t cipher_new_;
  EVP_CIPHER_CTX_free_t cipher_free_;
  EVP_EncryptInit_ex_t encrypt_init_;
  EVP_aes_256_ctr_t aes_256_ctr_;
  EVP_EncryptUpdate_t encrypt_update_;
  EVP_EncryptFinal_ex_t encrypt_final_;

  bool is_valid_{false};
  std::shared_ptr<DynamicLibrary> lib_;
};

struct ShaDescription {
  uint8_t desc[EVP_MAX_MD_SIZE];
  bool valid;

  ShaDescription() : valid(false) { memset(desc, 0, EVP_MAX_MD_SIZE); }

  ShaDescription(const ShaDescription &rhs) { *this = rhs; }

  ShaDescription &operator=(const ShaDescription &rhs) {
    memcpy(desc, rhs.desc, sizeof(desc));
    valid = rhs.valid;
    return *this;
  }

  ShaDescription(uint8_t *desc_in, size_t desc_len) : valid(false) {
    memset(desc, 0, EVP_MAX_MD_SIZE);
    if (desc_len <= EVP_MAX_MD_SIZE) {
      memcpy(desc, desc_in, desc_len);
      valid = true;
    }
  }

  ShaDescription(const std::string &key_desc_str);

  // see AesCtrKey destructor below.  This data is not really
  //  essential to clear, but trying to set pattern for future work.
  // goal is to explicitly remove desc from memory once no longer needed
  ~ShaDescription() {
    memset(desc, 0, EVP_MAX_MD_SIZE);
    valid = false;
  }

  bool operator<(const ShaDescription &rhs) const {
    return memcmp(desc, rhs.desc, EVP_MAX_MD_SIZE) < 0;
  }

  bool operator==(const ShaDescription &rhs) const {
    return 0 == memcmp(desc, rhs.desc, EVP_MAX_MD_SIZE) && valid == rhs.valid;
  }

  bool IsValid() const { return valid; }
};

std::shared_ptr<ShaDescription> NewShaDescription(
    const std::string &key_desc_str);

struct AesCtrKey {
  uint8_t key[EVP_MAX_KEY_LENGTH];
  bool valid;

  AesCtrKey() : valid(false) { memset(key, 0, EVP_MAX_KEY_LENGTH); }

  AesCtrKey(const uint8_t *key_in, size_t key_len) : valid(false) {
    memset(key, 0, EVP_MAX_KEY_LENGTH);
    if (key_len <= EVP_MAX_KEY_LENGTH) {
      memcpy(key, key_in, key_len);
      valid = true;
    } else {
      valid = false;
    }
  }

  AesCtrKey(const std::string &hex_key_str);

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

  bool operator==(const AesCtrKey &rhs) const {
    return (0 == memcmp(key, rhs.key, EVP_MAX_KEY_LENGTH)) &&
           (valid == rhs.valid);
  }

  bool IsValid() const { return valid; }
};

// code tests for 64 character hex string to yield 32 byte binary key
std::shared_ptr<AesCtrKey> NewAesCtrKey(const std::string &hex_key_str);

class AESBlockAccessCipherStream : public BlockAccessCipherStream {
 public:
  AESBlockAccessCipherStream(const AesCtrKey &key, uint8_t code_version,
                             const uint8_t nonce[])
      : key_(key), code_version_(code_version) {
    memcpy(&nonce_, nonce, AES_BLOCK_SIZE);
  }

  // BlockSize returns the size of each block supported by this cipher stream.
  size_t BlockSize() override { return AES_BLOCK_SIZE; };

  // Encrypt one or more (partial) blocks of data at the file offset.
  // Length of data is given in data_size.
  Status Encrypt(uint64_t file_offset, char *data, size_t data_size) override;

  // Decrypt one or more (partial) blocks of data at the file offset.
  // Length of data is given in data_size.
  Status Decrypt(uint64_t file_offset, char *data, size_t data_size) override;

  // helper routine to combine 128 bit nounce_ with offset
  static void BigEndianAdd128(uint8_t *buf, uint64_t value);

 protected:
  void AllocateScratch(std::string &) override{};

  Status EncryptBlock(uint64_t, char *, char *) override {
    return Status::NotSupported("Wrong EncryptionProvider assumed");
  };

  Status DecryptBlock(uint64_t, char *, char *) override {
    return Status::NotSupported("Wrong EncryptionProvider assumed");
  };

  AesCtrKey key_;
  uint8_t code_version_;
  uint8_t nonce_[AES_BLOCK_SIZE];
};

constexpr uint8_t kEncryptCodeVersion0{'0'};
constexpr uint8_t kEncryptCodeVersion1{'1'};

typedef char EncryptMarker[8];
extern EncryptMarker kEncryptMarker;

// long term:  code_version could be used in a switch statement or factory
// prefix version 0 is 12 byte sha1 description hash, 128 bit (16 byte)
// nounce (assumed to be packed/byte aligned)
typedef struct {
  uint8_t key_description_[EVP_MAX_MD_SIZE];  // max md is 64
  uint8_t nonce_[AES_BLOCK_SIZE];             // block size is 16
} PrefixVersion0;

class OpenSSLEncryptionProvider : public EncryptionProvider {
 public:
  OpenSSLEncryptionProvider(){};

  OpenSSLEncryptionProvider(const EncryptionProvider &&) = delete;

  OpenSSLEncryptionProvider(const OpenSSLEncryptionProvider &&) = delete;

  const char *Name() const override { return kName(); }

  static const char *kName() { return "OpenSSLEncryptionProvider"; }

  size_t GetPrefixLength() const override { return 4096; }

  Status CreateNewPrefix(const std::string & /*fname*/, char *prefix,
                         size_t prefixLength) const override;

  Status AddCipher(const std::string &descriptor, const char *cipher,
                   size_t len, bool for_write) override;

  Status CreateCipherStream(
      const std::string & /*fname*/, const EnvOptions & /*options*/,
      Slice & /*prefix*/,
      std::unique_ptr<BlockAccessCipherStream> * /*result*/) override;

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
    std::shared_ptr<EncryptionProvider> *result);

#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_OPENSSL_AES_CTR
