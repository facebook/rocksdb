// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifdef ROCKSDB_OPENSSL_AES_CTR
#ifndef ROCKSDB_LITE

#include <openssl/evp.h>

#include <map>
#include <memory>
#include <string>

#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

// Base class / interface
//  expectation is to derive one class for unux and one for Windows
//
class LibraryLoader {
 public:
  LibraryLoader() = delete;
  LibraryLoader(const char *library_name);
  virtual ~LibraryLoader() = default;

  bool IsValid() const { return is_valid_; }

  virtual void *GetEntryPoint(const char *function_name);

  virtual size_t GetEntryPoints(std::map<std::string, void *> &functions);

 protected:
  bool is_valid_;
  std::shared_ptr<DynamicLibrary> lib_;
};

class UnixLibCrypto : public LibraryLoader {
 public:
  UnixLibCrypto();
  virtual ~UnixLibCrypto() = default;

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
  std::map<std::string, void *> functions_{
      {"EVP_MD_CTX_new", nullptr},       {"EVP_MD_CTX_create", nullptr},
      {"EVP_DigestInit_ex", nullptr},    {"EVP_sha1", nullptr},
      {"EVP_DigestUpdate", nullptr},     {"EVP_DigestFinal_ex", nullptr},
      {"EVP_MD_CTX_free", nullptr},      {"EVP_MD_CTX_destroy", nullptr},

      {"RAND_bytes", nullptr},           {"RAND_poll", nullptr},

      {"EVP_CIPHER_CTX_new", nullptr},   {"EVP_CIPHER_CTX_free", nullptr},
      {"EVP_EncryptInit_ex", nullptr},   {"EVP_aes_256_ctr", nullptr},
      {"EVP_EncryptUpdate", nullptr},    {"EVP_EncryptFinal_ex", nullptr},
      {"EVP_CIPHER_CTX_reset", nullptr}, {"EVP_CIPHER_CTX_cleanup", nullptr},

  };

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
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
#endif  // ROCKSDB_OPENSSL_AES_CTR
