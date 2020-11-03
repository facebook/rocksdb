//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//
//  env_encryption.cc copied to this file then modified.

#ifdef ROCKSDB_OPENSSL_AES_CTR
#ifndef ROCKSDB_LITE

#include <algorithm>
#include <cctype>
#include <iostream>
#include <mutex>

#include "env/env_openssl_impl.h"
#include "monitoring/perf_context_imp.h"
#include "port/port.h"
#include "util/aligned_buffer.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

static std::once_flag crypto_loaded;
static std::shared_ptr<UnixLibCrypto> crypto_shared;

std::shared_ptr<UnixLibCrypto> GetCrypto() {
  std::call_once(crypto_loaded,
                 []() { crypto_shared = std::make_shared<UnixLibCrypto>(); });
  return crypto_shared;
}

// reuse cipher context between calls to Encrypt & Decrypt
static void do_nothing(EVP_CIPHER_CTX*){};
thread_local static std::unique_ptr<EVP_CIPHER_CTX, void (*)(EVP_CIPHER_CTX*)>
    aes_context(nullptr, &do_nothing);

ShaDescription::ShaDescription(const std::string& key_desc_str) {
  GetCrypto();  // ensure libcryto available
  bool good = {true};
  int ret_val;
  unsigned len;

  memset(desc, 0, EVP_MAX_MD_SIZE);
  if (0 != key_desc_str.length() && crypto_shared->IsValid()) {
    std::unique_ptr<EVP_MD_CTX, void (*)(EVP_MD_CTX*)> context(
        crypto_shared->EVP_MD_CTX_new(), crypto_shared->EVP_MD_CTX_free_ptr());

    ret_val = crypto_shared->EVP_DigestInit_ex(
        context.get(), crypto_shared->EVP_sha1(), nullptr);
    good = (1 == ret_val);
    if (good) {
      ret_val = crypto_shared->EVP_DigestUpdate(
          context.get(), key_desc_str.c_str(), key_desc_str.length());
      good = (1 == ret_val);
    }

    if (good) {
      ret_val = crypto_shared->EVP_DigestFinal_ex(context.get(), desc, &len);
      good = (1 == ret_val);
    }
  } else {
    good = false;
  }

  valid = good;
}

std::shared_ptr<ShaDescription> NewShaDescription(
    const std::string& key_desc_str) {
  return std::make_shared<ShaDescription>(key_desc_str);
}

AesCtrKey::AesCtrKey(const std::string& key_str) : valid(false) {
  GetCrypto();  // ensure libcryto available
  memset(key, 0, EVP_MAX_KEY_LENGTH);

  // simple parse:  must be 64 characters long and hexadecimal values
  if (64 == key_str.length()) {
    auto bad_pos = key_str.find_first_not_of("abcdefABCDEF0123456789");
    if (std::string::npos == bad_pos) {
      for (size_t idx = 0, idx2 = 0; idx < key_str.length(); idx += 2, ++idx2) {
        std::string hex_string(key_str.substr(idx, 2));
        key[idx2] = std::stoul(hex_string, 0, 16);
      }
      valid = true;
    }
  }
}

// code tests for 64 character hex string to yield 32 byte binary key
std::shared_ptr<AesCtrKey> NewAesCtrKey(const std::string& hex_key_str) {
  return std::make_shared<AesCtrKey>(hex_key_str);
}

void AESBlockAccessCipherStream::BigEndianAdd128(uint8_t* buf, uint64_t value) {
  uint8_t *sum, *addend, *carry, pre, post;

  sum = buf + 15;

  if (port::kLittleEndian) {
    addend = (uint8_t*)&value;
  } else {
    addend = (uint8_t*)&value + 7;
  }

  // future:  big endian could be written as uint64_t add
  for (int loop = 0; loop < 8 && value; ++loop) {
    pre = *sum;
    *sum += *addend;
    post = *sum;
    --sum;
    value >>= 8;

    carry = sum;
    // carry?
    while (post < pre && buf <= carry) {
      pre = *carry;
      *carry += 1;
      post = *carry;
      --carry;
    }
  }  // for
}

// "data" is assumed to be aligned at AES_BLOCK_SIZE or greater
Status AESBlockAccessCipherStream::Encrypt(uint64_t file_offset, char* data,
                                           size_t data_size) {
  Status status;
  if (0 < data_size) {
    if (crypto_shared->IsValid()) {
      int ret_val, out_len;
      ALIGN16 uint8_t iv[AES_BLOCK_SIZE];
      uint64_t block_index = file_offset / BlockSize();

      // make a context once per thread
      if (!aes_context) {
        aes_context =
            std::unique_ptr<EVP_CIPHER_CTX, void (*)(EVP_CIPHER_CTX*)>(
                crypto_shared->EVP_CIPHER_CTX_new(),
                crypto_shared->EVP_CIPHER_CTX_free_ptr());
      }

      memcpy(iv, nonce_, AES_BLOCK_SIZE);
      BigEndianAdd128(iv, block_index);

      ret_val = crypto_shared->EVP_EncryptInit_ex(
          aes_context.get(), crypto_shared->EVP_aes_256_ctr(), nullptr,
          key_.key, iv);
      if (1 == ret_val) {
        out_len = 0;
        ret_val = crypto_shared->EVP_EncryptUpdate(
            aes_context.get(), (unsigned char*)data, &out_len,
            (unsigned char*)data, (int)data_size);

        if (1 == ret_val && (int)data_size == out_len) {
          // this is a soft reset of aes_context per man pages
          uint8_t temp_buf[AES_BLOCK_SIZE];
          out_len = 0;
          ret_val = crypto_shared->EVP_EncryptFinal_ex(aes_context.get(),
                                                       temp_buf, &out_len);

          if (1 != ret_val || 0 != out_len) {
            status = Status::InvalidArgument(
                "EVP_EncryptFinal_ex failed: ",
                (1 != ret_val) ? "bad return value" : "output length short");
          }
        } else {
          status = Status::InvalidArgument("EVP_EncryptUpdate failed: ",
                                           (int)data_size == out_len
                                               ? "bad return value"
                                               : "output length short");
        }
      } else {
        status = Status::InvalidArgument("EVP_EncryptInit_ex failed.");
      }
    } else {
      status = Status::NotSupported(
          "libcrypto not available for encryption/decryption.");
    }
  }

  return status;
}

// Decrypt one or more (partial) blocks of data at the file offset.
//  Length of data is given in data_size.
//  CTR Encrypt and Decrypt are synonyms.  Using Encrypt calls here to reduce
//   count of symbols loaded from libcrypto.
Status AESBlockAccessCipherStream::Decrypt(uint64_t file_offset, char* data,
                                           size_t data_size) {
  // Calculate block index
  size_t block_size = BlockSize();
  uint64_t block_index = file_offset / block_size;
  size_t block_offset = file_offset % block_size;
  size_t remaining = data_size;
  size_t prefix_size = 0;
  uint8_t temp_buf[block_size];

  Status status;
  ALIGN16 uint8_t iv[AES_BLOCK_SIZE];
  int out_len = 0, ret_val;

  if (crypto_shared->IsValid()) {
    // make a context once per thread
    if (!aes_context) {
      aes_context = std::unique_ptr<EVP_CIPHER_CTX, void (*)(EVP_CIPHER_CTX*)>(
          crypto_shared->EVP_CIPHER_CTX_new(),
          crypto_shared->EVP_CIPHER_CTX_free_ptr());
    }

    memcpy(iv, nonce_, AES_BLOCK_SIZE);
    BigEndianAdd128(iv, block_index);

    ret_val = crypto_shared->EVP_EncryptInit_ex(
        aes_context.get(), crypto_shared->EVP_aes_256_ctr(), nullptr, key_.key,
        iv);
    if (1 == ret_val) {
      // handle uneven block start
      if (0 != block_offset) {
        prefix_size = block_size - block_offset;
        if (data_size < prefix_size) {
          prefix_size = data_size;
        }

        memcpy(temp_buf + block_offset, data, prefix_size);
        out_len = 0;
        ret_val = crypto_shared->EVP_EncryptUpdate(
            aes_context.get(), temp_buf, &out_len, temp_buf, (int)block_size);

        if (1 != ret_val || (int)block_size != out_len) {
          status = Status::InvalidArgument("EVP_EncryptUpdate failed 1: ",
                                           (int)block_size == out_len
                                               ? "bad return value"
                                               : "output length short");
        } else {
          memcpy(data, temp_buf + block_offset, prefix_size);
        }
      }

      // all remaining data, even block size not required
      remaining -= prefix_size;
      if (status.ok() && remaining) {
        out_len = 0;
        ret_val = crypto_shared->EVP_EncryptUpdate(
            aes_context.get(), (uint8_t*)data + prefix_size, &out_len,
            (uint8_t*)data + prefix_size, (int)remaining);

        if (1 != ret_val || (int)remaining != out_len) {
          status = Status::InvalidArgument("EVP_EncryptUpdate failed 2: ",
                                           (int)remaining == out_len
                                               ? "bad return value"
                                               : "output length short");
        }
      }

      // this is a soft reset of aes_context per man pages
      out_len = 0;
      ret_val = crypto_shared->EVP_EncryptFinal_ex(aes_context.get(), temp_buf,
                                                   &out_len);

      if (1 != ret_val || 0 != out_len) {
        status = Status::InvalidArgument("EVP_EncryptFinal_ex failed.");
      }
    } else {
      status = Status::InvalidArgument("EVP_EncryptInit_ex failed.");
    }
  } else {
    status = Status::NotSupported(
        "libcrypto not available for encryption/decryption.");
  }

  return status;
}
#if 0
std::shared_ptr<OpenSSLEncryptionProvider> NewOpenSSLEncryptionProvider(
    const std::shared_ptr<ShaDescription>& key_desc,
    const std::shared_ptr<AesCtrKey>& aes_ctr_key) {
  return std::make_shared<OpenSSLEncryptionProvider>(*key_desc.get(),
                                                   *aes_ctr_key.get());
}

std::shared_ptr<OpenSSLEncryptionProvider> NewOpenSSLEncryptionProvider(
    const std::string& key_desc_str, const uint8_t binary_key[], int bytes) {
  return std::make_shared<OpenSSLEncryptionProvider>(key_desc_str, binary_key,
                                                   bytes);
}
#endif
Status OpenSSLEncryptionProvider::CreateNewPrefix(const std::string& /*fname*/,
                                                char* prefix,
                                                size_t prefixLength) const {
  GetCrypto();  // ensure libcryto available
  Status s;
  if (crypto_shared->IsValid()) {
    if ((sizeof(EncryptMarker)+sizeof(PrefixVersion0)) <= prefixLength) {
      int ret_val;

      PrefixVersion0* pf = (PrefixVersion0*)(prefix + sizeof(EncryptMarker));
      memcpy(prefix, kEncryptMarker, sizeof(kEncryptMarker));
      *(prefix+7) = kEncryptCodeVersion1;
      memcpy(pf->key_description_, encrypt_write_.first.desc, sizeof(ShaDescription::desc));
      ret_val = crypto_shared->RAND_bytes((unsigned char*)&pf->nonce_,
                                          AES_BLOCK_SIZE);
      if (1 != ret_val) {
        s = Status::NotSupported("RAND_bytes failed");
      }
    } else {
      s = Status::NotSupported("Prefix size needs to be 28 or more");
    }
  } else {
    s = Status::NotSupported("RAND_bytes() from libcrypto not available.");
  }

  return s;
}

Status OpenSSLEncryptionProvider::CreateCipherStream(
    const std::string& /*fname*/, const EnvOptions& /*options*/,
    Slice& prefix,
    std::unique_ptr<BlockAccessCipherStream>* result) {

  Status status;

  if ((sizeof(EncryptMarker)+sizeof(PrefixVersion0)) <= prefix.size()
      && prefix.starts_with(kEncryptMarker)) {

    uint8_t code_version = (uint8_t)prefix[7];

    if (kEncryptCodeVersion1 == code_version) {
      Slice prefix_slice;
      PrefixVersion0 * prefix_buffer = (PrefixVersion0*)(prefix.data()+sizeof(EncryptMarker));
      ShaDescription desc(prefix_buffer->key_description_,
                          sizeof(PrefixVersion0::key_description_));

      ReadLock lock(&key_lock_);
      auto it = encrypt_read_.find(desc);
      if (encrypt_read_.end() != it) {
        result->reset(new AESBlockAccessCipherStream(
            it->second, code_version, prefix_buffer->nonce_));
      } else {
        status = Status::NotSupported(
            "No encryption key found to match input file");
      }
    } else {
      status =
          Status::NotSupported("Unknown encryption code version required.");
    }
  } else {
    status =
        Status::EncryptionUnknown("Unknown encryption marker or not encrypted.");
  }

  return status;
}

std::string OpenSSLEncryptionProvider::GetMarker() const {return kEncryptMarker;}





}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
#endif  // ROCKSDB_OPENSSL_AES_CTR
