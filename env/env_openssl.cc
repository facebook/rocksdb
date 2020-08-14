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

std::shared_ptr<CTREncryptionProviderV2> NewCTREncryptionProviderV2(
    const std::shared_ptr<ShaDescription>& key_desc,
    const std::shared_ptr<AesCtrKey>& aes_ctr_key) {
  return std::make_shared<CTREncryptionProviderV2>(*key_desc.get(),
                                                   *aes_ctr_key.get());
}

std::shared_ptr<CTREncryptionProviderV2> NewCTREncryptionProviderV2(
    const std::string& key_desc_str, const uint8_t binary_key[], int bytes) {
  return std::make_shared<CTREncryptionProviderV2>(key_desc_str, binary_key,
                                                   bytes);
}

Status CTREncryptionProviderV2::CreateNewPrefix(const std::string& /*fname*/,
                                                char* prefix,
                                                size_t prefixLength) const {
  GetCrypto();  // ensure libcryto available
  Status s;
  if (crypto_shared->IsValid()) {
    if (sizeof(PrefixVersion0) <= prefixLength) {
      int ret_val;

      PrefixVersion0* pf = {(PrefixVersion0*)prefix};
      memcpy(pf->key_description_, key_desc_.desc, sizeof(key_desc_.desc));
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

size_t CTREncryptionProviderV2::GetPrefixLength() const {
  return sizeof(PrefixVersion0) + sizeof(EncryptMarker);
}

BlockAccessCipherStream* CTREncryptionProviderV2::CreateCipherStream2(
    uint8_t code_version, const uint8_t nonce[]) const {
  return new AESBlockAccessCipherStream(key_, code_version, nonce);
}

Status EncryptedWritableFileV2::Append(const Slice& data) {
  AlignedBuffer buf;
  Status status;
  Slice dataToAppend(data);
  if (data.size() > 0) {
    size_t block_size = stream_->BlockSize();
    uint64_t offset = file_->GetFileSize();  // size including prefix
    uint64_t block_offset = offset % block_size;

    // Encrypt in cloned buffer
    buf.Alignment(block_size);
    // worst case is one byte only in first and in last block,
    //  so 2*block_size-2 might be needed (simplified to 2*block_size)
    buf.AllocateNewBuffer(data.size() + 2 * block_size);
    memcpy(buf.BufferStart() + block_offset, data.data(), data.size());
    buf.Size(data.size() + block_offset);
    {
      PERF_TIMER_GUARD(encrypt_data_nanos);
      status = stream_->Encrypt(offset - block_offset, buf.BufferStart(),
                                buf.CurrentSize());
    }
    if (status.ok()) {
      dataToAppend = Slice(buf.BufferStart() + block_offset, data.size());
    }
  }

  if (status.ok()) {
    status = file_->Append(dataToAppend);
  }

  return status;
}

Status EncryptedWritableFileV2::PositionedAppend(const Slice& data,
                                                 uint64_t offset) {
  AlignedBuffer buf;
  Status status;
  Slice dataToAppend(data);
  offset += prefixLength_;
  if (data.size() > 0) {
    size_t block_size = stream_->BlockSize();
    uint64_t block_offset = offset % block_size;

    // Encrypt in cloned buffer
    buf.Alignment(block_size);
    // worst case is one byte only in first and in last block,
    //  so 2*block_size-2 might be needed (simplified to 2*block_size)
    buf.AllocateNewBuffer(data.size() + 2 * block_size);
    memcpy(buf.BufferStart() + block_offset, data.data(), data.size());
    buf.Size(data.size() + block_offset);
    {
      PERF_TIMER_GUARD(encrypt_data_nanos);
      status = stream_->Encrypt(offset - block_offset, buf.BufferStart(),
                                buf.CurrentSize());
    }
    if (status.ok()) {
      dataToAppend = Slice(buf.BufferStart() + block_offset, data.size());
    }
  }

  if (status.ok()) {
    status = file_->PositionedAppend(dataToAppend, offset);
  }

  return status;
}

Status EncryptedRandomRWFileV2::Write(uint64_t offset, const Slice& data) {
  AlignedBuffer buf;
  Status status;
  Slice dataToWrite(data);
  offset += prefixLength_;
  if (data.size() > 0) {
    size_t block_size = stream_->BlockSize();
    uint64_t block_offset = offset % block_size;

    // Encrypt in cloned buffer
    buf.Alignment(block_size);
    // worst case is one byte only in first and in last block,
    //  so 2*block_size-2 might be needed (simplified to 2*block_size)
    buf.AllocateNewBuffer(data.size() + 2 * block_size);
    memcpy(buf.BufferStart() + block_offset, data.data(), data.size());
    buf.Size(data.size() + block_offset);
    {
      PERF_TIMER_GUARD(encrypt_data_nanos);
      status = stream_->Encrypt(offset - block_offset, buf.BufferStart(),
                                buf.CurrentSize());
    }
    if (status.ok()) {
      dataToWrite = Slice(buf.BufferStart() + block_offset, data.size());
    }
  }

  if (status.ok()) {
    status = file_->Write(offset, dataToWrite);
  }

  return status;
}

// Returns an Env that encrypts data when stored on disk and decrypts data when
// read from disk.
Env* NewOpenSSLEnv(Env* base_env, OpenSSLEnv::ReadKeys encrypt_read,
                   OpenSSLEnv::WriteKey encrypt_write) {
  Env* ret_env{base_env};
  OpenSSLEnv* new_env{nullptr};

  if (nullptr != base_env) {
    new_env = new OpenSSLEnv(base_env, encrypt_read, encrypt_write);
  }

  // warning, dynamic loading of libcrypto could be delayed ... making this
  // false
  if (nullptr != new_env && new_env->IsValid()) {
    ret_env = new_env;
  }

  return ret_env;
}

OpenSSLEnv::OpenSSLEnv(Env* base_env, OpenSSLEnv::ReadKeys encrypt_read,
                       OpenSSLEnv::WriteKey encrypt_write)
    : EnvWrapper(base_env), valid_(false) {
  init();
  SetKeys(encrypt_read, encrypt_write);
}

OpenSSLEnv::OpenSSLEnv(Env* base_env) : EnvWrapper(base_env), valid_(false) {
  init();
}

void OpenSSLEnv::init() {
  crypto_ = GetCrypto();

  valid_ = crypto_->IsValid();
  if (IsValid()) {
    crypto_->RAND_poll();
  }
}

void OpenSSLEnv::SetKeys(OpenSSLEnv::ReadKeys encrypt_read,
                         OpenSSLEnv::WriteKey encrypt_write) {
  WriteLock lock(&key_lock_);
  encrypt_read_ = encrypt_read;
  encrypt_write_ = encrypt_write;
}

bool OpenSSLEnv::IsWriteEncrypted() const {
  ReadLock lock(&key_lock_);
  bool ret_flag = (nullptr != encrypt_write_.second);
  return ret_flag;
}

//
// common functions used with different file types
//  (because there is not common base class for the file types
//
template <class TypeFile>
Status OpenSSLEnv::ReadSeqEncryptionPrefix(
    TypeFile* f, std::shared_ptr<const CTREncryptionProviderV2>& provider,
    std::unique_ptr<BlockAccessCipherStream>& stream) {
  Status status;

  provider.reset();  // nullptr for provider implies "no encryption"
  stream.release();

  // Look for encryption marker
  EncryptMarker marker;
  Slice marker_slice;
  status = f->Read(sizeof(marker), &marker_slice, marker);
  if (status.ok()) {
    if (sizeof(marker) == marker_slice.size() &&
        marker_slice.starts_with(kEncryptMarker)) {
      // code_version currently unused
      uint8_t code_version = (uint8_t)marker_slice[7];

      if (kEncryptCodeVersion0 == code_version) {
        Slice prefix_slice;
        PrefixVersion0 prefix_buffer;
        status = f->Read(sizeof(PrefixVersion0), &prefix_slice,
                         (char*)&prefix_buffer);
        if (status.ok() && sizeof(PrefixVersion0) == prefix_slice.size()) {
          ShaDescription desc(prefix_buffer.key_description_,
                              sizeof(prefix_buffer.key_description_));

          ReadLock lock(&key_lock_);
          auto it = encrypt_read_.find(desc);
          if (encrypt_read_.end() != it) {
            provider = it->second;
            stream.reset(new AESBlockAccessCipherStream(
                provider->key(), code_version, prefix_buffer.nonce_));

          } else {
            status = Status::NotSupported(
                "No encryption key found to match input file");
          }
        }
      } else {
        status =
            Status::NotSupported("Unknown encryption code version required.");
      }
    }
  }
  return status;
}

template <class TypeFile>
Status OpenSSLEnv::ReadRandEncryptionPrefix(
    TypeFile* f, std::shared_ptr<const CTREncryptionProviderV2>& provider,
    std::unique_ptr<BlockAccessCipherStream>& stream) {
  Status status;

  provider.reset();  // nullptr for provider implies "no encryption"
  stream.release();

  // Look for encryption marker
  EncryptMarker marker;
  Slice marker_slice;
  status = f->Read(0, sizeof(marker), &marker_slice, marker);
  if (status.ok()) {
    if (sizeof(marker) == marker_slice.size() &&
        marker_slice.starts_with(kEncryptMarker)) {
      uint8_t code_version = (uint8_t)marker_slice[7];

      if (kEncryptCodeVersion0 == code_version) {
        Slice prefix_slice;
        PrefixVersion0 prefix_buffer;
        status = f->Read(sizeof(marker), sizeof(PrefixVersion0), &prefix_slice,
                         (char*)&prefix_buffer);
        if (status.ok() && sizeof(PrefixVersion0) == prefix_slice.size()) {
          ShaDescription desc(prefix_buffer.key_description_,
                              sizeof(prefix_buffer.key_description_));

          ReadLock lock(&key_lock_);
          auto it = encrypt_read_.find(desc);
          if (encrypt_read_.end() != it) {
            provider = it->second;
            stream.reset(new AESBlockAccessCipherStream(
                provider->key(), code_version, prefix_buffer.nonce_));
          } else {
            status = Status::NotSupported(
                "No encryption key found to match input file");
          }
        }
      } else {
        status =
            Status::NotSupported("Unknown encryption code version required.");
      }
    }
  }
  return status;
}

template <class TypeFile>
Status OpenSSLEnv::WriteSeqEncryptionPrefix(
    TypeFile* f, std::shared_ptr<const CTREncryptionProviderV2> provider,
    std::unique_ptr<BlockAccessCipherStream>& stream) {
  Status status;

  // set up Encryption maker, code version '0'
  uint8_t code_version = {kEncryptCodeVersion0};
  PrefixVersion0 prefix;
  EncryptMarker marker;
  strncpy(marker, kEncryptMarker, sizeof(kEncryptMarker));
  marker[sizeof(EncryptMarker) - 1] = code_version;

  Slice marker_slice(marker, sizeof(EncryptMarker));
  status = f->Append(marker_slice);

  if (status.ok()) {
    // create nonce, then write it and key description
    Slice prefix_slice((char*)&prefix, sizeof(prefix));

    status = provider->CreateNewPrefix(std::string(), (char*)&prefix,
                                       provider->GetPrefixLength());

    if (status.ok()) {
      status = f->Append(prefix_slice);
    }
  }

  if (status.ok()) {
    stream.reset(new AESBlockAccessCipherStream(provider->key(), code_version,
                                                prefix.nonce_));
  }

  return status;
}

template <class TypeFile>
Status OpenSSLEnv::WriteRandEncryptionPrefix(
    TypeFile* f, std::shared_ptr<const CTREncryptionProviderV2> provider,
    std::unique_ptr<BlockAccessCipherStream>& stream) {
  Status status;

  // set up Encryption maker, code version '0'
  uint8_t code_version = {kEncryptCodeVersion0};
  PrefixVersion0 prefix;
  EncryptMarker marker;
  strncpy(marker, kEncryptMarker, sizeof(kEncryptMarker));
  marker[sizeof(EncryptMarker) - 1] = code_version;

  Slice marker_slice(marker, sizeof(EncryptMarker));
  status = f->Write(0, marker_slice);

  if (status.ok()) {
    // create nonce, then write it and key description
    Slice prefix_slice((char*)&prefix, sizeof(prefix));

    status = provider->CreateNewPrefix(std::string(), (char*)&prefix,
                                       provider->GetPrefixLength());

    if (status.ok()) {
      status = f->Write(sizeof(EncryptMarker), prefix_slice);
    }
  }

  if (status.ok()) {
    stream.reset(new AESBlockAccessCipherStream(provider->key(), code_version,
                                                prefix.nonce_));
  }

  return status;
}

// NewSequentialFile opens a file for sequential reading.
Status OpenSSLEnv::NewSequentialFile(const std::string& fname,
                                     std::unique_ptr<SequentialFile>* result,
                                     const EnvOptions& options) {
  result->reset();
  if (options.use_mmap_reads || options.use_direct_reads) {
    return Status::InvalidArgument();
  }

  // Open file using underlying Env implementation
  std::unique_ptr<SequentialFile> underlying;
  auto status = EnvWrapper::NewSequentialFile(fname, &underlying, options);
  if (status.ok()) {
    std::shared_ptr<const CTREncryptionProviderV2> provider;
    std::unique_ptr<BlockAccessCipherStream> stream;
    status = ReadSeqEncryptionPrefix<SequentialFile>(underlying.get(), provider,
                                                     stream);

    if (status.ok()) {
      if (provider) {
        (*result) = std::unique_ptr<SequentialFile>(new EncryptedSequentialFile(
            std::move(underlying), std::move(stream),
            provider->GetPrefixLength()));

      } else {
        // normal file, not encrypted
        // sequential file might not allow backing up to begining, close and
        // reopen
        underlying.reset(nullptr);
        status = EnvWrapper::NewSequentialFile(fname, result, options);
      }
    }
  }

  return status;
}

// NewRandomAccessFile opens a file for random read access.
Status OpenSSLEnv::NewRandomAccessFile(
    const std::string& fname, std::unique_ptr<RandomAccessFile>* result,
    const EnvOptions& options) {
  result->reset();
  if (options.use_mmap_reads || options.use_direct_reads) {
    return Status::InvalidArgument();
  }

  // Open file using underlying Env implementation
  std::unique_ptr<RandomAccessFile> underlying;
  auto status = EnvWrapper::NewRandomAccessFile(fname, &underlying, options);
  if (status.ok()) {
    std::shared_ptr<const CTREncryptionProviderV2> provider;
    std::unique_ptr<BlockAccessCipherStream> stream;
    status = ReadRandEncryptionPrefix<RandomAccessFile>(underlying.get(),
                                                        provider, stream);

    if (status.ok()) {
      if (provider) {
        (*result) =
            std::unique_ptr<RandomAccessFile>(new EncryptedRandomAccessFile(
                std::move(underlying), std::move(stream),
                provider->GetPrefixLength()));

      } else {
        // normal file, not encrypted
        (*result).reset(underlying.release());
      }
    }
  }
  return status;
}

// NewWritableFile opens a file for sequential writing.
Status OpenSSLEnv::NewWritableFile(const std::string& fname,
                                   std::unique_ptr<WritableFile>* result,
                                   const EnvOptions& options) {
  Status status;
  result->reset();

  if (!options.use_mmap_writes && !options.use_direct_writes) {
    // Open file using underlying Env implementation
    std::unique_ptr<WritableFile> underlying;
    status = EnvWrapper::NewWritableFile(fname, &underlying, options);

    if (status.ok()) {
      std::shared_ptr<const CTREncryptionProviderV2> provider;

      {
        ReadLock lock(&key_lock_);
        provider = encrypt_write_.second;
      }

      if (provider) {
        std::unique_ptr<BlockAccessCipherStream> stream;

        status = WriteSeqEncryptionPrefix(underlying.get(), provider, stream);

        if (status.ok()) {
          (*result) = std::unique_ptr<WritableFile>(new EncryptedWritableFileV2(
              std::move(underlying), std::move(stream),
              provider->GetPrefixLength()));
        }
      } else {
        (*result).reset(underlying.release());
      }
    }
  } else {
    status = Status::InvalidArgument();
  }

  return status;
}

// Create an object that writes to a new file with the specified
// name.  Deletes any existing file with the same name and creates a
// new file.  On success, stores a pointer to the new file in
// *result and returns OK.  On failure stores nullptr in *result and
// returns non-OK.
//
// The returned file will only be accessed by one thread at a time.
Status OpenSSLEnv::ReopenWritableFile(const std::string& fname,
                                      std::unique_ptr<WritableFile>* result,
                                      const EnvOptions& options) {
  Status status;
  result->reset();

  if (!options.use_mmap_writes && !options.use_direct_writes) {
    // Open file using underlying Env implementation
    std::unique_ptr<WritableFile> underlying;
    status = EnvWrapper::ReopenWritableFile(fname, &underlying, options);

    if (status.ok()) {
      std::shared_ptr<const CTREncryptionProviderV2> provider;

      {
        ReadLock lock(&key_lock_);
        provider = encrypt_write_.second;
      }

      if (provider) {
        std::unique_ptr<BlockAccessCipherStream> stream;

        status = WriteSeqEncryptionPrefix(underlying.get(), provider, stream);

        if (status.ok()) {
          (*result) = std::unique_ptr<WritableFile>(new EncryptedWritableFile(
              std::move(underlying), std::move(stream),
              provider->GetPrefixLength()));
        }
      } else {
        (*result).reset(underlying.release());
      }
    }
  } else {
    status = Status::InvalidArgument();
  }

  return status;
}

// Reuse an existing file by renaming it and opening it as writable.
Status OpenSSLEnv::ReuseWritableFile(const std::string& fname,
                                     const std::string& old_fname,
                                     std::unique_ptr<WritableFile>* result,
                                     const EnvOptions& options) {
  Status status;
  result->reset();

  if (!options.use_mmap_writes && !options.use_direct_writes) {
    // Open file using underlying Env implementation
    std::unique_ptr<WritableFile> underlying;
    status =
        EnvWrapper::ReuseWritableFile(fname, old_fname, &underlying, options);

    if (status.ok()) {
      std::shared_ptr<const CTREncryptionProviderV2> provider;

      {
        ReadLock lock(&key_lock_);
        provider = encrypt_write_.second;
      }

      if (provider) {
        std::unique_ptr<BlockAccessCipherStream> stream;

        status = WriteSeqEncryptionPrefix(underlying.get(), provider, stream);

        if (status.ok()) {
          (*result) = std::unique_ptr<WritableFile>(new EncryptedWritableFile(
              std::move(underlying), std::move(stream),
              provider->GetPrefixLength()));
        }
      } else {
        (*result).reset(underlying.release());
      }
    }
  } else {
    status = Status::InvalidArgument();
  }

  return status;
}

// Open `fname` for random read and write, if file doesn't exist the file
// will be created.  On success, stores a pointer to the new file in
// *result and returns OK.  On failure returns non-OK.
//
// The returned file will only be accessed by one thread at a time.
Status OpenSSLEnv::NewRandomRWFile(const std::string& fname,
                                   std::unique_ptr<RandomRWFile>* result,
                                   const EnvOptions& options) {
  Status status;
  result->reset();

  // Check file exists
  bool isNewFile = !FileExists(fname).ok();

  if (!options.use_mmap_writes && !options.use_mmap_reads &&
      !options.use_direct_writes && !options.use_direct_reads) {
    // Open file using underlying Env implementation
    std::unique_ptr<RandomRWFile> underlying;
    status = EnvWrapper::NewRandomRWFile(fname, &underlying, options);

    if (status.ok()) {
      std::shared_ptr<const CTREncryptionProviderV2> provider;
      std::unique_ptr<BlockAccessCipherStream> stream;

      if (!isNewFile) {
        // file exists, get existing crypto info
        status = ReadRandEncryptionPrefix<RandomRWFile>(underlying.get(),
                                                        provider, stream);
      } else {
        // new file
        {
          ReadLock lock(&key_lock_);
          provider = encrypt_write_.second;
        }

        if (provider) {
          status =
              WriteRandEncryptionPrefix(underlying.get(), provider, stream);
        }
      }

      // establish encrypt or not, finalize file object
      if (status.ok()) {
        if (provider) {
          (*result) = std::unique_ptr<RandomRWFile>(new EncryptedRandomRWFileV2(
              std::move(underlying), std::move(stream),
              provider->GetPrefixLength()));
        } else {
          (*result).reset(underlying.release());
        }
      }
    }
  } else {
    status = Status::InvalidArgument();
  }

  return status;
}

// Store in *result the attributes of the children of the specified directory.
// In case the implementation lists the directory prior to iterating the files
// and files are concurrently deleted, the deleted files will be omitted from
// result.
// The name attributes are relative to "dir".
// Original contents of *results are dropped.
// Returns OK if "dir" exists and "*result" contains its children.
//         NotFound if "dir" does not exist, the calling process does not have
//                  permission to access "dir", or if "dir" is invalid.
//         IOError if an IO Error was encountered
Status OpenSSLEnv::GetChildrenFileAttributes(
    const std::string& dir, std::vector<FileAttributes>* result) {
  auto status = EnvWrapper::GetChildrenFileAttributes(dir, result);
  if (status.ok()) {
    // this is slightly expensive, but fortunately not used heavily
    std::shared_ptr<const CTREncryptionProviderV2> provider;

    for (auto it = std::begin(*result); it != std::end(*result); ++it) {
      status = GetEncryptionProvider(it->name, provider);

      if (status.ok() && provider) {
        size_t prefixLength = provider->GetPrefixLength();

        if (prefixLength <= it->size_bytes) it->size_bytes -= prefixLength;
      }
    }
  }

  return status;
}

// Store the size of fname in *file_size.
Status OpenSSLEnv::GetFileSize(const std::string& fname, uint64_t* file_size) {
  Status status;
  status = EnvWrapper::GetFileSize(fname, file_size);

  if (status.ok()) {
    // this is slightly expensive, but fortunately not used heavily
    std::shared_ptr<const CTREncryptionProviderV2> provider;
    status = GetEncryptionProvider(fname, provider);
    if (status.ok() && provider) {
      size_t prefixLength = provider->GetPrefixLength();
      if (prefixLength <= *file_size) *file_size -= prefixLength;
    }
  }

  return status;
}

Status OpenSSLEnv::GetEncryptionProvider(
    const std::string& fname,
    std::shared_ptr<const CTREncryptionProviderV2>& provider) {
  std::unique_ptr<SequentialFile> underlying;
  EnvOptions options;
  Status status;

  provider.reset();
  status = Env::Default()->NewSequentialFile(fname, &underlying, options);

  if (status.ok()) {
    std::unique_ptr<BlockAccessCipherStream> stream;
    status =
        OpenSSLEnv::ReadSeqEncryptionPrefix(underlying.get(), provider, stream);
  }

  return status;
}

Env* OpenSSLEnv::Default() {
  // the rational for this routine is to help force the static
  //  loading of UnixLibCrypto before other routines start
  //  using the encryption code.
  static OpenSSLEnv default_env(Env::Default());
  return &default_env;
}

Env* OpenSSLEnv::Default(OpenSSLEnv::ReadKeys encrypt_read,
                         OpenSSLEnv::WriteKey encrypt_write) {
  OpenSSLEnv* default_env = (OpenSSLEnv*)Default();
  default_env->SetKeys(encrypt_read, encrypt_write);
  return default_env;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
#endif  // ROCKSDB_OPENSSL_AES_CTR
