//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  Copyright (c) 2020 Intel Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/ippcp_encryption_provider.h"

#ifdef IPPCP

#include <emmintrin.h>
#include <ippcp.h>

#include "rocksdb/utilities/object_registry.h"

#endif  // IPPCP

#include "util/string_util.h"

#endif

namespace ROCKSDB_NAMESPACE {

#ifndef ROCKSDB_LITE

#ifdef IPPCP

extern "C" FactoryFunc<EncryptionProvider> ippcp_reg;

FactoryFunc<EncryptionProvider> ippcp_reg =
    ObjectLibrary::Default()->Register<EncryptionProvider>(
        IppcpEncryptionProvider::kName(),
        [](const std::string& /* uri */, std::unique_ptr<EncryptionProvider>* f,
           std::string* /* errmsg */) {
          *f = IppcpEncryptionProvider::CreateProvider();
          return f->get();
        });

// IppcpCipherStream implements BlockAccessCipherStream using AES block
// cipher and a CTR mode of operation.
//
// Since ipp-crypto can handle block sizes larger than kBlockSize (16 bytes for
// AES) by chopping them internally into KBlockSize bytes, there is no need to
// support the EncryptBlock and DecryptBlock member functions (and they will
// never be called).
//
// See https://github.com/intel/ipp-crypto#documentation
class IppcpCipherStream : public BlockAccessCipherStream {
 public:
  static constexpr size_t kBlockSize = 16;    // in bytes
  static constexpr size_t kCounterLen = 128;  // in bits

  IppcpCipherStream(IppsAESSpec* aes_ctx, const char* init_vector);

  virtual Status Encrypt(uint64_t fileOffset, char* data,
                         size_t dataSize) override;
  virtual Status Decrypt(uint64_t fileOffset, char* data,
                         size_t dataSize) override;
  virtual size_t BlockSize() override { return kBlockSize; }

 protected:
  // These functions are not needed and will never be called!
  virtual void AllocateScratch(std::string&) override {}
  virtual Status EncryptBlock(uint64_t, char*, char*) override {
    return Status::NotSupported("Operation not supported.");
  }
  virtual Status DecryptBlock(uint64_t, char*, char*) override {
    return Status::NotSupported("Operation not supported.");
  }

 private:
  IppsAESSpec* aes_ctx_;
  __m128i init_vector_;
};

IppcpCipherStream::IppcpCipherStream(IppsAESSpec* aes_ctx,
                                     const char* init_vector)
    : aes_ctx_(aes_ctx) {
  init_vector_ = _mm_loadu_si128((__m128i*)init_vector);
}

Status IppcpCipherStream::Encrypt(uint64_t fileOffset, char* data,
                                  size_t dataSize) {
  if (dataSize == 0) return Status::OK();

  size_t index = fileOffset / kBlockSize;
  size_t offset = fileOffset % kBlockSize;

  Ipp8u ctr_block[kBlockSize];

  // evaluate the counter block from the block index
  __m128i counter = _mm_add_epi64(init_vector_, _mm_cvtsi64_si128(index));
  Ipp8u* ptr_counter = (Ipp8u*)&counter;
  for (size_t i = 0; i < kBlockSize; ++i)
    ctr_block[i] = ptr_counter[kBlockSize - 1 - i];

  IppStatus ipp_status = ippStsNoErr;

  // if we are block-aligned we can encrypt the entire dataset at once. If not,
  // we need to treat the last remaining (non-aligned) block separately.
  //
  // See:
  // https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38a.pdf
  if (offset == 0) {
    ipp_status = ippsAESEncryptCTR((Ipp8u*)(data), (Ipp8u*)data, dataSize,
                                   aes_ctx_, ctr_block, kCounterLen);
  } else {
    Ipp8u zero_block[kBlockSize]{0};
    ipp_status = ippsAESEncryptCTR(zero_block, zero_block, kBlockSize, aes_ctx_,
                                   ctr_block, kCounterLen);
    if (ipp_status != ippStsNoErr)
      return Status::Aborted(ippcpGetStatusString(ipp_status));

    size_t n = std::min(kBlockSize - offset, dataSize);
    for (size_t i = 0; i < n; ++i) data[i] ^= zero_block[offset + i];
    memset(zero_block, 0, kBlockSize);

    n = kBlockSize - offset;
    if (dataSize > n) {
      Ipp8u* ptr = (Ipp8u*)(data + n);
      ipp_status = ippsAESEncryptCTR(ptr, ptr, dataSize - n, aes_ctx_,
                                     ctr_block, kCounterLen);
    }
  }

  if (ipp_status == ippStsNoErr) return Status::OK();

  return Status::Aborted(ippcpGetStatusString(ipp_status));
}

Status IppcpCipherStream::Decrypt(uint64_t fileOffset, char* data,
                                  size_t dataSize) {
  // Decryption is implemented as encryption in CTR mode of operation
  return Encrypt(fileOffset, data, dataSize);
}

#endif  // IPPCP

std::unique_ptr<EncryptionProvider> IppcpEncryptionProvider::CreateProvider() {
#ifdef IPPCP
  return std::unique_ptr<EncryptionProvider>(new IppcpEncryptionProvider);
#else
  return nullptr;
#endif  // IPPCP
}

Status IppcpEncryptionProvider::AddCipher(const std::string& /*descriptor*/,
                                          const char* cipher, size_t len,
                                          bool /*for_write*/) {
#ifdef IPPCP
  // We currently don't support more than one encryption key
  if (aes_ctx_ != nullptr) {
    return Status::InvalidArgument("Multiple encryption keys not supported.");
  }

  // AES supports key sizes of only 16, 24, or 32 bytes
  if (len != 16 && len != 24 && len != 32) {
    return Status::InvalidArgument("Invalid key size in provider.");
  }

  // len is in bytes
  switch (len) {
    case 16:
      key_size_ = KeySize::AES_128;
      break;
    case 24:
      key_size_ = KeySize::AES_192;
      break;
    case 32:
      key_size_ = KeySize::AES_256;
      break;
  }

  // get size for context
  IppStatus ipp_status = ippsAESGetSize(&ctx_size_);
  if (ipp_status != ippStsNoErr) {
    return Status::Aborted("Failed to create provider.");
  }

  // allocate memory for context
  aes_ctx_ = new Ipp8u[ctx_size_];
  assert(aes_ctx_ != nullptr);

  // initialize context
  const Ipp8u* key = (const Ipp8u*)(cipher);
  ipp_status = ippsAESInit(key, static_cast<int>(key_size_),
                           static_cast<IppsAESSpec*>(aes_ctx_), ctx_size_);

  if (ipp_status != ippStsNoErr) {
    // clean up context and abort!
    ippsAESInit(0, static_cast<int>(key_size_),
                static_cast<IppsAESSpec*>(aes_ctx_), ctx_size_);
    delete[] static_cast<Ipp8u*>(aes_ctx_);
    return Status::Aborted("Failed to create provider.");
  }
#else
  (void)cipher;
  (void)len;
#endif  // IPPCP
  return Status::OK();
}

Status IppcpEncryptionProvider::CreateNewPrefix(const std::string& /*fname*/,
                                                char* prefix,
                                                size_t prefixLength) const {
#ifdef IPPCP
  IppStatus ipp_status;
  Ipp32u rnd;
  const size_t rnd_size = sizeof(Ipp32u);
  assert(prefixLength % rnd_size == 0);
  for (size_t i = 0; i < prefixLength; i += rnd_size) {
    // generate a cryptographically secured random number
    ipp_status = ippsPRNGenRDRAND(&rnd, rnd_size << 3, nullptr);
    if (ipp_status != ippStsNoErr)
      return Status::Aborted(ippcpGetStatusString(ipp_status));
    memcpy(prefix + i, &rnd, rnd_size);
  }
  IppcpCipherStream cs(static_cast<IppsAESSpec*>(aes_ctx_), prefix);
  return cs.Encrypt(0, prefix + IppcpCipherStream::kBlockSize,
                    prefixLength - IppcpCipherStream::kBlockSize);
#else
  (void)prefix;
  (void)prefixLength;
  return Status::OK();
#endif  // IPPCP
}

Status IppcpEncryptionProvider::CreateCipherStream(
    const std::string& /*fname*/, const EnvOptions& /*options*/, Slice& prefix,
    std::unique_ptr<BlockAccessCipherStream>* result) {
#ifdef IPPCP
  assert(result != nullptr);
  assert(prefix.size() >= IppcpCipherStream::kBlockSize);
  result->reset(new IppcpCipherStream(static_cast<IppsAESSpec*>(aes_ctx_),
                                      prefix.data()));
  Status ipp_status = (*result)->Decrypt(
      0, (char*)prefix.data() + IppcpCipherStream::kBlockSize,
      prefix.size() - IppcpCipherStream::kBlockSize);
  return ipp_status;
#else
  (void)prefix;
  (void)result;
  return Status::OK();
#endif  // IPPCP
}

IppcpEncryptionProvider::~IppcpEncryptionProvider() {
#ifdef IPPCP
  ippsAESInit(0, static_cast<int>(key_size_),
              static_cast<IppsAESSpec*>(aes_ctx_), ctx_size_);
  delete[] static_cast<Ipp8u*>(aes_ctx_);
#endif  // IPPCP
}

#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE
