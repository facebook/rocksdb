// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#ifndef ROCKSDB_LITE
#ifdef OPENSSL

#include "encryption/encryption.h"

#include <openssl/err.h>
#include <openssl/opensslv.h>
#include <openssl/rand.h>

#include <algorithm>
#include <limits>

#include "encryption/encryption.h"
#include "file/filename.h"
#include "port/likely.h"
#include "port/port.h"
#include "rocksdb/utilities/options_type.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
namespace encryption {

inline size_t KeySize(EncryptionMethod method) {
  switch (method) {
    case EncryptionMethod::kAES128_CTR:
      return 16;
    case EncryptionMethod::kAES192_CTR:
      return 24;
    case EncryptionMethod::kAES256_CTR:
      return 32;
    case EncryptionMethod::kSM4_CTR:
#if OPENSSL_VERSION_NUMBER >= 0x1010100fL && !defined(OPENSSL_NO_SM4)
      return 16;
#else
      return 0;
#endif
    default:
      return 0;
  };
}

size_t BlockSize(EncryptionMethod method) {
  switch (method) {
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
      return AES_BLOCK_SIZE;
    case EncryptionMethod::kSM4_CTR:
      // TODO: OpenSSL Lib does not export SM4_BLOCK_SIZE by now.
      // Need to use the macro exported from OpenSSL once it is available.
      // Ref:
      // https://github.com/openssl/openssl/blob/OpenSSL_1_1_1-stable/include/crypto/sm4.h#L24
#if OPENSSL_VERSION_NUMBER >= 0x1010100fL && !defined(OPENSSL_NO_SM4)
      return 16;
#else
      return 0;
#endif
    default:
      return 0;
  }
}

EncryptionMethod EncryptionMethodStringToEnum(const std::string& method) {
  if (!strcasecmp(method.c_str(), "AES128CTR")) {
    return ROCKSDB_NAMESPACE::encryption::EncryptionMethod::kAES128_CTR;
  } else if (!strcasecmp(method.c_str(), "AES192CTR")) {
    return ROCKSDB_NAMESPACE::encryption::EncryptionMethod::kAES192_CTR;
  } else if (!strcasecmp(method.c_str(), "AES256CTR")) {
    return ROCKSDB_NAMESPACE::encryption::EncryptionMethod::kAES256_CTR;
  } else if (!strcasecmp(method.c_str(), "SM4CTR")) {
    return ROCKSDB_NAMESPACE::encryption::EncryptionMethod::kSM4_CTR;
  }
  return ROCKSDB_NAMESPACE::encryption::EncryptionMethod::kUnknown;
}

const EVP_CIPHER* GetEVPCipher(EncryptionMethod method) {
  switch (method) {
    case EncryptionMethod::kAES128_CTR:
      return EVP_aes_128_ctr();
    case EncryptionMethod::kAES192_CTR:
      return EVP_aes_192_ctr();
    case EncryptionMethod::kAES256_CTR:
      return EVP_aes_256_ctr();
      //    case EncryptionMethod::AES128ECB:
      //      return EVP_aes_128_ecb();
      //    case EncryptionMethod::AES192ECB:
      //      return EVP_aes_192_ecb();
      //    case EncryptionMethod::AES256ECB:
      //      return EVP_aes_256_ecb();
    case EncryptionMethod::kSM4_CTR:
#if OPENSSL_VERSION_NUMBER < 0x1010100fL || defined(OPENSSL_NO_SM4)
      return Status::InvalidArgument(
          "Unsupport SM4 encryption method under OpenSSL version: " +
          std::string(OPENSSL_VERSION_TEXT));
#else
      // Openssl support SM4 after 1.1.1 release version.
      return EVP_sm4_ctr();
#endif
    default:
      return nullptr;
  }
}

std::string GetOpenSSLErrors() {
  std::ostringstream serr;
  unsigned long l;
  const char *file, *data, *func;
  int line, flags;

#if OPENSSL_VERSION_NUMBER >= 0x30000000
  l = ERR_peek_last_error_all(&file, &line, &func, &data, &flags);
#else
  l = ERR_peek_last_error_line_data(&file, &line, &data, &flags);
  func = ERR_func_error_string(l);
#endif

  if (l != 0) {
    serr << l << ":" << ERR_lib_error_string(l) << ":" << func << ":" << file
         << ":" << line;
    if ((flags & ERR_TXT_STRING) && data && *data) {
      serr << ":" << data;
    } else {
      serr << ":" << ERR_reason_error_string(l);
    }
  }
  return serr.str();
}

namespace {
const char* const kEncryptionHeaderMagic = "pegsenc";
const int kEncryptionHeaderMagicLength = 7;

uint64_t GetBigEndian64(const unsigned char* src) {
  if (port::kLittleEndian) {
    return (static_cast<uint64_t>(src[0]) << 56) +
           (static_cast<uint64_t>(src[1]) << 48) +
           (static_cast<uint64_t>(src[2]) << 40) +
           (static_cast<uint64_t>(src[3]) << 32) +
           (static_cast<uint64_t>(src[4]) << 24) +
           (static_cast<uint64_t>(src[5]) << 16) +
           (static_cast<uint64_t>(src[6]) << 8) +
           (static_cast<uint64_t>(src[7]));
  } else {
    return *(reinterpret_cast<const uint64_t*>(src));
  }
}

void PutBigEndian64(uint64_t value, unsigned char* dst) {
  if (port::kLittleEndian) {
    dst[0] = static_cast<unsigned char>((value >> 56) & 0xff);
    dst[1] = static_cast<unsigned char>((value >> 48) & 0xff);
    dst[2] = static_cast<unsigned char>((value >> 40) & 0xff);
    dst[3] = static_cast<unsigned char>((value >> 32) & 0xff);
    dst[4] = static_cast<unsigned char>((value >> 24) & 0xff);
    dst[5] = static_cast<unsigned char>((value >> 16) & 0xff);
    dst[6] = static_cast<unsigned char>((value >> 8) & 0xff);
    dst[7] = static_cast<unsigned char>(value & 0xff);
  } else {
    *(reinterpret_cast<uint64_t*>(dst)) = value;
  }
}

Status GenerateFileKey(EncryptionMethod method, char* file_key) {
  auto key_bytes = static_cast<int>(KeySize(method) / 8);
  OPENSSL_RET_NOT_OK(
      RAND_bytes(reinterpret_cast<unsigned char*>(file_key), key_bytes),
      "Failed to generate random key");
  return Status::OK();
}

// Use OpenSSL EVP API with CTR mode to encrypt and decrypt
// data, instead of using the CTR implementation provided by
// BlockAccessCipherStream. Benefits:
//
// 1. The EVP API automatically figure out if AES-NI can be enabled.
// 2. Keep the data format consistent with OpenSSL (e.g. how IV is interpreted
//    as block counter).
//
// References for the openssl EVP API:
// * man page: https://www.openssl.org/docs/man1.1.1/man3/EVP_EncryptUpdate.html
// * SO answer for random access: https://stackoverflow.com/a/57147140/11014942
// * https://medium.com/@amit.kulkarni/encrypting-decrypting-a-file-using-openssl-evp-b26e0e4d28d4
Status Cipher(const EncryptionMethod method, const std::string& key,
              const uint64_t initial_iv_high, const uint64_t initial_iv_low,
              uint64_t file_offset, char* data, size_t data_size,
              AESCTRCipherStream::EncryptType encrypt_type) {
#if OPENSSL_VERSION_NUMBER < 0x01000200f
  (void)file_offset;
  (void)data;
  (void)data_size;
  (void)encrypt_type;
  return Status::NotSupported("OpenSSL version < 1.0.2");
#else
  evp_ctx_unique_ptr ctx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);
  if (UNLIKELY(!ctx)) {
    return Status::IOError("Failed to create cipher context.");
  }

  const size_t kBlockSize = BlockSize(method);
  assert(kBlockSize > 0);

  uint64_t block_index = file_offset / kBlockSize;
  uint64_t block_offset = file_offset % kBlockSize;

  // In CTR mode, OpenSSL EVP API treat the IV as a 128-bit big-endien, and
  // increase it by 1 for each block.
  //
  // In case of unsigned integer overflow in c++, the result is moduloed by
  // range, means only the lowest bits of the result will be kept.
  // http://www.cplusplus.com/articles/DE18T05o/
  uint64_t iv_high = initial_iv_high;
  uint64_t iv_low = initial_iv_low + block_index;
  if (std::numeric_limits<uint64_t>::max() - block_index < initial_iv_low) {
    iv_high++;
  }
  unsigned char iv[kBlockSize];
  PutBigEndian64(iv_high, iv);
  PutBigEndian64(iv_low, iv + sizeof(uint64_t));

  OPENSSL_RET_NOT_OK(
      EVP_CipherInit(ctx.get(), GetEVPCipher(method),
                     reinterpret_cast<const unsigned char*>(key.data()), iv,
                     static_cast<int>(encrypt_type)),
      "Failed to init cipher.");

  // Disable padding. After disabling padding, data size should always be
  // multiply of kBlockSize.
  OPENSSL_RET_NOT_OK(EVP_CIPHER_CTX_set_padding(ctx.get(), 0),
                     "Failed to disable padding for cipher context.");

  uint64_t data_offset = 0;
  size_t remaining_data_size = data_size;
  int output_size = 0;
  unsigned char partial_block[kBlockSize];

  // In the following we assume EVP_CipherUpdate allow in and out buffer are
  // the same, to save one memcpy. This is not specified in official man page.

  // Handle partial block at the beginning. The partial block is copied to
  // buffer to fake a full block.
  if (block_offset > 0) {
    size_t partial_block_size =
        std::min<size_t>(kBlockSize - block_offset, remaining_data_size);
    memcpy(partial_block + block_offset, data, partial_block_size);
    OPENSSL_RET_NOT_OK(
        EVP_CipherUpdate(ctx.get(), partial_block, &output_size, partial_block,
                         static_cast<int>(kBlockSize)),
        "Crypter failed for first block, offset " +
            std::to_string(file_offset));
    if (UNLIKELY(output_size != static_cast<int>(kBlockSize))) {
      return Status::IOError(
          "Unexpected crypter output size for first block, expected " +
          std::to_string(kBlockSize) + " vs actual " +
          std::to_string(output_size));
    }
    memcpy(data, partial_block + block_offset, partial_block_size);
    data_offset += partial_block_size;
    remaining_data_size -= partial_block_size;
  }

  // Handle full blocks in the middle.
  if (remaining_data_size >= kBlockSize) {
    size_t actual_data_size =
        remaining_data_size - remaining_data_size % kBlockSize;
    unsigned char* full_blocks =
        reinterpret_cast<unsigned char*>(data) + data_offset;
    OPENSSL_RET_NOT_OK(
        EVP_CipherUpdate(ctx.get(), full_blocks, &output_size, full_blocks,
                         static_cast<int>(actual_data_size)),
        "Crypter failed at offset " +
            std::to_string(file_offset + data_offset));
    if (UNLIKELY(output_size != static_cast<int>(actual_data_size))) {
      return Status::IOError("Unexpected crypter output size, expected " +
                             std::to_string(actual_data_size) + " vs actual " +
                             std::to_string(output_size));
    }
    data_offset += actual_data_size;
    remaining_data_size -= actual_data_size;
  }

  // TODO(yingchun): Can we remove the end partial block handling if handling a
  //  suitable adjusted partial block at the beginning and full blocks in the
  //  middle?
  // Handle partial block at the end. The partial block is copied to buffer to
  // fake a full block.
  if (remaining_data_size > 0) {
    assert(remaining_data_size < kBlockSize);
    memcpy(partial_block, data + data_offset, remaining_data_size);
    OPENSSL_RET_NOT_OK(
        EVP_CipherUpdate(ctx.get(), partial_block, &output_size, partial_block,
                         static_cast<int>(kBlockSize)),
        "Crypter failed for last block, offset " +
            std::to_string(file_offset + data_offset));
    if (UNLIKELY(output_size != static_cast<int>(kBlockSize))) {
      return Status::IOError(
          "Unexpected crypter output size for last block, expected " +
          std::to_string(kBlockSize) + " vs actual " +
          std::to_string(output_size));
    }
    memcpy(data + data_offset, partial_block, remaining_data_size);
  }

  // Since padding is disabled, and the cipher flow always passes a multiply
  // of block size data while each EVP_CipherUpdate, there is no need to call
  // EVP_CipherFinal_ex to finish the last block cipher.
  // Reference to the implement of EVP_CipherFinal_ex:
  // https://github.com/openssl/openssl/blob/OpenSSL_1_1_1-stable/crypto/evp/evp_enc.c#L219
  return Status::OK();
#endif
}
}  // anonymous namespace

size_t AESCTRCipherStream::BlockSize() {
  return ROCKSDB_NAMESPACE::encryption::BlockSize(method_);
}

Status AESCTRCipherStream::Cipher(uint64_t file_offset, char* data,
                                  size_t data_size, EncryptType encrypt_type) {
  return ROCKSDB_NAMESPACE::encryption::Cipher(
      method_, file_key_, initial_iv_high_, initial_iv_low_, file_offset, data,
      data_size, encrypt_type);
}

bool AESEncryptionProvider::IsInstanceOf(const std::string& name) const {
  return EncryptionProvider::IsInstanceOf(name);
}

Status AESEncryptionProvider::ReadEncryptionHeader(
    Slice prefix, FileEncryptionInfo* file_info) const {
  // 1. Check the encryption header magic.
  if (UNLIKELY(!prefix.starts_with(kEncryptionHeaderMagic))) {
    return Status::Corruption("Invalid encryption header");
  }

  // 2. Read the encryption method.
  auto method = EncryptionMethod(prefix[kEncryptionHeaderMagicLength]);
  size_t key_size = KeySize(method);
  if (UNLIKELY(key_size == 0)) {
    return Status::Corruption("Unknown encryption algorithm " +
                              std::to_string(static_cast<char>(method)));
  }

  // 3. Read the encrypted file key and decrypt it.
  char encrypted_file_key[key_size];
  memcpy(encrypted_file_key, prefix.data() + kEncryptionHeaderMagicLength + 1,
         key_size);
  Status s = Cipher(method_, instance_key_, 0, 0, 0, encrypted_file_key,
                    key_size, AESCTRCipherStream::EncryptType::kDecrypt);
  if (UNLIKELY(!s.ok())) {
    return s;
  }

  // 4. Fill the FileEncryptionInfo.
  file_info->method = method;
  file_info->key.assign(encrypted_file_key, key_size);
  // TODO(yingchun): write a real IV to header_buf.
  static std::string fake_iv(AES_BLOCK_SIZE, '0');
  file_info->iv = fake_iv;
  return Status::OK();
}

Status AESEncryptionProvider::WriteEncryptionHeader(char* header_buf) const {
  auto method = EncryptionMethod(method_);
  size_t key_size = KeySize(method_);
  assert(key_size != 0);

  // 1. Write the encryption header magic.
  size_t offset = 0;
  memcpy(header_buf, kEncryptionHeaderMagic, kEncryptionHeaderMagicLength);
  offset += kEncryptionHeaderMagicLength;

  // 2. Write the encryption method.
  header_buf[offset] = static_cast<char>(method_);
  offset += 1;

  // 3. Generate a file key, encrypt and write it.
  char file_key[key_size];
  Status s = GenerateFileKey(method, file_key);
  if (UNLIKELY(!s.ok())) {
    return s;
  }
  s = Cipher(method_, instance_key_, 0, 0, 0, file_key, key_size,
             AESCTRCipherStream::EncryptType::kEncrypt);
  if (UNLIKELY(!s.ok())) {
    return s;
  }
  memcpy(header_buf + offset, file_key, key_size);
  offset += key_size;

  // 4. Pad with 0.
  memset(header_buf + offset, 0, (64 - offset));

  // TODO(yingchun): write IV to header_buf.
  return Status::OK();
}

Status AESEncryptionProvider::CreateNewPrefix(const std::string& fname,
                                              char* prefix,
                                              size_t prefix_length) const {
  if (prefix_length != GetPrefixLength()) {
    return IOStatus::Corruption("CreateNewPrefix with invalid prefix length: " +
                                std::to_string(prefix_length) + " for " +
                                fname);
  }
  auto s = WriteEncryptionHeader(prefix);
  if (!s.ok()) {
    s = Status::CopyAppendMessage(s, " in ", fname);
    return s;
  }
  return Status::OK();
}

Status AESEncryptionProvider::CreateCipherStream(
    const std::string& fname, const EnvOptions& /*options*/, Slice& prefix,
    std::unique_ptr<BlockAccessCipherStream>* result) {
  assert(result != nullptr);
  FileEncryptionInfo file_info;
  Status s = ReadEncryptionHeader(prefix, &file_info);
  if (!s.ok()) {
    s = Status::CopyAppendMessage(s, " in ", fname);
    return s;
  }
  std::unique_ptr<AESCTRCipherStream> cipher_stream;
  s = NewAESCTRCipherStream(file_info.method, file_info.key, file_info.iv,
                            &cipher_stream);
  if (!s.ok()) {
    s = Status::CopyAppendMessage(s, " in ", fname);
    return s;
  }
  *result = std::move(cipher_stream);
  return Status::OK();
}

Status NewAESCTRCipherStream(EncryptionMethod method,
                             const std::string& file_key,
                             const std::string& file_key_iv,
                             std::unique_ptr<AESCTRCipherStream>* result) {
  assert(result != nullptr);
  if (file_key.size() != KeySize(method)) {
    return Status::InvalidArgument(
        "Encryption file_key size mismatch. " +
        std::to_string(file_key.size()) + "(actual) vs. " +
        std::to_string(KeySize(method)) + "(expected).");
  }
  // TODO(yingchun): check the correction of the fixed length block size
  if (file_key_iv.size() != AES_BLOCK_SIZE) {
    return Status::InvalidArgument(
        "file_key_iv size not equal to block cipher block size: " +
        std::to_string(file_key_iv.size()) + "(actual) vs. " +
        std::to_string(AES_BLOCK_SIZE) + "(expected).");
  }
  uint64_t iv_high = GetBigEndian64(
      reinterpret_cast<const unsigned char*>(file_key_iv.data()));
  uint64_t iv_low = GetBigEndian64(reinterpret_cast<const unsigned char*>(
      file_key_iv.data() + sizeof(uint64_t)));
  result->reset(new AESCTRCipherStream(method, file_key, iv_high, iv_low));
  return Status::OK();
}

}  // namespace encryption
}  // namespace ROCKSDB_NAMESPACE

#endif  // OPENSSL
#endif  // !ROCKSDB_LITE
