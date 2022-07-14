// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#ifndef ROCKSDB_LITE
#ifdef OPENSSL

#include "encryption/encryption.h"

#include <openssl/opensslv.h>

#include <algorithm>
#include <limits>

#include "file/filename.h"
#include "port/port.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
namespace encryption {

namespace {
uint64_t GetBigEndian64(const unsigned char* buf) {
  if (port::kLittleEndian) {
    return (static_cast<uint64_t>(buf[0]) << 56) +
           (static_cast<uint64_t>(buf[1]) << 48) +
           (static_cast<uint64_t>(buf[2]) << 40) +
           (static_cast<uint64_t>(buf[3]) << 32) +
           (static_cast<uint64_t>(buf[4]) << 24) +
           (static_cast<uint64_t>(buf[5]) << 16) +
           (static_cast<uint64_t>(buf[6]) << 8) +
           (static_cast<uint64_t>(buf[7]));
  } else {
    return *(reinterpret_cast<const uint64_t*>(buf));
  }
}

void PutBigEndian64(uint64_t value, unsigned char* buf) {
  if (port::kLittleEndian) {
    buf[0] = static_cast<unsigned char>((value >> 56) & 0xff);
    buf[1] = static_cast<unsigned char>((value >> 48) & 0xff);
    buf[2] = static_cast<unsigned char>((value >> 40) & 0xff);
    buf[3] = static_cast<unsigned char>((value >> 32) & 0xff);
    buf[4] = static_cast<unsigned char>((value >> 24) & 0xff);
    buf[5] = static_cast<unsigned char>((value >> 16) & 0xff);
    buf[6] = static_cast<unsigned char>((value >> 8) & 0xff);
    buf[7] = static_cast<unsigned char>(value & 0xff);
  } else {
    *(reinterpret_cast<uint64_t*>(buf)) = value;
  }
}
}  // anonymous namespace

// AESCTRCipherStream use OpenSSL EVP API with CTR mode to encrypt and decrypt
// data, instead of using the CTR implementation provided by
// BlockAccessCipherStream. Benefits:
//
// 1. The EVP API automatically figure out if AES-NI can be enabled.
// 2. Keep the data format consistent with OpenSSL (e.g. how IV is interpreted
// as block counter).
//
// References for the openssl EVP API:
// * man page: https://www.openssl.org/docs/man1.1.1/man3/EVP_EncryptUpdate.html
// * SO answer for random access: https://stackoverflow.com/a/57147140/11014942
// *
// https://medium.com/@amit.kulkarni/encrypting-decrypting-a-file-using-openssl-evp-b26e0e4d28d4
Status AESCTRCipherStream::Cipher(uint64_t file_offset, char* data,
                                  size_t data_size, bool is_encrypt) {
#if OPENSSL_VERSION_NUMBER < 0x01000200f
  (void)file_offset;
  (void)data;
  (void)data_size;
  (void)is_encrypt;
  return Status::NotSupported("OpenSSL version < 1.0.2");
#else
  int ret = 1;
  EVP_CIPHER_CTX* ctx = nullptr;
  InitCipherContext(ctx);
  if (ctx == nullptr) {
    return Status::IOError("Failed to create cipher context.");
  }

  const size_t block_size = BlockSize();

  uint64_t block_index = file_offset / block_size;
  uint64_t block_offset = file_offset % block_size;

  // In CTR mode, OpenSSL EVP API treat the IV as a 128-bit big-endien, and
  // increase it by 1 for each block.
  //
  // In case of unsigned integer overflow in c++, the result is moduloed by
  // range, means only the lowest bits of the result will be kept.
  // http://www.cplusplus.com/articles/DE18T05o/
  uint64_t iv_high = initial_iv_high_;
  uint64_t iv_low = initial_iv_low_ + block_index;
  if (std::numeric_limits<uint64_t>::max() - block_index < initial_iv_low_) {
    iv_high++;
  }
  unsigned char iv[block_size];
  PutBigEndian64(iv_high, iv);
  PutBigEndian64(iv_low, iv + sizeof(uint64_t));

  ret = EVP_CipherInit(ctx, cipher_,
                       reinterpret_cast<const unsigned char*>(key_.data()), iv,
                       (is_encrypt ? 1 : 0));
  if (ret != 1) {
    return Status::IOError("Failed to init cipher.");
  }

  // Disable padding. After disabling padding, data size should always be
  // multiply of block size.
  ret = EVP_CIPHER_CTX_set_padding(ctx, 0);
  if (ret != 1) {
    FreeCipherContext(ctx);
    return Status::IOError("Failed to disable padding for cipher context.");
  }

  uint64_t data_offset = 0;
  size_t remaining_data_size = data_size;
  int output_size = 0;
  unsigned char partial_block[block_size];

  // In the following we assume EVP_CipherUpdate allow in and out buffer are
  // the same, to save one memcpy. This is not specified in official man page.

  // Handle partial block at the beginning. The parital block is copied to
  // buffer to fake a full block.
  if (block_offset > 0) {
    size_t partial_block_size =
        std::min<size_t>(block_size - block_offset, remaining_data_size);
    memcpy(partial_block + block_offset, data, partial_block_size);
    ret = EVP_CipherUpdate(ctx, partial_block, &output_size, partial_block,
                           static_cast<int>(block_size));
    if (ret != 1) {
      FreeCipherContext(ctx);
      return Status::IOError("Crypter failed for first block, offset " +
                             ToString(file_offset));
    }
    if (output_size != static_cast<int>(block_size)) {
      FreeCipherContext(ctx);
      return Status::IOError(
          "Unexpected crypter output size for first block, expected " +
          ToString(block_size) + " vs actual " + ToString(output_size));
    }
    memcpy(data, partial_block + block_offset, partial_block_size);
    data_offset += partial_block_size;
    remaining_data_size -= partial_block_size;
  }

  // Handle full blocks in the middle.
  if (remaining_data_size >= block_size) {
    size_t actual_data_size =
        remaining_data_size - remaining_data_size % block_size;
    unsigned char* full_blocks =
        reinterpret_cast<unsigned char*>(data) + data_offset;
    ret = EVP_CipherUpdate(ctx, full_blocks, &output_size, full_blocks,
                           static_cast<int>(actual_data_size));
    if (ret != 1) {
      FreeCipherContext(ctx);
      return Status::IOError("Crypter failed at offset " +
                             ToString(file_offset + data_offset));
    }
    if (output_size != static_cast<int>(actual_data_size)) {
      FreeCipherContext(ctx);
      return Status::IOError("Unexpected crypter output size, expected " +
                             ToString(actual_data_size) + " vs actual " +
                             ToString(output_size));
    }
    data_offset += actual_data_size;
    remaining_data_size -= actual_data_size;
  }

  // Handle partial block at the end. The parital block is copied to buffer to
  // fake a full block.
  if (remaining_data_size > 0) {
    assert(remaining_data_size < block_size);
    memcpy(partial_block, data + data_offset, remaining_data_size);
    ret = EVP_CipherUpdate(ctx, partial_block, &output_size, partial_block,
                           static_cast<int>(block_size));
    if (ret != 1) {
      FreeCipherContext(ctx);
      return Status::IOError("Crypter failed for last block, offset " +
                             ToString(file_offset + data_offset));
    }
    if (output_size != static_cast<int>(block_size)) {
      FreeCipherContext(ctx);
      return Status::IOError(
          "Unexpected crypter output size for last block, expected " +
          ToString(block_size) + " vs actual " + ToString(output_size));
    }
    memcpy(data + data_offset, partial_block, remaining_data_size);
  }

  // Since padding is disabled, and the cipher flow always passes a multiply
  // of block size data while each EVP_CipherUpdate, there is no need to call
  // EVP_CipherFinal_ex to finish the last block cipher.
  // Reference to the implement of EVP_CipherFinal_ex:
  // https://github.com/openssl/openssl/blob/OpenSSL_1_1_1-stable/crypto/evp/evp_enc.c#L219
  FreeCipherContext(ctx);
  return Status::OK();
#endif
}

Status NewAESCTRCipherStream(EncryptionMethod method, const std::string& key,
                             const std::string& iv,
                             std::unique_ptr<AESCTRCipherStream>* result) {
  assert(result != nullptr);
  const EVP_CIPHER* cipher = nullptr;
  switch (method) {
    case EncryptionMethod::kAES128_CTR:
      cipher = EVP_aes_128_ctr();
      break;
    case EncryptionMethod::kAES192_CTR:
      cipher = EVP_aes_192_ctr();
      break;
    case EncryptionMethod::kAES256_CTR:
      cipher = EVP_aes_256_ctr();
      break;
    case EncryptionMethod::kSM4_CTR:
#if OPENSSL_VERSION_NUMBER < 0x1010100fL || defined(OPENSSL_NO_SM4)
      return Status::InvalidArgument(
          "Unsupport SM4 encryption method under OpenSSL version: " +
          std::string(OPENSSL_VERSION_TEXT));
#else
      // Openssl support SM4 after 1.1.1 release version.
      cipher = EVP_sm4_ctr();
      break;
#endif
    default:
      return Status::InvalidArgument("Unsupported encryption method: " +
                                     ToString(static_cast<int>(method)));
  }
  if (key.size() != KeySize(method)) {
    return Status::InvalidArgument("Encryption key size mismatch. " +
                                   ToString(key.size()) + "(actual) vs. " +
                                   ToString(KeySize(method)) + "(expected).");
  }
  if (iv.size() != AES_BLOCK_SIZE) {
    return Status::InvalidArgument(
        "iv size not equal to block cipher block size: " + ToString(iv.size()) +
        "(actual) vs. " + ToString(AES_BLOCK_SIZE) + "(expected).");
  }
  Slice iv_slice(iv);
  uint64_t iv_high =
      GetBigEndian64(reinterpret_cast<const unsigned char*>(iv.data()));
  uint64_t iv_low = GetBigEndian64(
      reinterpret_cast<const unsigned char*>(iv.data() + sizeof(uint64_t)));
  result->reset(new AESCTRCipherStream(cipher, key, iv_high, iv_low));
  return Status::OK();
}

Status AESEncryptionProvider::CreateCipherStream(
    const std::string& fname, const EnvOptions& /*options*/, Slice& /*prefix*/,
    std::unique_ptr<BlockAccessCipherStream>* result) {
  assert(result != nullptr);
  FileEncryptionInfo file_info;
  Status s = key_manager_->GetFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<AESCTRCipherStream> cipher_stream;
  s = NewAESCTRCipherStream(file_info.method, file_info.key, file_info.iv,
                            &cipher_stream);
  if (!s.ok()) {
    return s;
  }
  *result = std::move(cipher_stream);
  return Status::OK();
}

KeyManagedEncryptedEnv::KeyManagedEncryptedEnv(
    Env* base_env, std::shared_ptr<KeyManager>& key_manager,
    std::shared_ptr<AESEncryptionProvider>& provider,
    std::unique_ptr<Env>&& encrypted_env)
    : EnvWrapper(base_env),
      key_manager_(key_manager),
      provider_(provider),
      encrypted_env_(std::move(encrypted_env)) {}

KeyManagedEncryptedEnv::~KeyManagedEncryptedEnv() = default;

Status KeyManagedEncryptedEnv::NewSequentialFile(
    const std::string& fname, std::unique_ptr<SequentialFile>* result,
    const EnvOptions& options) {
  FileEncryptionInfo file_info;
  Status s = key_manager_->GetFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      s = target()->NewSequentialFile(fname, result, options);
      break;
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
    case EncryptionMethod::kSM4_CTR:
      s = encrypted_env_->NewSequentialFile(fname, result, options);
      // Hack: when upgrading from TiKV <= v5.0.0-rc, the old current
      // file is encrypted but it could be replaced with a plaintext
      // current file. The operation below guarantee that the current
      // file is read correctly.
      if (s.ok() && IsCurrentFile(fname)) {
        if (!IsValidCurrentFile(std::move(*result))) {
          s = target()->NewSequentialFile(fname, result, options);
        } else {
          s = encrypted_env_->NewSequentialFile(fname, result, options);
        }
      }
      break;
    default:
      s = Status::InvalidArgument(
          "Unsupported encryption method: " +
          ROCKSDB_NAMESPACE::ToString(static_cast<int>(file_info.method)));
  }
  return s;
}

Status KeyManagedEncryptedEnv::NewRandomAccessFile(
    const std::string& fname, std::unique_ptr<RandomAccessFile>* result,
    const EnvOptions& options) {
  FileEncryptionInfo file_info;
  Status s = key_manager_->GetFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      s = target()->NewRandomAccessFile(fname, result, options);
      break;
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
    case EncryptionMethod::kSM4_CTR:
      s = encrypted_env_->NewRandomAccessFile(fname, result, options);
      break;
    default:
      s = Status::InvalidArgument(
          "Unsupported encryption method: " +
          ROCKSDB_NAMESPACE::ToString(static_cast<int>(file_info.method)));
  }
  return s;
}

Status KeyManagedEncryptedEnv::NewWritableFile(
    const std::string& fname, std::unique_ptr<WritableFile>* result,
    const EnvOptions& options) {
  FileEncryptionInfo file_info;
  Status s;
  bool skipped = IsCurrentFile(fname);
  TEST_SYNC_POINT_CALLBACK("KeyManagedEncryptedEnv::NewWritableFile", &skipped);
  if (!skipped) {
    s = key_manager_->NewFile(fname, &file_info);
    if (!s.ok()) {
      return s;
    }
  } else {
    file_info.method = EncryptionMethod::kPlaintext;
  }

  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      s = target()->NewWritableFile(fname, result, options);
      break;
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
    case EncryptionMethod::kSM4_CTR:
      s = encrypted_env_->NewWritableFile(fname, result, options);
      break;
    default:
      s = Status::InvalidArgument(
          "Unsupported encryption method: " +
          ROCKSDB_NAMESPACE::ToString(static_cast<int>(file_info.method)));
  }
  if (!s.ok() && !skipped) {
    // Ignore error
    key_manager_->DeleteFile(fname);
  }
  return s;
}

Status KeyManagedEncryptedEnv::ReopenWritableFile(
    const std::string& fname, std::unique_ptr<WritableFile>* result,
    const EnvOptions& options) {
  FileEncryptionInfo file_info;
  Status s = key_manager_->GetFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      s = target()->ReopenWritableFile(fname, result, options);
      break;
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
    case EncryptionMethod::kSM4_CTR:
      s = encrypted_env_->ReopenWritableFile(fname, result, options);
      break;
    default:
      s = Status::InvalidArgument(
          "Unsupported encryption method: " +
          ROCKSDB_NAMESPACE::ToString(static_cast<int>(file_info.method)));
  }
  return s;
}

Status KeyManagedEncryptedEnv::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    std::unique_ptr<WritableFile>* result, const EnvOptions& options) {
  FileEncryptionInfo file_info;
  // ReuseWritableFile is only used in the context of rotating WAL file and
  // reuse them. Old content is discardable and new WAL records are to
  // overwrite the file. So NewFile() should be called.
  Status s = key_manager_->NewFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      s = target()->ReuseWritableFile(fname, old_fname, result, options);
      break;
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
    case EncryptionMethod::kSM4_CTR:
      s = encrypted_env_->ReuseWritableFile(fname, old_fname, result, options);
      break;
    default:
      s = Status::InvalidArgument(
          "Unsupported encryption method: " +
          ROCKSDB_NAMESPACE::ToString(static_cast<int>(file_info.method)));
  }
  if (!s.ok()) {
    return s;
  }
  s = key_manager_->LinkFile(old_fname, fname);
  if (!s.ok()) {
    return s;
  }
  s = key_manager_->DeleteFile(old_fname);
  return s;
}

Status KeyManagedEncryptedEnv::NewRandomRWFile(
    const std::string& fname, std::unique_ptr<RandomRWFile>* result,
    const EnvOptions& options) {
  FileEncryptionInfo file_info;
  // NewRandomRWFile is only used in the context of external file ingestion,
  // for rewriting global seqno. So it should call GetFile() instead of
  // NewFile().
  Status s = key_manager_->GetFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      s = target()->NewRandomRWFile(fname, result, options);
      break;
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
    case EncryptionMethod::kSM4_CTR:
      s = encrypted_env_->NewRandomRWFile(fname, result, options);
      break;
    default:
      s = Status::InvalidArgument(
          "Unsupported encryption method: " +
          ROCKSDB_NAMESPACE::ToString(static_cast<int>(file_info.method)));
  }
  if (!s.ok()) {
    // Ignore error
    key_manager_->DeleteFile(fname);
  }
  return s;
}

Status KeyManagedEncryptedEnv::DeleteFile(const std::string& fname) {
  // Try deleting the file from file system before updating key_manager.
  Status s = target()->DeleteFile(fname);
  if (!s.ok()) {
    return s;
  }
  return key_manager_->DeleteFile(fname);
}

Status KeyManagedEncryptedEnv::LinkFile(const std::string& src_fname,
                                        const std::string& dst_fname) {
  if (IsCurrentFile(dst_fname)) {
    assert(IsCurrentFile(src_fname));
    Status s = target()->LinkFile(src_fname, dst_fname);
    return s;
  } else {
    assert(!IsCurrentFile(src_fname));
  }
  Status s = key_manager_->LinkFile(src_fname, dst_fname);
  if (!s.ok()) {
    return s;
  }
  s = target()->LinkFile(src_fname, dst_fname);
  if (!s.ok()) {
    Status delete_status __attribute__((__unused__)) =
        key_manager_->DeleteFile(dst_fname);
    assert(delete_status.ok());
  }
  return s;
}

Status KeyManagedEncryptedEnv::RenameFile(const std::string& src_fname,
                                          const std::string& dst_fname) {
  if (IsCurrentFile(dst_fname)) {
    assert(IsCurrentFile(src_fname));
    Status s = target()->RenameFile(src_fname, dst_fname);
    // Replacing with plaintext requires deleting the info in the key manager.
    // The stale current file info exists when upgrading from TiKV <= v5.0.0-rc.
    Status delete_status __attribute__((__unused__)) =
        key_manager_->DeleteFile(dst_fname);
    assert(delete_status.ok());
    return s;
  } else {
    assert(!IsCurrentFile(src_fname));
  }
  // Link(copy)File instead of RenameFile to avoid losing src_fname info when
  // failed to rename the src_fname in the file system.
  Status s = key_manager_->LinkFile(src_fname, dst_fname);
  if (!s.ok()) {
    return s;
  }
  s = target()->RenameFile(src_fname, dst_fname);
  if (s.ok()) {
    s = key_manager_->DeleteFile(src_fname);
  } else {
    Status delete_status __attribute__((__unused__)) =
        key_manager_->DeleteFile(dst_fname);
    assert(delete_status.ok());
  }
  return s;
}

Env* NewKeyManagedEncryptedEnv(Env* base_env,
                               std::shared_ptr<KeyManager>& key_manager) {
  std::shared_ptr<AESEncryptionProvider> provider(
      new AESEncryptionProvider(key_manager.get()));
  std::unique_ptr<Env> encrypted_env(NewEncryptedEnv(base_env, provider));
  return new KeyManagedEncryptedEnv(base_env, key_manager, provider,
                                    std::move(encrypted_env));
}

}  // namespace encryption
}  // namespace ROCKSDB_NAMESPACE

#endif  // OPENSSL
#endif  // !ROCKSDB_LITE
