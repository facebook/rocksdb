// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#ifdef OPENSSL

#include "encryption/encryption.h"

#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace encryption {

Status AESBlockCipher::InitKey(const std::string& key) {
  int ret =
      AES_set_encrypt_key(reinterpret_cast<const unsigned char*>(key.data()),
                          static_cast<int>(key.size()) * 8, &encrypt_key_);
  if (ret != 0) {
    return Status::InvalidArgument("AES set encrypt key error: " +
                                   ROCKSDB_NAMESPACE::ToString(ret));
  }
  ret = AES_set_decrypt_key(reinterpret_cast<const unsigned char*>(key.data()),
                            static_cast<int>(key.size()) * 8, &decrypt_key_);
  if (ret != 0) {
    return Status::InvalidArgument("AES set decrypt key error: " +
                                   ROCKSDB_NAMESPACE::ToString(ret));
  }
  return Status::OK();
}

Status NewAESCTRCipherStream(EncryptionMethod method, const std::string& key,
                             const std::string& iv,
                             std::unique_ptr<AESCTRCipherStream>* result) {
  assert(result != nullptr);
  size_t key_size = KeySize(method);
  if (key_size == 0) {
    return Status::InvalidArgument("Unsupported encryption method: " +
                                   ToString(static_cast<int>(method)));
  }
  if (key.size() != key_size) {
    return Status::InvalidArgument("Encryption key size mismatch. " +
                                   ToString(key.size()) + "(actual) vs. " +
                                   ToString(key_size) + "(expected).");
  }
  if (iv.size() != AES_BLOCK_SIZE) {
    return Status::InvalidArgument(
        "iv size not equal to block cipher block size: " + ToString(iv.size()) +
        "(actual) vs. " + ToString(AES_BLOCK_SIZE) + "(expected).");
  }
  std::unique_ptr<AESCTRCipherStream> cipher_stream(new AESCTRCipherStream(iv));
  Status s = cipher_stream->InitKey(key);
  if (!s.ok()) {
    return s;
  }
  *result = std::move(cipher_stream);
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
      s = encrypted_env_->NewSequentialFile(fname, result, options);
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
  Status s = key_manager_->NewFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      s = target()->NewWritableFile(fname, result, options);
      break;
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
      s = encrypted_env_->NewWritableFile(fname, result, options);
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
  Status s = key_manager_->GetFile(fname, &file_info);
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
      s = encrypted_env_->ReuseWritableFile(fname, old_fname, result, options);
      break;
    default:
      s = Status::InvalidArgument(
          "Unsupported encryption method: " +
          ROCKSDB_NAMESPACE::ToString(static_cast<int>(file_info.method)));
  }
  if (s.ok()) {
    s = key_manager_->RenameFile(old_fname, fname);
  }
  return s;
}

Status KeyManagedEncryptedEnv::NewRandomRWFile(
    const std::string& fname, std::unique_ptr<RandomRWFile>* result,
    const EnvOptions& options) {
  FileEncryptionInfo file_info;
  Status s = key_manager_->NewFile(fname, &file_info);
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
  Status s = key_manager_->LinkFile(src_fname, dst_fname);
  if (!s.ok()) {
    return s;
  }
  s = target()->LinkFile(src_fname, dst_fname);
  if (!s.ok()) {
    // Ignore error
    key_manager_->DeleteFile(dst_fname);
  }
  return s;
}

Status KeyManagedEncryptedEnv::RenameFile(const std::string& src_fname,
                                          const std::string& dst_fname) {
  Status s = key_manager_->RenameFile(src_fname, dst_fname);
  if (!s.ok()) {
    return s;
  }
  s = target()->RenameFile(src_fname, dst_fname);
  if (!s.ok()) {
    // Ignore error
    key_manager_->RenameFile(dst_fname, src_fname);
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
