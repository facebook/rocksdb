// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#pragma once
#ifndef ROCKSDB_LITE
#ifdef OPENSSL

#include <memory>
#include <string>

#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
namespace encryption {

class AESEncryptionProvider;

enum class EncryptionMethod : int {
  kUnknown = 0,
  kPlaintext = 1,
  kAES128_CTR = 2,
  kAES192_CTR = 3,
  kAES256_CTR = 4,
};

inline size_t KeySize(EncryptionMethod method) {
  switch (method) {
    case EncryptionMethod::kAES128_CTR:
      return 16;
    case EncryptionMethod::kAES192_CTR:
      return 24;
    case EncryptionMethod::kAES256_CTR:
      return 32;
    default:
      return 0;
  };
}

struct FileEncryptionInfo {
  EncryptionMethod method = EncryptionMethod::kUnknown;
  std::string key;
  std::string iv;
};

// Interface to manage encryption keys for files. KeyManagedEncryptedEnv
// will query KeyManager for the key being used for each file, and update
// KeyManager when it creates a new file or moving files around.
class KeyManager {
 public:
  virtual ~KeyManager() = default;

  virtual Status GetFile(const std::string& fname,
                         FileEncryptionInfo* file_info) = 0;
  virtual Status NewFile(const std::string& fname,
                         FileEncryptionInfo* file_info) = 0;
  virtual Status DeleteFile(const std::string& fname) = 0;
  virtual Status LinkFile(const std::string& src_fname,
                          const std::string& dst_fname) = 0;
  virtual Status RenameFile(const std::string& src_fname,
                            const std::string& dst_fname) = 0;
};

// An Env with underlying files being encrypted. It holds a reference to an
// external KeyManager for encryption key management.
class KeyManagedEncryptedEnv : public EnvWrapper {
 public:
  KeyManagedEncryptedEnv(Env* base_env,
                         std::shared_ptr<KeyManager>& key_manager,
                         std::shared_ptr<AESEncryptionProvider>& provider,
                         std::unique_ptr<Env>&& encrypted_env);

  virtual ~KeyManagedEncryptedEnv();

  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) override;
  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) override;
  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& options) override;
  Status ReopenWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>* result,
                            const EnvOptions& options) override;
  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* result,
                           const EnvOptions& options) override;
  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override;

  Status DeleteFile(const std::string& fname) override;
  Status LinkFile(const std::string& src_fname,
                  const std::string& dst_fname) override;
  Status RenameFile(const std::string& src_fname,
                    const std::string& dst_fname) override;

 private:
  const std::shared_ptr<KeyManager> key_manager_;
  const std::shared_ptr<AESEncryptionProvider> provider_;
  const std::unique_ptr<Env> encrypted_env_;
};

extern Env* NewKeyManagedEncryptedEnv(Env* base_env,
                                      std::shared_ptr<KeyManager>& key_manager);

}  // namespace encryption
}  // namespace ROCKSDB_NAMESPACE

#endif  // OPENSSL
#endif  // !ROCKSDB_LITE
