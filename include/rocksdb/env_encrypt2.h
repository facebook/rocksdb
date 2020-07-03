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

struct Sha1Description {
  uint8_t desc[EVP_MAX_MD_SIZE];
  bool valid;

  Sha1Description() : valid(false) { memset(desc, 0, EVP_MAX_MD_SIZE); }

  Sha1Description(const Sha1Description& rhs) { *this = rhs; }

  Sha1Description& operator=(const Sha1Description& rhs) {
    memcpy(desc, rhs.desc, sizeof(desc));
    valid = rhs.valid;
    return *this;
  }

  Sha1Description(uint8_t* desc_in, size_t desc_len) : valid(false) {
    memset(desc, 0, EVP_MAX_MD_SIZE);
    if (desc_len <= EVP_MAX_MD_SIZE) {
      memcpy(desc, desc_in, desc_len);
      valid = true;
    }
  }

  Sha1Description(const std::string& key_desc_str);

  // see AesCtrKey destructor below.  This data is not really
  //  essential to clear, but trying to set pattern for future work.
  // goal is to explicitly remove desc from memory once no longer needed
  ~Sha1Description() {
    memset(desc, 0, EVP_MAX_MD_SIZE);
    valid = false;
  }

  bool operator<(const Sha1Description& rhs) const {
    return memcmp(desc, rhs.desc, EVP_MAX_MD_SIZE) < 0;
  }

  bool operator==(const Sha1Description& rhs) const {
    return 0 == memcmp(desc, rhs.desc, EVP_MAX_MD_SIZE) && valid == rhs.valid;
  }

  bool IsValid() const { return valid; }
};

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

  AesCtrKey(const std::string& key_str);

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

class CTREncryptionProviderV2 : public EncryptionProvider {
 public:
  CTREncryptionProviderV2() = delete;

  CTREncryptionProviderV2(const CTREncryptionProvider&&) = delete;

  CTREncryptionProviderV2(const Sha1Description& key_desc_in,
                          const AesCtrKey& key_in)
      : valid_(false), key_desc_(key_desc_in), key_(key_in) {
    valid_ = key_desc_.IsValid() && key_.IsValid();
  }

  CTREncryptionProviderV2(const std::string& key_desc_str,
                          const uint8_t unformatted_key[], int bytes)
      : valid_(false), key_desc_(key_desc_str), key_(unformatted_key, bytes) {
    valid_ = key_desc_.IsValid() && key_.IsValid();
  }

  size_t GetPrefixLength() const override;

  Status CreateNewPrefix(const std::string& /*fname*/, char* prefix,
                         size_t prefixLength) const override;

  Status CreateCipherStream(
      const std::string& /*fname*/, const EnvOptions& /*options*/,
      Slice& /*prefix*/,
      std::unique_ptr<BlockAccessCipherStream>* /*result*/) override {
    return Status::NotSupported("Wrong EncryptionProvider assumed");
  }

  virtual BlockAccessCipherStream* CreateCipherStream2(
      uint8_t code_version, const uint8_t nonce[]) const;

  bool Valid() const { return valid_; };
  const Sha1Description& key_desc() const { return key_desc_; };
  const AesCtrKey& key() const { return key_; };

 protected:
  bool valid_;
  Sha1Description key_desc_;
  AesCtrKey key_;
};
#if 0
class EncryptedSequentialFileV2 : public EncryptedSequentialFile {
 protected:
  std::shared_ptr<const CTREncryptionProviderV2> provider_;

 public:
  // Default ctor. Given underlying sequential file is supposed to be at
  // offset == prefixLength.
  EncryptedSequentialFileV2(std::unique_ptr<SequentialFile>&& f,
                          std::shared_ptr<const CTREncryptionProviderV2>& p,
                          std::unique_ptr<BlockAccessCipherStream>&& s)

      : EncryptedSequentialFile(std::move(f), std::move(s), p->GetPrefixLength()),
      provider_(p) {}

  // encryption code alignment not proven for direct io
  bool use_direct_io() const override {return false;};

};
#endif

class EncryptedWritableFileV2 : public EncryptedWritableFile {
 public:
  // Default ctor. Prefix is assumed to be written already.
  EncryptedWritableFileV2(std::unique_ptr<WritableFile>&& f,
                          std::unique_ptr<BlockAccessCipherStream>&& s,
                          size_t prefix_length)
      : EncryptedWritableFile(std::move(f), std::move(s), prefix_length) {}

  Status Append(const Slice& data) override;

  Status PositionedAppend(const Slice& data, uint64_t offset) override;

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  bool use_direct_io() const override { return false; };
};

// EncryptedEnvV2 implements an Env wrapper that adds encryption to files stored
// on disk.
class EncryptedEnvV2 : public EnvWrapper {
 public:
  using WriteKey = std::pair<Sha1Description,
                             std::shared_ptr<const CTREncryptionProviderV2>>;
  using ReadKeys =
      std::map<Sha1Description, std::shared_ptr<const CTREncryptionProviderV2>>;

  static Env* Default();
  static Env* Default(ReadKeys encrypt_read, WriteKey encrypt_write);

  EncryptedEnvV2(Env* base_env);

  EncryptedEnvV2(Env* base_env, ReadKeys encrypt_read, WriteKey encrypt_write);

  bool IsWriteEncrypted() const;

  // NewSequentialFile opens a file for sequential reading.
  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) override;

  // NewRandomAccessFile opens a file for random read access.
  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) override;

  // NewWritableFile opens a file for sequential writing.
  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& options) override;

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  Status ReopenWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>* result,
                            const EnvOptions& options) override;

  // Reuse an existing file by renaming it and opening it as writable.
  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* result,
                           const EnvOptions& options) override;

  // Open `fname` for random read and write, if file doesn't exist the file
  // will be created.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override;

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
  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override;

  // Store the size of fname in *file_size.
  Status GetFileSize(const std::string& fname, uint64_t* file_size) override;

  // only needed for GetChildrenFileAttributes & GetFileSize
  virtual Status GetEncryptionProvider(
      const std::string& fname,
      std::shared_ptr<const CTREncryptionProviderV2>& provider);

  bool IsValid() const { return valid_; }

 protected:
  // following is not thread safe, intended for constuction
  //  and unit test only
  void SetKeys(ReadKeys encrypt_read, WriteKey encrypt_write);

  template <class TypeFile>
  Status ReadSeqEncryptionPrefix(
      TypeFile* f, std::shared_ptr<const CTREncryptionProviderV2>& provider,
      std::unique_ptr<BlockAccessCipherStream>& stream);

  template <class TypeFile>
  Status ReadRandEncryptionPrefix(
      TypeFile* f, std::shared_ptr<const CTREncryptionProviderV2>& provider,
      std::unique_ptr<BlockAccessCipherStream>& stream);

  template <class TypeFile>
  Status WriteSeqEncryptionPrefix(
      TypeFile* f, std::shared_ptr<const CTREncryptionProviderV2> provider,
      std::unique_ptr<BlockAccessCipherStream>& stream);

  template <class TypeFile>
  Status WriteRandEncryptionPrefix(
      TypeFile* f, std::shared_ptr<const CTREncryptionProviderV2> provider,
      std::unique_ptr<BlockAccessCipherStream>& stream);

 public:
  static UnixLibCrypto crypto_;

 protected:
  ReadKeys encrypt_read_;
  WriteKey encrypt_write_;

  bool valid_;
};

// Returns an Env that encrypts data when stored on disk and decrypts data when
// read from disk.  Prefer EncryptedEnvV2::Default().
Env* NewEncryptedEnvV2(Env* base_env, EncryptedEnvV2::ReadKeys encrypt_read,
                       EncryptedEnvV2::WriteKey encrypt_write);

#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_OPENSSL_AES_CTR
