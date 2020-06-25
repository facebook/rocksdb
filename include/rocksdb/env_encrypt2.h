//  copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//
// env_encryption.cc copied to this file then modified.

#ifdef ROCKSDB_OPENSSL_AES_CTR
#ifndef ROCKSDB_LITE

#include <algorithm>
#include <cctype>
#include <iostream>

#include "openssl/aes.h"
#include "openssl/evp.h"
#include "openssl/rand.h"
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

typedef char EncryptMarker[8];
static EncryptMarker Marker = "Encrypt";

// long term:  code_version could be used in a switch statement or factory
// parameter version 0 is 12 byte sha1 description hash, 128 bit (16 byte)
// nounce (assumed to be packed/byte aligned)
typedef struct {
  uint8_t key_description_[EVP_MAX_MD_SIZE];
  uint8_t nonce_[AES_BLOCK_SIZE / 2];  // block size is 16
} Prefix0;

class AESBlockAccessCipherStream : public BlockAccessCipherStream {
 public:
  AESBlockAccessCipherStream(const AesCtrKey& key, uint8_t code_version,
                             uint8_t nonce[])
      : key_(key), code_version_(code_version) {
    memcpy(&nonce_, nonce, AES_BLOCK_SIZE / 2);
  }

  // BlockSize returns the size of each block supported by this cipher stream.
  size_t BlockSize() override { return AES_BLOCK_SIZE; };

 protected:
  // Allocate scratch space which is passed to EncryptBlock/DecryptBlock.
  void AllocateScratch(std::string&) override{};

  // Encrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  Status EncryptBlock(uint64_t blockIndex, char* data,
                              char* scratch) override;

  // Decrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  Status DecryptBlock(uint64_t blockIndex, char* data,
                              char* scratch) override;

  AesCtrKey key_;
  uint8_t code_version_;
  uint8_t nonce_[AES_BLOCK_SIZE / 2];
};

class CTREncryptionProvider2 : public EncryptionProvider {
 public:
  CTREncryptionProvider2() = delete;

  CTREncryptionProvider2(const CTREncryptionProvider&&) = delete;

  CTREncryptionProvider2(const Sha1Description& key_desc_in,
                         const AesCtrKey& key_in)
      : valid_(false), key_desc_(key_desc_in), key_(key_in) {
    valid_ = key_desc_.IsValid() && key_.IsValid();
  }

  CTREncryptionProvider2(const std::string& key_desc_str,
                         const uint8_t unformatted_key[], int bytes)
      : valid_(false), key_desc_(key_desc_str), key_(unformatted_key, bytes) {
    valid_ = key_desc_.IsValid() && key_.IsValid();
  }

  size_t GetPrefixLength() override {
    return sizeof(Prefix0) + sizeof(EncryptMarker);
  }

  Status CreateNewPrefix(const std::string& /*fname*/, char* prefix,
                                 size_t prefixLength) override;

  Status CreateCipherStream(
      const std::string& /*fname*/, const EnvOptions& /*options*/,
      Slice& /*prefix*/,
      std::unique_ptr<BlockAccessCipherStream>* /*result*/) override {
    return Status::NotSupported("Wrong EncryptionProvider assumed");
  }

  virtual BlockAccessCipherStream* CreateCipherStream2(uint8_t code_version,
                                                       uint8_t nonce[]) {
    return new AESBlockAccessCipherStream(key_, code_version, nonce);
  }

  bool Valid() const { return valid_; };
  const Sha1Description& key_desc() const { return key_desc_; };
  const AesCtrKey& key() const { return key_; };

 protected:
  bool valid_;
  Sha1Description key_desc_;
  AesCtrKey key_;
};

// EncryptedEnv2 implements an Env wrapper that adds encryption to files stored
// on disk.

class EncryptedEnv2 : public EnvWrapper {
 public:
  using WriteKey =
      std::pair<Sha1Description, std::shared_ptr<EncryptionProvider>>;
  using ReadKeys =
      std::map<Sha1Description, std::shared_ptr<EncryptionProvider>>;

  static Env* Default();
  static Env* Default(ReadKeys encrypt_read, WriteKey encrypt_write);

  EncryptedEnv2(Env* base_env);

  EncryptedEnv2(Env* base_env, ReadKeys encrypt_read,
                WriteKey encrypt_write);

  void SetKeys(ReadKeys encrypt_read, WriteKey encrypt_write);

  bool IsWriteEncrypted() const { return nullptr != encrypt_write_.second; }

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
  Status GetFileSize(const std::string& fname,
                             uint64_t* file_size) override;

  // only needed for GetChildrenFileAttributes & GetFileSize
  virtual Status GetEncryptionProvider(
      const std::string& fname, std::shared_ptr<EncryptionProvider>& provider);

  template <class TypeFile>
  Status ReadSeqEncryptionPrefix(
      TypeFile* f, std::shared_ptr<EncryptionProvider>& provider,
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
          marker_slice.starts_with(Marker)) {
        // code_version currently unused
        uint8_t code_version = (uint8_t)marker_slice[7];

        Slice prefix_slice;
        Prefix0 prefix_buffer;
        status =
            f->Read(sizeof(Prefix0), &prefix_slice, (char*)&prefix_buffer);
        if (status.ok() && sizeof(Prefix0) == prefix_slice.size()) {
          Sha1Description desc(prefix_buffer.key_description_,
                                 sizeof(prefix_buffer.key_description_));

          auto it = encrypt_read_.find(desc);
          if (encrypt_read_.end() != it) {
            CTREncryptionProvider2* ptr =
                (CTREncryptionProvider2*)it->second.get();
            provider = it->second;
            stream.reset(new AESBlockAccessCipherStream(
                ptr->key(), code_version, prefix_buffer.nonce_));
          } else {
            status = Status::NotSupported(
                "No encryption key found to match input file");
          }
        }
      }
    }
    return status;
  }

  template <class TypeFile>
  Status ReadRandEncryptionPrefix(
      TypeFile* f, std::shared_ptr<EncryptionProvider>& provider,
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
          marker_slice.starts_with(Marker)) {
        // code_version currently unused
        uint8_t code_version = (uint8_t)marker_slice[7];

        Slice prefix_slice;
        Prefix0 prefix_buffer;
        status = f->Read(sizeof(marker), sizeof(Prefix0), &prefix_slice,
                         (char*)&prefix_buffer);
        if (status.ok() && sizeof(Prefix0) == prefix_slice.size()) {
          Sha1Description desc(prefix_buffer.key_description_,
                                 sizeof(prefix_buffer.key_description_));

          auto it = encrypt_read_.find(desc);
          if (encrypt_read_.end() != it) {
            CTREncryptionProvider2* ptr =
                (CTREncryptionProvider2*)it->second.get();
            provider = it->second;
            stream.reset(new AESBlockAccessCipherStream(
                ptr->key(), code_version, prefix_buffer.nonce_));
          } else {
            status = Status::NotSupported(
                "No encryption key found to match input file");
          }
        }
      }
    }
    return status;
  }

  template <class TypeFile>
  Status WriteSeqEncryptionPrefix(
      TypeFile* f, std::unique_ptr<BlockAccessCipherStream>& stream) {
    Status status;

    // set up Encryption maker, code version '0'
    uint8_t code_version = {'0'};
    Prefix0 prefix;
    EncryptMarker marker;
    strncpy(marker, Marker, sizeof(Marker));
    marker[sizeof(EncryptMarker) - 1] = code_version;

    Slice marker_slice(marker, sizeof(EncryptMarker));
    status = f->Append(marker_slice);

    if (status.ok()) {
      // create nonce, then write it and key description
      Slice prefix_slice((char*)&prefix, sizeof(prefix));

      status = encrypt_write_.second->CreateNewPrefix(
          std::string(), (char*)&prefix,
          encrypt_write_.second->GetPrefixLength());

      if (status.ok()) {
        status = f->Append(prefix_slice);
      }
    }

    if (status.ok()) {
      CTREncryptionProvider2* ptr =
          (CTREncryptionProvider2*)encrypt_write_.second.get();
      stream.reset(new AESBlockAccessCipherStream(ptr->key(), code_version,
                                                  prefix.nonce_));
    }

    return status;
  }

  template <class TypeFile>
  Status WriteRandEncryptionPrefix(
      TypeFile* f, std::unique_ptr<BlockAccessCipherStream>& stream) {
    Status status;

    // set up Encryption maker, code version '0'
    uint8_t code_version = {'0'};
    Prefix0 prefix;
    EncryptMarker marker;
    strncpy(marker, Marker, sizeof(Marker));
    marker[sizeof(EncryptMarker) - 1] = code_version;

    Slice marker_slice(marker, sizeof(EncryptMarker));
    status = f->Write(0, marker_slice);

    if (status.ok()) {
      // create nonce, then write it and key description
      Slice prefix_slice((char*)&prefix, sizeof(prefix));

      status = encrypt_write_.second->CreateNewPrefix(
          std::string(), (char*)&prefix,
          encrypt_write_.second->GetPrefixLength());

      if (status.ok()) {
        status = f->Write(sizeof(EncryptMarker), prefix_slice);
      }
    }

    if (status.ok()) {
      CTREncryptionProvider2* ptr =
          (CTREncryptionProvider2*)encrypt_write_.second.get();
      stream.reset(new AESBlockAccessCipherStream(ptr->key(), code_version,
                                                  prefix.nonce_));
    }

    return status;
  }

  bool IsValid() const { return valid_; }

  static UnixLibCrypto crypto_;

 protected:
  std::map<Sha1Description, std::shared_ptr<EncryptionProvider>>
      encrypt_read_;
  std::pair<Sha1Description, std::shared_ptr<EncryptionProvider>>
      encrypt_write_;

  bool valid_;
};

// Returns an Env that encrypts data when stored on disk and decrypts data when
// read from disk.  Prefer EncryptedEnv2::Default().
Env* NewEncryptedEnv2(Env* base_env, EncryptedEnv2::ReadKeys encrypt_read,
                      EncryptedEnv2::WriteKey encrypt_write);

#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_OPENSSL_AES_CTR
