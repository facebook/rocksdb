// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "env/env_openssl_impl.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_writer.h"
#include "test_util/testharness.h"

#ifndef ROCKSDB_LITE
#ifdef ROCKSDB_OPENSSL_AES_CTR

namespace ROCKSDB_NAMESPACE {

class EnvOpenssl_Sha1 {};

TEST(EnvOpenssl_Sha1, Default) {
  ShaDescription desc;

  ASSERT_FALSE(desc.IsValid());
  for (size_t idx = 0; idx < sizeof(desc.desc); ++idx) {
    ASSERT_TRUE('\0' == desc.desc[idx]);
  }
}

TEST(EnvOpenssl_Sha1, Constructors) {
  ShaDescription desc;

  // verify we know size of desc.desc
  ASSERT_TRUE(64 == sizeof(desc.desc));

  uint8_t bytes[128], *ptr;
  for (size_t idx = 0; idx < sizeof(bytes); ++idx) {
    bytes[idx] = idx + 1;
  }

  ShaDescription desc_bad1(bytes, 128);
  ASSERT_FALSE(desc_bad1.IsValid());

  ShaDescription desc_bad2(bytes, 65);
  ASSERT_FALSE(desc_bad2.IsValid());

  ShaDescription desc_good1(bytes, 64);
  ASSERT_TRUE(desc_good1.IsValid());
  ptr = (uint8_t*)memchr(desc_good1.desc, 0, 64);
  ASSERT_TRUE(nullptr == ptr);

  ShaDescription desc_good2(bytes, 63);
  ASSERT_TRUE(desc_good2.IsValid());
  ptr = (uint8_t*)memchr(desc_good2.desc, 0, 64);
  ASSERT_TRUE(&desc_good2.desc[63] == ptr);

  ShaDescription desc_good3(bytes, 1);
  ASSERT_TRUE(desc_good3.IsValid());
  ptr = (uint8_t*)memchr(desc_good3.desc, 0, 64);
  ASSERT_TRUE(&desc_good3.desc[1] == ptr);

  ShaDescription desc_good4(bytes, 0);
  ASSERT_TRUE(desc_good4.IsValid());
  ptr = (uint8_t*)memchr(desc_good4.desc, 0, 64);
  ASSERT_TRUE(&desc_good4.desc[0] == ptr);

  ShaDescription desc_str1("");
  ASSERT_FALSE(desc_str1.IsValid());

  uint8_t md2[] = {0x35, 0x6a, 0x19, 0x2b, 0x79, 0x13, 0xb0, 0x4c, 0x54, 0x57,
                   0x4d, 0x18, 0xc2, 0x8d, 0x46, 0xe6, 0x39, 0x54, 0x28, 0xab};
  ShaDescription desc_str2("1");
  ASSERT_TRUE(desc_str2.IsValid());
  ASSERT_TRUE(0 == memcmp(md2, desc_str2.desc, sizeof(md2)));
  for (size_t idx = sizeof(md2); idx < sizeof(desc_str2.desc); ++idx) {
    ASSERT_TRUE(0 == desc_str2.desc[idx]);
  }

  uint8_t md3[] = {0x7b, 0x52, 0x00, 0x9b, 0x64, 0xfd, 0x0a, 0x2a, 0x49, 0xe6,
                   0xd8, 0xa9, 0x39, 0x75, 0x30, 0x77, 0x79, 0x2b, 0x05, 0x54};
  ShaDescription desc_str3("12");
  ASSERT_TRUE(desc_str3.IsValid());
  ASSERT_TRUE(0 == memcmp(md3, desc_str3.desc, sizeof(md3)));
  for (size_t idx = sizeof(md3); idx < sizeof(desc_str3.desc); ++idx) {
    ASSERT_TRUE(0 == desc_str3.desc[idx]);
  }
}

TEST(EnvOpenssl_Sha1, Copy) {
  // assignment
  uint8_t md1[] = {0xdb, 0x8a, 0xc1, 0xc2, 0x59, 0xeb, 0x89, 0xd4, 0xa1, 0x31,
                   0xb2, 0x53, 0xba, 0xcf, 0xca, 0x5f, 0x31, 0x9d, 0x54, 0xf2};
  ShaDescription desc1("HelloWorld"), desc2;
  ASSERT_TRUE(desc1.IsValid());
  ASSERT_FALSE(desc2.IsValid());

  desc2 = desc1;
  ASSERT_TRUE(desc1.IsValid());
  ASSERT_TRUE(desc2.IsValid());
  ASSERT_TRUE(0 == memcmp(md1, desc1.desc, sizeof(md1)));
  for (size_t idx = sizeof(md1); idx < sizeof(desc1.desc); ++idx) {
    ASSERT_TRUE(0 == desc1.desc[idx]);
  }
  ASSERT_TRUE(0 == memcmp(md1, desc2.desc, sizeof(md1)));
  for (size_t idx = sizeof(md1); idx < sizeof(desc2.desc); ++idx) {
    ASSERT_TRUE(0 == desc2.desc[idx]);
  }

  // copy constructor
  uint8_t md3[] = {0x17, 0x09, 0xcc, 0x51, 0x65, 0xf5, 0x50, 0x4d, 0x46, 0xde,
                   0x2f, 0x3a, 0x7a, 0xff, 0x57, 0x45, 0x20, 0x8a, 0xed, 0x44};
  ShaDescription desc3("A little be longer title for a key");
  ASSERT_TRUE(desc3.IsValid());

  ShaDescription desc4(desc3);
  ASSERT_TRUE(desc3.IsValid());
  ASSERT_TRUE(desc4.IsValid());
  ASSERT_TRUE(0 == memcmp(md3, desc3.desc, sizeof(md3)));
  for (size_t idx = sizeof(md3); idx < sizeof(desc3.desc); ++idx) {
    ASSERT_TRUE(0 == desc3.desc[idx]);
  }
  ASSERT_TRUE(0 == memcmp(md3, desc4.desc, sizeof(md3)));
  for (size_t idx = sizeof(md3); idx < sizeof(desc4.desc); ++idx) {
    ASSERT_TRUE(0 == desc4.desc[idx]);
  }
}

class EnvOpenssl_Key {};

TEST(EnvOpenssl_Key, Default) {
  AesCtrKey key;

  ASSERT_FALSE(key.IsValid());
  for (size_t idx = 0; idx < sizeof(key.key); ++idx) {
    ASSERT_TRUE('\0' == key.key[idx]);
  }
}

TEST(EnvOpenssl_Key, Constructors) {
  AesCtrKey key;

  // verify we know size of key.key
  ASSERT_TRUE(64 == sizeof(key.key));

  uint8_t bytes[128], *ptr;
  for (size_t idx = 0; idx < sizeof(bytes); ++idx) {
    bytes[idx] = idx + 1;
  }

  AesCtrKey key_bad1(bytes, 128);
  ASSERT_FALSE(key_bad1.IsValid());

  AesCtrKey key_bad2(bytes, 65);
  ASSERT_FALSE(key_bad2.IsValid());

  AesCtrKey key_good1(bytes, 64);
  ASSERT_TRUE(key_good1.IsValid());
  ptr = (uint8_t*)memchr(key_good1.key, 0, 64);
  ASSERT_TRUE(nullptr == ptr);

  AesCtrKey key_good2(bytes, 63);
  ASSERT_TRUE(key_good2.IsValid());
  ptr = (uint8_t*)memchr(key_good2.key, 0, 64);
  ASSERT_TRUE(&key_good2.key[63] == ptr);

  AesCtrKey key_good3(bytes, 1);
  ASSERT_TRUE(key_good3.IsValid());
  ptr = (uint8_t*)memchr(key_good3.key, 0, 64);
  ASSERT_TRUE(&key_good3.key[1] == ptr);

  AesCtrKey key_good4(bytes, 0);
  ASSERT_TRUE(key_good4.IsValid());
  ptr = (uint8_t*)memchr(key_good4.key, 0, 64);
  ASSERT_TRUE(&key_good4.key[0] == ptr);

  AesCtrKey key_str1("");
  ASSERT_FALSE(key_str1.IsValid());

  AesCtrKey key_str2("0x35");
  ASSERT_FALSE(key_str2.IsValid());

  // 1234567890123456789012345678901234567890123456789012345678901234
  AesCtrKey key_str3(
      "RandomSixtyFourCharactersLaLaLaLaJust a bunch of letters, not 0x");
  ASSERT_FALSE(key_str2.IsValid());

  uint8_t key4[] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                    0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
                    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
                    0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20};
  // 1234567890123456789012345678901234567890123456789012345678901234
  AesCtrKey key_str4(
      "0102030405060708090A0B0C0D0E0F101112131415161718191a1b1c1d1e1f20");
  ASSERT_TRUE(key_str4.IsValid());
  ASSERT_TRUE(0 == memcmp(key4, key_str4.key, sizeof(key4)));
}

TEST(EnvOpenssl_Key, Copy) {
  // assignment
  uint8_t data1[] = {0x60, 0x3d, 0xeb, 0x10, 0x15, 0xca, 0x71, 0xbe,
                     0x2b, 0x73, 0xae, 0xf0, 0x85, 0x7d, 0x77, 0x81,
                     0x1f, 0x35, 0x2c, 0x07, 0x3b, 0x61, 0x08, 0xd7,
                     0x2d, 0x98, 0x10, 0xa3, 0x09, 0x14, 0xdf, 0xf4};
  AesCtrKey key1(data1, sizeof(data1)), key2;
  ASSERT_TRUE(key1.IsValid());
  ASSERT_FALSE(key2.IsValid());

  key2 = key1;
  ASSERT_TRUE(key1.IsValid());
  ASSERT_TRUE(key2.IsValid());
  ASSERT_TRUE(0 == memcmp(data1, key1.key, sizeof(data1)));
  ASSERT_TRUE(0 == memcmp(data1, key2.key, sizeof(data1)));

  // copy constructor
  uint8_t data3[] = {0x21, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29,
                     0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0x22, 0x20,
                     0x1f, 0x35, 0x2c, 0x07, 0x3b, 0x61, 0x08, 0xd7,
                     0x2d, 0x98, 0x10, 0xa3, 0x09, 0x14, 0xdf, 0xf4};
  AesCtrKey key3(data3, sizeof(data3));
  ASSERT_TRUE(key3.IsValid());

  AesCtrKey key4(key3);
  ASSERT_TRUE(key3.IsValid());
  ASSERT_TRUE(key4.IsValid());
  ASSERT_TRUE(0 == memcmp(data3, key3.key, sizeof(data3)));
  ASSERT_TRUE(0 == memcmp(data3, key4.key, sizeof(data3)));
}

class EnvOpenssl_Provider {};

TEST(EnvOpenssl_Provider, NistExamples) {
  uint8_t key[] = {0x60, 0x3d, 0xeb, 0x10, 0x15, 0xca, 0x71, 0xbe,
                   0x2b, 0x73, 0xae, 0xf0, 0x85, 0x7d, 0x77, 0x81,
                   0x1f, 0x35, 0x2c, 0x07, 0x3b, 0x61, 0x08, 0xd7,
                   0x2d, 0x98, 0x10, 0xa3, 0x09, 0x14, 0xdf, 0xf4};
  uint8_t init[] = {0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7,
                    0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff};

  uint8_t plain1[] = {0x6b, 0xc1, 0xbe, 0xe2, 0x2e, 0x40, 0x9f, 0x96,
                      0xe9, 0x3d, 0x7e, 0x11, 0x73, 0x93, 0x17, 0x2a};
  uint8_t cypher1[] = {0x60, 0x1e, 0xc3, 0x13, 0x77, 0x57, 0x89, 0xa5,
                       0xb7, 0xa7, 0xf5, 0x04, 0xbb, 0xf3, 0xd2, 0x28};

  uint8_t plain2[] = {0xae, 0x2d, 0x8a, 0x57, 0x1e, 0x03, 0xac, 0x9c,
                      0x9e, 0xb7, 0x6f, 0xac, 0x45, 0xaf, 0x8e, 0x51};
  uint8_t cypher2[] = {0xf4, 0x43, 0xe3, 0xca, 0x4d, 0x62, 0xb5, 0x9a,
                       0xca, 0x84, 0xe9, 0x90, 0xca, 0xca, 0xf5, 0xc5};

  uint8_t plain3[] = {0x30, 0xc8, 0x1c, 0x46, 0xa3, 0x5c, 0xe4, 0x11,
                      0xe5, 0xfb, 0xc1, 0x19, 0x1a, 0x0a, 0x52, 0xef};
  uint8_t cypher3[] = {0x2b, 0x09, 0x30, 0xda, 0xa2, 0x3d, 0xe9, 0x4c,
                       0xe8, 0x70, 0x17, 0xba, 0x2d, 0x84, 0x98, 0x8d};

  uint8_t plain4[] = {0xf6, 0x9f, 0x24, 0x45, 0xdf, 0x4f, 0x9b, 0x17,
                      0xad, 0x2b, 0x41, 0x7b, 0xe6, 0x6c, 0x37, 0x10};
  uint8_t cypher4[] = {0xdf, 0xc9, 0xc5, 0x8d, 0xb6, 0x7a, 0xad, 0xa6,
                       0x13, 0xc2, 0xdd, 0x08, 0x45, 0x79, 0x41, 0xa6};

  CTREncryptionProviderV2 provider("NistExampleKey", key, sizeof(key));

  std::unique_ptr<BlockAccessCipherStream> stream(
      provider.CreateCipherStream2(1, init));

  uint64_t offset;
  uint8_t block[sizeof(plain1)];

  //
  // forward ... encryption
  //
  //  memcpy((void*)&offset, (void*)&init[8], 8);
  offset = 0;
  memcpy((void*)block, (void*)plain1, 16);

  Status status = stream->Encrypt(offset, (char*)block, sizeof(block));
  ASSERT_TRUE(0 == memcmp(cypher1, block, sizeof(block)));

  offset = 16;
  memcpy((void*)block, (void*)plain2, 16);

  status = stream->Encrypt(offset, (char*)block, sizeof(block));
  ASSERT_TRUE(0 == memcmp(cypher2, block, sizeof(block)));

  offset = 32;
  memcpy((void*)block, (void*)plain3, 16);

  status = stream->Encrypt(offset, (char*)block, sizeof(block));
  ASSERT_TRUE(0 == memcmp(cypher3, block, sizeof(block)));

  offset = 48;
  memcpy((void*)block, (void*)plain4, 16);

  status = stream->Encrypt(offset, (char*)block, sizeof(block));
  ASSERT_TRUE(0 == memcmp(cypher4, block, sizeof(block)));

  //
  // backward -- decryption
  //
  offset = 0;
  memcpy((void*)block, (void*)cypher1, 16);

  status = stream->Decrypt(offset, (char*)block, sizeof(block));
  ASSERT_TRUE(0 == memcmp(plain1, block, sizeof(block)));

  offset = 16;
  memcpy((void*)block, (void*)cypher2, 16);

  status = stream->Decrypt(offset, (char*)block, sizeof(block));
  ASSERT_TRUE(0 == memcmp(plain2, block, sizeof(block)));

  offset = 32;
  memcpy((void*)block, (void*)cypher3, 16);

  status = stream->Decrypt(offset, (char*)block, sizeof(block));
  ASSERT_TRUE(0 == memcmp(plain3, block, sizeof(block)));

  offset = 48;
  memcpy((void*)block, (void*)cypher4, 16);

  status = stream->Decrypt(offset, (char*)block, sizeof(block));
  ASSERT_TRUE(0 == memcmp(plain4, block, sizeof(block)));
}

TEST(EnvOpenssl_Provider, NistSingleCall) {
  uint8_t key[] = {0x60, 0x3d, 0xeb, 0x10, 0x15, 0xca, 0x71, 0xbe,
                   0x2b, 0x73, 0xae, 0xf0, 0x85, 0x7d, 0x77, 0x81,
                   0x1f, 0x35, 0x2c, 0x07, 0x3b, 0x61, 0x08, 0xd7,
                   0x2d, 0x98, 0x10, 0xa3, 0x09, 0x14, 0xdf, 0xf4};
  uint8_t init[] = {0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7,
                    0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff};

  uint8_t plain1[] = {
      0x6b, 0xc1, 0xbe, 0xe2, 0x2e, 0x40, 0x9f, 0x96, 0xe9, 0x3d, 0x7e,
      0x11, 0x73, 0x93, 0x17, 0x2a, 0xae, 0x2d, 0x8a, 0x57, 0x1e, 0x03,
      0xac, 0x9c, 0x9e, 0xb7, 0x6f, 0xac, 0x45, 0xaf, 0x8e, 0x51, 0x30,
      0xc8, 0x1c, 0x46, 0xa3, 0x5c, 0xe4, 0x11, 0xe5, 0xfb, 0xc1, 0x19,
      0x1a, 0x0a, 0x52, 0xef, 0xf6, 0x9f, 0x24, 0x45, 0xdf, 0x4f, 0x9b,
      0x17, 0xad, 0x2b, 0x41, 0x7b, 0xe6, 0x6c, 0x37, 0x10};

  uint8_t cypher1[] = {
      0x60, 0x1e, 0xc3, 0x13, 0x77, 0x57, 0x89, 0xa5, 0xb7, 0xa7, 0xf5,
      0x04, 0xbb, 0xf3, 0xd2, 0x28, 0xf4, 0x43, 0xe3, 0xca, 0x4d, 0x62,
      0xb5, 0x9a, 0xca, 0x84, 0xe9, 0x90, 0xca, 0xca, 0xf5, 0xc5, 0x2b,
      0x09, 0x30, 0xda, 0xa2, 0x3d, 0xe9, 0x4c, 0xe8, 0x70, 0x17, 0xba,
      0x2d, 0x84, 0x98, 0x8d, 0xdf, 0xc9, 0xc5, 0x8d, 0xb6, 0x7a, 0xad,
      0xa6, 0x13, 0xc2, 0xdd, 0x08, 0x45, 0x79, 0x41, 0xa6};

  AesCtrKey aes_key(key, sizeof(key));
  uint8_t output[sizeof(plain1)];
  AESBlockAccessCipherStream stream(aes_key, 0, init);
  uint64_t offset;

  //
  // forward ... encryption
  //
  memcpy((void*)output, (void*)plain1, sizeof(plain1));
  // memcpy((void*)&offset, (void*)&init[8], 8);
  offset = 0;

  Status status = stream.Encrypt(offset, (char*)output, sizeof(plain1));
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(0 == memcmp(cypher1, output, sizeof(output)));
}

TEST(EnvOpenssl_Provider, BigEndianAdd) {
  uint8_t nounce1[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  uint8_t expect1[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};
  AESBlockAccessCipherStream::BigEndianAdd128(nounce1, 1);
  ASSERT_TRUE(0 == memcmp(nounce1, expect1, sizeof(nounce1)));

  uint8_t nounce2[] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                       0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
  uint8_t expect2[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  AESBlockAccessCipherStream::BigEndianAdd128(nounce2, 1);
  ASSERT_TRUE(0 == memcmp(nounce2, expect2, sizeof(nounce2)));

  uint8_t nounce3[] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                       0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
  uint8_t expect3[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x00};
  AESBlockAccessCipherStream::BigEndianAdd128(nounce3, 0xff01);
  ASSERT_TRUE(0 == memcmp(nounce3, expect3, sizeof(nounce3)));
}

//
// The following is copied from env_basic_test.cc
//

// Normalizes trivial differences across Envs such that these test cases can
// run on all Envs.
class NormalizingEnvWrapper : public EnvWrapper {
 public:
  explicit NormalizingEnvWrapper(Env* base) : EnvWrapper(base) {}

  // Removes . and .. from directory listing
  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) override {
    Status status = EnvWrapper::GetChildren(dir, result);
    if (status.ok()) {
      result->erase(std::remove_if(result->begin(), result->end(),
                                   [](const std::string& s) {
                                     return s == "." || s == "..";
                                   }),
                    result->end());
    }
    return status;
  }

  // Removes . and .. from directory listing
  virtual Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override {
    Status status = EnvWrapper::GetChildrenFileAttributes(dir, result);
    if (status.ok()) {
      result->erase(std::remove_if(result->begin(), result->end(),
                                   [](const FileAttributes& fa) {
                                     return fa.name == "." || fa.name == "..";
                                   }),
                    result->end());
    }
    return status;
  }
};

class EnvBasicTestWithParam : public testing::Test,
                              public ::testing::WithParamInterface<Env*> {
 public:
  Env* env_;
  const EnvOptions soptions_;
  std::string test_dir_;

  EnvBasicTestWithParam() : env_(GetParam()) {
    test_dir_ = test::PerThreadDBPath(env_, "env_openssl_test");
  }

  void SetUp() { env_->CreateDirIfMissing(test_dir_); }

  void TearDown() {
    std::vector<std::string> files;
    env_->GetChildren(test_dir_, &files);
    for (const auto& file : files) {
      // don't know whether it's file or directory, try both. The tests must
      // only create files or empty directories, so one must succeed, else the
      // directory's corrupted.
      Status s = env_->DeleteFile(test_dir_ + "/" + file);
      if (!s.ok()) {
        ASSERT_OK(env_->DeleteDir(test_dir_ + "/" + file));
      }
    }
  }
};

class EnvMoreTestWithParam : public EnvBasicTestWithParam {};

// next statements run env test against encrypt_2 code.
static std::string KeyName = {"A key name"};
static ShaDescription KeyDesc(KeyName);

// this key is from
// https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38a.pdf,
//  example F.5.5
static uint8_t key256[] = {0x60, 0x3d, 0xeb, 0x10, 0x15, 0xca, 0x71, 0xbe,
                           0x2b, 0x73, 0xae, 0xf0, 0x85, 0x7d, 0x77, 0x81,
                           0x1f, 0x35, 0x2c, 0x07, 0x3b, 0x61, 0x08, 0xd7,
                           0x2d, 0x98, 0x10, 0xa3, 0x09, 0x14, 0xdf, 0xf4};
std::shared_ptr<const CTREncryptionProviderV2> openssl_provider_ctr(
    new CTREncryptionProviderV2(KeyName, key256, 32));

static OpenSSLEnv::ReadKeys encrypt_readers = {{KeyDesc, openssl_provider_ctr}};
static OpenSSLEnv::WriteKey encrypt_writer = {KeyDesc, openssl_provider_ctr};

static std::unique_ptr<Env> openssl_env(new NormalizingEnvWrapper(
    OpenSSLEnv::Default(encrypt_readers, encrypt_writer)));

INSTANTIATE_TEST_CASE_P(OpenSSLEnv, EnvBasicTestWithParam,
                        ::testing::Values(openssl_env.get()));

TEST_P(EnvBasicTestWithParam, Basics) {
  uint64_t file_size;
  std::unique_ptr<WritableFile> writable_file;
  std::vector<std::string> children;

  // kill warning
  std::string warn(kEncryptMarker);
  warn.length();

  // Check that the directory is empty.
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/non_existent"));
  ASSERT_TRUE(!env_->GetFileSize(test_dir_ + "/non_existent", &file_size).ok());
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(0U, children.size());

  // Create a file.
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();

  // Check that the file exists.
  ASSERT_OK(env_->FileExists(test_dir_ + "/f"));
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/f", &file_size));
  ASSERT_EQ(0U, file_size);
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(1U, children.size());
  ASSERT_EQ("f", children[0]);
  ASSERT_OK(env_->DeleteFile(test_dir_ + "/f"));

  // Write to the file.
  ASSERT_OK(
      env_->NewWritableFile(test_dir_ + "/f1", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("abc"));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();
  ASSERT_OK(
      env_->NewWritableFile(test_dir_ + "/f2", &writable_file, soptions_));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();

  // Check for expected size.
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/f1", &file_size));
  ASSERT_EQ(3U, file_size);

  // Check that renaming works.
  ASSERT_TRUE(
      !env_->RenameFile(test_dir_ + "/non_existent", test_dir_ + "/g").ok());
  ASSERT_OK(env_->RenameFile(test_dir_ + "/f1", test_dir_ + "/g"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/f1"));
  ASSERT_OK(env_->FileExists(test_dir_ + "/g"));
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/g", &file_size));
  ASSERT_EQ(3U, file_size);

  // Check that renaming overwriting works
  ASSERT_OK(env_->RenameFile(test_dir_ + "/f2", test_dir_ + "/g"));
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/g", &file_size));
  ASSERT_EQ(0U, file_size);

  // Check that opening non-existent file fails.
  std::unique_ptr<SequentialFile> seq_file;
  std::unique_ptr<RandomAccessFile> rand_file;
  ASSERT_TRUE(!env_->NewSequentialFile(test_dir_ + "/non_existent", &seq_file,
                                       soptions_)
                   .ok());
  ASSERT_TRUE(!seq_file);
  ASSERT_TRUE(!env_->NewRandomAccessFile(test_dir_ + "/non_existent",
                                         &rand_file, soptions_)
                   .ok());
  ASSERT_TRUE(!rand_file);

  // Check that deleting works.
  ASSERT_TRUE(!env_->DeleteFile(test_dir_ + "/non_existent").ok());
  ASSERT_OK(env_->DeleteFile(test_dir_ + "/g"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/g"));
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(0U, children.size());
  ASSERT_TRUE(
      env_->GetChildren(test_dir_ + "/non_existent", &children).IsNotFound());
}

TEST_P(EnvBasicTestWithParam, ReadWrite) {
  std::unique_ptr<WritableFile> writable_file;
  std::unique_ptr<SequentialFile> seq_file;
  std::unique_ptr<RandomAccessFile> rand_file;
  Slice result;
  char scratch[100];

  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("hello "));
  ASSERT_OK(writable_file->Append("world"));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();

  // Read sequentially.
  ASSERT_OK(env_->NewSequentialFile(test_dir_ + "/f", &seq_file, soptions_));
  ASSERT_OK(seq_file->Read(5, &result, scratch));  // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_OK(seq_file->Skip(1));
  ASSERT_OK(seq_file->Read(1000, &result, scratch));  // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_OK(seq_file->Read(1000, &result, scratch));  // Try reading past EOF.
  ASSERT_EQ(0U, result.size());
  ASSERT_OK(seq_file->Skip(100));  // Try to skip past end of file.
  ASSERT_OK(seq_file->Read(1000, &result, scratch));
  ASSERT_EQ(0U, result.size());

  // Random reads.
  ASSERT_OK(env_->NewRandomAccessFile(test_dir_ + "/f", &rand_file, soptions_));
  ASSERT_OK(rand_file->Read(6, 5, &result, scratch));  // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_OK(rand_file->Read(0, 5, &result, scratch));  // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_OK(rand_file->Read(10, 100, &result, scratch));  // Read "d".
  ASSERT_EQ(0, result.compare("d"));

  // Too high offset.
  ASSERT_TRUE(rand_file->Read(1000, 5, &result, scratch).ok());
}

TEST_P(EnvBasicTestWithParam, Misc) {
  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/b", &writable_file, soptions_));

  // These are no-ops, but we test they return success.
  ASSERT_OK(writable_file->Sync());
  ASSERT_OK(writable_file->Flush());
  ASSERT_OK(writable_file->Close());
  writable_file.reset();
}

TEST_P(EnvBasicTestWithParam, LargeWrite) {
  const size_t kWriteSize = 300 * 1024;
  char* scratch = new char[kWriteSize * 2];

  std::string write_data;
  for (size_t i = 0; i < kWriteSize; ++i) {
    write_data.append(1, static_cast<char>(i));
  }

  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("foo"));
  ASSERT_OK(writable_file->Append(write_data));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();

  std::unique_ptr<SequentialFile> seq_file;
  Slice result;
  ASSERT_OK(env_->NewSequentialFile(test_dir_ + "/f", &seq_file, soptions_));
  ASSERT_OK(seq_file->Read(3, &result, scratch));  // Read "foo".
  ASSERT_EQ(0, result.compare("foo"));

  size_t read = 0;
  std::string read_data;
  while (read < kWriteSize) {
    ASSERT_OK(seq_file->Read(kWriteSize - read, &result, scratch));
    read_data.append(result.data(), result.size());
    read += result.size();
  }
  ASSERT_TRUE(write_data == read_data);
  delete[] scratch;
}

TEST_P(EnvMoreTestWithParam, GetModTime) {
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/dir1"));
  uint64_t mtime1 = 0x0;
  ASSERT_OK(env_->GetFileModificationTime(test_dir_ + "/dir1", &mtime1));
}

TEST_P(EnvMoreTestWithParam, MakeDir) {
  ASSERT_OK(env_->CreateDir(test_dir_ + "/j"));
  ASSERT_OK(env_->FileExists(test_dir_ + "/j"));
  std::vector<std::string> children;
  env_->GetChildren(test_dir_, &children);
  ASSERT_EQ(1U, children.size());
  // fail because file already exists
  ASSERT_TRUE(!env_->CreateDir(test_dir_ + "/j").ok());
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/j"));
  ASSERT_OK(env_->DeleteDir(test_dir_ + "/j"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/j"));
}

TEST_P(EnvMoreTestWithParam, GetChildren) {
  // empty folder returns empty vector
  std::vector<std::string> children;
  std::vector<Env::FileAttributes> childAttr;
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_));
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_OK(env_->FileExists(test_dir_));
  ASSERT_OK(env_->GetChildrenFileAttributes(test_dir_, &childAttr));
  ASSERT_EQ(0U, children.size());
  ASSERT_EQ(0U, childAttr.size());

  // folder with contents returns relative path to test dir
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/niu"));
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/you"));
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/guo"));
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_OK(env_->GetChildrenFileAttributes(test_dir_, &childAttr));
  ASSERT_EQ(3U, children.size());
  ASSERT_EQ(3U, childAttr.size());
  for (auto each : children) {
    env_->DeleteDir(test_dir_ + "/" + each);
  }  // necessary for default POSIX env

  // non-exist directory returns IOError
  ASSERT_OK(env_->DeleteDir(test_dir_));
  ASSERT_TRUE(!env_->FileExists(test_dir_).ok());
  ASSERT_TRUE(!env_->GetChildren(test_dir_, &children).ok());
  ASSERT_TRUE(!env_->GetChildrenFileAttributes(test_dir_, &childAttr).ok());

  // if dir is a file, returns IOError
  ASSERT_OK(env_->CreateDir(test_dir_));
  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(
      env_->NewWritableFile(test_dir_ + "/file", &writable_file, soptions_));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();
  ASSERT_TRUE(!env_->GetChildren(test_dir_ + "/file", &children).ok());
  ASSERT_EQ(0U, children.size());
}

class SstWriterBug : public testing::Test {
 public:
  std::string test_dir_;
  Env* env_default_ = Env::Default();

  SstWriterBug() {
    test_dir_ = test::PerThreadDBPath(env_default_, "env_openssl_test");
  }

  void SetUp() { env_default_->CreateDirIfMissing(test_dir_); }

  void TearDown() {
    std::vector<std::string> files;
    env_default_->GetChildren(test_dir_, &files);
    for (const auto& file : files) {
      // don't know whether it's file or directory, try both. The tests must
      // only create files or empty directories, so one must succeed, else the
      // directory's corrupted.
      Status s = env_default_->DeleteFile(test_dir_ + "/" + file);
      if (!s.ok()) {
        ASSERT_OK(env_default_->DeleteDir(test_dir_ + "/" + file));
      }
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_OPENSSL_AES_CTR
#endif  // ROCKSDB_LITE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
