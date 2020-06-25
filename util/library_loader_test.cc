//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <gtest/gtest.h>

#ifdef ROCKSDB_OPENSSL_AES_CTR
#ifndef ROCKSDB_LITE

#include "util/library_loader.h"

namespace ROCKSDB_NAMESPACE {

#ifdef OS_MACOSX
static const char* LIB_M_NAME = "libm";
static const char* LIB_BAD_NAME = "libbubbagump.dylib";
static const char* LIB_SSL_NAME = "ssl";
#else
static const char* LIB_M_NAME = "libm.so.6";
static const char* LIB_BAD_NAME = "libbubbagump.so";
static const char* LIB_SSL_NAME = "ssl";
#endif

class UnixLibraryLoaderTest {};

TEST(UnixLibraryLoaderTest, Simple) {
  LibraryLoader works(LIB_M_NAME);
  LibraryLoader fails(LIB_BAD_NAME);

  ASSERT_TRUE(works.IsValid());
  ASSERT_FALSE(fails.IsValid());

  double (*floor)(double);

  floor = (double (*)(double))works.GetEntryPoint("floor");
  ASSERT_TRUE(nullptr != floor);
  ASSERT_TRUE(2.0 == (*floor)(2.2));
}

TEST(UnixLibraryLoaderTest, SSL) {
  LibraryLoader ssl(LIB_SSL_NAME);
  LibraryLoader crypto(UnixLibCrypto::crypto_lib_name_);

  ASSERT_TRUE(ssl.IsValid());
  ASSERT_TRUE(crypto.IsValid());
}

TEST(UnixLibraryLoaderTest, Crypto) {
  UnixLibCrypto crypto;
  uint8_t desc[EVP_MAX_MD_SIZE];
  EVP_MD_CTX* context;
  int ret_val;
  unsigned len;

  ASSERT_TRUE(crypto.IsValid());

  //  context = crypto.EVP_MD_CTX_create();  ... old call
  context = crypto.EVP_MD_CTX_new();  //  new call
  ASSERT_TRUE(nullptr != context);

  ret_val = crypto.EVP_DigestInit_ex(context, crypto.EVP_sha1(), nullptr);
  ASSERT_TRUE(1 == ret_val);

  ret_val = crypto.EVP_DigestUpdate(context, "1", 1);
  ASSERT_TRUE(1 == ret_val);

  ret_val = crypto.EVP_DigestFinal_ex(context, desc, &len);
  ASSERT_TRUE(1 == ret_val);
  ASSERT_TRUE(20 == len);

  uint8_t md2[] = {0x35, 0x6a, 0x19, 0x2b, 0x79, 0x13, 0xb0, 0x4c, 0x54, 0x57,
                   0x4d, 0x18, 0xc2, 0x8d, 0x46, 0xe6, 0x39, 0x54, 0x28, 0xab};
  ASSERT_TRUE(0 == memcmp(md2, desc, sizeof(md2)));

  crypto.EVP_MD_CTX_free(context);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
#endif  // ROCKSDB_OPENSSL_AES_CTR

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
