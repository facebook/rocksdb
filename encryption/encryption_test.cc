// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#include "encryption/encryption.h"

#include <gtest/gtest.h>

#include "fuzz/util.h"
#include "port/stack_trace.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

#ifndef ROCKSDB_LITE
#ifdef OPENSSL

namespace ROCKSDB_NAMESPACE {
namespace encryption {

// Make sure the length of KEY is larger than the max KeySize(EncryptionMethod).
const unsigned char KEY[33] =
    "\xe4\x3e\x8e\xca\x2a\x83\xe1\x88\xfb\xd8\x02\xdc\xf3\x62\x65\x3e"
    "\x00\xee\x31\x39\xe7\xfd\x1d\x92\x20\xb1\x62\xae\xb2\xaf\x0f\x1a";

// Make sure the length of IV_RANDOM, IV_OVERFLOW_LOW and IV_OVERFLOW_FULL is
// larger than the max BlockSize(EncryptionMethod).
const unsigned char IV_RANDOM[17] =
    "\x77\x9b\x82\x72\x26\xb5\x76\x50\xf7\x05\xd2\xd6\xb8\xaa\xa9\x2c";
const unsigned char IV_OVERFLOW_LOW[17] =
    "\x77\x9b\x82\x72\x26\xb5\x76\x50\xff\xff\xff\xff\xff\xff\xff\xff";
const unsigned char IV_OVERFLOW_FULL[17] =
    "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";

TEST(EncryptionTest, KeySize) {
  ASSERT_EQ(16, KeySize(EncryptionMethod::kAES128_CTR));
  ASSERT_EQ(24, KeySize(EncryptionMethod::kAES192_CTR));
  ASSERT_EQ(32, KeySize(EncryptionMethod::kAES256_CTR));
  ASSERT_EQ(16, KeySize(EncryptionMethod::kSM4_CTR));
  ASSERT_EQ(0, KeySize(EncryptionMethod::kUnknown));
}

TEST(EncryptionTest, BlockSize) {
  ASSERT_EQ(16, BlockSize(EncryptionMethod::kAES128_CTR));
  ASSERT_EQ(16, BlockSize(EncryptionMethod::kAES192_CTR));
  ASSERT_EQ(16, BlockSize(EncryptionMethod::kAES256_CTR));
  ASSERT_EQ(16, BlockSize(EncryptionMethod::kSM4_CTR));
  ASSERT_EQ(0, BlockSize(EncryptionMethod::kUnknown));
}

TEST(EncryptionTest, EncryptionMethodStringToEnum) {
  ASSERT_EQ(EncryptionMethod::kAES128_CTR,
            EncryptionMethodStringToEnum("AES128CTR"));
  ASSERT_EQ(EncryptionMethod::kAES192_CTR,
            EncryptionMethodStringToEnum("AES192CTR"));
  ASSERT_EQ(EncryptionMethod::kAES256_CTR,
            EncryptionMethodStringToEnum("AES256CTR"));
  ASSERT_EQ(EncryptionMethod::kSM4_CTR, EncryptionMethodStringToEnum("SM4CTR"));
  ASSERT_EQ(EncryptionMethod::kUnknown,
            EncryptionMethodStringToEnum("unknown"));
}

// Test to make sure output of AESCTRCipherStream is the same as output from
// OpenSSL EVP API.
class EncryptionTest
    : public testing::TestWithParam<
          std::tuple<AESCTRCipherStream::EncryptType, EncryptionMethod>> {
 public:
  int kMaxSize;
  std::unique_ptr<unsigned char[]> plaintext;
  std::unique_ptr<unsigned char[]> ciphertext;
  const unsigned char* current_iv = nullptr;

  EncryptionTest() : kMaxSize(10 * BlockSize(std::get<1>(GetParam()))) {
    CHECK_OK(ReGenerateCiphertext(IV_RANDOM));
  }

  Status ReGenerateCiphertext(const unsigned char* iv) {
    current_iv = iv;

    Random rnd(test::RandomSeed());
    std::string random_string =
        rnd.HumanReadableString(static_cast<int>(kMaxSize));
    plaintext.reset(new unsigned char[kMaxSize]);
    memcpy(plaintext.get(), random_string.data(), kMaxSize);

    evp_ctx_unique_ptr ctx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);
    CHECK_TRUE(ctx);

    EncryptionMethod method = std::get<1>(GetParam());
    const EVP_CIPHER* cipher = GetEVPCipher(method);
    CHECK_TRUE(cipher != nullptr);

    OPENSSL_RET_NOT_OK(EVP_EncryptInit(ctx.get(), cipher, KEY, current_iv),
                       "EVP_EncryptInit failed.");
    int output_size = 0;
    ciphertext.reset(new unsigned char[kMaxSize]);
    OPENSSL_RET_NOT_OK(
        EVP_EncryptUpdate(ctx.get(), ciphertext.get(), &output_size,
                          plaintext.get(), static_cast<int>(kMaxSize)),
        "EVP_EncryptUpdate failed.");
    int final_output_size = 0;
    OPENSSL_RET_NOT_OK(
        EVP_EncryptFinal(ctx.get(), ciphertext.get() + output_size,
                         &final_output_size),
        "EVP_EncryptFinal failed.");
    CHECK_EQ(kMaxSize, output_size + final_output_size);
    return Status::OK();
  }

  void TestEncryption(size_t start, size_t end) {
    ASSERT_LT(start, end);
    ASSERT_LE(end, kMaxSize);

    EncryptionMethod method = std::get<1>(GetParam());
    std::string key_str(reinterpret_cast<const char*>(KEY), KeySize(method));
    std::string iv_str(reinterpret_cast<const char*>(current_iv),
                       BlockSize(method));
    std::unique_ptr<AESCTRCipherStream> cipher_stream;
    ASSERT_OK(NewAESCTRCipherStream(method, key_str, iv_str, &cipher_stream));
    ASSERT_TRUE(cipher_stream);

    size_t data_size = end - start;
    // Allocate exact size. AESCTRCipherStream should make sure there will be
    // no memory corruption.
    std::unique_ptr<char[]> data(new char[data_size]);

    if (std::get<0>(GetParam()) == AESCTRCipherStream::EncryptType::kEncrypt) {
      memcpy(data.get(), plaintext.get() + start, data_size);
      ASSERT_OK(cipher_stream->Encrypt(start, data.get(), data_size));
      ASSERT_EQ(0, memcmp(ciphertext.get() + start, data.get(), data_size));
    } else {
      ASSERT_EQ(AESCTRCipherStream::EncryptType::kDecrypt,
                std::get<0>(GetParam()));
      memcpy(data.get(), ciphertext.get() + start, data_size);
      ASSERT_OK(cipher_stream->Decrypt(start, data.get(), data_size));
      ASSERT_EQ(0, memcmp(plaintext.get() + start, data.get(), data_size));
    }
  }
};

TEST_P(EncryptionTest, AESCTRCipherStreamTest) {
  const size_t kBlockSize = BlockSize(std::get<1>(GetParam()));
  // TODO(yingchun): The following tests are based on the fact that the
  //  kBlockSize is 16, make sure they work if adding new encryption methods.
  ASSERT_EQ(kBlockSize, 16);

  // One full block.
  ASSERT_NO_FATAL_FAILURE(TestEncryption(0, kBlockSize));
  // One block in the middle.
  ASSERT_NO_FATAL_FAILURE(TestEncryption(kBlockSize * 5, kBlockSize * 6));
  // Multiple aligned blocks.
  ASSERT_NO_FATAL_FAILURE(TestEncryption(kBlockSize * 5, kBlockSize * 8));

  // Random byte at the beginning of a block.
  ASSERT_NO_FATAL_FAILURE(TestEncryption(kBlockSize * 5, kBlockSize * 5 + 1));
  // Random byte in the middle of a block.
  ASSERT_NO_FATAL_FAILURE(
      TestEncryption(kBlockSize * 5 + 4, kBlockSize * 5 + 5));
  // Random byte at the end of a block.
  ASSERT_NO_FATAL_FAILURE(TestEncryption(kBlockSize * 5 + 15, kBlockSize * 6));

  // Partial block aligned at the beginning.
  ASSERT_NO_FATAL_FAILURE(TestEncryption(kBlockSize * 5, kBlockSize * 5 + 15));
  // Partial block aligned at the end.
  ASSERT_NO_FATAL_FAILURE(TestEncryption(kBlockSize * 5 + 1, kBlockSize * 6));
  // Multiple blocks with a partial block at the end.
  ASSERT_NO_FATAL_FAILURE(TestEncryption(kBlockSize * 5, kBlockSize * 8 + 15));
  // Multiple blocks with a partial block at the beginning.
  ASSERT_NO_FATAL_FAILURE(TestEncryption(kBlockSize * 5 + 1, kBlockSize * 8));
  // Partial block at both ends.
  ASSERT_NO_FATAL_FAILURE(
      TestEncryption(kBlockSize * 5 + 1, kBlockSize * 8 + 15));

  // Lower bits of IV overflow.
  ASSERT_OK(ReGenerateCiphertext(IV_OVERFLOW_LOW));
  ASSERT_NO_FATAL_FAILURE(TestEncryption(kBlockSize, kBlockSize * 2));
  // Full IV overflow.
  ASSERT_OK(ReGenerateCiphertext(IV_OVERFLOW_FULL));
  ASSERT_NO_FATAL_FAILURE(TestEncryption(kBlockSize, kBlockSize * 2));
}

// Openssl support SM4 after 1.1.1 release version.
#if OPENSSL_VERSION_NUMBER < 0x1010100fL || defined(OPENSSL_NO_SM4)
INSTANTIATE_TEST_CASE_P(
    EncryptionTestInstance, EncryptionTest,
    testing::Combine(testing::Bool(),
                     testing::Values(EncryptionMethod::kAES128_CTR,
                                     EncryptionMethod::kAES192_CTR,
                                     EncryptionMethod::kAES256_CTR)));
#else
INSTANTIATE_TEST_CASE_P(
    EncryptionTestInstance, EncryptionTest,
    testing::Combine(testing::Values(AESCTRCipherStream::EncryptType::kEncrypt,
                                     AESCTRCipherStream::EncryptType::kDecrypt),
                     testing::Values(EncryptionMethod::kAES128_CTR,
                                     EncryptionMethod::kAES192_CTR,
                                     EncryptionMethod::kAES256_CTR,
                                     EncryptionMethod::kSM4_CTR)));
#endif

}  // namespace encryption
}  // namespace ROCKSDB_NAMESPACE

#endif  // OPENSSL
#endif  // !ROCKSDB_LITE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
