// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#include "encryption/encryption.h"

#include "port/stack_trace.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

#ifndef ROCKSDB_LITE
#ifdef OPENSSL

namespace ROCKSDB_NAMESPACE {
namespace encryption {

const unsigned char KEY[33] =
    "\xe4\x3e\x8e\xca\x2a\x83\xe1\x88\xfb\xd8\x02\xdc\xf3\x62\x65\x3e"
    "\x00\xee\x31\x39\xe7\xfd\x1d\x92\x20\xb1\x62\xae\xb2\xaf\x0f\x1a";
const unsigned char IV_RANDOM[17] =
    "\x77\x9b\x82\x72\x26\xb5\x76\x50\xf7\x05\xd2\xd6\xb8\xaa\xa9\x2c";
const unsigned char IV_OVERFLOW_LOW[17] =
    "\x77\x9b\x82\x72\x26\xb5\x76\x50\xff\xff\xff\xff\xff\xff\xff\xff";
const unsigned char IV_OVERFLOW_FULL[17] =
    "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";

constexpr size_t MAX_SIZE = 16 * 10;

// Test to make sure output of AESCTRCipherStream is the same as output from
// OpenSSL EVP API.
class EncryptionTest
    : public testing::TestWithParam<std::tuple<bool, EncryptionMethod>> {
 public:
  unsigned char plaintext[MAX_SIZE];
  // Reserve a bit more room to make sure OpenSSL have enough buffer.
  unsigned char ciphertext[MAX_SIZE + 16 * 2];

  void GenerateCiphertext(const unsigned char* iv) {
    Random rnd(666);
    std::string random_string =
        rnd.HumanReadableString(static_cast<int>(MAX_SIZE));
    memcpy(plaintext, random_string.data(), MAX_SIZE);

    int ret = 1;
    EVP_CIPHER_CTX* ctx;
    InitCipherContext(ctx);
    assert(ctx != nullptr);

    const EVP_CIPHER* cipher = nullptr;
    EncryptionMethod method = std::get<1>(GetParam());
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
#if OPENSSL_VERSION_NUMBER >= 0x1010100fL && !defined(OPENSSL_NO_SM4)
      // Openssl support SM4 after 1.1.1 release version.
      case EncryptionMethod::kSM4_CTR:
        cipher = EVP_sm4_ctr();
        break;
#endif
      default:
        assert(false);
    }
    assert(cipher != nullptr);

    ret = EVP_EncryptInit(ctx, cipher, KEY, iv);
    assert(ret == 1);
    int output_size = 0;
    ret = EVP_EncryptUpdate(ctx, ciphertext, &output_size, plaintext,
                            static_cast<int>(MAX_SIZE));
    assert(ret == 1);
    int final_output_size = 0;
    ret = EVP_EncryptFinal(ctx, ciphertext + output_size, &final_output_size);
    assert(ret == 1);
    assert(output_size + final_output_size == MAX_SIZE);
    FreeCipherContext(ctx);
  }

  void TestEncryptionImpl(size_t start, size_t end, const unsigned char* iv,
                          bool* success) {
    assert(start < end && end <= MAX_SIZE);
    GenerateCiphertext(iv);

    EncryptionMethod method = std::get<1>(GetParam());
    std::string key_str(reinterpret_cast<const char*>(KEY), KeySize(method));
    std::string iv_str(reinterpret_cast<const char*>(iv), 16);
    std::unique_ptr<AESCTRCipherStream> cipher_stream;
    ASSERT_OK(NewAESCTRCipherStream(method, key_str, iv_str, &cipher_stream));

    size_t data_size = end - start;
    // Allocate exact size. AESCTRCipherStream should make sure there will be
    // no memory corruption.
    std::unique_ptr<char[]> data(new char[data_size]);

    if (std::get<0>(GetParam())) {
      // Encrypt
      memcpy(data.get(), plaintext + start, data_size);
      ASSERT_OK(cipher_stream->Encrypt(start, data.get(), data_size));
      ASSERT_EQ(0, memcmp(ciphertext + start, data.get(), data_size));
    } else {
      // Decrypt
      memcpy(data.get(), ciphertext + start, data_size);
      ASSERT_OK(cipher_stream->Decrypt(start, data.get(), data_size));
      ASSERT_EQ(0, memcmp(plaintext + start, data.get(), data_size));
    }

    *success = true;
  }

  bool TestEncryption(size_t start, size_t end,
                      const unsigned char* iv = IV_RANDOM) {
    // Workaround failure of ASSERT_* result in return immediately.
    bool success = false;
    TestEncryptionImpl(start, end, iv, &success);
    return success;
  }
};

TEST_P(EncryptionTest, EncryptionTest) {
  // One full block.
  EXPECT_TRUE(TestEncryption(0, 16));
  // One block in the middle.
  EXPECT_TRUE(TestEncryption(16 * 5, 16 * 6));
  // Multiple aligned blocks.
  EXPECT_TRUE(TestEncryption(16 * 5, 16 * 8));

  // Random byte at the beginning of a block.
  EXPECT_TRUE(TestEncryption(16 * 5, 16 * 5 + 1));
  // Random byte in the middle of a block.
  EXPECT_TRUE(TestEncryption(16 * 5 + 4, 16 * 5 + 5));
  // Random byte at the end of a block.
  EXPECT_TRUE(TestEncryption(16 * 5 + 15, 16 * 6));

  // Partial block aligned at the beginning.
  EXPECT_TRUE(TestEncryption(16 * 5, 16 * 5 + 15));
  // Partial block aligned at the end.
  EXPECT_TRUE(TestEncryption(16 * 5 + 1, 16 * 6));
  // Multiple blocks with a partial block at the end.
  EXPECT_TRUE(TestEncryption(16 * 5, 16 * 8 + 15));
  // Multiple blocks with a partial block at the beginning.
  EXPECT_TRUE(TestEncryption(16 * 5 + 1, 16 * 8));
  // Partial block at both ends.
  EXPECT_TRUE(TestEncryption(16 * 5 + 1, 16 * 8 + 15));

  // Lower bits of IV overflow.
  EXPECT_TRUE(TestEncryption(16, 16 * 2, IV_OVERFLOW_LOW));
  // Full IV overflow.
  EXPECT_TRUE(TestEncryption(16, 16 * 2, IV_OVERFLOW_FULL));
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
    testing::Combine(testing::Bool(),
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
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
