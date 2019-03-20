#include "utilities/titandb/util.h"
#include "util/testharness.h"

namespace rocksdb {
namespace titandb {

class UtilTest : public testing::Test {};

TEST(UtilTest, Compression) {
  std::string input(1024, 'a');
  for (auto compression :
       {kSnappyCompression, kZlibCompression, kLZ4Compression, kZSTD}) {
    CompressionContext compression_ctx(compression);
    std::string buffer;
    auto compressed = Compress(compression_ctx, input, &buffer, &compression);
    if (compression != kNoCompression) {
      ASSERT_TRUE(compressed.size() <= input.size());
      UncompressionContext uncompression_ctx(compression);
      OwnedSlice output;
      ASSERT_OK(Uncompress(uncompression_ctx, compressed, &output));
      ASSERT_EQ(output, input);
    }
  }
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
