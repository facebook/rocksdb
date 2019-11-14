//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstring>
#include <vector>

#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/hash.h"

using rocksdb::EncodeFixed32;
using rocksdb::GetSliceHash64;
using rocksdb::Hash;
using rocksdb::Hash64;
using rocksdb::Lower32of64;
using rocksdb::Upper32of64;
using rocksdb::Slice;

// The hash algorithm is part of the file format, for example for the Bloom
// filters. Test that the hash values are stable for a set of random strings of
// varying lengths.
TEST(HashTest, Values) {
  constexpr uint32_t kSeed = 0xbc9f1d34;  // Same as BloomHash.

  EXPECT_EQ(Hash("", 0, kSeed), 3164544308u);
  EXPECT_EQ(Hash("\x08", 1, kSeed), 422599524u);
  EXPECT_EQ(Hash("\x17", 1, kSeed), 3168152998u);
  EXPECT_EQ(Hash("\x9a", 1, kSeed), 3195034349u);
  EXPECT_EQ(Hash("\x1c", 1, kSeed), 2651681383u);
  EXPECT_EQ(Hash("\x4d\x76", 2, kSeed), 2447836956u);
  EXPECT_EQ(Hash("\x52\xd5", 2, kSeed), 3854228105u);
  EXPECT_EQ(Hash("\x91\xf7", 2, kSeed), 31066776u);
  EXPECT_EQ(Hash("\xd6\x27", 2, kSeed), 1806091603u);
  EXPECT_EQ(Hash("\x30\x46\x0b", 3, kSeed), 3808221797u);
  EXPECT_EQ(Hash("\x56\xdc\xd6", 3, kSeed), 2157698265u);
  EXPECT_EQ(Hash("\xd4\x52\x33", 3, kSeed), 1721992661u);
  EXPECT_EQ(Hash("\x6a\xb5\xf4", 3, kSeed), 2469105222u);
  EXPECT_EQ(Hash("\x67\x53\x81\x1c", 4, kSeed), 118283265u);
  EXPECT_EQ(Hash("\x69\xb8\xc0\x88", 4, kSeed), 3416318611u);
  EXPECT_EQ(Hash("\x1e\x84\xaf\x2d", 4, kSeed), 3315003572u);
  EXPECT_EQ(Hash("\x46\xdc\x54\xbe", 4, kSeed), 447346355u);
  EXPECT_EQ(Hash("\xd0\x7a\x6e\xea\x56", 5, kSeed), 4255445370u);
  EXPECT_EQ(Hash("\x86\x83\xd5\xa4\xd8", 5, kSeed), 2390603402u);
  EXPECT_EQ(Hash("\xb7\x46\xbb\x77\xce", 5, kSeed), 2048907743u);
  EXPECT_EQ(Hash("\x6c\xa8\xbc\xe5\x99", 5, kSeed), 2177978500u);
  EXPECT_EQ(Hash("\x5c\x5e\xe1\xa0\x73\x81", 6, kSeed), 1036846008u);
  EXPECT_EQ(Hash("\x08\x5d\x73\x1c\xe5\x2e", 6, kSeed), 229980482u);
  EXPECT_EQ(Hash("\x42\xfb\xf2\x52\xb4\x10", 6, kSeed), 3655585422u);
  EXPECT_EQ(Hash("\x73\xe1\xff\x56\x9c\xce", 6, kSeed), 3502708029u);
  EXPECT_EQ(Hash("\x5c\xbe\x97\x75\x54\x9a\x52", 7, kSeed), 815120748u);
  EXPECT_EQ(Hash("\x16\x82\x39\x49\x88\x2b\x36", 7, kSeed), 3056033698u);
  EXPECT_EQ(Hash("\x59\x77\xf0\xa7\x24\xf4\x78", 7, kSeed), 587205227u);
  EXPECT_EQ(Hash("\xd3\xa5\x7c\x0e\xc0\x02\x07", 7, kSeed), 2030937252u);
  EXPECT_EQ(Hash("\x31\x1b\x98\x75\x96\x22\xd3\x9a", 8, kSeed), 469635402u);
  EXPECT_EQ(Hash("\x38\xd6\xf7\x28\x20\xb4\x8a\xe9", 8, kSeed), 3530274698u);
  EXPECT_EQ(Hash("\xbb\x18\x5d\xf4\x12\x03\xf7\x99", 8, kSeed), 1974545809u);
  EXPECT_EQ(Hash("\x80\xd4\x3b\x3b\xae\x22\xa2\x78", 8, kSeed), 3563570120u);
  EXPECT_EQ(Hash("\x1a\xb5\xd0\xfe\xab\xc3\x61\xb2\x99", 9, kSeed),
            2706087434u);
  EXPECT_EQ(Hash("\x8e\x4a\xc3\x18\x20\x2f\x06\xe6\x3c", 9, kSeed),
            1534654151u);
  EXPECT_EQ(Hash("\xb6\xc0\xdd\x05\x3f\xc4\x86\x4c\xef", 9, kSeed),
            2355554696u);
  EXPECT_EQ(Hash("\x9a\x5f\x78\x0d\xaf\x50\xe1\x1f\x55", 9, kSeed),
            1400800912u);
  EXPECT_EQ(Hash("\x22\x6f\x39\x1f\xf8\xdd\x4f\x52\x17\x94", 10, kSeed),
            3420325137u);
  EXPECT_EQ(Hash("\x32\x89\x2a\x75\x48\x3a\x4a\x02\x69\xdd", 10, kSeed),
            3427803584u);
  EXPECT_EQ(Hash("\x06\x92\x5c\xf4\x88\x0e\x7e\x68\x38\x3e", 10, kSeed),
            1152407945u);
  EXPECT_EQ(Hash("\xbd\x2c\x63\x38\xbf\xe9\x78\xb7\xbf\x15", 10, kSeed),
            3382479516u);
}

// The hash algorithm is part of the file format, for example for the Bloom
// filters.
TEST(HashTest, Hash64Misc) {
  constexpr uint32_t kSeed = 0;  // Same as GetSliceHash64

  for (char fill : {'\0', 'a', '1', '\xff'}) {
    const size_t max_size = 1000;
    const std::string str(max_size, fill);

    for (size_t size = 0; size <= max_size; ++size) {
      uint64_t here = Hash64(str.data(), size, kSeed);

      // Must be same as GetSliceHash64
      EXPECT_EQ(here, GetSliceHash64(Slice(str.data(), size)));

      // Upper and Lower must reconstruct hash
      EXPECT_EQ(here, (uint64_t{Upper32of64(here)} << 32) | Lower32of64(here));
      EXPECT_EQ(here, (uint64_t{Upper32of64(here)} << 32) + Lower32of64(here));
      EXPECT_EQ(here, (uint64_t{Upper32of64(here)} << 32) ^ Lower32of64(here));

      // Seed changes hash value (with high probability)
      for (uint64_t var_seed = 1; var_seed != 0; var_seed <<= 1) {
        EXPECT_NE(here, Hash64(str.data(), size, var_seed));
      }

      // Size changes hash value (with high probability)
      size_t max_smaller_by = std::min(size_t{30}, size);
      for (size_t smaller_by = 1; smaller_by <= max_smaller_by; ++smaller_by) {
        EXPECT_NE(here, Hash64(str.data(), size - smaller_by, kSeed));
      }
    }
  }
}

// Test that hash values are "non-trivial" for "trivial" inputs
TEST(HashTest, Hash64Trivial) {
  // Thorough test too slow for regression testing
  constexpr bool thorough = false;

  // For various seeds, make sure hash of empty string is not zero.
  constexpr uint64_t max_seed = thorough ? 0x1000000 : 0x10000;
  for (uint64_t seed = 0; seed < max_seed; ++seed) {
    uint64_t here = Hash64("", 0, seed);
    EXPECT_NE(Lower32of64(here), 0u);
    EXPECT_NE(Upper32of64(here), 0u);
  }

  // For standard seed, make sure hash of small strings are not zero
  constexpr uint32_t kSeed = 0;  // Same as GetSliceHash64
  char input[4];
  constexpr int max_len = thorough ? 3 : 2;
  for (int len = 1; len <= max_len; ++len) {
    for (uint32_t i = 0; (i >> (len * 8)) == 0; ++i) {
      EncodeFixed32(input, i);
      uint64_t here = Hash64(input, len, kSeed);
      EXPECT_NE(Lower32of64(here), 0u);
      EXPECT_NE(Upper32of64(here), 0u);
    }
  }
}

// Test that the hash values are stable for a set of random strings of
// varying small lengths.
TEST(HashTest, Hash64SmallValueSchema) {
  constexpr uint32_t kSeed = 0;  // Same as GetSliceHash64

  EXPECT_EQ(Hash64("", 0, kSeed), uint64_t{5999572062939766020u});
  EXPECT_EQ(Hash64("\x08", 1, kSeed), uint64_t{583283813901344696u});
  EXPECT_EQ(Hash64("\x17", 1, kSeed), uint64_t{16175549975585474943u});
  EXPECT_EQ(Hash64("\x9a", 1, kSeed), uint64_t{16322991629225003903u});
  EXPECT_EQ(Hash64("\x1c", 1, kSeed), uint64_t{13269285487706833447u});
  EXPECT_EQ(Hash64("\x4d\x76", 2, kSeed), uint64_t{6859542833406258115u});
  EXPECT_EQ(Hash64("\x52\xd5", 2, kSeed), uint64_t{4919611532550636959u});
  EXPECT_EQ(Hash64("\x91\xf7", 2, kSeed), uint64_t{14199427467559720719u});
  EXPECT_EQ(Hash64("\xd6\x27", 2, kSeed), uint64_t{12292689282614532691u});
  EXPECT_EQ(Hash64("\x30\x46\x0b", 3, kSeed), uint64_t{11404699285340020889u});
  EXPECT_EQ(Hash64("\x56\xdc\xd6", 3, kSeed), uint64_t{12404347133785524237u});
  EXPECT_EQ(Hash64("\xd4\x52\x33", 3, kSeed), uint64_t{15853805298481534034u});
  EXPECT_EQ(Hash64("\x6a\xb5\xf4", 3, kSeed), uint64_t{16863488758399383382u});
  EXPECT_EQ(Hash64("\x67\x53\x81\x1c", 4, kSeed),
            uint64_t{9010661983527562386u});
  EXPECT_EQ(Hash64("\x69\xb8\xc0\x88", 4, kSeed),
            uint64_t{6611781377647041447u});
  EXPECT_EQ(Hash64("\x1e\x84\xaf\x2d", 4, kSeed),
            uint64_t{15290969111616346501u});
  EXPECT_EQ(Hash64("\x46\xdc\x54\xbe", 4, kSeed),
            uint64_t{7063754590279313623u});
  EXPECT_EQ(Hash64("\xd0\x7a\x6e\xea\x56", 5, kSeed),
            uint64_t{6384167718754869899u});
  EXPECT_EQ(Hash64("\x86\x83\xd5\xa4\xd8", 5, kSeed),
            uint64_t{16874407254108011067u});
  EXPECT_EQ(Hash64("\xb7\x46\xbb\x77\xce", 5, kSeed),
            uint64_t{16809880630149135206u});
  EXPECT_EQ(Hash64("\x6c\xa8\xbc\xe5\x99", 5, kSeed),
            uint64_t{1249038833153141148u});
  EXPECT_EQ(Hash64("\x5c\x5e\xe1\xa0\x73\x81", 6, kSeed),
            uint64_t{17358142495308219330u});
  EXPECT_EQ(Hash64("\x08\x5d\x73\x1c\xe5\x2e", 6, kSeed),
            uint64_t{4237646583134806322u});
  EXPECT_EQ(Hash64("\x42\xfb\xf2\x52\xb4\x10", 6, kSeed),
            uint64_t{4373664924115234051u});
  EXPECT_EQ(Hash64("\x73\xe1\xff\x56\x9c\xce", 6, kSeed),
            uint64_t{12012981210634596029u});
  EXPECT_EQ(Hash64("\x5c\xbe\x97\x75\x54\x9a\x52", 7, kSeed),
            uint64_t{5716522398211028826u});
  EXPECT_EQ(Hash64("\x16\x82\x39\x49\x88\x2b\x36", 7, kSeed),
            uint64_t{15604531309862565013u});
  EXPECT_EQ(Hash64("\x59\x77\xf0\xa7\x24\xf4\x78", 7, kSeed),
            uint64_t{8601330687345614172u});
  EXPECT_EQ(Hash64("\xd3\xa5\x7c\x0e\xc0\x02\x07", 7, kSeed),
            uint64_t{8088079329364056942u});
  EXPECT_EQ(Hash64("\x31\x1b\x98\x75\x96\x22\xd3\x9a", 8, kSeed),
            uint64_t{9844314944338447628u});
  EXPECT_EQ(Hash64("\x38\xd6\xf7\x28\x20\xb4\x8a\xe9", 8, kSeed),
            uint64_t{10973293517982163143u});
  EXPECT_EQ(Hash64("\xbb\x18\x5d\xf4\x12\x03\xf7\x99", 8, kSeed),
            uint64_t{9986007080564743219u});
  EXPECT_EQ(Hash64("\x80\xd4\x3b\x3b\xae\x22\xa2\x78", 8, kSeed),
            uint64_t{1729303145008254458u});
  EXPECT_EQ(Hash64("\x1a\xb5\xd0\xfe\xab\xc3\x61\xb2\x99", 9, kSeed),
            uint64_t{13253403748084181481u});
  EXPECT_EQ(Hash64("\x8e\x4a\xc3\x18\x20\x2f\x06\xe6\x3c", 9, kSeed),
            uint64_t{7768754303876232188u});
  EXPECT_EQ(Hash64("\xb6\xc0\xdd\x05\x3f\xc4\x86\x4c\xef", 9, kSeed),
            uint64_t{12439346786701492u});
  EXPECT_EQ(Hash64("\x9a\x5f\x78\x0d\xaf\x50\xe1\x1f\x55", 9, kSeed),
            uint64_t{10841838338450144690u});
  EXPECT_EQ(Hash64("\x22\x6f\x39\x1f\xf8\xdd\x4f\x52\x17\x94", 10, kSeed),
            uint64_t{12883919702069153152u});
  EXPECT_EQ(Hash64("\x32\x89\x2a\x75\x48\x3a\x4a\x02\x69\xdd", 10, kSeed),
            uint64_t{12692903507676842188u});
  EXPECT_EQ(Hash64("\x06\x92\x5c\xf4\x88\x0e\x7e\x68\x38\x3e", 10, kSeed),
            uint64_t{6540985900674032620u});
  EXPECT_EQ(Hash64("\xbd\x2c\x63\x38\xbf\xe9\x78\xb7\xbf\x15", 10, kSeed),
            uint64_t{10551812464348219044u});
}

std::string Hash64TestDescriptor(const char *repeat, size_t limit) {
  const char *mod61_encode =
      "abcdefghijklmnopqrstuvwxyz123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  std::string input;
  while (input.size() < limit) {
    input.append(repeat);
  }
  std::string rv;
  for (size_t i = 0; i < limit; ++i) {
    uint64_t h = GetSliceHash64(Slice(input.data(), i));
    rv.append(1, mod61_encode[static_cast<size_t>(h % 61)]);
  }
  return rv;
}

// XXH3p changes its algorithm for various sizes up through 250 bytes, so
// we need to check the stability of larger sizes also.
TEST(HashTest, Hash64LargeValueSchema) {
  // Each of these derives a "descriptor" from the hash values for all
  // lengths up to 430.
  // Note that "c" is common for the zero-length string.
  EXPECT_EQ(
      Hash64TestDescriptor("foo", 430),
      "cRhyWsY67B6klRA1udmOuiYuX7IthyGBKqbeosz2hzVglWCmQx8nEdnpkvPfYX56Up2OWOTV"
      "lTzfAoYwvtqKzjD8E9xttR2unelbXbIV67NUe6bOO23BxaSFRcA3njGu5cUWfgwOqNoTsszp"
      "uPvKRP6qaUR5VdoBkJUCFIefd7edlNK5mv6JYWaGdwxehg65hTkTmjZoPKxTZo4PLyzbL9U4"
      "xt12ITSfeP2MfBHuLI2z2pDlBb44UQKVMx27LEoAHsdLp3WfWfgH3sdRBRCHm33UxCM4QmE2"
      "xJ7gqSvNwTeH7v9GlC8zWbGroyD3UVNeShMLx29O7tH1biemLULwAHyIw8zdtLMDpEJ8m2ic"
      "l6Lb4fDuuFNAs1GCVUthjK8CV8SWI8Rsz5THSwn5CGhpqUwSZcFknjwWIl5rNCvDxXJqYr");
  // Note that "1EeRk" is common for "Rocks"
  EXPECT_EQ(
      Hash64TestDescriptor("Rocks", 430),
      "c1EeRkrzgOYWLA8PuhJrwTePJewoB44WdXYDfhbk3ZxTqqg25WlPExDl7IKIQLJvnA6gJxxn"
      "9TCSLkFGfJeXehaSS1GBqWSzfhEH4VXiXIUCuxJXxtKXcSC6FrNIQGTZbYDiUOLD6Y5inzrF"
      "9etwQhXUBanw55xAUdNMFQAm2GjJ6UDWp2mISLiMMkLjANWMKLaZMqaFLX37qB4MRO1ooVRv"
      "zSvaNRSCLxlggQCasQq8icWjzf3HjBlZtU6pd4rkaUxSzHqmo9oM5MghbU5Rtxg8wEfO7lVN"
      "5wdMONYecslQTwjZUpO1K3LDf3K3XK6sUXM6ShQQ3RHmMn2acB4YtTZ3QQcHYJSOHn2DuWpa"
      "Q8RqzX5lab92YmOLaCdOHq1BPsM7SIBzMdLgePNsJ1vvMALxAaoDUHPxoFLO2wx18IXnyX");
  EXPECT_EQ(
      Hash64TestDescriptor("RocksDB", 430),
      "c1EeRkukbkb28wLTahwD2sfUhZzaBEnF8SVrxnPVB6A7b8CaAl3UKsDZISF92GSq2wDCukOq"
      "Jgrsp7A3KZhDiLW8dFXp8UPqPxMCRlMdZeVeJ2dJxrmA6cyt99zkQFj7ELbut6jAeVqARFnw"
      "fnWVXOsaLrq7bDCbMcns2DKvTaaqTCLMYxI7nhtLpFN1jR755FRQFcOzrrDbh7QhypjdvlYw"
      "cdAMSZgp9JMHxbM23wPSuH6BOFgxejz35PScZfhDPvTOxIy1jc3MZsWrMC3P324zNolO7JdW"
      "CX2I5UDKjjaEJfxbgVgJIXxtQGlmj2xkO5sPpjULQV4X2HlY7FQleJ4QRaJIB4buhCA4vUTF"
      "eMFlxCIYUpTCsal2qsmnGOWa8WCcefrohMjDj1fjzSvSaQwlpyR1GZHF2uPOoQagiCpHpm");
}

TEST(Fastrange32Test, Values) {
  using rocksdb::fastrange32;
  // Zero range
  EXPECT_EQ(fastrange32(0, 0), 0U);
  EXPECT_EQ(fastrange32(123, 0), 0U);
  EXPECT_EQ(fastrange32(0xffffffff, 0), 0U);

  // One range
  EXPECT_EQ(fastrange32(0, 1), 0U);
  EXPECT_EQ(fastrange32(123, 1), 0U);
  EXPECT_EQ(fastrange32(0xffffffff, 1), 0U);

  // Two range
  EXPECT_EQ(fastrange32(0, 2), 0U);
  EXPECT_EQ(fastrange32(123, 2), 0U);
  EXPECT_EQ(fastrange32(0x7fffffff, 2), 0U);
  EXPECT_EQ(fastrange32(0x80000000, 2), 1U);
  EXPECT_EQ(fastrange32(0xffffffff, 2), 1U);

  // Seven range
  EXPECT_EQ(fastrange32(0, 7), 0U);
  EXPECT_EQ(fastrange32(123, 7), 0U);
  EXPECT_EQ(fastrange32(613566756, 7), 0U);
  EXPECT_EQ(fastrange32(613566757, 7), 1U);
  EXPECT_EQ(fastrange32(1227133513, 7), 1U);
  EXPECT_EQ(fastrange32(1227133514, 7), 2U);
  // etc.
  EXPECT_EQ(fastrange32(0xffffffff, 7), 6U);

  // Big
  EXPECT_EQ(fastrange32(1, 0x80000000), 0U);
  EXPECT_EQ(fastrange32(2, 0x80000000), 1U);
  EXPECT_EQ(fastrange32(4, 0x7fffffff), 1U);
  EXPECT_EQ(fastrange32(4, 0x80000000), 2U);
  EXPECT_EQ(fastrange32(0xffffffff, 0x7fffffff), 0x7ffffffeU);
  EXPECT_EQ(fastrange32(0xffffffff, 0x80000000), 0x7fffffffU);
}

TEST(Fastrange64Test, Values) {
  using rocksdb::fastrange64;
  // Zero range
  EXPECT_EQ(fastrange64(0, 0), 0U);
  EXPECT_EQ(fastrange64(123, 0), 0U);
  EXPECT_EQ(fastrange64(0xffffFFFF, 0), 0U);
  EXPECT_EQ(fastrange64(0xffffFFFFffffFFFF, 0), 0U);

  // One range
  EXPECT_EQ(fastrange64(0, 1), 0U);
  EXPECT_EQ(fastrange64(123, 1), 0U);
  EXPECT_EQ(fastrange64(0xffffFFFF, 1), 0U);
  EXPECT_EQ(fastrange64(0xffffFFFFffffFFFF, 1), 0U);

  // Two range
  EXPECT_EQ(fastrange64(0, 2), 0U);
  EXPECT_EQ(fastrange64(123, 2), 0U);
  EXPECT_EQ(fastrange64(0xffffFFFF, 2), 0U);
  EXPECT_EQ(fastrange64(0x7fffFFFFffffFFFF, 2), 0U);
  EXPECT_EQ(fastrange64(0x8000000000000000, 2), 1U);
  EXPECT_EQ(fastrange64(0xffffFFFFffffFFFF, 2), 1U);

  // Seven range
  EXPECT_EQ(fastrange64(0, 7), 0U);
  EXPECT_EQ(fastrange64(123, 7), 0U);
  EXPECT_EQ(fastrange64(0xffffFFFF, 7), 0U);
  EXPECT_EQ(fastrange64(2635249153387078802, 7), 0U);
  EXPECT_EQ(fastrange64(2635249153387078803, 7), 1U);
  EXPECT_EQ(fastrange64(5270498306774157604, 7), 1U);
  EXPECT_EQ(fastrange64(5270498306774157605, 7), 2U);
  EXPECT_EQ(fastrange64(0x7fffFFFFffffFFFF, 7), 3U);
  EXPECT_EQ(fastrange64(0x8000000000000000, 7), 3U);
  EXPECT_EQ(fastrange64(0xffffFFFFffffFFFF, 7), 6U);

  // Big but 32-bit range
  EXPECT_EQ(fastrange64(0x100000000, 0x80000000), 0U);
  EXPECT_EQ(fastrange64(0x200000000, 0x80000000), 1U);
  EXPECT_EQ(fastrange64(0x400000000, 0x7fffFFFF), 1U);
  EXPECT_EQ(fastrange64(0x400000000, 0x80000000), 2U);
  EXPECT_EQ(fastrange64(0xffffFFFFffffFFFF, 0x7fffFFFF), 0x7fffFFFEU);
  EXPECT_EQ(fastrange64(0xffffFFFFffffFFFF, 0x80000000), 0x7fffFFFFU);

  // Big, > 32-bit range
#if SIZE_MAX == UINT64_MAX
  EXPECT_EQ(fastrange64(0x7fffFFFFffffFFFF, 0x4200000002), 0x2100000000U);
  EXPECT_EQ(fastrange64(0x8000000000000000, 0x4200000002), 0x2100000001U);

  EXPECT_EQ(fastrange64(0x0000000000000000, 420000000002), 0U);
  EXPECT_EQ(fastrange64(0x7fffFFFFffffFFFF, 420000000002), 210000000000U);
  EXPECT_EQ(fastrange64(0x8000000000000000, 420000000002), 210000000001U);
  EXPECT_EQ(fastrange64(0xffffFFFFffffFFFF, 420000000002), 420000000001U);

  EXPECT_EQ(fastrange64(0xffffFFFFffffFFFF, 0xffffFFFFffffFFFF),
            0xffffFFFFffffFFFEU);
#endif
}

// for inspection of disassembly
uint32_t fastrange32(uint32_t hash, uint32_t range) {
  return rocksdb::fastrange32(hash, range);
}

// for inspection of disassembly
size_t fastrange64(uint64_t hash, size_t range) {
  return rocksdb::fastrange64(hash, range);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
