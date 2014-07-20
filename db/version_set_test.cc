//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class GenerateFileLevelTest {
 public:
  std::vector<FileMetaData*> files_;
  FileLevel file_level_;
  Arena arena_;

  GenerateFileLevelTest() { }

  ~GenerateFileLevelTest() {
    for (unsigned int i = 0; i < files_.size(); i++) {
      delete files_[i];
    }
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    FileMetaData* f = new FileMetaData;
    f->fd = FileDescriptor(files_.size() + 1, 0, 0);
    f->smallest = InternalKey(smallest, smallest_seq, kTypeValue);
    f->largest = InternalKey(largest, largest_seq, kTypeValue);
    files_.push_back(f);
  }

  int Compare() {
    int diff = 0;
    for (size_t i = 0; i < files_.size(); i++) {
      if (file_level_.files[i].fd.GetNumber() != files_[i]->fd.GetNumber()) {
        diff++;
      }
    }
    return diff;
  }
};

TEST(GenerateFileLevelTest, Empty) {
  DoGenerateFileLevel(&file_level_, files_, &arena_);
  ASSERT_EQ(0u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

TEST(GenerateFileLevelTest, Single) {
  Add("p", "q");
  DoGenerateFileLevel(&file_level_, files_, &arena_);
  ASSERT_EQ(1u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}


TEST(GenerateFileLevelTest, Multiple) {
  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  DoGenerateFileLevel(&file_level_, files_, &arena_);
  ASSERT_EQ(4u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

class FindLevelFileTest {
 public:
  FileLevel file_level_;
  bool disjoint_sorted_files_;
  Arena arena_;

  FindLevelFileTest() : disjoint_sorted_files_(true) { }

  ~FindLevelFileTest() {
  }

  void LevelFileInit(size_t num = 0) {
    char* mem = arena_.AllocateAligned(num * sizeof(FdWithKeyRange));
    file_level_.files = new (mem)FdWithKeyRange[num];
    file_level_.num_files = 0;
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    InternalKey smallest_key = InternalKey(smallest, smallest_seq, kTypeValue);
    InternalKey largest_key = InternalKey(largest, largest_seq, kTypeValue);

    Slice smallest_slice = smallest_key.Encode();
    Slice largest_slice = largest_key.Encode();

    char* mem = arena_.AllocateAligned(
        smallest_slice.size() + largest_slice.size());
    memcpy(mem, smallest_slice.data(), smallest_slice.size());
    memcpy(mem + smallest_slice.size(), largest_slice.data(),
        largest_slice.size());

    // add to file_level_
    size_t num = file_level_.num_files;
    auto& file = file_level_.files[num];
    file.fd = FileDescriptor(num + 1, 0, 0);
    file.smallest_key = Slice(mem, smallest_slice.size());
    file.largest_key = Slice(mem + smallest_slice.size(),
        largest_slice.size());
    file_level_.num_files++;
  }

  int Find(const char* key) {
    InternalKey target(key, 100, kTypeValue);
    InternalKeyComparator cmp(BytewiseComparator());
    return FindFile(cmp, file_level_, target.Encode());
  }

  bool Overlaps(const char* smallest, const char* largest) {
    InternalKeyComparator cmp(BytewiseComparator());
    Slice s(smallest != nullptr ? smallest : "");
    Slice l(largest != nullptr ? largest : "");
    return SomeFileOverlapsRange(cmp, disjoint_sorted_files_, file_level_,
                                 (smallest != nullptr ? &s : nullptr),
                                 (largest != nullptr ? &l : nullptr));
  }
};

TEST(FindLevelFileTest, LevelEmpty) {
  LevelFileInit(0);

  ASSERT_EQ(0, Find("foo"));
  ASSERT_TRUE(! Overlaps("a", "z"));
  ASSERT_TRUE(! Overlaps(nullptr, "z"));
  ASSERT_TRUE(! Overlaps("a", nullptr));
  ASSERT_TRUE(! Overlaps(nullptr, nullptr));
}

TEST(FindLevelFileTest, LevelSingle) {
  LevelFileInit(1);

  Add("p", "q");
  ASSERT_EQ(0, Find("a"));
  ASSERT_EQ(0, Find("p"));
  ASSERT_EQ(0, Find("p1"));
  ASSERT_EQ(0, Find("q"));
  ASSERT_EQ(1, Find("q1"));
  ASSERT_EQ(1, Find("z"));

  ASSERT_TRUE(! Overlaps("a", "b"));
  ASSERT_TRUE(! Overlaps("z1", "z2"));
  ASSERT_TRUE(Overlaps("a", "p"));
  ASSERT_TRUE(Overlaps("a", "q"));
  ASSERT_TRUE(Overlaps("a", "z"));
  ASSERT_TRUE(Overlaps("p", "p1"));
  ASSERT_TRUE(Overlaps("p", "q"));
  ASSERT_TRUE(Overlaps("p", "z"));
  ASSERT_TRUE(Overlaps("p1", "p2"));
  ASSERT_TRUE(Overlaps("p1", "z"));
  ASSERT_TRUE(Overlaps("q", "q"));
  ASSERT_TRUE(Overlaps("q", "q1"));

  ASSERT_TRUE(! Overlaps(nullptr, "j"));
  ASSERT_TRUE(! Overlaps("r", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "p"));
  ASSERT_TRUE(Overlaps(nullptr, "p1"));
  ASSERT_TRUE(Overlaps("q", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
}

TEST(FindLevelFileTest, LevelMultiple) {
  LevelFileInit(4);

  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_EQ(0, Find("100"));
  ASSERT_EQ(0, Find("150"));
  ASSERT_EQ(0, Find("151"));
  ASSERT_EQ(0, Find("199"));
  ASSERT_EQ(0, Find("200"));
  ASSERT_EQ(1, Find("201"));
  ASSERT_EQ(1, Find("249"));
  ASSERT_EQ(1, Find("250"));
  ASSERT_EQ(2, Find("251"));
  ASSERT_EQ(2, Find("299"));
  ASSERT_EQ(2, Find("300"));
  ASSERT_EQ(2, Find("349"));
  ASSERT_EQ(2, Find("350"));
  ASSERT_EQ(3, Find("351"));
  ASSERT_EQ(3, Find("400"));
  ASSERT_EQ(3, Find("450"));
  ASSERT_EQ(4, Find("451"));

  ASSERT_TRUE(! Overlaps("100", "149"));
  ASSERT_TRUE(! Overlaps("251", "299"));
  ASSERT_TRUE(! Overlaps("451", "500"));
  ASSERT_TRUE(! Overlaps("351", "399"));

  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
}

TEST(FindLevelFileTest, LevelMultipleNullBoundaries) {
  LevelFileInit(4);

  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_TRUE(! Overlaps(nullptr, "149"));
  ASSERT_TRUE(! Overlaps("451", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "150"));
  ASSERT_TRUE(Overlaps(nullptr, "199"));
  ASSERT_TRUE(Overlaps(nullptr, "200"));
  ASSERT_TRUE(Overlaps(nullptr, "201"));
  ASSERT_TRUE(Overlaps(nullptr, "400"));
  ASSERT_TRUE(Overlaps(nullptr, "800"));
  ASSERT_TRUE(Overlaps("100", nullptr));
  ASSERT_TRUE(Overlaps("200", nullptr));
  ASSERT_TRUE(Overlaps("449", nullptr));
  ASSERT_TRUE(Overlaps("450", nullptr));
}

TEST(FindLevelFileTest, LevelOverlapSequenceChecks) {
  LevelFileInit(1);

  Add("200", "200", 5000, 3000);
  ASSERT_TRUE(! Overlaps("199", "199"));
  ASSERT_TRUE(! Overlaps("201", "300"));
  ASSERT_TRUE(Overlaps("200", "200"));
  ASSERT_TRUE(Overlaps("190", "200"));
  ASSERT_TRUE(Overlaps("200", "210"));
}

TEST(FindLevelFileTest, LevelOverlappingFiles) {
  LevelFileInit(2);

  Add("150", "600");
  Add("400", "500");
  disjoint_sorted_files_ = false;
  ASSERT_TRUE(! Overlaps("100", "149"));
  ASSERT_TRUE(! Overlaps("601", "700"));
  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
  ASSERT_TRUE(Overlaps("450", "700"));
  ASSERT_TRUE(Overlaps("600", "700"));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}
