// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace leveldb {

class FindFileTest {
 public:
  std::vector<FileMetaData*> files_;
  bool disjoint_sorted_files_;

  FindFileTest() : disjoint_sorted_files_(true) { }

  ~FindFileTest() {
    for (unsigned int i = 0; i < files_.size(); i++) {
      delete files_[i];
    }
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    FileMetaData* f = new FileMetaData;
    f->number = files_.size() + 1;
    f->smallest = InternalKey(smallest, smallest_seq, kTypeValue);
    f->largest = InternalKey(largest, largest_seq, kTypeValue);
    files_.push_back(f);
  }

  int Find(const char* key) {
    InternalKey target(key, 100, kTypeValue);
    InternalKeyComparator cmp(BytewiseComparator());
    return FindFile(cmp, files_, target.Encode());
  }

  bool Overlaps(const char* smallest, const char* largest) {
    InternalKeyComparator cmp(BytewiseComparator());
    Slice s(smallest != NULL ? smallest : "");
    Slice l(largest != NULL ? largest : "");
    return SomeFileOverlapsRange(cmp, disjoint_sorted_files_, files_,
                                 (smallest != NULL ? &s : NULL),
                                 (largest != NULL ? &l : NULL));
  }
};

TEST(FindFileTest, Empty) {
  ASSERT_EQ(0, Find("foo"));
  ASSERT_TRUE(! Overlaps("a", "z"));
  ASSERT_TRUE(! Overlaps(NULL, "z"));
  ASSERT_TRUE(! Overlaps("a", NULL));
  ASSERT_TRUE(! Overlaps(NULL, NULL));
}

TEST(FindFileTest, Single) {
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

  ASSERT_TRUE(! Overlaps(NULL, "j"));
  ASSERT_TRUE(! Overlaps("r", NULL));
  ASSERT_TRUE(Overlaps(NULL, "p"));
  ASSERT_TRUE(Overlaps(NULL, "p1"));
  ASSERT_TRUE(Overlaps("q", NULL));
  ASSERT_TRUE(Overlaps(NULL, NULL));
}


TEST(FindFileTest, Multiple) {
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

TEST(FindFileTest, MultipleNullBoundaries) {
  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_TRUE(! Overlaps(NULL, "149"));
  ASSERT_TRUE(! Overlaps("451", NULL));
  ASSERT_TRUE(Overlaps(NULL, NULL));
  ASSERT_TRUE(Overlaps(NULL, "150"));
  ASSERT_TRUE(Overlaps(NULL, "199"));
  ASSERT_TRUE(Overlaps(NULL, "200"));
  ASSERT_TRUE(Overlaps(NULL, "201"));
  ASSERT_TRUE(Overlaps(NULL, "400"));
  ASSERT_TRUE(Overlaps(NULL, "800"));
  ASSERT_TRUE(Overlaps("100", NULL));
  ASSERT_TRUE(Overlaps("200", NULL));
  ASSERT_TRUE(Overlaps("449", NULL));
  ASSERT_TRUE(Overlaps("450", NULL));
}

TEST(FindFileTest, OverlapSequenceChecks) {
  Add("200", "200", 5000, 3000);
  ASSERT_TRUE(! Overlaps("199", "199"));
  ASSERT_TRUE(! Overlaps("201", "300"));
  ASSERT_TRUE(Overlaps("200", "200"));
  ASSERT_TRUE(Overlaps("190", "200"));
  ASSERT_TRUE(Overlaps("200", "210"));
}

TEST(FindFileTest, OverlappingFiles) {
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

class FileGroupTest {
 public:
  std::vector<FileMetaData*> files_;
  std::vector<FileGroup*> groups_;

  FileGroupTest() { }

  ~FileGroupTest() {
    for (unsigned int i = 0; i < groups_.size(); i++) {
      delete groups_[i];
    }

    for (unsigned int i = 0; i < files_.size(); i++) {
      delete files_[i];
    }
  }

  void AddFile(const char* smallest, const char* largest,
               SequenceNumber smallest_seq = 100,
               SequenceNumber largest_seq = 100) {
    FileMetaData* f = new FileMetaData;
    f->number = files_.size() + 1;
    f->smallest = InternalKey(smallest, smallest_seq, kTypeValue);
    f->largest = InternalKey(largest, largest_seq, kTypeValue);
    f->file_size = smallest_seq + (largest_seq - smallest_seq)/2;
    files_.push_back(f);
  }

  void RebuildGroups() {
    for (unsigned int i = 0; i < groups_.size(); i++) {
      delete groups_[i];
    }

    InternalKeyComparator cmp(BytewiseComparator());
    GroupFiles(&cmp, files_, groups_);

    CheckGroupsValid();
  }

  size_t Find(const char* key, SequenceNumber seq = 100) {
    InternalKey target(key, seq, kTypeValue);
    InternalKeyComparator cmp(BytewiseComparator());
    return FindGroup(cmp, groups_, target.Encode());
  }

  bool Overlaps(const char* smallest, const char* largest) {
    InternalKeyComparator cmp(BytewiseComparator());
    Slice s(smallest != NULL ? smallest : "");
    Slice l(largest != NULL ? largest : "");
    return SomeGroupOverlapsRange(cmp, groups_,
                                 (smallest != NULL ? &s : NULL),
                                 (largest != NULL ? &l : NULL));
  }

  void CheckGroupsValid() {
    InternalKeyComparator cmp(BytewiseComparator());

    // Check group internal consistency.
    for (size_t i = 0; i < groups_.size(); ++i) {
      FileGroup& group = *groups_[i];

      // Check all groups are non-empty
      ASSERT_TRUE(!group.files.empty());

      // Check files in all groups are sorted by smallest key.
      for (size_t j = 0; j < group.files.size()-1; ++j) {
        ASSERT_TRUE(cmp.Compare(group.files[j]->smallest,
                                group.files[j+1]->smallest) < 0);
      }

      // Check smallest key in group is correct.
      ASSERT_TRUE(cmp.Compare(group.smallest, group.files[0]->smallest) == 0);

      // Check files in group actually overlap.
      InternalKey largest_so_far = group.files[0]->largest;
      for (size_t j = 0; j < group.files.size(); ++j) {
        ASSERT_TRUE(cmp.Compare(largest_so_far,
                                group.files[j]->smallest) >= 0);

        if (cmp.Compare(largest_so_far, group.files[j]->largest) < 0) {
          largest_so_far = group.files[j]->largest;
        }
      }

      // Check largest key in group is correct.
      ASSERT_TRUE(cmp.Compare(group.largest, largest_so_far) == 0);

      // Check total file size is valid.
      uint64_t total_file_size = 0;
      for (size_t j = 0; j < group.files.size(); ++j) {
        total_file_size += group.files[j]->file_size;
      }
      ASSERT_EQ(total_file_size, group.total_file_size);
    }

    // Check groups are sorted and don't overlap.
    for (size_t i = 0; i < groups_.size()-1; ++i) {
      ASSERT_TRUE(cmp.Compare(groups_[i]->largest,
                              groups_[i+1]->smallest) < 0);
    }
  }
};

TEST(FileGroupTest, Construction) {
  AddFile("150", "600");
  AddFile("250", "700");
  AddFile("650", "800");
  AddFile("801", "805");
  RebuildGroups();

  ASSERT_EQ(groups_.size(), 2u);
}

TEST(FileGroupTest, OverlappingGroups) {
  AddFile("150", "600");
  AddFile("250", "700");
  AddFile("650", "800");
  AddFile("851", "855");
  RebuildGroups();

  ASSERT_TRUE(Overlaps(NULL, NULL));
  ASSERT_TRUE(!Overlaps(NULL, "149"));
  ASSERT_TRUE(Overlaps(NULL, "150"));
  ASSERT_TRUE(Overlaps("855", NULL));
  ASSERT_TRUE(!Overlaps("801", "850"));
  ASSERT_TRUE(Overlaps("601", "649"));
}

TEST(FileGroupTest, FindingGroups) {
  AddFile("100", "200");
  AddFile("300", "400");
  AddFile("600", "800");
  RebuildGroups();

  ASSERT_EQ(groups_.size(), 3u);

  ASSERT_EQ(Find("0"), 0u);
  ASSERT_EQ(Find("200"), 0u);
  ASSERT_EQ(Find("201"), 1u);
  ASSERT_EQ(Find("400"), 1u);
  ASSERT_EQ(Find("401"), 2u);
  ASSERT_EQ(Find("800"), 2u);
  ASSERT_EQ(Find("801"), 3u);
}

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
