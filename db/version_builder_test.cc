//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include "db/version_edit.h"
#include "db/version_set.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class VersionBuilderTest {
 public:
  const Comparator* ucmp;
  InternalKeyComparator icmp;
  Options options;
  ImmutableCFOptions ioptions;
  MutableCFOptions mutable_cf_options;
  VersionStorageInfo vstorage;
  uint32_t file_num;
  CompactionOptionsFIFO fifo_options;
  std::vector<uint64_t> size_being_compacted;

  VersionBuilderTest()
      : ucmp(BytewiseComparator()),
        icmp(ucmp),
        ioptions(options),
        mutable_cf_options(options, ioptions),
        vstorage(&icmp, ucmp, options.num_levels, kCompactionStyleLevel,
                 nullptr),
        file_num(1) {
    mutable_cf_options.RefreshDerivedOptions(ioptions);
    size_being_compacted.resize(options.num_levels);
  }

  ~VersionBuilderTest() {
    for (int i = 0; i < vstorage.num_levels(); i++) {
      for (auto* f : vstorage.LevelFiles(i)) {
        if (--f->refs == 0) {
          delete f;
        }
      }
    }
  }

  InternalKey GetInternalKey(const char* ukey,
                             SequenceNumber smallest_seq = 100) {
    return InternalKey(ukey, smallest_seq, kTypeValue);
  }

  void Add(int level, uint32_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 0, uint32_t path_id = 0,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    assert(level < vstorage.num_levels());
    FileMetaData* f = new FileMetaData;
    f->fd = FileDescriptor(file_number, path_id, file_size);
    f->smallest = GetInternalKey(smallest, smallest_seq);
    f->largest = GetInternalKey(largest, largest_seq);
    f->compensated_file_size = file_size;
    f->refs = 0;
    vstorage.MaybeAddFile(level, f);
  }

  void UpdateVersionStorageInfo() {
    vstorage.ComputeCompactionScore(mutable_cf_options, fifo_options,
                                    size_being_compacted);
    vstorage.UpdateFilesBySize();
    vstorage.UpdateNumNonEmptyLevels();
    vstorage.GenerateFileIndexer();
    vstorage.GenerateLevelFilesBrief();
    vstorage.SetFinalized();
  }
};

TEST(VersionBuilderTest, ApplyAndSaveTo) {
  Add(0, 1U, "150", "200", 100U);
  // Level 1 score 1.2
  Add(1, 66U, "150", "200", 100U);
  Add(1, 88U, "201", "300", 100U);
  // Level 2 score 1.8. File 7 is the largest. Should be picked
  Add(2, 6U, "150", "179", 100U);
  Add(2, 7U, "180", "220", 100U);
  Add(2, 8U, "221", "300", 100U);
  // Level 3 score slightly larger than 1
  Add(3, 26U, "150", "170", 100U);
  Add(3, 27U, "171", "179", 100U);
  Add(3, 28U, "191", "220", 100U);
  Add(3, 29U, "221", "300", 100U);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(2, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200);
  version_edit.DeleteFile(3, 27U);

  EnvOptions env_options;

  VersionBuilder version_builder(env_options, nullptr, &vstorage);

  VersionStorageInfo new_vstorage(&icmp, ucmp, options.num_levels,
                                  kCompactionStyleLevel, nullptr);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage);

  ASSERT_EQ(400U, new_vstorage.NumLevelBytes(2));
  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(3));

  for (int i = 0; i < new_vstorage.num_levels(); i++) {
    for (auto* f : new_vstorage.LevelFiles(i)) {
      if (--f->refs == 0) {
        delete f;
      }
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) { return rocksdb::test::RunAllTests(); }
