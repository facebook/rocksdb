//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db_stress_tool/db_stress_listener.h"

#include <cstdint>

#include "file/file_util.h"
#include "rocksdb/file_system.h"
#include "util/coding_lean.h"

namespace ROCKSDB_NAMESPACE {

#ifdef GFLAGS
#ifndef ROCKSDB_LITE

// TODO: consider using expected_values_dir instead, but this is more
// convenient for now.
UniqueIdVerifier::UniqueIdVerifier(const std::string& db_name, Env* env)
    : path_(db_name + "/.unique_ids") {
  // We expect such a small number of files generated during this test
  // (thousands?), checking full 192-bit IDs for uniqueness is a very
  // weak check. For a stronger check, we pick a specific 64-bit
  // subsequence from the ID to check for uniqueness. All bits of the
  // ID should be high quality, and 64 bits should be unique with
  // very good probability for the quantities in this test.
  offset_ = Random::GetTLSInstance()->Uniform(17);  // 0 to 16

  const std::shared_ptr<FileSystem> fs = env->GetFileSystem();
  IOOptions opts;

  Status st = fs->CreateDirIfMissing(db_name, opts, nullptr);
  if (!st.ok()) {
    fprintf(stderr, "Failed to create directory %s: %s\n", db_name.c_str(),
            st.ToString().c_str());
    exit(1);
  }

  // Avoid relying on ReopenWritableFile which is not supported by all
  // file systems. Create a new file and copy the old file contents to it.
  std::string tmp_path = path_ + ".tmp";
  st = fs->FileExists(tmp_path, opts, /*dbg*/ nullptr);
  if (st.IsNotFound()) {
    st = fs->RenameFile(path_, tmp_path, opts, /*dbg*/ nullptr);
    // Either it should succeed or fail because src path doesn't exist
    assert(st.ok() || st.IsPathNotFound());
  } else {
    // If path_ and tmp_path both exist, retain tmp_path as its
    // guaranteed to be more complete. The order of operations are -
    // 1. Rename path_ to tmp_path
    // 2. Parse tmp_path contents
    // 3. Create path_
    // 4. Copy tmp_path contents to path_
    // 5. Delete tmp_path
    st = fs->DeleteFile(path_, opts, /*dbg*/ nullptr);
    assert(st.ok() || st.IsPathNotFound());
  }

  uint64_t size = 0;
  {
    std::unique_ptr<FSSequentialFile> reader;
    Status s = fs->NewSequentialFile(tmp_path, FileOptions(), &reader,
                                     /*dbg*/ nullptr);
    if (s.ok()) {
      // Load from file
      std::string id(24U, '\0');
      Slice result;
      for (;;) {
        s = reader->Read(id.size(), opts, &result, &id[0], /*dbg*/ nullptr);
        if (!s.ok()) {
          fprintf(stderr, "Error reading unique id file: %s\n",
                  s.ToString().c_str());
          assert(false);
        }
        if (result.size() < id.size()) {
          // EOF
          if (result.size() != 0) {
            // Corrupt file. Not a DB bug but could happen if OS doesn't provide
            // good guarantees on process crash.
            fprintf(stdout, "Warning: clearing corrupt unique id file\n");
            id_set_.clear();
            reader.reset();
            s = fs->DeleteFile(tmp_path, opts, /*dbg*/ nullptr);
            assert(s.ok());
            size = 0;
          }
          break;
        }
        size += 24U;
        VerifyNoWrite(id);
      }
    } else {
      // Newly created is ok.
      // But FileSystem doesn't tell us whether non-existence was the cause of
      // the failure. (Issue #9021)
      Status s2 = fs->FileExists(tmp_path, opts, /*dbg*/ nullptr);
      if (!s2.IsNotFound()) {
        fprintf(stderr, "Error opening unique id file: %s\n",
                s.ToString().c_str());
        assert(false);
      }
      size = 0;
    }
  }
  fprintf(stdout, "(Re-)verified %zu unique IDs\n", id_set_.size());

  std::unique_ptr<FSWritableFile> file_writer;
  st = fs->NewWritableFile(path_, FileOptions(), &file_writer, /*dbg*/ nullptr);
  if (!st.ok()) {
    fprintf(stderr, "Error creating the unique ids file: %s\n",
            st.ToString().c_str());
    assert(false);
  }
  data_file_writer_.reset(
      new WritableFileWriter(std::move(file_writer), path_, FileOptions()));

  if (size > 0) {
    st = CopyFile(fs.get(), tmp_path, data_file_writer_, size,
                  /*use_fsync*/ true, /*io_tracer*/ nullptr,
                  /*temparature*/ Temperature::kHot);
    if (!st.ok()) {
      fprintf(stderr, "Error copying contents of old unique id file: %s\n",
              st.ToString().c_str());
      assert(false);
    }
  }
  st = fs->DeleteFile(tmp_path, opts, /*dbg*/ nullptr);
  assert(st.ok() || st.IsPathNotFound());
}

UniqueIdVerifier::~UniqueIdVerifier() {
  IOStatus s = data_file_writer_->Close();
  assert(s.ok());
}

void UniqueIdVerifier::VerifyNoWrite(const std::string& id) {
  assert(id.size() == 24);
  bool is_new = id_set_.insert(DecodeFixed64(&id[offset_])).second;
  if (!is_new) {
    fprintf(stderr,
            "Duplicate partial unique ID found (offset=%zu, count=%zu)\n",
            offset_, id_set_.size());
    assert(false);
  }
}

void UniqueIdVerifier::Verify(const std::string& id) {
  assert(id.size() == 24);
  std::lock_guard<std::mutex> lock(mutex_);
  // If we accumulate more than ~4 million IDs, there would be > 1 in 1M
  // natural chance of collision. Thus, simply stop checking at that point.
  if (id_set_.size() >= 4294967) {
    return;
  }
  IOStatus s = data_file_writer_->Append(Slice(id));
  if (!s.ok()) {
    fprintf(stderr, "Error writing to unique id file: %s\n",
            s.ToString().c_str());
    assert(false);
  }
  s = data_file_writer_->Flush();
  if (!s.ok()) {
    fprintf(stderr, "Error flushing unique id file: %s\n",
            s.ToString().c_str());
    assert(false);
  }
  VerifyNoWrite(id);
}

void DbStressListener::VerifyTableFileUniqueId(
    const TableProperties& new_file_properties, const std::string& file_path) {
  // Verify unique ID
  std::string id;
  // Unit tests verify that GetUniqueIdFromTableProperties returns just a
  // substring of this, and we're only going to pull out 64 bits, so using
  // GetExtendedUniqueIdFromTableProperties is arguably stronger testing here.
  Status s = GetExtendedUniqueIdFromTableProperties(new_file_properties, &id);
  if (!s.ok()) {
    fprintf(stderr, "Error getting SST unique id for %s: %s\n",
            file_path.c_str(), s.ToString().c_str());
    assert(false);
  }
  unique_ids_.Verify(id);
}

#endif  // !ROCKSDB_LITE
#endif  // GFLAGS

}  // namespace ROCKSDB_NAMESPACE
