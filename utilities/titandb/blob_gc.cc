#include "utilities/titandb/blob_gc.h"

#include "utilities/titandb/version.h"

namespace rocksdb {
namespace titandb {

BlobGC::BlobGC(std::vector<BlobFileMeta*>&& blob_files,
               TitanCFOptions&& _titan_cf_options)
    : inputs_(std::move(blob_files)),
      titan_cf_options_(std::move(_titan_cf_options)) {
  MarkFilesBeingGC();
}

BlobGC::~BlobGC() {
  if (current_ != nullptr) {
    current_->Unref();
  }
}

void BlobGC::SetInputVersion(ColumnFamilyHandle* cfh, Version* version) {
  cfh_ = cfh;
  current_ = version;

  current_->Ref();
}

ColumnFamilyData* BlobGC::GetColumnFamilyData() {
  auto* cfhi = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh_);
  return cfhi->cfd();
}

void BlobGC::AddOutputFile(BlobFileMeta* blob_file) {
  blob_file->FileStateTransit(BlobFileMeta::FileEvent::kGCOutput);
  outputs_.push_back(blob_file);
}

void BlobGC::MarkFilesBeingGC() {
  for (auto& f : inputs_) {
    f->FileStateTransit(BlobFileMeta::FileEvent::kGCBegin);
  }
}

void BlobGC::ReleaseGcFiles() {
  for (auto& f : inputs_) {
    f->FileStateTransit(BlobFileMeta::FileEvent::kGCCompleted);
  }

  for (auto& f : outputs_) {
    f->FileStateTransit(BlobFileMeta::FileEvent::kGCCompleted);
  }
}

}  // namespace titandb
}  // namespace rocksdb
