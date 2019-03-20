#include "utilities/titandb/table_builder.h"

namespace rocksdb {
namespace titandb {

void TitanTableBuilder::Add(const Slice& key, const Slice& value) {
  if (!ok()) return;

  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    status_ = Status::Corruption(Slice());
    return;
  }

  if (ikey.type != kTypeValue || value.size() < cf_options_.min_blob_size) {
    base_builder_->Add(key, value);
    return;
  }

  std::string index_value;
  AddBlob(ikey.user_key, value, &index_value);
  if (!ok()) return;

  ikey.type = kTypeBlobIndex;
  std::string index_key;
  AppendInternalKey(&index_key, ikey);
  base_builder_->Add(index_key, index_value);
}

void TitanTableBuilder::AddBlob(const Slice& key, const Slice& value,
                                std::string* index_value) {
  if (!ok()) return;

  if (!blob_builder_) {
    status_ = blob_manager_->NewFile(&blob_handle_);
    if (!ok()) return;
    blob_builder_.reset(
        new BlobFileBuilder(cf_options_, blob_handle_->GetFile()));
  }

  BlobIndex index;
  BlobRecord record;
  record.key = key;
  record.value = value;
  index.file_number = blob_handle_->GetNumber();
  blob_builder_->Add(record, &index.blob_handle);
  if (ok()) {
    index.EncodeTo(index_value);
  }
}

Status TitanTableBuilder::status() const {
  Status s = status_;
  if (s.ok()) {
    s = base_builder_->status();
  }
  if (s.ok() && blob_builder_) {
    s = blob_builder_->status();
  }
  return s;
}

Status TitanTableBuilder::Finish() {
  base_builder_->Finish();
  if (blob_builder_) {
    blob_builder_->Finish();
    if (ok()) {
      std::shared_ptr<BlobFileMeta> file = std::make_shared<BlobFileMeta>(
          blob_handle_->GetNumber(), blob_handle_->GetFile()->GetFileSize());
      file->FileStateTransit(BlobFileMeta::FileEvent::kFlushOrCompactionOutput);
      status_ =
          blob_manager_->FinishFile(cf_id_, file, std::move(blob_handle_));
      //      ROCKS_LOG_INFO(db_options_.info_log, "[%u] AddFile %lu", cf_id_,
      //                     file->file_number_);
    } else {
      status_ = blob_manager_->DeleteFile(std::move(blob_handle_));
    }
  }
  return status();
}

void TitanTableBuilder::Abandon() {
  base_builder_->Abandon();
  if (blob_builder_) {
    blob_builder_->Abandon();
    status_ = blob_manager_->DeleteFile(std::move(blob_handle_));
  }
}

uint64_t TitanTableBuilder::NumEntries() const {
  return base_builder_->NumEntries();
}

uint64_t TitanTableBuilder::FileSize() const {
  return base_builder_->FileSize();
}

bool TitanTableBuilder::NeedCompact() const {
  return base_builder_->NeedCompact();
}

TableProperties TitanTableBuilder::GetTableProperties() const {
  return base_builder_->GetTableProperties();
}

}  // namespace titandb
}  // namespace rocksdb
