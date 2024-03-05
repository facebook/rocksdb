#include "db/replication_epoch_edit.h"

#include <algorithm>
#include <optional>
#include <sstream>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

ReplicationEpochAddition::ReplicationEpochAddition(uint64_t epoch,
                                                   uint64_t first_mus)
    : epoch_(epoch), first_mus_(first_mus) {}

void ReplicationEpochAddition::EncodeTo(std::string* output) const {
  PutVarint64(output, epoch_);
  PutVarint64(output, first_mus_);
}

Status ReplicationEpochAddition::DecodeFrom(Slice* input) {
  if (!GetVarint64(input, &epoch_)) {
    return Status::Corruption("ReplicationEpochAddition",
                              "Error decoding epoch");
  }
  if (!GetVarint64(input, &first_mus_)) {
    return Status::Corruption("ReplicationEpochAddition",
                              "Error decoding manifest update sequence");
  }
  return Status::OK();
}

std::string ReplicationEpochAddition::DebugString() const {
  std::ostringstream oss;
  oss << *this;
  return oss.str();
}

std::ostream& operator<<(std::ostream& os, const ReplicationEpochAddition& ea) {
  os << "epoch: " << ea.GetEpoch() << ", mus: " << ea.GetFirstMUS();
  return os;
}

Status ReplicationEpochSet::AddEpoch(const ReplicationEpochAddition& epoch,
                                     uint32_t max_num_replication_epochs) {
  if (!epochs_.empty() && epochs_.back() >= epoch) {
    std::stringstream ss;
    ss << "Misordered replication epoch. prev: " << epochs_.back()
       << ", next: " << epoch;
    return Status::Corruption(ss.str());
  }

  while (epochs_.size() >= max_num_replication_epochs) {
    epochs_.pop_front();
  }
  epochs_.push_back(epoch);
  return Status::OK();
}

Status ReplicationEpochSet::AddEpochs(const ReplicationEpochAdditions& epochs,
                                      uint32_t max_num_replication_epochs) {
  for (auto& epoch : epochs) {
    auto s = AddEpoch(epoch, max_num_replication_epochs);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status ReplicationEpochSet::VerifyNewEpochs(
    const ReplicationEpochAdditions& new_epochs) const {
  if (new_epochs.empty()) {
    return Status::OK();
  }

  std::optional<ReplicationEpochAddition> prev;
  if (!epochs_.empty()) {
    prev = epochs_.back();
  }

  for (auto& epoch : new_epochs) {
    if (!prev) {
      prev = epoch;
      continue;
    }

    if (*prev >= epoch) {
      std::stringstream ss;
      ss << "Misordered replication epoch. prev: " << *prev
         << ", next: " << epoch;
      return Status::Corruption(ss.str());
    }

    prev = epoch;
  }
  return Status::OK();
}

void ReplicationEpochSet::DeleteEpochsBefore(uint64_t epoch) {
  while (!epochs_.empty() && (epochs_.front().GetEpoch() < epoch)) {
    epochs_.pop_front();
  }
}

std::optional<uint64_t> ReplicationEpochSet::GetEpochForMUS(
    uint64_t mus) const {
  // fast path
  if (!epochs_.empty() && epochs_.back().GetFirstMUS() <= mus) {
    return epochs_.back().GetEpoch();
  }

  auto it = std::upper_bound(epochs_.begin(), epochs_.end(), mus,
                             [&](auto m, const auto& epoch_addition) {
                               return m < epoch_addition.GetFirstMUS();
                             });
  // mus is smaller than smallest mus in the epoch set, so we can't infer
  // its epoch
  if (it == epochs_.begin()) {
    return std::nullopt;
  }
  --it;
  return it->GetEpoch();
}

bool operator==(const ReplicationEpochSet& es1,
                const ReplicationEpochSet& es2) {
  if (es1.GetEpochs().size() != es2.GetEpochs().size()) {
    return false;
  }

  for (size_t i = 0; i < es1.GetEpochs().size(); i++) {
    if (es1.GetEpochs()[i] != es2.GetEpochs()[i]) {
      return false;
    }
  }
  return true;
}

std::ostream& operator<<(std::ostream& os, const ReplicationEpochSet& es) {
  for (const auto& e : es.GetEpochs()) {
    os << e << "; ";
  }
  return os;
}

}  // namespace ROCKSDB_NAMESPACE
