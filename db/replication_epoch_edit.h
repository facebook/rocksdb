#pragma once

#include <cassert>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class Status;

// Record the event of first manifest update (VersinEdit) generated in an
// replication epoch.
class ReplicationEpochAddition {
 public:
  ReplicationEpochAddition() = default;
  ReplicationEpochAddition(uint64_t epoch, uint64_t first_mus);

  uint64_t GetEpoch() const { return epoch_; }
  uint64_t GetFirstMUS() const { return first_mus_; }

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

  std::string DebugString() const;

 private:
  // The replication epoch
  uint64_t epoch_{0};
  // First manifest update sequence of the replication epoch
  uint64_t first_mus_{0};
};

inline bool operator==(const ReplicationEpochAddition& ea1,
                       const ReplicationEpochAddition& ea2) {
  return ea1.GetEpoch() == ea2.GetEpoch() &&
         ea1.GetFirstMUS() == ea2.GetFirstMUS();
}
inline bool operator!=(const ReplicationEpochAddition& ea1,
                       const ReplicationEpochAddition& ea2) {
  return !(ea1 == ea2);
}

inline bool operator<(const ReplicationEpochAddition& ea1,
                      const ReplicationEpochAddition& ea2) {
  return ea1.GetEpoch() <= ea2.GetEpoch() &&
         ea1.GetFirstMUS() < ea2.GetFirstMUS();
}
inline bool operator>=(const ReplicationEpochAddition& ea1,
                       const ReplicationEpochAddition ea2) {
  return !(ea1 < ea2);
}
std::ostream& operator<<(std::ostream& os, const ReplicationEpochAddition& ea);

using ReplicationEpochAdditions = std::vector<ReplicationEpochAddition>;

// A set of ordered(asc) replication epochs.
//
// We maintain all the (replication epoch, first manifest update sequence in
// that epoch) after the persisted replication sequence in the manifest file to
// help detect replication log divergence when recovering local replication log.
// Therefore
// - we add new (epoch, mus) into the manifest file whenever replication epoch
// changes and there is at least one VersionEdit in that replication epoch
// - The list is pruned automatically whenever persisted replication sequence
// changes
//
// Not thread safe, needs external synchronization such as holding DB mutex.
class ReplicationEpochSet {
 public:
  // Add a list of ordered epochs to the set.
  // - If number of epochs in the set exceeds `max_num_replication_epochs`,
  // smaller epochs will be pruned. See comments of
  // `DBOptions::max_num_replication_epochs` to see why we need to prune
  // - Returns error when newly added epochs are not ordered after existing
  // epochs in the set
  Status AddEpochs(const ReplicationEpochAdditions& epochs,
                   uint32_t max_num_replication_epochs);
  // Verify that newly added epochs >= existing epochs and newly added
  // epochs are ordered properly
  Status VerifyNewEpochs(const ReplicationEpochAdditions& epochs) const;

  void DeleteEpochsBefore(uint64_t epoch);

  // Infer corresponding epoch for manifest update sequence. Epoch for a
  // manifest update can only be inferred when the mus is >= smallest mus in
  // the set (i.e, mus of first epoch in the set). For example,
  // given a epoch set: [(epoch 2, mus 3), (epoch 3, mus 10), (epoch 5, mus
  // 20)],
  // - If mus < 3, then its epoch is not known
  // - If 3 < mus <= 10, the epoch is 2
  // - If 10 <= mus < 20, the epoch is 3
  // - If mus >= 20, epoch is 5
  //
  // -  Return std::nullopt if `mus` is smaller than the smallest manifest
  // update sequence in the set
  std::optional<uint64_t> GetEpochForMUS(uint64_t mus) const;

  const auto& GetEpochs() const { return epochs_; }
  uint64_t GetSmallestEpoch() const {
    assert(!epochs_.empty());
    return epochs_.front().GetEpoch();
  }
  uint64_t GetLargestEpoch() const {
    assert(!epochs_.empty());
    return epochs_.back().GetEpoch();
  }

  bool empty() const { return epochs_.empty(); }
  auto size() const { return epochs_.size(); }


 private:
  Status AddEpoch(const ReplicationEpochAddition& epoch,
                  uint32_t max_num_replication_epochs);
  // queue of epochs after persisted replication sequence, in ascending order.
  std::deque<ReplicationEpochAddition> epochs_;
};

bool operator==(const ReplicationEpochSet& es1, const ReplicationEpochSet& es2);
std::ostream& operator<<(std::ostream& os, const ReplicationEpochSet& es);

}  // namespace ROCKSDB_NAMESPACE
