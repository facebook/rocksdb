//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "util/auto_tune_compressor.h"

#include "options/options_helper.h"
#include "rocksdb/advanced_compression.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/rate_tracker.h"
#include "util/stop_watch.h"
namespace ROCKSDB_NAMESPACE {

int CompressionRejectionProbabilityPredictor::Predict() const {
  return pred_rejection_prob_percentage_;
}

size_t CompressionRejectionProbabilityPredictor::attempted_compression_count()
    const {
  return rejected_count_ + compressed_count_;
}

bool CompressionRejectionProbabilityPredictor::Record(
    Slice uncompressed_block_data, std::string* compressed_output,
    const CompressionOptions& opts) {
  if (compressed_output->size() >
      (static_cast<uint64_t>(opts.max_compressed_bytes_per_kb) *
       uncompressed_block_data.size()) >>
      10) {
    rejected_count_++;
  } else {
    compressed_count_++;
  }
  auto attempted = attempted_compression_count();
  if (attempted >= window_size_) {
    pred_rejection_prob_percentage_ =
        static_cast<int>(rejected_count_ * 100 / attempted);
    compressed_count_ = 0;
    rejected_count_ = 0;
    assert(attempted_compression_count() == 0);
  }
  return true;
}

AutoSkipCompressorWrapper::AutoSkipCompressorWrapper(
    std::unique_ptr<Compressor> compressor, const CompressionOptions& opts)
    : CompressorWrapper::CompressorWrapper(std::move(compressor)),
      opts_(opts) {}

const char* AutoSkipCompressorWrapper::Name() const {
  return "AutoSkipCompressorWrapper";
}

Status AutoSkipCompressorWrapper::CompressBlock(
    Slice uncompressed_data, std::string* compressed_output,
    CompressionType* out_compression_type, ManagedWorkingArea* wa) {
  // Check if the managed working area is provided or owned by this object.
  // If not, bypass auto-skip logic since the working area lacks a predictor to
  // record or make necessary decisions to compress or bypass compression of the
  // block.
  if (wa == nullptr || wa->owner() != this) {
    return wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                   out_compression_type, wa);
  }
  bool exploration =
      Random::GetTLSInstance()->PercentTrue(kExplorationPercentage);
  TEST_SYNC_POINT_CALLBACK(
      "AutoSkipCompressorWrapper::CompressBlock::exploitOrExplore",
      &exploration);
  auto autoskip_wa = static_cast<AutoSkipWorkingArea*>(wa->get());
  if (exploration) {
    return CompressBlockAndRecord(uncompressed_data, compressed_output,
                                  out_compression_type, autoskip_wa);
  } else {
    auto predictor_ptr = autoskip_wa->predictor;
    auto prediction = predictor_ptr->Predict();
    if (prediction <= kProbabilityCutOff) {
      // decide to compress
      return CompressBlockAndRecord(uncompressed_data, compressed_output,
                                    out_compression_type, autoskip_wa);
    } else {
      // decide to bypass compression
      *out_compression_type = kNoCompression;
      return Status::OK();
    }
  }
  return Status::OK();
}

Compressor::ManagedWorkingArea AutoSkipCompressorWrapper::ObtainWorkingArea() {
  auto wrap_wa = wrapped_->ObtainWorkingArea();
  return ManagedWorkingArea(new AutoSkipWorkingArea(std::move(wrap_wa)), this);
}
void AutoSkipCompressorWrapper::ReleaseWorkingArea(WorkingArea* wa) {
  delete static_cast<AutoSkipWorkingArea*>(wa);
}

Status AutoSkipCompressorWrapper::CompressBlockAndRecord(
    Slice uncompressed_data, std::string* compressed_output,
    CompressionType* out_compression_type, AutoSkipWorkingArea* wa) {
  Status status = wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                          out_compression_type, &(wa->wrapped));
  // determine if it was rejected or compressed
  auto predictor_ptr = wa->predictor;
  predictor_ptr->Record(uncompressed_data, compressed_output, opts_);
  return status;
}

const char* AutoSkipCompressorManager::Name() const {
  // should have returned "AutoSkipCompressorManager" but we currently have an
  // error so for now returning name of the wrapped container
  return wrapped_->Name();
}

std::unique_ptr<Compressor> AutoSkipCompressorManager::GetCompressorForSST(
    const FilterBuildingContext& context, const CompressionOptions& opts,
    CompressionType preferred) {
  assert(GetSupportedCompressions().size() > 1);
  assert(preferred != kNoCompression);
  return std::make_unique<AutoSkipCompressorWrapper>(
      wrapped_->GetCompressorForSST(context, opts, preferred), opts);
}
void AutoTuneCompressor::AddCompressors(
    CompressionType type, const std::initializer_list<int>& levels) {
  auto builtInManager = GetBuiltinV2CompressionManager();
  CompressionOptions new_opts = opts_;
  for (auto level : levels) {
    new_opts.level = level;
    compressors_.emplace_back(builtInManager->GetCompressor(new_opts, type));
  }
}
AutoTuneCompressor::AutoTuneCompressor(
    const CompressionOptions& opts, const CompressionType default_type,
    const std::shared_ptr<const Budget>& io_goal,
    const std::shared_ptr<const Budget>& cpu_budget,
    const std::shared_ptr<RateLimiter>& rate_limiter)
    : opts_(opts),
      io_goal_(std::const_pointer_cast<IOGoal>(
          std::static_pointer_cast<const IOGoal>(io_goal))),
      cpu_budget_(std::const_pointer_cast<CPUBudget>(
          std::static_pointer_cast<const CPUBudget>(cpu_budget))),
      usage_tracker_(rate_limiter) {
  assert(io_goal_ != nullptr);
  assert(cpu_budget_ != nullptr);
  // Create compressors supporting all the compression types and levels as per
  // the compression levels set in vector CompressionLevels.
  const auto& compressions = GetSupportedCompressions();
  assert(compressions.size() > 1);

  for (auto type : compressions) {
    if (type == kNoCompression) {
      continue;
    } else if (type == kSnappyCompression) {
      AddCompressors(type, {0});
    } else if (type == kLZ4Compression) {
      AddCompressors(type, {1, 4, 9});
    } else if (type == kLZ4HCCompression) {
      AddCompressors(type, {1, 4, 9});
    } else if (type == kZSTD) {
      AddCompressors(type, {1, 3, 9});
    }
  }
  assert(compressors_.size() > 0);
  MeasureUtilization();
  block_count_ = 0;
  cur_compressor_idx_ = 0;
  default_compressor_ =
      GetBuiltinV2CompressionManager()->GetCompressor(opts, default_type);
}
AutoTuneCompressor::~AutoTuneCompressor() {}
const char* AutoTuneCompressor::Name() const { return "AutoTuneCompressor"; }
std::unique_ptr<Compressor> AutoTuneCompressor::MaybeCloneSpecialized(
    CacheEntryRole block_type, DictSampleArgs&& dict_samples) {
  // TODO: full dictionary compression support. Currently this just falls
  // back on a non-multi compressor when asked to use a dictionary.
  if (compressors_.size() > 0) {
    auto idx = compressors_.size() - 1;
    return compressors_[idx]->MaybeCloneSpecialized(block_type,
                                                    std::move(dict_samples));
  } else {
    return default_compressor_->MaybeCloneSpecialized(block_type,
                                                      std::move(dict_samples));
  }
}
Status AutoTuneCompressor::CompressBlock(Slice uncompressed_data,
                                         std::string* compressed_output,
                                         CompressionType* out_compression_type,
                                         ManagedWorkingArea* wa) {
  // Check if the managed working area is provided or owned by this object.
  // If not, bypass compressor logic since the working area lacks a predictor.
  if (wa == nullptr || wa->owner() != this) {
    if (default_compressor_) {
      return default_compressor_->CompressBlock(
          uncompressed_data, compressed_output, out_compression_type, wa);
    } else {
      return Status::InvalidArgument(
          "Compression type should not be kNoCompression");
    }
  }

  if (io_goal_ == nullptr || cpu_budget_ == nullptr) {
    return Status::InvalidArgument("IOGoal or CPUBudget is not set");
  } else if (compressors_.size() == 0) {
    return Status::NotSupported(
        "No compression alorithm that AutoTune compressor can use are "
        "supported (SNAPPY, LZ4, LZ4HC, ZSTD)");
  }
  auto local_wa = static_cast<CostAwareWorkingArea*>(wa->get());
  bool exploration =
      Random::GetTLSInstance()->PercentTrue(kExplorationPercentage);
  TEST_SYNC_POINT_CALLBACK(
      "AutoTuneCompressorWrapper::CompressBlock::exploitOrExplore",
      &exploration);
  if (exploration) {
    size_t choosen_index = Random::GetTLSInstance()->Uniform(
        static_cast<int>(compressors_.size()));

    return CompressBlockAndRecord(choosen_index, uncompressed_data,
                                  compressed_output, out_compression_type,
                                  local_wa);
  } else {
    auto chosen_compressor = SelectCompressionBasedOnIOGoalCPUBudget(local_wa);
    TEST_SYNC_POINT_CALLBACK(
        "AutoTuneCompressorWrapper::CompressBlock::GetSelection",
        &chosen_compressor);
    // Check if the chosen compression type and level are available
    // if not, skip the compression
    if (chosen_compressor >= compressors_.size()) {
      *out_compression_type = kNoCompression;
      return Status::OK();
    }
    return CompressBlockAndRecord(chosen_compressor, uncompressed_data,
                                  compressed_output, out_compression_type,
                                  local_wa);
  }
}

Compressor::ManagedWorkingArea AutoTuneCompressor::ObtainWorkingArea() {
  if (compressors_.size() > 0) {
    auto wrap_wa = compressors_.back()->ObtainWorkingArea();
    auto wa = new CostAwareWorkingArea(std::move(wrap_wa));
    // Create cost predictors for each compression type and level
    wa->cost_predictors_.reserve(compressors_.size());
    for (size_t i = 0; i < compressors_.size(); i++) {
      wa->cost_predictors_.emplace_back(
          std::make_unique<IOCPUCostPredictor>(kWindow));
    }
    return ManagedWorkingArea(wa, this);
  } else {
    return default_compressor_->ObtainWorkingArea();
  }
}
void AutoTuneCompressor::MeasureUtilization() { usage_tracker_.Record(); }
void AutoTuneCompressor::ReleaseWorkingArea(WorkingArea* wa) {
  // remove all created cost predictors
  delete static_cast<CostAwareWorkingArea*>(wa);
}
// Select the compression type and level based on the IO and CPU usage.
// The ultimate goal is to select the compression type and level which
// will result in IO and CPU usage between the lower and upper bounds
// provided by the user using IOGoal and CPUBudget.
// In order to achieve this, we measure the current IO and CPU usage and
// the CPU and IO costs of different compression algorithms and levels.
// We then select the compression type and level that will either
// increase or decrease the IO and CPU usage based on the user-provided
// IOGoal and CPUBudget.
size_t AutoTuneCompressor::SelectCompressionBasedOnIOGoalCPUBudget(
    CostAwareWorkingArea* wa) {
  if ((block_count_++) % kCompressionEvaluationInterval != 0) {
    return cur_compressor_idx_;
  }
  // Measure current resource utilization
  MeasureUtilization();
  auto cpu_io_util = usage_tracker_.GetUtilization();
  TEST_SYNC_POINT_CALLBACK("AutoTuneCompressorWrapper::SetCPUIOUsage",
                           &cpu_io_util);
  auto& cpu_util = cpu_io_util.first;
  auto& io_util = cpu_io_util.second;
  // Get available budgets
  auto cpu_upper_bound = cpu_budget_->GetMaxRate();
  auto io_upper_bound = io_goal_->GetMaxRate();
  auto cpu_lower_bound = cpu_budget_->GetMinRate();
  auto io_lower_bound = io_goal_->GetMinRate();
  // Detect the 4 quadrant that we want to explore
  // Stable region is between the cpu_lower_bound to cpu_upper_bound
  // and io_lower_bound to io_upper_bound
  bool increase_io = io_util < io_lower_bound;
  bool decrease_io = io_util > io_upper_bound;
  bool increase_cpu = cpu_util < cpu_lower_bound;
  bool decrease_cpu = cpu_util > cpu_upper_bound;

  // If we are in the stable region, then we just go ahead with the current
  // compression type and level
  if (!increase_io && !increase_cpu && !decrease_io && !decrease_cpu) {
    return cur_compressor_idx_;
  } else if (cur_compressor_idx_ >= compressors_.size()) {
    // If the current compression type and level are not available, i.e.,
    // compression is disabled, we can switch from no compression if we can
    // decrease IO and increase CPU usage. Otherwise, our current
    // no-compression setting is the best we can do.
    if (decrease_io && increase_cpu) {
      cur_compressor_idx_ = 0;
      return cur_compressor_idx_;
    }
    return cur_compressor_idx_;
  }

  TEST_SYNC_POINT_CALLBACK(
      "AutoTuneCompressorWrapper::CompressBlock::GetPredictors",
      &(wa->cost_predictors_));
  // If we are not in the stable region, then we need to explore other
  // compression algorithms and levels that are in the right quadrant.
  // The right quadrant is determined by whether we want to increase or
  // decrease the CPU and IO usage based on our current measurements.
  auto cur_cpu_cost =
      wa->cost_predictors_[cur_compressor_idx_]->CPUPredictor.Predict();
  auto cur_io_cost =
      wa->cost_predictors_[cur_compressor_idx_]->IOPredictor.Predict();

  for (size_t choice = 0; choice < compressors_.size(); choice++) {
    auto predicted_io_cost =
        wa->cost_predictors_[choice]->IOPredictor.Predict();
    auto predicted_cpu_cost =
        wa->cost_predictors_[choice]->CPUPredictor.Predict();
    if (predicted_cpu_cost == 0 || predicted_io_cost == 0) {
      continue;
    }
    if (IsInValidQuadrant(predicted_io_cost, predicted_cpu_cost, cur_io_cost,
                          cur_cpu_cost, increase_io, increase_cpu, decrease_io,
                          decrease_cpu)) {
      cur_compressor_idx_ = choice;
      return cur_compressor_idx_;
    }
  }
  // If we did not find any other compression type and level in our intended
  // quadrant, we may choose not to compress if we aim to increase IO and
  // decrease CPU usage. Otherwise, the current compression type and level
  // is the best we can do.
  if (increase_io && decrease_cpu) {
    // An index that is above the size of created compressors is treated as a
    // signal for no compression
    cur_compressor_idx_ = std::numeric_limits<size_t>::max();
  }
  return cur_compressor_idx_;
}

Status AutoTuneCompressor::CompressBlockAndRecord(
    size_t compressor_index, Slice uncompressed_data,
    std::string* compressed_output, CompressionType* out_compression_type,
    CostAwareWorkingArea* wa) {
  assert(compressor_index < compressors_.size());
  StopWatchNano<> timer(Env::Default()->GetSystemClock().get(), true);
  Status status = compressors_[compressor_index]->CompressBlock(
      uncompressed_data, compressed_output, out_compression_type,
      &(wa->wrapped_));
  std::pair<size_t, size_t> measured_data(timer.ElapsedMicros(),
                                          compressed_output->size());
  auto output_length = measured_data.second;
  auto cpu_time = measured_data.first;
  wa->cost_predictors_[compressor_index]->CPUPredictor.Record(cpu_time);
  wa->cost_predictors_[compressor_index]->IOPredictor.Record(output_length);
  return status;
}

std::shared_ptr<CompressionManagerWrapper> CreateAutoSkipCompressionManager(
    std::shared_ptr<CompressionManager> wrapped) {
  return std::make_shared<AutoSkipCompressorManager>(
      wrapped == nullptr ? GetBuiltinV2CompressionManager() : wrapped);
}
const char* AutoTuneCompressorManager::Name() const {
  // should have returned "AutoTuneCompressorManager" but we
  // currently have an error so for now returning name of the wrapped
  // container
  return wrapped_->Name();
}

std::unique_ptr<Compressor> AutoTuneCompressorManager::GetCompressorForSST(
    const FilterBuildingContext& context, const CompressionOptions& opts,
    CompressionType preferred) {
  (void)context;
  if (AutoTuneCompressionManagerSupported() && preferred != kNoCompression) {
    return std::make_unique<AutoTuneCompressor>(opts, preferred, io_goal_,
                                                cpu_budget_, rate_limiter_);
  } else {
    return nullptr;
  }
}

std::shared_ptr<CompressionManagerWrapper> CreateAutoTuneCompressionManager(
    const std::shared_ptr<CompressionManager>& wrapped,
    const std::shared_ptr<IOGoal>& io_goal,
    const std::shared_ptr<CPUBudget>& cpu_budget,
    const std::shared_ptr<RateLimiter>& rate_limiter) {
  return std::make_shared<AutoTuneCompressorManager>(
      wrapped == nullptr ? GetBuiltinV2CompressionManager() : wrapped,
      io_goal == nullptr ? std::make_shared<IOGoal>(0.99, 0.9) : io_goal,
      cpu_budget == nullptr ? std::make_shared<CPUBudget>(0.9, 0.8)
                            : cpu_budget,
      rate_limiter);
}
bool AutoTuneCompressionManagerSupported() {
  auto supported_compressions = GetSupportedCompressions();
  // Check if KLZ4Compression, KLZ4HCCompression and kZSTDCompression are
  // supported before running the test case as they must be supported for us
  // to have at least two compressors
  return std::find(supported_compressions.begin(), supported_compressions.end(),
                   kLZ4Compression) != supported_compressions.end() ||
         std::find(supported_compressions.begin(), supported_compressions.end(),
                   kLZ4HCCompression) != supported_compressions.end() ||
         std::find(supported_compressions.begin(), supported_compressions.end(),
                   kZSTD) != supported_compressions.end();
}
}  // namespace ROCKSDB_NAMESPACE
