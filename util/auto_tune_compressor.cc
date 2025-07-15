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
const std::vector<std::vector<int>>
    AutoCompressionAlgoLevelSelector::kCompressionLevels{
        {0},         // KSnappyCompression
        {},          // kZlibCompression
        {},          // kBZip2Compression
        {1, 4, 9},   // kLZ4Compression
        {1, 4, 9},   // klZ4HCCompression
        {},          // kXpressCompression
        {1, 15, 22}  // kZSTD
    };

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

AutoCompressionAlgoLevelSelector::AutoCompressionAlgoLevelSelector(
    const CompressionOptions& opts, std::shared_ptr<IOBudget> io_budget,
    std::shared_ptr<CPUBudget> cpu_budget,
    std::shared_ptr<RateLimiter> rate_limiter)
    : MultiCompressorWrapper(opts),
      opts_(opts),
      io_budget_(io_budget),
      cpu_budget_(cpu_budget),
      rate_limiter_(rate_limiter),
      usage_tracker_(rate_limiter) {
  // Create compressors supporting all the compression types and levels as per
  // the compression levels set in vector CompressionLevels.
  auto builtInManager = GetBuiltinV2CompressionManager();
  const auto& compressions = GetSupportedCompressions();
  for (size_t i = 0; i < kCompressionLevels.size(); i++) {
    CompressionType type = static_cast<CompressionType>(i + 1);
    if (type == kNoCompression) {
      continue;
    }
    if (kCompressionLevels[type - 1].size() == 0) {
      continue;
    } else {
      // If the compression type is not supported, then skip and remove
      // compression levels from the supported compression level list.
      if (std::find(compressions.begin(), compressions.end(), type) ==
          compressions.end()) {
        continue;
      }
      for (size_t j = 0; j < kCompressionLevels[type - 1].size(); j++) {
        auto level = kCompressionLevels[type - 1][j];
        CompressionOptions new_opts = opts;
        new_opts.level = level;
        compressors_.emplace_back(
            builtInManager->GetCompressor(new_opts, type));
      }
    }
  }
  MeasureUtilization();
  block_count_ = 0;
  cur_compressor_idx_ = 0;
}
AutoCompressionAlgoLevelSelector::~AutoCompressionAlgoLevelSelector() {}
const char* AutoCompressionAlgoLevelSelector::Name() const {
  return "AutoCompressionAlgoLevelSelector";
}
std::unique_ptr<Compressor>
AutoCompressionAlgoLevelSelector::MaybeCloneSpecialized(
    CacheEntryRole block_type, DictSampleArgs&& dict_samples) {
  // TODO: full dictionary compression support. Currently this just falls
  // back on a non-multi compressor when asked to use a dictionary.
  assert(compressors_.size() > 0);
  auto idx = compressors_.size() - 1;
  return compressors_[idx]->MaybeCloneSpecialized(block_type,
                                                  std::move(dict_samples));
}
Status AutoCompressionAlgoLevelSelector::CompressBlock(
    Slice uncompressed_data, std::string* compressed_output,
    CompressionType* out_compression_type, ManagedWorkingArea* wa) {
  // Check if the managed working area is provided or owned by this object.
  // If not, bypass compressor logic since the working area lacks a predictor.
  if (wa == nullptr || wa->owner() != this) {
    assert(compressors_.size() > 0);
    auto idx = 0;
    return compressors_[idx]->CompressBlock(
        uncompressed_data, compressed_output, out_compression_type, wa);
  }
  if (compressors_.size() == 0) {
    return Status::NotSupported("No compression type supported");
  }

  auto local_wa = static_cast<CostAwareWorkingArea*>(wa->get());
  bool exploration =
      Random::GetTLSInstance()->PercentTrue(kExplorationPercentage);
  if (exploration) {
    size_t choosen_index = Random::GetTLSInstance()->Uniform(
        static_cast<int>(compressors_.size()));

    return CompressBlockAndRecord(choosen_index, uncompressed_data,
                                  compressed_output, out_compression_type,
                                  local_wa);
  } else {
    auto chosen_index = SelectCompressionBasedOnGoal(local_wa);
    // Check if the chosen compression type and level are available
    // if not, skip the compression
    if (chosen_index >= compressors_.size()) {
      *out_compression_type = kNoCompression;
      return Status::OK();
    }
    return CompressBlockAndRecord(chosen_index, uncompressed_data,
                                  compressed_output, out_compression_type,
                                  local_wa);
  }
}

Compressor::ManagedWorkingArea
AutoCompressionAlgoLevelSelector::ObtainWorkingArea() {
  auto wrap_wa = compressors_.back()->ObtainWorkingArea();
  auto wa = new CostAwareWorkingArea(std::move(wrap_wa));
  // Create cost predictors for each compression type and level
  wa->cost_predictors_.reserve(compressors_.size());
  for (size_t i = 0; i < kCompressionLevels.size(); i++) {
    for (size_t j = 0; j < kCompressionLevels[i].size(); j++) {
      wa->cost_predictors_.emplace_back(new IOCPUCostPredictor(kWindow));
    }
  }
  return ManagedWorkingArea(wa, this);
}
void AutoCompressionAlgoLevelSelector::MeasureUtilization() {
  usage_tracker_.Record();
}
void AutoCompressionAlgoLevelSelector::ReleaseWorkingArea(WorkingArea* wa) {
  // remove all created cost predictors
  for (auto& predictor :
       static_cast<CostAwareWorkingArea*>(wa)->cost_predictors_) {
    delete predictor;
  }
  delete static_cast<CostAwareWorkingArea*>(wa);
}
size_t AutoCompressionAlgoLevelSelector::SelectCompressionBasedOnGoal(
    CostAwareWorkingArea* wa) {
  // If no budgets are available, use default choice
  if (!cpu_budget_ || !io_budget_) {
    size_t default_choice = 0;
    return default_choice;
  }
  if ((block_count_.fetch_add(1, std::memory_order_relaxed)) %
          kDecideEveryNBlocks !=
      0) {
    return cur_compressor_idx_;
  }
  MeasureUtilization();
  auto cpu_util = usage_tracker_.GetCpuUtilization();
  auto io_util = usage_tracker_.GetIoUtilization();
  // Select compression whose cpu cost and io cost are within budget
  // Return the first compression type and level that fits within budget
  // Get available budgets
  auto cpu_goal = cpu_budget_->GetRate() / kMicrosInSecond;
  auto io_goal = io_budget_->GetRate();
  static constexpr double kAcceptableLowCpuGoal = 0.8;
  static constexpr double kAcceptableLowIOGoal = 0.9;
  // Detect the 4 quadrant that we want to explore
  // Stable region is between the (kAcceptableLowCpuGoal * cpu_goal) to cpu_goal
  // and  and (kAcceptableLowIOGoal * io_goal) to io_goal
  bool increase_io = io_util < (kAcceptableLowIOGoal * io_goal);
  bool decrease_io = io_util > io_goal;
  bool increase_cpu = cpu_util < (kAcceptableLowCpuGoal * cpu_goal);
  bool decrease_cpu = cpu_util > cpu_goal;
  // If we are in the stable region, then we just go ahead with the current
  // compression type and level
  if (!increase_io && !increase_cpu && !decrease_io && !decrease_cpu) {
    return cur_compressor_idx_;
  } else if (cur_compressor_idx_ >= compressors_.size()) {
    // If the current compression type and level are not available i.e.
    // Compression is disabled We can switch from no compression if we can
    // decrease io and increase cpu usage else we current no compression is the
    // best we can do
    if (decrease_io && increase_cpu) {
      cur_compressor_idx_ = 0;
      return cur_compressor_idx_;
    }
    return cur_compressor_idx_;
  }
  // If we are in the unstable region, then we need to explore the other which
  // is in the right quadrant where we want to move to
  auto cur_cpu_cost =
      wa->cost_predictors_[cur_compressor_idx_]->CPUPredictor.Predict();
  auto cur_io_cost =
      wa->cost_predictors_[cur_compressor_idx_]->IOPredictor.Predict();
  for (size_t choice = 0; choice < compressors_.size(); choice++) {
    auto predicted_io_cost =
        wa->cost_predictors_[choice]->IOPredictor.Predict();
    auto predicted_cpu_cost =
        wa->cost_predictors_[choice]->CPUPredictor.Predict();
    bool flag = true;
    if (increase_io) {
      if (predicted_io_cost <= cur_io_cost) {
        flag = false;
      }
    }
    if (decrease_io) {
      if (predicted_io_cost >= cur_io_cost) {
        flag = false;
      }
    }
    if (increase_cpu) {
      if (predicted_cpu_cost <= cur_cpu_cost) {
        flag = false;
      }
    }
    if (decrease_cpu) {
      if (predicted_cpu_cost >= cur_cpu_cost) {
        flag = false;
      }
    }
    if (flag) {
      cur_compressor_idx_ = choice;
      return cur_compressor_idx_;
    }
  }
  // If we did not find any other compression type and level in our intended
  // quadrant, we may choose not to compress if we aim to increase IO and
  // decrease CPU usage. Otherwise, the current compression type and level
  // is the best we can do.
  if (increase_io && decrease_cpu) {
    cur_compressor_idx_ = std::numeric_limits<size_t>::max();
  }
  return cur_compressor_idx_;
}

Status AutoCompressionAlgoLevelSelector::CompressBlockAndRecord(
    size_t chosen_index, Slice uncompressed_data,
    std::string* compressed_output, CompressionType* out_compression_type,
    CostAwareWorkingArea* wa) {
  assert(chosen_index < compressors_.size());
  StopWatchNano<> timer(Env::Default()->GetSystemClock().get(), true);
  Status status = compressors_[chosen_index]->CompressBlock(
      uncompressed_data, compressed_output, out_compression_type,
      &(wa->wrapped_));
  std::pair<size_t, size_t> measured_data(timer.ElapsedMicros(),
                                          compressed_output->size());
  auto predictor = wa->cost_predictors_[chosen_index];
  auto output_length = measured_data.second;
  auto cpu_time = measured_data.first;
  predictor->CPUPredictor.Record(cpu_time);
  predictor->IOPredictor.Record(output_length);
  return status;
}

std::shared_ptr<CompressionManagerWrapper> CreateAutoSkipCompressionManager(
    std::shared_ptr<CompressionManager> wrapped) {
  return std::make_shared<AutoSkipCompressorManager>(
      wrapped == nullptr ? GetBuiltinV2CompressionManager() : wrapped);
}
const char* AutoCompressionAlgoLevelSelectorManager::Name() const {
  // should have returned "AutoCompressionAlgoLevelSelectorManager" but we
  // currently have an error so for now returning name of the wrapped container
  return wrapped_->Name();
}

std::unique_ptr<Compressor>
AutoCompressionAlgoLevelSelectorManager::GetCompressorForSST(
    const FilterBuildingContext& context, const CompressionOptions& opts,
    CompressionType preferred) {
  assert(GetSupportedCompressions().size() > 1);
  (void)context;
  (void)preferred;

  // Get budgets from budget factory if available
  std::shared_ptr<IOBudget> io_budget = nullptr;
  std::shared_ptr<CPUBudget> cpu_budget = nullptr;
  std::shared_ptr<RateLimiter> rate_limiter = nullptr;
  if (budget_factory_) {
    auto budgets = budget_factory_->GetBudget();
    io_budget = budgets.first;
    cpu_budget = budgets.second;
    rate_limiter = budget_factory_->GetOptions().rate_limiter;
  }

  return std::make_unique<AutoCompressionAlgoLevelSelector>(
      opts, io_budget, cpu_budget, rate_limiter);
}

std::shared_ptr<CompressionManagerWrapper> CreateCostAwareCompressionManager(
    std::shared_ptr<CompressionManager> wrapped,
    std::shared_ptr<CPUIOBudgetFactory> budget_factory) {
  return std::make_shared<AutoCompressionAlgoLevelSelectorManager>(
      wrapped == nullptr ? GetBuiltinV2CompressionManager() : wrapped,
      budget_factory);
}

std::pair<std::shared_ptr<IOBudget>, std::shared_ptr<CPUBudget>>
DefaultBudgetFactory::GetBudget() {
  static std::shared_ptr<IOBudget> io_budget =
      std::make_shared<IOBudget>(io_budget_, us_per_time_);
  static std::shared_ptr<CPUBudget> cpu_budget =
      std::make_shared<CPUBudget>(cpu_budget_, us_per_time_);
  return {io_budget, cpu_budget};
}
}  // namespace ROCKSDB_NAMESPACE
