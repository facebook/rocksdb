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
#include "util/stop_watch.h"
namespace ROCKSDB_NAMESPACE {
const std::vector<std::vector<int>> CostAwareCompressor::kCompressionLevels{
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
  // block
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

CostAwareCompressor::CostAwareCompressor(const CompressionOptions& opts,
                                         IOBudget* io_budget,
                                         CPUBudget* cpu_budget,
                                         RateLimiter* rate_limiter)
    : opts_(opts),
      io_budget_(io_budget),
      cpu_budget_(cpu_budget),
      rate_limiter_(rate_limiter) {
  // Creates compressor supporting all the compression types and levels as per
  // the compression levels set in vector CompressionLevels
  auto builtInManager = GetBuiltinV2CompressionManager();
  const auto& compressions = GetSupportedCompressions();
  for (size_t i = 0; i < kCompressionLevels.size(); i++) {
    CompressionType type = static_cast<CompressionType>(i + 1);
    if (type == kNoCompression) {
      continue;
    }
    if (kCompressionLevels[type - 1].size() == 0) {
      allcompressors_.emplace_back();
      continue;
    } else {
      // if the compression type is not supported, then skip and remove
      // compression levels from the supported compression level list
      if (std::find(compressions.begin(), compressions.end(), type) ==
          compressions.end()) {
        allcompressors_.emplace_back();
        continue;
      }
      std::vector<std::unique_ptr<Compressor>> compressors_diff_levels;
      for (size_t j = 0; j < kCompressionLevels[type - 1].size(); j++) {
        auto level = kCompressionLevels[type - 1][j];
        CompressionOptions new_opts = opts;
        new_opts.level = level;
        compressors_diff_levels.push_back(
            builtInManager->GetCompressor(new_opts, type));
        allcompressors_index_.emplace_back(i, j);
      }
      allcompressors_.push_back(std::move(compressors_diff_levels));
    }
  }
  MeasureUtilization();
  block_count_ = 0;
  cur_comp_idx_ = allcompressors_index_.front();
}
CostAwareCompressor::~CostAwareCompressor() {}
const char* CostAwareCompressor::Name() const { return "CostAwareCompressor"; }
size_t CostAwareCompressor::GetMaxSampleSizeIfWantDict(
    CacheEntryRole block_type) const {
  auto idx = allcompressors_index_.back();
  return allcompressors_[idx.first][idx.second]->GetMaxSampleSizeIfWantDict(
      block_type);
}

Slice CostAwareCompressor::GetSerializedDict() const {
  auto idx = allcompressors_index_.back();
  return allcompressors_[idx.first][idx.second]->GetSerializedDict();
}

CompressionType CostAwareCompressor::GetPreferredCompressionType() const {
  return kZSTD;
}
std::unique_ptr<Compressor> CostAwareCompressor::MaybeCloneSpecialized(
    CacheEntryRole block_type, DictSampleArgs&& dict_samples) {
  // TODO: full dictionary compression support. Currently this just falls
  // back on a non-multi compressor when asked to use a dictionary.
  auto idx = allcompressors_index_.back();
  return allcompressors_[idx.first][idx.second]->MaybeCloneSpecialized(
      block_type, std::move(dict_samples));
}
Status CostAwareCompressor::CompressBlock(Slice uncompressed_data,
                                          std::string* compressed_output,
                                          CompressionType* out_compression_type,
                                          ManagedWorkingArea* wa) {
  // Check if the managed working area is provided or owned by this object.
  // If not, bypass compressor logic since the working area lacks a predictor
  if (allcompressors_.size() == 0) {
    return Status::NotSupported("No compression type supported");
  }

  // Check budget availability before compression
  if (cpu_budget_ != nullptr && cpu_budget_->GetAvailableBudget() <= 0) {
    // CPU budget exhausted, skip compression
    *out_compression_type = kNoCompression;
    return Status::OK();
  }

  if (wa == nullptr || wa->owner() != this) {
    // highest compression level of Zstd
    size_t choosen_compression_type = 6;
    size_t compression_level_ptr = 0;
    return allcompressors_[choosen_compression_type][compression_level_ptr]
        ->CompressBlock(uncompressed_data, compressed_output,
                        out_compression_type, wa);
  }

  auto local_wa = static_cast<CostAwareWorkingArea*>(wa->get());
  bool exploration =
      Random::GetTLSInstance()->PercentTrue(kExplorationPercentage);
  if (exploration) {
    std::pair<size_t, size_t> choosen_index =
        allcompressors_index_[Random::GetTLSInstance()->Uniform(
            static_cast<int>(allcompressors_index_.size()))];
    size_t choosen_compression_type = choosen_index.first;
    size_t compresion_level_ptr = choosen_index.second;

    return CompressBlockAndRecord(
        choosen_compression_type, compresion_level_ptr, uncompressed_data,
        compressed_output, out_compression_type, local_wa);
  } else {
    // Switching to planned approach using following method for the choosing
    // compression level std::pair<size_t, size_t> choosen_index =
    //     SelectCompressionInDirectionOfBudget(local_wa);
    // Reactive approach to select compression algorithm to compression level
    // based on goal
    std::pair<size_t, size_t> choosen_index =
        SelectCompressionBasedOnGoal(local_wa);
    size_t choosen_compression_type = choosen_index.first;
    size_t compresion_level_ptr = choosen_index.second;
    // Check if the chosen compression type and level are available
    // if not, skip the compression
    if (choosen_compression_type >= allcompressors_.size() ||
        compresion_level_ptr >=
            allcompressors_[choosen_compression_type].size()) {
      *out_compression_type = kNoCompression;
      return Status::OK();
    }
    return CompressBlockAndRecord(
        choosen_compression_type, compresion_level_ptr, uncompressed_data,
        compressed_output, out_compression_type, local_wa);
  }
}

Compressor::ManagedWorkingArea CostAwareCompressor::ObtainWorkingArea() {
  auto wrap_wa = allcompressors_.back().back()->ObtainWorkingArea();
  auto wa = new CostAwareWorkingArea(std::move(wrap_wa));
  // Create cost predictors for each compression type and level
  wa->cost_predictors_.reserve(allcompressors_.size());
  for (size_t i = 0; i < allcompressors_.size(); i++) {
    CompressionType type = static_cast<CompressionType>(i + 1);
    if (allcompressors_[type - 1].size() == 0) {
      wa->cost_predictors_.emplace_back();
      continue;
    } else {
      std::vector<IOCPUCostPredictor*> predictors_diff_levels;
      predictors_diff_levels.reserve(kCompressionLevels[type - 1].size());
      for (size_t j = 0; j < kCompressionLevels[type - 1].size(); j++) {
        predictors_diff_levels.emplace_back(new IOCPUCostPredictor(10));
      }
      wa->cost_predictors_.emplace_back(std::move(predictors_diff_levels));
    }
  }
  return ManagedWorkingArea(wa, this);
}
void CostAwareCompressor::MeasureUtilization() {
  auto total_bytes = rate_limiter_->GetTotalBytesThrough();
  io_tracker_.Record(total_bytes);

#if defined(_WIN32)
  // Windows implementation
  fprintf(stderr, "Windows implementation not supported\n");
  exit(1);
#else
  // Unix/Linux implementation - use getrusage
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  double cpu_time_used =
      (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) +
      (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
  cpu_tracker_.Record(cpu_time_used);
#endif
}
void CostAwareCompressor::ReleaseWorkingArea(WorkingArea* wa) {
  // remove all created cost predictors
  for (auto& prdictors_diff_levels :
       static_cast<CostAwareWorkingArea*>(wa)->cost_predictors_) {
    for (auto& predictor : prdictors_diff_levels) {
      delete predictor;
    }
  }
  delete static_cast<CostAwareWorkingArea*>(wa);
}
std::pair<size_t, size_t> CostAwareCompressor::SelectCompressionBasedOnGoal(
    CostAwareWorkingArea* wa) {
  // If no budgets are available, use default choice
  if (!cpu_budget_ || !io_budget_) {
    std::pair<size_t, size_t> default_choice = allcompressors_index_.front();
    return default_choice;
  }
  block_count_++;
  if ((block_count_ % 2000) != 0) {
    return cur_comp_idx_;
  }
  MeasureUtilization();
  auto cpu_util = cpu_tracker_.GetRate();
  auto io_util = io_tracker_.GetRate();
  // Select compression whose cpu cost and io cost are within budget
  // Return the first compression type and level that fits within budget
  // Get available budgets
  auto cpu_goal = cpu_budget_->GetRate() / kMicrosInSecond;
  auto io_goal = io_budget_->GetRate();

  // Check if we need to increase or decrease io utilization
  bool increase_io = io_util < (0.9 * io_goal);
  bool decrease_io = io_util > (1 * io_goal);
  bool increase_cpu = cpu_util < (0.8 * cpu_goal);
  bool decrease_cpu = cpu_util > (1 * cpu_goal);
  if (!increase_io && !increase_cpu && !decrease_io && !decrease_cpu) {
    return cur_comp_idx_;
  } else if (cur_comp_idx_.first >= allcompressors_.size() ||
             cur_comp_idx_.second >=
                 allcompressors_[cur_comp_idx_.first].size()) {
    if (decrease_io && increase_cpu) {
      // if we switch from no compression to compression, then we need to use
      // more cpu and than may lead to less io
      cur_comp_idx_ = allcompressors_index_.front();
      return cur_comp_idx_;
    }
    return cur_comp_idx_;
  }

  auto cur_cpu_cost =
      wa->cost_predictors_[cur_comp_idx_.first][cur_comp_idx_.second]
          ->CPUPredictor.Predict();
  auto cur_io_cost =
      wa->cost_predictors_[cur_comp_idx_.first][cur_comp_idx_.second]
          ->IOPredictor.Predict();
  for (const auto& choice : allcompressors_index_) {
    size_t comp_type = choice.first;
    size_t comp_level = choice.second;
    auto predicted_io_cost =
        wa->cost_predictors_[comp_type][comp_level]->IOPredictor.Predict();
    auto predicted_cpu_cost =
        wa->cost_predictors_[comp_type][comp_level]->CPUPredictor.Predict();
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
      cur_comp_idx_ = choice;
      return cur_comp_idx_;
    }
  }

  if (increase_io && decrease_cpu) {
    cur_comp_idx_ = {std::numeric_limits<size_t>::max(),
                     std::numeric_limits<size_t>::max()};
  }
  return cur_comp_idx_;
}
std::pair<size_t, size_t>
CostAwareCompressor::SelectCompressionInDirectionOfBudget(
    CostAwareWorkingArea* wa) {
  // If no budgets are available, use default choice
  if (!cpu_budget_ || !io_budget_) {
    std::pair<size_t, size_t> default_choice = allcompressors_index_.front();
    return default_choice;
  }
  // Select compression whose cpu cost and io cost are within budget
  // Return the first compression type and level that fits within budget
  // Get available budgets
  size_t available_cpu = cpu_budget_->GetAvailableBudget();
  size_t available_io = io_budget_->GetAvailableBudget();
  size_t total_cpu = cpu_budget_->GetTotalBudget();
  size_t total_io = io_budget_->GetTotalBudget();
  std::pair<size_t, size_t> best_choice(std::numeric_limits<size_t>::max(),
                                        std::numeric_limits<size_t>::max());
  if (available_cpu <= 0) {
    return best_choice;
  }
  double total = sqrt(
      static_cast<double>(available_cpu) * static_cast<double>(available_cpu) +
      static_cast<double>(available_io) * static_cast<double>(available_io));
  double min_cosine_distance = std::numeric_limits<double>::max();
  auto cosine_distance = [&](int64_t cpu_cost, int64_t io_cost) {
    return available_cpu / total * cpu_cost / total_cpu +
           available_io / total * io_cost / total_io;
  };
  for (const auto& choice : allcompressors_index_) {
    size_t comp_type = choice.first;
    size_t comp_level = choice.second;
    auto predicted_cpu_cost =
        wa->cost_predictors_[comp_type][comp_level]->CPUPredictor.Predict();
    auto predicted_io_cost =
        wa->cost_predictors_[comp_type][comp_level]->IOPredictor.Predict();
    auto distance = cosine_distance(predicted_cpu_cost, predicted_io_cost);
    if (distance < min_cosine_distance) {
      min_cosine_distance = distance;
      best_choice = choice;
    }
  }
  return best_choice;
}

Status CostAwareCompressor::CompressBlockAndRecord(
    size_t choosen_compression_type, size_t compression_level_ptr,
    Slice uncompressed_data, std::string* compressed_output,
    CompressionType* out_compression_type, CostAwareWorkingArea* wa) {
  assert(choosen_compression_type < allcompressors_.size());
  assert(compression_level_ptr <
         allcompressors_[choosen_compression_type].size());
  assert(choosen_compression_type < wa->cost_predictors_.size());
  assert(compression_level_ptr <
         wa->cost_predictors_[choosen_compression_type].size());
  StopWatchNano<> timer(Env::Default()->GetSystemClock().get(), true);
  Status status =
      allcompressors_[choosen_compression_type][compression_level_ptr]
          ->CompressBlock(uncompressed_data, compressed_output,
                          out_compression_type, &(wa->wrapped_));
  std::pair<size_t, size_t> measured_data(timer.ElapsedMicros(),
                                          compressed_output->size());
  auto predictor =
      wa->cost_predictors_[choosen_compression_type][compression_level_ptr];
  auto output_length = measured_data.second;
  auto cpu_time = measured_data.first;
  predictor->CPUPredictor.Record(cpu_time);
  predictor->IOPredictor.Record(output_length);
  // Consume the budget in the planned approach
  // if (cpu_budget_) cpu_budget_->TryConsume(cpu_time);
  // if (io_budget_) io_budget_->TryConsume(output_length);
  return status;
}

std::shared_ptr<CompressionManagerWrapper> CreateAutoSkipCompressionManager(
    std::shared_ptr<CompressionManager> wrapped) {
  return std::make_shared<AutoSkipCompressorManager>(
      wrapped == nullptr ? GetBuiltinV2CompressionManager() : wrapped);
}
const char* CostAwareCompressorManager::Name() const {
  // should have returned "CostAwareCompressorManager" but we currently have
  // an error so for now returning name of the wrapped container
  return wrapped_->Name();
}

std::unique_ptr<Compressor> CostAwareCompressorManager::GetCompressorForSST(
    const FilterBuildingContext& context, const CompressionOptions& opts,
    CompressionType preferred) {
  assert(GetSupportedCompressions().size() > 1);
  (void)context;
  (void)preferred;

  // Get budgets from budget factory if available
  IOBudget* io_budget = nullptr;
  CPUBudget* cpu_budget = nullptr;
  RateLimiter* rate_limiter = nullptr;
  if (budget_factory_) {
    auto budgets = budget_factory_->GetBudget();
    io_budget = budgets.first;
    cpu_budget = budgets.second;
    rate_limiter = budget_factory_->GetOptions().rate_limiter.get();
  }

  return std::make_unique<CostAwareCompressor>(opts, io_budget, cpu_budget,
                                               rate_limiter);
}

std::shared_ptr<CompressionManagerWrapper> CreateCostAwareCompressionManager(
    std::shared_ptr<CompressionManager> wrapped,
    std::shared_ptr<CPUIOBudgetFactory> budget_factory) {
  return std::make_shared<CostAwareCompressorManager>(
      wrapped == nullptr ? GetBuiltinV2CompressionManager() : wrapped,
      budget_factory);
}

std::pair<IOBudget*, CPUBudget*> DefaultBudgetFactory::GetBudget() {
  static IOBudget io_budget(io_budget_, us_per_time_);
  static CPUBudget cpu_budget(cpu_budget_, us_per_time_);
  // fprintf(stderr, "io_budget: %f cpu_budget: %f\n", io_budget.GetRate(),
  // cpu_budget.GetRate());
  return std::pair<IOBudget*, CPUBudget*>(&io_budget, &cpu_budget);
}
}  // namespace ROCKSDB_NAMESPACE
