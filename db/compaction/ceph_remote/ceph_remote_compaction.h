// ceph_remote_compaction.h - Fixed for RocksDB internal integration
#ifndef ROCKSDB_CEPH_REMOTE_COMPACTION_H
#define ROCKSDB_CEPH_REMOTE_COMPACTION_H

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include <grpcpp/grpcpp.h>
#include "agent_service.grpc.pb.h"
#include "worker_service.grpc.pb.h"
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include <map>
#include <condition_variable>
#include <chrono>
#include <thread>
#include <sstream>
#include <iomanip>
#include "db/compaction/compaction_job.h"

namespace ROCKSDB_NAMESPACE {

class CephRemoteCompactionService : public CompactionService {
public:
    struct Config {
        std::string cephfs_path;
        std::string agent_endpoint;
        int timeout_seconds;
        bool enable_fallback;
        uint64_t min_compaction_size;
        bool enable_detailed_logging;
        
        Config() 
            : cephfs_path("/mnt/cephfs/rocksdb_test")
            , agent_endpoint("220.149.236.151:50050")
            , timeout_seconds(300)
            , enable_fallback(true)
            , min_compaction_size(50 * 1024 * 1024)
            , enable_detailed_logging(true)
        {}
    };

    explicit CephRemoteCompactionService(const Config& config = Config());
    ~CephRemoteCompactionService() override;

    // CompactionService interface
    const char* Name() const override { return "CephRemoteCompactionService"; }
    
    CompactionServiceScheduleResponse Schedule(
        const CompactionServiceJobInfo& info,
        const std::string& compaction_service_input) override;
    
    CompactionServiceJobStatus Wait(
        const std::string& scheduled_job_id, std::string* result) override;

    // Statistics
    void GetStatistics(std::map<std::string, uint64_t>* stats) const;

private:
    struct JobInfo {
        std::string job_id;
        std::string worker_endpoint;
        std::string input_data;
        std::vector<std::string> input_files;
        int output_level;
        std::string output_data;
        CompactionServiceJobStatus status;
        std::chrono::steady_clock::time_point start_time;
        std::mutex mutex;
        std::condition_variable cv;
        bool completed;
        
        JobInfo() 
            : output_level(0)
            , status(CompactionServiceJobStatus::kSuccess)
            , completed(false) 
        {}
    };

    Config config_;
    
    // Agent gRPC connection
    std::shared_ptr<grpc::Channel> agent_channel_;
    std::unique_ptr<agent::RemoteCompactionAgent::Stub> agent_stub_;
    
    mutable std::mutex jobs_mutex_;
    std::map<std::string, std::shared_ptr<JobInfo>> active_jobs_;
    std::atomic<uint64_t> job_counter_;
    
    // Statistics
    mutable std::atomic<uint64_t> total_compactions_;
    mutable std::atomic<uint64_t> remote_compactions_;
    mutable std::atomic<uint64_t> local_compactions_;
    mutable std::atomic<uint64_t> failed_compactions_;
    mutable std::atomic<uint64_t> total_bytes_processed_;
    
    // Core functionality
    void TestAgentConnection();
    bool ShouldUseRemoteCompaction(const std::string& input_data);
    std::string SelectWorkerViaAgent(const std::vector<std::string>& input_files);
    void ExecuteRemoteCompactionAsync(std::shared_ptr<JobInfo> job);
    
    // Input parsing
    struct CompactionInputInfo {
        std::vector<std::string> input_files;
        int output_level = 0;
        uint64_t total_size = 0;

        CompactionInputInfo() : total_size(0) {}
    };
    
    CompactionInputInfo ParseInputData(const std::string& input_data);
    std::string CreateOutputData(const worker::CompactionResponse& response, int output_level);
    //std::string CreateOutputData(const std::string& output_file);

    
    // Utility functions
    std::string GenerateJobId();
    std::string FormatBytes(uint64_t bytes) const;
    std::string CompactionReasonToString(CompactionReason reason) const;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_CEPH_REMOTE_COMPACTION_H