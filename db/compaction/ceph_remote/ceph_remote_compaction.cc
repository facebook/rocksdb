// ceph_remote_compaction.cc - Agent gRPC 통합 버전
// 수정: CreateOutputData()에서 output_file_metadata 활용 (다중 파일 지원)
#include "ceph_remote_compaction.h"
#include "worker_service.grpc.pb.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <sstream>
#include <iomanip>
#include <sys/stat.h>
#include "db/compaction/compaction_job.h"
#include "table/table_properties_internal.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/format.h"
#include "rocksdb/sst_file_reader.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/table_properties_internal.h"
#include "rocksdb/sst_file_reader.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"
#include "file/random_access_file_reader.h"


namespace ROCKSDB_NAMESPACE {

CephRemoteCompactionService::CephRemoteCompactionService(const Config& config)
    : config_(config)
    , job_counter_(0)
    , total_compactions_(0)
    , remote_compactions_(0)
    , local_compactions_(0)
    , failed_compactions_(0)
    , total_bytes_processed_(0) {
    
    // Create persistent Agent gRPC channel
    agent_channel_ = grpc::CreateChannel(
        config_.agent_endpoint,
        grpc::InsecureChannelCredentials()
    );
    agent_stub_ = agent::RemoteCompactionAgent::NewStub(agent_channel_);
    
    if (config_.enable_detailed_logging) {
        std::cout << "CephRemoteCompactionService initialized (Agent gRPC Mode)" << std::endl;
        std::cout << "  Agent endpoint: " << config_.agent_endpoint << std::endl;
        std::cout << "  CephFS path: " << config_.cephfs_path << std::endl;
    }
    
    // Test agent connectivity
    TestAgentConnection();
}

CephRemoteCompactionService::~CephRemoteCompactionService() {
    // Wait for all jobs to complete
    std::lock_guard<std::mutex> lock(jobs_mutex_);
    for (auto& [job_id, job] : active_jobs_) {
        std::unique_lock<std::mutex> job_lock(job->mutex);
        job->cv.wait_for(job_lock, std::chrono::seconds(5), 
                        [&job] { return job->completed; });
    }
    
    if (config_.enable_detailed_logging && total_compactions_.load() > 0) {
        std::cout << "CephRemoteCompactionService shutdown. Stats:" << std::endl;
        std::cout << "  Total: " << total_compactions_.load() << std::endl;
        std::cout << "  Remote: " << remote_compactions_.load() << std::endl;
        std::cout << "  Local: " << local_compactions_.load() << std::endl;
    }
}

void CephRemoteCompactionService::TestAgentConnection() {
    try {
        grpc::ClientContext context;
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
        context.set_deadline(deadline);
        
        agent::AgentStatusRequest request;
        agent::AgentStatusResponse response;
        
        grpc::Status status = agent_stub_->GetAgentStatus(&context, request, &response);
        
        if (status.ok() && response.ceph_connected()) {
            if (config_.enable_detailed_logging) {
                std::cout << "✓ Agent connected successfully" << std::endl;
                std::cout << "  Total OSDs: " << response.total_osds() << std::endl;
                std::cout << "  Available workers: " << response.available_workers() << std::endl;
            }
        } else {
            std::cerr << "⚠ Agent connection warning: " << status.error_message() << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "⚠ Agent test failed: " << e.what() << std::endl;
    }
}

rocksdb::CompactionServiceScheduleResponse CephRemoteCompactionService::Schedule(
    const rocksdb::CompactionServiceJobInfo& info,
    const std::string& compaction_service_input) {
    
    total_compactions_++;
    
    if (config_.enable_detailed_logging) {
        std::cout << "\n" << std::string(60, '=') << std::endl;
        std::cout << "=== REMOTE COMPACTION TRIGGERED ===" << std::endl;
        std::cout << "DB: " << info.db_name << std::endl;
        std::cout << "Job ID: " << info.job_id << std::endl;
        std::cout << "Reason: " << CompactionReasonToString(info.compaction_reason) << std::endl;
        std::cout << "Manual: " << (info.is_manual_compaction ? "YES" : "NO") << std::endl;
        std::cout << "Full: " << (info.is_full_compaction ? "YES" : "NO") << std::endl;
        std::cout << std::string(60, '=') << std::endl;
    }
    
    try {
        // Check if we should use remote compaction
        if (!ShouldUseRemoteCompaction(compaction_service_input)) {
            local_compactions_++;
            if (config_.enable_detailed_logging) {
                std::cout << "Decision: Use LOCAL compaction (below threshold)" << std::endl;
            }
            return rocksdb::CompactionServiceScheduleResponse(
                rocksdb::CompactionServiceJobStatus::kUseLocal);
        }
        
        // Parse input to extract SST files
        CompactionInputInfo input_info = ParseInputData(compaction_service_input);

        // Generate unique job ID
        std::string job_id = GenerateJobId();
        auto job = std::make_shared<JobInfo>();
        job->job_id = job_id;
        job->input_data = compaction_service_input;
        job->input_files = input_info.input_files;
        job->status = rocksdb::CompactionServiceJobStatus::kSuccess;
        job->start_time = std::chrono::steady_clock::now();
        
        // Call Agent to select worker via gRPC
        std::string worker_endpoint = SelectWorkerViaAgent(input_info.input_files);
        
        if (worker_endpoint.empty()) {
            failed_compactions_++;
            std::cerr << "Failed to select worker via Agent" << std::endl;
            
            if (config_.enable_fallback) {
                local_compactions_++;
                return rocksdb::CompactionServiceScheduleResponse(
                    rocksdb::CompactionServiceJobStatus::kUseLocal);
            } else {
                return rocksdb::CompactionServiceScheduleResponse(
                    rocksdb::CompactionServiceJobStatus::kFailure);
            }
        }
        
        job->worker_endpoint = worker_endpoint;
        
        // Store job for tracking
        {
            std::lock_guard<std::mutex> lock(jobs_mutex_);
            active_jobs_[job_id] = job;
        }
        
        // Start async execution
        std::thread([this, job]() {
            this->ExecuteRemoteCompactionAsync(job);
        }).detach();
        
        remote_compactions_++;
        
        if (config_.enable_detailed_logging) {
            std::cout << "✓ Scheduled REMOTE compaction" << std::endl;
            std::cout << "  Job ID: " << job_id << std::endl;
            std::cout << "  Worker: " << job->worker_endpoint << std::endl;
            std::cout << "  Files: " << job->input_files.size() << std::endl;
        }
        
        return rocksdb::CompactionServiceScheduleResponse(
            job_id, rocksdb::CompactionServiceJobStatus::kSuccess);
        
    } catch (const std::exception& e) {
        failed_compactions_++;
        std::cerr << "Schedule error: " << e.what() << std::endl;
        
        if (config_.enable_fallback) {
            local_compactions_++;
            return rocksdb::CompactionServiceScheduleResponse(
                rocksdb::CompactionServiceJobStatus::kUseLocal);
        } else {
            return rocksdb::CompactionServiceScheduleResponse(
                rocksdb::CompactionServiceJobStatus::kFailure);
        }
    }
}

rocksdb::CompactionServiceJobStatus CephRemoteCompactionService::Wait(
    const std::string& scheduled_job_id, std::string* result) {

    std::shared_ptr<JobInfo> job;
    {
        std::lock_guard<std::mutex> lock(jobs_mutex_);
        auto it = active_jobs_.find(scheduled_job_id);
        if (it == active_jobs_.end()) {
            if (config_.enable_detailed_logging) {
                std::cout << "Job not found: " << scheduled_job_id << std::endl;
            }
            if (result) {
                *result = "";
            }
            return rocksdb::CompactionServiceJobStatus::kFailure;
        }
        job = it->second;
    }

    // Wait for completion with timeout
    std::unique_lock<std::mutex> job_lock(job->mutex);
    bool completed = job->cv.wait_for(
        job_lock,
        std::chrono::seconds(config_.timeout_seconds),
        [&job] { return job->completed; }
    );

    if (!completed) {
        if (config_.enable_detailed_logging) {
            std::cout << "Job timeout: " << scheduled_job_id << std::endl;
        }
        job->status = rocksdb::CompactionServiceJobStatus::kFailure;
        failed_compactions_++;
    }

    // Set result if provided
    if (result) {
        *result = job->output_data;
    }

    rocksdb::CompactionServiceJobStatus final_status = job->status;

    if (config_.enable_detailed_logging) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - job->start_time).count();
        std::cout << "Job wait completed: " << scheduled_job_id
                  << ", duration: " << duration << "ms"
                  << ", status: " << (int)final_status << std::endl;
    }

    // Cleanup completed job
    {
        std::lock_guard<std::mutex> lock(jobs_mutex_);
        active_jobs_.erase(scheduled_job_id);
    }

    if (config_.enable_detailed_logging) {
        std::cout << "=== WAIT() RETURNING ===" << std::endl;
        std::cout << "final_status: " << (int)final_status << std::endl;
        std::cout << "kSuccess=" << (int)rocksdb::CompactionServiceJobStatus::kSuccess << std::endl;
        std::cout << "kUseLocal=" << (int)rocksdb::CompactionServiceJobStatus::kUseLocal << std::endl;
        std::cout << "kFailure=" << (int)rocksdb::CompactionServiceJobStatus::kFailure << std::endl;
    }
    return final_status;
}

std::string CephRemoteCompactionService::SelectWorkerViaAgent(
    const std::vector<std::string>& input_files) {

    try {
        grpc::ClientContext context;
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
        context.set_deadline(deadline);

        // Build request
        agent::WorkerSelectionRequest request;
        for (const auto& file : input_files) {
            request.add_input_sst_files(file);
        }
        request.set_pool_name("cephfs_data");

        if (config_.enable_detailed_logging) {
            std::cout << "Calling Agent for worker selection..." << std::endl;
            std::cout << "  Input files: " << input_files.size() << std::endl;
        }

        // Call Agent
        agent::WorkerSelectionResponse response;
        grpc::Status status = agent_stub_->SelectWorker(&context, request, &response);

        if (!status.ok()) {
            std::cerr << "Agent SelectWorker RPC failed: " << status.error_message() << std::endl;
            return "";
        }

        if (!response.success()) {
            std::cerr << "Agent returned error: " << response.error_message() << std::endl;
            return "";
        }

        if (config_.enable_detailed_logging) {
            std::cout << "✓ Agent response:" << std::endl;
            std::cout << "  Selected host: " << response.selected_worker_host() << std::endl;
            std::cout << "  Endpoint: " << response.worker_endpoint() << std::endl;
            std::cout << "  Locality score: " << response.locality_score() << "%" << std::endl;
        }

        return response.worker_endpoint();

    } catch (const std::exception& e) {
        std::cerr << "SelectWorkerViaAgent exception: " << e.what() << std::endl;
        return "";
    }
}

void CephRemoteCompactionService::ExecuteRemoteCompactionAsync(std::shared_ptr<JobInfo> job) {
    try {
        auto parsed_info = ParseInputData(job->input_data);
        job->input_files = parsed_info.input_files;
        job->output_level = parsed_info.output_level;
        
        if (config_.enable_detailed_logging) {
            std::cout << "\n--- Executing Remote Compaction ---" << std::endl;
            std::cout << "Job: " << job->job_id << std::endl;
            std::cout << "Worker: " << job->worker_endpoint << std::endl;
            std::cout << "Files: " << job->input_files.size() << std::endl;
            std::cout << "Output level: " << job->output_level << std::endl;
        }

        // Create gRPC connection to worker
        auto channel = grpc::CreateChannel(job->worker_endpoint,
                                         grpc::InsecureChannelCredentials());
        auto stub = worker::CompactionWorkerService::NewStub(channel);

        // Build compaction request
        worker::CompactionRequest request;
        request.set_job_id(job->job_id);

        // 파일 검증
        if (job->input_files.empty()) {
            std::lock_guard<std::mutex> job_lock(job->mutex);
            job->status = rocksdb::CompactionServiceJobStatus::kFailure;
            job->completed = true;
            job->cv.notify_all();
            failed_compactions_++;
            
            std::cerr << "ERROR: No input files parsed from compaction request" << std::endl;
            return;
        }

        // 파싱된 파일명 전달
        for (const auto& filename : job->input_files) {
            request.add_input_sst_files(filename);
        }

        if (config_.enable_detailed_logging) {
            std::cout << "Sending " << job->input_files.size() << " files to worker" << std::endl;
        }

        auto* options = request.mutable_options();
        options->set_compression_type("snappy");
        options->set_target_file_size(64 * 1024 * 1024);
        options->set_enable_statistics(true);

        // Execute with timeout
        grpc::ClientContext context;
        auto deadline = std::chrono::system_clock::now() +
                       std::chrono::seconds(config_.timeout_seconds);
        context.set_deadline(deadline);

        worker::CompactionResponse response;
        grpc::Status grpc_status = stub->PerformCompaction(&context, request, &response);

        // Process result
        std::lock_guard<std::mutex> job_lock(job->mutex);

        if (grpc_status.ok() && response.success()) {
            job->status = rocksdb::CompactionServiceJobStatus::kSuccess;

            if (response.output_sst_files_size() > 0) {
                job->output_data = CreateOutputData(response, job->output_level);
            }
                
            total_bytes_processed_ += response.input_size_bytes();

            if (config_.enable_detailed_logging) {
                std::cout << "✓ Remote compaction SUCCESS:" << std::endl;
                std::cout << "  Input: " << FormatBytes(response.input_size_bytes()) << std::endl;
                std::cout << "  Output: " << FormatBytes(response.output_size_bytes()) << std::endl;
                std::cout << "  Output files: " << response.output_sst_files_size() << std::endl;
                std::cout << "  Output file metadata: " << response.output_file_metadata_size() << std::endl;
                std::cout << "  Ratio: " << response.compression_ratio() << std::endl;
                std::cout << "  Time: " << response.processing_time_ms() << "ms" << std::endl;
                std::cout << "  Num entries: " << response.num_entries() << std::endl;
            }

        } else {
            job->status = rocksdb::CompactionServiceJobStatus::kFailure;
            failed_compactions_++;

            std::cerr << "✗ Remote compaction FAILED:" << std::endl;
            if (!grpc_status.ok()) {
                std::cerr << "  gRPC: " << grpc_status.error_message() << std::endl;
            } else {
                std::cerr << "  Worker: " << response.error_message() << std::endl;
            }
        }

        job->completed = true;
        job->cv.notify_all();

    } catch (const std::exception& e) {
        std::lock_guard<std::mutex> job_lock(job->mutex);
        job->status = rocksdb::CompactionServiceJobStatus::kFailure;
        job->completed = true;
        job->cv.notify_all();
        failed_compactions_++;

        std::cerr << "ExecuteRemoteCompactionAsync error: " << e.what() << std::endl;
    }
}

// Remote Compaction 강제로 실행 (디버깅용)
bool CephRemoteCompactionService::ShouldUseRemoteCompaction(const std::string& input_data) {
    CompactionInputInfo info = ParseInputData(input_data);
    
    bool has_agent = !config_.agent_endpoint.empty();
    
    // TEMPORARY: 크기 체크 무시하고 무조건 remote 시도
    bool force_remote = has_agent;
    
    if (config_.enable_detailed_logging) {
        std::cout << "\n--- Compaction Decision (FORCED REMOTE MODE) ---" << std::endl;
        std::cout << "Input data size: " << input_data.size() << " bytes" << std::endl;
        std::cout << "Parsed files: " << info.input_files.size() << std::endl;
        std::cout << "Has agent: " << (has_agent ? "YES" : "NO") << std::endl;
        std::cout << "Decision: REMOTE (FORCED FOR TESTING)" << std::endl;
    }
    
    return force_remote;
}

// =======================================================================
// Utility functions
// =======================================================================

std::string CephRemoteCompactionService::CompactionReasonToString(
    rocksdb::CompactionReason reason) const {
    switch (reason) {
        case rocksdb::CompactionReason::kUnknown:
            return "Unknown";
        case rocksdb::CompactionReason::kLevelL0FilesNum:
            return "Level0 Files";
        case rocksdb::CompactionReason::kLevelMaxLevelSize:
            return "Level Max Size";
        case rocksdb::CompactionReason::kUniversalSizeAmplification:
            return "Universal Size Amp";
        case rocksdb::CompactionReason::kUniversalSizeRatio:
            return "Universal Size Ratio";
        case rocksdb::CompactionReason::kUniversalSortedRunNum:
            return "Universal Sorted Runs";
        case rocksdb::CompactionReason::kFIFOMaxSize:
            return "FIFO Max Size";
        case rocksdb::CompactionReason::kFIFOReduceNumFiles:
            return "FIFO Reduce Files";
        case rocksdb::CompactionReason::kFIFOTtl:
            return "FIFO TTL";
        case rocksdb::CompactionReason::kManualCompaction:
            return "Manual";
        case rocksdb::CompactionReason::kFilesMarkedForCompaction:
            return "Files Marked";
        case rocksdb::CompactionReason::kBottommostFiles:
            return "Bottommost Files";
        case rocksdb::CompactionReason::kTtl:
            return "TTL";
        case rocksdb::CompactionReason::kFlush:
            return "Flush";
        case rocksdb::CompactionReason::kExternalSstIngestion:
            return "SST Ingestion";
        default:
            return "Other";
    }
}

CephRemoteCompactionService::CompactionInputInfo 
CephRemoteCompactionService::ParseInputData(const std::string& input_data) {
    CompactionInputInfo info;
    
    if (config_.enable_detailed_logging) {
        std::cout << "Parsing input data (" << input_data.size() << " bytes)" << std::endl;
    }
    
    // 방법 1: CompactionServiceInput 역직렬화 (표준)
    CompactionServiceInput cs_input;
    Status s = CompactionServiceInput::Read(input_data, &cs_input);
    
    if (s.ok()) {
        info.input_files = cs_input.input_files;
        info.output_level = cs_input.output_level;
        
        for (size_t i = 0; i < info.input_files.size(); i++) {
            info.total_size += 50 * 1024 * 1024;
        }
        
        if (config_.enable_detailed_logging) {
            std::cout << "✓ Parsed CompactionServiceInput:" << std::endl;
            std::cout << "  Files: " << info.input_files.size() << std::endl;
            std::cout << "  Output level: " << info.output_level << std::endl;
            for (const auto& f : info.input_files) {
                std::cout << "    - " << f << std::endl;
            }
        }
        return info;
    }
    
    // 방법 2: HEX 파싱 (Fallback)
    if (config_.enable_detailed_logging) {
        std::cout << "Standard deserialization failed, trying HEX parsing" << std::endl;
    }
    
    size_t pos = input_data.find("input_files=");
    if (pos != std::string::npos) {
        size_t start = pos + 12;
        size_t end = input_data.find(";", start);
        if (end == std::string::npos) {
            end = input_data.size();
        }
        
        std::string files_str = input_data.substr(start, end - start);
        
        std::istringstream iss(files_str);
        std::string hex_file;
        
        while (std::getline(iss, hex_file, ':')) {
            if (hex_file.empty()) continue;
            
            std::string filename;
            for (size_t i = 0; i < hex_file.length(); i += 2) {
                if (i + 1 < hex_file.length()) {
                    std::string byte_str = hex_file.substr(i, 2);
                    char byte = static_cast<char>(std::stoi(byte_str, nullptr, 16));
                    filename += byte;
                }
            }
            
            if (!filename.empty() && filename.find(".sst") != std::string::npos) {
                info.input_files.push_back(filename);
                info.total_size += 50 * 1024 * 1024;
            }
        }
    }
    
    info.output_level = 0;
    
    if (config_.enable_detailed_logging) {
        std::cout << "Parsed " << info.input_files.size() << " files:" << std::endl;
        for (const auto& f : info.input_files) {
            std::cout << "  - " << f << std::endl;
        }
        std::cout << "Estimated size: " << FormatBytes(info.total_size) << std::endl;
    }
    
    return info;
}

// =======================================================================
// CreateOutputData - 다중 파일 출력 지원
// =======================================================================
//
// 변경 사항:
//   기존: response.output_sst_files(0) 하나만 사용, entries/size를 N으로 나눠 추정
//   수정: response.output_file_metadata()가 있으면 파일별 정확한 메타데이터 사용
//         없으면 output_sst_files 순회 + 추정치 사용 (하위 호환)
//
// 핵심: result.output_files에 여러 CompactionServiceOutputFile을 push_back하면
//       compaction_service_job.cc가 각각에 대해 ReconstructRemoteFileMetadata()를
//       호출하여 Internal Key를 파일에서 직접 읽어옴.
//
std::string CephRemoteCompactionService::CreateOutputData(
    const worker::CompactionResponse& response,
    int output_level) {
    
    // 출력 파일 개수 확인
    if (response.output_sst_files_size() == 0) {
        std::cerr << "✗ No output files in response" << std::endl;
        return "";
    }
    
    // 첫 번째 파일에서 DB 경로 추출
    std::string first_file = response.output_sst_files(0);
    std::string db_path = first_file;
    size_t last_slash = first_file.find_last_of('/');
    if (last_slash != std::string::npos) {
        db_path = first_file.substr(0, last_slash);
    }
    
    if (config_.enable_detailed_logging) {
        std::cout << "\n=== CreateOutputData ===" << std::endl;
        std::cout << "Output files: " << response.output_sst_files_size() << std::endl;
        std::cout << "File metadata entries: " << response.output_file_metadata_size() << std::endl;
        std::cout << "DB path: " << db_path << std::endl;
        std::cout << "Output level: " << output_level << std::endl;
    }
    
    // CompactionServiceResult 생성
    CompactionServiceResult result;
    result.status = Status::OK();
    result.output_level = output_level;
    result.output_path = db_path;
    
    // 통계 정보
    result.bytes_read = response.input_size_bytes();
    result.bytes_written = response.output_size_bytes();
    result.stats.num_input_records = response.num_input_entries();
    result.stats.num_output_records = response.num_entries();
    result.stats.num_output_files = response.output_sst_files_size();
    
    result.output_files.clear();
    
    // ★ 방법 1: output_file_metadata가 있는 경우 (새 Worker 버전)
    //    파일별 정확한 num_entries, file_size 사용
    if (response.output_file_metadata_size() > 0) {
        if (config_.enable_detailed_logging) {
            std::cout << "Using per-file metadata from Worker" << std::endl;
        }
        
        for (int i = 0; i < response.output_file_metadata_size(); i++) {
            const auto& fmeta = response.output_file_metadata(i);
            
            // 파일명 추출 (전체 경로에서)
            std::string file_path = fmeta.file_path();
            std::string filename = file_path;
            size_t slash = file_path.rfind('/');
            if (slash != std::string::npos) {
                filename = file_path.substr(slash + 1);
            }
            
            // CompactionServiceOutputFile 생성
            CompactionServiceOutputFile output_file;
            output_file.file_name = filename;
            output_file.smallest_seqno = 0;
            output_file.largest_seqno = kMaxSequenceNumber;
            output_file.smallest_internal_key = "";  // 비워둠 → ReconstructRemoteFileMetadata
            output_file.largest_internal_key = "";   // 비워둠 → ReconstructRemoteFileMetadata
            output_file.marked_for_compaction = false;
            output_file.oldest_ancester_time = 0;
            output_file.file_creation_time = 0;
            output_file.epoch_number = 0;
            output_file.file_checksum = "";
            output_file.file_checksum_func_name = "";
            output_file.unique_id = {};
            output_file.paranoid_hash = 0;
            
            // ★ 파일별 정확한 메타데이터 설정
            output_file.table_properties.num_entries = fmeta.num_entries();
            output_file.table_properties.data_size = fmeta.file_size();
            output_file.table_properties.raw_key_size = 0;
            output_file.table_properties.raw_value_size = 0;
            output_file.table_properties.index_size = 0;
            output_file.table_properties.filter_size = 0;
            
            result.output_files.push_back(output_file);
            
            if (config_.enable_detailed_logging) {
                std::cout << "  File " << i << ": " << filename
                          << " (" << fmeta.num_entries() << " keys, "
                          << FormatBytes(fmeta.file_size()) << ")" << std::endl;
            }
        }
    }
    // ★ 방법 2: output_file_metadata가 없는 경우 (기존 Worker 하위 호환)
    //    output_sst_files 리스트 순회 + 전체 수치를 파일 수로 나눠 추정
    else {
        if (config_.enable_detailed_logging) {
            std::cout << "No per-file metadata, using estimation" << std::endl;
        }
        
        int num_files = response.output_sst_files_size();
        
        for (int i = 0; i < num_files; i++) {
            std::string full_path = response.output_sst_files(i);
            
            // 파일명 추출
            std::string filename = full_path;
            size_t slash = full_path.rfind('/');
            if (slash != std::string::npos) {
                filename = full_path.substr(slash + 1);
            }
            
            CompactionServiceOutputFile output_file;
            output_file.file_name = filename;
            output_file.smallest_seqno = 0;
            output_file.largest_seqno = kMaxSequenceNumber;
            output_file.smallest_internal_key = "";
            output_file.largest_internal_key = "";
            output_file.marked_for_compaction = false;
            output_file.oldest_ancester_time = 0;
            output_file.file_creation_time = 0;
            output_file.epoch_number = 0;
            output_file.file_checksum = "";
            output_file.file_checksum_func_name = "";
            output_file.unique_id = {};
            output_file.paranoid_hash = 0;
            
            // 추정치: 전체를 파일 수로 나눔
            uint64_t est_entries = response.num_entries() / num_files;
            uint64_t est_size = response.output_size_bytes() / num_files;
            
            // 마지막 파일에 나머지 할당
            if (i == num_files - 1) {
                est_entries = response.num_entries() - (est_entries * (num_files - 1));
                est_size = response.output_size_bytes() - (est_size * (num_files - 1));
            }
            
            output_file.table_properties.num_entries = est_entries;
            output_file.table_properties.data_size = est_size;
            output_file.table_properties.raw_key_size = 0;
            output_file.table_properties.raw_value_size = 0;
            output_file.table_properties.index_size = 0;
            output_file.table_properties.filter_size = 0;
            
            result.output_files.push_back(output_file);
            
            if (config_.enable_detailed_logging) {
                std::cout << "  File " << i << ": " << filename
                          << " (~" << est_entries << " keys, "
                          << FormatBytes(est_size) << " estimated)" << std::endl;
            }
        }
    }
    
    if (config_.enable_detailed_logging) {
        std::cout << "✓ Created " << result.output_files.size() << " output file entries" << std::endl;
        std::cout << "  Stats: input=" << result.stats.num_input_records
                  << ", output=" << result.stats.num_output_records << std::endl;
    }
    
    // 직렬화
    std::string serialized;
    Status s = result.Write(&serialized);
    
    if (!s.ok()) {
        std::cerr << "✗ Failed to serialize: " << s.ToString() << std::endl;
        return "";
    }
    
    if (config_.enable_detailed_logging) {
        std::cout << "✓ Serialized: " << serialized.size() << " bytes" << std::endl;
    }
    
    return serialized;
}

std::string CephRemoteCompactionService::GenerateJobId() {
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return "remote_" + std::to_string(timestamp) + "_" + std::to_string(job_counter_++);
}

std::string CephRemoteCompactionService::FormatBytes(uint64_t bytes) const {
    const char* units[] = {"B", "KB", "MB", "GB"};
    double size = static_cast<double>(bytes);
    int unit = 0;

    while (size >= 1024 && unit < 3) {
        size /= 1024;
        unit++;
    }

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << size << " " << units[unit];
    return oss.str();
}

void CephRemoteCompactionService::GetStatistics(std::map<std::string, uint64_t>* stats) const {
    (*stats)["total_compactions"] = total_compactions_.load();
    (*stats)["remote_compactions"] = remote_compactions_.load();
    (*stats)["local_compactions"] = local_compactions_.load();
    (*stats)["failed_compactions"] = failed_compactions_.load();
    (*stats)["total_bytes_processed"] = total_bytes_processed_.load();

    uint64_t total = total_compactions_.load();
    if (total > 0) {
        (*stats)["remote_percentage"] = (remote_compactions_.load() * 100) / total;
    }
}

}  // namespace ROCKSDB_NAMESPACE