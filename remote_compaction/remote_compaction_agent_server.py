#!/usr/bin/env python3
"""
Remote Compaction Agent - gRPC Server
RocksDB C++ 코드에서 직접 호출할 수 있는 상시 실행 Agent
"""

import grpc
from concurrent import futures
import logging
import time
import os
from typing import Dict, List, Tuple

# Import locality analyzer and scheduler
from locality_analyzer import LocalityAnalyzer
from scheduler import WorkerScheduler
import worker_service_pb2
import worker_service_pb2_grpc
import agent_service_pb2
import agent_service_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RemoteCompactionAgentServicer(agent_service_pb2_grpc.RemoteCompactionAgentServicer):
    """
    RocksDB와 Ceph Workers 사이의 중개 Agent
    실시간으로 locality 분석 및 worker 선택 제공
    """
    
    def __init__(self, pool_name: str = "cephfs_data", cephfs_path: str = "/mnt/cephfs"):
        self.pool_name = pool_name
        self.cephfs_path = cephfs_path
        self.sst_base_path = os.path.join(cephfs_path, "rocksdb_test/test_compaction_db")
        
        # Initialize components
        self.locality_analyzer = LocalityAnalyzer()
        self.scheduler = WorkerScheduler()
        
        # Statistics
        self.total_jobs = 0
        self.successful_jobs = 0
        self.failed_jobs = 0
        
        # Initialize connections
        self._initialize()
    
    def _initialize(self):
        """Initialize Ceph and Worker connections"""
        try:
            # Connect to Ceph cluster
            if not self.locality_analyzer.connect():
                raise RuntimeError("Failed to connect to Ceph cluster")
            
            logger.info(f"Connected to Ceph cluster successfully")
            logger.info(f"Total OSDs: {len(self.locality_analyzer.osd_to_host_map)}")
            
            # Initialize workers
            worker_endpoints = [
                "172.20.41.151:50051",  # solid-001
                "172.20.41.153:50051",  # solid-003
                "172.20.41.154:50051",   # solid-004
                "172.20.41.155:50051",  # solid-005
                "172.20.41.157:50051",  # solid-007
                "172.20.41.158:50051",  # solid-008
                "172.20.41.159:50051",  # solid-009
                "172.20.41.160:50051",  # solid-010
            ]
            
            if not self.scheduler.initialize_workers(worker_endpoints):
                raise RuntimeError("Failed to initialize worker connections")
            
            logger.info(f"Initialized {len(worker_endpoints)} workers")
            
            # Verify CephFS mount
            if not os.path.exists(self.sst_base_path):
                logger.warning(f"SST base path not found: {self.sst_base_path}")
            
            logger.info("=== Remote Compaction Agent Ready ===")
            logger.info(f"CephFS Path: {self.cephfs_path}")
            logger.info(f"SST Base Path: {self.sst_base_path}")
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise
    
    def SelectWorker(self, request, context):
        """
        Locality 분석을 통해 최적의 Worker 선택
        RocksDB C++ 코드에서 실시간으로 호출됨
        """
        try:
            logger.info(f"=== SelectWorker Request ===")
            logger.info(f"Input files: {list(request.input_sst_files)[:5]}...")
            
            # Perform locality analysis
            best_host = self.locality_analyzer.get_best_worker_host(
                request.pool_name or self.pool_name,
                list(request.input_sst_files)
            )
            
            # Get worker endpoint
            worker_endpoint = self.scheduler.get_worker_endpoint(best_host)
            
            if not worker_endpoint:
                return agent_service_pb2.WorkerSelectionResponse(
                    success=False,
                    error_message=f"No worker available for host {best_host}"
                )
            
            # Get detailed locality information
            locality_info = self.locality_analyzer.analyze_locality(
                request.pool_name or self.pool_name,
                list(request.input_sst_files)
            )
            
            logger.info(f"Selected worker: {best_host} ({worker_endpoint})")
            logger.info(f"Locality score: {locality_info.get('best_locality', 0):.1f}%")
            
            return agent_service_pb2.WorkerSelectionResponse(
                success=True,
                selected_worker_host=best_host,
                worker_endpoint=worker_endpoint,
                locality_score=locality_info.get('best_locality', 0),
                host_locality_map=locality_info.get('host_locality', {})
            )
            
        except Exception as e:
            logger.error(f"SelectWorker error: {e}")
            return agent_service_pb2.WorkerSelectionResponse(
                success=False,
                error_message=str(e)
            )
    
    def GetAgentStatus(self, request, context):
        """Agent 상태 정보 반환"""
        try:
            return agent_service_pb2.AgentStatusResponse(
                ceph_connected=self.locality_analyzer.connected,
                total_osds=len(self.locality_analyzer.osd_to_host_map),
                available_workers=len(self.scheduler.get_available_workers()),
                total_jobs_processed=self.total_jobs,
                successful_jobs=self.successful_jobs,
                failed_jobs=self.failed_jobs
            )
        except Exception as e:
            logger.error(f"GetAgentStatus error: {e}")
            return agent_service_pb2.AgentStatusResponse(
                ceph_connected=False,
                total_osds=0,
                available_workers=0
            )
    
    def SubmitCompaction(self, request, context):
        """
        통합 컴팩션 작업: Locality 분석 + Worker 선택 + 실행
        RocksDB가 한 번의 호출로 전체 작업 완료 가능
        """
        self.total_jobs += 1
        start_time = time.time()
        
        try:
            logger.info(f"=== SubmitCompaction Request ===")
            logger.info(f"Job ID: {request.job_id}")
            logger.info(f"Input files: {len(request.input_sst_files)}")
            
            # Step 1: Select best worker
            best_host = self.locality_analyzer.get_best_worker_host(
                self.pool_name,
                list(request.input_sst_files)
            )
            
            worker_endpoint = self.scheduler.get_worker_endpoint(best_host)
            if not worker_endpoint:
                raise RuntimeError(f"No worker available for {best_host}")
            
            logger.info(f"Selected worker: {best_host} ({worker_endpoint})")
            
            # Step 2: Create gRPC connection to worker
            channel = grpc.insecure_channel(worker_endpoint)
            stub = worker_service_pb2_grpc.CompactionWorkerServiceStub(channel)
            
            # Step 3: Build compaction request
            worker_request = worker_service_pb2.CompactionRequest(
                job_id=request.job_id,
                input_sst_files=request.input_sst_files
            )
            
            if request.HasField('options'):
                worker_request.options.CopyFrom(
                    worker_service_pb2.CompactionOptions(
                        compression_type=request.options.compression_type,
                        target_file_size=request.options.target_file_size,
                        enable_statistics=request.options.enable_statistics
                    )
                )
            
            # Step 4: Execute compaction on worker
            logger.info(f"Sending compaction to worker {best_host}...")
            worker_response = stub.PerformCompaction(worker_request, timeout=300)
            
            # Step 5: Process result
            if worker_response.success:
                self.successful_jobs += 1
                processing_time = int((time.time() - start_time) * 1000)
                
                logger.info(f"Compaction SUCCESS:")
                logger.info(f"  Worker: {best_host}")
                logger.info(f"  Input: {worker_response.input_size_bytes:,} bytes")
                logger.info(f"  Output: {worker_response.output_size_bytes:,} bytes")
                logger.info(f"  Time: {worker_response.processing_time_ms}ms")
                
                return agent_service_pb2.CompactionJobResponse(
                    success=True,
                    job_id=worker_response.job_id,
                    selected_worker=best_host,
                    output_files=worker_response.output_sst_files,
                    input_size_bytes=worker_response.input_size_bytes,
                    output_size_bytes=worker_response.output_size_bytes,
                    compression_ratio=worker_response.compression_ratio,
                    processing_time_ms=worker_response.processing_time_ms
                )
            else:
                raise RuntimeError(f"Worker compaction failed: {worker_response.error_message}")
                
        except Exception as e:
            self.failed_jobs += 1
            logger.error(f"SubmitCompaction error: {e}")
            
            return agent_service_pb2.CompactionJobResponse(
                success=False,
                job_id=request.job_id,
                error_message=str(e)
            )
        finally:
            try:
                channel.close()
            except:
                pass

def serve(port=50050):
    """Start Agent gRPC Server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    agent_service_pb2_grpc.add_RemoteCompactionAgentServicer_to_server(
        RemoteCompactionAgentServicer(),
        server
    )
    
    server.add_insecure_port(f'[::]:{port}')
    
    logger.info("=" * 60)
    logger.info("=== REMOTE COMPACTION AGENT STARTING ===")
    logger.info(f"Port: {port}")
    logger.info(f"Ready to receive RocksDB compaction requests")
    logger.info("=" * 60)
    
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve(port=50050)
