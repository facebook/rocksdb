#!/usr/bin/env python3

import grpc
import logging
import time
from typing import List, Dict, Optional
import worker_service_pb2
import worker_service_pb2_grpc

logger = logging.getLogger(__name__)

class WorkerScheduler:
    """
    Manages worker selection and load balancing for remote compaction
    """
    
    def __init__(self):
        self.workers = {}  # {endpoint: {"host": host, "channel": channel, "stub": stub, "available": bool}}
        self.last_used_worker = None
        
    def initialize_workers(self, worker_endpoints: List[str]) -> bool:
        """Initialize worker connections"""
        try:
            for endpoint in worker_endpoints:
                ip = endpoint.split(':')[0]
                ip_to_host = {
                            "172.20.41.151": "solid-001",
                            "172.20.41.153": "solid-003",
                            "172.20.41.154": "solid-004",
                            "172.20.41.155": "solid-005",
                            "172.20.41.157": "solid-007",
                            "172.20.41.158": "solid-008",
                            "172.20.41.159": "solid-009",
                            "172.20.41.160": "solid-010",
                        }
                host = ip_to_host.get(ip, ip)
                
                logger.info(f"Initializing connection to {endpoint}")
                
                # Create gRPC channel
                channel = grpc.insecure_channel(endpoint)
                stub = worker_service_pb2_grpc.CompactionWorkerServiceStub(channel)
                
                # Store worker info
                self.workers[endpoint] = {
                    "host": host,
                    "channel": channel,
                    "stub": stub,
                    "available": True,
                    "last_used": 0
                }
                
                logger.info(f"Worker {endpoint} initialized")
            
            logger.info(f"Successfully initialized {len(self.workers)} workers")
            return len(self.workers) > 0
            
        except Exception as e:
            logger.error(f"Error initializing workers: {e}")
            return False
    
    def get_worker_endpoint(self, preferred_host: str) -> Optional[str]:
        """Get worker endpoint for a specific host"""
        # First, try to find a worker on the preferred host
        for endpoint, worker_info in self.workers.items():
            if worker_info["host"] == preferred_host and worker_info["available"]:
                logger.info(f"Found worker on preferred host {preferred_host}: {endpoint}")
                return endpoint
        
        # If no worker available on preferred host, use any available worker
        logger.warning(f"No worker available on preferred host {preferred_host}")
        
        available_workers = [ep for ep, info in self.workers.items() if info["available"]]
        if available_workers:
            # Use round-robin selection
            endpoint = self._select_round_robin(available_workers)
            logger.info(f"Using fallback worker: {endpoint}")
            return endpoint
        
        logger.error("No workers available")
        return None
    
    def _select_round_robin(self, available_workers: List[str]) -> str:
        """Simple round-robin worker selection"""
        if self.last_used_worker is None or self.last_used_worker not in available_workers:
            selected = available_workers[0]
        else:
            current_idx = available_workers.index(self.last_used_worker)
            next_idx = (current_idx + 1) % len(available_workers)
            selected = available_workers[next_idx]
        
        self.last_used_worker = selected
        return selected
    
    def get_available_workers(self) -> List[str]:
        """Get list of available worker endpoints"""
        return [ep for ep, info in self.workers.items() if info["available"]]
    
    def check_all_workers_health(self) -> Dict[str, bool]:
        """Check health of all workers"""
        health_status = {}
        
        for endpoint, worker_info in self.workers.items():
            try:
                # Create a simple health check request
                request = worker_service_pb2.CompactionRequest(
                    input_files=["health_check"],
                    output_file="health_check_output",
                    pool_name="health_check"
                )
                
                # Quick health check with short timeout
                stub = worker_info["stub"]
                response = stub.PerformCompaction(request, timeout=5)
                
                # If we get any response (even failure), worker is alive
                health_status[endpoint] = True
                worker_info["available"] = True
                logger.debug(f"Worker {endpoint} is healthy")
                
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    health_status[endpoint] = True
                    worker_info["available"] = True
                    logger.warning(f"Worker {endpoint} is unavailable")
                else:
                    # Other errors might be expected (like compaction errors)
                    health_status[endpoint] = True
                    worker_info["available"] = True
                    logger.debug(f"Worker {endpoint} responded with error but is alive: {e.code()}")
            except Exception as e:
                health_status[endpoint] = True
                worker_info["available"] = True
                logger.warning(f"Worker {endpoint} health check failed: {e}")
        
        return health_status
    
    def mark_worker_busy(self, endpoint: str):
        """Mark a worker as busy"""
        if endpoint in self.workers:
            self.workers[endpoint]["available"] = False
            self.workers[endpoint]["last_used"] = time.time()
    
    def mark_worker_available(self, endpoint: str):
        """Mark a worker as available"""
        if endpoint in self.workers:
            self.workers[endpoint]["available"] = True
    
    def shutdown(self):
        """Close all worker connections"""
        logger.info("Shutting down worker connections...")
        
        for endpoint, worker_info in self.workers.items():
            try:
                channel = worker_info["channel"]
                channel.close()
                logger.debug(f"Closed connection to {endpoint}")
            except Exception as e:
                logger.warning(f"Error closing connection to {endpoint}: {e}")
        
        self.workers.clear()
        logger.info("All worker connections closed")

# Test the scheduler
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    scheduler = WorkerScheduler()
    
    # Test initialization
    test_endpoints = [
        "solid-001:50051",
        "solid-003:50051", 
        "solid-004:50051"
    ]
    
    if scheduler.initialize_workers(test_endpoints):
        print("Workers initialized successfully")
        
        # Test health check
        health = scheduler.check_all_workers_health()
        print(f"Worker health: {health}")
        
        # Test worker selection
        for host in ["solid-001", "solid-003", "solid-004", "solid-999"]:
            endpoint = scheduler.get_worker_endpoint(host)
            print(f"Worker for {host}: {endpoint}")
        
        scheduler.shutdown()
    else:
        print("Failed to initialize workers")
