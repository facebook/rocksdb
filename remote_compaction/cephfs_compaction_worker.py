#!/usr/bin/env python3
"""
CephFS Compaction Worker - Multi-File Output 지원 버전
======================================================
변경 사항 (기존 대비):
  1. _parse_compaction_stats(): 새 메타데이터 형식 파싱 추가
     - COMPACTION_METADATA_START + FILE_METADATA_START (다중 파일)
     - METADATA_START (단일 파일, 하위 호환)
  2. PerformCompaction(): 다중 출력 파일 처리
     - output_sst_files에 여러 파일 경로 포함
     - output_file_metadata에 파일별 메타데이터 포함
  3. _discover_output_files(): 분할된 출력 파일 자동 탐색
  4. Worker 응답에 전체 경로 반환 (기존 파일명만 반환하던 문제 수정)
"""

import grpc
import subprocess
import os
import time
import glob
import shutil
from concurrent import futures
import logging
import socket
import worker_service_pb2_grpc
import worker_service_pb2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CephFSCompactionWorkerServicer(worker_service_pb2_grpc.CompactionWorkerServiceServicer):
    def __init__(self):
        self.node_name = socket.gethostname()
        self.active_jobs = 0
        self.total_completed = 0
        
        # CephFS 마운트 포인트
        self.cephfs_mount = "/mnt/cephfs"
        self.sst_directory = os.path.join(self.cephfs_mount, "rocksdb_test/auto_test_db")
        
        # 컴팩션 도구 경로
        self.compaction_tool_path = "./compaction_tool"
        
        # 작업 디렉토리
        self.work_dir = "/tmp/compaction_work"
        os.makedirs(self.work_dir, exist_ok=True)
        
        # CephFS 마운트 상태 확인
        self._check_cephfs_mount()
        
        logger.info(f"CephFSCompactionWorkerServicer initialized on {self.node_name}")
        logger.info(f"CephFS mount: {self.cephfs_mount}")
        logger.info(f"SST directory: {self.sst_directory}")
        logger.info(f"Compaction tool: {self.compaction_tool_path}")
        logger.info(f"Multi-file output: ENABLED")
    
    def _check_cephfs_mount(self):
        """CephFS 마운트 상태 확인"""
        if not os.path.ismount(self.cephfs_mount):
            logger.warning(f"CephFS not mounted at {self.cephfs_mount}")
            try:
                subprocess.run([
                    "sudo", "mount", "-t", "ceph",
                    "172.20.41.151:6789,172.20.41.153:6789,172.20.41.154:6789:/",
                    self.cephfs_mount,
                    "-o", "name=admin,secret=<admin-key>"
                ], check=True)
                logger.info(f"Successfully mounted CephFS at {self.cephfs_mount}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to mount CephFS: {e}")
        else:
            logger.info(f"CephFS already mounted at {self.cephfs_mount}")
    
    # ==================================================================
    # PerformCompaction - 다중 파일 출력 지원
    # ==================================================================
    def PerformCompaction(self, request, context):
        start_time = time.time()
        self.active_jobs += 1
        job_id = request.job_id
        
        try:
            logger.info(f"=== COMPACTION REQUEST ON {self.node_name} ===")
            logger.info(f"Job ID: {job_id}")
            logger.info(f"Input SST files: {list(request.input_sst_files)}")
            
            # ---- 입력 파일 검증 ----
            input_file_paths = []
            total_input_size = 0
            
            if not request.input_sst_files:
                raise Exception("No input SST files specified")
            
            for sst_file in request.input_sst_files:
                file_path = os.path.join(self.sst_directory, sst_file)
                if not os.path.exists(file_path):
                    raise Exception(f"Input file not found: {sst_file} (path: {file_path})")
                
                file_size = os.path.getsize(file_path)
                input_file_paths.append(file_path)
                total_input_size += file_size
                logger.info(f"  Input: {sst_file} ({file_size:,} bytes)")
            
            logger.info(f"Total input: {len(input_file_paths)} files, {total_input_size:,} bytes")
            
            # ---- 출력 base path 설정 ----
            # compaction_tool이 다중 파일 생성 시:
            #   compacted_{job_id}.sst       (단일 파일)
            #   compacted_{job_id}_0.sst     (다중 파일 첫 번째)
            #   compacted_{job_id}_1.sst     (다중 파일 두 번째)
            #   ...
            output_base = os.path.join(self.sst_directory, f"compacted_{job_id}.sst")
            
            logger.info(f"Output base: {output_base}")
            
            # ---- 컴팩션 실행 ----
            compaction_success, compaction_stats = self._execute_compaction_tool(
                input_file_paths, output_base
            )
            
            if not compaction_success:
                raise Exception(f"Compaction failed: {compaction_stats}")
            
            # ---- 출력 파일 수집 ----
            # compaction_tool의 메타데이터 출력에서 파일 목록을 가져오거나,
            # 파일시스템에서 직접 탐색
            output_file_paths = []
            output_file_metadata_list = []
            total_output_size = 0
            total_output_entries = 0
            
            parsed_files = compaction_stats.get('output_files', [])
            
            if parsed_files:
                # ★ 새 형식: compaction_tool이 파일별 메타데이터를 제공
                logger.info(f"Using parsed metadata for {len(parsed_files)} output files")
                
                for fmeta in parsed_files:
                    fpath = fmeta.get('file_path', '')
                    
                    # 경로 검증: 절대경로가 아니면 sst_directory 기준으로 변환
                    if fpath and not os.path.isabs(fpath):
                        fpath = os.path.join(self.sst_directory, fpath)
                    
                    # 파일 존재 확인
                    if not os.path.exists(fpath):
                        # sst_directory 하위에서 파일명으로 재탐색
                        basename = os.path.basename(fpath)
                        alt_path = os.path.join(self.sst_directory, basename)
                        if os.path.exists(alt_path):
                            fpath = alt_path
                        else:
                            logger.warning(f"  Output file not found: {fpath}")
                            continue
                    
                    fsize = os.path.getsize(fpath)
                    fentries = int(fmeta.get('num_entries', 0))
                    
                    output_file_paths.append(fpath)
                    total_output_size += fsize
                    total_output_entries += fentries
                    
                    # OutputFileMetadata proto 메시지 생성
                    output_file_metadata_list.append(
                        worker_service_pb2.OutputFileMetadata(
                            file_path=fpath,
                            smallest_key_hex=fmeta.get('smallest_key_hex', ''),
                            largest_key_hex=fmeta.get('largest_key_hex', ''),
                            num_entries=fentries,
                            file_size=fsize,
                        )
                    )
                    
                    logger.info(f"  Output [{len(output_file_paths)-1}]: "
                               f"{os.path.basename(fpath)} "
                               f"({fentries:,} keys, {fsize:,} bytes)")
            
            else:
                # 기존 형식 또는 파싱 실패: 파일시스템에서 직접 탐색
                logger.info("Using filesystem discovery for output files")
                output_file_paths = self._discover_output_files(output_base, job_id)
                
                for fpath in output_file_paths:
                    fsize = os.path.getsize(fpath)
                    total_output_size += fsize
                    
                    # 메타데이터 없이 파일 경로만 제공
                    # (Client의 ReconstructRemoteFileMetadata가 파일에서 읽음)
                    output_file_metadata_list.append(
                        worker_service_pb2.OutputFileMetadata(
                            file_path=fpath,
                            num_entries=0,  # Client가 파일에서 읽을 것
                            file_size=fsize,
                        )
                    )
                    logger.info(f"  Output: {os.path.basename(fpath)} ({fsize:,} bytes)")
                
                total_output_entries = compaction_stats.get('num_entries', 0)
            
            if not output_file_paths:
                raise Exception("No output files found after compaction")
            
            # ---- 응답 구성 ----
            processing_time = int((time.time() - start_time) * 1000)
            compression_ratio = total_output_size / max(total_input_size, 1)
            
            self.active_jobs -= 1
            self.total_completed += 1
            
            logger.info(f"=== COMPACTION COMPLETED ON {self.node_name} ===")
            logger.info(f"  Job ID: {job_id}")
            logger.info(f"  Output files: {len(output_file_paths)}")
            logger.info(f"  Input: {total_input_size:,} bytes")
            logger.info(f"  Output: {total_output_size:,} bytes")
            logger.info(f"  Entries: {total_output_entries:,}")
            logger.info(f"  Compression: {compression_ratio:.3f}")
            logger.info(f"  Time: {processing_time}ms")
            
            return worker_service_pb2.CompactionResponse(
                success=True,
                job_id=job_id,
                # ★ 전체 경로 리스트 (기존: 파일명만 1개)
                output_sst_files=output_file_paths,
                input_size_bytes=total_input_size,
                output_size_bytes=total_output_size,
                compression_ratio=compression_ratio,
                processing_time_ms=processing_time,
                # 하위 호환 필드 (단일 파일용, 기존 Client가 읽는 경우)
                num_entries=total_output_entries,
                num_input_entries=compaction_stats.get('num_input_entries', 0),
                # ★ 새 필드: 파일별 메타데이터
                output_file_metadata=output_file_metadata_list,
            )
            
        except Exception as e:
            logger.error(f"COMPACTION ERROR ON {self.node_name}: {e}", exc_info=True)
            self.active_jobs -= 1
            return worker_service_pb2.CompactionResponse(
                success=False,
                job_id=job_id,
                error_message=str(e)
            )
    
    # ==================================================================
    # 출력 파일 탐색 (메타데이터 파싱 실패 시 fallback)
    # ==================================================================
    def _discover_output_files(self, output_base, job_id):
        """
        compaction_tool이 생성한 출력 파일들을 파일시스템에서 탐색.
        
        파일명 패턴:
          단일: compacted_{job_id}.sst
          다중: compacted_{job_id}_0.sst, compacted_{job_id}_1.sst, ...
        """
        found_files = []
        
        # 1. 단일 파일 확인
        if os.path.exists(output_base):
            found_files.append(output_base)
            return found_files
        
        # 2. 다중 파일 확인 (compacted_{job_id}_*.sst 패턴)
        stem = output_base.replace('.sst', '')
        pattern = f"{stem}_*.sst"
        split_files = sorted(glob.glob(pattern))
        
        if split_files:
            found_files.extend(split_files)
            logger.info(f"Discovered {len(split_files)} split output files")
            return found_files
        
        # 3. 더 넓은 패턴으로 탐색
        pattern2 = os.path.join(self.sst_directory, f"compacted_{job_id}*.sst")
        alt_files = sorted(glob.glob(pattern2))
        if alt_files:
            found_files.extend(alt_files)
            logger.info(f"Discovered {len(alt_files)} output files via broad pattern")
        
        return found_files
    
    # ==================================================================
    # compaction_tool 실행
    # ==================================================================
    def _execute_compaction_tool(self, input_files, output_file):
        """C++ 컴팩션 도구 실행"""
        try:
            input_files_str = ",".join(input_files)
            
            cmd = [
                self.compaction_tool_path,
                "--input", input_files_str,
                "--output", output_file,
                # target_file_size는 compaction_tool 기본값(64MB) 사용
                # 명시적으로 지정하려면:
                # "--target_file_size", "67108864",
            ]
            
            logger.info(f"Executing compaction tool:")
            logger.info(f"  Command: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,  # 다중 파일 생성 시 시간이 더 걸릴 수 있음
                cwd=os.path.dirname(self.compaction_tool_path) or "."
            )
            
            if result.stdout:
                # 전체 출력은 길 수 있으므로 마지막 부분만 로그
                stdout_lines = result.stdout.strip().split('\n')
                if len(stdout_lines) > 30:
                    logger.info(f"Compaction output (last 30 lines):")
                    for line in stdout_lines[-30:]:
                        logger.info(f"  {line}")
                else:
                    logger.info(f"Compaction output:")
                    for line in stdout_lines:
                        logger.info(f"  {line}")
            
            if result.stderr:
                logger.warning(f"Compaction stderr: {result.stderr[-500:]}")
            
            if result.returncode == 0:
                stats = self._parse_compaction_stats(result.stdout)
                logger.info(f"Compaction successful: "
                           f"{len(stats.get('output_files', []))} output files, "
                           f"{stats.get('num_entries', 0)} total entries")
                return True, stats
            else:
                return False, f"Exit code: {result.returncode}"
                
        except subprocess.TimeoutExpired:
            return False, "Compaction tool timed out (600s)"
        except Exception as e:
            return False, f"Compaction execution error: {e}"
    
    # ==================================================================
    # compaction_tool stdout 메타데이터 파싱
    # ==================================================================
    def _parse_compaction_stats(self, output):
        """
        compaction_tool stdout 파싱.
        
        새 형식 (다중 파일):
            COMPACTION_METADATA_START
            num_output_files=3
            num_input_entries=500000
            total_output_entries=480000
            FILE_METADATA_START
            file_index=0
            file_path=/path/to/compacted_job_0.sst
            smallest_key_hex=...
            largest_key_hex=...
            num_entries=160000
            file_size=67108864
            FILE_METADATA_END
            FILE_METADATA_START
            ...
            FILE_METADATA_END
            COMPACTION_METADATA_END
        
        기존 형식 (단일 파일, 하위 호환):
            METADATA_START
            smallest_key_hex=...
            largest_key_hex=...
            num_entries=480000
            num_input_entries=500000
            file_size=134000000
            METADATA_END
        """
        stats = {
            'compression_ratio': 0.9,
            'num_entries': 0,
            'num_input_entries': 0,
            'output_files': [],      # 파일별 메타데이터 리스트
        }
        
        try:
            lines = output.split('\n')
            i = 0
            
            while i < len(lines):
                line = lines[i].strip()
                
                # ======================================================
                # 새 형식: COMPACTION_METADATA_START (다중 파일)
                # ======================================================
                if line == 'COMPACTION_METADATA_START':
                    i += 1
                    while i < len(lines):
                        line = lines[i].strip()
                        
                        if line == 'COMPACTION_METADATA_END':
                            break
                        
                        # 개별 파일 메타데이터 블록
                        if line == 'FILE_METADATA_START':
                            file_meta = {}
                            i += 1
                            while i < len(lines):
                                fline = lines[i].strip()
                                if fline == 'FILE_METADATA_END':
                                    break
                                if '=' in fline:
                                    key, value = fline.split('=', 1)
                                    file_meta[key.strip()] = value.strip()
                                i += 1
                            
                            if file_meta:
                                stats['output_files'].append(file_meta)
                        
                        # 전체 통계 필드
                        elif '=' in line:
                            key, value = line.split('=', 1)
                            key, value = key.strip(), value.strip()
                            if key == 'num_input_entries':
                                stats['num_input_entries'] = int(value)
                            elif key == 'total_output_entries':
                                stats['num_entries'] = int(value)
                            elif key == 'total_output_size':
                                stats['total_output_size'] = int(value)
                            elif key == 'num_output_files':
                                stats['num_output_files'] = int(value)
                        
                        i += 1
                
                # ======================================================
                # 기존 형식: METADATA_START (단일 파일, 하위 호환)
                # ======================================================
                elif line == 'METADATA_START':
                    i += 1
                    while i < len(lines):
                        line = lines[i].strip()
                        if line == 'METADATA_END':
                            break
                        if '=' in line:
                            key, value = line.split('=', 1)
                            key, value = key.strip(), value.strip()
                            if key == 'num_entries':
                                stats['num_entries'] = int(value)
                            elif key == 'num_input_entries':
                                stats['num_input_entries'] = int(value)
                            elif key == 'smallest_key_hex':
                                stats['smallest_key_hex'] = value
                            elif key == 'largest_key_hex':
                                stats['largest_key_hex'] = value
                            elif key == 'file_size':
                                stats['file_size'] = int(value)
                        i += 1
                
                # ======================================================
                # 일반 텍스트 통계 (Compression ratio 등)
                # ======================================================
                elif 'Compression ratio:' in line:
                    try:
                        stats['compression_ratio'] = float(
                            line.split('Compression ratio:')[1].strip())
                    except (IndexError, ValueError):
                        pass
                elif 'Input size:' in line:
                    try:
                        stats['input_size'] = int(
                            line.split(':')[1].strip().split()[0])
                    except (IndexError, ValueError):
                        pass
                elif 'Output size:' in line:
                    try:
                        stats['output_size'] = int(
                            line.split(':')[1].strip().split()[0])
                    except (IndexError, ValueError):
                        pass
                
                i += 1
        
        except Exception as e:
            logger.warning(f"Failed to parse compaction stats: {e}", exc_info=True)
        
        logger.info(f"Parsed stats: {len(stats['output_files'])} output files, "
                    f"{stats['num_entries']} entries, "
                    f"{stats['num_input_entries']} input entries")
        
        return stats
    
    # ==================================================================
    # Worker 상태 조회
    # ==================================================================
    def GetWorkerStatus(self, request, context):
        cephfs_healthy = (os.path.ismount(self.cephfs_mount) and 
                         os.path.exists(self.sst_directory))
        
        return worker_service_pb2.StatusResponse(
            node_name=self.node_name,
            is_healthy=cephfs_healthy,
            active_jobs=self.active_jobs,
            total_completed=self.total_completed,
            avg_processing_time_ms=2000
        )


# ==================================================================
# 서버 시작
# ==================================================================
def serve(port=50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    worker_service_pb2_grpc.add_CompactionWorkerServiceServicer_to_server(
        CephFSCompactionWorkerServicer(), server
    )
    server.add_insecure_port(f'[::]:{port}')
    
    logger.info(f"=== CEPHFS COMPACTION WORKER STARTING ===")
    logger.info(f"Node: {socket.gethostname()}")
    logger.info(f"Port: {port}")
    logger.info(f"Multi-file output: ENABLED")
    logger.info(f"Direct CephFS file access enabled")
    
    server.start()
    logger.info("=== READY FOR CEPHFS COMPACTION REQUESTS ===")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
