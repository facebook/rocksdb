import json
import os
import rados
import logging
import hashlib
from typing import Dict, List, Set, Tuple

logger = logging.getLogger(__name__)

# ============================================================
# 전체 Worker 호스트 목록 (8개 OSD 노드)
# ============================================================
WORKER_HOSTS = [
    'solid-001', 'solid-003', 'solid-004', 'solid-005',
    'solid-007', 'solid-008', 'solid-009', 'solid-010'
]

# ============================================================
# Fallback OSD 매핑 (실제 ceph osd tree 기준, 2026-03-18)
# ============================================================
FALLBACK_OSD_MAPPING = {
    'solid-001': [0, 1, 12, 13],
    'solid-003': [4, 5, 16, 17],
    'solid-004': [2, 20, 21, 22],
    'solid-005': [3, 10, 11, 14],
    'solid-007': [6, 7, 8, 9],
    'solid-008': [30, 31, 32, 33],
    'solid-009': [36, 37, 38, 39],
    'solid-010': [42, 43, 44, 45],
}

# CephFS 기본 object 크기 (4MB)
CEPHFS_OBJECT_SIZE = 4 * 1024 * 1024


class LocalityAnalyzer:
    def __init__(self, worker_hosts: List[str] = None, sst_directory: str = None):
        self.cluster = None
        self.osd_to_host_map = {}   # {osd_id: hostname}
        self.host_to_osds_map = {}  # {hostname: [osd_ids]}
        self.connected = False
        self.worker_hosts = worker_hosts or WORKER_HOSTS
        self.sst_directory = sst_directory or "/mnt/cephfs/rocksdb_test/auto_test_db"

        # Pool 정보 캐시 (반복 조회 방지)
        self._pool_cache = {}  # {pool_name: (pool_id, pg_num)}
        
    def connect(self):
        """Connect to Ceph cluster"""
        try:
            self.cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
            self.cluster.connect()
            self.connected = True
            logger.info("Connected to Ceph cluster successfully")
            self._build_osd_host_mapping_real()
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Ceph cluster: {e}")
            self.connected = False
            return False
    
    def _build_osd_host_mapping_real(self):
        """Build real OSD-Host mapping from CRUSH buckets"""
        try:
            cmd = '{"prefix": "osd crush dump", "format": "json"}'
            ret, buf, errs = self.cluster.mon_command(cmd, b'')
            
            if ret != 0:
                logger.error(f"Failed to get CRUSH dump: {errs}")
                self._build_osd_host_mapping_fallback()
                return
            
            crush_info = json.loads(buf)
            logger.info(f"CRUSH dump keys: {list(crush_info.keys())}")
            
            self.osd_to_host_map.clear()
            self.host_to_osds_map.clear()
            
            buckets = crush_info.get('buckets', [])
            logger.info(f"Found {len(buckets)} buckets in CRUSH map")
            
            processed_hosts = set()
            host_buckets = []
            for bucket in buckets:
                if bucket.get('type_name') == 'host':
                    bucket_name = bucket['name']
                    base_host = bucket_name.replace('~ssd', '')
                    if base_host in processed_hosts:
                        continue
                    processed_hosts.add(base_host)
                    host_buckets.append((base_host, bucket))
            
            logger.info(f"Found {len(host_buckets)} unique hosts after deduplication")
            
            total_osds = 0
            for base_host, bucket in host_buckets:
                host_osds = []
                for item in bucket.get('items', []):
                    item_id = item['id']
                    if item_id >= 0:
                        self.osd_to_host_map[item_id] = base_host
                        host_osds.append(item_id)
                        total_osds += 1
                if host_osds:
                    self.host_to_osds_map[base_host] = sorted(host_osds)
                    logger.info(f"Host {base_host}: {len(host_osds)} OSDs {sorted(host_osds)}")
            
            logger.info(f"OSD-Host mapping completed: {total_osds} OSDs mapped to {len(self.host_to_osds_map)} hosts")
            
            logger.info("=== Final OSD-Host Mapping ===")
            for host in sorted(self.host_to_osds_map.keys()):
                osds = self.host_to_osds_map[host]
                logger.info(f"  {host}: {len(osds)} OSDs {osds}")
            
            available_workers = []
            for worker in self.worker_hosts:
                if worker in self.host_to_osds_map:
                    available_workers.append(worker)
                    logger.info(f"Worker {worker}: {len(self.host_to_osds_map[worker])} OSDs")
                else:
                    logger.warning(f"Worker {worker}: NOT found in CRUSH map")
            
            logger.info(f"Available workers: {available_workers}")
            
            if total_osds == 0:
                logger.warning("No OSDs found in CRUSH map, falling back to simulation")
                self._build_osd_host_mapping_fallback()
                
        except Exception as e:
            logger.error(f"Error parsing CRUSH buckets: {e}")
            logger.info("Falling back to simulation mapping")
            self._build_osd_host_mapping_fallback()
    
    def _build_osd_host_mapping_fallback(self):
        """Fallback simulation mapping based on actual ceph osd tree"""
        logger.info("Building fallback OSD-Host mapping based on actual cluster structure")
        
        for host, osds in FALLBACK_OSD_MAPPING.items():
            self.host_to_osds_map[host] = osds
            for osd_id in osds:
                self.osd_to_host_map[osd_id] = host
            
            worker_status = "WORKER" if host in self.worker_hosts else "NO WORKER"
            logger.info(f"{host}: {len(osds)} OSDs {osds} {worker_status}")
        
        available = [h for h in self.worker_hosts if h in self.host_to_osds_map]
        logger.info(f"Fallback mapping: {len(self.osd_to_host_map)} OSDs across {len(self.host_to_osds_map)} hosts")
        logger.info(f"Available workers: {available}")

    # ================================================================
    # Pool 정보 조회 (캐시 사용)
    # ================================================================
    def _get_pool_info(self, pool_name: str) -> Tuple[int, int]:
        """Get pool_id and pg_num, cached"""
        if pool_name in self._pool_cache:
            return self._pool_cache[pool_name]

        osd_dump_cmd = '{"prefix": "osd dump", "format": "json"}'
        ret, buf, errs = self.cluster.mon_command(osd_dump_cmd, b'')
        if ret != 0:
            raise RuntimeError(f"Failed to get OSD dump: {errs}")

        osd_dump = json.loads(buf)
        for pool in osd_dump.get('pools', []):
            if pool['pool_name'] == pool_name:
                info = (pool['pool'], pool['pg_num'])
                self._pool_cache[pool_name] = info
                logger.info(f"Pool {pool_name}: ID={info[0]}, PG_NUM={info[1]}")
                return info

        raise RuntimeError(f"Pool {pool_name} not found")

    # ================================================================
    # PG → OSD 조회
    # ================================================================
    def _get_pg_acting_osds(self, pool_id: int, pg_num: int, object_name: str) -> List[int]:
        """Get acting OSD set for a specific object name"""
        obj_hash = int(hashlib.md5(object_name.encode()).hexdigest()[:8], 16)
        pg_id = obj_hash % pg_num
        full_pg_id = f"{pool_id}.{pg_id:x}"

        pg_map_cmd = f'{{"prefix": "pg map", "pgid": "{full_pg_id}", "format": "json"}}'
        ret, buf, errs = self.cluster.mon_command(pg_map_cmd, b'')

        if ret == 0:
            pg_info = json.loads(buf)
            return pg_info.get('acting', [])
        return []

    # ================================================================
    # SST 파일 → CephFS object 이름 생성
    # ================================================================
    def _get_cephfs_object_names(self, sst_filename: str) -> List[str]:
        """
        SST 파일의 inode와 크기로 CephFS object 이름들을 생성한다.
        
        CephFS object naming: {inode_hex}.{index_hex}
          - inode_hex: 파일 inode의 13자리 hex (zero-padded)
          - index_hex: 8자리 hex index (0x00000000, 0x00000001, ...)
          - object 개수 = ceil(file_size / 4MB)
        
        예: inode=1099512170917, size=35MB
          → 10000084f83.00000000
          → 10000084f83.00000001
          → ...
          → 10000084f83.00000008  (9개 object)
        """
        file_path = os.path.join(self.sst_directory, sst_filename)
        
        try:
            stat_result = os.stat(file_path)
        except FileNotFoundError:
            logger.warning(f"File not found for stat: {file_path}")
            return [sst_filename]  # fallback: 파일명 그대로 사용 (기존 방식)
        
        inode = stat_result.st_ino
        file_size = stat_result.st_size
        
        # object 개수 계산
        num_objects = max(1, (file_size + CEPHFS_OBJECT_SIZE - 1) // CEPHFS_OBJECT_SIZE)
        
        # inode를 13자리 hex로 변환
        inode_hex = f"{inode:013x}"
        
        # object 이름 생성
        object_names = []
        for idx in range(num_objects):
            obj_name = f"{inode_hex}.{idx:08x}"
            object_names.append(obj_name)
        
        logger.debug(f"{sst_filename}: inode={inode} (0x{inode_hex}), "
                     f"size={file_size}, objects={num_objects}")
        
        return object_names

    # ================================================================
    # Object 단위 위치 조회 (핵심 변경)
    # ================================================================
    def get_object_locations(self, pool_name: str, sst_files: List[str]) -> Dict[str, List[List[int]]]:
        """
        각 SST 파일에 대해 모든 CephFS object의 OSD 위치를 조회한다.
        
        반환 형식 변경:
          기존: {sst_filename: [primary_osd1, osd2, osd3]}           (파일당 OSD 1세트)
          수정: {sst_filename: [[osd1,osd2,osd3], [osd4,osd5,osd6], ...]}  (object당 OSD 1세트)
        """
        if not self.connected:
            logger.error("Not connected to Ceph cluster")
            return {}
        
        try:
            pool_id, pg_num = self._get_pool_info(pool_name)
        except RuntimeError as e:
            logger.error(str(e))
            return {}

        object_locations = {}  # {sst_file: [[osds_obj0], [osds_obj1], ...]}
        total_objects_queried = 0
        
        for sst_file in sst_files:
            cephfs_objects = self._get_cephfs_object_names(sst_file)
            
            file_object_osds = []
            for obj_name in cephfs_objects:
                acting_osds = self._get_pg_acting_osds(pool_id, pg_num, obj_name)
                if acting_osds:
                    file_object_osds.append(acting_osds)
                    total_objects_queried += 1
            
            if file_object_osds:
                object_locations[sst_file] = file_object_osds
                
                # 로그: 파일별 요약
                all_osds = [osd for obj_osds in file_object_osds for osd in obj_osds]
                unique_hosts = set(self.osd_to_host_map.get(osd, '?') for osd in all_osds)
                logger.info(f"{sst_file}: {len(cephfs_objects)} objects -> "
                           f"{len(file_object_osds)} mapped, "
                           f"hosts={sorted(unique_hosts)}")
        
        logger.info(f"Object-level locations: {len(object_locations)} files, "
                    f"{total_objects_queried} total objects queried")
        
        return object_locations
    
    # ================================================================
    # Locality 분석 (object 단위)
    # ================================================================
    def analyze_locality(self, pool_name: str, sst_files: List[str]) -> Dict[str, float]:
        """
        Object 단위 locality 분석.
        
        Locality 정의:
          "컴팩션 실행 노드에서 네트워크 없이 로컬 디스크로 직접 접근 가능한
           데이터(object)의 비율"
        
        계산 방식:
          각 object에 대해 acting OSD set(primary + replicas)에 해당 호스트의
          OSD가 포함되어 있으면, 그 object는 해당 호스트에서 로컬 접근 가능.
          
          Locality(host) = (host에서 로컬 접근 가능한 object 수) / (전체 object 수) × 100
        
        예: 5개 SST → 44개 object (9+9+9+9+8)
            solid-010의 OSD(42,43,44,45)가 포함된 object = 27개
            → solid-010 locality = 27/44 = 61.4%
        """
        if not self.osd_to_host_map:
            logger.warning("No OSD-Host mapping available, using fallback")
            return self._analyze_locality_fallback(sst_files)
        
        object_locations = self.get_object_locations(pool_name, sst_files)
        
        if not object_locations:
            logger.warning("No object locations found, using fallback")
            return self._analyze_locality_fallback(sst_files)
        
        # 각 호스트별: "로컬 접근 가능한 object 수" 카운트
        host_local_objects = {}   # {host: count}
        total_objects = 0
        
        for sst_file, file_object_osds in object_locations.items():
            for obj_osds in file_object_osds:
                total_objects += 1
                
                # 이 object의 acting OSD들이 어떤 호스트에 있는지 확인
                # 한 호스트에 replica가 여러 개 있어도 1번만 카운트
                hosts_with_replica = set()
                for osd_id in obj_osds:
                    host = self.osd_to_host_map.get(osd_id)
                    if host:
                        hosts_with_replica.add(host)
                
                # 해당 object에 replica가 있는 호스트들에 +1
                for host in hosts_with_replica:
                    host_local_objects[host] = host_local_objects.get(host, 0) + 1
        
        # Locality 점수 계산
        locality_scores = {}
        if total_objects > 0:
            for host, count in host_local_objects.items():
                locality_scores[host] = (count / total_objects) * 100
        
        logger.info(f"Object-level locality for {len(sst_files)} files "
                    f"({total_objects} objects):")
        for host, score in sorted(locality_scores.items(), key=lambda x: x[1], reverse=True):
            local_count = host_local_objects.get(host, 0)
            logger.info(f"  {host}: {score:.1f}% ({local_count}/{total_objects} objects local)")
        
        return locality_scores
    
    def _analyze_locality_fallback(self, sst_files: List[str]) -> Dict[str, float]:
        """Fallback locality analysis"""
        logger.info("Using fallback locality analysis")
        
        host_scores = {host: 0.0 for host in self.worker_hosts}
        
        for sst_file in sst_files:
            hash_val = int(hashlib.md5(sst_file.encode()).hexdigest(), 16)
            primary_host = self.worker_hosts[hash_val % len(self.worker_hosts)]
            
            host_scores[primary_host] += 60.0
            for host in self.worker_hosts:
                if host != primary_host:
                    host_scores[host] += 20.0 / (len(self.worker_hosts) - 1)
        
        total_files = len(sst_files)
        if total_files > 0:
            for host in host_scores:
                host_scores[host] = host_scores[host] / total_files
        
        logger.info(f"Fallback locality scores: {host_scores}")
        return host_scores
    
    def get_best_worker_host(self, pool_name: str, sst_files: List[str]) -> str:
        """Get the best worker host based on object-level locality"""
        locality_scores = self.analyze_locality(pool_name, sst_files)
        
        worker_scores = {host: score for host, score in locality_scores.items() 
                        if host in self.worker_hosts}
        
        if not worker_scores:
            logger.warning("No locality scores for available workers, using round-robin")
            import random
            return random.choice(self.worker_hosts)
        
        best_host = max(worker_scores.items(), key=lambda x: x[1])
        logger.info(f"Selected best worker: {best_host[0]} (score: {best_host[1]:.1f}%)")
        
        logger.info("Worker locality scores:")
        for host in self.worker_hosts:
            score = worker_scores.get(host, 0.0)
            status = "SELECTED" if host == best_host[0] else "  "
            logger.info(f"  {host}: {score:.1f}% {status}")
        
        return best_host[0]
    
    def disconnect(self):
        """Disconnect from Ceph cluster"""
        if self.cluster:
            self.cluster.shutdown()
            self.connected = False
            logger.info("Disconnected from Ceph cluster")


# Test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    analyzer = LocalityAnalyzer()
    
    if analyzer.connect():
        print(f"Connected: {len(analyzer.osd_to_host_map)} OSDs, {len(analyzer.host_to_osds_map)} hosts")
        print(f"Workers ({len(analyzer.worker_hosts)}): {analyzer.worker_hosts}")
        
        test_files = ["000009.sst", "000011.sst", "000013.sst", "000015.sst"]
        
        # Object-level locality test
        print("\n=== Object-level locality ===")
        best_host = analyzer.get_best_worker_host("cephfs_data", test_files)
        print(f"Best worker: {best_host}")
        
        analyzer.disconnect()
    else:
        print("Failed to connect")
