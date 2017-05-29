cmake_minimum_required(VERSION 2.6)

set(JNI_NATIVE_SOURCES
        rocksjni/backupablejni.cc
        rocksjni/backupenginejni.cc
        rocksjni/checkpoint.cc
        rocksjni/clock_cache.cc
        rocksjni/columnfamilyhandle.cc
        rocksjni/compaction_filter.cc
        rocksjni/compaction_options_fifo.cc
        rocksjni/compaction_options_universal.cc
        rocksjni/comparator.cc
        rocksjni/comparatorjnicallback.cc
        rocksjni/compression_options.cc
        rocksjni/env.cc
        rocksjni/env_options.cc
        rocksjni/filter.cc
        rocksjni/ingest_external_file_options.cc
        rocksjni/iterator.cc
        rocksjni/loggerjnicallback.cc
        rocksjni/lru_cache.cc
        rocksjni/memtablejni.cc
        rocksjni/merge_operator.cc
        rocksjni/options.cc
        rocksjni/ratelimiterjni.cc
        rocksjni/remove_emptyvalue_compactionfilterjni.cc
        rocksjni/restorejni.cc
        rocksjni/rocksdb_exception_test.cc
        rocksjni/rocksjni.cc
        rocksjni/slice.cc
        rocksjni/snapshot.cc
        rocksjni/sst_file_writerjni.cc
        rocksjni/statistics.cc
        rocksjni/table.cc
        rocksjni/transaction_log.cc
        rocksjni/ttl.cc
        rocksjni/write_batch.cc
        rocksjni/write_batch_test.cc
        rocksjni/write_batch_with_index.cc
        rocksjni/writebatchhandlerjnicallback.cc
)

set(NATIVE_JAVA_CLASSES
        org.rocksdb.AbstractCompactionFilter
        org.rocksdb.AbstractComparator
        org.rocksdb.AbstractImmutableNativeReference
        org.rocksdb.AbstractNativeReference
        org.rocksdb.AbstractRocksIterator
        org.rocksdb.AbstractSlice
        org.rocksdb.AbstractWriteBatch
        org.rocksdb.BackupableDBOptions
        org.rocksdb.BackupEngine
        org.rocksdb.BackupEngineTest
        org.rocksdb.BlockBasedTableConfig
        org.rocksdb.BloomFilter
        org.rocksdb.Cache
        org.rocksdb.Checkpoint
        org.rocksdb.ClockCache
        org.rocksdb.ColumnFamilyHandle
        org.rocksdb.ColumnFamilyOptions
        org.rocksdb.CompactionOptionsFIFO
        org.rocksdb.CompactionOptionsUniversal
        org.rocksdb.Comparator
        org.rocksdb.ComparatorOptions
        org.rocksdb.CompressionOptions
        org.rocksdb.DBOptions
        org.rocksdb.DirectComparator
        org.rocksdb.DirectSlice
        org.rocksdb.Env
        org.rocksdb.EnvOptions
        org.rocksdb.ExternalSstFileInfo
        org.rocksdb.Filter
        org.rocksdb.FlushOptions
        org.rocksdb.HashLinkedListMemTableConfig
        org.rocksdb.HashSkipListMemTableConfig
        org.rocksdb.Logger
        org.rocksdb.LRUCache
        org.rocksdb.MemTableConfig
        org.rocksdb.MergeOperator
        org.rocksdb.NativeLibraryLoader
        org.rocksdb.Options
        org.rocksdb.PlainTableConfig
        org.rocksdb.RateLimiter
        org.rocksdb.ReadOptions
        org.rocksdb.RemoveEmptyValueCompactionFilter
        org.rocksdb.RestoreOptions
        org.rocksdb.RocksDB
        org.rocksdb.RocksDBExceptionTest
        org.rocksdb.RocksEnv
        org.rocksdb.RocksIterator
        org.rocksdb.RocksIteratorInterface
        org.rocksdb.RocksMemEnv
        org.rocksdb.RocksMutableObject
        org.rocksdb.RocksObject
        org.rocksdb.SkipListMemTableConfig
        org.rocksdb.Slice
        org.rocksdb.Snapshot
        org.rocksdb.SnapshotTest
        org.rocksdb.SstFileWriter
        org.rocksdb.Statistics
        org.rocksdb.StringAppendOperator
        org.rocksdb.TableFormatConfig
        org.rocksdb.TransactionLogIterator
        org.rocksdb.TtlDB
        org.rocksdb.VectorMemTableConfig
        org.rocksdb.WBWIRocksIterator
        org.rocksdb.WriteBatch
        org.rocksdb.WriteBatch.Handler
        org.rocksdb.WriteBatchTest
        org.rocksdb.WriteBatchTestInternalHelper
        org.rocksdb.WriteBatchWithIndex
        org.rocksdb.WriteOptions
)

include_directories($ENV{JAVA_HOME}/include)
include_directories($ENV{JAVA_HOME}/include/win32)
include_directories(${PROJECT_SOURCE_DIR}/java)

set(JAVA_TEST_LIBDIR ${PROJECT_SOURCE_DIR}/java/test-libs)
set(JAVA_TMP_JAR ${JAVA_TEST_LIBDIR}/tmp.jar)
set(JAVA_JUNIT_JAR ${JAVA_TEST_LIBDIR}/junit-4.12.jar)
set(JAVA_HAMCR_JAR ${JAVA_TEST_LIBDIR}/hamcrest-core-1.3.jar)
set(JAVA_MOCKITO_JAR ${JAVA_TEST_LIBDIR}/mockito-all-1.10.19.jar)
set(JAVA_CGLIB_JAR ${JAVA_TEST_LIBDIR}/cglib-2.2.2.jar)
set(JAVA_ASSERTJ_JAR ${JAVA_TEST_LIBDIR}/assertj-core-1.7.1.jar)
set(JAVA_TESTCLASSPATH "${JAVA_JUNIT_JAR}\;${JAVA_HAMCR_JAR}\;${JAVA_MOCKITO_JAR}\;${JAVA_CGLIB_JAR}\;${JAVA_ASSERTJ_JAR}")

if(NOT EXISTS ${PROJECT_SOURCE_DIR}/java/classes)
  file(MAKE_DIRECTORY ${PROJECT_SOURCE_DIR}/java/classes)
endif()

if(NOT EXISTS ${JAVA_TEST_LIBDIR})
  file(MAKE_DIRECTORY mkdir ${JAVA_TEST_LIBDIR})
endif()

if (DEFINED CUSTOM_REPO_URL)
  set(SEARCH_REPO_URL ${CUSTOM_REPO_URL}/)
  set(CENTRAL_REPO_URL ${CUSTOM_REPO_URL}/)
else ()
  set(SEARCH_REPO_URL "http://search.maven.org/remotecontent?filepath=")
  set(CENTRAL_REPO_URL "http://central.maven.org/maven2/")
endif()

if(NOT EXISTS ${JAVA_JUNIT_JAR})
  message("Downloading ${JAVA_JUNIT_JAR}")
  file(DOWNLOAD ${SEARCH_REPO_URL}junit/junit/4.12/junit-4.12.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_JUNIT_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_JUNIT_JAR})
endif()
if(NOT EXISTS ${JAVA_HAMCR_JAR})
  message("Downloading ${JAVA_HAMCR_JAR}")
  file(DOWNLOAD ${SEARCH_REPO_URL}org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_HAMCR_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_HAMCR_JAR})
endif()
if(NOT EXISTS ${JAVA_MOCKITO_JAR})
  message("Downloading ${JAVA_MOCKITO_JAR}")
  file(DOWNLOAD ${SEARCH_REPO_URL}org/mockito/mockito-all/1.10.19/mockito-all-1.10.19.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_MOCKITO_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_MOCKITO_JAR})
endif()
if(NOT EXISTS ${JAVA_CGLIB_JAR})
  message("Downloading ${JAVA_CGLIB_JAR}")
  file(DOWNLOAD ${SEARCH_REPO_URL}cglib/cglib/2.2.2/cglib-2.2.2.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_CGLIB_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_CGLIB_JAR})
endif()
if(NOT EXISTS ${JAVA_ASSERTJ_JAR})
  message("Downloading ${JAVA_ASSERTJ_JAR}")
  file(DOWNLOAD ${CENTRAL_REPO_URL}org/assertj/assertj-core/1.7.1/assertj-core-1.7.1.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_ASSERTJ_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_ASSERTJ_JAR})
endif()

if(WIN32)
  set(JAVAC cmd /c javac)
  set(JAVAH cmd /c javah)
else()
  set(JAVAC javac)
  set(JAVAH javah)
endif()

execute_process(COMMAND ${JAVAC} -cp ${JAVA_TESTCLASSPATH} -d ${PROJECT_SOURCE_DIR}/java/classes ${PROJECT_SOURCE_DIR}/java/src/main/java/org/rocksdb/util/*.java ${PROJECT_SOURCE_DIR}/java/src/main/java/org/rocksdb/*.java ${PROJECT_SOURCE_DIR}/java/src/test/java/org/rocksdb/*.java)
execute_process(COMMAND ${JAVAH} -cp ${PROJECT_SOURCE_DIR}/java/classes -d ${PROJECT_SOURCE_DIR}/java/include -jni ${NATIVE_JAVA_CLASSES})
add_library(rocksdbjni${ARTIFACT_SUFFIX} SHARED ${JNI_NATIVE_SOURCES})
set_target_properties(rocksdbjni${ARTIFACT_SUFFIX} PROPERTIES COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/rocksdbjni${ARTIFACT_SUFFIX}.pdb")
target_link_libraries(rocksdbjni${ARTIFACT_SUFFIX} rocksdb${ARTIFACT_SUFFIX} ${LIBS})
