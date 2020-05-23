# Copyright (c) 2011 The LevelDB Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

# Inherit some settings from environment variables, if available

#-----------------------------------------------

BASH_EXISTS := $(shell which bash)
SHELL := $(shell which bash)
PYTHON?=$(shell which python)

CLEAN_FILES = # deliberately empty, so we can append below.
CFLAGS += ${EXTRA_CFLAGS}
CXXFLAGS += ${EXTRA_CXXFLAGS}
LDFLAGS += $(EXTRA_LDFLAGS)
MACHINE ?= $(shell uname -m)
ARFLAGS = ${EXTRA_ARFLAGS} rs
STRIPFLAGS = -S -x

# Transform parallel LOG output into something more readable.
perl_command = perl -n \
  -e '@a=split("\t",$$_,-1); $$t=$$a[8];'				\
  -e '$$t =~ /.*if\s\[\[\s"(.*?\.[\w\/]+)/ and $$t=$$1;'		\
  -e '$$t =~ s,^\./,,;'							\
  -e '$$t =~ s, >.*,,; chomp $$t;'					\
  -e '$$t =~ /.*--gtest_filter=(.*?\.[\w\/]+)/ and $$t=$$1;'		\
  -e 'printf "%7.3f %s %s\n", $$a[3], $$a[6] == 0 ? "PASS" : "FAIL", $$t'
quoted_perl_command = $(subst ','\'',$(perl_command))

# DEBUG_LEVEL can have three values:
# * DEBUG_LEVEL=2; this is the ultimate debug mode. It will compile rocksdb
# without any optimizations. To compile with level 2, issue `make dbg`
# * DEBUG_LEVEL=1; debug level 1 enables all assertions and debug code, but
# compiles rocksdb with -O2 optimizations. this is the default debug level.
# `make all` or `make <binary_target>` compile RocksDB with debug level 1.
# We use this debug level when developing RocksDB.
# * DEBUG_LEVEL=0; this is the debug level we use for release. If you're
# running rocksdb in production you most definitely want to compile RocksDB
# with debug level 0. To compile with level 0, run `make shared_lib`,
# `make install-shared`, `make static_lib`, `make install-static` or
# `make install`

# Set the default DEBUG_LEVEL to 1
DEBUG_LEVEL?=1

ifeq ($(MAKECMDGOALS),dbg)
	DEBUG_LEVEL=2
endif

ifeq ($(MAKECMDGOALS),clean)
	DEBUG_LEVEL=0
endif

ifeq ($(MAKECMDGOALS),release)
	DEBUG_LEVEL=0
endif

ifeq ($(MAKECMDGOALS),shared_lib)
	DEBUG_LEVEL=0
endif

ifeq ($(MAKECMDGOALS),install-shared)
	DEBUG_LEVEL=0
endif

ifeq ($(MAKECMDGOALS),static_lib)
	DEBUG_LEVEL=0
endif

ifeq ($(MAKECMDGOALS),install-static)
	DEBUG_LEVEL=0
endif

ifeq ($(MAKECMDGOALS),install)
	DEBUG_LEVEL=0
endif

ifeq ($(MAKECMDGOALS),rocksdbjavastatic)
	ifneq ($(DEBUG_LEVEL),2)
		DEBUG_LEVEL=0
	endif
endif

ifeq ($(MAKECMDGOALS),rocksdbjavastaticrelease)
	ifneq ($(DEBUG_LEVEL),2)
		DEBUG_LEVEL=0
	endif
endif

ifeq ($(MAKECMDGOALS),rocksdbjavastaticreleasedocker)
	ifneq ($(DEBUG_LEVEL),2)
		DEBUG_LEVEL=0
	endif
endif

ifeq ($(MAKECMDGOALS),rocksdbjavastaticpublish)
	DEBUG_LEVEL=0
endif

$(info $$DEBUG_LEVEL is ${DEBUG_LEVEL})

# Lite build flag.
LITE ?= 0
ifeq ($(LITE), 0)
ifneq ($(filter -DROCKSDB_LITE,$(OPT)),)
  # Be backward compatible and support older format where OPT=-DROCKSDB_LITE is
  # specified instead of LITE=1 on the command line.
  LITE=1
endif
else ifeq ($(LITE), 1)
ifeq ($(filter -DROCKSDB_LITE,$(OPT)),)
	OPT += -DROCKSDB_LITE
endif
endif

# Figure out optimize level.
ifneq ($(DEBUG_LEVEL), 2)
ifeq ($(LITE), 0)
	OPT += -O2
else
	OPT += -Os
endif
endif

# compile with -O2 if debug level is not 2
ifneq ($(DEBUG_LEVEL), 2)
OPT += -fno-omit-frame-pointer
# Skip for archs that don't support -momit-leaf-frame-pointer
ifeq (,$(shell $(CXX) -fsyntax-only -momit-leaf-frame-pointer -xc /dev/null 2>&1))
OPT += -momit-leaf-frame-pointer
endif
endif

ifeq (,$(shell $(CXX) -fsyntax-only -maltivec -xc /dev/null 2>&1))
CXXFLAGS += -DHAS_ALTIVEC
CFLAGS += -DHAS_ALTIVEC
HAS_ALTIVEC=1
endif

ifeq (,$(shell $(CXX) -fsyntax-only -mcpu=power8 -xc /dev/null 2>&1))
CXXFLAGS += -DHAVE_POWER8
CFLAGS +=  -DHAVE_POWER8
HAVE_POWER8=1
endif

ifeq (,$(shell $(CXX) -fsyntax-only -march=armv8-a+crc+crypto -xc /dev/null 2>&1))
CXXFLAGS += -march=armv8-a+crc+crypto
CFLAGS += -march=armv8-a+crc+crypto
ARMCRC_SOURCE=1
endif

# if we're compiling for release, compile without debug code (-DNDEBUG)
ifeq ($(DEBUG_LEVEL),0)
OPT += -DNDEBUG

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti
else
	CXXFLAGS += -DROCKSDB_USE_RTTI
endif
else
ifneq ($(USE_RTTI), 0)
	CXXFLAGS += -DROCKSDB_USE_RTTI
else
	CXXFLAGS += -fno-rtti
endif

$(warning Warning: Compiling in debug mode. Don't use the resulting binary in production)
endif

#-----------------------------------------------
include src.mk

AM_DEFAULT_VERBOSITY = 0

AM_V_GEN = $(am__v_GEN_$(V))
am__v_GEN_ = $(am__v_GEN_$(AM_DEFAULT_VERBOSITY))
am__v_GEN_0 = @echo "  GEN     " $@;
am__v_GEN_1 =
AM_V_at = $(am__v_at_$(V))
am__v_at_ = $(am__v_at_$(AM_DEFAULT_VERBOSITY))
am__v_at_0 = @
am__v_at_1 =

AM_V_CC = $(am__v_CC_$(V))
am__v_CC_ = $(am__v_CC_$(AM_DEFAULT_VERBOSITY))
am__v_CC_0 = @echo "  CC      " $@;
am__v_CC_1 =
CCLD = $(CC)
LINK = $(CCLD) $(AM_CFLAGS) $(CFLAGS) $(AM_LDFLAGS) $(LDFLAGS) -o $@
AM_V_CCLD = $(am__v_CCLD_$(V))
am__v_CCLD_ = $(am__v_CCLD_$(AM_DEFAULT_VERBOSITY))
am__v_CCLD_0 = @echo "  CCLD    " $@;
am__v_CCLD_1 =
AM_V_AR = $(am__v_AR_$(V))
am__v_AR_ = $(am__v_AR_$(AM_DEFAULT_VERBOSITY))
am__v_AR_0 = @echo "  AR      " $@;
am__v_AR_1 =

ifdef ROCKSDB_USE_LIBRADOS
LIB_SOURCES += utilities/env_librados.cc
LDFLAGS += -lrados
endif

AM_LINK = $(AM_V_CCLD)$(CXX) $^ $(EXEC_LDFLAGS) -o $@ $(LDFLAGS) $(COVERAGEFLAGS)
# Detect what platform we're building on.
# Export some common variables that might have been passed as Make variables
# instead of environment variables.
dummy := $(shell (export ROCKSDB_ROOT="$(CURDIR)"; \
                  export COMPILE_WITH_ASAN="$(COMPILE_WITH_ASAN)"; \
                  export COMPILE_WITH_TSAN="$(COMPILE_WITH_TSAN)"; \
                  export COMPILE_WITH_UBSAN="$(COMPILE_WITH_UBSAN)"; \
                  export PORTABLE="$(PORTABLE)"; \
                  export ROCKSDB_NO_FBCODE="$(ROCKSDB_NO_FBCODE)"; \
                  export USE_CLANG="$(USE_CLANG)"; \
                  "$(CURDIR)/build_tools/build_detect_platform" "$(CURDIR)/make_config.mk"))
# this file is generated by the previous line to set build flags and sources
include make_config.mk
export JAVAC_ARGS
CLEAN_FILES += make_config.mk

missing_make_config_paths := $(shell				\
	grep "\./\S*\|/\S*" -o $(CURDIR)/make_config.mk | 	\
	while read path;					\
		do [ -e $$path ] || echo $$path; 		\
	done | sort | uniq)

$(foreach path, $(missing_make_config_paths), \
	$(warning Warning: $(path) does not exist))

ifeq ($(PLATFORM), OS_AIX)
# no debug info
else ifneq ($(PLATFORM), IOS)
CFLAGS += -g
CXXFLAGS += -g
else
# no debug info for IOS, that will make our library big
OPT += -DNDEBUG
endif

ifeq ($(PLATFORM), OS_AIX)
ARFLAGS = -X64 rs
STRIPFLAGS = -X64 -x
endif

ifeq ($(PLATFORM), OS_SOLARIS)
	PLATFORM_CXXFLAGS += -D _GLIBCXX_USE_C99
endif
ifneq ($(filter -DROCKSDB_LITE,$(OPT)),)
	# found
	CFLAGS += -fno-exceptions
	CXXFLAGS += -fno-exceptions
	# LUA is not supported under ROCKSDB_LITE
	LUA_PATH =
endif

# ASAN doesn't work well with jemalloc. If we're compiling with ASAN, we should use regular malloc.
ifdef COMPILE_WITH_ASAN
	DISABLE_JEMALLOC=1
	EXEC_LDFLAGS += -fsanitize=address
	PLATFORM_CCFLAGS += -fsanitize=address
	PLATFORM_CXXFLAGS += -fsanitize=address
endif

# TSAN doesn't work well with jemalloc. If we're compiling with TSAN, we should use regular malloc.
ifdef COMPILE_WITH_TSAN
	DISABLE_JEMALLOC=1
	EXEC_LDFLAGS += -fsanitize=thread
	PLATFORM_CCFLAGS += -fsanitize=thread -fPIC -DFOLLY_SANITIZE_THREAD
	PLATFORM_CXXFLAGS += -fsanitize=thread -fPIC -DFOLLY_SANITIZE_THREAD
        # Turn off -pg when enabling TSAN testing, because that induces
        # a link failure.  TODO: find the root cause
	PROFILING_FLAGS =
	# LUA is not supported under TSAN
	LUA_PATH =
	# Limit keys for crash test under TSAN to avoid error:
	# "ThreadSanitizer: DenseSlabAllocator overflow. Dying."
	CRASH_TEST_EXT_ARGS += --max_key=1000000
endif

# AIX doesn't work with -pg
ifeq ($(PLATFORM), OS_AIX)
	PROFILING_FLAGS =
endif

# USAN doesn't work well with jemalloc. If we're compiling with USAN, we should use regular malloc.
ifdef COMPILE_WITH_UBSAN
	DISABLE_JEMALLOC=1
	# Suppress alignment warning because murmurhash relies on casting unaligned
	# memory to integer. Fixing it may cause performance regression. 3-way crc32
	# relies on it too, although it can be rewritten to eliminate with minimal
	# performance regression.
	EXEC_LDFLAGS += -fsanitize=undefined -fno-sanitize-recover=all
	PLATFORM_CCFLAGS += -fsanitize=undefined -fno-sanitize-recover=all -DROCKSDB_UBSAN_RUN
	PLATFORM_CXXFLAGS += -fsanitize=undefined -fno-sanitize-recover=all -DROCKSDB_UBSAN_RUN
endif

ifdef ROCKSDB_VALGRIND_RUN
	PLATFORM_CCFLAGS += -DROCKSDB_VALGRIND_RUN
	PLATFORM_CXXFLAGS += -DROCKSDB_VALGRIND_RUN
endif

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
		PLATFORM_CCFLAGS  += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	ifdef WITH_JEMALLOC_FLAG
		PLATFORM_LDFLAGS += -ljemalloc
		JAVA_LDFLAGS += -ljemalloc
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS)
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
	PLATFORM_CCFLAGS += $(JEMALLOC_INCLUDE)
endif

ifndef USE_FOLLY_DISTRIBUTED_MUTEX
	USE_FOLLY_DISTRIBUTED_MUTEX=0
endif

export GTEST_THROW_ON_FAILURE=1
export GTEST_HAS_EXCEPTIONS=1
GTEST_DIR = ./third-party/gtest-1.8.1/fused-src
# AIX: pre-defined system headers are surrounded by an extern "C" block
ifeq ($(PLATFORM), OS_AIX)
	PLATFORM_CCFLAGS += -I$(GTEST_DIR)
	PLATFORM_CXXFLAGS += -I$(GTEST_DIR)
else
	PLATFORM_CCFLAGS += -isystem $(GTEST_DIR)
	PLATFORM_CXXFLAGS += -isystem $(GTEST_DIR)
endif

ifeq ($(USE_FOLLY_DISTRIBUTED_MUTEX),1)
	FOLLY_DIR = ./third-party/folly
	# AIX: pre-defined system headers are surrounded by an extern "C" block
	ifeq ($(PLATFORM), OS_AIX)
		PLATFORM_CCFLAGS += -I$(FOLLY_DIR)
		PLATFORM_CXXFLAGS += -I$(FOLLY_DIR)
	else
		PLATFORM_CCFLAGS += -isystem $(FOLLY_DIR)
		PLATFORM_CXXFLAGS += -isystem $(FOLLY_DIR)
	endif
endif

ifdef TEST_CACHE_LINE_SIZE
  PLATFORM_CCFLAGS += -DTEST_CACHE_LINE_SIZE=$(TEST_CACHE_LINE_SIZE)
  PLATFORM_CXXFLAGS += -DTEST_CACHE_LINE_SIZE=$(TEST_CACHE_LINE_SIZE)
endif

# This (the first rule) must depend on "all".
default: all

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare -Wshadow \
  -Wunused-parameter

ifeq ($(PLATFORM), OS_OPENBSD)
	WARNING_FLAGS += -Wno-unused-lambda-capture
endif

ifndef DISABLE_WARNING_AS_ERROR
	WARNING_FLAGS += -Werror
endif


ifdef LUA_PATH

ifndef LUA_INCLUDE
LUA_INCLUDE=$(LUA_PATH)/include
endif

LUA_INCLUDE_FILE=$(LUA_INCLUDE)/lualib.h

ifeq ("$(wildcard $(LUA_INCLUDE_FILE))", "")
# LUA_INCLUDE_FILE does not exist
$(error Cannot find lualib.h under $(LUA_INCLUDE).  Try to specify both LUA_PATH and LUA_INCLUDE manually)
endif
LUA_FLAGS = -I$(LUA_INCLUDE) -DLUA -DLUA_COMPAT_ALL
CFLAGS += $(LUA_FLAGS)
CXXFLAGS += $(LUA_FLAGS)

ifndef LUA_LIB
LUA_LIB = $(LUA_PATH)/lib/liblua.a
endif
ifeq ("$(wildcard $(LUA_LIB))", "") # LUA_LIB does not exist
$(error $(LUA_LIB) does not exist.  Try to specify both LUA_PATH and LUA_LIB manually)
endif
EXEC_LDFLAGS += $(LUA_LIB)

endif

ifeq ($(NO_THREEWAY_CRC32C), 1)
	CXXFLAGS += -DNO_THREEWAY_CRC32C
endif

CFLAGS += $(WARNING_FLAGS) -I. -I./include $(PLATFORM_CCFLAGS) $(OPT)
CXXFLAGS += $(WARNING_FLAGS) -I. -I./include $(PLATFORM_CXXFLAGS) $(OPT) -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers

LDFLAGS += $(PLATFORM_LDFLAGS)

# If NO_UPDATE_BUILD_VERSION is set we don't update util/build_version.cc, but
# the file needs to already exist or else the build will fail
ifndef NO_UPDATE_BUILD_VERSION
date := $(shell date +%F)
ifdef FORCE_GIT_SHA
	git_sha := $(FORCE_GIT_SHA)
else
	git_sha := $(shell git rev-parse HEAD 2>/dev/null)
endif
gen_build_version = sed -e s/@@GIT_SHA@@/$(git_sha)/ -e s/@@GIT_DATE_TIME@@/$(date)/ util/build_version.cc.in

# Record the version of the source that we are compiling.
# We keep a record of the git revision in this file.  It is then built
# as a regular source file as part of the compilation process.
# One can run "strings executable_filename | grep _build_" to find
# the version of the source that we used to build the executable file.
FORCE:
util/build_version.cc: FORCE
	$(AM_V_GEN)rm -f $@-t
	$(AM_V_at)$(gen_build_version) > $@-t
	$(AM_V_at)if test -f $@; then					\
	  cmp -s $@-t $@ && rm -f $@-t || mv -f $@-t $@;		\
	else mv -f $@-t $@; fi
endif

LIBOBJECTS = $(LIB_SOURCES:.cc=.o)
ifeq ($(HAVE_POWER8),1)
LIB_CC_OBJECTS = $(LIB_SOURCES:.cc=.o)
LIBOBJECTS += $(LIB_SOURCES_C:.c=.o)
LIBOBJECTS += $(LIB_SOURCES_ASM:.S=.o)
else
LIB_CC_OBJECTS = $(LIB_SOURCES:.cc=.o)
endif

LIBOBJECTS += $(TOOL_LIB_SOURCES:.cc=.o)
MOCKOBJECTS = $(MOCK_LIB_SOURCES:.cc=.o)
ifeq ($(USE_FOLLY_DISTRIBUTED_MUTEX),1)
  FOLLYOBJECTS = $(FOLLY_SOURCES:.cpp=.o)
endif

GTEST = $(GTEST_DIR)/gtest/gtest-all.o
TESTUTIL = ./test_util/testutil.o
TESTHARNESS = ./test_util/testharness.o $(TESTUTIL) $(MOCKOBJECTS) $(GTEST)
VALGRIND_ERROR = 2
VALGRIND_VER := $(join $(VALGRIND_VER),valgrind)

VALGRIND_OPTS = --error-exitcode=$(VALGRIND_ERROR) --leak-check=full

BENCHTOOLOBJECTS = $(BENCH_LIB_SOURCES:.cc=.o) $(LIBOBJECTS) $(TESTUTIL)

ANALYZETOOLOBJECTS = $(ANALYZER_LIB_SOURCES:.cc=.o)

ifeq ($(DEBUG_LEVEL),0)
STRESSTOOLOBJECTS = $(STRESS_LIB_SOURCES:.cc=.o) $(LIBOBJECTS) $(TESTUTIL)
else
STRESSTOOLOBJECTS = $(STRESS_LIB_SOURCES:.cc=.o) $(LIBOBJECTS) $(TESTUTIL) \
	$(TESTHARNESS)
endif

EXPOBJECTS = $(LIBOBJECTS) $(TESTUTIL)

TESTS = \
	db_basic_test \
	db_with_timestamp_basic_test \
	db_encryption_test \
	db_test2 \
	external_sst_file_basic_test \
	auto_roll_logger_test \
	bloom_test \
	dynamic_bloom_test \
	c_test \
	checkpoint_test \
	crc32c_test \
	coding_test \
	inlineskiplist_test \
	env_basic_test \
	env_test \
	env_logger_test \
	io_posix_test \
	hash_test \
	random_test \
	thread_local_test \
	work_queue_test \
	rate_limiter_test \
	perf_context_test \
	iostats_context_test \
	db_wal_test \
	db_block_cache_test \
	db_test \
	db_logical_block_size_cache_test \
	db_blob_index_test \
	db_iter_test \
	db_iter_stress_test \
	db_log_iter_test \
	db_bloom_filter_test \
	db_compaction_filter_test \
	db_compaction_test \
	db_dynamic_level_test \
	db_flush_test \
	db_inplace_update_test \
	db_iterator_test \
	db_memtable_test \
	db_merge_operator_test \
	db_merge_operand_test \
	db_options_test \
	db_range_del_test \
	db_secondary_test \
	db_sst_test \
	db_tailing_iter_test \
	db_io_failure_test \
	db_properties_test \
	db_table_properties_test \
	db_statistics_test \
	db_write_test \
	error_handler_fs_test \
	autovector_test \
	blob_db_test \
	cleanable_test \
	column_family_test \
	table_properties_collector_test \
	arena_test \
	memkind_kmem_allocator_test \
	block_test \
	data_block_hash_index_test \
	cache_test \
	corruption_test \
	slice_test \
	slice_transform_test \
	dbformat_test \
	fault_injection_test \
	filelock_test \
	filename_test \
	random_access_file_reader_test \
	file_reader_writer_test \
	block_based_filter_block_test \
	full_filter_block_test \
	partitioned_filter_block_test \
	hash_table_test \
	histogram_test \
	log_test \
	manual_compaction_test \
	mock_env_test \
	memtable_list_test \
	merge_helper_test \
	memory_test \
	merge_test \
	merger_test \
	util_merge_operators_test \
	options_file_test \
	reduce_levels_test \
	plain_table_db_test \
	comparator_db_test \
	external_sst_file_test \
	import_column_family_test \
	prefix_test \
	skiplist_test \
	write_buffer_manager_test \
	stringappend_test \
	cassandra_format_test \
	cassandra_functional_test \
	cassandra_row_merge_test \
	cassandra_serialize_test \
	ttl_test \
	backupable_db_test \
	cache_simulator_test \
	sim_cache_test \
	version_edit_test \
	version_set_test \
	compaction_picker_test \
	version_builder_test \
	file_indexer_test \
	write_batch_test \
	write_batch_with_index_test \
	write_controller_test\
	deletefile_test \
	obsolete_files_test \
	table_test \
	block_fetcher_test \
	delete_scheduler_test \
	options_test \
	options_settable_test \
	options_util_test \
	event_logger_test \
	timer_queue_test \
	cuckoo_table_builder_test \
	cuckoo_table_reader_test \
	cuckoo_table_db_test \
	flush_job_test \
	wal_manager_test \
	listener_test \
	compaction_iterator_test \
	compaction_job_test \
	thread_list_test \
	sst_dump_test \
	compact_files_test \
	optimistic_transaction_test \
	write_callback_test \
	heap_test \
	compact_on_deletion_collector_test \
	compaction_job_stats_test \
	option_change_migration_test \
	transaction_test \
	transaction_lock_mgr_test \
	ldb_cmd_test \
	persistent_cache_test \
	statistics_test \
	stats_history_test \
	lru_cache_test \
	object_registry_test \
	repair_test \
	env_timed_test \
	write_prepared_transaction_test \
	write_unprepared_transaction_test \
	db_universal_compaction_test \
	trace_analyzer_test \
	repeatable_thread_test \
	range_tombstone_fragmenter_test \
	range_del_aggregator_test \
	sst_file_reader_test \
	db_secondary_test \
	block_cache_tracer_test \
	block_cache_trace_analyzer_test \
	defer_test \
	blob_file_addition_test \
	blob_file_garbage_test \
	timer_test \
	db_with_timestamp_compaction_test \

PARALLEL_TEST = \
	backupable_db_test \
	db_bloom_filter_test \
	db_compaction_filter_test \
	db_compaction_test \
	db_merge_operator_test \
	db_sst_test \
	db_test \
	db_universal_compaction_test \
	db_wal_test \
	external_sst_file_test \
	import_column_family_test \
	fault_injection_test \
	file_reader_writer_test \
	inlineskiplist_test \
	manual_compaction_test \
	persistent_cache_test \
	table_test \
	transaction_test \
	transaction_lock_mgr_test \
	write_prepared_transaction_test \
	write_unprepared_transaction_test \

ifeq ($(USE_FOLLY_DISTRIBUTED_MUTEX),1)
	TESTS += folly_synchronization_distributed_mutex_test
	PARALLEL_TEST += folly_synchronization_distributed_mutex_test
endif

# options_settable_test doesn't pass with UBSAN as we use hack in the test
ifdef COMPILE_WITH_UBSAN
        TESTS := $(shell echo $(TESTS) | sed 's/\boptions_settable_test\b//g')
endif
SUBSET := $(TESTS)
ifdef ROCKSDBTESTS_START
        SUBSET := $(shell echo $(SUBSET) | sed 's/^.*$(ROCKSDBTESTS_START)/$(ROCKSDBTESTS_START)/')
endif

ifdef ROCKSDBTESTS_END
        SUBSET := $(shell echo $(SUBSET) | sed 's/$(ROCKSDBTESTS_END).*//')
endif

TOOLS = \
	sst_dump \
	db_sanity_test \
	db_stress \
	write_stress \
	ldb \
	db_repl_stress \
	rocksdb_dump \
	rocksdb_undump \
	blob_dump \
	trace_analyzer \
	block_cache_trace_analyzer \

TEST_LIBS = \
	librocksdb_env_basic_test.a

# TODO: add back forward_iterator_bench, after making it build in all environemnts.
BENCHMARKS = db_bench table_reader_bench cache_bench memtablerep_bench filter_bench persistent_cache_bench range_del_aggregator_bench

# if user didn't config LIBNAME, set the default
ifeq ($(LIBNAME),)
# we should only run rocksdb in production with DEBUG_LEVEL 0
ifeq ($(DEBUG_LEVEL),0)
        LIBNAME=librocksdb
else
        LIBNAME=librocksdb_debug
endif
endif
LIBRARY = ${LIBNAME}.a
TOOLS_LIBRARY = ${LIBNAME}_tools.a
STRESS_LIBRARY = ${LIBNAME}_stress.a

ROCKSDB_MAJOR = $(shell egrep "ROCKSDB_MAJOR.[0-9]" include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_MINOR = $(shell egrep "ROCKSDB_MINOR.[0-9]" include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_PATCH = $(shell egrep "ROCKSDB_PATCH.[0-9]" include/rocksdb/version.h | cut -d ' ' -f 3)

default: all

#-----------------------------------------------
# Create platform independent shared libraries.
#-----------------------------------------------
ifneq ($(PLATFORM_SHARED_EXT),)

ifneq ($(PLATFORM_SHARED_VERSIONED),true)
SHARED1 = ${LIBNAME}.$(PLATFORM_SHARED_EXT)
SHARED2 = $(SHARED1)
SHARED3 = $(SHARED1)
SHARED4 = $(SHARED1)
SHARED = $(SHARED1)
else
SHARED_MAJOR = $(ROCKSDB_MAJOR)
SHARED_MINOR = $(ROCKSDB_MINOR)
SHARED_PATCH = $(ROCKSDB_PATCH)
SHARED1 = ${LIBNAME}.$(PLATFORM_SHARED_EXT)
ifeq ($(PLATFORM), OS_MACOSX)
SHARED_OSX = $(LIBNAME).$(SHARED_MAJOR)
SHARED2 = $(SHARED_OSX).$(PLATFORM_SHARED_EXT)
SHARED3 = $(SHARED_OSX).$(SHARED_MINOR).$(PLATFORM_SHARED_EXT)
SHARED4 = $(SHARED_OSX).$(SHARED_MINOR).$(SHARED_PATCH).$(PLATFORM_SHARED_EXT)
else
SHARED2 = $(SHARED1).$(SHARED_MAJOR)
SHARED3 = $(SHARED1).$(SHARED_MAJOR).$(SHARED_MINOR)
SHARED4 = $(SHARED1).$(SHARED_MAJOR).$(SHARED_MINOR).$(SHARED_PATCH)
endif
SHARED = $(SHARED1) $(SHARED2) $(SHARED3) $(SHARED4)
$(SHARED1): $(SHARED4)
	ln -fs $(SHARED4) $(SHARED1)
$(SHARED2): $(SHARED4)
	ln -fs $(SHARED4) $(SHARED2)
$(SHARED3): $(SHARED4)
	ln -fs $(SHARED4) $(SHARED3)
endif
ifeq ($(HAVE_POWER8),1)
SHARED_C_OBJECTS = $(LIB_SOURCES_C:.c=.o)
SHARED_ASM_OBJECTS = $(LIB_SOURCES_ASM:.S=.o)
SHARED_C_LIBOBJECTS = $(patsubst %.o,shared-objects/%.o,$(SHARED_C_OBJECTS))
SHARED_ASM_LIBOBJECTS = $(patsubst %.o,shared-objects/%.o,$(SHARED_ASM_OBJECTS))
shared_libobjects = $(patsubst %,shared-objects/%,$(LIB_CC_OBJECTS))
else
shared_libobjects = $(patsubst %,shared-objects/%,$(LIBOBJECTS))
endif

CLEAN_FILES += shared-objects
shared_all_libobjects = $(shared_libobjects)

ifeq ($(HAVE_POWER8),1)
shared-ppc-objects = $(SHARED_C_LIBOBJECTS) $(SHARED_ASM_LIBOBJECTS)

shared-objects/util/crc32c_ppc.o: util/crc32c_ppc.c
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@

shared-objects/util/crc32c_ppc_asm.o: util/crc32c_ppc_asm.S
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@
endif
$(shared_libobjects): shared-objects/%.o: %.cc
	$(AM_V_CC)mkdir -p $(@D) && $(CXX) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) -c $< -o $@

ifeq ($(HAVE_POWER8),1)
shared_all_libobjects = $(shared_libobjects) $(shared-ppc-objects)
endif
$(SHARED4): $(shared_all_libobjects)
	$(CXX) $(PLATFORM_SHARED_LDFLAGS)$(SHARED3) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) $(shared_all_libobjects) $(LDFLAGS) -o $@

endif  # PLATFORM_SHARED_EXT

.PHONY: blackbox_crash_test check clean coverage crash_test ldb_tests package \
	release tags tags0 valgrind_check whitebox_crash_test format static_lib shared_lib all \
	dbg rocksdbjavastatic rocksdbjava install install-static install-shared uninstall \
	analyze tools tools_lib \
	blackbox_crash_test_with_atomic_flush whitebox_crash_test_with_atomic_flush  \
	blackbox_crash_test_with_txn whitebox_crash_test_with_txn


all: $(LIBRARY) $(BENCHMARKS) tools tools_lib test_libs $(TESTS)

all_but_some_tests: $(LIBRARY) $(BENCHMARKS) tools tools_lib test_libs $(SUBSET)

static_lib: $(LIBRARY)

shared_lib: $(SHARED)

stress_lib: $(STRESS_LIBRARY)

tools: $(TOOLS)

tools_lib: $(TOOLS_LIBRARY)

test_libs: $(TEST_LIBS)

dbg: $(LIBRARY) $(BENCHMARKS) tools $(TESTS)

# creates static library and programs
release:
	$(MAKE) clean
	DEBUG_LEVEL=0 $(MAKE) static_lib tools db_bench

coverage:
	$(MAKE) clean
	COVERAGEFLAGS="-fprofile-arcs -ftest-coverage" LDFLAGS+="-lgcov" $(MAKE) J=1 all check
	cd coverage && ./coverage_test.sh
        # Delete intermediate files
	$(FIND) . -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm {} \;

ifneq (,$(filter check parallel_check,$(MAKECMDGOALS)),)
# Use /dev/shm if it has the sticky bit set (otherwise, /tmp),
# and create a randomly-named rocksdb.XXXX directory therein.
# We'll use that directory in the "make check" rules.
ifeq ($(TMPD),)
TMPDIR := $(shell echo $${TMPDIR:-/tmp})
TMPD := $(shell f=/dev/shm; test -k $$f || f=$(TMPDIR);     \
  perl -le 'use File::Temp "tempdir";'					\
    -e 'print tempdir("'$$f'/rocksdb.XXXX", CLEANUP => 0)')
endif
endif

# Run all tests in parallel, accumulating per-test logs in t/log-*.
#
# Each t/run-* file is a tiny generated bourne shell script that invokes one of
# sub-tests. Why use a file for this?  Because that makes the invocation of
# parallel below simpler, which in turn makes the parsing of parallel's
# LOG simpler (the latter is for live monitoring as parallel
# tests run).
#
# Test names are extracted by running tests with --gtest_list_tests.
# This filter removes the "#"-introduced comments, and expands to
# fully-qualified names by changing input like this:
#
#   DBTest.
#     Empty
#     WriteEmptyBatch
#   MultiThreaded/MultiThreadedDBTest.
#     MultiThreaded/0  # GetParam() = 0
#     MultiThreaded/1  # GetParam() = 1
#
# into this:
#
#   DBTest.Empty
#   DBTest.WriteEmptyBatch
#   MultiThreaded/MultiThreadedDBTest.MultiThreaded/0
#   MultiThreaded/MultiThreadedDBTest.MultiThreaded/1
#

parallel_tests = $(patsubst %,parallel_%,$(PARALLEL_TEST))
.PHONY: gen_parallel_tests $(parallel_tests)
$(parallel_tests): $(PARALLEL_TEST)
	$(AM_V_at)TEST_BINARY=$(patsubst parallel_%,%,$@); \
  TEST_NAMES=` \
    ./$$TEST_BINARY --gtest_list_tests \
    | perl -n \
      -e 's/ *\#.*//;' \
      -e '/^(\s*)(\S+)/; !$$1 and do {$$p=$$2; break};'	\
      -e 'print qq! $$p$$2!'`; \
	for TEST_NAME in $$TEST_NAMES; do \
		TEST_SCRIPT=t/run-$$TEST_BINARY-$${TEST_NAME//\//-}; \
		echo "  GEN     " $$TEST_SCRIPT; \
    printf '%s\n' \
      '#!/bin/sh' \
      "d=\$(TMPD)$$TEST_SCRIPT" \
      'mkdir -p $$d' \
      "TEST_TMPDIR=\$$d $(DRIVER) ./$$TEST_BINARY --gtest_filter=$$TEST_NAME" \
		> $$TEST_SCRIPT; \
		chmod a=rx $$TEST_SCRIPT; \
	done

gen_parallel_tests:
	$(AM_V_at)mkdir -p t
	$(AM_V_at)rm -f t/run-*
	$(MAKE) $(parallel_tests)

# Reorder input lines (which are one per test) so that the
# longest-running tests appear first in the output.
# Do this by prefixing each selected name with its duration,
# sort the resulting names, and remove the leading numbers.
# FIXME: the "100" we prepend is a fake time, for now.
# FIXME: squirrel away timings from each run and use them
# (when present) on subsequent runs to order these tests.
#
# Without this reordering, these two tests would happen to start only
# after almost all other tests had completed, thus adding 100 seconds
# to the duration of parallel "make check".  That's the difference
# between 4 minutes (old) and 2m20s (new).
#
# 152.120 PASS t/DBTest.FileCreationRandomFailure
# 107.816 PASS t/DBTest.EncodeDecompressedBlockSizeTest
#
slow_test_regexp = \
	^.*SnapshotConcurrentAccessTest.*$$|^t/run-table_test-HarnessTest.Randomized$$|^t/run-db_test-.*(?:FileCreationRandomFailure|EncodeDecompressedBlockSizeTest)$$|^.*RecoverFromCorruptedWALWithoutFlush$$
prioritize_long_running_tests =						\
  perl -pe 's,($(slow_test_regexp)),100 $$1,'				\
    | sort -k1,1gr							\
    | sed 's/^[.0-9]* //'

# "make check" uses
# Run with "make J=1 check" to disable parallelism in "make check".
# Run with "make J=200% check" to run two parallel jobs per core.
# The default is to run one job per core (J=100%).
# See "man parallel" for its "-j ..." option.
J ?= 100%

# Use this regexp to select the subset of tests whose names match.
tests-regexp = .

.PHONY: check_0
check_0:
	$(AM_V_GEN)export TEST_TMPDIR=$(TMPD); \
	printf '%s\n' ''						\
	  'To monitor subtest <duration,pass/fail,name>,'		\
	  '  run "make watch-log" in a separate window' '';		\
	test -t 1 && eta=--eta || eta=; \
	{ \
		printf './%s\n' $(filter-out $(PARALLEL_TEST),$(TESTS)); \
		find t -name 'run-*' -print; \
	} \
	  | $(prioritize_long_running_tests)				\
	  | grep -E '$(tests-regexp)'					\
	  | build_tools/gnu_parallel -j$(J) --plain --joblog=LOG $$eta --gnu '{} >& t/log-{/}'

valgrind-blacklist-regexp = InlineSkipTest.ConcurrentInsert|TransactionStressTest.DeadlockStress|DBCompactionTest.SuggestCompactRangeNoTwoLevel0Compactions|BackupableDBTest.RateLimiting|DBTest.CloseSpeedup|DBTest.ThreadStatusFlush|DBTest.RateLimitingTest|DBTest.EncodeDecompressedBlockSizeTest|FaultInjectionTest.UninstalledCompaction|HarnessTest.Randomized|ExternalSSTFileTest.CompactDuringAddFileRandom|ExternalSSTFileTest.IngestFileWithGlobalSeqnoRandomized|MySQLStyleTransactionTest.TransactionStressTest

.PHONY: valgrind_check_0
valgrind_check_0:
	$(AM_V_GEN)export TEST_TMPDIR=$(TMPD);				\
	printf '%s\n' ''						\
	  'To monitor subtest <duration,pass/fail,name>,'		\
	  '  run "make watch-log" in a separate window' '';		\
	test -t 1 && eta=--eta || eta=;					\
	{								\
	  printf './%s\n' $(filter-out $(PARALLEL_TEST) %skiplist_test options_settable_test, $(TESTS));		\
	  find t -name 'run-*' -print; \
	}								\
	  | $(prioritize_long_running_tests)				\
	  | grep -E '$(tests-regexp)'					\
	  | grep -E -v '$(valgrind-blacklist-regexp)'					\
	  | build_tools/gnu_parallel -j$(J) --plain --joblog=LOG $$eta --gnu \
	  '(if [[ "{}" == "./"* ]] ; then $(DRIVER) {}; else {}; fi) ' \
	  '>& t/valgrind_log-{/}'

CLEAN_FILES += t LOG $(TMPD)

# When running parallel "make check", you can monitor its progress
# from another window.
# Run "make watch_LOG" to show the duration,PASS/FAIL,name of parallel
# tests as they are being run.  We sort them so that longer-running ones
# appear at the top of the list and any failing tests remain at the top
# regardless of their duration. As with any use of "watch", hit ^C to
# interrupt.
watch-log:
	$(WATCH) --interval=0 'sort -k7,7nr -k4,4gr LOG|$(quoted_perl_command)'

# If J != 1 and GNU parallel is installed, run the tests in parallel,
# via the check_0 rule above.  Otherwise, run them sequentially.
check: all
	$(MAKE) gen_parallel_tests
	$(AM_V_GEN)if test "$(J)" != 1                                  \
	    && (build_tools/gnu_parallel --gnu --help 2>/dev/null) |                    \
	        grep -q 'GNU Parallel';                                 \
	then                                                            \
	    $(MAKE) T="$$t" TMPD=$(TMPD) check_0;                       \
	else                                                            \
	    for t in $(TESTS); do                                       \
	      echo "===== Running $$t (`date`)"; ./$$t || exit 1; done;          \
	fi
	rm -rf $(TMPD)
ifneq ($(PLATFORM), OS_AIX)
	$(PYTHON) tools/check_all_python.py
ifeq ($(filter -DROCKSDB_LITE,$(OPT)),)
ifndef ASSERT_STATUS_CHECKED # not yet working with these tests
	$(PYTHON) tools/ldb_test.py
	sh tools/rocksdb_dump_test.sh
endif
endif
endif
	$(MAKE) check-format
	$(MAKE) check-buck-targets

# TODO add ldb_tests
check_some: $(SUBSET)
	for t in $(SUBSET); do echo "===== Running $$t (`date`)"; ./$$t || exit 1; done

.PHONY: ldb_tests
ldb_tests: ldb
	$(PYTHON) tools/ldb_test.py

crash_test: whitebox_crash_test blackbox_crash_test

crash_test_with_atomic_flush: whitebox_crash_test_with_atomic_flush blackbox_crash_test_with_atomic_flush

crash_test_with_txn: whitebox_crash_test_with_txn blackbox_crash_test_with_txn

blackbox_crash_test: db_stress
	$(PYTHON) -u tools/db_crashtest.py --simple blackbox $(CRASH_TEST_EXT_ARGS)
	$(PYTHON) -u tools/db_crashtest.py blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_atomic_flush: db_stress
	$(PYTHON) -u tools/db_crashtest.py --cf_consistency blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_txn: db_stress
	$(PYTHON) -u tools/db_crashtest.py --txn blackbox $(CRASH_TEST_EXT_ARGS)

ifeq ($(CRASH_TEST_KILL_ODD),)
  CRASH_TEST_KILL_ODD=888887
endif

whitebox_crash_test: db_stress
	$(PYTHON) -u tools/db_crashtest.py --simple whitebox --random_kill_odd \
      $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)
	$(PYTHON) -u tools/db_crashtest.py whitebox  --random_kill_odd \
      $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)

whitebox_crash_test_with_atomic_flush: db_stress
	$(PYTHON) -u tools/db_crashtest.py --cf_consistency whitebox  --random_kill_odd \
      $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)

whitebox_crash_test_with_txn: db_stress
	$(PYTHON) -u tools/db_crashtest.py --txn whitebox --random_kill_odd \
      $(CRASH_TEST_KILL_ODD) $(CRASH_TEST_EXT_ARGS)

asan_check:
	$(MAKE) clean
	COMPILE_WITH_ASAN=1 $(MAKE) check -j32
	$(MAKE) clean

asan_crash_test:
	$(MAKE) clean
	COMPILE_WITH_ASAN=1 $(MAKE) crash_test
	$(MAKE) clean

asan_crash_test_with_atomic_flush:
	$(MAKE) clean
	COMPILE_WITH_ASAN=1 $(MAKE) crash_test_with_atomic_flush
	$(MAKE) clean

asan_crash_test_with_txn:
	$(MAKE) clean
	COMPILE_WITH_ASAN=1 $(MAKE) crash_test_with_txn
	$(MAKE) clean

ubsan_check:
	$(MAKE) clean
	COMPILE_WITH_UBSAN=1 $(MAKE) check -j32
	$(MAKE) clean

ubsan_crash_test:
	$(MAKE) clean
	COMPILE_WITH_UBSAN=1 $(MAKE) crash_test
	$(MAKE) clean

ubsan_crash_test_with_atomic_flush:
	$(MAKE) clean
	COMPILE_WITH_UBSAN=1 $(MAKE) crash_test_with_atomic_flush
	$(MAKE) clean

ubsan_crash_test_with_txn:
	$(MAKE) clean
	COMPILE_WITH_UBSAN=1 $(MAKE) crash_test_with_txn
	$(MAKE) clean

valgrind_test:
	ROCKSDB_VALGRIND_RUN=1 DISABLE_JEMALLOC=1 $(MAKE) valgrind_check

valgrind_check: $(TESTS)
	$(MAKE) DRIVER="$(VALGRIND_VER) $(VALGRIND_OPTS)" gen_parallel_tests
	$(AM_V_GEN)if test "$(J)" != 1                                  \
	    && (build_tools/gnu_parallel --gnu --help 2>/dev/null) |                    \
	        grep -q 'GNU Parallel';                                 \
	then                                                            \
      $(MAKE) TMPD=$(TMPD)                                        \
      DRIVER="$(VALGRIND_VER) $(VALGRIND_OPTS)" valgrind_check_0; \
	else                                                            \
		for t in $(filter-out %skiplist_test options_settable_test,$(TESTS)); do \
			$(VALGRIND_VER) $(VALGRIND_OPTS) ./$$t; \
			ret_code=$$?; \
			if [ $$ret_code -ne 0 ]; then \
				exit $$ret_code; \
			fi; \
		done; \
	fi


ifneq ($(PAR_TEST),)
parloop:
	ret_bad=0;							\
	for t in $(PAR_TEST); do		\
		echo "===== Running $$t in parallel $(NUM_PAR) (`date`)";\
		if [ $(db_test) -eq 1 ]; then \
			seq $(J) | v="$$t" build_tools/gnu_parallel --gnu --plain 's=$(TMPD)/rdb-{};  export TEST_TMPDIR=$$s;' \
				'timeout 2m ./db_test --gtest_filter=$$v >> $$s/log-{} 2>1'; \
		else\
			seq $(J) | v="./$$t" build_tools/gnu_parallel --gnu --plain 's=$(TMPD)/rdb-{};' \
			     'export TEST_TMPDIR=$$s; timeout 10m $$v >> $$s/log-{} 2>1'; \
		fi; \
		ret_code=$$?; \
		if [ $$ret_code -ne 0 ]; then \
			ret_bad=$$ret_code; \
			echo $$t exited with $$ret_code; \
		fi; \
	done; \
	exit $$ret_bad;
endif

test_names = \
  ./db_test --gtest_list_tests						\
    | perl -n								\
      -e 's/ *\#.*//;'							\
      -e '/^(\s*)(\S+)/; !$$1 and do {$$p=$$2; break};'			\
      -e 'print qq! $$p$$2!'

parallel_check: $(TESTS)
	$(AM_V_GEN)if test "$(J)" > 1                                  \
	    && (build_tools/gnu_parallel --gnu --help 2>/dev/null) |                    \
	        grep -q 'GNU Parallel';                                 \
	then                                                            \
	    echo Running in parallel $(J);			\
	else                                                            \
	    echo "Need to have GNU Parallel and J > 1"; exit 1;		\
	fi;								\
	ret_bad=0;							\
	echo $(J);\
	echo Test Dir: $(TMPD); \
        seq $(J) | build_tools/gnu_parallel --gnu --plain 's=$(TMPD)/rdb-{}; rm -rf $$s; mkdir $$s'; \
	$(MAKE)  PAR_TEST="$(shell $(test_names))" TMPD=$(TMPD) \
		J=$(J) db_test=1 parloop; \
	$(MAKE) PAR_TEST="$(filter-out db_test, $(TESTS))" \
		TMPD=$(TMPD) J=$(J) db_test=0 parloop;

analyze: clean
	USE_CLANG=1 $(MAKE) analyze_incremental

analyze_incremental:
	$(CLANG_SCAN_BUILD) --use-analyzer=$(CLANG_ANALYZER) \
		--use-c++=$(CXX) --use-cc=$(CC) --status-bugs \
		-o $(CURDIR)/scan_build_report \
		$(MAKE) dbg

CLEAN_FILES += unity.cc
unity.cc: Makefile
	rm -f $@ $@-t
	for source_file in $(LIB_SOURCES); do \
		echo "#include \"$$source_file\"" >> $@-t; \
	done
	chmod a=r $@-t
	mv $@-t $@

unity.a: unity.o
	$(AM_V_AR)rm -f $@
	$(AM_V_at)$(AR) $(ARFLAGS) $@ unity.o


TOOLLIBOBJECTS = $(TOOL_LIB_SOURCES:.cc=.o)
# try compiling db_test with unity
unity_test: db/db_test.o db/db_test_util.o $(TESTHARNESS) $(TOOLLIBOBJECTS) unity.a
	$(AM_LINK)
	./unity_test

rocksdb.h rocksdb.cc: build_tools/amalgamate.py Makefile $(LIB_SOURCES) unity.cc
	build_tools/amalgamate.py -I. -i./include unity.cc -x include/rocksdb/c.h -H rocksdb.h -o rocksdb.cc

clean: clean-ext-libraries-all clean-rocks clean-rocksjava

clean-not-downloaded: clean-ext-libraries-bin clean-rocks clean-not-downloaded-rocksjava

clean-rocks:
	rm -f $(BENCHMARKS) $(TOOLS) $(TESTS) $(PARALLEL_TEST) $(LIBRARY) $(SHARED)
	rm -rf $(CLEAN_FILES) ios-x86 ios-arm scan_build_report
	$(FIND) . -name "*.[oda]" -exec rm -f {} \;
	$(FIND) . -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm {} \;

clean-rocksjava:
	cd java && $(MAKE) clean

clean-not-downloaded-rocksjava:
	cd java && $(MAKE) clean-not-downloaded

clean-ext-libraries-all:
	rm -rf bzip2* snappy* zlib* lz4* zstd*

clean-ext-libraries-bin:
	find . -maxdepth 1 -type d \( -name bzip2\* -or -name snappy\* -or -name zlib\* -or -name lz4\* -or -name zstd\* \) -prune -exec rm -rf {} \;

tags:
	ctags -R .
	cscope -b `$(FIND) . -name '*.cc'` `$(FIND) . -name '*.h'` `$(FIND) . -name '*.c'`
	ctags -e -R -o etags *

tags0:
	ctags -R .
	cscope -b `$(FIND) . -name '*.cc' -and ! -name '*_test.cc'` \
		  `$(FIND) . -name '*.c' -and ! -name '*_test.c'` \
		  `$(FIND) . -name '*.h' -and ! -name '*_test.h'`
	ctags -e -R -o etags *

format:
	build_tools/format-diff.sh

check-format:
	build_tools/format-diff.sh -c

check-buck-targets:
	buckifier/check_buck_targets.sh

package:
	bash build_tools/make_package.sh $(SHARED_MAJOR).$(SHARED_MINOR)

# ---------------------------------------------------------------------------
# 	Unit tests and tools
# ---------------------------------------------------------------------------
$(LIBRARY): $(LIBOBJECTS)
	$(AM_V_AR)rm -f $@
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $(LIBOBJECTS)

$(TOOLS_LIBRARY): $(BENCH_LIB_SOURCES:.cc=.o) $(TOOL_LIB_SOURCES:.cc=.o) $(LIB_SOURCES:.cc=.o) $(TESTUTIL) $(ANALYZER_LIB_SOURCES:.cc=.o)
	$(AM_V_AR)rm -f $@
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $^

$(STRESS_LIBRARY): $(LIB_SOURCES:.cc=.o) $(TESTUTIL) $(ANALYZER_LIB_SOURCES:.cc=.o) $(STRESS_LIB_SOURCES:.cc=.o)
	$(AM_V_AR)rm -f $@
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $^

librocksdb_env_basic_test.a: env/env_basic_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_V_AR)rm -f $@
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $^

db_bench: tools/db_bench.o $(BENCHTOOLOBJECTS)
	$(AM_LINK)

trace_analyzer: tools/trace_analyzer.o $(ANALYZETOOLOBJECTS) $(LIBOBJECTS)
	$(AM_LINK)

block_cache_trace_analyzer: tools/block_cache_analyzer/block_cache_trace_analyzer_tool.o $(ANALYZETOOLOBJECTS) $(LIBOBJECTS)
	$(AM_LINK)

ifeq ($(USE_FOLLY_DISTRIBUTED_MUTEX),1)
folly_synchronization_distributed_mutex_test: $(LIBOBJECTS) $(TESTHARNESS) $(FOLLYOBJECTS) third-party/folly/folly/synchronization/test/DistributedMutexTest.o
	$(AM_LINK)
endif

cache_bench: cache/cache_bench.o $(LIBOBJECTS) $(TESTUTIL)
	$(AM_LINK)

persistent_cache_bench: utilities/persistent_cache/persistent_cache_bench.o $(LIBOBJECTS) $(TESTUTIL)
	$(AM_LINK)

memtablerep_bench: memtable/memtablerep_bench.o $(LIBOBJECTS) $(TESTUTIL)
	$(AM_LINK)

filter_bench: util/filter_bench.o $(LIBOBJECTS) $(TESTUTIL)
	$(AM_LINK)

db_stress: db_stress_tool/db_stress.o $(STRESSTOOLOBJECTS)
	$(AM_LINK)

write_stress: tools/write_stress.o $(LIBOBJECTS) $(TESTUTIL)
	$(AM_LINK)

db_sanity_test: tools/db_sanity_test.o $(LIBOBJECTS) $(TESTUTIL)
	$(AM_LINK)

db_repl_stress: tools/db_repl_stress.o $(LIBOBJECTS) $(TESTUTIL)
	$(AM_LINK)

arena_test: memory/arena_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)
	
memkind_kmem_allocator_test: memory/memkind_kmem_allocator_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)	

autovector_test: util/autovector_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

column_family_test: db/column_family_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

table_properties_collector_test: db/table_properties_collector_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

bloom_test: util/bloom_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

dynamic_bloom_test: util/dynamic_bloom_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

c_test: db/c_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

cache_test: cache/cache_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

coding_test: util/coding_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

hash_test: util/hash_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

random_test: util/random_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

option_change_migration_test: utilities/option_change_migration/option_change_migration_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

stringappend_test: utilities/merge_operators/string_append/stringappend_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

cassandra_format_test: utilities/cassandra/cassandra_format_test.o utilities/cassandra/test_utils.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

cassandra_functional_test: utilities/cassandra/cassandra_functional_test.o utilities/cassandra/test_utils.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

cassandra_row_merge_test: utilities/cassandra/cassandra_row_merge_test.o utilities/cassandra/test_utils.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

cassandra_serialize_test: utilities/cassandra/cassandra_serialize_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

hash_table_test: utilities/persistent_cache/hash_table_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

histogram_test: monitoring/histogram_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

thread_local_test: util/thread_local_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

work_queue_test: util/work_queue_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

corruption_test: db/corruption_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

crc32c_test: util/crc32c_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

slice_test: util/slice_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

slice_transform_test: util/slice_transform_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_basic_test: db/db_basic_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_with_timestamp_basic_test: db/db_with_timestamp_basic_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_with_timestamp_compaction_test: db/db_with_timestamp_compaction_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_encryption_test: db/db_encryption_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_test: db/db_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_test2: db/db_test2.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_logical_block_size_cache_test: db/db_logical_block_size_cache_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_blob_index_test: db/blob/db_blob_index_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_block_cache_test: db/db_block_cache_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_bloom_filter_test: db/db_bloom_filter_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_log_iter_test: db/db_log_iter_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_compaction_filter_test: db/db_compaction_filter_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_compaction_test: db/db_compaction_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_dynamic_level_test: db/db_dynamic_level_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_flush_test: db/db_flush_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_inplace_update_test: db/db_inplace_update_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_iterator_test: db/db_iterator_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_memtable_test: db/db_memtable_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_merge_operator_test: db/db_merge_operator_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_merge_operand_test: db/db_merge_operand_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_options_test: db/db_options_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_range_del_test: db/db_range_del_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_sst_test: db/db_sst_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_statistics_test: db/db_statistics_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_write_test: db/db_write_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

error_handler_fs_test: db/error_handler_fs_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

external_sst_file_basic_test: db/external_sst_file_basic_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

external_sst_file_test: db/external_sst_file_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

import_column_family_test: db/import_column_family_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_tailing_iter_test: db/db_tailing_iter_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_iter_test: db/db_iter_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_iter_stress_test: db/db_iter_stress_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_universal_compaction_test: db/db_universal_compaction_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_wal_test: db/db_wal_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_io_failure_test: db/db_io_failure_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_properties_test: db/db_properties_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_table_properties_test: db/db_table_properties_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

log_write_bench: util/log_write_bench.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK) $(PROFILING_FLAGS)

plain_table_db_test: db/plain_table_db_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

comparator_db_test: db/comparator_db_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

table_reader_bench: table/table_reader_bench.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK) $(PROFILING_FLAGS)

perf_context_test: db/perf_context_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_V_CCLD)$(CXX) $^ $(EXEC_LDFLAGS) -o $@ $(LDFLAGS)

prefix_test: db/prefix_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_V_CCLD)$(CXX) $^ $(EXEC_LDFLAGS) -o $@ $(LDFLAGS)

backupable_db_test: utilities/backupable/backupable_db_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

checkpoint_test: utilities/checkpoint/checkpoint_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

cache_simulator_test: utilities/simulator_cache/cache_simulator_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

sim_cache_test: utilities/simulator_cache/sim_cache_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

env_mirror_test: utilities/env_mirror_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

env_timed_test: utilities/env_timed_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

ifdef ROCKSDB_USE_LIBRADOS
env_librados_test: utilities/env_librados_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_V_CCLD)$(CXX) $^ $(EXEC_LDFLAGS) -o $@ $(LDFLAGS) $(COVERAGEFLAGS)
endif

object_registry_test: utilities/object_registry_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

ttl_test: utilities/ttl/ttl_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

write_batch_with_index_test: utilities/write_batch_with_index/write_batch_with_index_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

flush_job_test: db/flush_job_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

compaction_iterator_test: db/compaction/compaction_iterator_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

compaction_job_test: db/compaction/compaction_job_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

compaction_job_stats_test: db/compaction/compaction_job_stats_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

compact_on_deletion_collector_test: utilities/table_properties_collectors/compact_on_deletion_collector_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

wal_manager_test: db/wal_manager_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

dbformat_test: db/dbformat_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

env_basic_test: env/env_basic_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

env_test: env/env_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

io_posix_test: env/io_posix_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

fault_injection_test: db/fault_injection_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

rate_limiter_test: util/rate_limiter_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

delete_scheduler_test: file/delete_scheduler_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

filename_test: db/filename_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

random_access_file_reader_test: file/random_access_file_reader_test.o $(LIBOBJECTS) $(TESTHARNESS) $(TESTUTIL)
	$(AM_LINK)

file_reader_writer_test: util/file_reader_writer_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

block_based_filter_block_test: table/block_based/block_based_filter_block_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

full_filter_block_test: table/block_based/full_filter_block_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

partitioned_filter_block_test: table/block_based/partitioned_filter_block_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

log_test: db/log_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

cleanable_test: table/cleanable_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

table_test: table/table_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

block_fetcher_test: table/block_fetcher_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

block_test: table/block_based/block_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

data_block_hash_index_test: table/block_based/data_block_hash_index_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

inlineskiplist_test: memtable/inlineskiplist_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

skiplist_test: memtable/skiplist_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

write_buffer_manager_test: memtable/write_buffer_manager_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

version_edit_test: db/version_edit_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

version_set_test: db/version_set_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

compaction_picker_test: db/compaction/compaction_picker_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

version_builder_test: db/version_builder_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

file_indexer_test: db/file_indexer_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

reduce_levels_test: tools/reduce_levels_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

write_batch_test: db/write_batch_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

write_controller_test: db/write_controller_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

merge_helper_test: db/merge_helper_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

memory_test: utilities/memory/memory_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

merge_test: db/merge_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

merger_test: table/merger_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

util_merge_operators_test: utilities/util_merge_operators_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

options_file_test: db/options_file_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

deletefile_test: db/deletefile_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

obsolete_files_test: db/obsolete_files_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

rocksdb_dump: tools/dump/rocksdb_dump.o $(LIBOBJECTS)
	$(AM_LINK)

rocksdb_undump: tools/dump/rocksdb_undump.o $(LIBOBJECTS)
	$(AM_LINK)

cuckoo_table_builder_test: table/cuckoo/cuckoo_table_builder_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

cuckoo_table_reader_test: table/cuckoo/cuckoo_table_reader_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

cuckoo_table_db_test: db/cuckoo_table_db_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

listener_test: db/listener_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

thread_list_test: util/thread_list_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

compact_files_test: db/compact_files_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

options_test: options/options_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

options_settable_test: options/options_settable_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

options_util_test: utilities/options/options_util_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_bench_tool_test: tools/db_bench_tool_test.o $(BENCHTOOLOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

trace_analyzer_test: tools/trace_analyzer_test.o $(LIBOBJECTS) $(ANALYZETOOLOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

event_logger_test: logging/event_logger_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

timer_queue_test: util/timer_queue_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

sst_dump_test: tools/sst_dump_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

optimistic_transaction_test: utilities/transactions/optimistic_transaction_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

mock_env_test : env/mock_env_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

manual_compaction_test: db/manual_compaction_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

filelock_test: util/filelock_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

auto_roll_logger_test: logging/auto_roll_logger_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

env_logger_test: logging/env_logger_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

memtable_list_test: db/memtable_list_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

write_callback_test: db/write_callback_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

heap_test: util/heap_test.o $(GTEST)
	$(AM_LINK)

transaction_lock_mgr_test: utilities/transactions/transaction_lock_mgr_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

transaction_test: utilities/transactions/transaction_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

write_prepared_transaction_test: utilities/transactions/write_prepared_transaction_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

write_unprepared_transaction_test: utilities/transactions/write_unprepared_transaction_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

sst_dump: tools/sst_dump.o $(LIBOBJECTS)
	$(AM_LINK)

blob_dump: tools/blob_dump.o $(LIBOBJECTS)
	$(AM_LINK)

repair_test: db/repair_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

ldb_cmd_test: tools/ldb_cmd_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

ldb: tools/ldb.o $(LIBOBJECTS)
	$(AM_LINK)

iostats_context_test: monitoring/iostats_context_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_V_CCLD)$(CXX) $^ $(EXEC_LDFLAGS) -o $@ $(LDFLAGS)

persistent_cache_test: utilities/persistent_cache/persistent_cache_test.o  db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

statistics_test: monitoring/statistics_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

stats_history_test: monitoring/stats_history_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

lru_cache_test: cache/lru_cache_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

range_del_aggregator_test: db/range_del_aggregator_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

range_del_aggregator_bench: db/range_del_aggregator_bench.o $(LIBOBJECTS) $(TESTUTIL)
	$(AM_LINK)

blob_db_test: utilities/blob_db/blob_db_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

repeatable_thread_test: util/repeatable_thread_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

range_tombstone_fragmenter_test: db/range_tombstone_fragmenter_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

sst_file_reader_test: table/sst_file_reader_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

db_secondary_test: db/db_impl/db_secondary_test.o db/db_test_util.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

block_cache_tracer_test: trace_replay/block_cache_tracer_test.o trace_replay/block_cache_tracer.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

block_cache_trace_analyzer_test: tools/block_cache_analyzer/block_cache_trace_analyzer_test.o tools/block_cache_analyzer/block_cache_trace_analyzer.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

defer_test: util/defer_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

blob_file_addition_test: db/blob/blob_file_addition_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

blob_file_garbage_test: db/blob/blob_file_garbage_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

timer_test: util/timer_test.o $(LIBOBJECTS) $(TESTHARNESS)
	$(AM_LINK)

#-------------------------------------------------
# make install related stuff
INSTALL_PATH ?= /usr/local

uninstall:
	rm -rf $(INSTALL_PATH)/include/rocksdb \
	  $(INSTALL_PATH)/lib/$(LIBRARY) \
	  $(INSTALL_PATH)/lib/$(SHARED4) \
	  $(INSTALL_PATH)/lib/$(SHARED3) \
	  $(INSTALL_PATH)/lib/$(SHARED2) \
	  $(INSTALL_PATH)/lib/$(SHARED1)

install-headers:
	install -d $(INSTALL_PATH)/lib
	for header_dir in `$(FIND) "include/rocksdb" -type d`; do \
		install -d $(INSTALL_PATH)/$$header_dir; \
	done
	for header in `$(FIND) "include/rocksdb" -type f -name *.h`; do \
		install -C -m 644 $$header $(INSTALL_PATH)/$$header; \
	done

install-static: install-headers $(LIBRARY)
	install -C -m 755 $(LIBRARY) $(INSTALL_PATH)/lib

install-shared: install-headers $(SHARED4)
	install -C -m 755 $(SHARED4) $(INSTALL_PATH)/lib && \
		ln -fs $(SHARED4) $(INSTALL_PATH)/lib/$(SHARED3) && \
		ln -fs $(SHARED4) $(INSTALL_PATH)/lib/$(SHARED2) && \
		ln -fs $(SHARED4) $(INSTALL_PATH)/lib/$(SHARED1)

# install static by default + install shared if it exists
install: install-static
	[ -e $(SHARED4) ] && $(MAKE) install-shared || :

#-------------------------------------------------


# ---------------------------------------------------------------------------
# Jni stuff
# ---------------------------------------------------------------------------

JAVA_INCLUDE = -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux
ifeq ($(PLATFORM), OS_SOLARIS)
	ARCH := $(shell isainfo -b)
else ifeq ($(PLATFORM), OS_OPENBSD)
	ifneq (,$(filter amd64 ppc64 ppc64le arm64 aarch64 sparc64, $(MACHINE)))
		ARCH := 64
	else
		ARCH := 32
	endif
else
	ARCH := $(shell getconf LONG_BIT)
endif

ifeq ($(shell ldd /usr/bin/env 2>/dev/null | grep -q musl; echo $$?),0)
        JNI_LIBC = musl
# GNU LibC (or glibc) is so pervasive we can assume it is the default
# else
#        JNI_LIBC = glibc
endif

ifneq ($(origin JNI_LIBC), undefined)
  JNI_LIBC_POSTFIX = -$(JNI_LIBC)
endif

ifneq (,$(filter ppc% arm64 aarch64 sparc64, $(MACHINE)))
	ROCKSDBJNILIB = librocksdbjni-linux-$(MACHINE)$(JNI_LIBC_POSTFIX).so
else
	ROCKSDBJNILIB = librocksdbjni-linux$(ARCH)$(JNI_LIBC_POSTFIX).so
endif
ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-linux$(ARCH)$(JNI_LIBC_POSTFIX).jar
ROCKSDB_JAR_ALL = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH).jar
ROCKSDB_JAVADOCS_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-javadoc.jar
ROCKSDB_SOURCES_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-sources.jar
SHA256_CMD = sha256sum

ZLIB_VER ?= 1.2.11
ZLIB_SHA256 ?= c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1
ZLIB_DOWNLOAD_BASE ?= http://zlib.net
BZIP2_VER ?= 1.0.8
BZIP2_SHA256 ?= ab5a03176ee106d3f0fa90e381da478ddae405918153cca248e682cd0c4a2269
BZIP2_DOWNLOAD_BASE ?= https://sourceware.org/pub/bzip2
SNAPPY_VER ?= 1.1.8
SNAPPY_SHA256 ?= 16b677f07832a612b0836178db7f374e414f94657c138e6993cbfc5dcc58651f
SNAPPY_DOWNLOAD_BASE ?= https://github.com/google/snappy/archive
LZ4_VER ?= 1.9.2
LZ4_SHA256 ?= 658ba6191fa44c92280d4aa2c271b0f4fbc0e34d249578dd05e50e76d0e5efcc
LZ4_DOWNLOAD_BASE ?= https://github.com/lz4/lz4/archive
ZSTD_VER ?= 1.4.4
ZSTD_SHA256 ?= a364f5162c7d1a455cc915e8e3cf5f4bd8b75d09bc0f53965b0c9ca1383c52c8
ZSTD_DOWNLOAD_BASE ?= https://github.com/facebook/zstd/archive
CURL_SSL_OPTS ?= --tlsv1

ifeq ($(PLATFORM), OS_MACOSX)
	ROCKSDBJNILIB = librocksdbjni-osx.jnilib
	ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-osx.jar
	SHA256_CMD = openssl sha256 -r
ifneq ("$(wildcard $(JAVA_HOME)/include/darwin)","")
	JAVA_INCLUDE = -I$(JAVA_HOME)/include -I $(JAVA_HOME)/include/darwin
else
	JAVA_INCLUDE = -I/System/Library/Frameworks/JavaVM.framework/Headers/
endif
endif
ifeq ($(PLATFORM), OS_FREEBSD)
	JAVA_INCLUDE = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/freebsd
	ROCKSDBJNILIB = librocksdbjni-freebsd$(ARCH).so
	ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-freebsd$(ARCH).jar
endif
ifeq ($(PLATFORM), OS_SOLARIS)
	ROCKSDBJNILIB = librocksdbjni-solaris$(ARCH).so
	ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-solaris$(ARCH).jar
	JAVA_INCLUDE = -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/solaris
	SHA256_CMD = digest -a sha256
endif
ifeq ($(PLATFORM), OS_AIX)
	JAVA_INCLUDE = -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/aix
	ROCKSDBJNILIB = librocksdbjni-aix.so
	EXTRACT_SOURCES = gunzip < TAR_GZ | tar xvf -
	SNAPPY_MAKE_TARGET = libsnappy.la
endif
ifeq ($(PLATFORM), OS_OPENBSD)
        JAVA_INCLUDE = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/openbsd
	ROCKSDBJNILIB = librocksdbjni-openbsd$(ARCH).so
        ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-openbsd$(ARCH).jar
endif

libz.a:
	-rm -rf zlib-$(ZLIB_VER)
ifeq (,$(wildcard ./zlib-$(ZLIB_VER).tar.gz))
	curl --fail --output zlib-$(ZLIB_VER).tar.gz --location ${ZLIB_DOWNLOAD_BASE}/zlib-$(ZLIB_VER).tar.gz
endif
	ZLIB_SHA256_ACTUAL=`$(SHA256_CMD) zlib-$(ZLIB_VER).tar.gz | cut -d ' ' -f 1`; \
	if [ "$(ZLIB_SHA256)" != "$$ZLIB_SHA256_ACTUAL" ]; then \
		echo zlib-$(ZLIB_VER).tar.gz checksum mismatch, expected=\"$(ZLIB_SHA256)\" actual=\"$$ZLIB_SHA256_ACTUAL\"; \
		exit 1; \
	fi
	tar xvzf zlib-$(ZLIB_VER).tar.gz
	cd zlib-$(ZLIB_VER) && CFLAGS='-fPIC ${EXTRA_CFLAGS}' LDFLAGS='${EXTRA_LDFLAGS}' ./configure --static && $(MAKE)
	cp zlib-$(ZLIB_VER)/libz.a .

libbz2.a:
	-rm -rf bzip2-$(BZIP2_VER)
ifeq (,$(wildcard ./bzip2-$(BZIP2_VER).tar.gz))
	curl --fail --output bzip2-$(BZIP2_VER).tar.gz --location ${CURL_SSL_OPTS} ${BZIP2_DOWNLOAD_BASE}/bzip2-$(BZIP2_VER).tar.gz
endif
	BZIP2_SHA256_ACTUAL=`$(SHA256_CMD) bzip2-$(BZIP2_VER).tar.gz | cut -d ' ' -f 1`; \
	if [ "$(BZIP2_SHA256)" != "$$BZIP2_SHA256_ACTUAL" ]; then \
		echo bzip2-$(BZIP2_VER).tar.gz checksum mismatch, expected=\"$(BZIP2_SHA256)\" actual=\"$$BZIP2_SHA256_ACTUAL\"; \
		exit 1; \
	fi
	tar xvzf bzip2-$(BZIP2_VER).tar.gz
	cd bzip2-$(BZIP2_VER) && $(MAKE) CFLAGS='-fPIC -O2 -g -D_FILE_OFFSET_BITS=64 ${EXTRA_CFLAGS}' AR='ar ${EXTRA_ARFLAGS}'
	cp bzip2-$(BZIP2_VER)/libbz2.a .

libsnappy.a:
	-rm -rf snappy-$(SNAPPY_VER)
ifeq (,$(wildcard ./snappy-$(SNAPPY_VER).tar.gz))
	curl --fail --output snappy-$(SNAPPY_VER).tar.gz --location ${CURL_SSL_OPTS} ${SNAPPY_DOWNLOAD_BASE}/$(SNAPPY_VER).tar.gz
endif
	SNAPPY_SHA256_ACTUAL=`$(SHA256_CMD) snappy-$(SNAPPY_VER).tar.gz | cut -d ' ' -f 1`; \
	if [ "$(SNAPPY_SHA256)" != "$$SNAPPY_SHA256_ACTUAL" ]; then \
		echo snappy-$(SNAPPY_VER).tar.gz checksum mismatch, expected=\"$(SNAPPY_SHA256)\" actual=\"$$SNAPPY_SHA256_ACTUAL\"; \
		exit 1; \
	fi
	tar xvzf snappy-$(SNAPPY_VER).tar.gz
	mkdir snappy-$(SNAPPY_VER)/build
	cd snappy-$(SNAPPY_VER)/build && CFLAGS='${EXTRA_CFLAGS}' CXXFLAGS='${EXTRA_CXXFLAGS}' LDFLAGS='${EXTRA_LDFLAGS}' cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON .. && $(MAKE) ${SNAPPY_MAKE_TARGET}
	cp snappy-$(SNAPPY_VER)/build/libsnappy.a .

liblz4.a:
	-rm -rf lz4-$(LZ4_VER)
ifeq (,$(wildcard ./lz4-$(LZ4_VER).tar.gz))
	curl --fail --output lz4-$(LZ4_VER).tar.gz --location ${CURL_SSL_OPTS} ${LZ4_DOWNLOAD_BASE}/v$(LZ4_VER).tar.gz
endif
	LZ4_SHA256_ACTUAL=`$(SHA256_CMD) lz4-$(LZ4_VER).tar.gz | cut -d ' ' -f 1`; \
	if [ "$(LZ4_SHA256)" != "$$LZ4_SHA256_ACTUAL" ]; then \
		echo lz4-$(LZ4_VER).tar.gz checksum mismatch, expected=\"$(LZ4_SHA256)\" actual=\"$$LZ4_SHA256_ACTUAL\"; \
		exit 1; \
	fi
	tar xvzf lz4-$(LZ4_VER).tar.gz
	cd lz4-$(LZ4_VER)/lib && $(MAKE) CFLAGS='-fPIC -O2 ${EXTRA_CFLAGS}' all
	cp lz4-$(LZ4_VER)/lib/liblz4.a .

libzstd.a:
	-rm -rf zstd-$(ZSTD_VER)
ifeq (,$(wildcard ./zstd-$(ZSTD_VER).tar.gz))
	curl --fail --output zstd-$(ZSTD_VER).tar.gz --location ${CURL_SSL_OPTS} ${ZSTD_DOWNLOAD_BASE}/v$(ZSTD_VER).tar.gz
endif
	ZSTD_SHA256_ACTUAL=`$(SHA256_CMD) zstd-$(ZSTD_VER).tar.gz | cut -d ' ' -f 1`; \
	if [ "$(ZSTD_SHA256)" != "$$ZSTD_SHA256_ACTUAL" ]; then \
		echo zstd-$(ZSTD_VER).tar.gz checksum mismatch, expected=\"$(ZSTD_SHA256)\" actual=\"$$ZSTD_SHA256_ACTUAL\"; \
		exit 1; \
	fi
	tar xvzf zstd-$(ZSTD_VER).tar.gz
	cd zstd-$(ZSTD_VER)/lib && DESTDIR=. PREFIX= $(MAKE) CFLAGS='-fPIC -O2 ${EXTRA_CFLAGS}' install
	cp zstd-$(ZSTD_VER)/lib/libzstd.a .

# A version of each $(LIBOBJECTS) compiled with -fPIC and a fixed set of static compression libraries
java_static_libobjects = $(patsubst %,jls/%,$(LIB_CC_OBJECTS))
CLEAN_FILES += jls
java_static_all_libobjects = $(java_static_libobjects)

ifneq ($(ROCKSDB_JAVA_NO_COMPRESSION), 1)
JAVA_COMPRESSIONS = libz.a libbz2.a libsnappy.a liblz4.a libzstd.a
endif

JAVA_STATIC_FLAGS = -DZLIB -DBZIP2 -DSNAPPY -DLZ4 -DZSTD
JAVA_STATIC_INCLUDES = -I./zlib-$(ZLIB_VER) -I./bzip2-$(BZIP2_VER) -I./snappy-$(SNAPPY_VER) -I./lz4-$(LZ4_VER)/lib -I./zstd-$(ZSTD_VER)/lib/include

ifeq ($(HAVE_POWER8),1)
JAVA_STATIC_C_LIBOBJECTS = $(patsubst %.c.o,jls/%.c.o,$(LIB_SOURCES_C:.c=.o))
JAVA_STATIC_ASM_LIBOBJECTS = $(patsubst %.S.o,jls/%.S.o,$(LIB_SOURCES_ASM:.S=.o))

java_static_ppc_libobjects = $(JAVA_STATIC_C_LIBOBJECTS) $(JAVA_STATIC_ASM_LIBOBJECTS)

jls/util/crc32c_ppc.o: util/crc32c_ppc.c
	$(AM_V_CC)$(CC) $(CFLAGS) $(JAVA_STATIC_FLAGS) $(JAVA_STATIC_INCLUDES) -c $< -o $@

jls/util/crc32c_ppc_asm.o: util/crc32c_ppc_asm.S
	$(AM_V_CC)$(CC) $(CFLAGS) $(JAVA_STATIC_FLAGS) $(JAVA_STATIC_INCLUDES) -c $< -o $@

java_static_all_libobjects += $(java_static_ppc_libobjects)
endif

$(java_static_libobjects): jls/%.o: %.cc $(JAVA_COMPRESSIONS)
	$(AM_V_CC)mkdir -p $(@D) && $(CXX) $(CXXFLAGS) $(JAVA_STATIC_FLAGS) $(JAVA_STATIC_INCLUDES) -fPIC -c $< -o $@ $(COVERAGEFLAGS)

rocksdbjavastatic: $(java_static_all_libobjects)
	cd java;$(MAKE) javalib;
	rm -f ./java/target/$(ROCKSDBJNILIB)
	$(CXX) $(CXXFLAGS) -I./java/. $(JAVA_INCLUDE) -shared -fPIC \
	  -o ./java/target/$(ROCKSDBJNILIB) $(JNI_NATIVE_SOURCES) \
	  $(java_static_all_libobjects) $(COVERAGEFLAGS) \
	  $(JAVA_COMPRESSIONS) $(JAVA_STATIC_LDFLAGS)
	cd java/target;if [ "$(DEBUG_LEVEL)" == "0" ]; then \
		strip $(STRIPFLAGS) $(ROCKSDBJNILIB); \
	fi
	cd java;jar -cf target/$(ROCKSDB_JAR) HISTORY*.md
	cd java/target;jar -uf $(ROCKSDB_JAR) $(ROCKSDBJNILIB)
	cd java/target/classes;jar -uf ../$(ROCKSDB_JAR) org/rocksdb/*.class org/rocksdb/util/*.class
	cd java/target/apidocs;jar -cf ../$(ROCKSDB_JAVADOCS_JAR) *
	cd java/src/main/java;jar -cf ../../../target/$(ROCKSDB_SOURCES_JAR) org

rocksdbjavastaticrelease: rocksdbjavastatic
	cd java/crossbuild && (vagrant destroy -f || true) && vagrant up linux32 && vagrant halt linux32 && vagrant up linux64 && vagrant halt linux64 && vagrant up linux64-musl && vagrant halt linux64-musl
	cd java;jar -cf target/$(ROCKSDB_JAR_ALL) HISTORY*.md
	cd java/target;jar -uf $(ROCKSDB_JAR_ALL) librocksdbjni-*.so librocksdbjni-*.jnilib
	cd java/target/classes;jar -uf ../$(ROCKSDB_JAR_ALL) org/rocksdb/*.class org/rocksdb/util/*.class

rocksdbjavastaticreleasedocker: rocksdbjavastatic rocksdbjavastaticdockerx86 rocksdbjavastaticdockerx86_64 rocksdbjavastaticdockerx86musl rocksdbjavastaticdockerx86_64musl
	cd java;jar -cf target/$(ROCKSDB_JAR_ALL) HISTORY*.md
	cd java/target;jar -uf $(ROCKSDB_JAR_ALL) librocksdbjni-*.so librocksdbjni-*.jnilib
	cd java/target/classes;jar -uf ../$(ROCKSDB_JAR_ALL) org/rocksdb/*.class org/rocksdb/util/*.class

rocksdbjavastaticdockerx86:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_x86-be --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) evolvedbinary/rocksjava:centos6_x86-be /rocksdb-host/java/crossbuild/docker-build-linux-centos.sh

rocksdbjavastaticdockerx86_64:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_x64-be --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) evolvedbinary/rocksjava:centos6_x64-be /rocksdb-host/java/crossbuild/docker-build-linux-centos.sh

rocksdbjavastaticdockerppc64le:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_ppc64le-be --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) evolvedbinary/rocksjava:centos7_ppc64le-be /rocksdb-host/java/crossbuild/docker-build-linux-centos.sh

rocksdbjavastaticdockerarm64v8:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_arm64v8-be --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) evolvedbinary/rocksjava:centos7_arm64v8-be /rocksdb-host/java/crossbuild/docker-build-linux-centos.sh

rocksdbjavastaticdockerx86musl:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_x86-musl-be --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) evolvedbinary/rocksjava:alpine3_x86-be /rocksdb-host/java/crossbuild/docker-build-linux-centos.sh

rocksdbjavastaticdockerx86_64musl:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_x64-musl-be --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) evolvedbinary/rocksjava:alpine3_x64-be /rocksdb-host/java/crossbuild/docker-build-linux-centos.sh

rocksdbjavastaticdockerppc64lemusl:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_ppc64le-musl-be --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) evolvedbinary/rocksjava:alpine3_ppc64le-be /rocksdb-host/java/crossbuild/docker-build-linux-centos.sh

rocksdbjavastaticdockerarm64v8musl:
	mkdir -p java/target
	docker run --rm --name rocksdb_linux_arm64v8-musl-be --attach stdin --attach stdout --attach stderr --volume $(HOME)/.m2:/root/.m2:ro --volume `pwd`:/rocksdb-host:ro --volume /rocksdb-local-build --volume `pwd`/java/target:/rocksdb-java-target --env DEBUG_LEVEL=$(DEBUG_LEVEL) evolvedbinary/rocksjava:alpine3_arm64v8-be /rocksdb-host/java/crossbuild/docker-build-linux-centos.sh

rocksdbjavastaticpublish: rocksdbjavastaticrelease rocksdbjavastaticpublishcentral

rocksdbjavastaticpublishdocker: rocksdbjavastaticreleasedocker rocksdbjavastaticpublishcentral

rocksdbjavastaticpublishcentral:
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/rocksjni.pom -Dfile=java/target/rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-javadoc.jar -Dclassifier=javadoc
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/rocksjni.pom -Dfile=java/target/rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-sources.jar -Dclassifier=sources
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/rocksjni.pom -Dfile=java/target/rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-linux64.jar -Dclassifier=linux64
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/rocksjni.pom -Dfile=java/target/rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-linux32.jar -Dclassifier=linux32
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/rocksjni.pom -Dfile=java/target/rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-linux64-musl.jar -Dclassifier=linux64-musl
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/rocksjni.pom -Dfile=java/target/rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-linux32-musl.jar -Dclassifier=linux32-musl
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/rocksjni.pom -Dfile=java/target/rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-osx.jar -Dclassifier=osx
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/rocksjni.pom -Dfile=java/target/rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-win64.jar -Dclassifier=win64
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/rocksjni.pom -Dfile=java/target/rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH).jar

# A version of each $(LIBOBJECTS) compiled with -fPIC
ifeq ($(HAVE_POWER8),1)
JAVA_CC_OBJECTS = $(SHARED_CC_OBJECTS)
JAVA_C_OBJECTS = $(SHARED_C_OBJECTS)
JAVA_ASM_OBJECTS = $(SHARED_ASM_OBJECTS)

JAVA_C_LIBOBJECTS = $(patsubst %.c.o,jl/%.c.o,$(JAVA_C_OBJECTS))
JAVA_ASM_LIBOBJECTS = $(patsubst %.S.o,jl/%.S.o,$(JAVA_ASM_OBJECTS))
endif

java_libobjects = $(patsubst %,jl/%,$(LIB_CC_OBJECTS))
CLEAN_FILES += jl
java_all_libobjects = $(java_libobjects)

ifeq ($(HAVE_POWER8),1)
java_ppc_libobjects = $(JAVA_C_LIBOBJECTS) $(JAVA_ASM_LIBOBJECTS)

jl/crc32c_ppc.o: util/crc32c_ppc.c
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@

jl/crc32c_ppc_asm.o: util/crc32c_ppc_asm.S
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@
java_all_libobjects += $(java_ppc_libobjects)
endif

$(java_libobjects): jl/%.o: %.cc
	$(AM_V_CC)mkdir -p $(@D) && $(CXX) $(CXXFLAGS) -fPIC -c $< -o $@ $(COVERAGEFLAGS)



rocksdbjava: $(java_all_libobjects)
	$(AM_V_GEN)cd java;$(MAKE) javalib;
	$(AM_V_at)rm -f ./java/target/$(ROCKSDBJNILIB)
	$(AM_V_at)$(CXX) $(CXXFLAGS) -I./java/. $(JAVA_INCLUDE) -shared -fPIC -o ./java/target/$(ROCKSDBJNILIB) $(JNI_NATIVE_SOURCES) $(java_all_libobjects) $(JAVA_LDFLAGS) $(COVERAGEFLAGS)
	$(AM_V_at)cd java;jar -cf target/$(ROCKSDB_JAR) HISTORY*.md
	$(AM_V_at)cd java/target;jar -uf $(ROCKSDB_JAR) $(ROCKSDBJNILIB)
	$(AM_V_at)cd java/target/classes;jar -uf ../$(ROCKSDB_JAR) org/rocksdb/*.class org/rocksdb/util/*.class

jclean:
	cd java;$(MAKE) clean;

jtest_compile: rocksdbjava
	cd java;$(MAKE) java_test

jtest_run:
	cd java;$(MAKE) run_test

jtest: rocksdbjava
	cd java;$(MAKE) sample;$(MAKE) test;
	$(PYTHON) tools/check_all_python.py # TODO peterd: find a better place for this check in CI targets

jdb_bench:
	cd java;$(MAKE) db_bench;

commit_prereq: build_tools/rocksdb-lego-determinator \
               build_tools/precommit_checker.py
	J=$(J) build_tools/precommit_checker.py unit unit_481 clang_unit release release_481 clang_release tsan asan ubsan lite unit_non_shm
	$(MAKE) clean && $(MAKE) jclean && $(MAKE) rocksdbjava;

# ---------------------------------------------------------------------------
#  	Platform-specific compilation
# ---------------------------------------------------------------------------

ifeq ($(PLATFORM), IOS)
# For iOS, create universal object files to be used on both the simulator and
# a device.
XCODEROOT=$(shell xcode-select -print-path)
PLATFORMSROOT=$(XCODEROOT)/Platforms
SIMULATORROOT=$(PLATFORMSROOT)/iPhoneSimulator.platform/Developer
DEVICEROOT=$(PLATFORMSROOT)/iPhoneOS.platform/Developer
IOSVERSION=$(shell defaults read $(PLATFORMSROOT)/iPhoneOS.platform/version CFBundleShortVersionString)

.cc.o:
	mkdir -p ios-x86/$(dir $@)
	$(CXX) $(CXXFLAGS) -isysroot $(SIMULATORROOT)/SDKs/iPhoneSimulator$(IOSVERSION).sdk -arch i686 -arch x86_64 -c $< -o ios-x86/$@
	mkdir -p ios-arm/$(dir $@)
	xcrun -sdk iphoneos $(CXX) $(CXXFLAGS) -isysroot $(DEVICEROOT)/SDKs/iPhoneOS$(IOSVERSION).sdk -arch armv6 -arch armv7 -arch armv7s -arch arm64 -c $< -o ios-arm/$@
	lipo ios-x86/$@ ios-arm/$@ -create -output $@

.c.o:
	mkdir -p ios-x86/$(dir $@)
	$(CC) $(CFLAGS) -isysroot $(SIMULATORROOT)/SDKs/iPhoneSimulator$(IOSVERSION).sdk -arch i686 -arch x86_64 -c $< -o ios-x86/$@
	mkdir -p ios-arm/$(dir $@)
	xcrun -sdk iphoneos $(CC) $(CFLAGS) -isysroot $(DEVICEROOT)/SDKs/iPhoneOS$(IOSVERSION).sdk -arch armv6 -arch armv7 -arch armv7s -arch arm64 -c $< -o ios-arm/$@
	lipo ios-x86/$@ ios-arm/$@ -create -output $@

else
ifeq ($(HAVE_POWER8),1)
util/crc32c_ppc.o: util/crc32c_ppc.c
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@

util/crc32c_ppc_asm.o: util/crc32c_ppc_asm.S
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@
endif
.cc.o:
	$(AM_V_CC)$(CXX) $(CXXFLAGS) -c $< -o $@ $(COVERAGEFLAGS)

.cpp.o:
	$(AM_V_CC)$(CXX) $(CXXFLAGS) -c $< -o $@ $(COVERAGEFLAGS)

.c.o:
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@
endif
# ---------------------------------------------------------------------------
#  	Source files dependencies detection
# ---------------------------------------------------------------------------

all_sources = $(LIB_SOURCES) $(MAIN_SOURCES) $(MOCK_LIB_SOURCES) $(TOOL_LIB_SOURCES) $(BENCH_LIB_SOURCES) $(TEST_LIB_SOURCES) $(ANALYZER_LIB_SOURCES) $(STRESS_LIB_SOURCES)
DEPFILES = $(all_sources:.cc=.cc.d)

ifeq ($(USE_FOLLY_DISTRIBUTED_MUTEX),1)
  DEPFILES += $(FOLLY_SOURCES:.cpp=.cpp.d)
endif

# Add proper dependency support so changing a .h file forces a .cc file to
# rebuild.

# The .d file indicates .cc file's dependencies on .h files. We generate such
# dependency by g++'s -MM option, whose output is a make dependency rule.
%.cc.d: %.cc
	@$(CXX) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) \
	  -MM -MT'$@' -MT'$(<:.cc=.o)' "$<" -o '$@'

%.cpp.d: %.cpp
	@$(CXX) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) \
	  -MM -MT'$@' -MT'$(<:.cpp=.o)' "$<" -o '$@'

ifeq ($(HAVE_POWER8),1)
DEPFILES_C = $(LIB_SOURCES_C:.c=.c.d)
DEPFILES_ASM = $(LIB_SOURCES_ASM:.S=.S.d)

%.c.d: %.c
	@$(CXX) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) \
	  -MM -MT'$@' -MT'$(<:.c=.o)' "$<" -o '$@'

%.S.d: %.S
	@$(CXX) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) \
	  -MM -MT'$@' -MT'$(<:.S=.o)' "$<" -o '$@'

$(DEPFILES_C): %.c.d

$(DEPFILES_ASM): %.S.d
depend: $(DEPFILES) $(DEPFILES_C) $(DEPFILES_ASM)
else
depend: $(DEPFILES)
endif

# if the make goal is either "clean" or "format", we shouldn't
# try to import the *.d files.
# TODO(kailiu) The unfamiliarity of Make's conditions leads to the ugly
# working solution.
ifneq ($(MAKECMDGOALS),clean)
ifneq ($(MAKECMDGOALS),format)
ifneq ($(MAKECMDGOALS),jclean)
ifneq ($(MAKECMDGOALS),jtest)
ifneq ($(MAKECMDGOALS),package)
ifneq ($(MAKECMDGOALS),analyze)
-include $(DEPFILES)
endif
endif
endif
endif
endif
endif
