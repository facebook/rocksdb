# Copyright (c) 2011 The LevelDB Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

# Inherit some settings from environment variables, if available

#-----------------------------------------------

BASH_EXISTS := $(shell which bash)
SHELL := $(shell which bash)
# Default to python3. Some distros like CentOS 8 do not have `python`.
ifeq ($(origin PYTHON), undefined)
	PYTHON := $(shell which python3 || which python || echo python3)
endif
export PYTHON

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

# LIB_MODE says whether or not to use/build "shared" or "static" libraries.
# Mode "static" means to link against static libraries (.a)
# Mode "shared" means to link against shared libraries (.so, .sl, .dylib, etc)
#
# Set the default LIB_MODE to static
LIB_MODE?=static

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
	LIB_MODE=shared
	DEBUG_LEVEL=0
endif

ifeq ($(MAKECMDGOALS),install-shared)
	LIB_MODE=shared
	DEBUG_LEVEL=0
endif

ifeq ($(MAKECMDGOALS),static_lib)
	DEBUG_LEVEL=0
	LIB_MODE=static
endif

ifeq ($(MAKECMDGOALS),install-static)
	DEBUG_LEVEL=0
	LIB_MODE=static
endif

ifeq ($(MAKECMDGOALS),install)
	DEBUG_LEVEL=0
endif


ifneq ($(findstring jtest, $(MAKECMDGOALS)),)
	OBJ_DIR=jl
	LIB_MODE=shared
endif

ifneq ($(findstring rocksdbjava, $(MAKECMDGOALS)),)
	LIB_MODE=shared
        ifneq ($(findstring rocksdbjavastatic, $(MAKECMDGOALS)),)
		OBJ_DIR=jls
	        ifneq ($(DEBUG_LEVEL),2)
	            DEBUG_LEVEL=0
                endif
                ifeq ($(MAKECMDGOALS),rocksdbjavastaticpublish)
	            DEBUG_LEVEL=0
                endif
	else
		OBJ_DIR=jl
	endif
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
	OPTIMIZE_LEVEL ?= -O2
else
	OPTIMIZE_LEVEL ?= -Os
endif
endif
# `OPTIMIZE_LEVEL` is empty when the user does not set it and `DEBUG_LEVEL=2`.
# In that case, the compiler default (`-O0` for gcc and clang) will be used.
OPT += $(OPTIMIZE_LEVEL)

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

# if we're compiling for shared libraries, add the shared flags
ifeq ($(LIB_MODE),shared)
CXXFLAGS += $(PLATFORM_SHARED_CFLAGS) -DROCKSDB_DLL
CFLAGS +=  $(PLATFORM_SHARED_CFLAGS) -DROCKSDB_DLL
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

ifdef ASSERT_STATUS_CHECKED
ifeq ($(filter -DROCKSDB_ASSERT_STATUS_CHECKED,$(OPT)),)
	OPT += -DROCKSDB_ASSERT_STATUS_CHECKED
endif
endif

$(warning Warning: Compiling in debug mode. Don't use the resulting binary in production)
endif

# `USE_LTO=1` enables link-time optimizations. Among other things, this enables
# more devirtualization opportunities and inlining across translation units.
# This can save significant overhead introduced by RocksDB's pluggable
# interfaces/internal abstractions, like in the iterator hierarchy. It works
# better when combined with profile-guided optimizations (not currently
# supported natively in Makefile).
ifeq ($(USE_LTO), 1)
	CXXFLAGS += -flto
	LDFLAGS += -flto -fuse-linker-plugin
endif

#-----------------------------------------------
include src.mk

AM_DEFAULT_VERBOSITY ?= 0

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

AM_V_CCLD = $(am__v_CCLD_$(V))
am__v_CCLD_ = $(am__v_CCLD_$(AM_DEFAULT_VERBOSITY))
ifneq ($(SKIP_LINK), 1)
am__v_CCLD_0 = @echo "  CCLD    " $@;
am__v_CCLD_1 =
else
am__v_CCLD_0 = @echo "  !CCLD   " $@; true skip
am__v_CCLD_1 = true skip
endif
AM_V_AR = $(am__v_AR_$(V))
am__v_AR_ = $(am__v_AR_$(AM_DEFAULT_VERBOSITY))
am__v_AR_0 = @echo "  AR      " $@;
am__v_AR_1 =

ifdef ROCKSDB_USE_LIBRADOS
LIB_SOURCES += utilities/env_librados.cc
LDFLAGS += -lrados
endif

AM_LINK = $(AM_V_CCLD)$(CXX) -L. $(patsubst lib%.a, -l%, $(patsubst lib%.$(PLATFORM_SHARED_EXT), -l%, $^)) $(EXEC_LDFLAGS) -o $@ $(LDFLAGS) $(COVERAGEFLAGS)
AM_SHARE = $(AM_V_CCLD) $(CXX) $(PLATFORM_SHARED_LDFLAGS)$@ -L. $(patsubst lib%.$(PLATFORM_SHARED_EXT), -l%, $^) $(LDFLAGS) -o $@

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
CLEAN_FILES += make_config.mk rocksdb.pc

ifeq ($(V), 1)
$(info $(shell uname -a))
$(info $(shell $(CC) --version))
$(info $(shell $(CXX) --version))
endif

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

ifeq ($(LIB_MODE),shared)
# So that binaries are executable from build location, in addition to install location
EXEC_LDFLAGS += -Wl,-rpath -Wl,'$$ORIGIN'
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

ifndef GTEST_THROW_ON_FAILURE
	export GTEST_THROW_ON_FAILURE=1
endif
ifndef GTEST_HAS_EXCEPTIONS
	export GTEST_HAS_EXCEPTIONS=1
endif

GTEST_DIR = third-party/gtest-1.8.1/fused-src
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
ifdef TEST_UINT128_COMPAT
  PLATFORM_CCFLAGS += -DTEST_UINT128_COMPAT=1
  PLATFORM_CXXFLAGS += -DTEST_UINT128_COMPAT=1
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

OBJ_DIR?=.
LIB_OBJECTS = $(patsubst %.cc, $(OBJ_DIR)/%.o, $(LIB_SOURCES))
ifeq ($(HAVE_POWER8),1)
LIB_OBJECTS += $(patsubst %.c, $(OBJ_DIR)/%.o, $(LIB_SOURCES_C))
LIB_OBJECTS += $(patsubst %.S, $(OBJ_DIR)/%.o, $(LIB_SOURCES_ASM))
endif

ifeq ($(USE_FOLLY_DISTRIBUTED_MUTEX),1)
  LIB_OBJECTS += $(patsubst %.cpp, $(OBJ_DIR)/%.o, $(FOLLY_SOURCES))
endif

GTEST = $(OBJ_DIR)/$(GTEST_DIR)/gtest/gtest-all.o
TESTUTIL = $(OBJ_DIR)/test_util/testutil.o
TESTHARNESS = $(OBJ_DIR)/test_util/testharness.o $(TESTUTIL) $(GTEST)
VALGRIND_ERROR = 2
VALGRIND_VER := $(join $(VALGRIND_VER),valgrind)

VALGRIND_OPTS = --error-exitcode=$(VALGRIND_ERROR) --leak-check=full

TEST_OBJECTS = $(patsubst %.cc, $(OBJ_DIR)/%.o, $(TEST_LIB_SOURCES) $(MOCK_LIB_SOURCES)) $(GTEST)
BENCH_OBJECTS = $(patsubst %.cc, $(OBJ_DIR)/%.o, $(BENCH_LIB_SOURCES))
TOOL_OBJECTS = $(patsubst %.cc, $(OBJ_DIR)/%.o, $(TOOL_LIB_SOURCES))
ANALYZE_OBJECTS = $(patsubst %.cc, $(OBJ_DIR)/%.o, $(ANALYZER_LIB_SOURCES))
STRESS_OBJECTS =  $(patsubst %.cc, $(OBJ_DIR)/%.o, $(STRESS_LIB_SOURCES))

ALL_SOURCES  = $(LIB_SOURCES) $(TEST_LIB_SOURCES) $(MOCK_LIB_SOURCES) $(GTEST_DIR)/gtest/gtest-all.cc
ALL_SOURCES += $(TOOL_LIB_SOURCES) $(BENCH_LIB_SOURCES) $(ANALYZER_LIB_SOURCES) $(STRESS_LIB_SOURCES)
ALL_SOURCES += $(TEST_MAIN_SOURCES) $(TOOL_MAIN_SOURCES) $(BENCH_MAIN_SOURCES)

TESTS = $(patsubst %.cc, %, $(notdir $(TEST_MAIN_SOURCES)))
TESTS += $(patsubst %.c, %, $(notdir $(TEST_MAIN_SOURCES_C)))

ifeq ($(USE_FOLLY_DISTRIBUTED_MUTEX),1)
	TESTS += folly_synchronization_distributed_mutex_test
	ALL_SOURCES += third-party/folly/folly/synchronization/test/DistributedMutexTest.cc
endif

PARALLEL_TEST = \
	backupable_db_test \
	db_bloom_filter_test \
	db_compaction_filter_test \
	db_compaction_test \
	db_merge_operator_test \
	db_sst_test \
	db_test \
	db_test2 \
	db_universal_compaction_test \
	db_wal_test \
	column_family_test \
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
ifdef ASSERT_STATUS_CHECKED
# This is a new check for which we will add support incrementally. This
# list can be removed once support is fully added.
	TESTS_PASSING_ASC = \
		arena_test \
		autovector_test \
		cache_test \
		lru_cache_test \
		blob_file_addition_test \
		blob_file_builder_test \
		blob_file_garbage_test \
		bloom_test \
		cassandra_format_test \
		cassandra_row_merge_test \
		cassandra_serialize_test \
		cleanable_test \
		coding_test \
		crc32c_test \
		dbformat_test \
		defer_test \
		filename_test \
		dynamic_bloom_test \
		env_basic_test \
		env_test \
		env_logger_test \
		event_logger_test \
		file_indexer_test \
		hash_table_test \
		hash_test \
		heap_test \
		histogram_test \
		inlineskiplist_test \
		io_posix_test \
		iostats_context_test \
		memkind_kmem_allocator_test \
		merger_test \
		mock_env_test \
		object_registry_test \
		options_settable_test \
		options_test \
		random_test \
		range_del_aggregator_test \
		range_tombstone_fragmenter_test \
		repeatable_thread_test \
		skiplist_test \
		slice_test \
		sst_dump_test \
		statistics_test \
		thread_local_test \
		env_timed_test \
		filelock_test \
		timer_queue_test \
		timer_test \
		util_merge_operators_test \
		block_cache_trace_analyzer_test \
		block_cache_tracer_test \
		cache_simulator_test \
		sim_cache_test \
		version_edit_test \
		work_queue_test \
		write_controller_test \

ifeq ($(USE_FOLLY_DISTRIBUTED_MUTEX),1)
TESTS_PASSING_ASC += folly_synchronization_distributed_mutex_test
endif

	# Enable building all unit tests, but use check_some to run only tests
	# known to pass ASC (ASSERT_STATUS_CHECKED)
	SUBSET := $(TESTS_PASSING_ASC)
	# Alternate: only build unit tests known to pass ASC, and run them
	# with make check
	#TESTS := $(filter $(TESTS_PASSING_ASC),$(TESTS))
	#PARALLEL_TEST := $(filter $(TESTS_PASSING_ASC),$(PARALLEL_TEST))
else
	SUBSET := $(TESTS)
endif
ifdef ROCKSDBTESTS_START
        SUBSET := $(shell echo $(SUBSET) | sed 's/^.*$(ROCKSDBTESTS_START)/$(ROCKSDBTESTS_START)/')
endif

ifdef ROCKSDBTESTS_END
        SUBSET := $(shell echo $(SUBSET) | sed 's/$(ROCKSDBTESTS_END).*//')
endif

# bench_tool_analyer main is in bench_tool_analyzer_tool, or this would be simpler...
TOOLS = $(patsubst %.cc, %, $(notdir $(patsubst %_tool.cc, %.cc, $(TOOLS_MAIN_SOURCES))))

TEST_LIBS = \
	librocksdb_env_basic_test.a

# TODO: add back forward_iterator_bench, after making it build in all environemnts.
BENCHMARKS = $(patsubst %.cc, %, $(notdir $(BENCH_MAIN_SOURCES)))

# if user didn't config LIBNAME, set the default
ifeq ($(LIBNAME),)
  LIBNAME=librocksdb
# we should only run rocksdb in production with DEBUG_LEVEL 0
ifneq ($(DEBUG_LEVEL),0)
  LIBDEBUG=_debug
endif
endif
STATIC_LIBRARY = ${LIBNAME}$(LIBDEBUG).a
STATIC_TEST_LIBRARY =  ${LIBNAME}_test$(LIBDEBUG).a
STATIC_TOOLS_LIBRARY = ${LIBNAME}_tools$(LIBDEBUG).a
STATIC_STRESS_LIBRARY = ${LIBNAME}_stress$(LIBDEBUG).a

ALL_STATIC_LIBS = $(STATIC_LIBRARY) $(STATIC_TEST_LIBRARY) $(STATIC_TOOLS_LIBRARY) $(STATIC_STRESS_LIBRARY)

SHARED_TEST_LIBRARY =  ${LIBNAME}_test$(LIBDEBUG).$(PLATFORM_SHARED_EXT)
SHARED_TOOLS_LIBRARY = ${LIBNAME}_tools$(LIBDEBUG).$(PLATFORM_SHARED_EXT)
SHARED_STRESS_LIBRARY = ${LIBNAME}_stress$(LIBDEBUG).$(PLATFORM_SHARED_EXT)

ALL_SHARED_LIBS = $(SHARED1) $(SHARED2) $(SHARED3) $(SHARED4) $(SHARED_TEST_LIBRARY) $(SHARED_TOOLS_LIBRARY) $(SHARED_STRESS_LIBRARY)

ifeq ($(LIB_MODE),shared)
LIBRARY=$(SHARED1)
TEST_LIBRARY=$(SHARED_TEST_LIBRARY)
TOOLS_LIBRARY=$(SHARED_TOOLS_LIBRARY)
STRESS_LIBRARY=$(SHARED_STRESS_LIBRARY)
CLOUD_LIBRARY=$(SHARED_CLOUD_LIBRARY)
else
LIBRARY=$(STATIC_LIBRARY)
TEST_LIBRARY=$(STATIC_TEST_LIBRARY)
TOOLS_LIBRARY=$(STATIC_TOOLS_LIBRARY)
STRESS_LIBRARY=$(STATIC_STRESS_LIBRARY)
endif

ROCKSDB_MAJOR = $(shell egrep "ROCKSDB_MAJOR.[0-9]" include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_MINOR = $(shell egrep "ROCKSDB_MINOR.[0-9]" include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_PATCH = $(shell egrep "ROCKSDB_PATCH.[0-9]" include/rocksdb/version.h | cut -d ' ' -f 3)

default: all

#-----------------------------------------------
# Create platform independent shared libraries.
#-----------------------------------------------
ifneq ($(PLATFORM_SHARED_EXT),)

ifneq ($(PLATFORM_SHARED_VERSIONED),true)
SHARED1 = ${LIBNAME}$(LIBDEBUG).$(PLATFORM_SHARED_EXT)
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
SHARED_OSX = $(LIBNAME)$(LIBDEBUG).$(SHARED_MAJOR)
SHARED2 = $(SHARED_OSX).$(PLATFORM_SHARED_EXT)
SHARED3 = $(SHARED_OSX).$(SHARED_MINOR).$(PLATFORM_SHARED_EXT)
SHARED4 = $(SHARED_OSX).$(SHARED_MINOR).$(SHARED_PATCH).$(PLATFORM_SHARED_EXT)
else
SHARED2 = $(SHARED1).$(SHARED_MAJOR)
SHARED3 = $(SHARED1).$(SHARED_MAJOR).$(SHARED_MINOR)
SHARED4 = $(SHARED1).$(SHARED_MAJOR).$(SHARED_MINOR).$(SHARED_PATCH)
endif # MACOSX
SHARED = $(SHARED1) $(SHARED2) $(SHARED3) $(SHARED4)
$(SHARED1): $(SHARED4) $(SHARED2)
	ln -fs $(SHARED4) $(SHARED1)
$(SHARED2): $(SHARED4) $(SHARED3)
	ln -fs $(SHARED4) $(SHARED2)
$(SHARED3): $(SHARED4)
	ln -fs $(SHARED4) $(SHARED3)

endif   # PLATFORM_SHARED_VERSIONED
$(SHARED4): $(LIB_OBJECTS)
	$(AM_V_CCLD) $(CXX) $(PLATFORM_SHARED_LDFLAGS)$(SHARED3) $(LIB_OBJECTS) $(LDFLAGS) -o $@
endif  # PLATFORM_SHARED_EXT

.PHONY: blackbox_crash_test check clean coverage crash_test ldb_tests package \
	release tags tags0 valgrind_check whitebox_crash_test format static_lib shared_lib all \
	dbg rocksdbjavastatic rocksdbjava gen-pc install install-static install-shared uninstall \
	analyze tools tools_lib \
	blackbox_crash_test_with_atomic_flush whitebox_crash_test_with_atomic_flush  \
	blackbox_crash_test_with_txn whitebox_crash_test_with_txn \
	blackbox_crash_test_with_best_efforts_recovery


all: $(LIBRARY) $(BENCHMARKS) tools tools_lib test_libs $(TESTS)

all_but_some_tests: $(LIBRARY) $(BENCHMARKS) tools tools_lib test_libs $(SUBSET)

static_lib: $(STATIC_LIBRARY)

shared_lib: $(SHARED)

stress_lib: $(STRESS_LIBRARY)

tools: $(TOOLS)

tools_lib: $(TOOLS_LIBRARY)

test_libs: $(TEST_LIBS)

benchmarks: $(BENCHMARKS)

dbg: $(LIBRARY) $(BENCHMARKS) tools $(TESTS)

# creates library and programs
release: clean
	LIB_MODE=$(LIB_MODE) DEBUG_LEVEL=0 $(MAKE) $(LIBRARY) tools db_bench

coverage: clean
	COVERAGEFLAGS="-fprofile-arcs -ftest-coverage" LDFLAGS+="-lgcov" $(MAKE) J=1 all check
	cd coverage && ./coverage_test.sh
        # Delete intermediate files
	$(FIND) . -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm -f {} \;

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
	$(AM_V_at)$(FIND) t -type f -name 'run-*' -exec rm -f {} \;
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
EXCLUDE_TESTS_REGEX ?= "^$"

ifeq ($(PRINT_PARALLEL_OUTPUTS), 1)
	parallel_com = '{}'
else
	parallel_com = '{} >& t/log-{/}'
endif

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
	  | grep -E -v '$(EXCLUDE_TESTS_REGEX)'					\
	  | build_tools/gnu_parallel -j$(J) --plain --joblog=LOG $$eta --gnu  $(parallel_com) ; \
	parallel_retcode=$$? ; \
	awk '{ if ($$7 != 0 || $$8 != 0) { if ($$7 == "Exitval") { h = $$0; } else { if (!f) print h; print; f = 1 } } } END { if(f) exit 1; }' < LOG ; \
	awk_retcode=$$?; \
	if [ $$parallel_retcode -ne 0 ] || [ $$awk_retcode -ne 0 ] ; then exit 1 ; fi

valgrind-exclude-regexp = InlineSkipTest.ConcurrentInsert|TransactionStressTest.DeadlockStress|DBCompactionTest.SuggestCompactRangeNoTwoLevel0Compactions|BackupableDBTest.RateLimiting|DBTest.CloseSpeedup|DBTest.ThreadStatusFlush|DBTest.RateLimitingTest|DBTest.EncodeDecompressedBlockSizeTest|FaultInjectionTest.UninstalledCompaction|HarnessTest.Randomized|ExternalSSTFileTest.CompactDuringAddFileRandom|ExternalSSTFileTest.IngestFileWithGlobalSeqnoRandomized|MySQLStyleTransactionTest.TransactionStressTest

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
	  | grep -E -v '$(valgrind-exclude-regexp)'					\
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

dump-log:
	bash -c '$(quoted_perl_command)' < LOG

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
ifndef SKIP_FORMAT_BUCK_CHECKS
	$(MAKE) check-format
	$(MAKE) check-buck-targets
endif

# TODO add ldb_tests
check_some: $(SUBSET)
	for t in $(SUBSET); do echo "===== Running $$t (`date`)"; ./$$t || exit 1; done

.PHONY: ldb_tests
ldb_tests: ldb
	$(PYTHON) tools/ldb_test.py

crash_test: whitebox_crash_test blackbox_crash_test

crash_test_with_atomic_flush: whitebox_crash_test_with_atomic_flush blackbox_crash_test_with_atomic_flush

crash_test_with_txn: whitebox_crash_test_with_txn blackbox_crash_test_with_txn

crash_test_with_best_efforts_recovery: blackbox_crash_test_with_best_efforts_recovery

blackbox_crash_test: db_stress
	$(PYTHON) -u tools/db_crashtest.py --simple blackbox $(CRASH_TEST_EXT_ARGS)
	$(PYTHON) -u tools/db_crashtest.py blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_atomic_flush: db_stress
	$(PYTHON) -u tools/db_crashtest.py --cf_consistency blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_txn: db_stress
	$(PYTHON) -u tools/db_crashtest.py --txn blackbox $(CRASH_TEST_EXT_ARGS)

blackbox_crash_test_with_best_efforts_recovery: db_stress
	$(PYTHON) -u tools/db_crashtest.py --test_best_efforts_recovery blackbox $(CRASH_TEST_EXT_ARGS)

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

asan_check: clean
	COMPILE_WITH_ASAN=1 $(MAKE) check -j32
	$(MAKE) clean

asan_crash_test: clean
	COMPILE_WITH_ASAN=1 $(MAKE) crash_test
	$(MAKE) clean

whitebox_asan_crash_test: clean
	COMPILE_WITH_ASAN=1 $(MAKE) whitebox_crash_test
	$(MAKE) clean

blackbox_asan_crash_test: clean
	COMPILE_WITH_ASAN=1 $(MAKE) blackbox_crash_test
	$(MAKE) clean

asan_crash_test_with_atomic_flush: clean
	COMPILE_WITH_ASAN=1 $(MAKE) crash_test_with_atomic_flush
	$(MAKE) clean

asan_crash_test_with_txn: clean
	COMPILE_WITH_ASAN=1 $(MAKE) crash_test_with_txn
	$(MAKE) clean

asan_crash_test_with_best_efforts_recovery: clean
	COMPILE_WITH_ASAN=1 $(MAKE) crash_test_with_best_efforts_recovery
	$(MAKE) clean

ubsan_check: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) check -j32
	$(MAKE) clean

ubsan_crash_test: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) crash_test
	$(MAKE) clean

whitebox_ubsan_crash_test: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) whitebox_crash_test
	$(MAKE) clean

blackbox_ubsan_crash_test: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) blackbox_crash_test
	$(MAKE) clean

ubsan_crash_test_with_atomic_flush: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) crash_test_with_atomic_flush
	$(MAKE) clean

ubsan_crash_test_with_txn: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) crash_test_with_txn
	$(MAKE) clean

ubsan_crash_test_with_best_efforts_recovery: clean
	COMPILE_WITH_UBSAN=1 $(MAKE) crash_test_with_best_efforts_recovery
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

unity.a: $(OBJ_DIR)/unity.o
	$(AM_V_AR)rm -f $@
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $(OBJ_DIR)/unity.o


# try compiling db_test with unity
unity_test: $(OBJ_DIR)/db/db_basic_test.o $(OBJ_DIR)/db/db_test_util.o $(TEST_OBJECTS) $(TOOL_OBJECTS) unity.a
	$(AM_LINK)
	./unity_test

rocksdb.h rocksdb.cc: build_tools/amalgamate.py Makefile $(LIB_SOURCES) unity.cc
	build_tools/amalgamate.py -I. -i./include unity.cc -x include/rocksdb/c.h -H rocksdb.h -o rocksdb.cc

clean: clean-ext-libraries-all clean-rocks clean-rocksjava

clean-not-downloaded: clean-ext-libraries-bin clean-rocks clean-not-downloaded-rocksjava

clean-rocks:
	echo shared=$(ALL_SHARED_LIBS)
	echo static=$(ALL_STATIC_LIBS)
	rm -f $(BENCHMARKS) $(TOOLS) $(TESTS) $(PARALLEL_TEST) $(ALL_STATIC_LIBS) $(ALL_SHARED_LIBS)
	rm -rf $(CLEAN_FILES) ios-x86 ios-arm scan_build_report
	$(FIND) . -name "*.[oda]" -exec rm -f {} \;
	$(FIND) . -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm -f {} \;

clean-rocksjava:
	rm -rf jl jls
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
$(STATIC_LIBRARY): $(LIB_OBJECTS)
	$(AM_V_AR)rm -f $@ $(SHARED1) $(SHARED2) $(SHARED3) $(SHARED4)
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $(LIB_OBJECTS)

$(STATIC_TEST_LIBRARY): $(TEST_OBJECTS)
	$(AM_V_AR)rm -f $@ $(SHARED_TEST_LIBRARY)
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $^

$(STATIC_TOOLS_LIBRARY): $(BENCH_OBJECTS) $(TOOL_OBJECTS)
	$(AM_V_AR)rm -f $@ $(SHARED_TOOLS_LIBRARY)
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $^

$(STATIC_STRESS_LIBRARY): $(ANALYZE_OBJECTS) $(STRESS_OBJECTS)
	$(AM_V_AR)rm -f $@ $(SHARED_STRESS_LIBRARY)
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $^

$(SHARED_TEST_LIBRARY): $(TEST_OBJECTS) $(SHARED1)
	$(AM_V_AR)rm -f $@ $(STATIC_TEST_LIBRARY)
	$(AM_SHARE)

$(SHARED_TOOLS_LIBRARY): $(TOOL_OBJECTS) $(SHARED1)
	$(AM_V_AR)rm -f $@ $(STATIC_TOOLS_LIBRARY)
	$(AM_SHARE)

$(SHARED_STRESS_LIBRARY): $(ANALYZE_OBJECTS) $(STRESS_OBJECTS) $(SHARED_TOOLS_LIBRARY) $(SHARED1)
	$(AM_V_AR)rm -f $@ $(STATIC_STRESS_LIBRARY)
	$(AM_SHARE)

librocksdb_env_basic_test.a: $(OBJ_DIR)/env/env_basic_test.o $(LIB_OBJECTS) $(TESTHARNESS)
	$(AM_V_AR)rm -f $@
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $^

db_bench: $(OBJ_DIR)/tools/db_bench.o $(BENCH_OBJECTS) $(TESTUTIL) $(LIBRARY)
	$(AM_LINK)

trace_analyzer: $(OBJ_DIR)/tools/trace_analyzer.o $(ANALYZE_OBJECTS) $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_cache_trace_analyzer: $(OBJ_DIR)/tools/block_cache_analyzer/block_cache_trace_analyzer_tool.o $(ANALYZE_OBJECTS) $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

ifeq ($(USE_FOLLY_DISTRIBUTED_MUTEX),1)
folly_synchronization_distributed_mutex_test: $(OBJ_DIR)/third-party/folly/folly/synchronization/test/DistributedMutexTest.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)
endif

cache_bench: $(OBJ_DIR)/cache/cache_bench.o $(LIBRARY)
	$(AM_LINK)

persistent_cache_bench: $(OBJ_DIR)/utilities/persistent_cache/persistent_cache_bench.o $(LIBRARY)
	$(AM_LINK)

memtablerep_bench: $(OBJ_DIR)/memtable/memtablerep_bench.o $(LIBRARY)
	$(AM_LINK)

filter_bench: $(OBJ_DIR)/util/filter_bench.o $(LIBRARY)
	$(AM_LINK)

db_stress: $(OBJ_DIR)/db_stress_tool/db_stress.o $(STRESS_LIBRARY) $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_stress: $(OBJ_DIR)/tools/write_stress.o $(LIBRARY)
	$(AM_LINK)

db_sanity_test: $(OBJ_DIR)/tools/db_sanity_test.o $(LIBRARY)
	$(AM_LINK)

db_repl_stress: $(OBJ_DIR)/tools/db_repl_stress.o $(LIBRARY)
	$(AM_LINK)

arena_test: $(OBJ_DIR)/memory/arena_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

memkind_kmem_allocator_test: memory/memkind_kmem_allocator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

autovector_test: $(OBJ_DIR)/util/autovector_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

column_family_test: $(OBJ_DIR)/db/column_family_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

table_properties_collector_test: $(OBJ_DIR)/db/table_properties_collector_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

bloom_test: $(OBJ_DIR)/util/bloom_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

dynamic_bloom_test: $(OBJ_DIR)/util/dynamic_bloom_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

c_test: $(OBJ_DIR)/db/c_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cache_test: $(OBJ_DIR)/cache/cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

coding_test: $(OBJ_DIR)/util/coding_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

hash_test: $(OBJ_DIR)/util/hash_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

random_test: $(OBJ_DIR)/util/random_test.o  $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

option_change_migration_test: $(OBJ_DIR)/utilities/option_change_migration/option_change_migration_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

stringappend_test: $(OBJ_DIR)/utilities/merge_operators/string_append/stringappend_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cassandra_format_test: $(OBJ_DIR)/utilities/cassandra/cassandra_format_test.o $(OBJ_DIR)/utilities/cassandra/test_utils.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cassandra_functional_test: $(OBJ_DIR)/utilities/cassandra/cassandra_functional_test.o $(OBJ_DIR)/utilities/cassandra/test_utils.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cassandra_row_merge_test: $(OBJ_DIR)/utilities/cassandra/cassandra_row_merge_test.o $(OBJ_DIR)/utilities/cassandra/test_utils.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cassandra_serialize_test: $(OBJ_DIR)/utilities/cassandra/cassandra_serialize_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

hash_table_test: $(OBJ_DIR)/utilities/persistent_cache/hash_table_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

histogram_test: $(OBJ_DIR)/monitoring/histogram_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

thread_local_test: $(OBJ_DIR)/util/thread_local_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

work_queue_test: $(OBJ_DIR)/util/work_queue_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

corruption_test: $(OBJ_DIR)/db/corruption_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

crc32c_test: $(OBJ_DIR)/util/crc32c_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

slice_test: $(OBJ_DIR)/util/slice_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

slice_transform_test: $(OBJ_DIR)/util/slice_transform_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_basic_test: $(OBJ_DIR)/db/db_basic_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_with_timestamp_basic_test: $(OBJ_DIR)/db/db_with_timestamp_basic_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_with_timestamp_compaction_test: db/db_with_timestamp_compaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_encryption_test: $(OBJ_DIR)/db/db_encryption_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_test: $(OBJ_DIR)/db/db_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_test2: $(OBJ_DIR)/db/db_test2.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_logical_block_size_cache_test: $(OBJ_DIR)/db/db_logical_block_size_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_blob_index_test: $(OBJ_DIR)/db/blob/db_blob_index_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_block_cache_test: $(OBJ_DIR)/db/db_block_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_bloom_filter_test: $(OBJ_DIR)/db/db_bloom_filter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_log_iter_test: $(OBJ_DIR)/db/db_log_iter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_compaction_filter_test: $(OBJ_DIR)/db/db_compaction_filter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_compaction_test: $(OBJ_DIR)/db/db_compaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_dynamic_level_test: $(OBJ_DIR)/db/db_dynamic_level_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_flush_test: $(OBJ_DIR)/db/db_flush_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_inplace_update_test: $(OBJ_DIR)/db/db_inplace_update_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_iterator_test: $(OBJ_DIR)/db/db_iterator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_memtable_test: $(OBJ_DIR)/db/db_memtable_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_merge_operator_test: $(OBJ_DIR)/db/db_merge_operator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_merge_operand_test: $(OBJ_DIR)/db/db_merge_operand_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_options_test: $(OBJ_DIR)/db/db_options_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_range_del_test: $(OBJ_DIR)/db/db_range_del_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_sst_test: $(OBJ_DIR)/db/db_sst_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_statistics_test: $(OBJ_DIR)/db/db_statistics_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_write_test: $(OBJ_DIR)/db/db_write_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

error_handler_fs_test: $(OBJ_DIR)/db/error_handler_fs_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

external_sst_file_basic_test: $(OBJ_DIR)/db/external_sst_file_basic_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

external_sst_file_test: $(OBJ_DIR)/db/external_sst_file_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

import_column_family_test: $(OBJ_DIR)/db/import_column_family_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_tailing_iter_test: $(OBJ_DIR)/db/db_tailing_iter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_iter_test: $(OBJ_DIR)/db/db_iter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_iter_stress_test: $(OBJ_DIR)/db/db_iter_stress_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_universal_compaction_test: $(OBJ_DIR)/db/db_universal_compaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_wal_test: $(OBJ_DIR)/db/db_wal_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_io_failure_test: $(OBJ_DIR)/db/db_io_failure_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_properties_test: $(OBJ_DIR)/db/db_properties_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_table_properties_test: $(OBJ_DIR)/db/db_table_properties_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

log_write_bench: $(OBJ_DIR)/util/log_write_bench.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK) $(PROFILING_FLAGS)

plain_table_db_test: $(OBJ_DIR)/db/plain_table_db_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

comparator_db_test: $(OBJ_DIR)/db/comparator_db_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

table_reader_bench: $(OBJ_DIR)/table/table_reader_bench.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK) $(PROFILING_FLAGS)

perf_context_test: $(OBJ_DIR)/db/perf_context_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

prefix_test: $(OBJ_DIR)/db/prefix_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

backupable_db_test: $(OBJ_DIR)/utilities/backupable/backupable_db_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

checkpoint_test: $(OBJ_DIR)/utilities/checkpoint/checkpoint_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cache_simulator_test: $(OBJ_DIR)/utilities/simulator_cache/cache_simulator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

sim_cache_test: $(OBJ_DIR)/utilities/simulator_cache/sim_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

env_mirror_test: $(OBJ_DIR)/utilities/env_mirror_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

env_timed_test: $(OBJ_DIR)/utilities/env_timed_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

ifdef ROCKSDB_USE_LIBRADOS
env_librados_test: $(OBJ_DIR)/utilities/env_librados_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)
endif

object_registry_test: $(OBJ_DIR)/utilities/object_registry_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

ttl_test: $(OBJ_DIR)/utilities/ttl/ttl_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_batch_with_index_test: $(OBJ_DIR)/utilities/write_batch_with_index/write_batch_with_index_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

flush_job_test: $(OBJ_DIR)/db/flush_job_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compaction_iterator_test: $(OBJ_DIR)/db/compaction/compaction_iterator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compaction_job_test: $(OBJ_DIR)/db/compaction/compaction_job_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compaction_job_stats_test: $(OBJ_DIR)/db/compaction/compaction_job_stats_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compact_on_deletion_collector_test: $(OBJ_DIR)/utilities/table_properties_collectors/compact_on_deletion_collector_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

wal_manager_test: $(OBJ_DIR)/db/wal_manager_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

wal_edit_test: $(OBJ_DIR)/db/wal_edit_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

dbformat_test: $(OBJ_DIR)/db/dbformat_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

env_basic_test: $(OBJ_DIR)/env/env_basic_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

env_test: $(OBJ_DIR)/env/env_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

io_posix_test: $(OBJ_DIR)/env/io_posix_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

fault_injection_test: $(OBJ_DIR)/db/fault_injection_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

rate_limiter_test: $(OBJ_DIR)/util/rate_limiter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

delete_scheduler_test: $(OBJ_DIR)/file/delete_scheduler_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

filename_test: $(OBJ_DIR)/db/filename_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

random_access_file_reader_test: $(OBJ_DIR)/file/random_access_file_reader_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

file_reader_writer_test: $(OBJ_DIR)/util/file_reader_writer_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_based_filter_block_test: $(OBJ_DIR)/table/block_based/block_based_filter_block_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_based_table_reader_test: table/block_based/block_based_table_reader_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

full_filter_block_test: $(OBJ_DIR)/table/block_based/full_filter_block_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

partitioned_filter_block_test: $(OBJ_DIR)/table/block_based/partitioned_filter_block_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

log_test: $(OBJ_DIR)/db/log_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cleanable_test: $(OBJ_DIR)/table/cleanable_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

table_test: $(OBJ_DIR)/table/table_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_fetcher_test: table/block_fetcher_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_test: $(OBJ_DIR)/table/block_based/block_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

data_block_hash_index_test: $(OBJ_DIR)/table/block_based/data_block_hash_index_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

inlineskiplist_test: $(OBJ_DIR)/memtable/inlineskiplist_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

skiplist_test: $(OBJ_DIR)/memtable/skiplist_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_buffer_manager_test: $(OBJ_DIR)/memtable/write_buffer_manager_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

version_edit_test: $(OBJ_DIR)/db/version_edit_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

version_set_test: $(OBJ_DIR)/db/version_set_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compaction_picker_test: $(OBJ_DIR)/db/compaction/compaction_picker_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

version_builder_test: $(OBJ_DIR)/db/version_builder_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

file_indexer_test: $(OBJ_DIR)/db/file_indexer_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

reduce_levels_test: $(OBJ_DIR)/tools/reduce_levels_test.o $(TOOLS_LIBRARY) $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_batch_test: $(OBJ_DIR)/db/write_batch_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_controller_test: $(OBJ_DIR)/db/write_controller_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

merge_helper_test: $(OBJ_DIR)/db/merge_helper_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

memory_test: $(OBJ_DIR)/utilities/memory/memory_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

merge_test: $(OBJ_DIR)/db/merge_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

merger_test: $(OBJ_DIR)/table/merger_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

util_merge_operators_test: $(OBJ_DIR)/utilities/util_merge_operators_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

options_file_test: $(OBJ_DIR)/db/options_file_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

deletefile_test: $(OBJ_DIR)/db/deletefile_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

obsolete_files_test: $(OBJ_DIR)/db/obsolete_files_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

rocksdb_dump: $(OBJ_DIR)/tools/dump/rocksdb_dump.o $(LIBRARY)
	$(AM_LINK)

rocksdb_undump: $(OBJ_DIR)/tools/dump/rocksdb_undump.o $(LIBRARY)
	$(AM_LINK)

cuckoo_table_builder_test: $(OBJ_DIR)/table/cuckoo/cuckoo_table_builder_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cuckoo_table_reader_test: $(OBJ_DIR)/table/cuckoo/cuckoo_table_reader_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

cuckoo_table_db_test: $(OBJ_DIR)/db/cuckoo_table_db_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

listener_test: $(OBJ_DIR)/db/listener_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

thread_list_test: $(OBJ_DIR)/util/thread_list_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

compact_files_test: $(OBJ_DIR)/db/compact_files_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

options_test: $(OBJ_DIR)/options/options_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

options_settable_test: $(OBJ_DIR)/options/options_settable_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

options_util_test: $(OBJ_DIR)/utilities/options/options_util_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_bench_tool_test: $(OBJ_DIR)/tools/db_bench_tool_test.o $(BENCH_OBJECTS) $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

trace_analyzer_test: $(OBJ_DIR)/tools/trace_analyzer_test.o $(ANALYZE_OBJECTS) $(TOOLS_LIBRARY) $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

event_logger_test: $(OBJ_DIR)/logging/event_logger_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

timer_queue_test: $(OBJ_DIR)/util/timer_queue_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

sst_dump_test: $(OBJ_DIR)/tools/sst_dump_test.o $(TOOLS_LIBRARY) $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

optimistic_transaction_test: $(OBJ_DIR)/utilities/transactions/optimistic_transaction_test.o  $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

mock_env_test : $(OBJ_DIR)/env/mock_env_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

manual_compaction_test: $(OBJ_DIR)/db/manual_compaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

filelock_test: $(OBJ_DIR)/util/filelock_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

auto_roll_logger_test: $(OBJ_DIR)/logging/auto_roll_logger_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

env_logger_test: $(OBJ_DIR)/logging/env_logger_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

memtable_list_test: $(OBJ_DIR)/db/memtable_list_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_callback_test: $(OBJ_DIR)/db/write_callback_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

heap_test: $(OBJ_DIR)/util/heap_test.o $(GTEST)
	$(AM_LINK)

transaction_lock_mgr_test: utilities/transactions/transaction_lock_mgr_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

transaction_test: $(OBJ_DIR)/utilities/transactions/transaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_prepared_transaction_test: $(OBJ_DIR)/utilities/transactions/write_prepared_transaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

write_unprepared_transaction_test: $(OBJ_DIR)/utilities/transactions/write_unprepared_transaction_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

sst_dump: $(OBJ_DIR)/tools/sst_dump.o $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_dump: $(OBJ_DIR)/tools/blob_dump.o $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

repair_test: $(OBJ_DIR)/db/repair_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

ldb_cmd_test: $(OBJ_DIR)/tools/ldb_cmd_test.o $(TOOLS_LIBRARY) $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

ldb: $(OBJ_DIR)/tools/ldb.o $(TOOLS_LIBRARY) $(LIBRARY)
	$(AM_LINK)

iostats_context_test: $(OBJ_DIR)/monitoring/iostats_context_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_V_CCLD)$(CXX) $^ $(EXEC_LDFLAGS) -o $@ $(LDFLAGS)

persistent_cache_test: $(OBJ_DIR)/utilities/persistent_cache/persistent_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

statistics_test: $(OBJ_DIR)/monitoring/statistics_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

stats_history_test: $(OBJ_DIR)/monitoring/stats_history_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

lru_cache_test: $(OBJ_DIR)/cache/lru_cache_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

range_del_aggregator_test: $(OBJ_DIR)/db/range_del_aggregator_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

range_del_aggregator_bench: $(OBJ_DIR)/db/range_del_aggregator_bench.o $(LIBRARY)
	$(AM_LINK)

blob_db_test: $(OBJ_DIR)/utilities/blob_db/blob_db_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

repeatable_thread_test: $(OBJ_DIR)/util/repeatable_thread_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

range_tombstone_fragmenter_test: $(OBJ_DIR)/db/range_tombstone_fragmenter_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

sst_file_reader_test: $(OBJ_DIR)/table/sst_file_reader_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

db_secondary_test: $(OBJ_DIR)/db/db_impl/db_secondary_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_cache_tracer_test: $(OBJ_DIR)/trace_replay/block_cache_tracer_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

block_cache_trace_analyzer_test: $(OBJ_DIR)/tools/block_cache_analyzer/block_cache_trace_analyzer_test.o $(OBJ_DIR)/tools/block_cache_analyzer/block_cache_trace_analyzer.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

defer_test: $(OBJ_DIR)/util/defer_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_file_addition_test: $(OBJ_DIR)/db/blob/blob_file_addition_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_file_builder_test: $(OBJ_DIR)/db/blob/blob_file_builder_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

blob_file_garbage_test: $(OBJ_DIR)/db/blob/blob_file_garbage_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

timer_test: $(OBJ_DIR)/util/timer_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

stats_dump_scheduler_test: $(OBJ_DIR)/monitoring/stats_dump_scheduler_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

testutil_test: $(OBJ_DIR)/test_util/testutil_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

io_tracer_test: $(OBJ_DIR)/trace_replay/io_tracer_test.o $(OBJ_DIR)/trace_replay/io_tracer.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

prefetch_test: $(OBJ_DIR)/file/prefetch_test.o $(TEST_LIBRARY) $(LIBRARY)
	$(AM_LINK)

#-------------------------------------------------
# make install related stuff
PREFIX ?= /usr/local
LIBDIR ?= $(PREFIX)/lib
INSTALL_LIBDIR = $(DESTDIR)$(LIBDIR)

uninstall:
	rm -rf $(DESTDIR)$(PREFIX)/include/rocksdb \
	  $(INSTALL_LIBDIR)/$(LIBRARY) \
	  $(INSTALL_LIBDIR)/$(SHARED4) \
	  $(INSTALL_LIBDIR)/$(SHARED3) \
	  $(INSTALL_LIBDIR)/$(SHARED2) \
	  $(INSTALL_LIBDIR)/$(SHARED1) \
	  $(INSTALL_LIBDIR)/pkgconfig/rocksdb.pc

install-headers: gen-pc
	install -d $(INSTALL_LIBDIR)
	install -d $(INSTALL_LIBDIR)/pkgconfig
	for header_dir in `$(FIND) "include/rocksdb" -type d`; do \
		install -d $(DESTDIR)/$(PREFIX)/$$header_dir; \
	done
	for header in `$(FIND) "include/rocksdb" -type f -name *.h`; do \
		install -C -m 644 $$header $(DESTDIR)/$(PREFIX)/$$header; \
	done
	install -C -m 644 rocksdb.pc $(INSTALL_LIBDIR)/pkgconfig/rocksdb.pc

install-static: install-headers $(LIBRARY)
	install -d $(INSTALL_LIBDIR)
	install -C -m 755 $(LIBRARY) $(INSTALL_LIBDIR)

install-shared: install-headers $(SHARED4)
	install -d $(INSTALL_LIBDIR)
	install -C -m 755 $(SHARED4) $(INSTALL_LIBDIR)
	ln -fs $(SHARED4) $(INSTALL_LIBDIR)/$(SHARED3)
	ln -fs $(SHARED4) $(INSTALL_LIBDIR)/$(SHARED2)
	ln -fs $(SHARED4) $(INSTALL_LIBDIR)/$(SHARED1)

# install static by default + install shared if it exists
install: install-static
	[ -e $(SHARED4) ] && $(MAKE) install-shared || :

# Generate the pkg-config file
gen-pc:
	-echo 'prefix=$(PREFIX)' > rocksdb.pc
	-echo 'exec_prefix=$${prefix}' >> rocksdb.pc
	-echo 'includedir=$${prefix}/include' >> rocksdb.pc
	-echo 'libdir=$(LIBDIR)' >> rocksdb.pc
	-echo '' >> rocksdb.pc
	-echo 'Name: rocksdb' >> rocksdb.pc
	-echo 'Description: An embeddable persistent key-value store for fast storage' >> rocksdb.pc
	-echo Version: $(shell ./build_tools/version.sh full) >> rocksdb.pc
	-echo 'Libs: -L$${libdir} $(EXEC_LDFLAGS) -lrocksdb' >> rocksdb.pc
	-echo 'Libs.private: $(PLATFORM_LDFLAGS)' >> rocksdb.pc
	-echo 'Cflags: -I$${includedir} $(PLATFORM_CXXFLAGS)' >> rocksdb.pc

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
ROCKSDB_JAVA_VERSION ?= $(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)
ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_JAVA_VERSION)-linux$(ARCH)$(JNI_LIBC_POSTFIX).jar
ROCKSDB_JAR_ALL = rocksdbjni-$(ROCKSDB_JAVA_VERSION).jar
ROCKSDB_JAVADOCS_JAR = rocksdbjni-$(ROCKSDB_JAVA_VERSION)-javadoc.jar
ROCKSDB_SOURCES_JAR = rocksdbjni-$(ROCKSDB_JAVA_VERSION)-sources.jar
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
	ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_JAVA_VERSION)-osx.jar
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
	ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_JAVA_VERSION)-freebsd$(ARCH).jar
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
        ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_JAVA_VERSION)-openbsd$(ARCH).jar
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

# A version of each $(LIB_OBJECTS) compiled with -fPIC and a fixed set of static compression libraries
ifneq ($(ROCKSDB_JAVA_NO_COMPRESSION), 1)
JAVA_COMPRESSIONS = libz.a libbz2.a libsnappy.a liblz4.a libzstd.a
endif

JAVA_STATIC_FLAGS = -DZLIB -DBZIP2 -DSNAPPY -DLZ4 -DZSTD
JAVA_STATIC_INCLUDES = -I./zlib-$(ZLIB_VER) -I./bzip2-$(BZIP2_VER) -I./snappy-$(SNAPPY_VER) -I./lz4-$(LZ4_VER)/lib -I./zstd-$(ZSTD_VER)/lib/include
ifneq ($(findstring rocksdbjavastatic, $(MAKECMDGOALS)),)
CXXFLAGS += $(JAVA_STATIC_FLAGS) $(JAVA_STATIC_INCLUDES)
CFLAGS +=  $(JAVA_STATIC_FLAGS) $(JAVA_STATIC_INCLUDES)
endif
rocksdbjavastatic: $(LIB_OBJECTS) $(JAVA_COMPRESSIONS)
	cd java;$(MAKE) javalib;
	rm -f ./java/target/$(ROCKSDBJNILIB)
	$(CXX) $(CXXFLAGS) -I./java/. $(JAVA_INCLUDE) -shared -fPIC \
	  -o ./java/target/$(ROCKSDBJNILIB) $(JNI_NATIVE_SOURCES) \
	  $(LIB_OBJECTS) $(COVERAGEFLAGS) \
	  $(JAVA_COMPRESSIONS) $(JAVA_STATIC_LDFLAGS)
	cd java/target;if [ "$(DEBUG_LEVEL)" == "0" ]; then \
		strip $(STRIPFLAGS) $(ROCKSDBJNILIB); \
	fi
	cd java;jar -cf target/$(ROCKSDB_JAR) HISTORY*.md
	cd java/target;jar -uf $(ROCKSDB_JAR) $(ROCKSDBJNILIB)
	cd java/target/classes;jar -uf ../$(ROCKSDB_JAR) org/rocksdb/*.class org/rocksdb/util/*.class
	cd java/target/apidocs;jar -cf ../$(ROCKSDB_JAVADOCS_JAR) *
	cd java/src/main/java;jar -cf ../../../target/$(ROCKSDB_SOURCES_JAR) org
	openssl sha1 java/target/$(ROCKSDB_JAR) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_JAR).sha1
	openssl sha1 java/target/$(ROCKSDB_JAVADOCS_JAR) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_JAVADOCS_JAR).sha1
	openssl sha1 java/target/$(ROCKSDB_SOURCES_JAR) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_SOURCES_JAR).sha1

rocksdbjavastaticrelease: rocksdbjavastatic
	cd java/crossbuild && (vagrant destroy -f || true) && vagrant up linux32 && vagrant halt linux32 && vagrant up linux64 && vagrant halt linux64 && vagrant up linux64-musl && vagrant halt linux64-musl
	cd java;jar -cf target/$(ROCKSDB_JAR_ALL) HISTORY*.md
	cd java/target;jar -uf $(ROCKSDB_JAR_ALL) librocksdbjni-*.so librocksdbjni-*.jnilib
	cd java/target/classes;jar -uf ../$(ROCKSDB_JAR_ALL) org/rocksdb/*.class org/rocksdb/util/*.class
	openssl sha1 java/target/$(ROCKSDB_JAR_ALL) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_JAR_ALL).sha1

rocksdbjavastaticreleasedocker: rocksdbjavastatic rocksdbjavastaticdockerx86 rocksdbjavastaticdockerx86_64 rocksdbjavastaticdockerx86musl rocksdbjavastaticdockerx86_64musl
	cd java;jar -cf target/$(ROCKSDB_JAR_ALL) HISTORY*.md
	cd java/target;jar -uf $(ROCKSDB_JAR_ALL) librocksdbjni-*.so librocksdbjni-*.jnilib
	cd java/target/classes;jar -uf ../$(ROCKSDB_JAR_ALL) org/rocksdb/*.class org/rocksdb/util/*.class
	openssl sha1 java/target/$(ROCKSDB_JAR_ALL) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_JAR_ALL).sha1

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

rocksdbjavastaticpublishcentral: rocksdbjavageneratepom
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/pom.xml -Dfile=java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION)-javadoc.jar -Dclassifier=javadoc
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/pom.xml -Dfile=java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION)-sources.jar -Dclassifier=sources
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/pom.xml -Dfile=java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION)-linux64.jar -Dclassifier=linux64
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/pom.xml -Dfile=java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION)-linux32.jar -Dclassifier=linux32
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/pom.xml -Dfile=java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION)-linux64-musl.jar -Dclassifier=linux64-musl
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/pom.xml -Dfile=java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION)-linux32-musl.jar -Dclassifier=linux32-musl
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/pom.xml -Dfile=java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION)-osx.jar -Dclassifier=osx
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/pom.xml -Dfile=java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION)-win64.jar -Dclassifier=win64
	mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=java/pom.xml -Dfile=java/target/rocksdbjni-$(ROCKSDB_JAVA_VERSION).jar

rocksdbjavageneratepom:
	cd java;cat pom.xml.template | sed 's/\$${ROCKSDB_JAVA_VERSION}/$(ROCKSDB_JAVA_VERSION)/' > pom.xml

# A version of each $(LIBOBJECTS) compiled with -fPIC

jl/%.o: %.cc
	$(AM_V_CC)mkdir -p $(@D) && $(CXX) $(CXXFLAGS) -fPIC -c $< -o $@ $(COVERAGEFLAGS)

jl/crc32c_ppc.o: util/crc32c_ppc.c
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@

jl/crc32c_ppc_asm.o: util/crc32c_ppc_asm.S
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@


rocksdbjava: $(LIB_OBJECTS)
	$(AM_V_GEN)cd java;$(MAKE) javalib;
	$(AM_V_at)rm -f ./java/target/$(ROCKSDBJNILIB)
	$(AM_V_at)$(CXX) $(CXXFLAGS) -I./java/. $(JAVA_INCLUDE) -shared -fPIC -o ./java/target/$(ROCKSDBJNILIB) $(JNI_NATIVE_SOURCES) $(LIB_OBJECTS) $(JAVA_LDFLAGS) $(COVERAGEFLAGS)
	$(AM_V_at)cd java;jar -cf target/$(ROCKSDB_JAR) HISTORY*.md
	$(AM_V_at)cd java/target;jar -uf $(ROCKSDB_JAR) $(ROCKSDBJNILIB)
	$(AM_V_at)cd java/target/classes;jar -uf ../$(ROCKSDB_JAR) org/rocksdb/*.class org/rocksdb/util/*.class
	$(AM_V_at)openssl sha1 java/target/$(ROCKSDB_JAR) | sed 's/.*= \([0-9a-f]*\)/\1/' > java/target/$(ROCKSDB_JAR).sha1

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
$(OBJ_DIR)/util/crc32c_ppc.o: util/crc32c_ppc.c
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@

+$(OBJ_DIR)/util/crc32c_ppc_asm.o: util/crc32c_ppc_asm.S
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@
endif
$(OBJ_DIR)/%.o: %.cc
	$(AM_V_CC)mkdir -p $(@D) && $(CXX) $(CXXFLAGS) -c $< -o $@ $(COVERAGEFLAGS)

$(OBJ_DIR)/%.o: %.cpp
	$(AM_V_CC)mkdir -p $(@D) && $(CXX) $(CXXFLAGS) -c $< -o $@ $(COVERAGEFLAGS)

$(OBJ_DIR)/%.o: %.c
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@
endif

# ---------------------------------------------------------------------------
#  	Source files dependencies detection
# ---------------------------------------------------------------------------

DEPFILES = $(patsubst %.cc, $(OBJ_DIR)/%.cc.d, $(ALL_SOURCES))
DEPFILES+ = $(patsubst %.c, $(OBJ_DIR)/%.c.d, $(LIB_SOURCES_C) $(TEST_MAIN_SOURCES_C))
ifeq ($(USE_FOLLY_DISTRIBUTED_MUTEX),1)
  DEPFILES +=$(patsubst %.cpp, $(OBJ_DIR)/%.cpp.d, $(FOLLY_SOURCES))
endif

# Add proper dependency support so changing a .h file forces a .cc file to
# rebuild.

# The .d file indicates .cc file's dependencies on .h files. We generate such
# dependency by g++'s -MM option, whose output is a make dependency rule.
$(OBJ_DIR)/%.cc.d: %.cc
	@mkdir -p $(@D) && $(CXX) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) \
	  -MM -MT'$@' -MT'$(<:.cc=.o)' -MT'$(<:%.cc=$(OBJ_DIR)/%.o)' \
          "$<" -o '$@'

$(OBJ_DIR)/%.cpp.d: %.cpp
	@mkdir -p $(@D) && $(CXX) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) \
	  -MM -MT'$@' -MT'$(<:.cpp=.o)' -MT'$(<:%.cpp=$(OBJ_DIR)/%.o)' \
          "$<" -o '$@'

ifeq ($(HAVE_POWER8),1)
DEPFILES_C = $(patsubst %.c, $(OBJ_DIR)/%.c.d, $(LIB_SOURCES_C))
DEPFILES_ASM = $(patsubst %.S, $(OBJ_DIR)/%.S.d, $(LIB_SOURCES_ASM))

$(OBJ_DIR)/%.c.d: %.c
	@$(CXX) $(CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) \
	  -MM -MT'$@' -MT'$(<:.c=.o)' "$<" -o '$@'

+$(OBJ_DIR)/%.S.d: %.S
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
