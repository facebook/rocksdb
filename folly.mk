# This file contains the vast majority of folly-related build configuration
# for the checkout_folly and build_folly targets, so that this file can be
# hashed for purposes of caching folly builds and not hitting that cache when
# something here changes.

# This provides a Makefile simulation of a Meta-internal folly integration.
# It is not validated for general use.
#
# USE_FOLLY links the build targets with libfolly.a. The latter could be
# built using 'make build_folly', or built externally and specified in
# the CXXFLAGS and EXTRA_LDFLAGS env variables. The build_detect_platform
# script tries to detect if an external folly dependency has been specified.
# If not, it exports FOLLY_PATH to the path of the installed Folly and
# dependency libraries.
#
# USE_FOLLY_LITE cherry picks source files from Folly to include in the
# RocksDB library. Its faster and has fewer dependencies on 3rd party
# libraries, but with limited functionality. For example, coroutine
# functionality is not available.
ifeq ($(USE_FOLLY),1)
ifeq ($(USE_FOLLY_LITE),1)
$(error Please specify only one of USE_FOLLY and USE_FOLLY_LITE)
endif
ifneq ($(strip $(FOLLY_PATH)),)
	BOOST_PATH = $(shell (ls -d $(FOLLY_PATH)/../boost*))
	DBL_CONV_PATH = $(shell (ls -d $(FOLLY_PATH)/../double-conversion*))
	GFLAGS_PATH = $(shell (ls -d $(FOLLY_PATH)/../gflags*))
	GLOG_PATH = $(shell (ls -d $(FOLLY_PATH)/../glog*))
	LIBEVENT_PATH = $(shell (ls -d $(FOLLY_PATH)/../libevent*))
	XZ_PATH = $(shell (ls -d $(FOLLY_PATH)/../xz*))
	LIBSODIUM_PATH = $(shell (ls -d $(FOLLY_PATH)/../libsodium*))
	FMT_PATH = $(shell (ls -d $(FOLLY_PATH)/../fmt*))

	# For some reason, glog and fmt libraries are under either lib or lib64
	GLOG_LIB_PATH = $(shell (ls -d $(GLOG_PATH)/lib*))
	FMT_LIB_PATH = $(shell (ls -d $(FMT_PATH)/lib*))

	# AIX: pre-defined system headers are surrounded by an extern "C" block
	ifeq ($(PLATFORM), OS_AIX)
		PLATFORM_CCFLAGS += -I$(BOOST_PATH)/include -I$(DBL_CONV_PATH)/include -I$(GLOG_PATH)/include -I$(LIBEVENT_PATH)/include -I$(XZ_PATH)/include -I$(LIBSODIUM_PATH)/include -I$(FOLLY_PATH)/include -I$(FMT_PATH)/include
		PLATFORM_CXXFLAGS += -I$(BOOST_PATH)/include -I$(DBL_CONV_PATH)/include -I$(GLOG_PATH)/include -I$(LIBEVENT_PATH)/include -I$(XZ_PATH)/include -I$(LIBSODIUM_PATH)/include -I$(FOLLY_PATH)/include -I$(FMT_PATH)/include
	else
		PLATFORM_CCFLAGS += -isystem $(BOOST_PATH)/include -isystem $(DBL_CONV_PATH)/include -isystem $(GLOG_PATH)/include -isystem $(LIBEVENT_PATH)/include -isystem $(XZ_PATH)/include -isystem $(LIBSODIUM_PATH)/include -isystem $(FOLLY_PATH)/include -isystem $(FMT_PATH)/include
		PLATFORM_CXXFLAGS += -isystem $(BOOST_PATH)/include -isystem $(DBL_CONV_PATH)/include -isystem $(GLOG_PATH)/include -isystem $(LIBEVENT_PATH)/include -isystem $(XZ_PATH)/include -isystem $(LIBSODIUM_PATH)/include -isystem $(FOLLY_PATH)/include -isystem $(FMT_PATH)/include
	endif

	# Add -ldl at the end as gcc resolves a symbol in a library by searching only in libraries specified later
	# in the command line

	PLATFORM_LDFLAGS += $(FOLLY_PATH)/lib/libfolly.a $(BOOST_PATH)/lib/libboost_context.a $(BOOST_PATH)/lib/libboost_filesystem.a $(BOOST_PATH)/lib/libboost_atomic.a $(BOOST_PATH)/lib/libboost_program_options.a $(BOOST_PATH)/lib/libboost_regex.a $(BOOST_PATH)/lib/libboost_system.a $(BOOST_PATH)/lib/libboost_thread.a $(DBL_CONV_PATH)/lib/libdouble-conversion.a $(LIBEVENT_PATH)/lib/libevent-2.1.so $(LIBSODIUM_PATH)/lib/libsodium.a -ldl
ifneq ($(DEBUG_LEVEL),0)
	PLATFORM_LDFLAGS += $(FMT_LIB_PATH)/libfmtd.a $(GLOG_LIB_PATH)/libglogd.so $(GFLAGS_PATH)/lib/libgflags_debug.so.2.2
else
	PLATFORM_LDFLAGS += $(FMT_LIB_PATH)/libfmt.a $(GLOG_LIB_PATH)/libglog.so $(GFLAGS_PATH)/lib/libgflags.so.2.2
endif
	PLATFORM_LDFLAGS += -Wl,-rpath=$(LIBEVENT_PATH)/lib -Wl,-rpath=$(GLOG_LIB_PATH) -Wl,-rpath=$(GFLAGS_PATH)/lib
endif
	PLATFORM_CCFLAGS += -DUSE_FOLLY -DFOLLY_NO_CONFIG
	PLATFORM_CXXFLAGS += -DUSE_FOLLY -DFOLLY_NO_CONFIG
endif

ifeq ($(USE_FOLLY_LITE),1)
	# Path to the Folly source code and include files
	FOLLY_DIR = ./third-party/folly
ifneq ($(strip $(BOOST_SOURCE_PATH)),)
	BOOST_INCLUDE = $(shell (ls -d $(BOOST_SOURCE_PATH)/boost*/))
	# AIX: pre-defined system headers are surrounded by an extern "C" block
	ifeq ($(PLATFORM), OS_AIX)
		PLATFORM_CCFLAGS += -I$(BOOST_INCLUDE)
		PLATFORM_CXXFLAGS += -I$(BOOST_INCLUDE)
	else
		PLATFORM_CCFLAGS += -isystem $(BOOST_INCLUDE)
		PLATFORM_CXXFLAGS += -isystem $(BOOST_INCLUDE)
	endif
endif  # BOOST_SOURCE_PATH
ifneq ($(strip $(FMT_SOURCE_PATH)),)
	FMT_INCLUDE = $(shell (ls -d $(FMT_SOURCE_PATH)/fmt*/include/))
	# AIX: pre-defined system headers are surrounded by an extern "C" block
	ifeq ($(PLATFORM), OS_AIX)
		PLATFORM_CCFLAGS += -I$(FMT_INCLUDE)
		PLATFORM_CXXFLAGS += -I$(FMT_INCLUDE)
	else
		PLATFORM_CCFLAGS += -isystem $(FMT_INCLUDE)
		PLATFORM_CXXFLAGS += -isystem $(FMT_INCLUDE)
	endif
endif  # FMT_SOURCE_PATH
	# AIX: pre-defined system headers are surrounded by an extern "C" block
	ifeq ($(PLATFORM), OS_AIX)
		PLATFORM_CCFLAGS += -I$(FOLLY_DIR)
		PLATFORM_CXXFLAGS += -I$(FOLLY_DIR)
	else
		PLATFORM_CCFLAGS += -isystem $(FOLLY_DIR)
		PLATFORM_CXXFLAGS += -isystem $(FOLLY_DIR)
	endif
	PLATFORM_CCFLAGS += -DUSE_FOLLY -DFOLLY_NO_CONFIG
	PLATFORM_CXXFLAGS += -DUSE_FOLLY -DFOLLY_NO_CONFIG
# TODO: fix linking with fbcode compiler config
	PLATFORM_LDFLAGS += -lglog
endif

FOLLY_COMMIT_HASH = 94a8e82cf16a0e229fc4fc89140219434ba78fa2

# For public CI runs, checkout folly in a way that can build with RocksDB.
# This is mostly intended as a test-only simulation of Meta-internal folly
# integration.
checkout_folly:
	if [ -e third-party/folly ]; then \
		cd third-party/folly && ${GIT_COMMAND} fetch origin; \
	else \
		cd third-party && ${GIT_COMMAND} clone https://github.com/facebook/folly.git; \
	fi
	@# Pin to a particular version for public CI, so that PR authors don't
	@# need to worry about folly breaking our integration. Update periodically
	cd third-party/folly && git reset --hard $(FOLLY_COMMIT_HASH)
	@# Apparently missing include
	perl -pi -e 's/(#include <atomic>)/$$1\n#include <cstring>/' third-party/folly/folly/lang/Exception.h
	@# const mismatch
	perl -pi -e 's/: environ/: (const char**)(environ)/' third-party/folly/folly/Subprocess.cpp
	@# Use gnu.org mirrors to improve download speed (ftp.gnu.org is often super slow)
	cd third-party/folly && perl -pi -e 's/ftp.gnu.org/ftpmirror.gnu.org/' `git grep -l ftp.gnu.org` README.md
	@# NOTE: boost and fmt source will be needed for any build including `USE_FOLLY_LITE` builds as those depend on those headers
	cd third-party/folly && GETDEPS_USE_WGET=1 $(PYTHON) build/fbcode_builder/getdeps.py fetch boost && GETDEPS_USE_WGET=1 $(PYTHON) build/fbcode_builder/getdeps.py fetch fmt

CXX_M_FLAGS = $(filter -m%, $(CXXFLAGS))

FOLLY_BUILD_FLAGS = --no-tests
# NOTE: To avoid ODR violations, we must build folly in debug mode iff
# building RocksDB in debug mode.
ifneq ($(DEBUG_LEVEL),0)
FOLLY_BUILD_FLAGS += --build-type Debug
endif

build_folly:
	FOLLY_INST_PATH=`cd third-party/folly && $(PYTHON) build/fbcode_builder/getdeps.py show-inst-dir`; \
	if [ "$$FOLLY_INST_PATH" ]; then \
		rm -rf $${FOLLY_INST_PATH}/../../*; \
	else \
		echo "Please run checkout_folly first"; \
		false; \
	fi
	cd third-party/folly && \
		CXXFLAGS=" $(CXX_M_FLAGS) -DHAVE_CXX11_ATOMIC " GETDEPS_USE_WGET=1 $(PYTHON) build/fbcode_builder/getdeps.py build $(FOLLY_BUILD_FLAGS)
	@# In the folly build, glog and gflags are only built as dynamic libraries,
	@# not static. This patchelf command is needed to reliably have the glog
	@# library find its dependency gflags, because apparently the rpath of the
	@# final binary is not used in resolving that transitive dependency.
	FOLLY_INST_PATH=`cd third-party/folly && $(PYTHON) build/fbcode_builder/getdeps.py show-inst-dir`; \
	cd "$$FOLLY_INST_PATH" && patchelf --add-rpath $$PWD/../gflags-*/lib ../glog-*/lib*/libglog*.so.*.*.*
