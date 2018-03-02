include make_config.mk

INSTALL_PATH ?= /usr/local

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti
endif

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

INSTALL_FILES = $(INSTALL_PATH)/include/rocksdb \
								$(INSTALL_PATH)/lib/$(LIBRARY)

.PHONY: clean librocksdb


all: simple_example column_families_example compact_files_example c_simple_example optimistic_transaction_example transaction_example compaction_filter_example options_file_example

simple_example: librocksdb simple_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ $(INSTALL_PATH)/lib/librocksdb.a -I$(INSTALL_PATH)/include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

column_families_example: librocksdb column_families_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ $(INSTALL_PATH)/lib/librocksdb.a -I$(INSTALL_PATH)/include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

compaction_filter_example: librocksdb compaction_filter_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ $(INSTALL_PATH)/lib/librocksdb.a -I$(INSTALL_PATH)/include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

compact_files_example: librocksdb compact_files_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ $(INSTALL_PATH)/lib/librocksdb.a -I$(INSTALL_PATH)/include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@ -I$(INSTALL_PATH)/include

c_simple_example: librocksdb c_simple_example.o
	$(CXX) $@.o -o$@ $(INSTALL_PATH)/lib/librocksdb.a $(PLATFORM_LDFLAGS) $(EXEC_LDFLAGS)

optimistic_transaction_example: librocksdb optimistic_transaction_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ $(INSTALL_PATH)/lib/librocksdb.a -I$(INSTALL_PATH)/include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

transaction_example: librocksdb transaction_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ $(INSTALL_PATH)/lib/librocksdb.a -I$(INSTALL_PATH)/include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

options_file_example: librocksdb options_file_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ $(INSTALL_PATH)/lib/librocksdb.a -I$(INSTALL_PATH)/include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./simple_example ./column_families_example ./compact_files_example ./compaction_filter_example ./c_simple_example c_simple_example.o ./optimistic_transaction_example ./transaction_example ./options_file_example

librocksdb:
ifeq ($(wildcard $(INSTALL_FILES)),)
	# installing library before compiling examples
	../make
	sudo ../make install
endif
