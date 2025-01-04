include ../make_config.mk

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

CFLAGS += -Wstrict-prototypes
STRIPFLAGS = -S -x

ifeq ($(PLATFORM), OS_AIX)
ARFLAGS = -X64 rs
STRIPFLAGS = -X64 -x
endif

.PHONY: clean librocksdb

all: simple_example column_families_example compact_files_example c_simple_example optimistic_transaction_example transaction_example compaction_filter_example options_file_example rocksdb_backup_restore_example

simple_example: librocksdb simple_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) && strip $(STRIPFLAGS) $@

column_families_example: librocksdb column_families_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) && strip $(STRIPFLAGS) $@

compaction_filter_example: librocksdb compaction_filter_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) && strip $(STRIPFLAGS) $@

compact_files_example: librocksdb compact_files_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) && strip $(STRIPFLAGS) $@

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@ -I../include

c_simple_example: librocksdb c_simple_example.o
	$(CXX) $@.o -o$@ ../librocksdb.a $(PLATFORM_LDFLAGS) $(EXEC_LDFLAGS) && strip $(STRIPFLAGS) c_simple_example && strip $(STRIPFLAGS) $@

optimistic_transaction_example: librocksdb optimistic_transaction_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) && strip $(STRIPFLAGS) $@

transaction_example: librocksdb transaction_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) && strip $(STRIPFLAGS) $@

options_file_example: librocksdb options_file_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) && strip $(STRIPFLAGS) $@

multi_processes_example: librocksdb multi_processes_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) && strip $(STRIPFLAGS) $@

rocksdb_backup_restore_example: librocksdb rocksdb_backup_restore_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) && strip $(STRIPFLAGS) $@

clean:
	rm -rf ./simple_example ./column_families_example ./compact_files_example ./compaction_filter_example ./c_simple_example c_simple_example.o ./optimistic_transaction_example ./transaction_example ./options_file_example ./multi_processes_example ./rocksdb_backup_restore_example

librocksdb:
	cd .. && $(MAKE) static_lib
