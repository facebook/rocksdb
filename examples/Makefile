include ../build_config.mk

.PHONY: clean

all: simple_example column_families_example compact_files_example simple_example-c

simple_example: simple_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

column_families_example: column_families_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

compact_files_example: compact_files_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

simple_example-c: simple_example.c
	$(CXX) -xc -I../include simple_example.c -o$@ -L.. -lrocksdb -pthread -lsnappy -lbz2 -lz -lrt

clean:
	rm -rf ./simple_example ./column_families_example ./compact_files_example ./simple_example-c
