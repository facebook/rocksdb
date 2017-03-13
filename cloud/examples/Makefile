include ../../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += "-DROCKSDB_JEMALLOC"
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

.PHONY: clean librocksdb

all: cloud_durable_example 

cloud_durable_example: librocksdb cloud_durable_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o $@ ../../librocksdb.a -I../../include $(OPT) -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./cloud_durable_example 

librocksdb:
	cd ../.. && $(MAKE) static_lib
