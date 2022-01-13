# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

ROOT_DIR = $(abspath $(shell pwd)/../)

include $(ROOT_DIR)/make_config.mk

PROTOBUF_CFLAGS = `pkg-config --cflags protobuf`
PROTOBUF_LDFLAGS = `pkg-config --libs protobuf`

PROTOBUF_MUTATOR_CFLAGS = `pkg-config --cflags libprotobuf-mutator`
PROTOBUF_MUTATOR_LDFLAGS = `pkg-config --libs libprotobuf-mutator`

ROCKSDB_INCLUDE_DIR = $(ROOT_DIR)/include
ROCKSDB_LIB_DIR = $(ROOT_DIR)

PROTO_IN = $(ROOT_DIR)/fuzz/proto
PROTO_OUT = $(ROOT_DIR)/fuzz/proto/gen

ifneq ($(FUZZ_ENV), ossfuzz)
CC = clang++
CCFLAGS += -Wall -fsanitize=address,fuzzer
CFLAGS += $(PLATFORM_CXXFLAGS) $(PROTOBUF_CFLAGS) $(PROTOBUF_MUTATOR_CFLAGS) -I$(PROTO_OUT) -I$(ROCKSDB_INCLUDE_DIR) -I$(ROCKSDB_LIB_DIR)
LDFLAGS += $(PLATFORM_LDFLAGS) $(PROTOBUF_LDFLAGS) $(PROTOBUF_MUTATOR_LDFLAGS) -L$(ROCKSDB_LIB_DIR) -lrocksdb
else
# OSS-Fuzz sets various environment flags that are used for compilation.
# These environment flags depend on which type of sanitizer build is being
# used, however, an ASan build would set the environment flags as follows:
# CFLAGS="-O1 -fno-omit-frame-pointer -gline-tables-only \
         -DFUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION -fsanitize=address \
         -fsanitize-address-use-after-scope -fsanitize=fuzzer-no-link"
# CXXFLAGS="-O1 -fno-omit-frame-pointer -gline-tables-only \
           -DFUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION -fsanitize=address \
           -fsanitize-address-use-after-scope -fsanitize=fuzzer-no-link \
           -stdlib=libc++"
# LIB_FUZZING_ENGINE="-fsanitize=fuzzer"
CC = $(CXX)
CCFLAGS = $(CXXFLAGS)
CFLAGS += $(PROTOBUF_CFLAGS) $(PROTOBUF_MUTATOR_CFLAGS) -I$(PROTO_OUT) -I$(ROCKSDB_INCLUDE_DIR) -I$(ROCKSDB_LIB_DIR)
LDFLAGS += $(PLATFORM_LDFLAGS) $(LIB_FUZZING_ENGINE) $(PROTOBUF_MUTATOR_LDFLAGS) $(PROTOBUF_LDFLAGS) -L$(ROCKSDB_LIB_DIR) -lrocksdb
endif

.PHONY: gen_proto

gen_proto:
	mkdir -p $(PROTO_OUT)
	protoc \
		--proto_path=$(PROTO_IN) \
		--cpp_out=$(PROTO_OUT) \
		$(PROTO_IN)/*.proto

db_fuzzer: db_fuzzer.cc
	$(CC) $(CCFLAGS) -o db_fuzzer db_fuzzer.cc $(CFLAGS) $(LDFLAGS)

db_map_fuzzer: gen_proto db_map_fuzzer.cc proto/gen/db_operation.pb.cc
	$(CC) $(CCFLAGS) -o db_map_fuzzer db_map_fuzzer.cc proto/gen/db_operation.pb.cc $(CFLAGS) $(LDFLAGS)

sst_file_writer_fuzzer: gen_proto sst_file_writer_fuzzer.cc proto/gen/db_operation.pb.cc
	$(CC) $(CCFLAGS) -o sst_file_writer_fuzzer sst_file_writer_fuzzer.cc proto/gen/db_operation.pb.cc $(CFLAGS) $(LDFLAGS)
