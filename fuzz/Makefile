# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

CWD = $(shell pwd)

PROTOBUF_CFLAGS = `pkg-config --cflags protobuf`
PROTOBUF_LDFLAGS = `pkg-config --libs protobuf`

PROTOBUF_MUTATOR_CFLAGS = `pkg-config --cflags libprotobuf-mutator`
PROTOBUF_MUTATOR_LDFLAGS = `pkg-config --libs libprotobuf-mutator`

ROCKSDB_INCLUDE_DIR = $(CWD)/../include
ROCKSDB_LIB_DIR = $(CWD)/..

PROTO_IN = $(CWD)/proto
PROTO_OUT = $(CWD)/proto/gen

ifneq ($(FUZZ_ENV), ossfuzz)
CC = clang++
CCFLAGS += -std=c++14 -Wall -fsanitize=address,fuzzer
CFLAGS += $(PROTOBUF_CFLAGS) $(PROTOBUF_MUTATOR_CFLAGS) -I$(PROTO_OUT) -I$(ROCKSDB_INCLUDE_DIR)
LDFLAGS += $(PROTOBUF_LDFLAGS) $(PROTOBUF_MUTATOR_LDFLAGS) -L$(ROCKSDB_LIB_DIR) -lrocksdb -lz -lbz2
else
CC = $(CXX)
CCFLAGS = $(CXXFLAGS)
CFLAGS += $(PROTOBUF_CFLAGS) $(PROTOBUF_MUTATOR_CFLAGS) -I$(PROTO_OUT) -I$(ROCKSDB_INCLUDE_DIR)
LDFLAGS += $(LIB_FUZZING_ENGINE) $(PROTOBUF_MUTATOR_LDFLAGS) $(PROTOBUF_LDFLAGS) -L$(ROCKSDB_LIB_DIR) -lrocksdb -lz -lbz2
endif

.PHONY: librocksdb gen_proto

librocksdb:
	cd $(CWD)/.. && make static_lib

gen_proto:
	mkdir -p $(PROTO_OUT)
	protoc \
		--proto_path=$(PROTO_IN) \
		--cpp_out=$(PROTO_OUT) \
		$(PROTO_IN)/*.proto

db_fuzzer: librocksdb
	$(CC) $(CCFLAGS) -o db_fuzzer db_fuzzer.cc $(CFLAGS) $(LDFLAGS)

sst_file_writer_fuzzer: librocksdb gen_proto sst_file_writer_fuzzer.cc proto/gen/db_operation.pb.cc
	$(CC) $(CCFLAGS) -o sst_file_writer_fuzzer sst_file_writer_fuzzer.cc proto/gen/db_operation.pb.cc $(CFLAGS) $(LDFLAGS)
