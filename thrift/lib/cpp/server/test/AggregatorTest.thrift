#!/usr/local/bin/thrift -cpp

include "common/fb303/if/fb303.thrift"

namespace cpp apache.thrift.async

struct StructRequest {
		1:i32 i32Val,
		2:i64 i64Val,
		3:double doubleVal,
		4:string stringVal,
}

struct StructResponse {
  1:StructRequest request,
  2:i32 errorCode
  3:string answerString,
}

service AggregatorTest extends fb303.FacebookService {
  StructResponse sendStructRecvStruct(1:StructRequest request),
  oneway void sendStructNoRecv(1:StructRequest request),
  StructResponse sendMultiParamsRecvStruct(
		1:i32 i32Val,
		2:i64 i64Val,
		3:double doubleVal,
		4:string stringVal,
		5:StructRequest structVal,
	),
  oneway void sendMultiParamsNoRecv(
		1:i32 i32Val,
		2:i64 i64Val,
		3:double doubleVal,
		4:string stringVal,
		5:StructRequest structVal,
	),
	StructResponse noSendRecvStruct(),
	oneway void noSendNoRecv(),
}


