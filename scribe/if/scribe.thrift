#!/usr/local/bin/thrift --cpp --php

##  Copyright (c) 2007-2012 Facebook
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
## See accompanying file LICENSE or visit the Scribe site at:
## http://developers.facebook.com/scribe/

namespace cpp Tleveldb
namespace java Tleveldb

// Max message length allowed to log through scribe
const i32 SCRIBE_MAX_MESSAGE_LENGTH = 26214400;

enum ResultCode
{
  OK,
  TRY_LATER,
  ERROR_DECOMPRESS
}

struct SourceInfo
{
  1:  binary host,
  2:  i32 port,
  3:  i64 timestamp
}

struct LogEntry
{
  1:  binary category,
  2:  binary message,
  3:  optional map<string, string> metadata,
  4:  optional i32 checksum,
  5:  optional SourceInfo source,
  6:  optional i32 bucket
}

struct MessageList
{
  1: list<LogEntry> messages
}

service scribe
{
  #
  # Delivers a list of LogEntry messages to the Scribe server.
  # A returned ResultCode of anything other than OK indicates that the
  # whole batch was unable to be delivered to the server.
  # If data loss is a concern, the caller should buffer and retry the messages.
  #
  ResultCode Log(1: list<LogEntry> messages);

  #
  # NOTE: FOR INTERNAL USE ONLY!
  #
  # Delivers a list of LogEntry messages to the Scribe server, but
  # allows partial successes. A list of ResultCodes will be returned to
  # indicate the success or failure of each message at the corresponding index.
  # If data loss is a concern, the caller should retry only the failed messages.
  #
  list<ResultCode> LogMulti(1: list<LogEntry> messages);

  #
  # NOTE: FOR INTERNAL USE ONLY!
  #
  # The same as Log(...) except that the list of messages must first be
  # serialized and compressed in some internal format.
  #
  ResultCode LogCompressedMsg(1: binary compressedMessages);
}
