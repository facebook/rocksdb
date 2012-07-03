#ifndef THRIFT_TRANSPORT_THTTPPARSER_H_
#define THRIFT_TRANSPORT_THTTPPARSER_H_ 1

#include <transport/TBufferTransports.h>

namespace apache { namespace thrift { namespace util {

class THttpParser {
 protected:
  enum HttpParseState {
    HTTP_PARSE_START = 0,
    HTTP_PARSE_HEADER,
    HTTP_PARSE_CHUNK,
    HTTP_PARSE_CONTENT,
    HTTP_PARSE_CHUNKFOOTER,
    HTTP_PARSE_TRAILING
  };

  enum HttpParseResult {
    HTTP_PARSE_RESULT_CONTINUE,
    HTTP_PARSE_RESULT_BLOCK
  };

 public:
  THttpParser();
  ~THttpParser();

  void getReadBuffer(void** bufReturn, size_t* lenReturn);
  bool readDataAvailable(size_t len);
  void setDataBuffer(apache::thrift::transport::TMemoryBuffer* buffer) {
    dataBuf_ = buffer;
  }
  void unsetDataBuffer() {
    dataBuf_ = NULL;
  }
  void setMaxSize(uint32_t size) {
    maxSize_ = size;
  }
  uint32_t getMaxSize() {
    return maxSize_;
  }
  bool hasReadAheadData() {
    return (state_ == HTTP_PARSE_START) && (httpBufLen_ > httpPos_);
  }
  bool hasPartialMessage() {
    return partialMessageSize_ > 0;
  }
  virtual int constructHeader(iovec* ops,
                               int opsLen,
                               int contentLength,
                               char* contentLengthBuf) = 0;

 protected:
  HttpParseResult parseStart();
  HttpParseResult parseHeader();
  HttpParseResult parseContent();
  HttpParseResult parseChunk();
  HttpParseResult parseChunkFooter();
  HttpParseResult parseTrailing();

  virtual bool parseStatusLine(const char* status) = 0;
  virtual void parseHeaderLine(const char* header) = 0;

  void shift();
  char* readLine();
  void checkMessageSize(uint32_t more, bool added);

  char* httpBuf_;
  uint32_t httpPos_;
  uint32_t httpBufLen_;
  uint32_t httpBufSize_;

  HttpParseState state_;

  // for read header
  bool statusLine_;
  bool finished_;
  bool chunked_;

  size_t contentLength_;

  // max http message size
  uint32_t maxSize_;
  uint32_t partialMessageSize_;

  apache::thrift::transport::TMemoryBuffer* dataBuf_;

  static const char* CRLF;
  static const int CRLF_LEN;
};

class THttpClientParser : public THttpParser {
 public:
  THttpClientParser(std::string host, std::string path) {
    host_ = host;
    path_ = path;
    userAgent_ = "C++/THttpClient";
  }
  void setHost(const std::string& host) { host_ = host; }
  void setPath(const std::string& path) { path_ = path; }
  void resetConnectClosedByServer();
  bool isConnectClosedByServer();
  void setUserAgent(std::string userAgent) {
    userAgent_ = userAgent;
  }
  virtual int constructHeader(iovec* ops,
                               int opsLen,
                               int contentLength,
                               char* contentLengthBuf);

 protected:
  virtual void parseHeaderLine(const char* header);
  virtual bool parseStatusLine(const char* status);
  void setiovec(iovec* ops, const char* data, int size) {
    ops->iov_base = (void*)data;
    ops->iov_len = size;
  }

 private:
  bool connectionClosedByServer_;
  std::string host_;
  std::string path_;
  std::string userAgent_;
};


}}} // apache::thrift::util

#endif // #ifndef THRIFT_TRANSPORT_THTTPPARSER_H_

