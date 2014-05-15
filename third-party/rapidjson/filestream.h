#ifndef RAPIDJSON_FILESTREAM_H_
#define RAPIDJSON_FILESTREAM_H_

#include <cstdio>

namespace rapidjson {

//! Wrapper of C file stream for input or output.
/*!
	This simple wrapper does not check the validity of the stream.
	\implements Stream
*/
class FileStream {
public:
	typedef char Ch;	//!< Character type. Only support char.

	FileStream(FILE* fp) : fp_(fp), count_(0) { Read(); }
	char Peek() const { return current_; }
	char Take() { char c = current_; Read(); return c; }
	size_t Tell() const { return count_; }
	void Put(char c) { fputc(c, fp_); }

	// Not implemented
	char* PutBegin() { return 0; }
	size_t PutEnd(char*) { return 0; }

private:
	void Read() {
		RAPIDJSON_ASSERT(fp_ != 0);
		int c = fgetc(fp_);
		if (c != EOF) {
			current_ = (char)c;
			count_++;
		}
		else
			current_ = '\0';
	}

	FILE* fp_;
	char current_;
	size_t count_;
};

} // namespace rapidjson

#endif // RAPIDJSON_FILESTREAM_H_
