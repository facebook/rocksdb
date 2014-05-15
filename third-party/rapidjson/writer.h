#ifndef RAPIDJSON_WRITER_H_
#define RAPIDJSON_WRITER_H_

#include "rapidjson.h"
#include "internal/stack.h"
#include "internal/strfunc.h"
#include <cstdio>	// snprintf() or _sprintf_s()
#include <new>		// placement new

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4127) // conditional expression is constant
#endif

namespace rapidjson {

//! JSON writer
/*! Writer implements the concept Handler.
	It generates JSON text by events to an output stream.

	User may programmatically calls the functions of a writer to generate JSON text.

	On the other side, a writer can also be passed to objects that generates events, 

	for example Reader::Parse() and Document::Accept().

	\tparam Stream Type of ouptut stream.
	\tparam Encoding Encoding of both source strings and output.
	\implements Handler
*/
template<typename Stream, typename Encoding = UTF8<>, typename Allocator = MemoryPoolAllocator<> >
class Writer {
public:
	typedef typename Encoding::Ch Ch;

	Writer(Stream& stream, Allocator* allocator = 0, size_t levelDepth = kDefaultLevelDepth) : 
		stream_(stream), level_stack_(allocator, levelDepth * sizeof(Level)) {}

	//@name Implementation of Handler
	//@{
	Writer& Null()					{ Prefix(kNullType);   WriteNull();			return *this; }
	Writer& Bool(bool b)			{ Prefix(b ? kTrueType : kFalseType); WriteBool(b); return *this; }
	Writer& Int(int i)				{ Prefix(kNumberType); WriteInt(i);			return *this; }
	Writer& Uint(unsigned u)		{ Prefix(kNumberType); WriteUint(u);		return *this; }
	Writer& Int64(int64_t i64)		{ Prefix(kNumberType); WriteInt64(i64);		return *this; }
	Writer& Uint64(uint64_t u64)	{ Prefix(kNumberType); WriteUint64(u64);	return *this; }
	Writer& Double(double d)		{ Prefix(kNumberType); WriteDouble(d);		return *this; }

	Writer& String(const Ch* str, SizeType length, bool copy = false) {
		(void)copy;
		Prefix(kStringType);
		WriteString(str, length);
		return *this;
	}

	Writer& StartObject() {
		Prefix(kObjectType);
		new (level_stack_.template Push<Level>()) Level(false);
		WriteStartObject();
		return *this;
	}

	Writer& EndObject(SizeType memberCount = 0) {
		(void)memberCount;
		RAPIDJSON_ASSERT(level_stack_.GetSize() >= sizeof(Level));
		RAPIDJSON_ASSERT(!level_stack_.template Top<Level>()->inArray);
		level_stack_.template Pop<Level>(1);
		WriteEndObject();
		return *this;
	}

	Writer& StartArray() {
		Prefix(kArrayType);
		new (level_stack_.template Push<Level>()) Level(true);
		WriteStartArray();
		return *this;
	}

	Writer& EndArray(SizeType elementCount = 0) {
		(void)elementCount;
		RAPIDJSON_ASSERT(level_stack_.GetSize() >= sizeof(Level));
		RAPIDJSON_ASSERT(level_stack_.template Top<Level>()->inArray);
		level_stack_.template Pop<Level>(1);
		WriteEndArray();
		return *this;
	}
	//@}

	//! Simpler but slower overload.
	Writer& String(const Ch* str) { return String(str, internal::StrLen(str)); }

protected:
	//! Information for each nested level
	struct Level {
		Level(bool inArray_) : inArray(inArray_), valueCount(0) {}
		bool inArray;		//!< true if in array, otherwise in object
		size_t valueCount;	//!< number of values in this level
	};

	static const size_t kDefaultLevelDepth = 32;

	void WriteNull()  {
		stream_.Put('n'); stream_.Put('u'); stream_.Put('l'); stream_.Put('l');
	}

	void WriteBool(bool b)  {
		if (b) {
			stream_.Put('t'); stream_.Put('r'); stream_.Put('u'); stream_.Put('e');
		}
		else {
			stream_.Put('f'); stream_.Put('a'); stream_.Put('l'); stream_.Put('s'); stream_.Put('e');
		}
	}

	void WriteInt(int i) {
		if (i < 0) {
			stream_.Put('-');
			i = -i;
		}
		WriteUint((unsigned)i);
	}

	void WriteUint(unsigned u) {
		char buffer[10];
		char *p = buffer;
		do {
			*p++ = (u % 10) + '0';
			u /= 10;
		} while (u > 0);

		do {
			--p;
			stream_.Put(*p);
		} while (p != buffer);
	}

	void WriteInt64(int64_t i64) {
		if (i64 < 0) {
			stream_.Put('-');
			i64 = -i64;
		}
		WriteUint64((uint64_t)i64);
	}

	void WriteUint64(uint64_t u64) {
		char buffer[20];
		char *p = buffer;
		do {
			*p++ = char(u64 % 10) + '0';
			u64 /= 10;
		} while (u64 > 0);

		do {
			--p;
			stream_.Put(*p);
		} while (p != buffer);
	}

	//! \todo Optimization with custom double-to-string converter.
	void WriteDouble(double d) {
		char buffer[100];
#if _MSC_VER
		int ret = sprintf_s(buffer, sizeof(buffer), "%g", d);
#else
		int ret = snprintf(buffer, sizeof(buffer), "%g", d);
#endif
		RAPIDJSON_ASSERT(ret >= 1);
		for (int i = 0; i < ret; i++)
			stream_.Put(buffer[i]);
	}

	void WriteString(const Ch* str, SizeType length)  {
		static const char hexDigits[] = "0123456789ABCDEF";
		static const char escape[256] = {
#define Z16 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
			//0    1    2    3    4    5    6    7    8    9    A    B    C    D    E    F
			'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'b', 't', 'n', 'u', 'f', 'r', 'u', 'u', // 00
			'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', // 10
			  0,   0, '"',   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0, // 20
			Z16, Z16,																		// 30~4F
			  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,'\\',   0,   0,   0, // 50
			Z16, Z16, Z16, Z16, Z16, Z16, Z16, Z16, Z16, Z16								// 60~FF
#undef Z16
		};

		stream_.Put('\"');
		for (const Ch* p = str; p != str + length; ++p) {
			if ((sizeof(Ch) == 1 || *p < 256) && escape[(unsigned char)*p])  {
				stream_.Put('\\');
				stream_.Put(escape[(unsigned char)*p]);
				if (escape[(unsigned char)*p] == 'u') {
					stream_.Put('0');
					stream_.Put('0');
					stream_.Put(hexDigits[(*p) >> 4]);
					stream_.Put(hexDigits[(*p) & 0xF]);
				}
			}
			else
				stream_.Put(*p);
		}
		stream_.Put('\"');
	}

	void WriteStartObject()	{ stream_.Put('{'); }
	void WriteEndObject()	{ stream_.Put('}'); }
	void WriteStartArray()	{ stream_.Put('['); }
	void WriteEndArray()	{ stream_.Put(']'); }

	void Prefix(Type type) {
		(void)type;
		if (level_stack_.GetSize() != 0) { // this value is not at root
			Level* level = level_stack_.template Top<Level>();
			if (level->valueCount > 0) {
				if (level->inArray) 
					stream_.Put(','); // add comma if it is not the first element in array
				else  // in object
					stream_.Put((level->valueCount % 2 == 0) ? ',' : ':');
			}
			if (!level->inArray && level->valueCount % 2 == 0)
				RAPIDJSON_ASSERT(type == kStringType);  // if it's in object, then even number should be a name
			level->valueCount++;
		}
		else
			RAPIDJSON_ASSERT(type == kObjectType || type == kArrayType);
	}

	Stream& stream_;
	internal::Stack<Allocator> level_stack_;

private:
	// Prohibit assignment for VC C4512 warning
	Writer& operator=(const Writer& w);
};

} // namespace rapidjson

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // RAPIDJSON_RAPIDJSON_H_
