#ifndef RAPIDJSON_PRETTYWRITER_H_
#define RAPIDJSON_PRETTYWRITER_H_

#include "writer.h"

namespace rapidjson {

//! Writer with indentation and spacing.
/*!
	\tparam Stream Type of ouptut stream.
	\tparam Encoding Encoding of both source strings and output.
	\tparam Allocator Type of allocator for allocating memory of stack.
*/
template<typename Stream, typename Encoding = UTF8<>, typename Allocator = MemoryPoolAllocator<> >
class PrettyWriter : public Writer<Stream, Encoding, Allocator> {
public:
	typedef Writer<Stream, Encoding, Allocator> Base;
	typedef typename Base::Ch Ch;

	//! Constructor
	/*! \param stream Output stream.
		\param allocator User supplied allocator. If it is null, it will create a private one.
		\param levelDepth Initial capacity of 
	*/
	PrettyWriter(Stream& stream, Allocator* allocator = 0, size_t levelDepth = Base::kDefaultLevelDepth) : 
		Base(stream, allocator, levelDepth), indentChar_(' '), indentCharCount_(4) {}

	//! Set custom indentation.
	/*! \param indentChar		Character for indentation. Must be whitespace character (' ', '\t', '\n', '\r').
		\param indentCharCount	Number of indent characters for each indentation level.
		\note The default indentation is 4 spaces.
	*/
	PrettyWriter& SetIndent(Ch indentChar, unsigned indentCharCount) {
		RAPIDJSON_ASSERT(indentChar == ' ' || indentChar == '\t' || indentChar == '\n' || indentChar == '\r');
		indentChar_ = indentChar;
		indentCharCount_ = indentCharCount;
		return *this;
	}

	//@name Implementation of Handler.
	//@{

	PrettyWriter& Null()				{ PrettyPrefix(kNullType);   Base::WriteNull();			return *this; }
	PrettyWriter& Bool(bool b)			{ PrettyPrefix(b ? kTrueType : kFalseType); Base::WriteBool(b); return *this; }
	PrettyWriter& Int(int i)			{ PrettyPrefix(kNumberType); Base::WriteInt(i);			return *this; }
	PrettyWriter& Uint(unsigned u)		{ PrettyPrefix(kNumberType); Base::WriteUint(u);		return *this; }
	PrettyWriter& Int64(int64_t i64)	{ PrettyPrefix(kNumberType); Base::WriteInt64(i64);		return *this; }
	PrettyWriter& Uint64(uint64_t u64)	{ PrettyPrefix(kNumberType); Base::WriteUint64(u64);	return *this; }
	PrettyWriter& Double(double d)		{ PrettyPrefix(kNumberType); Base::WriteDouble(d);		return *this; }

	PrettyWriter& String(const Ch* str, SizeType length, bool copy = false) {
		(void)copy;
		PrettyPrefix(kStringType);
		Base::WriteString(str, length);
		return *this;
	}

	PrettyWriter& StartObject() {
		PrettyPrefix(kObjectType);
		new (Base::level_stack_.template Push<typename Base::Level>()) typename Base::Level(false);
		Base::WriteStartObject();
		return *this;
	}

	PrettyWriter& EndObject(SizeType memberCount = 0) {
		(void)memberCount;
		RAPIDJSON_ASSERT(Base::level_stack_.GetSize() >= sizeof(typename Base::Level));
		RAPIDJSON_ASSERT(!Base::level_stack_.template Top<typename Base::Level>()->inArray);
		bool empty = Base::level_stack_.template Pop<typename Base::Level>(1)->valueCount == 0;

		if (!empty) {
			Base::stream_.Put('\n');
			WriteIndent();
		}
		Base::WriteEndObject();
		return *this;
	}

	PrettyWriter& StartArray() {
		PrettyPrefix(kArrayType);
		new (Base::level_stack_.template Push<typename Base::Level>()) typename Base::Level(true);
		Base::WriteStartArray();
		return *this;
	}

	PrettyWriter& EndArray(SizeType memberCount = 0) {
		(void)memberCount;
		RAPIDJSON_ASSERT(Base::level_stack_.GetSize() >= sizeof(typename Base::Level));
		RAPIDJSON_ASSERT(Base::level_stack_.template Top<typename Base::Level>()->inArray);
		bool empty = Base::level_stack_.template Pop<typename Base::Level>(1)->valueCount == 0;

		if (!empty) {
			Base::stream_.Put('\n');
			WriteIndent();
		}
		Base::WriteEndArray();
		return *this;
	}

	//@}

	//! Simpler but slower overload.
	PrettyWriter& String(const Ch* str) { return String(str, internal::StrLen(str)); }

protected:
	void PrettyPrefix(Type type) {
		(void)type;
		if (Base::level_stack_.GetSize() != 0) { // this value is not at root
			typename Base::Level* level = Base::level_stack_.template Top<typename Base::Level>();

			if (level->inArray) {
				if (level->valueCount > 0) {
					Base::stream_.Put(','); // add comma if it is not the first element in array
					Base::stream_.Put('\n');
				}
				else
					Base::stream_.Put('\n');
				WriteIndent();
			}
			else {	// in object
				if (level->valueCount > 0) {
					if (level->valueCount % 2 == 0) {
						Base::stream_.Put(',');
						Base::stream_.Put('\n');
					}
					else {
						Base::stream_.Put(':');
						Base::stream_.Put(' ');
					}
				}
				else
					Base::stream_.Put('\n');

				if (level->valueCount % 2 == 0)
					WriteIndent();
			}
			if (!level->inArray && level->valueCount % 2 == 0)
				RAPIDJSON_ASSERT(type == kStringType);  // if it's in object, then even number should be a name
			level->valueCount++;
		}
		else
			RAPIDJSON_ASSERT(type == kObjectType || type == kArrayType);
	}

	void WriteIndent()  {
		size_t count = (Base::level_stack_.GetSize() / sizeof(typename Base::Level)) * indentCharCount_;
		PutN(Base::stream_, indentChar_, count);
	}

	Ch indentChar_;
	unsigned indentCharCount_;
};

} // namespace rapidjson

#endif // RAPIDJSON_RAPIDJSON_H_
