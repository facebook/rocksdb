#ifndef RAPIDJSON_DOCUMENT_H_
#define RAPIDJSON_DOCUMENT_H_

#include "reader.h"
#include "internal/strfunc.h"
#include <new>		// placement new

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4127) // conditional expression is constant
#endif

namespace rapidjson {

///////////////////////////////////////////////////////////////////////////////
// GenericValue

//! Represents a JSON value. Use Value for UTF8 encoding and default allocator.
/*!
	A JSON value can be one of 7 types. This class is a variant type supporting
	these types.

	Use the Value if UTF8 and default allocator

	\tparam Encoding	Encoding of the value. (Even non-string values need to have the same encoding in a document)
	\tparam Allocator	Allocator type for allocating memory of object, array and string.
*/
#pragma pack (push, 4)
template <typename Encoding, typename Allocator = MemoryPoolAllocator<> > 
class GenericValue {
public:
	//! Name-value pair in an object.
	struct Member { 
		GenericValue<Encoding, Allocator> name;		//!< name of member (must be a string)
		GenericValue<Encoding, Allocator> value;	//!< value of member.
	};

	typedef Encoding EncodingType;					//!< Encoding type from template parameter.
	typedef Allocator AllocatorType;				//!< Allocator type from template parameter.
	typedef typename Encoding::Ch Ch;				//!< Character type derived from Encoding.
	typedef Member* MemberIterator;					//!< Member iterator for iterating in object.
	typedef const Member* ConstMemberIterator;		//!< Constant member iterator for iterating in object.
	typedef GenericValue* ValueIterator;			//!< Value iterator for iterating in array.
	typedef const GenericValue* ConstValueIterator;	//!< Constant value iterator for iterating in array.

	//!@name Constructors and destructor.
	//@{

	//! Default constructor creates a null value.
	GenericValue() : flags_(kNullFlag) {}

	//! Copy constructor is not permitted.
private:
	GenericValue(const GenericValue& rhs);

public:

	//! Constructor with JSON value type.
	/*! This creates a Value of specified type with default content.
		\param type	Type of the value.
		\note Default content for number is zero.
	*/
	GenericValue(Type type) {
		static const unsigned defaultFlags[7] = {
			kNullFlag, kFalseFlag, kTrueFlag, kObjectFlag, kArrayFlag, kConstStringFlag,
			kNumberFlag | kIntFlag | kUintFlag | kInt64Flag | kUint64Flag | kDoubleFlag
		};
		RAPIDJSON_ASSERT(type <= kNumberType);
		flags_ = defaultFlags[type];
		memset(&data_, 0, sizeof(data_));
	}

	//! Constructor for boolean value.
	GenericValue(bool b) : flags_(b ? kTrueFlag : kFalseFlag) {}

	//! Constructor for int value.
	GenericValue(int i) : flags_(kNumberIntFlag) { 
		data_.n.i64 = i;
		if (i >= 0)
			flags_ |= kUintFlag | kUint64Flag;
	}

	//! Constructor for unsigned value.
	GenericValue(unsigned u) : flags_(kNumberUintFlag) {
		data_.n.u64 = u; 
		if (!(u & 0x80000000))
			flags_ |= kIntFlag | kInt64Flag;
	}

	//! Constructor for int64_t value.
	GenericValue(int64_t i64) : flags_(kNumberInt64Flag) {
		data_.n.i64 = i64;
		if (i64 >= 0) {
			flags_ |= kNumberUint64Flag;
			if (!(i64 & 0xFFFFFFFF00000000LL))
				flags_ |= kUintFlag;
			if (!(i64 & 0xFFFFFFFF80000000LL))
				flags_ |= kIntFlag;
		}
		else if (i64 >= -2147483648LL)
			flags_ |= kIntFlag;
	}

	//! Constructor for uint64_t value.
	GenericValue(uint64_t u64) : flags_(kNumberUint64Flag) {
		data_.n.u64 = u64;
		if (!(u64 & 0x8000000000000000ULL))
			flags_ |= kInt64Flag;
		if (!(u64 & 0xFFFFFFFF00000000ULL))
			flags_ |= kUintFlag;
		if (!(u64 & 0xFFFFFFFF80000000ULL))
			flags_ |= kIntFlag;
	}

	//! Constructor for double value.
	GenericValue(double d) : flags_(kNumberDoubleFlag) { data_.n.d = d; }

	//! Constructor for constant string (i.e. do not make a copy of string)
	GenericValue(const Ch* s, SizeType length) { 
		RAPIDJSON_ASSERT(s != NULL);
		flags_ = kConstStringFlag;
		data_.s.str = s;
		data_.s.length = length;
	}

	//! Constructor for constant string (i.e. do not make a copy of string)
	GenericValue(const Ch* s) { SetStringRaw(s, internal::StrLen(s)); }

	//! Constructor for copy-string (i.e. do make a copy of string)
	GenericValue(const Ch* s, SizeType length, Allocator& allocator) { SetStringRaw(s, length, allocator); }

	//! Constructor for copy-string (i.e. do make a copy of string)
	GenericValue(const Ch*s, Allocator& allocator) { SetStringRaw(s, internal::StrLen(s), allocator); }

	//! Destructor.
	/*! Need to destruct elements of array, members of object, or copy-string.
	*/
	~GenericValue() {
		if (Allocator::kNeedFree) {	// Shortcut by Allocator's trait
			switch(flags_) {
			case kArrayFlag:
				for (GenericValue* v = data_.a.elements; v != data_.a.elements + data_.a.size; ++v)
					v->~GenericValue();
				Allocator::Free(data_.a.elements);
				break;

			case kObjectFlag:
				for (Member* m = data_.o.members; m != data_.o.members + data_.o.size; ++m) {
					m->name.~GenericValue();
					m->value.~GenericValue();
				}
				Allocator::Free(data_.o.members);
				break;

			case kCopyStringFlag:
				Allocator::Free(const_cast<Ch*>(data_.s.str));
				break;
			}
		}
	}

	//@}

	//!@name Assignment operators
	//@{

	//! Assignment with move semantics.
	/*! \param rhs Source of the assignment. It will become a null value after assignment.
	*/
	GenericValue& operator=(GenericValue& rhs) {
		RAPIDJSON_ASSERT(this != &rhs);
		this->~GenericValue();
		memcpy(this, &rhs, sizeof(GenericValue));
		rhs.flags_ = kNullFlag;
		return *this;
	}

	//! Assignment with primitive types.
	/*! \tparam T Either Type, int, unsigned, int64_t, uint64_t, const Ch*
		\param value The value to be assigned.
	*/
	template <typename T>
	GenericValue& operator=(T value) {
		this->~GenericValue();
		new (this) GenericValue(value);
		return *this;
	}
	//@}

	//!@name Type
	//@{

	Type GetType()	const { return static_cast<Type>(flags_ & kTypeMask); }
	bool IsNull()	const { return flags_ == kNullFlag; }
	bool IsFalse()	const { return flags_ == kFalseFlag; }
	bool IsTrue()	const { return flags_ == kTrueFlag; }
	bool IsBool()	const { return (flags_ & kBoolFlag) != 0; }
	bool IsObject()	const { return flags_ == kObjectFlag; }
	bool IsArray()	const { return flags_ == kArrayFlag; }
	bool IsNumber() const { return (flags_ & kNumberFlag) != 0; }
	bool IsInt()	const { return (flags_ & kIntFlag) != 0; }
	bool IsUint()	const { return (flags_ & kUintFlag) != 0; }
	bool IsInt64()	const { return (flags_ & kInt64Flag) != 0; }
	bool IsUint64()	const { return (flags_ & kUint64Flag) != 0; }
	bool IsDouble() const { return (flags_ & kDoubleFlag) != 0; }
	bool IsString() const { return (flags_ & kStringFlag) != 0; }

	//@}

	//!@name Null
	//@{

	GenericValue& SetNull() { this->~GenericValue(); new (this) GenericValue(); return *this; }

	//@}

	//!@name Bool
	//@{

	bool GetBool() const { RAPIDJSON_ASSERT(IsBool()); return flags_ == kTrueFlag; }
	GenericValue& SetBool(bool b) { this->~GenericValue(); new (this) GenericValue(b); return *this; }

	//@}

	//!@name Object
	//@{

	//! Set this value as an empty object.
	GenericValue& SetObject() { this->~GenericValue(); new (this) GenericValue(kObjectType); return *this; }

	//! Get the value associated with the object's name.
	GenericValue& operator[](const Ch* name) {
		if (Member* member = FindMember(name))
			return member->value;
		else {
			static GenericValue NullValue;
			return NullValue;
		}
	}
	const GenericValue& operator[](const Ch* name) const { return const_cast<GenericValue&>(*this)[name]; }

	//! Member iterators.
	ConstMemberIterator MemberBegin() const	{ RAPIDJSON_ASSERT(IsObject()); return data_.o.members; }
	ConstMemberIterator MemberEnd()	const	{ RAPIDJSON_ASSERT(IsObject()); return data_.o.members + data_.o.size; }
	MemberIterator MemberBegin()			{ RAPIDJSON_ASSERT(IsObject()); return data_.o.members; }
	MemberIterator MemberEnd()				{ RAPIDJSON_ASSERT(IsObject()); return data_.o.members + data_.o.size; }

	//! Check whether a member exists in the object.
	bool HasMember(const Ch* name) const { return FindMember(name) != 0; }

	//! Add a member (name-value pair) to the object.
	/*! \param name A string value as name of member.
		\param value Value of any type.
	    \param allocator Allocator for reallocating memory.
	    \return The value itself for fluent API.
	    \note The ownership of name and value will be transfered to this object if success.
	*/
	GenericValue& AddMember(GenericValue& name, GenericValue& value, Allocator& allocator) {
		RAPIDJSON_ASSERT(IsObject());
		RAPIDJSON_ASSERT(name.IsString());
		Object& o = data_.o;
		if (o.size >= o.capacity) {
			if (o.capacity == 0) {
				o.capacity = kDefaultObjectCapacity;
				o.members = (Member*)allocator.Malloc(o.capacity * sizeof(Member));
			}
			else {
				SizeType oldCapacity = o.capacity;
				o.capacity *= 2;
				o.members = (Member*)allocator.Realloc(o.members, oldCapacity * sizeof(Member), o.capacity * sizeof(Member));
			}
		}
		o.members[o.size].name.RawAssign(name);
		o.members[o.size].value.RawAssign(value);
		o.size++;
		return *this;
	}

	GenericValue& AddMember(const Ch* name, Allocator& nameAllocator, GenericValue& value, Allocator& allocator) {
		GenericValue n(name, internal::StrLen(name), nameAllocator);
		return AddMember(n, value, allocator);
	}

	GenericValue& AddMember(const Ch* name, GenericValue& value, Allocator& allocator) {
		GenericValue n(name, internal::StrLen(name));
		return AddMember(n, value, allocator);
	}

	template <typename T>
	GenericValue& AddMember(const Ch* name, T value, Allocator& allocator) {
		GenericValue n(name, internal::StrLen(name));
		GenericValue v(value);
		return AddMember(n, v, allocator);
	}

	//! Remove a member in object by its name.
	/*! \param name Name of member to be removed.
	    \return Whether the member existed.
	    \note Removing member is implemented by moving the last member. So the ordering of members is changed.
	*/
	bool RemoveMember(const Ch* name) {
		RAPIDJSON_ASSERT(IsObject());
		if (Member* m = FindMember(name)) {
			RAPIDJSON_ASSERT(data_.o.size > 0);
			RAPIDJSON_ASSERT(data_.o.members != 0);

			Member* last = data_.o.members + (data_.o.size - 1);
			if (data_.o.size > 1 && m != last) {
				// Move the last one to this place
				m->name = last->name;
				m->value = last->value;
			}
			else {
				// Only one left, just destroy
				m->name.~GenericValue();
				m->value.~GenericValue();
			}
			--data_.o.size;
			return true;
		}
		return false;
	}

	//@}

	//!@name Array
	//@{

	//! Set this value as an empty array.
	GenericValue& SetArray() {	this->~GenericValue(); new (this) GenericValue(kArrayType); return *this; }

	//! Get the number of elements in array.
	SizeType Size() const { RAPIDJSON_ASSERT(IsArray()); return data_.a.size; }

	//! Get the capacity of array.
	SizeType Capacity() const { RAPIDJSON_ASSERT(IsArray()); return data_.a.capacity; }

	//! Check whether the array is empty.
	bool Empty() const { RAPIDJSON_ASSERT(IsArray()); return data_.a.size == 0; }

	//! Remove all elements in the array.
	/*! This function do not deallocate memory in the array, i.e. the capacity is unchanged.
	*/
	void Clear() {
		RAPIDJSON_ASSERT(IsArray()); 
		for (SizeType i = 0; i < data_.a.size; ++i)
			data_.a.elements[i].~GenericValue();
		data_.a.size = 0;
	}

	//! Get an element from array by index.
	/*! \param index Zero-based index of element.
		\note
\code
Value a(kArrayType);
a.PushBack(123);
int x = a[0].GetInt();				// Error: operator[ is ambiguous, as 0 also mean a null pointer of const char* type.
int y = a[SizeType(0)].GetInt();	// Cast to SizeType will work.
int z = a[0u].GetInt();				// This works too.
\endcode
	*/
	GenericValue& operator[](SizeType index) {
		RAPIDJSON_ASSERT(IsArray());
		RAPIDJSON_ASSERT(index < data_.a.size);
		return data_.a.elements[index];
	}
	const GenericValue& operator[](SizeType index) const { return const_cast<GenericValue&>(*this)[index]; }

	//! Element iterator
	ValueIterator Begin() { RAPIDJSON_ASSERT(IsArray()); return data_.a.elements; }
	ValueIterator End() { RAPIDJSON_ASSERT(IsArray()); return data_.a.elements + data_.a.size; }
	ConstValueIterator Begin() const { return const_cast<GenericValue&>(*this).Begin(); }
	ConstValueIterator End() const { return const_cast<GenericValue&>(*this).End(); }

	//! Request the array to have enough capacity to store elements.
	/*! \param newCapacity	The capacity that the array at least need to have.
		\param allocator	The allocator for allocating memory. It must be the same one use previously.
		\return The value itself for fluent API.
	*/
	GenericValue& Reserve(SizeType newCapacity, Allocator &allocator) {
		RAPIDJSON_ASSERT(IsArray());
		if (newCapacity > data_.a.capacity) {
			data_.a.elements = (GenericValue*)allocator.Realloc(data_.a.elements, data_.a.capacity * sizeof(GenericValue), newCapacity * sizeof(GenericValue));
			data_.a.capacity = newCapacity;
		}
		return *this;
	}

	//! Append a value at the end of the array.
	/*! \param value		The value to be appended.
	    \param allocator	The allocator for allocating memory. It must be the same one use previously.
	    \return The value itself for fluent API.
	    \note The ownership of the value will be transfered to this object if success.
	    \note If the number of elements to be appended is known, calls Reserve() once first may be more efficient.
	*/
	GenericValue& PushBack(GenericValue& value, Allocator& allocator) {
		RAPIDJSON_ASSERT(IsArray());
		if (data_.a.size >= data_.a.capacity)
			Reserve(data_.a.capacity == 0 ? kDefaultArrayCapacity : data_.a.capacity * 2, allocator);
		data_.a.elements[data_.a.size++].RawAssign(value);
		return *this;
	}

	template <typename T>
	GenericValue& PushBack(T value, Allocator& allocator) {
		GenericValue v(value);
		return PushBack(v, allocator);
	}

	//! Remove the last element in the array.
	GenericValue& PopBack() {
		RAPIDJSON_ASSERT(IsArray());
		RAPIDJSON_ASSERT(!Empty());
		data_.a.elements[--data_.a.size].~GenericValue();
		return *this;
	}
	//@}

	//!@name Number
	//@{

	int GetInt() const			{ RAPIDJSON_ASSERT(flags_ & kIntFlag);   return data_.n.i.i;   }
	unsigned GetUint() const	{ RAPIDJSON_ASSERT(flags_ & kUintFlag);  return data_.n.u.u;   }
	int64_t GetInt64() const	{ RAPIDJSON_ASSERT(flags_ & kInt64Flag); return data_.n.i64; }
	uint64_t GetUint64() const	{ RAPIDJSON_ASSERT(flags_ & kUint64Flag); return data_.n.u64; }

	double GetDouble() const {
		RAPIDJSON_ASSERT(IsNumber());
		if ((flags_ & kDoubleFlag) != 0)				return data_.n.d;	// exact type, no conversion.
		if ((flags_ & kIntFlag) != 0)					return data_.n.i.i;	// int -> double
		if ((flags_ & kUintFlag) != 0)					return data_.n.u.u;	// unsigned -> double
		if ((flags_ & kInt64Flag) != 0)					return (double)data_.n.i64; // int64_t -> double (may lose precision)
		RAPIDJSON_ASSERT((flags_ & kUint64Flag) != 0);	return (double)data_.n.u64;	// uint64_t -> double (may lose precision)
	}

	GenericValue& SetInt(int i)				{ this->~GenericValue(); new (this) GenericValue(i);	return *this; }
	GenericValue& SetUint(unsigned u)		{ this->~GenericValue(); new (this) GenericValue(u);	return *this; }
	GenericValue& SetInt64(int64_t i64)		{ this->~GenericValue(); new (this) GenericValue(i64);	return *this; }
	GenericValue& SetUint64(uint64_t u64)	{ this->~GenericValue(); new (this) GenericValue(u64);	return *this; }
	GenericValue& SetDouble(double d)		{ this->~GenericValue(); new (this) GenericValue(d);	return *this; }

	//@}

	//!@name String
	//@{

	const Ch* GetString() const { RAPIDJSON_ASSERT(IsString()); return data_.s.str; }

	//! Get the length of string.
	/*! Since rapidjson permits "\u0000" in the json string, strlen(v.GetString()) may not equal to v.GetStringLength().
	*/
	SizeType GetStringLength() const { RAPIDJSON_ASSERT(IsString()); return data_.s.length; }

	//! Set this value as a string without copying source string.
	/*! This version has better performance with supplied length, and also support string containing null character.
		\param s source string pointer. 
		\param length The length of source string, excluding the trailing null terminator.
		\return The value itself for fluent API.
	*/
	GenericValue& SetString(const Ch* s, SizeType length) { this->~GenericValue(); SetStringRaw(s, length); return *this; }

	//! Set this value as a string without copying source string.
	/*! \param s source string pointer. 
		\return The value itself for fluent API.
	*/
	GenericValue& SetString(const Ch* s) { return SetString(s, internal::StrLen(s)); }

	//! Set this value as a string by copying from source string.
	/*! This version has better performance with supplied length, and also support string containing null character.
		\param s source string. 
		\param length The length of source string, excluding the trailing null terminator.
		\param allocator Allocator for allocating copied buffer. Commonly use document.GetAllocator().
		\return The value itself for fluent API.
	*/
	GenericValue& SetString(const Ch* s, SizeType length, Allocator& allocator) { this->~GenericValue(); SetStringRaw(s, length, allocator); return *this; }

	//! Set this value as a string by copying from source string.
	/*!	\param s source string. 
		\param allocator Allocator for allocating copied buffer. Commonly use document.GetAllocator().
		\return The value itself for fluent API.
	*/
	GenericValue& SetString(const Ch* s, Allocator& allocator) {	SetString(s, internal::StrLen(s), allocator); return *this; }

	//@}

	//! Generate events of this value to a Handler.
	/*! This function adopts the GoF visitor pattern.
		Typical usage is to output this JSON value as JSON text via Writer, which is a Handler.
		It can also be used to deep clone this value via GenericDocument, which is also a Handler.
		\tparam Handler type of handler.
		\param handler An object implementing concept Handler.
	*/
	template <typename Handler>
	const GenericValue& Accept(Handler& handler) const {
		switch(GetType()) {
		case kNullType:		handler.Null(); break;
		case kFalseType:	handler.Bool(false); break;
		case kTrueType:		handler.Bool(true); break;

		case kObjectType:
			handler.StartObject();
			for (Member* m = data_.o.members; m != data_.o.members + data_.o.size; ++m) {
				handler.String(m->name.data_.s.str, m->name.data_.s.length, false);
				m->value.Accept(handler);
			}
			handler.EndObject(data_.o.size);
			break;

		case kArrayType:
			handler.StartArray();
			for (GenericValue* v = data_.a.elements; v != data_.a.elements + data_.a.size; ++v)
				v->Accept(handler);
			handler.EndArray(data_.a.size);
			break;

		case kStringType:
			handler.String(data_.s.str, data_.s.length, false);
			break;

		case kNumberType:
			if (IsInt())			handler.Int(data_.n.i.i);
			else if (IsUint())		handler.Uint(data_.n.u.u);
			else if (IsInt64())		handler.Int64(data_.n.i64);
			else if (IsUint64())	handler.Uint64(data_.n.u64);
			else					handler.Double(data_.n.d);
			break;
		}
		return *this;
	}

private:
	template <typename, typename>
	friend class GenericDocument;

	enum {
		kBoolFlag = 0x100,
		kNumberFlag = 0x200,
		kIntFlag = 0x400,
		kUintFlag = 0x800,
		kInt64Flag = 0x1000,
		kUint64Flag = 0x2000,
		kDoubleFlag = 0x4000,
		kStringFlag = 0x100000,
		kCopyFlag = 0x200000,

		// Initial flags of different types.
		kNullFlag = kNullType,
		kTrueFlag = kTrueType | kBoolFlag,
		kFalseFlag = kFalseType | kBoolFlag,
		kNumberIntFlag = kNumberType | kNumberFlag | kIntFlag | kInt64Flag,
		kNumberUintFlag = kNumberType | kNumberFlag | kUintFlag | kUint64Flag | kInt64Flag,
		kNumberInt64Flag = kNumberType | kNumberFlag | kInt64Flag,
		kNumberUint64Flag = kNumberType | kNumberFlag | kUint64Flag,
		kNumberDoubleFlag = kNumberType | kNumberFlag | kDoubleFlag,
		kConstStringFlag = kStringType | kStringFlag,
		kCopyStringFlag = kStringType | kStringFlag | kCopyFlag,
		kObjectFlag = kObjectType,
		kArrayFlag = kArrayType,

		kTypeMask = 0xFF	// bitwise-and with mask of 0xFF can be optimized by compiler
	};

	static const SizeType kDefaultArrayCapacity = 16;
	static const SizeType kDefaultObjectCapacity = 16;

	struct String {
		const Ch* str;
		SizeType length;
		unsigned hashcode;	//!< reserved
	};	// 12 bytes in 32-bit mode, 16 bytes in 64-bit mode

	// By using proper binary layout, retrieval of different integer types do not need conversions.
	union Number {
#if RAPIDJSON_ENDIAN == RAPIDJSON_LITTLEENDIAN
		struct I {
			int i;
			char padding[4];
		}i;
		struct U {
			unsigned u;
			char padding2[4];
		}u;
#else
		struct I {
			char padding[4];
			int i;
		}i;
		struct U {
			char padding2[4];
			unsigned u;
		}u;
#endif
		int64_t i64;
		uint64_t u64;
		double d;
	};	// 8 bytes

	struct Object {
		Member* members;
		SizeType size;
		SizeType capacity;
	};	// 12 bytes in 32-bit mode, 16 bytes in 64-bit mode

	struct Array {
		GenericValue<Encoding, Allocator>* elements;
		SizeType size;
		SizeType capacity;
	};	// 12 bytes in 32-bit mode, 16 bytes in 64-bit mode

	union Data {
		String s;
		Number n;
		Object o;
		Array a;
	};	// 12 bytes in 32-bit mode, 16 bytes in 64-bit mode

	//! Find member by name.
	Member* FindMember(const Ch* name) {
		RAPIDJSON_ASSERT(name);
		RAPIDJSON_ASSERT(IsObject());

		SizeType length = internal::StrLen(name);

		Object& o = data_.o;
		for (Member* member = o.members; member != data_.o.members + data_.o.size; ++member)
			if (length == member->name.data_.s.length && memcmp(member->name.data_.s.str, name, length * sizeof(Ch)) == 0)
				return member;

		return 0;
	}
	const Member* FindMember(const Ch* name) const { return const_cast<GenericValue&>(*this).FindMember(name); }

	// Initialize this value as array with initial data, without calling destructor.
	void SetArrayRaw(GenericValue* values, SizeType count, Allocator& alloctaor) {
		flags_ = kArrayFlag;
		data_.a.elements = (GenericValue*)alloctaor.Malloc(count * sizeof(GenericValue));
		memcpy(data_.a.elements, values, count * sizeof(GenericValue));
		data_.a.size = data_.a.capacity = count;
	}

	//! Initialize this value as object with initial data, without calling destructor.
	void SetObjectRaw(Member* members, SizeType count, Allocator& alloctaor) {
		flags_ = kObjectFlag;
		data_.o.members = (Member*)alloctaor.Malloc(count * sizeof(Member));
		memcpy(data_.o.members, members, count * sizeof(Member));
		data_.o.size = data_.o.capacity = count;
	}

	//! Initialize this value as constant string, without calling destructor.
	void SetStringRaw(const Ch* s, SizeType length) {
		RAPIDJSON_ASSERT(s != NULL);
		flags_ = kConstStringFlag;
		data_.s.str = s;
		data_.s.length = length;
	}

	//! Initialize this value as copy string with initial data, without calling destructor.
	void SetStringRaw(const Ch* s, SizeType length, Allocator& allocator) {
		RAPIDJSON_ASSERT(s != NULL);
		flags_ = kCopyStringFlag;
		data_.s.str = (Ch *)allocator.Malloc((length + 1) * sizeof(Ch));
		data_.s.length = length;
		memcpy(const_cast<Ch*>(data_.s.str), s, length * sizeof(Ch));
		const_cast<Ch*>(data_.s.str)[length] = '\0';
	}

	//! Assignment without calling destructor
	void RawAssign(GenericValue& rhs) {
		memcpy(this, &rhs, sizeof(GenericValue));
		rhs.flags_ = kNullFlag;
	}

	Data data_;
	unsigned flags_;
};
#pragma pack (pop)

//! Value with UTF8 encoding.
typedef GenericValue<UTF8<> > Value;

///////////////////////////////////////////////////////////////////////////////
// GenericDocument 

//! A document for parsing JSON text as DOM.
/*!
	\implements Handler
	\tparam Encoding encoding for both parsing and string storage.
	\tparam Alloactor allocator for allocating memory for the DOM, and the stack during parsing.
*/
template <typename Encoding, typename Allocator = MemoryPoolAllocator<> >
class GenericDocument : public GenericValue<Encoding, Allocator> {
public:
	typedef typename Encoding::Ch Ch;						//!< Character type derived from Encoding.
	typedef GenericValue<Encoding, Allocator> ValueType;	//!< Value type of the document.
	typedef Allocator AllocatorType;						//!< Allocator type from template parameter.

	//! Constructor
	/*! \param allocator		Optional allocator for allocating stack memory.
		\param stackCapacity	Initial capacity of stack in bytes.
	*/
	GenericDocument(Allocator* allocator = 0, size_t stackCapacity = kDefaultStackCapacity) : stack_(allocator, stackCapacity), parseError_(0), errorOffset_(0) {}

	//! Parse JSON text from an input stream.
	/*! \tparam parseFlags Combination of ParseFlag.
		\param stream Input stream to be parsed.
		\return The document itself for fluent API.
	*/
	template <unsigned parseFlags, typename Stream>
	GenericDocument& ParseStream(Stream& stream) {
		ValueType::SetNull(); // Remove existing root if exist
		GenericReader<Encoding, Allocator> reader;
		if (reader.template Parse<parseFlags>(stream, *this)) {
			RAPIDJSON_ASSERT(stack_.GetSize() == sizeof(ValueType)); // Got one and only one root object
			this->RawAssign(*stack_.template Pop<ValueType>(1));	// Add this-> to prevent issue 13.
			parseError_ = 0;
			errorOffset_ = 0;
		}
		else {
			parseError_ = reader.GetParseError();
			errorOffset_ = reader.GetErrorOffset();
			ClearStack();
		}
		return *this;
	}

	//! Parse JSON text from a mutable string.
	/*! \tparam parseFlags Combination of ParseFlag.
		\param str Mutable zero-terminated string to be parsed.
		\return The document itself for fluent API.
	*/
	template <unsigned parseFlags>
	GenericDocument& ParseInsitu(Ch* str) {
		GenericInsituStringStream<Encoding> s(str);
		return ParseStream<parseFlags | kParseInsituFlag>(s);
	}

	//! Parse JSON text from a read-only string.
	/*! \tparam parseFlags Combination of ParseFlag (must not contain kParseInsituFlag).
		\param str Read-only zero-terminated string to be parsed.
	*/
	template <unsigned parseFlags>
	GenericDocument& Parse(const Ch* str) {
		RAPIDJSON_ASSERT(!(parseFlags & kParseInsituFlag));
		GenericStringStream<Encoding> s(str);
		return ParseStream<parseFlags>(s);
	}

	//! Whether a parse error was occured in the last parsing.
	bool HasParseError() const { return parseError_ != 0; }

	//! Get the message of parsing error.
	const char* GetParseError() const { return parseError_; }

	//! Get the offset in character of the parsing error.
	size_t GetErrorOffset() const { return errorOffset_; }

	//! Get the allocator of this document.
	Allocator& GetAllocator() {	return stack_.GetAllocator(); }

	//! Get the capacity of stack in bytes.
	size_t GetStackCapacity() const { return stack_.GetCapacity(); }

private:
	// Prohibit assignment
	GenericDocument& operator=(const GenericDocument&);

	friend class GenericReader<Encoding, Allocator>;	// for Reader to call the following private handler functions

	// Implementation of Handler
	void Null()	{ new (stack_.template Push<ValueType>()) ValueType(); }
	void Bool(bool b) { new (stack_.template Push<ValueType>()) ValueType(b); }
	void Int(int i) { new (stack_.template Push<ValueType>()) ValueType(i); }
	void Uint(unsigned i) { new (stack_.template Push<ValueType>()) ValueType(i); }
	void Int64(int64_t i) { new (stack_.template Push<ValueType>()) ValueType(i); }
	void Uint64(uint64_t i) { new (stack_.template Push<ValueType>()) ValueType(i); }
	void Double(double d) { new (stack_.template Push<ValueType>()) ValueType(d); }

	void String(const Ch* str, SizeType length, bool copy) { 
		if (copy) 
			new (stack_.template Push<ValueType>()) ValueType(str, length, GetAllocator());
		else
			new (stack_.template Push<ValueType>()) ValueType(str, length);
	}

	void StartObject() { new (stack_.template Push<ValueType>()) ValueType(kObjectType); }
	
	void EndObject(SizeType memberCount) {
		typename ValueType::Member* members = stack_.template Pop<typename ValueType::Member>(memberCount);
		stack_.template Top<ValueType>()->SetObjectRaw(members, (SizeType)memberCount, GetAllocator());
	}

	void StartArray() { new (stack_.template Push<ValueType>()) ValueType(kArrayType); }
	
	void EndArray(SizeType elementCount) {
		ValueType* elements = stack_.template Pop<ValueType>(elementCount);
		stack_.template Top<ValueType>()->SetArrayRaw(elements, elementCount, GetAllocator());
	}

	void ClearStack() {
		if (Allocator::kNeedFree)
			while (stack_.GetSize() > 0)	// Here assumes all elements in stack array are GenericValue (Member is actually 2 GenericValue objects)
				(stack_.template Pop<ValueType>(1))->~ValueType();
		else
			stack_.Clear();
	}

	static const size_t kDefaultStackCapacity = 1024;
	internal::Stack<Allocator> stack_;
	const char* parseError_;
	size_t errorOffset_;
};

typedef GenericDocument<UTF8<> > Document;

} // namespace rapidjson

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // RAPIDJSON_DOCUMENT_H_
