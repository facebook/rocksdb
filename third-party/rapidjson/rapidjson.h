#ifndef RAPIDJSON_RAPIDJSON_H_
#define RAPIDJSON_RAPIDJSON_H_

// Copyright (c) 2011-2012 Milo Yip (miloyip@gmail.com)
// Version 0.11

#include <cstdlib>	// malloc(), realloc(), free()
#include <cstring>	// memcpy()

///////////////////////////////////////////////////////////////////////////////
// RAPIDJSON_NO_INT64DEFINE

// Here defines int64_t and uint64_t types in global namespace.
// If user have their own definition, can define RAPIDJSON_NO_INT64DEFINE to disable this.
#ifndef RAPIDJSON_NO_INT64DEFINE
#ifdef _MSC_VER
typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;
#else
#include <inttypes.h>
#endif
#endif // RAPIDJSON_NO_INT64TYPEDEF

///////////////////////////////////////////////////////////////////////////////
// RAPIDJSON_ENDIAN
#define RAPIDJSON_LITTLEENDIAN	0	//!< Little endian machine
#define RAPIDJSON_BIGENDIAN		1	//!< Big endian machine

//! Endianness of the machine.
/*!	GCC provided macro for detecting endianness of the target machine. But other
	compilers may not have this. User can define RAPIDJSON_ENDIAN to either
	RAPIDJSON_LITTLEENDIAN or RAPIDJSON_BIGENDIAN.
*/
#ifndef RAPIDJSON_ENDIAN
#ifdef __BYTE_ORDER__
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define RAPIDJSON_ENDIAN RAPIDJSON_LITTLEENDIAN
#else
#define RAPIDJSON_ENDIAN RAPIDJSON_BIGENDIAN
#endif // __BYTE_ORDER__
#else
#define RAPIDJSON_ENDIAN RAPIDJSON_LITTLEENDIAN	// Assumes little endian otherwise.
#endif
#endif // RAPIDJSON_ENDIAN

///////////////////////////////////////////////////////////////////////////////
// RAPIDJSON_SSE2/RAPIDJSON_SSE42/RAPIDJSON_SIMD

// Enable SSE2 optimization.
//#define RAPIDJSON_SSE2

// Enable SSE4.2 optimization.
//#define RAPIDJSON_SSE42

#if defined(RAPIDJSON_SSE2) || defined(RAPIDJSON_SSE42)
#define RAPIDJSON_SIMD
#endif

///////////////////////////////////////////////////////////////////////////////
// RAPIDJSON_NO_SIZETYPEDEFINE

#ifndef RAPIDJSON_NO_SIZETYPEDEFINE
namespace rapidjson {
//! Use 32-bit array/string indices even for 64-bit platform, instead of using size_t.
/*! User may override the SizeType by defining RAPIDJSON_NO_SIZETYPEDEFINE.
*/
typedef unsigned SizeType;
} // namespace rapidjson
#endif

///////////////////////////////////////////////////////////////////////////////
// RAPIDJSON_ASSERT

//! Assertion.
/*! By default, rapidjson uses C assert() for assertion.
	User can override it by defining RAPIDJSON_ASSERT(x) macro.
*/
#ifndef RAPIDJSON_ASSERT
#include <cassert>
#define RAPIDJSON_ASSERT(x) assert(x)
#endif // RAPIDJSON_ASSERT

///////////////////////////////////////////////////////////////////////////////
// Helpers

#define RAPIDJSON_MULTILINEMACRO_BEGIN do {  
#define RAPIDJSON_MULTILINEMACRO_END \
} while((void)0, 0)

namespace rapidjson {

///////////////////////////////////////////////////////////////////////////////
// Allocator

/*! \class rapidjson::Allocator
	\brief Concept for allocating, resizing and freeing memory block.
	
	Note that Malloc() and Realloc() are non-static but Free() is static.
	
	So if an allocator need to support Free(), it needs to put its pointer in 
	the header of memory block.

\code
concept Allocator {
	static const bool kNeedFree;	//!< Whether this allocator needs to call Free().

	// Allocate a memory block.
	// \param size of the memory block in bytes.
	// \returns pointer to the memory block.
	void* Malloc(size_t size);

	// Resize a memory block.
	// \param originalPtr The pointer to current memory block. Null pointer is permitted.
	// \param originalSize The current size in bytes. (Design issue: since some allocator may not book-keep this, explicitly pass to it can save memory.)
	// \param newSize the new size in bytes.
	void* Realloc(void* originalPtr, size_t originalSize, size_t newSize);

	// Free a memory block.
	// \param pointer to the memory block. Null pointer is permitted.
	static void Free(void *ptr);
};
\endcode
*/

///////////////////////////////////////////////////////////////////////////////
// CrtAllocator

//! C-runtime library allocator.
/*! This class is just wrapper for standard C library memory routines.
	\implements Allocator
*/
class CrtAllocator {
public:
	static const bool kNeedFree = true;
	void* Malloc(size_t size) { return malloc(size); }
	void* Realloc(void* originalPtr, size_t originalSize, size_t newSize) { (void)originalSize; return realloc(originalPtr, newSize); }
	static void Free(void *ptr) { free(ptr); }
};

///////////////////////////////////////////////////////////////////////////////
// MemoryPoolAllocator

//! Default memory allocator used by the parser and DOM.
/*! This allocator allocate memory blocks from pre-allocated memory chunks. 

    It does not free memory blocks. And Realloc() only allocate new memory.

    The memory chunks are allocated by BaseAllocator, which is CrtAllocator by default.

    User may also supply a buffer as the first chunk.

    If the user-buffer is full then additional chunks are allocated by BaseAllocator.

    The user-buffer is not deallocated by this allocator.

    \tparam BaseAllocator the allocator type for allocating memory chunks. Default is CrtAllocator.
	\implements Allocator
*/
template <typename BaseAllocator = CrtAllocator>
class MemoryPoolAllocator {
public:
	static const bool kNeedFree = false;	//!< Tell users that no need to call Free() with this allocator. (concept Allocator)

	//! Constructor with chunkSize.
	/*! \param chunkSize The size of memory chunk. The default is kDefaultChunkSize.
		\param baseAllocator The allocator for allocating memory chunks.
	*/
	MemoryPoolAllocator(size_t chunkSize = kDefaultChunkCapacity, BaseAllocator* baseAllocator = 0) : 
		chunkHead_(0), chunk_capacity_(chunkSize), userBuffer_(0), baseAllocator_(baseAllocator), ownBaseAllocator_(0)
	{
		if (!baseAllocator_)
			ownBaseAllocator_ = baseAllocator_ = new BaseAllocator();
		AddChunk(chunk_capacity_);
	}

	//! Constructor with user-supplied buffer.
	/*! The user buffer will be used firstly. When it is full, memory pool allocates new chunk with chunk size.

		The user buffer will not be deallocated when this allocator is destructed.

		\param buffer User supplied buffer.
		\param size Size of the buffer in bytes. It must at least larger than sizeof(ChunkHeader).
		\param chunkSize The size of memory chunk. The default is kDefaultChunkSize.
		\param baseAllocator The allocator for allocating memory chunks.
	*/
	MemoryPoolAllocator(char *buffer, size_t size, size_t chunkSize = kDefaultChunkCapacity, BaseAllocator* baseAllocator = 0) :
		chunkHead_(0), chunk_capacity_(chunkSize), userBuffer_(buffer), baseAllocator_(baseAllocator), ownBaseAllocator_(0)
	{
		RAPIDJSON_ASSERT(buffer != 0);
		RAPIDJSON_ASSERT(size > sizeof(ChunkHeader));
		chunkHead_ = (ChunkHeader*)buffer;
		chunkHead_->capacity = size - sizeof(ChunkHeader);
		chunkHead_->size = 0;
		chunkHead_->next = 0;
	}

	//! Destructor.
	/*! This deallocates all memory chunks, excluding the user-supplied buffer.
	*/
	~MemoryPoolAllocator() {
		Clear();
		delete ownBaseAllocator_;
	}

	//! Deallocates all memory chunks, excluding the user-supplied buffer.
	void Clear() {
		while(chunkHead_ != 0 && chunkHead_ != (ChunkHeader *)userBuffer_) {
			ChunkHeader* next = chunkHead_->next;
			baseAllocator_->Free(chunkHead_);
			chunkHead_ = next;
		}
	}

	//! Computes the total capacity of allocated memory chunks.
	/*! \return total capacity in bytes.
	*/
	size_t Capacity() {
		size_t capacity = 0;
		for (ChunkHeader* c = chunkHead_; c != 0; c = c->next)
			capacity += c->capacity;
		return capacity;
	}

	//! Computes the memory blocks allocated.
	/*! \return total used bytes.
	*/
	size_t Size() {
		size_t size = 0;
		for (ChunkHeader* c = chunkHead_; c != 0; c = c->next)
			size += c->size;
		return size;
	}

	//! Allocates a memory block. (concept Allocator)
	void* Malloc(size_t size) {
		size = (size + 3) & ~3;	// Force aligning size to 4

		if (chunkHead_->size + size > chunkHead_->capacity)
			AddChunk(chunk_capacity_ > size ? chunk_capacity_ : size);

		char *buffer = (char *)(chunkHead_ + 1) + chunkHead_->size;
		RAPIDJSON_ASSERT(((uintptr_t)buffer & 3) == 0);	// returned buffer is aligned to 4
		chunkHead_->size += size;

		return buffer;
	}

	//! Resizes a memory block (concept Allocator)
	void* Realloc(void* originalPtr, size_t originalSize, size_t newSize) {
		if (originalPtr == 0)
			return Malloc(newSize);

		// Do not shrink if new size is smaller than original
		if (originalSize >= newSize)
			return originalPtr;

		// Simply expand it if it is the last allocation and there is sufficient space
		if (originalPtr == (char *)(chunkHead_ + 1) + chunkHead_->size - originalSize) {
			size_t increment = newSize - originalSize;
			increment = (increment + 3) & ~3;	// Force aligning size to 4
			if (chunkHead_->size + increment <= chunkHead_->capacity) {
				chunkHead_->size += increment;
				RAPIDJSON_ASSERT(((uintptr_t)originalPtr & 3) == 0);	// returned buffer is aligned to 4
				return originalPtr;
			}
		}

		// Realloc process: allocate and copy memory, do not free original buffer.
		void* newBuffer = Malloc(newSize);
		RAPIDJSON_ASSERT(newBuffer != 0);	// Do not handle out-of-memory explicitly.
		return memcpy(newBuffer, originalPtr, originalSize);
	}

	//! Frees a memory block (concept Allocator)
	static void Free(void *) {} // Do nothing

private:
	//! Creates a new chunk.
	/*! \param capacity Capacity of the chunk in bytes.
	*/
	void AddChunk(size_t capacity) {
		ChunkHeader* chunk = (ChunkHeader*)baseAllocator_->Malloc(sizeof(ChunkHeader) + capacity);
		chunk->capacity = capacity;
		chunk->size = 0;
		chunk->next = chunkHead_;
		chunkHead_ =  chunk;
	}

	static const int kDefaultChunkCapacity = 64 * 1024; //!< Default chunk capacity.

	//! Chunk header for perpending to each chunk.
	/*! Chunks are stored as a singly linked list.
	*/
	struct ChunkHeader {
		size_t capacity;	//!< Capacity of the chunk in bytes (excluding the header itself).
		size_t size;		//!< Current size of allocated memory in bytes.
		ChunkHeader *next;	//!< Next chunk in the linked list.
	};

	ChunkHeader *chunkHead_;	//!< Head of the chunk linked-list. Only the head chunk serves allocation.
	size_t chunk_capacity_;		//!< The minimum capacity of chunk when they are allocated.
	char *userBuffer_;			//!< User supplied buffer.
	BaseAllocator* baseAllocator_;	//!< base allocator for allocating memory chunks.
	BaseAllocator* ownBaseAllocator_;	//!< base allocator created by this object.
};

///////////////////////////////////////////////////////////////////////////////
// Encoding

/*! \class rapidjson::Encoding
	\brief Concept for encoding of Unicode characters.

\code
concept Encoding {
	typename Ch;	//! Type of character.

	//! \brief Encode a Unicode codepoint to a buffer.
	//! \param buffer pointer to destination buffer to store the result. It should have sufficient size of encoding one character.
	//! \param codepoint An unicode codepoint, ranging from 0x0 to 0x10FFFF inclusively.
	//! \returns the pointer to the next character after the encoded data.
	static Ch* Encode(Ch *buffer, unsigned codepoint);
};
\endcode
*/

///////////////////////////////////////////////////////////////////////////////
// UTF8

//! UTF-8 encoding.
/*! http://en.wikipedia.org/wiki/UTF-8
	\tparam CharType Type for storing 8-bit UTF-8 data. Default is char.
	\implements Encoding
*/
template<typename CharType = char>
struct UTF8 {
	typedef CharType Ch;

	static Ch* Encode(Ch *buffer, unsigned codepoint) {
		if (codepoint <= 0x7F) 
			*buffer++ = codepoint & 0xFF;
		else if (codepoint <= 0x7FF) {
			*buffer++ = 0xC0 | ((codepoint >> 6) & 0xFF);
			*buffer++ = 0x80 | ((codepoint & 0x3F));
		}
		else if (codepoint <= 0xFFFF) {
			*buffer++ = 0xE0 | ((codepoint >> 12) & 0xFF);
			*buffer++ = 0x80 | ((codepoint >> 6) & 0x3F);
			*buffer++ = 0x80 | (codepoint & 0x3F);
		}
		else {
			RAPIDJSON_ASSERT(codepoint <= 0x10FFFF);
			*buffer++ = 0xF0 | ((codepoint >> 18) & 0xFF);
			*buffer++ = 0x80 | ((codepoint >> 12) & 0x3F);
			*buffer++ = 0x80 | ((codepoint >> 6) & 0x3F);
			*buffer++ = 0x80 | (codepoint & 0x3F);
		}
		return buffer;
	}
};

///////////////////////////////////////////////////////////////////////////////
// UTF16

//! UTF-16 encoding.
/*! http://en.wikipedia.org/wiki/UTF-16
	\tparam CharType Type for storing 16-bit UTF-16 data. Default is wchar_t. C++11 may use char16_t instead.
	\implements Encoding
*/
template<typename CharType = wchar_t>
struct UTF16 {
	typedef CharType Ch;

	static Ch* Encode(Ch* buffer, unsigned codepoint) {
		if (codepoint <= 0xFFFF) {
			RAPIDJSON_ASSERT(codepoint < 0xD800 || codepoint > 0xDFFF); // Code point itself cannot be surrogate pair 
			*buffer++ = static_cast<Ch>(codepoint);
		}
		else {
			RAPIDJSON_ASSERT(codepoint <= 0x10FFFF);
			unsigned v = codepoint - 0x10000;
			*buffer++ = static_cast<Ch>((v >> 10) + 0xD800);
			*buffer++ = (v & 0x3FF) + 0xDC00;
		}
		return buffer;
	}
};

///////////////////////////////////////////////////////////////////////////////
// UTF32

//! UTF-32 encoding. 
/*! http://en.wikipedia.org/wiki/UTF-32
	\tparam Ch Type for storing 32-bit UTF-32 data. Default is unsigned. C++11 may use char32_t instead.
	\implements Encoding
*/
template<typename CharType = unsigned>
struct UTF32 {
	typedef CharType Ch;

	static Ch *Encode(Ch* buffer, unsigned codepoint) {
		RAPIDJSON_ASSERT(codepoint <= 0x10FFFF);
		*buffer++ = codepoint;
		return buffer;
	}
};

///////////////////////////////////////////////////////////////////////////////
//  Stream

/*! \class rapidjson::Stream
	\brief Concept for reading and writing characters.

	For read-only stream, no need to implement PutBegin(), Put() and PutEnd().

	For write-only stream, only need to implement Put().

\code
concept Stream {
	typename Ch;	//!< Character type of the stream.

	//! Read the current character from stream without moving the read cursor.
	Ch Peek() const;

	//! Read the current character from stream and moving the read cursor to next character.
	Ch Take();

	//! Get the current read cursor.
	//! \return Number of characters read from start.
	size_t Tell();

	//! Begin writing operation at the current read pointer.
	//! \return The begin writer pointer.
	Ch* PutBegin();

	//! Write a character.
	void Put(Ch c);

	//! End the writing operation.
	//! \param begin The begin write pointer returned by PutBegin().
	//! \return Number of characters written.
	size_t PutEnd(Ch* begin);
}
\endcode
*/

//! Put N copies of a character to a stream.
template<typename Stream, typename Ch>
inline void PutN(Stream& stream, Ch c, size_t n) {
	for (size_t i = 0; i < n; i++)
		stream.Put(c);
}

///////////////////////////////////////////////////////////////////////////////
// StringStream

//! Read-only string stream.
/*! \implements Stream
*/
template <typename Encoding>
struct GenericStringStream {
	typedef typename Encoding::Ch Ch;

	GenericStringStream(const Ch *src) : src_(src), head_(src) {}

	Ch Peek() const { return *src_; }
	Ch Take() { return *src_++; }
	size_t Tell() const { return src_ - head_; }

	Ch* PutBegin() { RAPIDJSON_ASSERT(false); return 0; }
	void Put(Ch) { RAPIDJSON_ASSERT(false); }
	size_t PutEnd(Ch*) { RAPIDJSON_ASSERT(false); return 0; }

	const Ch* src_;		//!< Current read position.
	const Ch* head_;	//!< Original head of the string.
};

typedef GenericStringStream<UTF8<> > StringStream;

///////////////////////////////////////////////////////////////////////////////
// InsituStringStream

//! A read-write string stream.
/*! This string stream is particularly designed for in-situ parsing.
	\implements Stream
*/
template <typename Encoding>
struct GenericInsituStringStream {
	typedef typename Encoding::Ch Ch;

	GenericInsituStringStream(Ch *src) : src_(src), dst_(0), head_(src) {}

	// Read
	Ch Peek() { return *src_; }
	Ch Take() { return *src_++; }
	size_t Tell() { return src_ - head_; }

	// Write
	Ch* PutBegin() { return dst_ = src_; }
	void Put(Ch c) { RAPIDJSON_ASSERT(dst_ != 0); *dst_++ = c; }
	size_t PutEnd(Ch* begin) { return dst_ - begin; }

	Ch* src_;
	Ch* dst_;
	Ch* head_;
};

typedef GenericInsituStringStream<UTF8<> > InsituStringStream;

///////////////////////////////////////////////////////////////////////////////
// Type

//! Type of JSON value
enum Type {
	kNullType = 0,		//!< null
	kFalseType = 1,		//!< false
	kTrueType = 2,		//!< true
	kObjectType = 3,	//!< object
	kArrayType = 4,		//!< array 
	kStringType = 5,	//!< string
	kNumberType = 6,	//!< number
};

} // namespace rapidjson

#endif // RAPIDJSON_RAPIDJSON_H_
