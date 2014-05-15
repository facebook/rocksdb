#ifndef RAPIDJSON_INTERNAL_STRFUNC_H_
#define RAPIDJSON_INTERNAL_STRFUNC_H_

namespace rapidjson {
namespace internal {

//! Custom strlen() which works on different character types.
/*!	\tparam Ch Character type (e.g. char, wchar_t, short)
	\param s Null-terminated input string.
	\return Number of characters in the string. 
	\note This has the same semantics as strlen(), the return value is not number of Unicode codepoints.
*/
template <typename Ch>
inline SizeType StrLen(const Ch* s) {
	const Ch* p = s;
	while (*p != '\0')
		++p;
	return SizeType(p - s);
}

} // namespace internal
} // namespace rapidjson

#endif // RAPIDJSON_INTERNAL_STRFUNC_H_
