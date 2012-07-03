/**
 * Copyright 2012 Facebook
 * @author Tudor Bosman (tudorb@fb.com)
 */

#ifndef THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_SCHEMA_INL_H_
#define THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_SCHEMA_INL_H_

#ifndef THRIFT_INCLUDE_SCHEMA_INL
#error This file may only be included from Schema.h
#endif

namespace apache {
namespace thrift {
namespace protocol {
namespace neutronium {

namespace detail {
extern const TType typeToTType[];
}  // namespace detail

inline TType toTType(reflection::Type t) {
  return detail::typeToTType[t];
}

}  // namespace neutronium
}  // namespace protocol
}  // namespace thrift
}  // namespace apache

#endif /* THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_SCHEMA_INL_H_ */

