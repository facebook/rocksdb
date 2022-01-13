#ifndef _CPLUSPLUS_TO_JAVA_CONVERT_H
#define _CPLUSPLUS_TO_JAVA_CONVERT_H

  #define GET_CPLUSPLUS_POINTER(_pointer)                        \
    static_cast<jlong>(reinterpret_cast<size_t>(_pointer))

#endif
