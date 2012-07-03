#ifndef _THRIFT_ASYNC_SIMPLECALLBACK_H_
#define _THRIFT_ASYNC_SIMPLECALLBACK_H_ 1

#include "thrift/lib/cpp/Thrift.h"
namespace apache { namespace thrift {

/**
 * A template class for forming simple method callbacks with either an empty
 * argument list or one argument of known type.
 *
 * For more efficiency where tr1::function is overkill.
 */

template<typename C,              ///< class whose method we wish to wrap
         typename A = void,       ///< type of argument
         typename R = void>       ///< type of return value
class SimpleCallback {
  typedef R (C::*cfptr_t)(A);     ///< pointer-to-member-function type
  cfptr_t fptr_;                  ///< the embedded function pointer
  C* obj_;                        ///< object whose function we're wrapping
 public:
  /**
   * Constructor for empty callback object.
   */
  SimpleCallback() :
    fptr_(NULL), obj_(NULL) {}
  /**
   * Construct callback wrapper for member function.
   *
   * @param fptr pointer-to-member-function
   * @param "this" for object associated with callback
   */
  SimpleCallback(cfptr_t fptr, const C* obj) :
    fptr_(fptr), obj_(const_cast<C*>(obj))
  {}

  /**
   * Make a call to the member function we've wrapped.
   *
   * @param i argument for the wrapped member function
   * @return value from that function
   */
  R operator()(A i) const {
    (obj_->*fptr_)(i);
  }

  operator bool() const {
    return obj_ != NULL && fptr_ != NULL;
  }

  ~SimpleCallback() {}
};

/**
 * Specialization of SimpleCallback for empty argument list.
 */
template<typename C,              ///< class whose method we wish to wrap
         typename R>              ///< type of return value
class SimpleCallback<C, void, R> {
  typedef R (C::*cfptr_t)();      ///< pointer-to-member-function type
  cfptr_t fptr_;                  ///< the embedded function pointer
  C* obj_;                        ///< object whose function we're wrapping
 public:
  /**
   * Constructor for empty callback object.
   */
  SimpleCallback() :
    fptr_(NULL), obj_(NULL) {}

  /**
   * Construct callback wrapper for member function.
   *
   * @param fptr pointer-to-member-function
   * @param obj "this" for object associated with callback
   */
  SimpleCallback(cfptr_t fptr, const C* obj) :
    fptr_(fptr), obj_(const_cast<C*>(obj))
  {}

  /**
   * Make a call to the member function we've wrapped.
   *
   * @return value from that function
   */
  R operator()() const {
    (obj_->*fptr_)();
  }

  operator bool() const {
    return obj_ != NULL && fptr_ != NULL;
  }

  ~SimpleCallback() {}
};

}} // apache::thrift

#endif /* !_THRIFT_ASYNC_SIMPLECALLBACK_H_ */
