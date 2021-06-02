//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

/*
 * Optional - For conditional initialization of values, like boost::optional,
 * but with support for move semantics and emplacement.  Reference type support
 * has not been included due to limited use cases and potential confusion with
 * semantics of assignment: Assigning to an optional reference could quite
 * reasonably copy its value or redirect the reference.
 *
 * Optional can be useful when a variable might or might not be needed:
 *
 *  Optional<Logger> maybeLogger = ...;
 *  if (maybeLogger) {
 *    maybeLogger->log("hello");
 *  }
 *
 * Optional enables a 'null' value for types which do not otherwise have
 * nullability, especially useful for parameter passing:
 *
 * void testIterator(const unique_ptr<Iterator>& it,
 *                   initializer_list<int> idsExpected,
 *                   Optional<initializer_list<int>> ranksExpected = none) {
 *   for (int i = 0; it->next(); ++i) {
 *     EXPECT_EQ(it->doc().id(), idsExpected[i]);
 *     if (ranksExpected) {
 *       EXPECT_EQ(it->doc().rank(), (*ranksExpected)[i]);
 *     }
 *   }
 * }
 *
 * Optional models OptionalPointee, so calling 'get_pointer(opt)' will return a
 * pointer to nullptr if the 'opt' is empty, and a pointer to the value if it is
 * not:
 *
 *  Optional<int> maybeInt = ...;
 *  if (int* v = get_pointer(maybeInt)) {
 *    cout << *v << endl;
 *  }
 */

#include <cstddef>
#include <functional>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <folly/CPortability.h>
#include <folly/Traits.h>
#include <folly/Utility.h>

namespace folly {

template <class Value>
class Optional;

namespace detail {
template <class Value>
struct OptionalPromiseReturn;
} // namespace detail

struct None {
  enum class _secret { _token };

  /**
   * No default constructor to support both `op = {}` and `op = none`
   * as syntax for clearing an Optional, just like std::nullopt_t.
   */
  constexpr explicit None(_secret) {}
};
constexpr None none{None::_secret::_token};

class FOLLY_EXPORT OptionalEmptyException : public std::runtime_error {
 public:
  OptionalEmptyException()
      : std::runtime_error("Empty Optional cannot be unwrapped") {}
};

template <class Value>
class Optional {
 public:
  typedef Value value_type;

  static_assert(
      !std::is_reference<Value>::value,
      "Optional may not be used with reference types");
  static_assert(
      !std::is_abstract<Value>::value,
      "Optional may not be used with abstract types");

  Optional() noexcept {}

  Optional(const Optional& src) noexcept(
      std::is_nothrow_copy_constructible<Value>::value) {
    if (src.hasValue()) {
      construct(src.value());
    }
  }

  Optional(Optional&& src) noexcept(
      std::is_nothrow_move_constructible<Value>::value) {
    if (src.hasValue()) {
      construct(std::move(src.value()));
      src.clear();
    }
  }

  /* implicit */ Optional(const None&) noexcept {}

  /* implicit */ Optional(Value&& newValue) noexcept(
      std::is_nothrow_move_constructible<Value>::value) {
    construct(std::move(newValue));
  }

  /* implicit */ Optional(const Value& newValue) noexcept(
      std::is_nothrow_copy_constructible<Value>::value) {
    construct(newValue);
  }

  template <typename... Args>
  explicit Optional(in_place_t, Args&&... args) noexcept(
      std::is_nothrow_constructible<Value, Args...>::value)
      : Optional{PrivateConstructor{}, std::forward<Args>(args)...} {}

  template <typename U, typename... Args>
  explicit Optional(
      in_place_t,
      std::initializer_list<U> il,
      Args&&... args) noexcept(std::
                                   is_nothrow_constructible<
                                       Value,
                                       std::initializer_list<U>,
                                       Args...>::value)
      : Optional{PrivateConstructor{}, il, std::forward<Args>(args)...} {}

  // Used only when an Optional is used with coroutines on MSVC
  /* implicit */ Optional(const detail::OptionalPromiseReturn<Value>& p)
      : Optional{} {
    p.promise_->value_ = this;
  }

  void assign(const None&) {
    clear();
  }

  void assign(Optional&& src) {
    if (this != &src) {
      if (src.hasValue()) {
        assign(std::move(src.value()));
        src.clear();
      } else {
        clear();
      }
    }
  }

  void assign(const Optional& src) {
    if (src.hasValue()) {
      assign(src.value());
    } else {
      clear();
    }
  }

  void assign(Value&& newValue) {
    if (hasValue()) {
      storage_.value = std::move(newValue);
    } else {
      construct(std::move(newValue));
    }
  }

  void assign(const Value& newValue) {
    if (hasValue()) {
      storage_.value = newValue;
    } else {
      construct(newValue);
    }
  }

  Optional& operator=(None) noexcept {
    reset();
    return *this;
  }

  template <class Arg>
  Optional& operator=(Arg&& arg) {
    assign(std::forward<Arg>(arg));
    return *this;
  }

  Optional& operator=(Optional&& other) noexcept(
      std::is_nothrow_move_assignable<Value>::value) {
    assign(std::move(other));
    return *this;
  }

  Optional& operator=(const Optional& other) noexcept(
      std::is_nothrow_copy_assignable<Value>::value) {
    assign(other);
    return *this;
  }

  template <class... Args>
  Value& emplace(Args&&... args) {
    clear();
    construct(std::forward<Args>(args)...);
    return value();
  }

  template <class U, class... Args>
  typename std::enable_if<
      std::is_constructible<Value, std::initializer_list<U>&, Args&&...>::value,
      Value&>::type
  emplace(std::initializer_list<U> ilist, Args&&... args) {
    clear();
    construct(ilist, std::forward<Args>(args)...);
    return value();
  }

  void reset() noexcept {
    storage_.clear();
  }

  void clear() noexcept {
    reset();
  }

  void swap(Optional& that) noexcept(IsNothrowSwappable<Value>::value) {
    if (hasValue() && that.hasValue()) {
      using std::swap;
      swap(value(), that.value());
    } else if (hasValue()) {
      that.emplace(std::move(value()));
      reset();
    } else if (that.hasValue()) {
      emplace(std::move(that.value()));
      that.reset();
    }
  }

  const Value& value() const& {
    require_value();
    return storage_.value;
  }

  Value& value() & {
    require_value();
    return storage_.value;
  }

  Value&& value() && {
    require_value();
    return std::move(storage_.value);
  }

  const Value&& value() const&& {
    require_value();
    return std::move(storage_.value);
  }

  const Value* get_pointer() const& {
    return storage_.hasValue ? &storage_.value : nullptr;
  }
  Value* get_pointer() & {
    return storage_.hasValue ? &storage_.value : nullptr;
  }
  Value* get_pointer() && = delete;

  bool has_value() const noexcept {
    return storage_.hasValue;
  }

  bool hasValue() const noexcept {
    return has_value();
  }

  explicit operator bool() const noexcept {
    return has_value();
  }

  const Value& operator*() const& {
    return value();
  }
  Value& operator*() & {
    return value();
  }
  const Value&& operator*() const&& {
    return std::move(value());
  }
  Value&& operator*() && {
    return std::move(value());
  }

  const Value* operator->() const {
    return &value();
  }
  Value* operator->() {
    return &value();
  }

  // Return a copy of the value if set, or a given default if not.
  template <class U>
  Value value_or(U&& dflt) const& {
    if (storage_.hasValue) {
      return storage_.value;
    }

    return std::forward<U>(dflt);
  }

  template <class U>
  Value value_or(U&& dflt) && {
    if (storage_.hasValue) {
      return std::move(storage_.value);
    }

    return std::forward<U>(dflt);
  }

 private:
  template <class T>
  friend Optional<_t<std::decay<T>>> make_optional(T&&);
  template <class T, class... Args>
  friend Optional<T> make_optional(Args&&... args);
  template <class T, class U, class... As>
  friend Optional<T> make_optional(std::initializer_list<U>, As&&...);

  /**
   * Construct the optional in place, this is duplicated as a non-explicit
   * constructor to allow returning values that are non-movable from
   * make_optional using list initialization.
   *
   * Until C++17, at which point this will become unnecessary because of
   * specified prvalue elision.
   */
  struct PrivateConstructor {
    explicit PrivateConstructor() = default;
  };
  template <typename... Args>
  Optional(PrivateConstructor, Args&&... args) noexcept(
      std::is_constructible<Value, Args&&...>::value) {
    construct(std::forward<Args>(args)...);
  }

  void require_value() const {
    if (!storage_.hasValue) {
      throw OptionalEmptyException{};
    }
  }

  template <class... Args>
  void construct(Args&&... args) {
    const void* ptr = &storage_.value;
    // For supporting const types.
    new (const_cast<void*>(ptr)) Value(std::forward<Args>(args)...);
    storage_.hasValue = true;
  }

  struct StorageTriviallyDestructible {
    union {
      char emptyState;
      Value value;
    };
    bool hasValue;

    StorageTriviallyDestructible()
        : emptyState('\0'), hasValue{false} {}
    void clear() {
      hasValue = false;
    }
  };

  struct StorageNonTriviallyDestructible {
    union {
      char emptyState;
      Value value;
    };
    bool hasValue;

    StorageNonTriviallyDestructible() : hasValue{false} {}
    ~StorageNonTriviallyDestructible() {
      clear();
    }

    void clear() {
      if (hasValue) {
        hasValue = false;
        value.~Value();
      }
    }
  };

  using Storage = typename std::conditional<
      std::is_trivially_destructible<Value>::value,
      StorageTriviallyDestructible,
      StorageNonTriviallyDestructible>::type;

  Storage storage_;
};

template <class T>
const T* get_pointer(const Optional<T>& opt) {
  return opt.get_pointer();
}

template <class T>
T* get_pointer(Optional<T>& opt) {
  return opt.get_pointer();
}

template <class T>
void swap(Optional<T>& a, Optional<T>& b) noexcept(noexcept(a.swap(b))) {
  a.swap(b);
}

template <class T>
Optional<_t<std::decay<T>>> make_optional(T&& v) {
  using PrivateConstructor =
      typename folly::Optional<_t<std::decay<T>>>::PrivateConstructor;
  return {PrivateConstructor{}, std::forward<T>(v)};
}

template <class T, class... Args>
folly::Optional<T> make_optional(Args&&... args) {
  using PrivateConstructor = typename folly::Optional<T>::PrivateConstructor;
  return {PrivateConstructor{}, std::forward<Args>(args)...};
}

template <class T, class U, class... Args>
folly::Optional<T> make_optional(
    std::initializer_list<U> il,
    Args&&... args) {
  using PrivateConstructor = typename folly::Optional<T>::PrivateConstructor;
  return {PrivateConstructor{}, il, std::forward<Args>(args)...};
}

///////////////////////////////////////////////////////////////////////////////
// Comparisons.

template <class U, class V>
bool operator==(const Optional<U>& a, const V& b) {
  return a.hasValue() && a.value() == b;
}

template <class U, class V>
bool operator!=(const Optional<U>& a, const V& b) {
  return !(a == b);
}

template <class U, class V>
bool operator==(const U& a, const Optional<V>& b) {
  return b.hasValue() && b.value() == a;
}

template <class U, class V>
bool operator!=(const U& a, const Optional<V>& b) {
  return !(a == b);
}

template <class U, class V>
bool operator==(const Optional<U>& a, const Optional<V>& b) {
  if (a.hasValue() != b.hasValue()) {
    return false;
  }
  if (a.hasValue()) {
    return a.value() == b.value();
  }
  return true;
}

template <class U, class V>
bool operator!=(const Optional<U>& a, const Optional<V>& b) {
  return !(a == b);
}

template <class U, class V>
bool operator<(const Optional<U>& a, const Optional<V>& b) {
  if (a.hasValue() != b.hasValue()) {
    return a.hasValue() < b.hasValue();
  }
  if (a.hasValue()) {
    return a.value() < b.value();
  }
  return false;
}

template <class U, class V>
bool operator>(const Optional<U>& a, const Optional<V>& b) {
  return b < a;
}

template <class U, class V>
bool operator<=(const Optional<U>& a, const Optional<V>& b) {
  return !(b < a);
}

template <class U, class V>
bool operator>=(const Optional<U>& a, const Optional<V>& b) {
  return !(a < b);
}

// Suppress comparability of Optional<T> with T, despite implicit conversion.
template <class V>
bool operator<(const Optional<V>&, const V& other) = delete;
template <class V>
bool operator<=(const Optional<V>&, const V& other) = delete;
template <class V>
bool operator>=(const Optional<V>&, const V& other) = delete;
template <class V>
bool operator>(const Optional<V>&, const V& other) = delete;
template <class V>
bool operator<(const V& other, const Optional<V>&) = delete;
template <class V>
bool operator<=(const V& other, const Optional<V>&) = delete;
template <class V>
bool operator>=(const V& other, const Optional<V>&) = delete;
template <class V>
bool operator>(const V& other, const Optional<V>&) = delete;

// Comparisons with none
template <class V>
bool operator==(const Optional<V>& a, None) noexcept {
  return !a.hasValue();
}
template <class V>
bool operator==(None, const Optional<V>& a) noexcept {
  return !a.hasValue();
}
template <class V>
bool operator<(const Optional<V>&, None) noexcept {
  return false;
}
template <class V>
bool operator<(None, const Optional<V>& a) noexcept {
  return a.hasValue();
}
template <class V>
bool operator>(const Optional<V>& a, None) noexcept {
  return a.hasValue();
}
template <class V>
bool operator>(None, const Optional<V>&) noexcept {
  return false;
}
template <class V>
bool operator<=(None, const Optional<V>&) noexcept {
  return true;
}
template <class V>
bool operator<=(const Optional<V>& a, None) noexcept {
  return !a.hasValue();
}
template <class V>
bool operator>=(const Optional<V>&, None) noexcept {
  return true;
}
template <class V>
bool operator>=(None, const Optional<V>& a) noexcept {
  return !a.hasValue();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace folly
