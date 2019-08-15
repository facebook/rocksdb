//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <utility>
#include <type_traits>

namespace folly {

/**
 *  Backports from C++17 of:
 *    std::in_place_t
 *    std::in_place_type_t
 *    std::in_place_index_t
 *    std::in_place
 *    std::in_place_type
 *    std::in_place_index
 */

struct in_place_tag {};
template <class>
struct in_place_type_tag {};
template <std::size_t>
struct in_place_index_tag {};

using in_place_t = in_place_tag (&)(in_place_tag);
template <class T>
using in_place_type_t = in_place_type_tag<T> (&)(in_place_type_tag<T>);
template <std::size_t I>
using in_place_index_t = in_place_index_tag<I> (&)(in_place_index_tag<I>);

inline in_place_tag in_place(in_place_tag = {}) {
  return {};
}
template <class T>
inline in_place_type_tag<T> in_place_type(in_place_type_tag<T> = {}) {
  return {};
}
template <std::size_t I>
inline in_place_index_tag<I> in_place_index(in_place_index_tag<I> = {}) {
  return {};
}

template <class T, class U = T>
T exchange(T& obj, U&& new_value) {
  T old_value = std::move(obj);
  obj = std::forward<U>(new_value);
  return old_value;
}

namespace utility_detail {
template <typename...>
struct make_seq_cat;
template <
    template <typename T, T...> class S,
    typename T,
    T... Ta,
    T... Tb,
    T... Tc>
struct make_seq_cat<S<T, Ta...>, S<T, Tb...>, S<T, Tc...>> {
  using type =
      S<T,
        Ta...,
        (sizeof...(Ta) + Tb)...,
        (sizeof...(Ta) + sizeof...(Tb) + Tc)...>;
};

// Not parameterizing by `template <typename T, T...> class, typename` because
// clang precisely v4.0 fails to compile that. Note that clang v3.9 and v5.0
// handle that code correctly.
//
// For this to work, `S0` is required to be `Sequence<T>` and `S1` is required
// to be `Sequence<T, 0>`.

template <std::size_t Size>
struct make_seq {
  template <typename S0, typename S1>
  using apply = typename make_seq_cat<
      typename make_seq<Size / 2>::template apply<S0, S1>,
      typename make_seq<Size / 2>::template apply<S0, S1>,
      typename make_seq<Size % 2>::template apply<S0, S1>>::type;
};
template <>
struct make_seq<1> {
  template <typename S0, typename S1>
  using apply = S1;
};
template <>
struct make_seq<0> {
  template <typename S0, typename S1>
  using apply = S0;
};
} // namespace utility_detail

// TODO: Remove after upgrading to C++14 baseline

template <class T, T... Ints>
struct integer_sequence {
  using value_type = T;

  static constexpr std::size_t size() noexcept {
    return sizeof...(Ints);
  }
};

template <std::size_t... Ints>
using index_sequence = integer_sequence<std::size_t, Ints...>;

template <typename T, std::size_t Size>
using make_integer_sequence = typename utility_detail::make_seq<
    Size>::template apply<integer_sequence<T>, integer_sequence<T, 0>>;

template <std::size_t Size>
using make_index_sequence = make_integer_sequence<std::size_t, Size>;
template <class... T>
using index_sequence_for = make_index_sequence<sizeof...(T)>;

/**
 * A simple helper for getting a constant reference to an object.
 *
 * Example:
 *
 *   std::vector<int> v{1,2,3};
 *   // The following two lines are equivalent:
 *   auto a = const_cast<const std::vector<int>&>(v).begin();
 *   auto b = folly::as_const(v).begin();
 *
 * Like C++17's std::as_const. See http://wg21.link/p0007
 */
template <class T>
T const& as_const(T& t) noexcept {
  return t;
}

template <class T>
void as_const(T const&&) = delete;

} // namespace folly
