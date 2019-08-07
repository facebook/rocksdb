//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>
#include <type_traits>
#include <utility>

#include <folly/Traits.h>
#include <folly/Utility.h>

namespace folly {

namespace array_detail {
template <typename>
struct is_ref_wrapper : std::false_type {};
template <typename T>
struct is_ref_wrapper<std::reference_wrapper<T>> : std::true_type {};

template <typename T>
using not_ref_wrapper =
    folly::Negation<is_ref_wrapper<typename std::decay<T>::type>>;

template <typename D, typename...>
struct return_type_helper {
  using type = D;
};
template <typename... TList>
struct return_type_helper<void, TList...> {
  static_assert(
      folly::Conjunction<not_ref_wrapper<TList>...>::value,
      "TList cannot contain reference_wrappers when D is void");
  using type = typename std::common_type<TList...>::type;
};

template <typename D, typename... TList>
using return_type = std::
    array<typename return_type_helper<D, TList...>::type, sizeof...(TList)>;
} // namespace array_detail

template <typename D = void, typename... TList>
constexpr array_detail::return_type<D, TList...> make_array(TList&&... t) {
  using value_type =
      typename array_detail::return_type_helper<D, TList...>::type;
  return {{static_cast<value_type>(std::forward<TList>(t))...}};
}

namespace array_detail {
template <typename MakeItem, std::size_t... Index>
inline constexpr auto make_array_with(
    MakeItem const& make,
    folly::index_sequence<Index...>)
      -> std::array<decltype(make(0)), sizeof...(Index)> {
  return std::array<decltype(make(0)), sizeof...(Index)>{{make(Index)...}};
}
} // namespace array_detail

//  make_array_with
//
//  Constructs a std::array<..., Size> with elements m(i) for i in [0, Size).
template <std::size_t Size, typename MakeItem>
constexpr auto make_array_with(MakeItem const& make)
    -> decltype(array_detail::make_array_with(
          make,
          folly::make_index_sequence<Size>{})) {
  return array_detail::make_array_with(
      make,
      folly::make_index_sequence<Size>{});
}

} // namespace folly
