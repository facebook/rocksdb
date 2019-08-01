//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <type_traits>

namespace folly {

/// In functional programming, the degenerate case is often called "unit". In
/// C++, "void" is often the best analogue. However, because of the syntactic
/// special-casing required for void, it is frequently a liability for template
/// metaprogramming. So, instead of writing specializations to handle cases like
/// SomeContainer<void>, a library author may instead rule that out and simply
/// have library users use SomeContainer<Unit>. Contained values may be ignored.
/// Much easier.
///
/// "void" is the type that admits of no values at all. It is not possible to
/// construct a value of this type.
/// "unit" is the type that admits of precisely one unique value. It is
/// possible to construct a value of this type, but it is always the same value
/// every time, so it is uninteresting.
struct Unit {
  constexpr bool operator==(const Unit& /*other*/) const {
    return true;
  }
  constexpr bool operator!=(const Unit& /*other*/) const {
    return false;
  }
};

constexpr Unit unit{};

template <typename T>
struct lift_unit {
  using type = T;
};
template <>
struct lift_unit<void> {
  using type = Unit;
};
template <typename T>
using lift_unit_t = typename lift_unit<T>::type;

template <typename T>
struct drop_unit {
  using type = T;
};
template <>
struct drop_unit<Unit> {
  using type = void;
};
template <typename T>
using drop_unit_t = typename drop_unit<T>::type;

} // namespace folly

