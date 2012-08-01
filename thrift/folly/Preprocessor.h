/*
 * Copyright 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// @author: Andrei Alexandrescu

#ifndef FOLLY_PREPROCESSOR_
#define FOLLY_PREPROCESSOR_

/**
 * Necessarily evil preprocessor-related amenities.
 */

/**
 * FB_ONE_OR_NONE(hello, world) expands to hello and
 * FB_ONE_OR_NONE(hello) expands to nothing. This macro is used to
 * insert or eliminate text based on the presence of another argument.
 */
#define FB_ONE_OR_NONE(a, ...) FB_THIRD(a, ## __VA_ARGS__, a)
#define FB_THIRD(a, b, ...) __VA_ARGS__

/**
 * Helper macro that extracts the first argument out of a list of any
 * number of arguments.
 */
#define FB_ARG_1(a, ...) a

/**
 * Helper macro that extracts the second argument out of a list of any
 * number of arguments. If only one argument is given, it returns
 * that.
 */
#define FB_ARG_2_OR_1(...) FB_ARG_2_OR_1_IMPL(__VA_ARGS__, __VA_ARGS__)
// Support macro for the above
#define FB_ARG_2_OR_1_IMPL(a, b, ...) b

/**
 * FB_ANONYMOUS_VARIABLE(str) introduces an identifier starting with
 * str and ending with a number that varies with the line.
 */
#ifndef FB_ANONYMOUS_VARIABLE
#define FB_CONCATENATE_IMPL(s1, s2) s1##s2
#define FB_CONCATENATE(s1, s2) FB_CONCATENATE_IMPL(s1, s2)
#ifdef __COUNTER__
#define FB_ANONYMOUS_VARIABLE(str) FB_CONCATENATE(str, __COUNTER__)
#else
#define FB_ANONYMOUS_VARIABLE(str) FB_CONCATENATE(str, __LINE__)
#endif
#endif

/**
 * Use FB_STRINGIZE(name) when you'd want to do what #name does inside
 * another macro expansion.
 */
#define FB_STRINGIZE(name) #name


#endif // FOLLY_PREPROCESSOR_
