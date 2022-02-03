#pragma once

#define CHECK_OK(expression)                       \
  do {                                             \
    auto status = (expression);                    \
    if (!status.ok()) {                            \
      std::cerr << status.ToString() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0)

#define CHECK_EQ(a, b)                                                      \
  if (a != b) {                                                             \
    std::cerr << "(" << #a << "=" << a << ") != (" << #b << "=" << b << ")" \
              << std::endl;                                                 \
    abort();                                                                \
  }

#define CHECK_TRUE(cond)                                      \
  if (!(cond)) {                                              \
    std::cerr << "\"" << #cond << "\" is false" << std::endl; \
    abort();                                                  \
  }
