# - Find OpenSSL includes
# Find the native OpenSSL includes
#
# OpenSSL_INCLUDE_DIRS - where to find aes.h, etc.
# OpenSSL_FOUND - True if OpenSSL found.

find_path(OPENSSL_INCLUDE_DIRS
  NAMES openssl/aes.h openssl/evp.h openssl/rand.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(OpenSSL DEFAULT_MSG OPENSSL_INCLUDE_DIRS)

mark_as_advanced(
  OPENSSL_INCLUDE_DIRS)
