//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef ROCKSDB_OPENSSL_AES_CTR
#ifndef ROCKSDB_LITE

#include "util/library_loader.h"

#include <dlfcn.h>

// link with -ldl

namespace ROCKSDB_NAMESPACE {

#ifdef OS_MACOSX
const char* UnixLibCrypto::crypto_lib_name_ = "libcrypto.dylib";
#else
const char* UnixLibCrypto::crypto_lib_name_ = "libcrypto.so";
#endif

UnixLibraryLoader::UnixLibraryLoader(const char* library_name)
    : dl_handle_(nullptr) {
  if (nullptr != library_name && '\0' != *library_name) {
    dl_handle_ = dlopen(library_name, RTLD_NOW | RTLD_GLOBAL);

    is_valid_ = (nullptr != dl_handle_);

    if (!is_valid_) {
      last_error_msg_ = dlerror();
    }
  }
}

UnixLibraryLoader::~UnixLibraryLoader() {
  if (nullptr != dl_handle_) {
    int ret_val = dlclose(dl_handle_);
    dl_handle_ = nullptr;
    is_valid_ = false;

    if (0 != ret_val) {
      last_error_msg_ = dlerror();
    }
  }
}

void* UnixLibraryLoader::GetEntryPoint(const char* function_name) {
  void* ret_ptr = {nullptr};

  if (is_valid_) {
    ret_ptr = dlsym(dl_handle_, function_name);
    if (nullptr == ret_ptr) {
      last_error_msg_ = dlerror();
    }
  }

  return ret_ptr;
}

size_t UnixLibraryLoader::GetEntryPoints(
    std::map<std::string, void*>& functions) {
  size_t num_found{0};

  if (is_valid_) {
    for (auto& func : functions) {
      void* tmp_ptr;

      tmp_ptr = GetEntryPoint(func.first.c_str());
      if (nullptr != tmp_ptr) {
        ++num_found;
        func.second = tmp_ptr;
      }
    }
  }

  return num_found;
}

UnixLibCrypto::UnixLibCrypto() : UnixLibraryLoader(crypto_lib_name_) {
  if (is_valid_) {
    // size of map minus two since _new/_create and _free/_destroy
    //  only resolve one of the two.
    is_valid_ = ((functions_.size() - 2) == GetEntryPoints(functions_));

    ctx_new_ = (EVP_MD_CTX_new_t)functions_["EVP_MD_CTX_new"];
    if (nullptr == ctx_new_) {
      ctx_new_ = (EVP_MD_CTX_new_t)functions_["EVP_MD_CTX_create"];
    }

    digest_init_ = (EVP_DigestInit_ex_t)functions_["EVP_DigestInit_ex"];
    sha1_ = (EVP_sha1_t)functions_["EVP_sha1"];
    digest_update_ = (EVP_DigestUpdate_t)functions_["EVP_DigestUpdate"];
    digest_final_ = (EVP_DigestFinal_ex_t)functions_["EVP_DigestFinal_ex"];

    ctx_free_ = (EVP_MD_CTX_free_t)functions_["EVP_MD_CTX_free"];
    if (nullptr == ctx_free_) {
      ctx_free_ = (EVP_MD_CTX_free_t)functions_["EVP_MD_CTX_destroy"];
    }

    rand_bytes_ = (RAND_bytes_t)functions_["RAND_bytes"];
    rand_poll_ = (RAND_poll_t)functions_["RAND_poll"];

    cipher_new_ = (EVP_CIPHER_CTX_new_t)functions_["EVP_CIPHER_CTX_new"];
    cipher_free_ = (EVP_CIPHER_CTX_free_t)functions_["EVP_CIPHER_CTX_free"];
    encrypt_init_ = (EVP_EncryptInit_ex_t)functions_["EVP_EncryptInit_ex"];
    aes_256_ctr_ = (EVP_aes_256_ctr_t)functions_["EVP_aes_256_ctr"];
    encrypt_update_ = (EVP_EncryptUpdate_t)functions_["EVP_EncryptUpdate"];
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
#endif  // ROCKSDB_OPENSSL_AES_CTR
