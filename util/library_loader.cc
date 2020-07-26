//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef ROCKSDB_OPENSSL_AES_CTR
#ifndef ROCKSDB_LITE

#include "util/library_loader.h"

#include <stdio.h>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

const char* UnixLibCrypto::crypto_lib_name_ = "crypto";

LibraryLoader::LibraryLoader(const char* library_name) : is_valid_(false) {
  Status stat = Env::Default()->LoadLibrary(library_name, std::string(), &lib_);
  is_valid_ = stat.ok() && nullptr != lib_.get();
}

void* LibraryLoader::GetEntryPoint(const char* function_name) {
  void* ret_ptr = {nullptr};

  if (is_valid_) {
    Status stat = lib_->LoadSymbol(function_name, &ret_ptr);
    if (!stat.ok()) {
      ret_ptr = nullptr;  // redundant but safe
    }
  }

  return ret_ptr;
}

size_t LibraryLoader::GetEntryPoints(std::map<std::string, void*>& functions) {
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

UnixLibCrypto::UnixLibCrypto() : LibraryLoader(crypto_lib_name_) {
  if (is_valid_) {
    // size of map minus three since _new/_create, _free/_destroy
    //  and _reset/_cleanup only resolve one of the two.
    is_valid_ = ((functions_.size() - 3) == GetEntryPoints(functions_));

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
    cipher_reset_ = (EVP_CIPHER_CTX_reset_t)functions_["EVP_CIPHER_CTX_reset"];
    if (nullptr == cipher_reset_) {
      cipher_reset_ =
          (EVP_CIPHER_CTX_reset_t)functions_["EVP_CIPHER_CTX_cleanup"];
    }
    cipher_free_ = (EVP_CIPHER_CTX_free_t)functions_["EVP_CIPHER_CTX_free"];
    encrypt_init_ = (EVP_EncryptInit_ex_t)functions_["EVP_EncryptInit_ex"];
    aes_256_ctr_ = (EVP_aes_256_ctr_t)functions_["EVP_aes_256_ctr"];
    encrypt_update_ = (EVP_EncryptUpdate_t)functions_["EVP_EncryptUpdate"];
    encrypt_final_ = (EVP_EncryptFinal_ex_t)functions_["EVP_EncryptFinal_ex"];
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
#endif  // ROCKSDB_OPENSSL_AES_CTR
