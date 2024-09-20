#pragma once

#include <photon/rpc/serialize.h>

#include <cstdint>

struct KvPut {
  const static uint32_t IID = 1;
  const static uint32_t FID = 1;

  struct Request : public photon::rpc::Message {
    photon::rpc::string key;
    photon::rpc::string value;

    PROCESS_FIELDS(key, value);
  };

  struct Response : public photon::rpc::Message {
    int32_t ret;

    PROCESS_FIELDS(ret);
  };
};

struct KvGet {
  const static uint32_t IID = 1;
  const static uint32_t FID = 2;

  struct Request : public photon::rpc::Message {
    photon::rpc::string key;

    PROCESS_FIELDS(key);
  };

  struct Response : public photon::rpc::Message {
    int32_t ret;
    photon::rpc::string value;

    PROCESS_FIELDS(ret, value);
  };
};