#pragma once

#include <photon/rpc/serialize.h>

#include <cstdint>

struct Echo {
  const static uint32_t IID = 1;
  const static uint32_t FID = 2;

  struct Request : public photon::rpc::Message {
    photon::rpc::string key;
    bool write;

    PROCESS_FIELDS(key, write);
  };

  struct Response : public photon::rpc::Message {
    int32_t ret;

    PROCESS_FIELDS(ret);
  };
};