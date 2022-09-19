#include <random>
#include <gflags/gflags.h>
#include <photon/net/socket.h>
#include <photon/common/alog.h>
#include <photon/photon.h>
#include <photon/rpc/rpc.h>
#include <photon/thread/thread11.h>

#include "protocol.h"

DEFINE_int32(port, 9527, "server port");
DEFINE_string(host, "127.0.0.1", "server ip");
DEFINE_string(type, "fill", "fill/read/write");

constexpr int CONCURRENCY = 32;
constexpr int MAX_KEY_NUM = 10'000;

int gen_random_key() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dist(0, MAX_KEY_NUM - 1);
  return dist(gen);
}

void run_perf(photon::net::EndPoint ep, photon::rpc::StubPool* pool) {
  int ret;
  auto stub = pool->get_stub(ep, false);
  DEFER(pool->put_stub(ep, ret < 0));

  while (true) {
    Echo::Request req;
    std::string key = std::to_string(gen_random_key());
    req.key.assign(key);
    req.write = FLAGS_type == "read" ? false : true;

    Echo::Response resp;
    ret = stub->call<Echo>(req, resp);
    if (ret < 0 || resp.ret != 0) abort();
  }
}

void run_fill(photon::net::EndPoint ep, photon::rpc::StubPool* pool) {
  int ret;
  auto stub = pool->get_stub(ep, false);
  DEFER(pool->put_stub(ep, ret < 0));

  for (int i = 0; i < MAX_KEY_NUM; ++i) {
    Echo::Request req;
    std::string key = std::to_string(i);
    req.key.assign(key);
    req.write = FLAGS_type == "read" ? false : true;

    Echo::Response resp;
    ret = stub->call<Echo>(req, resp);
    if (ret < 0 || resp.ret != 0) abort();
  }
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (photon::init(photon::INIT_EVENT_IOURING, photon::INIT_IO_NONE))
    LOG_ERROR_RETURN(0, -1, "fail to init photon");
  DEFER(photon::fini());

  auto ep = photon::net::EndPoint(photon::net::IPAddr(FLAGS_host.c_str()),
                                  FLAGS_port);

  auto pool = photon::rpc::new_stub_pool(-1, -1, -1);

  if (FLAGS_type == "fill") {
    run_fill(ep, pool);
  } else {
    for (int i = 0; i < CONCURRENCY; ++i) {
      photon::thread_create11(run_perf, ep, pool);
    }
    photon::thread_sleep(-1);
  }
}