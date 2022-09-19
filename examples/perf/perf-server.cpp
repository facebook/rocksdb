#include <thread>
#include <atomic>
#include <vector>
#include <random>

#include <gflags/gflags.h>
#include <photon/common/alog.h>
#include <photon/common/utility.h>
#include <photon/net/socket.h>
#include <photon/photon.h>
#include <photon/rpc/rpc.h>
#include <photon/thread/thread11.h>
#include <photon/thread/workerpool.h>
#include <photon/thread/std-compat.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <unistd.h>

#include "protocol.h"

DEFINE_int32(port, 9527, "Server listen port");
DEFINE_int32(show_qps_interval, 10, "interval seconds to show qps");
DEFINE_int32(vcpu_num, 8, "vcpu number");
DEFINE_bool(create_new_db, false, "create new db");

static std::atomic<uint64_t> qps{0};

static void show_qps_loop() {
  while (true) {
    photon::thread_sleep(FLAGS_show_qps_interval);
    LOG_INFO("QPS: `", qps.load() / FLAGS_show_qps_interval);
    qps = 0;
  }
}

class ExampleServer {
 public:
  // 协程池对性能影响巨大，如果这里将thread_pool_size降为0，即关闭协程池，则性能变为原先1/3 ~ 1/2
  explicit ExampleServer(int db_num = 1, int thread_pool_size = 65536)
      : skeleton(photon::rpc::new_skeleton(true, thread_pool_size)),
        server(photon::net::new_tcp_socket_server()),
        m_db_num(db_num) {
    skeleton->register_service<Echo>(this);
    writeOptions.sync = true;
    db_sharding.resize(db_num);
    LOG_INFO(VALUE(m_db_num));
  }

  virtual int do_rpc_service(Echo::Request* req, Echo::Response* resp,
                             IOVector*, IStream*) {
    photon_std::this_thread::migrate();
    rocksdb::Status s;
    std::string val;
    rocksdb::DB* db = db_sharding[std::stoi(req->key.to_std()) % m_db_num];
    if (req->write) {
      s = db->Put(writeOptions,
                  rocksdb::Slice(req->key.c_str(), req->key.size()), "1");
    } else {
      s = db->Get(readOptions,
                  rocksdb::Slice(req->key.c_str(), req->key.size()), &val);
      if (val != "1") {
        LOG_ERROR("read value error");
        abort();
      }
    }
    if (!s.ok()) {
      LOG_ERROR("db error");
      abort();
    }
    resp->ret = 0;
    qps++;
    return 0;
  }

  int serve(photon::net::ISocketStream* stream) {
    return skeleton->serve(stream, false);
  }

  int run(int port) {
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();

    options.stats_dump_period_sec = 0;
    options.stats_persist_period_sec = 0;
    options.enable_pipelined_write = true;
    options.compression = rocksdb::CompressionType::kLZ4Compression;
    // create the DB if it's not already present
    options.create_if_missing = true;

    if (open_db()) return -1;

    server->set_handler({this, &ExampleServer::serve});
    server->setsockopt(SOL_SOCKET, SO_REUSEPORT, 1);
    if (server->bind(port) < 0)
      LOG_ERRNO_RETURN(0, -1, "Failed to bind port `", port)
    if (server->listen() < 0) LOG_ERRNO_RETURN(0, -1, "Failed to listen");
    LOG_INFO("Started rpc server at `", server->getsockname());
    return server->start_loop(true);
  }

 protected:
  static constexpr const char* db_dir = "perf-db";

  std::unique_ptr<photon::rpc::Skeleton> skeleton{};
  std::unique_ptr<photon::net::ISocketServer> server{};
  std::vector<rocksdb::DB*> db_sharding{};  // 在一个server里open多个db
  rocksdb::Options options;
  rocksdb::WriteOptions writeOptions;
  rocksdb::ReadOptions readOptions;
  int m_db_num;

  virtual int open_db() {
    for (int i = 0; i < m_db_num; ++i) {
      if (open_db_at_index(i))
        abort();
    }
    return 0;
  }

  virtual int open_db_at_index(int index) {
    std::string path = std::string(get_current_dir_name()) + "/" +
                       std::string(db_dir) + "-" + std::to_string(index);
    if (FLAGS_create_new_db) {
      system((std::string("rm -rf ") + path).c_str());
      LOG_INFO("Create new db at `", path.c_str());
    } else {
      LOG_INFO("Open db at `", path.c_str());
    }
    rocksdb::Status s = rocksdb::DB::Open(options, path, &db_sharding[index]);
    if (!s.ok()) {
      LOG_ERROR_RETURN(0, -1, "open db ` failed:`", index, s.ToString().c_str());
    }
    return 0;
  }
};

class ExampleServerWithNativeRocksdb : public ExampleServer {
 public:
  // 同步线程模式下，线程数量需要设置大一点。可以用taskset限制程序的cpu数量等于协程的vcpu数
  explicit ExampleServerWithNativeRocksdb()
      : pool(new photon::WorkPool(256, photon::INIT_EVENT_IOURING, 0)),
        ExampleServer() {

  }
  int do_rpc_service(Echo::Request* req, Echo::Response* resp, IOVector*,
                     IStream*) override {
    // 使用work pool进行同步线程调用
    pool->call([&] {
      rocksdb::Status s;
      std::string val;
      rocksdb::DB* db = db_sharding[std::stoi(req->key.to_std()) % m_db_num];
      if (req->write) {
        s = db->Put(writeOptions,
                    rocksdb::Slice(req->key.c_str(), req->key.size()), "1");
      } else {
        s = db->Get(readOptions,
                    rocksdb::Slice(req->key.c_str(), req->key.size()), &val);
        if (val != "1") abort();
      }
    });
    resp->ret = 0;
    qps++;
    return 0;
  }

 private:
  photon::WorkPool* pool;
};

class MultiExampleServer : public ExampleServer {
 public:
  explicit MultiExampleServer(int index)
      : m_index(index), ExampleServer() {
  }

  int do_rpc_service(Echo::Request* req, Echo::Response* resp, IOVector*,
                     IStream*) override {
    rocksdb::Status s;
    std::string val;
    if (req->write) {
      s = db_alone->Put(writeOptions,
                  rocksdb::Slice(req->key.c_str(), req->key.size()), "1");
    } else {
      s = db_alone->Get(readOptions,
                  rocksdb::Slice(req->key.c_str(), req->key.size()), &val);
      if (val != "1") abort();
    }
    resp->ret = 0;
    qps++;
    return 0;
  }

 private:
  int m_index;
  rocksdb::DB* db_alone = nullptr;      // 每个server配一个db

  int open_db() override {
    std::string path = std::string(get_current_dir_name()) + "/" +
                       std::string(db_dir) + "-" + std::to_string(m_index);
    system((std::string("rm -rf ") + path).c_str());
    LOG_INFO("Create new db at `", path.c_str());
    rocksdb::Status s = rocksdb::DB::Open(options, path, &db_alone);
    if (!s.ok()) {
      LOG_ERROR_RETURN(0, -1, "open db failed");
    }
    return 0;
  }
};

class MultiDBExampleServer : public ExampleServer {
 public:
  explicit MultiDBExampleServer(int db_num) : ExampleServer(db_num) {}

  int do_rpc_service(Echo::Request* req, Echo::Response* resp, IOVector*,
                     IStream*) override {
    rocksdb::Status s;
    std::string val;
    size_t index = std::stoi(req->key.to_std()) % m_db_num;

    // TODO: modify photon
    // photon_std::this_thread::migrate(index);

    rocksdb::DB* db = db_sharding[index];
    if (req->write) {
      s = db->Put(writeOptions,
                  rocksdb::Slice(req->key.c_str(), req->key.size()), "1");
    } else {
      s = db->Get(readOptions,
                  rocksdb::Slice(req->key.c_str(), req->key.size()), &val);
      if (val != "1") {
        LOG_ERROR("read value error");
        abort();
      }
    }
    if (!s.ok()) {
      LOG_ERROR("db error");
      abort();
    }
    resp->ret = 0;
    qps++;
    return 0;
  }

 private:
  int open_db() override {
    for (int i = 0; i < m_db_num; ++i) {
      photon::thread_create11(&MultiDBExampleServer::open_db_at_index, this, i);
    }
    return 0;
  }

  int open_db_at_index(int index) override {
    // TODO modify photon
    // photon_std::this_thread::migrate(index);
    LOG_INFO("Open db ` in vcpu `", index, photon::get_vcpu());
    return ExampleServer::open_db_at_index(index);
  }
};

// 单server，用thread_migrate迁移到多vcpu
void test_single_server() {
  photon_std::work_pool_init(FLAGS_vcpu_num, photon::INIT_EVENT_IOURING, 0);
  auto server = new ExampleServer();
  server->run(FLAGS_port);
}

// 单server，原生多线程版本db
void test_single_server_with_native_rocksdb() {
  auto server = new ExampleServerWithNativeRocksdb();
  server->run(FLAGS_port);
}

// 多server监听同一端口，让内核来分发连接，每个vcpu有一个server，每个server一个db实例
// 需要修改std-compat.h，让rocksdb内部的thread不会自动迁移
void test_multiple_servers() {
  for (int i = 0; i < FLAGS_vcpu_num; ++i) {
    new std::thread([i] {
      photon::init(photon::INIT_EVENT_IOURING, 0);
      auto server = new MultiExampleServer(i);
      server->run(FLAGS_port);
      photon::thread_sleep(-1);
    });
  }
  photon::thread_sleep(-1);
}

// 一个server，open多个db。每个db只处理自己vcpu上的读请求，不跨vcpu
void test_multi_db_server() {
  photon_std::work_pool_init(FLAGS_vcpu_num, photon::INIT_EVENT_IOURING, 0);
  auto server = new MultiDBExampleServer(FLAGS_vcpu_num);
  server->run(FLAGS_port);
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (photon::init(photon::INIT_EVENT_IOURING, photon::INIT_IO_NONE))
    LOG_ERROR_RETURN(0, -1, "fail to init photon");
  DEFER(photon::fini());

  photon::thread_create11(show_qps_loop);

  test_single_server();
  // test_single_server_with_native_rocksdb();
  // test_multiple_servers();
  // test_multi_db_server();
}
