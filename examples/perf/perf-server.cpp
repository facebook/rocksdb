// Photon版本RocksDB server，在接收RPC的vCPU直接查询DB
// 原生多线程版本RocksDB server，使用WorkPool派发任务到多线程

#include <unistd.h>
#include <thread>
#include <atomic>
#include <vector>
#include <random>

#include <gflags/gflags.h>
#include <photon/common/alog.h>
#include <photon/common/alog-stdstring.h>
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

#include "protocol.h"

DEFINE_int32(port, 9527, "Server listen port");
DEFINE_int32(show_qps_interval, 1, "Interval seconds to show qps");
DEFINE_int32(vcpu_num, 8, "vCPU number");
DEFINE_bool(use_photon, false, "Use photon rocksdb instead of the native");
DEFINE_string(db_dir, "perf-db", "DB dir");
DEFINE_bool(clean_db, false, "Clean db before tests");

static std::atomic<uint64_t> qps{0};

static void show_qps_loop() {
    while (true) {
        photon::thread_sleep(FLAGS_show_qps_interval);
        LOG_INFO("QPS: `", qps.load() / FLAGS_show_qps_interval);
        qps = 0;
    }
}

class IOHandler {
public:
    IOHandler(rocksdb::DB* db, rocksdb::WriteOptions* writeOptions,
              rocksdb::ReadOptions* readOptions, photon::WorkPool* work_pool) :
            skeleton_(photon::rpc::new_skeleton(65536U)),
            socket_server_(photon::net::new_tcp_socket_server()),
            db_(db),
            writeOptions_(writeOptions),
            readOptions_(readOptions),
            work_pool_(work_pool) {
        skeleton_->register_service<KvGet, KvPut>(this);
    }

    int serve(photon::net::ISocketStream* stream) {
        return skeleton_->serve(stream);
    }

    int run() {
        socket_server_->set_handler({this, &IOHandler::serve});
        socket_server_->setsockopt<int>(SOL_SOCKET, SO_REUSEPORT, 1);
        if (socket_server_->bind(FLAGS_port) < 0) {
            LOG_ERRNO_RETURN(0, -1, "Failed to bind port `", FLAGS_port)
        }
        if (socket_server_->listen() < 0) {
            LOG_ERRNO_RETURN(0, -1, "Failed to listen");
        }
        LOG_INFO("Started rpc server at `", socket_server_->getsockname());
        return socket_server_->start_loop(true);
    }

    int do_rpc_service(KvPut::Request* req, KvPut::Response* resp, IOVector*, IStream*) {
        if (FLAGS_use_photon) {
            do_put(req);
        } else {
            photon::semaphore sem;
            auto func = new auto([&]() {
                do_put(req);
                sem.signal(1);
            });
            work_pool_->async_call(func);
            sem.wait(1);
        }
        resp->ret = 0;
        qps++;
        return 0;
    }

    int do_rpc_service(KvGet::Request* req, KvGet::Response* resp, IOVector*, IStream*) {
        std::string val;
        if (FLAGS_use_photon) {
            do_get(req, &val);
        } else {
            photon::semaphore sem;
            auto func = new auto([&]() {
                do_get(req, &val);
                sem.signal(1);
            });
            work_pool_->async_call(func);
            sem.wait(1);
        }
        resp->ret = 0;
        resp->value.assign(val);
        qps++;
        return 0;
    }

private:
    std::unique_ptr<photon::rpc::Skeleton> skeleton_;
    std::unique_ptr<photon::net::ISocketServer> socket_server_;
    rocksdb::DB* db_;                     // Owned by others
    rocksdb::WriteOptions* writeOptions_; // Owned by others
    rocksdb::ReadOptions* readOptions_;   // Owned by others
    photon::WorkPool* work_pool_;         // Owned by others

    void do_put(KvPut::Request* req) {
        rocksdb::Slice key(req->key.c_str(), req->key.size());
        rocksdb::Slice val(req->value.c_str(), req->value.size());
        rocksdb::Status s = db_->Put(*writeOptions_, key, val);
        if (!s.ok()) {
            LOG_ERROR("db write error");
            abort();
        }
    }

    void do_get(KvGet::Request* req, std::string* val) {
        rocksdb::Slice key(req->key.c_str(), req->key.size());
        rocksdb::Status s = db_->Get(*readOptions_, key, val);
        if (!s.ok()) {
            LOG_ERROR("db read error");
            abort();
        }
    }
};

class ExampleServer {
public:
    ExampleServer() {
        writeOptions.sync = false;
        pool = new photon::WorkPool(FLAGS_vcpu_num, photon::INIT_EVENT_IOURING, 0);
    }

    int run() {
        // Optimize RocksDB
        if (FLAGS_use_photon) {
            options.IncreaseParallelism(256);
            options.allow_concurrent_memtable_write = false;
            options.enable_pipelined_write = false;
        } else {
            options.IncreaseParallelism(8);
        }
        options.OptimizeLevelStyleCompaction();
        options.compression = rocksdb::CompressionType::kNoCompression;
        // create the DB if it's not already present
        options.create_if_missing = true;

        if (open_db()) {
            return -1;
        }

        for (int i = 0; i < FLAGS_vcpu_num; ++i) {
            std::thread([&] {
                int ret = photon::init(photon::INIT_EVENT_IOURING, photon::INIT_IO_NONE);
                if (ret) {
                    abort();
                }
                DEFER(photon::fini());
                IOHandler handler(db, &writeOptions, &readOptions, pool);
                handler.run();
            }).detach();
        }
        return 0;
    }

private:
    rocksdb::DB* db = nullptr;
    rocksdb::Options options;
    rocksdb::WriteOptions writeOptions;
    rocksdb::ReadOptions readOptions;
    photon::WorkPool* pool = nullptr;

    int open_db() {
        auto path = std::string(get_current_dir_name()) + "/" + FLAGS_db_dir;
        if (FLAGS_clean_db) {
            system((std::string("rm -rf ") + path).c_str());
            LOG_INFO("Create new db at `", path.c_str());
        } else {
            LOG_INFO("Open db at `", path.c_str());
        }
        rocksdb::Status s = rocksdb::DB::Open(options, path, &db);
        if (!s.ok()) {
            LOG_ERROR_RETURN(0, -1, "open db failed:`", s.ToString());
        }
        return 0;
    }
};

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    set_log_output_level(ALOG_INFO);
    if (photon::init(photon::INIT_EVENT_IOURING, photon::INIT_IO_NONE)) {
        LOG_ERROR_RETURN(0, -1, "fail to init photon");
    }
    DEFER(photon::fini());

    photon::thread_create11(show_qps_loop);

    auto server = new ExampleServer();
    if (server->run()) {
        return -1;
    }
    photon::thread_sleep(-1UL);
}
