#include <photon/thread/std-compat.h>

class PhotonEnv {
 public:
  PhotonEnv() {
    puts("init photon");
    int ret = photon::init(photon::INIT_EVENT_IOURING | photon::INIT_EVENT_SIGNAL, 0);
    if (ret != 0) {
      puts("photon init failed");
      abort();
    }
    puts("init workpool");
    ret = photon::std::work_pool_init(1, photon::INIT_EVENT_IOURING, 0);
    if (ret != 0) {
      puts("std work pool init failed");
      abort();
    }
  }
  ~PhotonEnv() {
    photon::std::work_pool_fini();
    photon::fini();
  }
};

static PhotonEnv env;

int main() {
  puts("ready to sleep ...");
  photon::thread_sleep(-1);
  return 0;
}