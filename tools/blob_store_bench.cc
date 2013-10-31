#include <cstdio>
#include <vector>
#include <atomic>

#include "rocksdb/env.h"
#include "util/blob_store.h"
#include "util/testutil.h"

#define KB 1024LL
#define MB 1024*1024LL
// BlobStore does costly asserts to make sure it's running correctly, which
// significantly impacts benchmark runtime.
// NDEBUG will compile out those asserts.
#ifndef NDEBUG
#define NDEBUG
#endif

using namespace rocksdb;
using namespace std;

// used by all threads
uint64_t timeout_sec;
Env *env;
BlobStore* bs;

static std::string RandomString(Random* rnd, uint64_t len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}

struct Result {
  uint32_t writes;
  uint32_t reads;
  uint32_t deletes;
  uint64_t data_written;
  uint64_t data_read;

  void print() {
    printf("Total writes = %u\n", writes);
    printf("Total reads = %u\n", reads);
    printf("Total deletes = %u\n", deletes);
    printf("Write throughput = %lf MB/s\n",
           (double)data_written / (1024*1024.0) / timeout_sec);
    printf("Read throughput = %lf MB/s\n",
           (double)data_read / (1024*1024.0) / timeout_sec);
    printf("Total throughput = %lf MB/s\n",
           (double)(data_read + data_written) / (1024*1024.0) / timeout_sec);
  }

  Result() {
    writes = reads = deletes = data_read = data_written = 0;
  }

  Result (uint32_t writes, uint32_t reads, uint32_t deletes,
          uint64_t data_written, uint64_t data_read) :
    writes(writes), reads(reads), deletes(deletes),
    data_written(data_written), data_read(data_read) {}

};

Result operator + (const Result &a, const Result &b) {
  return Result(a.writes + b.writes, a.reads + b.reads,
                a.deletes + b.deletes, a.data_written + b.data_written,
                a.data_read + b.data_read);
}

struct WorkerThread {
  uint64_t data_size_from, data_size_to;
  double read_ratio;
  uint64_t working_set_size; // start deleting once you reach this
  Result result;
  atomic<bool> stopped;

  WorkerThread(uint64_t data_size_from, uint64_t data_size_to,
                double read_ratio, uint64_t working_set_size) :
    data_size_from(data_size_from), data_size_to(data_size_to),
    read_ratio(read_ratio), working_set_size(working_set_size),
    stopped(false) {}

  WorkerThread(const WorkerThread& wt) :
    data_size_from(wt.data_size_from), data_size_to(wt.data_size_to),
    read_ratio(wt.read_ratio), working_set_size(wt.working_set_size),
    stopped(false) {}
};

static void WorkerThreadBody(void* arg) {
  WorkerThread* t = reinterpret_cast<WorkerThread*>(arg);
  Random rnd(5);
  string buf;
  vector<pair<Blob, uint64_t>> blobs;
  vector<string> random_strings;

  for (int i = 0; i < 10; ++i) {
    random_strings.push_back(RandomString(&rnd, t->data_size_to));
  }

  uint64_t total_size = 0;

  uint64_t start_micros = env->NowMicros();
  while (env->NowMicros() - start_micros < timeout_sec * 1000 * 1000) {
    if (blobs.size() && rand() < RAND_MAX * t->read_ratio) {
      // read
      int bi = rand() % blobs.size();
      Status s = bs->Get(blobs[bi].first, &buf);
      assert(s.ok());
      t->result.data_read += buf.size();
      t->result.reads++;
    } else {
      // write
      uint64_t size = rand() % (t->data_size_to - t->data_size_from) +
        t->data_size_from;
      total_size += size;
      string put_str = random_strings[rand() % random_strings.size()];
      blobs.push_back(make_pair(Blob(), size));
      Status s = bs->Put(Slice(put_str.data(), size), &blobs.back().first);
      assert(s.ok());
      t->result.data_written += size;
      t->result.writes++;
    }

    while (total_size >= t->working_set_size) {
      // delete random
      int bi = rand() % blobs.size();
      total_size -= blobs[bi].second;
      bs->Delete(blobs[bi].first);
      blobs.erase(blobs.begin() + bi);
      t->result.deletes++;
    }
  }
  t->stopped.store(true);
}

Result StartBenchmark(vector<WorkerThread*>& config) {
  for (auto w : config) {
    env->StartThread(WorkerThreadBody, w);
  }

  Result result;

  for (auto w : config) {
    while (!w->stopped.load());
    result = result + w->result;
  }

  for (auto w : config) {
    delete w;
  }

  delete bs;

  return result;
}

vector<WorkerThread*> SetupBenchmarkBalanced() {
  string test_path;
  env->GetTestDirectory(&test_path);
  test_path.append("/blob_store");

  // config start
  uint32_t block_size = 16*KB;
  uint32_t file_size = 1*MB;
  double read_write_ratio = 0.5;
  uint64_t data_read_from = 16*KB;
  uint64_t data_read_to = 32*KB;
  int number_of_threads = 10;
  uint64_t working_set_size = 5*MB;
  timeout_sec = 5;
  // config end

  bs = new BlobStore(test_path, block_size, file_size / block_size, 10000, env);

  vector <WorkerThread*> config;

  for (int i = 0; i < number_of_threads; ++i) {
    config.push_back(new WorkerThread(data_read_from,
                                      data_read_to,
                                      read_write_ratio,
                                      working_set_size));
  };

  return config;
}

vector<WorkerThread*> SetupBenchmarkWriteHeavy() {
  string test_path;
  env->GetTestDirectory(&test_path);
  test_path.append("/blob_store");

  // config start
  uint32_t block_size = 16*KB;
  uint32_t file_size = 1*MB;
  double read_write_ratio = 0.1;
  uint64_t data_read_from = 16*KB;
  uint64_t data_read_to = 32*KB;
  int number_of_threads = 10;
  uint64_t working_set_size = 5*MB;
  timeout_sec = 5;
  // config end

  bs = new BlobStore(test_path, block_size, file_size / block_size, 10000, env);

  vector <WorkerThread*> config;

  for (int i = 0; i < number_of_threads; ++i) {
    config.push_back(new WorkerThread(data_read_from,
                                      data_read_to,
                                      read_write_ratio,
                                      working_set_size));
  };

  return config;
}

vector<WorkerThread*> SetupBenchmarkReadHeavy() {
  string test_path;
  env->GetTestDirectory(&test_path);
  test_path.append("/blob_store");

  // config start
  uint32_t block_size = 16*KB;
  uint32_t file_size = 1*MB;
  double read_write_ratio = 0.9;
  uint64_t data_read_from = 16*KB;
  uint64_t data_read_to = 32*KB;
  int number_of_threads = 10;
  uint64_t working_set_size = 5*MB;
  timeout_sec = 5;
  // config end

  bs = new BlobStore(test_path, block_size, file_size / block_size, 10000, env);

  vector <WorkerThread*> config;

  for (int i = 0; i < number_of_threads; ++i) {
    config.push_back(new WorkerThread(data_read_from,
                                      data_read_to,
                                      read_write_ratio,
                                      working_set_size));
  };

  return config;
}

int main(int argc, const char** argv) {
  srand(33);
  env = Env::Default();

  {
    printf("--- Balanced read/write benchmark ---\n");
    vector <WorkerThread*> config = SetupBenchmarkBalanced();
    Result r = StartBenchmark(config);
    r.print();
  }
  {
    printf("--- Write heavy benchmark ---\n");
    vector <WorkerThread*> config = SetupBenchmarkWriteHeavy();
    Result r = StartBenchmark(config);
    r.print();
  }
  {
    printf("--- Read heavy benchmark ---\n");
    vector <WorkerThread*> config = SetupBenchmarkReadHeavy();
    Result r = StartBenchmark(config);
    r.print();
  }

  return 0;
}
