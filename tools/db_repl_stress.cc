
#include <cstdio>

#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/types.h"
#include "port/atomic_pointer.h"
#include "util/testutil.h"


// Run a thread to perform Put's.
// Another thread uses GetUpdatesSince API to keep getting the updates.
// options :
// --num_inserts = the num of inserts the first thread should perform.
// --wal_ttl = the wal ttl for the run.

using namespace leveldb;

struct DataPumpThread {
  size_t no_records;
  DB* db; // Assumption DB is Open'ed already.
  volatile bool is_running;
};

static std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}

static void DataPumpThreadBody(void* arg) {
  DataPumpThread* t = reinterpret_cast<DataPumpThread*>(arg);
  DB* db = t->db;
  Random rnd(301);
  size_t i = 0;
  t->is_running = true;
  while( i < t->no_records ) {
    db->Put(WriteOptions(),
            Slice(RandomString(&rnd, 50)),
            Slice(RandomString(&rnd, 500)));
    ++i;
  }
  t->is_running = false;
}

struct ReplicationThread {
  port::AtomicPointer stop;
  DB* db;
  volatile SequenceNumber latest;
  volatile size_t no_read;
  volatile bool has_more;
};

//  experimenting with isNull. Makes code more readable?
static inline bool isNull(const void * const ptr) {
  return ptr == NULL;
}

static void ReplicationThreadBody(void* arg) {
  ReplicationThread* t = reinterpret_cast<ReplicationThread*>(arg);
  DB* db = t->db;
  TransactionLogIterator* iter = NULL;
  SequenceNumber currentSeqNum = 0;
  while (t->stop.Acquire_Load() != NULL) {
    if (isNull(iter)) {
      db->GetUpdatesSince(currentSeqNum, &iter);
      fprintf(stdout, "Refreshing iterator\n");
      iter->Next();
      while(iter->Valid()) {
        WriteBatch batch;
        SequenceNumber seq;
        iter->GetBatch(&batch, &seq);
        if (seq != currentSeqNum +1 && seq != currentSeqNum) {
          fprintf(stderr,
                  "Missed a seq no. b/w %ld and %ld\n",
                  currentSeqNum,
                  seq);
          exit(1);
        }
        currentSeqNum = seq;
        t->latest = seq;
        iter->Next();
        t->no_read++;
      }
    }
    delete iter;
    iter = NULL;
  }
}


int main(int argc, const char** argv) {

  long FLAGS_num_inserts = 1000;
  long FLAGS_WAL_ttl_seconds = 1000;
  char junk;
  long l;

  for (int i = 1; i < argc; ++i) {
    if (sscanf(argv[i], "--num_inserts=%ld%c", &l, &junk) == 1) {
      FLAGS_num_inserts = l;
    } else if (sscanf(argv[i], "--wal_ttl=%ld%c", &l, &junk) == 1) {
      FLAGS_WAL_ttl_seconds = l;
    } else {
      fprintf(stderr, "Invalid Flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  Env* env = Env::Default();
  std::string default_db_path;
  env->GetTestDirectory(&default_db_path);
  default_db_path += "db_repl_stress";
  Options options;
  options.create_if_missing = true;
  options.WAL_ttl_seconds = FLAGS_WAL_ttl_seconds;
  DB* db;

  Status s = DB::Open(options, default_db_path, &db);

  if (!s.ok()) {
    fprintf(stderr, "Could not open DB due to %s\n", s.ToString().c_str());
  }

  DataPumpThread dataPump;
  dataPump.no_records = FLAGS_num_inserts;
  dataPump.db = db;
  dataPump.is_running = true;
  env->StartThread(DataPumpThreadBody, &dataPump);

  ReplicationThread replThread;
  replThread.db = db;
  replThread.no_read = 0;
  replThread.has_more = true;
  replThread.stop.Release_Store(env); // store something to make it non-null.

  env->StartThread(ReplicationThreadBody, &replThread);
  while(dataPump.is_running) {
    continue;
  }
  replThread.stop.Release_Store(NULL);
  if ( replThread.no_read < dataPump.no_records ) {
    // no. read should be => than inserted.
    fprintf(stderr, "No. of Record's written and read not same\nRead : %ld"
            " Written : %ld", replThread.no_read, dataPump.no_records);
    exit(1);
  }
  exit(0);
  fprintf(stdout, "ALL IS FINE");
}
