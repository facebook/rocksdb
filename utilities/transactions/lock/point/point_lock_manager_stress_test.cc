#include "utilities/transactions/lock/point/point_lock_manager_test.h"

namespace ROCKSDB_NAMESPACE {
#ifdef DEBUG_POINT_LOCK_MANAGER_TEST
#define DEBUG_LOG(...)            \
  do {                            \
    fprintf(stderr, __VA_ARGS__); \
    fflush(stderr);               \
  } while (0)
#else
#define DEBUG_LOG(...)
#endif

enum class LockTypeToTest : int8_t {
  EXCLUSIVE_ONLY = 0,
  SHARED_ONLY = 1,
  EXCLUSIVE_AND_SHARED = 2,
};

struct PointLockCorrectnessCheckTestParam {
  bool is_per_key_point_lock_manager;
  size_t thread_count;
  size_t key_count;
  size_t max_num_keys_to_lock_per_txn;
  size_t execution_time_sec;
  LockTypeToTest lock_type;
  int64_t lock_timeout_us;
  int64_t lock_expiration_us;
  bool allow_non_deadlock_error;
  // to simulate some useful work
  bool sleep_after_lock_acquisition;
};

class PointLockCorrectnessCheckTest
    : public PointLockManagerTest,
      public testing::WithParamInterface<PointLockCorrectnessCheckTestParam> {
 public:
  void SetUp() override {
    init();
    auto const& param = GetParam();
    auto per_key_lock_manager = param.is_per_key_point_lock_manager;
    if (per_key_lock_manager) {
      locker_.reset(new PerKeyPointLockManager(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
    } else {
      locker_.reset(new PointLockManager(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
    }

    txn_opt_.deadlock_detect = true;
    txn_opt_.lock_timeout = param.lock_timeout_us;
    txn_opt_.expiration = param.lock_expiration_us;

    values_.resize(param.key_count, 0);
    exclusive_lock_status_.resize(param.key_count, 0);

    // init counters and values
    for (size_t i = 0; i < param.key_count; i++) {
      counters_.emplace_back(std::make_unique<std::atomic_int>(0));
      shared_lock_count_.emplace_back(std::make_unique<std::atomic_int>(0));
    }
  }

 protected:
  TransactionOptions txn_opt_;
  std::vector<std::thread> threads_;
  std::atomic_int num_of_locks_acquired_ = 0;
  std::atomic_int num_of_shared_locks_acquired_ = 0;
  std::atomic_int num_of_exclusive_locks_acquired_ = 0;
  std::atomic_int num_of_deadlock_detected_ = 0;

  // Lock status only tracks whether
  std::vector<std::unique_ptr<std::atomic_int>> counters_;
  // values are read/write protected by the locks
  std::vector<int> values_;
  // use int64_t for boolean to track exclusive lock status. vector<bool> does
  // something special underneath, causes consistency issue.
  std::vector<int64_t> exclusive_lock_status_;
  // use counter to track number of shared locks to track shared lock status
  std::vector<std::unique_ptr<std::atomic_int>> shared_lock_count_;

  // shutdown flag
  std::atomic_bool shutdown_ = false;
};

TEST_P(PointLockCorrectnessCheckTest, LockCorrectnessValidation) {
  // Verify lock guarantee. Exclusive lock provide unique access guarantee.
  // Shared lock provide shared access guarantee.
  // Create multiple threads. Each try to grab a lock with random type on
  // random key. On exclusive lock, bump the value by 1. Meantime, update a
  // global counter for validation On shared lock, read the value and compare
  // it against the global counter to make sure its value matches At the end,
  // validate the value against the global counter.

  auto const& param = GetParam();

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);

  for (size_t thd_idx = 0; thd_idx < param.thread_count; thd_idx++) {
    threads_.emplace_back([this, &param, thd_idx]() {
      while (!shutdown_) {
        DEBUG_LOG("Thd %lu new txn\n", thd_idx);
        auto txn = NewTxn(txn_opt_);
        std::vector<std::pair<uint32_t, bool>> locked_key_with_types;
        // try to grab a random number of locks
        auto num_key_to_lock =
            Random::GetTLSInstance()->Uniform(
                static_cast<uint32_t>(param.max_num_keys_to_lock_per_txn)) +
            1;
        Status s;

        for (uint32_t j = 0; j < num_key_to_lock; j++) {
          uint32_t key = 0;
          key = Random::GetTLSInstance()->Uniform(
              static_cast<uint32_t>(param.key_count));
          auto key_str = std::to_string(key);
          bool isUpgrade = false;
          bool isDowngrade = false;

          // Decide lock type
          auto exclusive_lock_type = Random::GetTLSInstance()->OneIn(2);
          // check whether a lock on the same key is already held
          auto it = std::find_if(
              locked_key_with_types.begin(), locked_key_with_types.end(),
              [&key](std::pair<uint32_t, bool>& e) { return e.first == key; });
          if (it != locked_key_with_types.end()) {
            // a lock on the same key is already held.
            if (param.lock_type == LockTypeToTest::EXCLUSIVE_AND_SHARED) {
              // if test both shared and exclusive locks, switch their type
              if (it->second == false) {
                // If it is a shared lock, switch to an exclusive lock
                exclusive_lock_type = true;
                isUpgrade = true;
              } else {
                // If it is an exclusive lock, downgrade to a shared lock
                exclusive_lock_type = false;
                isDowngrade = true;
              }
            } else {
              // try to lock a different key
              j--;
              continue;
            }
          }
          if (param.lock_type != LockTypeToTest::EXCLUSIVE_AND_SHARED) {
            // if only one type of locks to be acquired, update its type
            exclusive_lock_type =
                (param.lock_type == LockTypeToTest::EXCLUSIVE_ONLY);
          }

          if (!param.allow_non_deadlock_error) {
            if (isDowngrade) {
              // Before downgrade, validate the lock is in exlusive status
              // This could not be done after downgrade, as another thread could
              // take a shared lock and update lock status
              ASSERT_TRUE(exclusive_lock_status_[key])
                  << "Thd " << thd_idx << " key " << key;
              ASSERT_EQ(*shared_lock_count_[key], 0)
                  << "Thd " << thd_idx << " key " << key;
              // for downgrade, update the lock status before acquiring the
              // lock, as afterwards, it will not have exclusive access to it
              exclusive_lock_status_[key] = 0;
            }
          }

          // try to acquire the lock
          DEBUG_LOG("Thd %lu try to acquire lock %u type %s\n", thd_idx, key,
                    exclusive_lock_type ? "exclusive" : "shared");
          s = locker_->TryLock(txn, 1, key_str, env_, exclusive_lock_type);
          if (s.ok()) {
            DEBUG_LOG("Thd %lu acquired lock %u type %s\n", thd_idx, key,
                      exclusive_lock_type ? "exclusive" : "shared");

            // update local lock status
            if (exclusive_lock_type) {
              if (isUpgrade) {
                it->second = true;
              } else {
                locked_key_with_types.emplace_back(key, exclusive_lock_type);
              }
              num_of_exclusive_locks_acquired_++;
            } else {
              if (isDowngrade) {
                it->second = false;
              } else {
                // Could not validate status was not in exclusive status, as
                // the lock could be downgraded by another thread.
                locked_key_with_types.emplace_back(key, exclusive_lock_type);
              }
              num_of_shared_locks_acquired_++;
            }
            num_of_locks_acquired_++;

            // Check and update global lock status
            if (!param.allow_non_deadlock_error) {
              // Validate lock status, if deadlock is the only allowed error.
              // otherwise, lock could be expired and stolen
              if (exclusive_lock_type) {
                // validate the lock is not in exclusive status
                ASSERT_FALSE(exclusive_lock_status_[key])
                    << "Thd " << thd_idx << " key " << key;
                if (isUpgrade) {
                  // validate the lock is in shared status and only had one
                  // shared lock
                  ASSERT_EQ(*shared_lock_count_[key], 1)
                      << "Thd " << thd_idx << " key " << key;
                  shared_lock_count_[key]->fetch_sub(1);
                } else {
                  ASSERT_EQ(*shared_lock_count_[key], 0)
                      << "Thd " << thd_idx << " key " << key;
                }
                // update the lock status
                exclusive_lock_status_[key] = 1;
              } else {
                shared_lock_count_[key]->fetch_add(1);
                ASSERT_FALSE(exclusive_lock_status_[key])
                    << "Thd " << thd_idx << " key " << key;
              }
            }
          } else {
            if (!param.allow_non_deadlock_error) {
              ASSERT_TRUE(s.IsDeadlock());
            }
            if (s.IsDeadlock()) {
              DEBUG_LOG("Thd %lu detected deadlock on key %u, abort\n", thd_idx,
                        key);
              num_of_deadlock_detected_++;
              // for deadlock, release all locks acquired
              break;
            } else {
              // for other errors, try again
              DEBUG_LOG(
                  "Thd %lu failed to acquire lock on key %u, due to %s, "
                  "abort\n",
                  thd_idx, key, s.ToString().c_str());
            }
          }
        }

        if (param.sleep_after_lock_acquisition && s.ok()) {
          // sleep for a random time between 0.5 and 1.5 times of the
          // expiration time to pretend to do some work, and allow some of
          // the lock expires
          auto sleep_time_us =
              param.lock_expiration_us / 2 +
              Random::GetTLSInstance()->Uniform(
                  static_cast<uint32_t>(param.lock_expiration_us));
          std::this_thread::sleep_for(std::chrono::microseconds(sleep_time_us));
        }

        // release all locks
        for (auto& key_type : locked_key_with_types) {
          auto key = key_type.first;
          ASSERT_TRUE(key < param.key_count);
          // Check global lock status
          if (!param.allow_non_deadlock_error) {
            ASSERT_EQ(counters_[key]->load(), values_[key])
                << "Thd " << thd_idx << " key " << key;
            auto exclusive = key_type.second;
            if (exclusive) {
              // exclusive lock
              // bump the value by 1
              (*counters_[key])++;
              values_[key]++;
              DEBUG_LOG("Thd %lu bump key %u by 1 to %d\n", thd_idx, key,
                        values[key]);
              ASSERT_EQ(counters_[key]->load(), values_[key])
                  << "Thd " << thd_idx << " key " << key;
            }
            // Validate lock status, if deadlock is the only allowed error.
            // otherwise, lock could be expired and stolen
            if (exclusive) {
              // exclusive lock
              ASSERT_TRUE(exclusive_lock_status_[key])
                  << "Thd " << thd_idx << " key " << key;
              ASSERT_EQ(*shared_lock_count_[key], 0)
                  << "Thd " << thd_idx << " key " << key;
              exclusive_lock_status_[key] = 0;
            } else {
              // shared lock
              ASSERT_FALSE(exclusive_lock_status_[key])
                  << "Thd " << thd_idx << " key " << key;
              ASSERT_GE(shared_lock_count_[key]->fetch_sub(1), 1)
                  << "Thd " << thd_idx << " key " << key;
            }
          }
          DEBUG_LOG("Thd %lu release lock %u\n", thd_idx, key);
          locker_->UnLock(txn, 1, std::to_string(key), env_);
        }
        delete txn;
      }
    });
  }

  // run test for a few seconds
  // print progress
  auto prev_num_of_locks_acquired = num_of_locks_acquired_.load();
  for (size_t i = 0; i < param.execution_time_sec; i++) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_GT(num_of_locks_acquired_.load(), prev_num_of_locks_acquired);
    prev_num_of_locks_acquired = num_of_locks_acquired_.load();
    DEBUG_LOG("num_of_locks_acquired: %d\n", num_of_locks_acquired_.load());
    DEBUG_LOG("num_of_exclusive_locks_acquired: %d\n",
              num_of_exclusive_locks_acquired_.load());
    DEBUG_LOG("num_of_shared_locks_acquired: %d\n",
              num_of_shared_locks_acquired_.load());
    DEBUG_LOG("num_of_deadlock_detected: %d\n",
              num_of_deadlock_detected_.load());
  }

  shutdown_ = true;
  for (auto& t : threads_) {
    t.join();
  }

  // validate values against counters
  for (size_t i = 0; i < param.key_count; i++) {
    ASSERT_EQ(counters_[i]->load(), values_[i]);
  }

  ASSERT_GE(num_of_locks_acquired_.load(), 0);
  printf("num_of_locks_acquired: %d\n", num_of_locks_acquired_.load());
}

INSTANTIATE_TEST_CASE_P(
    PointLockCorrectnessCheckTestSuite, PointLockCorrectnessCheckTest,
    ::testing::ValuesIn(std::vector<PointLockCorrectnessCheckTestParam>{
        // short timeout and expiration
        {true, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_AND_SHARED, 10, 10,
         true, true},
        {false, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_AND_SHARED, 10, 10,
         true, true},
        {true, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_ONLY, 10, 10, true,
         true},
        {false, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_ONLY, 10, 10, true,
         true},
        {true, 64, 16, 8, 10, LockTypeToTest::SHARED_ONLY, 10, 10, true, true},
        {false, 64, 16, 8, 10, LockTypeToTest::SHARED_ONLY, 10, 10, true, true},
        // long timeout and expiration
        {true, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_AND_SHARED,
         kLongTxnTimeoutUs, kLongTxnTimeoutUs, false, false},
        {false, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_AND_SHARED,
         kLongTxnTimeoutUs, kLongTxnTimeoutUs, false, false},
        {true, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_ONLY, kLongTxnTimeoutUs,
         kLongTxnTimeoutUs, false, false},
        {false, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_ONLY,
         kLongTxnTimeoutUs, kLongTxnTimeoutUs, false, false},
        {true, 64, 16, 8, 10, LockTypeToTest::SHARED_ONLY, kLongTxnTimeoutUs,
         kLongTxnTimeoutUs, false, false},
        {false, 64, 16, 8, 10, LockTypeToTest::SHARED_ONLY, kLongTxnTimeoutUs,
         kLongTxnTimeoutUs, false, false},
        // Low lock contention
        {true, 16, 4096, 2, 10, LockTypeToTest::SHARED_ONLY, kLongTxnTimeoutUs,
         kLongTxnTimeoutUs, false, false},
        {false, 16, 4096, 2, 10, LockTypeToTest::SHARED_ONLY, kLongTxnTimeoutUs,
         kLongTxnTimeoutUs, false, false},
    }));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
