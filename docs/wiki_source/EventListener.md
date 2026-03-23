# EventListener
EventListener class contains a set of call-back functions that will be called when a specific RocksDB event happens such as completing a flush or compaction job.  Such callback APIs can be used as a building block for developing custom features such as stats-collector or external compaction algorithm.  Available EventListener callbacks can be found in [include/rocksdb/listener.h](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/listener.h).

## How to use it?
In DBOptions, there's a variable called `listeners`, which allows developers to add custom EventListener to listen to the events of a specific rocksdb instance.

    // A vector of EventListeners which call-back functions will be called
    // when specific RocksDB event happens.
    std::vector<std::shared_ptr<EventListener>> listeners;

To listen to a rocksdb instance, it can be done by simply adding a custom EventListener to `DBOptions::listeners` and use that options to open a DB:

    // listen to a rocksdb instance
    DBOptions db_options;
    ...
    db_options.listeners.emplace_back(new MyListener());


Note that in either case, unless specially specified in the documentation, all EventListener call-backs must be implemented in a thread-safe way even when an EventListener only listens to a single column family (For example, imagine the case where `OnCompactionCompleted()` could be called by multiple threads at the same time as a single column family might complete more than one compaction jobs at the same time.

## Listen to a specific event
The default behavior of all EventListener callbacks is no-op.  This allows developers to only focus the event they're interested in.  To listen to a specific event, it is as simple as implementing its related call-back.  For example, the following EventListener counts the number of flush job completed since DB open by implementing `OnFlushCompleted()`:

    class FlushCountListener : public EventListener {
     public:
      FlushCountListener() : flush_count_(0) {}
      void OnFlushCompleted(
          DB* db,
          const FlushJobInfo& /*flush_job_info*/) override {
        flush_count_++;
      }
     private:
      std::atomic_int flush_count_;
    }; 

## Threading
All EventListener callbacks will be called using the actual thread that involves in that specific event.  For example, it is the RocksDB background flush thread that does the actual flush to call `EventListener::OnFlushCompleted()`.  This allows developers to collect thread-dependent stats from the EventListener callback such as via thread local variable.

## Locking
All EventListener callbacks are designed to be called without the current thread holding any DB mutex.  This is to prevent potential deadlock and performance issue when using EventListener callback in a complex way.  However, all EventListener call-back functions should not run for an extended period of time before the function returns, otherwise RocksDB may be blocked.  For example, it is not suggested to do `DB::CompactFiles()` (as it may run for a long while) or issue many of `DB::Put()` (as Put may be blocked in certain cases) in the same thread in the EventListener callback.  However, doing `DB::CompactFiles()` and `DB::Put()` in a thread other than the EventListener callback thread is considered safe.