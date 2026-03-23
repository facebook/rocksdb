RocksDB can reduce the latency of certain IO bound user queries using asynchronous IO. This capability currently benefits long scans and MultiGet. It requires a FileSystem implementation that supports async IO. Currently, PosixFileSystem supports it on Linux kernel versions that support IO uring.

# Scan

When async IO is enabled for an iterator, the iterator tries to parallelize operations as much as possible by not blocking on IO. This is accomplished in 2 ways -
* Seek - A seek operation on an iterator positions all the child iterators at the given target key. There is a child iterator for each level in the LSM, and the ```MergingIterator``` calls seek on each child ```LevelIterator```, which has to open the appropriate SST file and position the table iterator on the data block containing the closest key at or after the target key in the file. If the data block is not found in cache, it must be read from disk. This is typically a blocking read. However, with async IO, the table iterator seek happens in two stages. In the first stage, an async read is issued for the data block and a special status (```Status::TryAgain()```) is returned to the ```MergingIterator``` to indicate that a read is in progress. After a first pass through all the ```LevelIterators```, the ```MergingIterator``` makes a second pass through the ones that returned ```Status::TryAgain()```. In the second pass, each iterator waits for the read to complete, finishes positioning the iterator and then returns. This allows the data block cache misses and the resultant reads to be done in parallel.
* Next - When the user calls next on the iterator, it may require one or more child iterators to advance to the next data block. If its not in the block cache, a file read may be triggered. The iterator has prefetching logic to read ahead some amount of data beyond what's requested into a buffer. Subsequent Next operations can read data blocks from the buffer directly, until the end of buffer is reached and another read is required. Async IO takes this a step further by initiating a prefetch read when the iterator is at the midpoint of the prefetch buffer. The async prefetch read is for data beyond the current prefetch buffer. As long as prefetched data is useful, the iterator will keep asynchronously prefetching more data.

This option applies to direct IO. If buffered IO is used, the iterator relies on the page cache readahead.

Known limitations -
* Short scans may prefetch more data than necessary, compared to a scan without async_io.
* It does not apply to the ```CompactionIterator``` at the moment.

# MultiGet

The batched MultiGet API will use async IO where possible to read data blocks from multiple SST files in the same non-L0 level in parallel. A batch of MultiGet keys may overlap with many SST files in a level. By reading from these files in parallel using async IO, the overall MultiGet latency is reduced.

Known limitations -
* This feature requires RocksDB to be compiled with folly using a compiler with C++ 20 support. It relies on coroutines support in folly. The integration with folly is currently experimental.
* Metadata block reads are blocking reads.
* No parallelism across levels. The lookup in each LSM level will happen only after the previous level is finished.
* No parallelism in L0.
* This works best with larger batch sizes with IO bound workloads. CPU usage may increase due to the coroutine overhead.

# Configuration

Asynchronous IO for scans and MultiGet can be enabled by setting the ```async_io``` option in ```ReadOptions```. For MultiGet async IO, RocksDB has to be compiled using c++ 20 and ```-DUSE_COROUTINES``` compiler flag, and linked with [folly](https://github.com/facebook/folly).