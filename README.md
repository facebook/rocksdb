# RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![CircleCI Status](https://circleci.com/gh/facebook/rocksdb.svg?style=svg)](https://circleci.com/gh/facebook/rocksdb) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![License](https://img.shields.io/badge/License-GPLv2-blue.svg)](https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html)

RocksDB is a high-performance embedded database for key-value data, developed and maintained by Facebook Database Engineering Team. It's optimized for fast storage (SSD/NVM) and built on [LevelDB](https://github.com/google/leveldb) by Sanjay Ghemawat (sanjay@google.com) and Jeff Dean (jeff@google.com) at Google.

## Why RocksDB?

RocksDB excels when you need:
- **Fast storage on SSD/NVM** - Optimized for modern storage hardware
- **High write throughput** - Handles millions of writes per second
- **Large datasets** - Efficiently manages multiple terabytes in a single database
- **Fine-tuned performance** - Highly configurable to match your workload
- **Embedded database** - No separate server process required

## 🚀 Key Features

### Core Architecture
- **LSM-Tree Design** - Log-Structured Merge tree minimizes random writes
- **Multi-threaded Compaction** - Parallel background operations for consistent performance
- **Column Families** - Logical partitions with independent configurations
- **Configurable Trade-offs** - Balance between write, read, and space amplification

### Advanced Capabilities
- **Transactions** - ACID guarantees with optimistic and pessimistic concurrency
- **Backup & Checkpoints** - Online backups without blocking writes
- **TTL Support** - Automatic data expiration
- **Merge Operators** - Efficient read-modify-write operations
- **Compression** - Multiple algorithms (LZ4, Snappy, Zlib, ZSTD)
- **Bloom Filters** - Reduce unnecessary disk reads

## 📦 Installation

### Using Package Managers

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y librocksdb-dev
```

**macOS:**
```bash
brew install rocksdb
```

**CentOS/RHEL:**
```bash
sudo yum install -y rocksdb rocksdb-devel
```

### Building from Source

```bash
# Clone the repository
git clone https://github.com/facebook/rocksdb.git
cd rocksdb

# Install dependencies (Ubuntu/Debian)
sudo apt-get install -y build-essential libsnappy-dev zlib1g-dev libbz2-dev \
                         liblz4-dev libzstd-dev libgflags-dev

# Build shared library
make shared_lib -j$(nproc)

# Build static library
make static_lib -j$(nproc)

# Install (optional)
sudo make install-shared INSTALL_PATH=/usr/local
```

For other platforms (Windows, AIX, Solaris), see [INSTALL.md](https://github.com/facebook/rocksdb/blob/main/INSTALL.md).

## 💻 Quick Start

### C++ Example

```cpp
#include <iostream>
#include "rocksdb/db.h"

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    
    // Open database
    rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/testdb", &db);
    if (!status.ok()) {
        std::cerr << "Unable to open database: " << status.ToString() << std::endl;
        return 1;
    }
    
    // Write key-value
    status = db->Put(rocksdb::WriteOptions(), "key1", "value1");
    
    // Read value
    std::string value;
    status = db->Get(rocksdb::ReadOptions(), "key1", &value);
    if (status.ok()) {
        std::cout << "Retrieved: " << value << std::endl;
    }
    
    // Cleanup
    delete db;
    return 0;
}
```

Compile with:
```bash
g++ -std=c++17 example.cpp -lrocksdb -lpthread -lz -lbz2 -lsnappy -llz4 -lzstd
```

### Language Bindings

- **Java**: [RocksJava](https://github.com/facebook/rocksdb/wiki/RocksJava-Basics) (included)
- **Python**: [python-rocksdb](https://python-rocksdb.readthedocs.io/)
- **Go**: [gorocksdb](https://github.com/tecbot/gorocksdb)
- **Rust**: [rust-rocksdb](https://github.com/rust-rocksdb/rust-rocksdb)
- **Node.js**: [rocksdb-node](https://github.com/Level/rocksdb)

## 🎯 Use Cases

RocksDB powers critical infrastructure at scale:

| Company | Use Case |
|---------|----------|
| **Facebook** | MyRocks (MySQL storage engine), ZippyDB, Social Graph |
| **LinkedIn** | Stream processing, Apache Samza state store |
| **Netflix** | SSD caching layer for video streaming |
| **Uber** | Schemaless datastore |
| **Apache Flink** | State backend for stream processing |
| **TiKV** | Distributed transactional key-value database |

Common applications:
- Time-series data storage
- Blockchain and cryptocurrency nodes
- Real-time analytics
- Message queuing systems
- Caching layers
- Graph databases

## ⚙️ Configuration

RocksDB offers extensive tuning options. Here's a performance-oriented configuration:

```cpp
rocksdb::Options options;

// Optimize for SSD
options.compression = rocksdb::kLZ4Compression;
options.bottommost_compression = rocksdb::kZSTD;
options.write_buffer_size = 64 * 1024 * 1024;  // 64MB
options.max_write_buffer_number = 3;
options.target_file_size_base = 64 * 1024 * 1024;  // 64MB

// Parallelism
options.max_background_compactions = 4;
options.max_background_flushes = 2;

// Bloom filters for read performance
rocksdb::BlockBasedTableOptions table_options;
table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
table_options.block_cache = rocksdb::NewLRUCache(512 * 1024 * 1024);  // 512MB
options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
```

See the [Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) for detailed optimization strategies.

## 📊 Performance

Typical performance characteristics on modern NVMe SSD:

- **Random Writes**: 500K+ ops/sec
- **Random Reads**: 1M+ ops/sec (from cache)
- **Sequential Writes**: Saturates storage bandwidth
- **Compression**: 2-10x reduction depending on data

Performance varies significantly based on configuration, hardware, and workload.

## 🛠️ Tools and Utilities

- `sst_dump` - Inspect SST files
- `ldb` - Command-line database tool
- `db_bench` - Benchmarking tool
- `db_stress` - Stress testing tool

Example:
```bash
# Benchmark random writes
./db_bench --benchmarks=fillrandom --num=1000000 --value_size=100

# Inspect database
./ldb --db=/tmp/testdb scan
```

## 📚 Documentation

- **[Wiki](https://github.com/facebook/rocksdb/wiki)** - Comprehensive documentation
- **[API Reference](https://github.com/facebook/rocksdb/tree/main/include/rocksdb)** - Header files in `include/`
- **[Examples](https://github.com/facebook/rocksdb/tree/main/examples)** - Sample applications
- **[Blog](http://rocksdb.org/blog/)** - Technical articles and updates

## 🤝 Community

- **[GitHub Issues](https://github.com/facebook/rocksdb/issues)** - Bug reports and feature requests
- **[Google Groups](https://groups.google.com/g/rocksdb)** - Mailing list
- **[Facebook Group](https://www.facebook.com/groups/rocksdb.dev/)** - Community discussions

## 📄 License

RocksDB is dual-licensed:
- **Apache License 2.0** ([LICENSE.Apache](LICENSE.Apache))
- **GPL v2** ([COPYING](COPYING))

Choose the license that best suits your project.

## 🙏 Acknowledgments

RocksDB builds on the excellent foundation of [LevelDB](https://github.com/google/leveldb) created by Sanjay Ghemawat (sanjay@google.com) and Jeff Dean (jeff@google.com). We thank the LevelDB authors and the open-source community for their contributions.

---

*Developed and maintained by Facebook Database Engineering Team*
