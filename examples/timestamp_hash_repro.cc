#include <cstdint>
#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "testharness.h"
#include "rocksdb/sst_file_manager.h"
#include "util/hash.h"

// Taken from rocksdb
rocksdb::Slice StripTimestampFromUserKey(const rocksdb::Slice& user_key, const size_t ts_sz) noexcept {
        rocksdb::Slice ret = user_key;
        ret.remove_suffix(ts_sz);
        return ret;
}

// Taken from rocksdb
rocksdb::Slice ExtractTimestampFromUserKey(const rocksdb::Slice& user_key, const size_t ts_sz) noexcept {
        assert(user_key.size() >= ts_sz);
        return {user_key.data() + user_key.size() - ts_sz, ts_sz};
}

// Taken from rocksdb
void EncodeFixed32(char* buf, uint32_t value) noexcept {
        buf[0] = static_cast<char>(value & 0xff);
        buf[1] = static_cast<char>((value >> 8) & 0xff);
        buf[2] = static_cast<char>((value >> 16) & 0xff);
        buf[3] = static_cast<char>((value >> 24) & 0xff);
}

// Taken from rocksdb
uint32_t DecodeFixed32(const char* ptr) noexcept {
        return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0]))) |
                        (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8) |
                        (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16) |
                        (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
}

// Taken from rocksdb https://github.com/facebook/rocksdb/blob/main/util/comparator.cc#L237
class ComparatorWithTimestamp : public rocksdb::Comparator {
        public:
                explicit ComparatorWithTimestamp() noexcept : Comparator(/*ts_sz=*/sizeof(uint32_t)) {
                        assert(cmp_without_ts_->timestamp_size() == 0);
                }

                // The comparator that compares the user key without timestamp part is treated
                // as the root comparator.
                [[nodiscard]] const Comparator* GetRootComparator() const override { return cmp_without_ts_; }

                void FindShortSuccessor(std::string* /*key*/) const override {}
                void FindShortestSeparator(std::string* /*start*/, const rocksdb::Slice& /*limit*/) const override {}
                [[nodiscard]] int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
                        const int ret = CompareWithoutTimestamp(a, b);
                        const size_t ts_sz = timestamp_size();
                        if (ret != 0) {
                                return ret;
                        }
                        // Compare timestamp.
                        // For the same user key with different timestamps, larger (newer) timestamp
                        // comes first.
                        return -CompareTimestamp(ExtractTimestampFromUserKey(a, ts_sz), ExtractTimestampFromUserKey(b, ts_sz));
                }
                using Comparator::CompareWithoutTimestamp;
                [[nodiscard]] int CompareWithoutTimestamp(const rocksdb::Slice& a, bool a_has_ts, const rocksdb::Slice& b,
                                bool b_has_ts) const override {
                        const size_t ts_sz = timestamp_size();
                        assert(!a_has_ts || a.size() >= ts_sz);
                        assert(!b_has_ts || b.size() >= ts_sz);
                        const rocksdb::Slice lhs = a_has_ts ? StripTimestampFromUserKey(a, ts_sz) : a;
                        const rocksdb::Slice rhs = b_has_ts ? StripTimestampFromUserKey(b, ts_sz) : b;
                        return cmp_without_ts_->Compare(lhs, rhs);
                }
                [[nodiscard]] int CompareTimestamp(const rocksdb::Slice& ts1, const rocksdb::Slice& ts2) const override {
                        assert(ts1.size() == sizeof(uint32_t));
                        assert(ts2.size() == sizeof(uint32_t));
                        const auto lhs = DecodeFixed32(ts1.data());
                        const auto rhs = DecodeFixed32(ts2.data());
                        if (lhs < rhs) {
                                return -1;
                        } else if (lhs > rhs) {
                                return 1;
                        } else {
                                return 0;
                        }
                }
                [[nodiscard]] bool CanKeysWithDifferentByteContentsBeEqual() const override { return false; }

                [[nodiscard]] const char* Name() const override { return "name"; }

        private:
                const rocksdb::Comparator* cmp_without_ts_ = rocksdb::BytewiseComparator();
};

int main(int argc, char **argv) {
        size_t count = 10'000;
        if (argc == 2) {
                count = atoi(argv[1]);
        }
        const auto* path = "/tmp/rocksdb_timestamp_hash_repro"; // set your path

        rocksdb::DB* db_ptr{};
        rocksdb::Options options;
        ComparatorWithTimestamp cmp;
        options.create_if_missing = true;
        options.comparator = &cmp;

        rocksdb::BlockBasedTableOptions table_options;
        // Comment this line to make the test pass
        table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

        assert(rocksdb::DB::Open(options, path, &db_ptr).ok());
        const std::unique_ptr<rocksdb::DB> db(db_ptr);

        std::string timestamp(4, '\0');
        std::string key(32, '\0');
        char buf[32] = {0};

        for (uint32_t i = 0; i < count; ++i) {
                EncodeFixed32(timestamp.data(), i);
                db->Put(rocksdb::WriteOptions(), key, timestamp, std::to_string(i));
                assert(!memcmp(key.c_str(), buf, sizeof(buf)));
        }
        db->Flush(rocksdb::FlushOptions());

        std::string result;
        rocksdb::ReadOptions read_options;
        const rocksdb::Slice slice(timestamp);
        EncodeFixed32(timestamp.data(), count);
        read_options.timestamp = &slice;
        const auto status = db->Get(read_options, key, &result);
        assert(status.ok());
        flockfile(stdout);
        std::cout << "result:\t" << result << "\n";
        std::cout << "desired result:\t" << count - 1 << "\n";
        assert(result == std::to_string(count - 1)); // This fails and returnls 9823 instead of 9999
        std::cout << "passed!\n";
        funlockfile(stdout);
}
