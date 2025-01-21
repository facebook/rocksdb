//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <algorithm>
#include <atomic>

#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/skiplist.h"
#include "monitoring/histogram.h"
#include "port/port.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/options_type.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {
namespace {

using Key = const char*;
using MemtableSkipList = SkipList<Key, const MemTableRep::KeyComparator&>;
using Pointer = std::atomic<void*>;

// A data structure used as the header of a link list of a hash bucket.
struct BucketHeader {
  Pointer next;
  std::atomic<uint32_t> num_entries;

  explicit BucketHeader(void* n, uint32_t count)
      : next(n), num_entries(count) {}

  bool IsSkipListBucket() {
    return next.load(std::memory_order_relaxed) == this;
  }

  uint32_t GetNumEntries() const {
    return num_entries.load(std::memory_order_relaxed);
  }

  // REQUIRES: called from single-threaded Insert()
  void IncNumEntries() {
    // Only one thread can do write at one time. No need to do atomic
    // incremental. Update it with relaxed load and store.
    num_entries.store(GetNumEntries() + 1, std::memory_order_relaxed);
  }
};

// A data structure used as the header of a skip list of a hash bucket.
struct SkipListBucketHeader {
  BucketHeader Counting_header;
  MemtableSkipList skip_list;

  explicit SkipListBucketHeader(const MemTableRep::KeyComparator& cmp,
                                Allocator* allocator, uint32_t count)
      : Counting_header(this,  // Pointing to itself to indicate header type.
                        count),
        skip_list(cmp, allocator) {}
};

struct Node {
  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next() {
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return next_.load(std::memory_order_acquire);
  }
  void SetNext(Node* x) {
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_.store(x, std::memory_order_release);
  }
  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next() { return next_.load(std::memory_order_relaxed); }

  void NoBarrier_SetNext(Node* x) { next_.store(x, std::memory_order_relaxed); }

  // Needed for placement new below which is fine
  Node() = default;

 private:
  std::atomic<Node*> next_;

  // Prohibit copying due to the below
  Node(const Node&) = delete;
  Node& operator=(const Node&) = delete;

 public:
  char key[1];
};

// Memory structure of the mem table:
// It is a hash table, each bucket points to one entry, a linked list or a
// skip list. In order to track total number of records in a bucket to determine
// whether should switch to skip list, a header is added just to indicate
// number of entries in the bucket.
//
//
//          +-----> NULL    Case 1. Empty bucket
//          |
//          |
//          | +---> +-------+
//          | |     | Next  +--> NULL
//          | |     +-------+
//  +-----+ | |     |       |  Case 2. One Entry in bucket.
//  |     +-+ |     | Data  |          next pointer points to
//  +-----+   |     |       |          NULL. All other cases
//  |     |   |     |       |          next pointer is not NULL.
//  +-----+   |     +-------+
//  |     +---+
//  +-----+     +-> +-------+  +> +-------+  +-> +-------+
//  |     |     |   | Next  +--+  | Next  +--+   | Next  +-->NULL
//  +-----+     |   +-------+     +-------+      +-------+
//  |     +-----+   | Count |     |       |      |       |
//  +-----+         +-------+     | Data  |      | Data  |
//  |     |                       |       |      |       |
//  +-----+          Case 3.      |       |      |       |
//  |     |          A header     +-------+      +-------+
//  +-----+          points to
//  |     |          a linked list. Count indicates total number
//  +-----+          of rows in this bucket.
//  |     |
//  +-----+    +-> +-------+ <--+
//  |     |    |   | Next  +----+
//  +-----+    |   +-------+   Case 4. A header points to a skip
//  |     +----+   | Count |           list and next pointer points to
//  +-----+        +-------+           itself, to distinguish case 3 or 4.
//  |     |        |       |           Count still is kept to indicates total
//  +-----+        | Skip +-->         of entries in the bucket for debugging
//  |     |        | List  |   Data    purpose.
//  |     |        |      +-->
//  +-----+        |       |
//  |     |        +-------+
//  +-----+
//
// We don't have data race when changing cases because:
// (1) When changing from case 2->3, we create a new bucket header, put the
//     single node there first without changing the original node, and do a
//     release store when changing the bucket pointer. In that case, a reader
//     who sees a stale value of the bucket pointer will read this node, while
//     a reader sees the correct value because of the release store.
// (2) When changing case 3->4, a new header is created with skip list points
//     to the data, before doing an acquire store to change the bucket pointer.
//     The old header and nodes are never changed, so any reader sees any
//     of those existing pointers will guarantee to be able to iterate to the
//     end of the linked list.
// (3) Header's next pointer in case 3 might change, but they are never equal
//     to itself, so no matter a reader sees any stale or newer value, it will
//     be able to correctly distinguish case 3 and 4.
//
// The reason that we use case 2 is we want to make the format to be efficient
// when the utilization of buckets is relatively low. If we use case 3 for
// single entry bucket, we will need to waste 12 bytes for every entry,
// which can be significant decrease of memory utilization.
class HashLinkListRep : public MemTableRep {
 public:
  HashLinkListRep(const MemTableRep::KeyComparator& compare,
                  Allocator* allocator, const SliceTransform* transform,
                  size_t bucket_size, uint32_t threshold_use_skiplist,
                  size_t huge_page_tlb_size, Logger* logger,
                  int bucket_entries_logging_threshold,
                  bool if_log_bucket_dist_when_flash);

  KeyHandle Allocate(const size_t len, char** buf) override;

  void Insert(KeyHandle handle) override;

  bool Contains(const char* key) const override;

  size_t ApproximateMemoryUsage() override;

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  ~HashLinkListRep() override;

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  MemTableRep::Iterator* GetDynamicPrefixIterator(
      Arena* arena = nullptr) override;

 private:
  friend class DynamicIterator;

  size_t bucket_size_;

  // Maps slices (which are transformed user keys) to buckets of keys sharing
  // the same transform.
  Pointer* buckets_;

  const uint32_t threshold_use_skiplist_;

  // The user-supplied transform whose domain is the user keys.
  const SliceTransform* transform_;

  const MemTableRep::KeyComparator& compare_;

  Logger* logger_;
  int bucket_entries_logging_threshold_;
  bool if_log_bucket_dist_when_flash_;

  bool LinkListContains(Node* head, const Slice& key) const;

  bool IsEmptyBucket(Pointer& bucket_pointer) const {
    return bucket_pointer.load(std::memory_order_acquire) == nullptr;
  }

  // Precondition: GetLinkListFirstNode() must have been called first and return
  // null so that it must be a skip list bucket
  SkipListBucketHeader* GetSkipListBucketHeader(Pointer& bucket_pointer) const;

  // Returning nullptr indicates it is a skip list bucket.
  Node* GetLinkListFirstNode(Pointer& bucket_pointer) const;

  Slice GetPrefix(const Slice& internal_key) const {
    return transform_->Transform(ExtractUserKey(internal_key));
  }

  size_t GetHash(const Slice& slice) const {
    return GetSliceRangedNPHash(slice, bucket_size_);
  }

  Pointer& GetBucket(size_t i) const { return buckets_[i]; }

  Pointer& GetBucket(const Slice& slice) const {
    return GetBucket(GetHash(slice));
  }

  bool Equal(const Slice& a, const Key& b) const {
    return (compare_(b, a) == 0);
  }

  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  bool KeyIsAfterNode(const Slice& internal_key, const Node* n) const {
    // nullptr n is considered infinite
    return (n != nullptr) && (compare_(n->key, internal_key) < 0);
  }

  bool KeyIsAfterNode(const Key& key, const Node* n) const {
    // nullptr n is considered infinite
    return (n != nullptr) && (compare_(n->key, key) < 0);
  }

  bool KeyIsAfterOrAtNode(const Slice& internal_key, const Node* n) const {
    // nullptr n is considered infinite
    return (n != nullptr) && (compare_(n->key, internal_key) <= 0);
  }

  bool KeyIsAfterOrAtNode(const Key& key, const Node* n) const {
    // nullptr n is considered infinite
    return (n != nullptr) && (compare_(n->key, key) <= 0);
  }

  Node* FindGreaterOrEqualInBucket(Node* head, const Slice& key) const;
  Node* FindLessOrEqualInBucket(Node* head, const Slice& key) const;

  class FullListIterator : public MemTableRep::Iterator {
   public:
    explicit FullListIterator(MemtableSkipList* list, Allocator* allocator)
        : iter_(list), full_list_(list), allocator_(allocator) {}

    ~FullListIterator() override = default;

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override { return iter_.Valid(); }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const override {
      assert(Valid());
      return iter_.key();
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() override {
      assert(Valid());
      iter_.Next();
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev() override {
      assert(Valid());
      iter_.Prev();
    }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& internal_key, const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);
      iter_.Seek(encoded_key);
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);
      iter_.SeekForPrev(encoded_key);
    }

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    void SeekToFirst() override { iter_.SeekToFirst(); }

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    void SeekToLast() override { iter_.SeekToLast(); }

   private:
    MemtableSkipList::Iterator iter_;
    // To destruct with the iterator.
    std::unique_ptr<MemtableSkipList> full_list_;
    std::unique_ptr<Allocator> allocator_;
    std::string tmp_;  // For passing to EncodeKey
  };

  class LinkListIterator : public MemTableRep::Iterator {
   public:
    explicit LinkListIterator(const HashLinkListRep* const hash_link_list_rep,
                              Node* head)
        : hash_link_list_rep_(hash_link_list_rep),
          head_(head),
          node_(nullptr) {}

    ~LinkListIterator() override = default;

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override { return node_ != nullptr; }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const override {
      assert(Valid());
      return node_->key;
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() override {
      assert(Valid());
      node_ = node_->Next();
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev() override {
      // Prefix iterator does not support total order.
      // We simply set the iterator to invalid state
      Reset(nullptr);
    }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& internal_key,
              const char* /*memtable_key*/) override {
      node_ =
          hash_link_list_rep_->FindGreaterOrEqualInBucket(head_, internal_key);
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& /*internal_key*/,
                     const char* /*memtable_key*/) override {
      // Since we do not support Prev()
      // We simply do not support SeekForPrev
      Reset(nullptr);
    }

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    void SeekToFirst() override {
      // Prefix iterator does not support total order.
      // We simply set the iterator to invalid state
      Reset(nullptr);
    }

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    void SeekToLast() override {
      // Prefix iterator does not support total order.
      // We simply set the iterator to invalid state
      Reset(nullptr);
    }

   protected:
    void Reset(Node* head) {
      head_ = head;
      node_ = nullptr;
    }

   private:
    friend class HashLinkListRep;
    const HashLinkListRep* const hash_link_list_rep_;
    Node* head_;
    Node* node_;

    virtual void SeekToHead() { node_ = head_; }
  };

  class DynamicIterator : public HashLinkListRep::LinkListIterator {
   public:
    explicit DynamicIterator(HashLinkListRep& memtable_rep)
        : HashLinkListRep::LinkListIterator(&memtable_rep, nullptr),
          memtable_rep_(memtable_rep) {}

    // Advance to the first entry with a key >= target
    void Seek(const Slice& k, const char* memtable_key) override {
      auto transformed = memtable_rep_.GetPrefix(k);
      Pointer& bucket = memtable_rep_.GetBucket(transformed);

      if (memtable_rep_.IsEmptyBucket(bucket)) {
        skip_list_iter_.reset();
        Reset(nullptr);
      } else {
        Node* first_linked_list_node =
            memtable_rep_.GetLinkListFirstNode(bucket);
        if (first_linked_list_node != nullptr) {
          // The bucket is organized as a linked list
          skip_list_iter_.reset();
          Reset(first_linked_list_node);
          HashLinkListRep::LinkListIterator::Seek(k, memtable_key);

        } else {
          SkipListBucketHeader* skip_list_header =
              memtable_rep_.GetSkipListBucketHeader(bucket);
          assert(skip_list_header != nullptr);
          // The bucket is organized as a skip list
          if (!skip_list_iter_) {
            skip_list_iter_.reset(
                new MemtableSkipList::Iterator(&skip_list_header->skip_list));
          } else {
            skip_list_iter_->SetList(&skip_list_header->skip_list);
          }
          if (memtable_key != nullptr) {
            skip_list_iter_->Seek(memtable_key);
          } else {
            IterKey encoded_key;
            encoded_key.EncodeLengthPrefixedKey(k);
            skip_list_iter_->Seek(encoded_key.GetUserKey().data());
          }
        }
      }
    }

    bool Valid() const override {
      if (skip_list_iter_) {
        return skip_list_iter_->Valid();
      }
      return HashLinkListRep::LinkListIterator::Valid();
    }

    const char* key() const override {
      if (skip_list_iter_) {
        return skip_list_iter_->key();
      }
      return HashLinkListRep::LinkListIterator::key();
    }

    void Next() override {
      if (skip_list_iter_) {
        skip_list_iter_->Next();
      } else {
        HashLinkListRep::LinkListIterator::Next();
      }
    }

   private:
    // the underlying memtable
    const HashLinkListRep& memtable_rep_;
    std::unique_ptr<MemtableSkipList::Iterator> skip_list_iter_;
  };

  class EmptyIterator : public MemTableRep::Iterator {
    // This is used when there wasn't a bucket. It is cheaper than
    // instantiating an empty bucket over which to iterate.
   public:
    EmptyIterator() = default;
    bool Valid() const override { return false; }
    const char* key() const override {
      assert(false);
      return nullptr;
    }
    void Next() override {}
    void Prev() override {}
    void Seek(const Slice& /*user_key*/,
              const char* /*memtable_key*/) override {}
    void SeekForPrev(const Slice& /*user_key*/,
                     const char* /*memtable_key*/) override {}
    void SeekToFirst() override {}
    void SeekToLast() override {}

   private:
  };
};

HashLinkListRep::HashLinkListRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* transform, size_t bucket_size,
    uint32_t threshold_use_skiplist, size_t huge_page_tlb_size, Logger* logger,
    int bucket_entries_logging_threshold, bool if_log_bucket_dist_when_flash)
    : MemTableRep(allocator),
      bucket_size_(bucket_size),
      // Threshold to use skip list doesn't make sense if less than 3, so we
      // force it to be minimum of 3 to simplify implementation.
      threshold_use_skiplist_(std::max(threshold_use_skiplist, 3U)),
      transform_(transform),
      compare_(compare),
      logger_(logger),
      bucket_entries_logging_threshold_(bucket_entries_logging_threshold),
      if_log_bucket_dist_when_flash_(if_log_bucket_dist_when_flash) {
  char* mem = allocator_->AllocateAligned(sizeof(Pointer) * bucket_size,
                                          huge_page_tlb_size, logger);

  buckets_ = new (mem) Pointer[bucket_size];

  for (size_t i = 0; i < bucket_size_; ++i) {
    buckets_[i].store(nullptr, std::memory_order_relaxed);
  }
}

HashLinkListRep::~HashLinkListRep() = default;

KeyHandle HashLinkListRep::Allocate(const size_t len, char** buf) {
  char* mem = allocator_->AllocateAligned(sizeof(Node) + len);
  Node* x = new (mem) Node();
  *buf = x->key;
  return static_cast<void*>(x);
}

SkipListBucketHeader* HashLinkListRep::GetSkipListBucketHeader(
    Pointer& bucket_pointer) const {
  Pointer* first_next_pointer =
      static_cast<Pointer*>(bucket_pointer.load(std::memory_order_acquire));
  assert(first_next_pointer != nullptr);
  assert(first_next_pointer->load(std::memory_order_relaxed) != nullptr);

  // Counting header
  BucketHeader* header = reinterpret_cast<BucketHeader*>(first_next_pointer);
  assert(header->IsSkipListBucket());
  assert(header->GetNumEntries() > threshold_use_skiplist_);
  auto* skip_list_bucket_header =
      reinterpret_cast<SkipListBucketHeader*>(header);
  assert(skip_list_bucket_header->Counting_header.next.load(
             std::memory_order_relaxed) == header);
  return skip_list_bucket_header;
}

Node* HashLinkListRep::GetLinkListFirstNode(Pointer& bucket_pointer) const {
  Pointer* first_next_pointer =
      static_cast<Pointer*>(bucket_pointer.load(std::memory_order_acquire));
  assert(first_next_pointer != nullptr);
  if (first_next_pointer->load(std::memory_order_relaxed) == nullptr) {
    // Single entry bucket
    return reinterpret_cast<Node*>(first_next_pointer);
  }

  // It is possible that after we fetch first_next_pointer it is modified
  // and the next is not null anymore. In this case, the bucket should have been
  // modified to a counting header, so we should reload the first_next_pointer
  // to make sure we see the update.
  first_next_pointer =
      static_cast<Pointer*>(bucket_pointer.load(std::memory_order_acquire));
  // Counting header
  BucketHeader* header = reinterpret_cast<BucketHeader*>(first_next_pointer);
  if (!header->IsSkipListBucket()) {
    assert(header->GetNumEntries() <= threshold_use_skiplist_);
    return reinterpret_cast<Node*>(
        header->next.load(std::memory_order_acquire));
  }
  assert(header->GetNumEntries() > threshold_use_skiplist_);
  return nullptr;
}

void HashLinkListRep::Insert(KeyHandle handle) {
  Node* x = static_cast<Node*>(handle);
  assert(!Contains(x->key));
  Slice internal_key = GetLengthPrefixedSlice(x->key);
  auto transformed = GetPrefix(internal_key);
  auto& bucket = buckets_[GetHash(transformed)];
  Pointer* first_next_pointer =
      static_cast<Pointer*>(bucket.load(std::memory_order_relaxed));

  if (first_next_pointer == nullptr) {
    // Case 1. empty bucket
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(nullptr);
    bucket.store(x, std::memory_order_release);
    return;
  }

  BucketHeader* header = nullptr;
  if (first_next_pointer->load(std::memory_order_relaxed) == nullptr) {
    // Case 2. only one entry in the bucket
    // Need to convert to a Counting bucket and turn to case 4.
    Node* first = reinterpret_cast<Node*>(first_next_pointer);
    // Need to add a bucket header.
    // We have to first convert it to a bucket with header before inserting
    // the new node. Otherwise, we might need to change next pointer of first.
    // In that case, a reader might sees the next pointer is NULL and wrongly
    // think the node is a bucket header.
    auto* mem = allocator_->AllocateAligned(sizeof(BucketHeader));
    header = new (mem) BucketHeader(first, 1);
    bucket.store(header, std::memory_order_release);
  } else {
    header = reinterpret_cast<BucketHeader*>(first_next_pointer);
    if (header->IsSkipListBucket()) {
      // Case 4. Bucket is already a skip list
      assert(header->GetNumEntries() > threshold_use_skiplist_);
      auto* skip_list_bucket_header =
          reinterpret_cast<SkipListBucketHeader*>(header);
      // Only one thread can execute Insert() at one time. No need to do atomic
      // incremental.
      skip_list_bucket_header->Counting_header.IncNumEntries();
      skip_list_bucket_header->skip_list.Insert(x->key);
      return;
    }
  }

  if (bucket_entries_logging_threshold_ > 0 &&
      header->GetNumEntries() ==
          static_cast<uint32_t>(bucket_entries_logging_threshold_)) {
    Info(logger_,
         "HashLinkedList bucket %" ROCKSDB_PRIszt
         " has more than %d "
         "entries. Key to insert: %s",
         GetHash(transformed), header->GetNumEntries(),
         GetLengthPrefixedSlice(x->key).ToString(true).c_str());
  }

  if (header->GetNumEntries() == threshold_use_skiplist_) {
    // Case 3. number of entries reaches the threshold so need to convert to
    // skip list.
    LinkListIterator bucket_iter(
        this, reinterpret_cast<Node*>(
                  first_next_pointer->load(std::memory_order_relaxed)));
    auto mem = allocator_->AllocateAligned(sizeof(SkipListBucketHeader));
    SkipListBucketHeader* new_skip_list_header = new (mem)
        SkipListBucketHeader(compare_, allocator_, header->GetNumEntries() + 1);
    auto& skip_list = new_skip_list_header->skip_list;

    // Add all current entries to the skip list
    for (bucket_iter.SeekToHead(); bucket_iter.Valid(); bucket_iter.Next()) {
      skip_list.Insert(bucket_iter.key());
    }

    // insert the new entry
    skip_list.Insert(x->key);
    // Set the bucket
    bucket.store(new_skip_list_header, std::memory_order_release);
  } else {
    // Case 5. Need to insert to the sorted linked list without changing the
    // header.
    Node* first =
        reinterpret_cast<Node*>(header->next.load(std::memory_order_relaxed));
    assert(first != nullptr);
    // Advance counter unless the bucket needs to be advanced to skip list.
    // In that case, we need to make sure the previous count never exceeds
    // threshold_use_skiplist_ to avoid readers to cast to wrong format.
    header->IncNumEntries();

    Node* cur = first;
    Node* prev = nullptr;
    while (true) {
      if (cur == nullptr) {
        break;
      }
      Node* next = cur->Next();
      // Make sure the lists are sorted.
      // If x points to head_ or next points nullptr, it is trivially satisfied.
      assert((cur == first) || (next == nullptr) ||
             KeyIsAfterNode(next->key, cur));
      if (KeyIsAfterNode(internal_key, cur)) {
        // Keep searching in this list
        prev = cur;
        cur = next;
      } else {
        break;
      }
    }

    // Our data structure does not allow duplicate insertion
    assert(cur == nullptr || !Equal(x->key, cur->key));

    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(cur);

    if (prev) {
      prev->SetNext(x);
    } else {
      header->next.store(static_cast<void*>(x), std::memory_order_release);
    }
  }
}

bool HashLinkListRep::Contains(const char* key) const {
  Slice internal_key = GetLengthPrefixedSlice(key);

  auto transformed = GetPrefix(internal_key);
  Pointer& bucket = GetBucket(transformed);
  if (IsEmptyBucket(bucket)) {
    return false;
  }

  Node* linked_list_node = GetLinkListFirstNode(bucket);
  if (linked_list_node != nullptr) {
    return LinkListContains(linked_list_node, internal_key);
  }

  SkipListBucketHeader* skip_list_header = GetSkipListBucketHeader(bucket);
  if (skip_list_header != nullptr) {
    return skip_list_header->skip_list.Contains(key);
  }
  return false;
}

size_t HashLinkListRep::ApproximateMemoryUsage() {
  // Memory is always allocated from the allocator.
  return 0;
}

void HashLinkListRep::Get(const LookupKey& k, void* callback_args,
                          bool (*callback_func)(void* arg, const char* entry)) {
  auto transformed = transform_->Transform(k.user_key());
  Pointer& bucket = GetBucket(transformed);

  if (IsEmptyBucket(bucket)) {
    return;
  }

  auto* link_list_head = GetLinkListFirstNode(bucket);
  if (link_list_head != nullptr) {
    LinkListIterator iter(this, link_list_head);
    for (iter.Seek(k.internal_key(), nullptr);
         iter.Valid() && callback_func(callback_args, iter.key());
         iter.Next()) {
    }
  } else {
    auto* skip_list_header = GetSkipListBucketHeader(bucket);
    if (skip_list_header != nullptr) {
      // Is a skip list
      MemtableSkipList::Iterator iter(&skip_list_header->skip_list);
      for (iter.Seek(k.memtable_key().data());
           iter.Valid() && callback_func(callback_args, iter.key());
           iter.Next()) {
      }
    }
  }
}

MemTableRep::Iterator* HashLinkListRep::GetIterator(Arena* alloc_arena) {
  // allocate a new arena of similar size to the one currently in use
  Arena* new_arena = new Arena(allocator_->BlockSize());
  auto list = new MemtableSkipList(compare_, new_arena);
  HistogramImpl keys_per_bucket_hist;

  for (size_t i = 0; i < bucket_size_; ++i) {
    int count = 0;
    Pointer& bucket = GetBucket(i);
    if (!IsEmptyBucket(bucket)) {
      auto* link_list_head = GetLinkListFirstNode(bucket);
      if (link_list_head != nullptr) {
        LinkListIterator itr(this, link_list_head);
        for (itr.SeekToHead(); itr.Valid(); itr.Next()) {
          list->Insert(itr.key());
          count++;
        }
      } else {
        auto* skip_list_header = GetSkipListBucketHeader(bucket);
        assert(skip_list_header != nullptr);
        // Is a skip list
        MemtableSkipList::Iterator itr(&skip_list_header->skip_list);
        for (itr.SeekToFirst(); itr.Valid(); itr.Next()) {
          list->Insert(itr.key());
          count++;
        }
      }
    }
    if (if_log_bucket_dist_when_flash_) {
      keys_per_bucket_hist.Add(count);
    }
  }
  if (if_log_bucket_dist_when_flash_ && logger_ != nullptr) {
    Info(logger_, "hashLinkedList Entry distribution among buckets: %s",
         keys_per_bucket_hist.ToString().c_str());
  }

  if (alloc_arena == nullptr) {
    return new FullListIterator(list, new_arena);
  } else {
    auto mem = alloc_arena->AllocateAligned(sizeof(FullListIterator));
    return new (mem) FullListIterator(list, new_arena);
  }
}

MemTableRep::Iterator* HashLinkListRep::GetDynamicPrefixIterator(
    Arena* alloc_arena) {
  if (alloc_arena == nullptr) {
    return new DynamicIterator(*this);
  } else {
    auto mem = alloc_arena->AllocateAligned(sizeof(DynamicIterator));
    return new (mem) DynamicIterator(*this);
  }
}

bool HashLinkListRep::LinkListContains(Node* head,
                                       const Slice& user_key) const {
  Node* x = FindGreaterOrEqualInBucket(head, user_key);
  return (x != nullptr && Equal(user_key, x->key));
}

Node* HashLinkListRep::FindGreaterOrEqualInBucket(Node* head,
                                                  const Slice& key) const {
  Node* x = head;
  while (true) {
    if (x == nullptr) {
      return x;
    }
    Node* next = x->Next();
    // Make sure the lists are sorted.
    // If x points to head_ or next points nullptr, it is trivially satisfied.
    assert((x == head) || (next == nullptr) || KeyIsAfterNode(next->key, x));
    if (KeyIsAfterNode(key, x)) {
      // Keep searching in this list
      x = next;
    } else {
      break;
    }
  }
  return x;
}

struct HashLinkListRepOptions {
  static const char* kName() { return "HashLinkListRepFactoryOptions"; }
  size_t bucket_count;
  uint32_t threshold_use_skiplist;
  size_t huge_page_tlb_size;
  int bucket_entries_logging_threshold;
  bool if_log_bucket_dist_when_flash;
};

static std::unordered_map<std::string, OptionTypeInfo> hash_linklist_info = {
    {"bucket_count",
     {offsetof(struct HashLinkListRepOptions, bucket_count), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"threshold",
     {offsetof(struct HashLinkListRepOptions, threshold_use_skiplist),
      OptionType::kUInt32T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"huge_page_size",
     {offsetof(struct HashLinkListRepOptions, huge_page_tlb_size),
      OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"logging_threshold",
     {offsetof(struct HashLinkListRepOptions, bucket_entries_logging_threshold),
      OptionType::kInt, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"log_when_flash",
     {offsetof(struct HashLinkListRepOptions, if_log_bucket_dist_when_flash),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
};

class HashLinkListRepFactory : public MemTableRepFactory {
 public:
  explicit HashLinkListRepFactory(size_t bucket_count,
                                  uint32_t threshold_use_skiplist,
                                  size_t huge_page_tlb_size,
                                  int bucket_entries_logging_threshold,
                                  bool if_log_bucket_dist_when_flash) {
    options_.bucket_count = bucket_count;
    options_.threshold_use_skiplist = threshold_use_skiplist;
    options_.huge_page_tlb_size = huge_page_tlb_size;
    options_.bucket_entries_logging_threshold =
        bucket_entries_logging_threshold;
    options_.if_log_bucket_dist_when_flash = if_log_bucket_dist_when_flash;
    RegisterOptions(&options_, &hash_linklist_info);
  }

  using MemTableRepFactory::CreateMemTableRep;
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger) override;

  static const char* kClassName() { return "HashLinkListRepFactory"; }
  static const char* kNickName() { return "hash_linkedlist"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

 private:
  HashLinkListRepOptions options_;
};

}  // namespace

MemTableRep* HashLinkListRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* transform, Logger* logger) {
  return new HashLinkListRep(
      compare, allocator, transform, options_.bucket_count,
      options_.threshold_use_skiplist, options_.huge_page_tlb_size, logger,
      options_.bucket_entries_logging_threshold,
      options_.if_log_bucket_dist_when_flash);
}

MemTableRepFactory* NewHashLinkListRepFactory(
    size_t bucket_count, size_t huge_page_tlb_size,
    int bucket_entries_logging_threshold, bool if_log_bucket_dist_when_flash,
    uint32_t threshold_use_skiplist) {
  return new HashLinkListRepFactory(
      bucket_count, threshold_use_skiplist, huge_page_tlb_size,
      bucket_entries_logging_threshold, if_log_bucket_dist_when_flash);
}

}  // namespace ROCKSDB_NAMESPACE
