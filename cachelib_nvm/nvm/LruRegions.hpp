#pragma once

#include "cachelib_nvm/allocator/MMLru.hpp"
#include "cachelib_nvm/nvm/EvictionPolicy.hpp"

#include <vector>

namespace facebook {
namespace cachelib {
namespace details {
template <typename T>
struct CACHELIB_PACKED_ATTR RawPointer {
  using SerializedPtrType = uintptr_t;

  RawPointer() = default;
  explicit RawPointer(T* p) : ptr(p) {}

  SerializedPtrType saveState() const noexcept {
    return reinterpret_cast<SerializedPtrType>(ptr);
  }

  T* ptr{};
};

template <typename T>
struct IdentityCompressor {
  RawPointer<T> compress(const T* uncompressed) const noexcept {
    return RawPointer<T>{const_cast<T*>(uncompressed)};
  }

  T* unCompress(RawPointer<T> compressed) const noexcept {
    return compressed.ptr;
  }
};
}

// currently tracks an LRU of regions based on their access pattern
class LruRegions : public EvictionPolicy {
 public:
   explicit LruRegions(uint32_t regions);
  ~LruRegions() = default;

  void recordHit(RegionId id) override;
  void track(RegionId id) override;
  RegionId evict() override;

 private:
  using MMType = MMLru;

  struct Node {
    using CompressedPtr = details::RawPointer<Node>;
    using PtrCompressor = details::IdentityCompressor<Node>;

    enum class Flags : uint8_t {
      // special flags used by MMType
      MM_FLAG_0 = 0,
      MM_FLAG_1 = 1,
      MM_FLAG_2 = 2,
      // our own flag to indicate that node is in MMContainer
      IN_CONTAINER = 7,
    };

    explicit Node(RegionId id) : id_{id} {
      (void)dummy_;
    }

    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;

    Node(Node&&) = default;
    Node& operator=(Node&&) = delete;

    RegionId getId() const noexcept {
      return id_;
    }

    template <Flags flagBit>
    void setFlag() {
      constexpr auto bitmask = 1u << static_cast<uint8_t>(flagBit);
      flags_ |= bitmask;
    }

    template <Flags flagBit>
    void unSetFlag() {
      constexpr auto bitmask = 1u << static_cast<uint8_t>(flagBit);
      flags_ &= ~bitmask;
    }

    template <Flags flagBit>
    bool isFlagSet() const {
      constexpr auto bitmask = 1u << static_cast<uint8_t>(flagBit);
      return (flags_ & bitmask) != 0;
    }

    bool isInMMContainer() const noexcept {
      return isFlagSet<Flags::IN_CONTAINER>();
    }

    void markInMMContainer() {
      setFlag<Flags::IN_CONTAINER>();
    }

    void unmarkInMMContainer() {
      unSetFlag<Flags::IN_CONTAINER>();
    }

    // hook for LRU
    MMType::Hook<Node> lruHook;

   private:
    const RegionId id_;
    uint32_t flags_{};
    uint32_t dummy_{};
  };

  // See note in RegionManager
  static_assert(sizeof(Node) == 32, "performance warning");

  // TODO: Make this one LRU per size
  MMType::Container<Node, &Node::lruHook> lru_;

  std::vector<Node> nodes_;
};
}
}
