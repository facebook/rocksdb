#ifndef RAPIDJSON_INTERNAL_STACK_H_
#define RAPIDJSON_INTERNAL_STACK_H_

namespace rapidjson {
namespace internal {

///////////////////////////////////////////////////////////////////////////////
// Stack

//! A type-unsafe stack for storing different types of data.
/*! \tparam Allocator Allocator for allocating stack memory.
*/
template <typename Allocator>
class Stack {
public:
	Stack(Allocator* allocator, size_t stack_capacity) : allocator_(allocator), own_allocator_(0), stack_(0), stack_top_(0), stack_end_(0), stack_capacity_(stack_capacity) {
		RAPIDJSON_ASSERT(stack_capacity_ > 0);
		if (!allocator_)
			own_allocator_ = allocator_ = new Allocator();
		stack_top_ = stack_ = (char*)allocator_->Malloc(stack_capacity_);
		stack_end_ = stack_ + stack_capacity_;
	}

	~Stack() {
		Allocator::Free(stack_);
		delete own_allocator_; // Only delete if it is owned by the stack
	}

	void Clear() { /*stack_top_ = 0;*/ stack_top_ = stack_; }

	template<typename T>
	T* Push(size_t count = 1) {
		 // Expand the stack if needed
		if (stack_top_ + sizeof(T) * count >= stack_end_) {
			size_t new_capacity = stack_capacity_ * 2;
			size_t size = GetSize();
			size_t new_size = GetSize() + sizeof(T) * count;
			if (new_capacity < new_size)
				new_capacity = new_size;
			stack_ = (char*)allocator_->Realloc(stack_, stack_capacity_, new_capacity);
			stack_capacity_ = new_capacity;
			stack_top_ = stack_ + size;
			stack_end_ = stack_ + stack_capacity_;
		}
		T* ret = (T*)stack_top_;
		stack_top_ += sizeof(T) * count;
		return ret;
	}

	template<typename T>
	T* Pop(size_t count) {
		RAPIDJSON_ASSERT(GetSize() >= count * sizeof(T));
		stack_top_ -= count * sizeof(T);
		return (T*)stack_top_;
	}

	template<typename T>
	T* Top() { 
		RAPIDJSON_ASSERT(GetSize() >= sizeof(T));
		return (T*)(stack_top_ - sizeof(T));
	}

	template<typename T>
	T* Bottom() { return (T*)stack_; }

	Allocator& GetAllocator() { return *allocator_; }
	size_t GetSize() const { return stack_top_ - stack_; }
	size_t GetCapacity() const { return stack_capacity_; }

private:
	Allocator* allocator_;
	Allocator* own_allocator_;
	char *stack_;
	char *stack_top_;
	char *stack_end_;
	size_t stack_capacity_;
};

} // namespace internal
} // namespace rapidjson

#endif // RAPIDJSON_STACK_H_
