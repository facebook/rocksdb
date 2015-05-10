#pragma once

#include <atomic>
#include <set>
#include <limits.h>
#include "util/instrumented_mutex.h"
#include "util/read_write_lock.h"

#define SMALL_ARRAY_SIZE (32) 

#define BOTTOM (ULLONG_MAX)
 
namespace rocksdb {
	
class SequenceNumberMgr
{
private:
	class AdHocSmallSet 
	{
	public:

		AdHocSmallSet() 
		{
			for(int i=0; i < SMALL_ARRAY_SIZE ; i++)
				m_Array[i].store(0);
			m_setIsEmpty.store(true);
		}

		~AdHocSmallSet()
		{
		}

		void Insert(uint64_t item) 
		{
			for(int j=0; j < SMALL_ARRAY_SIZE ; j++)
			{				
				int i = (j + (int)item) % SMALL_ARRAY_SIZE; // TODO: consider using the current cpu/core id

				if(m_Array[i].load() == 0)
				{
					uint64_t c = 0;
					if(m_Array[i].compare_exchange_strong(c,item))					
						return;
				}
			}

			// the array is full (from the point of view of this thread)
			{
				InstrumentedMutexLock l(&m_lock);

				m_Set.insert(item);
				m_setIsEmpty.store(false);
			}
		}


		void Remove(uint64_t item) 
		{
			for(int j=0; j < SMALL_ARRAY_SIZE ; j++)
			{				
				int i = (j + (int)item) % SMALL_ARRAY_SIZE; // TODO: consider using the current cpu/core id

				if(m_Array[i].load() == item)
				{					
					if(m_Array[i].compare_exchange_strong(item,0))					
						return;
				}
			}

			if(!m_setIsEmpty.load()) 
			{
				InstrumentedMutexLock l(&m_lock);
				std::set<uint64_t>::iterator it = m_Set.find(item);
				m_Set.erase (it);
				if(m_Set.empty()) m_setIsEmpty.store(true);
			}
		}

		uint64_t FindMin()
		{
			uint64_t currentMin = BOTTOM ;

			for(int i=0; i < SMALL_ARRAY_SIZE ; i++)
			{		
				uint64_t x = m_Array[i].load();
				if(x != 0 && (x < currentMin))
					currentMin = x;					
			}

			if(!m_setIsEmpty.load()) 
			{
				InstrumentedMutexLock l(&m_lock);
				uint64_t firstItemInSet = *(m_Set.begin());
				if(firstItemInSet < currentMin)
					currentMin = firstItemInSet;

			}

			return currentMin;
		}


		bool IsEmpty()
		{
			for(int i=0; i < SMALL_ARRAY_SIZE ; i++)
			{
				if(m_Array[i] != 0)
					return false;
			}

			if(!m_setIsEmpty.load()) 
			{
				InstrumentedMutexLock l(&m_lock);
				return  m_Set.empty();
			}
			else 
				return true; // empty
		}

	private:

		std::atomic<uint64_t> m_Array[SMALL_ARRAY_SIZE]; 
		std::set<uint64_t> m_Set;
		std::atomic<bool> m_setIsEmpty;

		InstrumentedMutex m_lock ; 
	};

public:
	SequenceNumberMgr() : 
		m_last_sequnce_number(0),
		m_last_stable_sequnce_number(0),
		m_setOfUsedNumbers()
	{
	}

	uint64_t CreateNew(unsigned short toAdd)
	{
		uint64_t v ;
		uint64_t ts;

		if(toAdd==0) toAdd = 1; // TODO: corner case

		while(true)
		{
			v = m_last_sequnce_number.fetch_add(toAdd);
			ts = v + toAdd;
			m_setOfUsedNumbers.Insert(ts);
			if(ts <= m_last_stable_sequnce_number.load())
				m_setOfUsedNumbers.Remove(ts);
			else
				break;
		}
		return ts;
	}

	void Finish(uint64_t seq_num)
	{
		m_setOfUsedNumbers.Remove(seq_num);
	}


	uint64_t LastVersion() const
	{
		uint64_t r = m_last_sequnce_number.load();
		return r;
	}

	uint64_t LastStableVersion() 
	{ 
		uint64_t ts   = m_last_sequnce_number.load();
		uint64_t ts_a = m_setOfUsedNumbers.FindMin();
		if(ts_a != BOTTOM) ts = ts_a - 1;
		while(true)
		{
			uint64_t expected = m_last_stable_sequnce_number.load();
			if(expected >= ts) break;
			if(m_last_stable_sequnce_number.compare_exchange_strong(expected, ts)) break;				
		}

		while(true)
		{
			Backoff backoff;
			uint64_t snapTime = m_last_stable_sequnce_number.load();
			uint64_t currMin = m_setOfUsedNumbers.FindMin();			
			
			if(currMin==BOTTOM || snapTime < currMin) return snapTime;
			backoff.wait() ;			
		}
	}

	void Reset(uint64_t seq_num)
	{
		assert( m_setOfUsedNumbers.IsEmpty() ); 
		m_last_sequnce_number.store(seq_num);
		m_last_stable_sequnce_number.store(seq_num);
	}

private:

	std::atomic<uint64_t>  m_last_sequnce_number;
	std::atomic<uint64_t>  m_last_stable_sequnce_number;

	AdHocSmallSet    m_setOfUsedNumbers ;
  
};

}  // namespace rocksdb



