#pragma once
#include <atomic>
#include "rocksdb/env.h"
  
// Simple Read-Write lock
// In order to avoid starvation of writers,  this lock prefer writers over readers
// Inspired by the read-write lock algorithm of libcds (See http://libcds.sourceforge.net/ by Maxim Khiszinsky)

namespace rocksdb {

	class Backoff 
	{
	private:
		static const size_t  cStart = (1 << 4);
		static const size_t  cEnd = (1 << 14);

		size_t  current_;
	
	public:
		Backoff()
		{
			reset();
		}

		void wait()
		{
			if (current_ <= cEnd) {
				for (size_t i = 0; i < current_; i++); // nop
				current_ *= 2;
			}
			else
			{
				Env::Default()->SleepForMicroseconds(1000);
				reset();
			}

		}
		void reset()
		{
			current_ = cStart;
		}
	};


	class ReadWriteLock
	{
	private:
		union Data {
			struct {
				unsigned int numberOfReadersThatOwnTheLock : 32 / 2;
				unsigned int numberOfWriters			   : 32 / 2 - 1;
				unsigned int lockedInWriteMode			   : 1;
			};
			uint32_t all; 
		};

		volatile std::atomic<uint32_t> state_;

	public:
		ReadWriteLock()
		{
			state_.store(0);
		}
		~ReadWriteLock()
		{
			assert(state_.load() == 0);
		}

		void LockRead()
		{
			Data expectedData;
			Data newData;
			Backoff backoff;
			while (true) 
			{
				expectedData.all = state_.load();
				newData.all = expectedData.all;
				expectedData.lockedInWriteMode = 0;
				expectedData.numberOfWriters = 0;
				newData.numberOfReadersThatOwnTheLock++;
				newData.lockedInWriteMode = 0;
				newData.numberOfWriters = 0;
				if (state_.compare_exchange_strong(expectedData.all, newData.all))
					break;
				backoff.wait();
			}
		}


		void UnlockRead()
		{
			Data expectedData;
			Data newData;
			Backoff backoff;
			while (true) 
			{
				expectedData.all = state_.load();
				newData.all = expectedData.all;
				newData.numberOfReadersThatOwnTheLock--;
				assert(expectedData.lockedInWriteMode == 0);
				if (state_.compare_exchange_strong(expectedData.all, newData.all))
					break;
				backoff.wait();
			}
		}


		void LockWrite()
		{
			Data expectedData;
			Data newData;
			Backoff backoff;

			while (true) {
				expectedData.all = state_.load();
				newData.all = expectedData.all;
				newData.numberOfWriters++;
				if (state_.compare_exchange_strong(expectedData.all, newData.all))
					break;
				backoff.wait();
			}

			backoff.reset();


			while (true) {
				expectedData.all = state_.load();
				newData.all = expectedData.all;
				expectedData.numberOfReadersThatOwnTheLock = 0;
				expectedData.lockedInWriteMode = 0;
				newData.numberOfReadersThatOwnTheLock = 0;
				newData.lockedInWriteMode = 1;
				if (state_.compare_exchange_strong(expectedData.all, newData.all))
					break;
				backoff.wait();
			}
		}


		void UnlockWrite()
		{
			Data expectedData;
			Data newData;
			Backoff backoff;

			while (true) {
				expectedData.all = state_.load();
				newData.all = expectedData.all;
				assert(expectedData.lockedInWriteMode == 1);
				assert(expectedData.numberOfReadersThatOwnTheLock == 0);
				newData.lockedInWriteMode = 0;
				newData.numberOfWriters--;
				if (state_.compare_exchange_strong(expectedData.all, newData.all))
					break;
				backoff.wait();
			}
		}

		bool isWriting() const
		{
			Data data;
			data.all = state_.load();
			return data.lockedInWriteMode == 1;
		}

		bool isReading() const
		{
			Data data;
			data.all = state_.load();
			return data.numberOfReadersThatOwnTheLock != 0;
		}
	};
};
