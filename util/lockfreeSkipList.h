//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
  
#include <assert.h>
#include <atomic>
#include <stdlib.h>
#include "port/port.h"
#include "util/allocator.h"
#include "util/random.h"
#include "rocksdb/env.h"

// Lock-free cncurrent skip-list. 
// This skip-list is a variant of the algorithm in Chapter 14.4 of the book: The Art of Multiprocessor Programming, 2008, Herlihy and Shavit.
// It does not support delete operations, and includes iterators that guarantee  weak-consistency.

namespace rocksdb {

	template<typename TItem, typename UserComparator>
	class LockFreeSkipList {

	private:

		const static int MAX_LEVEL = 32;

		// Internal skip-list node
		struct Node 
		{
			Node(TItem x, int height) {
				value = x; 
				topLevel = height;
				for (int i = 0; i < height + 1; i++)  
				{
					next[i] = nullptr;
				}
			}
			TItem value;
			int topLevel;			
			std::atomic<Node*> next[1]; // the actual size of this array is height+1 (allocated externally)
		};

		// Random level generator 
		// Inspired by the random level generator from Doug Lea's ConcurrentSkipListMap.java 
		// (based on a pseudo-random function that was used in turbo pascal)
		class RandomLevelGenerator
		{
		public:
			RandomLevelGenerator(){
				currentState_.store((unsigned int)Env::Default()->NowMicros(), std::memory_order_relaxed);
			}
			int nextRandomLevel() {
				unsigned int num = currentState_.load(std::memory_order_relaxed) * 134775813 + 1;
				currentState_.store(num, std::memory_order_relaxed);

				num = num | 1;
				unsigned int one = 1;
				for (int i = 0; i <= 31; i++)
				{
					if ((num & (one << (31 - i))) != 0)
					{
						return i % MAX_LEVEL;
					}
				}
				assert(0);
				return 0;
			}

		private:
			std::atomic<unsigned int>    currentState_;
		};

		friend class Iterator;
		RandomLevelGenerator rand;
		Allocator* allocator_;
		UserComparator& cmp_;
		Node* pHead_;
		Node* pTail_;

		bool find(TItem x, Node** preds, Node** succs, bool stopIfFound)
		{
			Node* pred ;
			Node* curr ;
			Node* succ ;
			bool stop  ;
			bool found ;
			int maxLevel = MAX_LEVEL;
		start:
			pred = nullptr;
			curr = nullptr;
			succ = nullptr;
			stop = false;
			found = false;

			pred = pHead_;
			for (int level = maxLevel; level >= 0 && !stop; level--) {
				curr = pred->next[level].load(std::memory_order_relaxed);
				while (true) {
					succ = curr->next[level].load(std::memory_order_relaxed);

					// read validation
					if (curr != pred->next[level].load(std::memory_order_acquire) ||
						succ != curr->next[level].load(std::memory_order_acquire))
						goto start; // restart

					if (curr == pTail_) break;
					int icmp = cmp_(curr->value, x);
					if (icmp<0){
						pred = curr;
						curr = succ;
					}
					else {
						if (icmp == 0) found = true;
						if (stopIfFound && icmp == 0) stop = true;
						break;
					}
				}
				preds[level] = pred;
				succs[level] = curr;
			}
			return found;
		}

		Node* find_minimal_larger_equal(TItem x) const
		{
			const Node* pPred = nullptr;
			Node* pCurr = nullptr;

			int nCmp = 1;

			pPred = pHead_;

			for (int level = (int)MAX_LEVEL; level >= 0; level--)
			{
				pCurr = pPred->next[level].load(std::memory_order_acquire);

				if (pCurr == pTail_) continue;

				nCmp = cmp_(pCurr->value, x);

				while (nCmp < 0)
				{
					pPred = pCurr;
					pCurr = pPred->next[level].load(std::memory_order_acquire);

					if (pCurr == pTail_) break;

					nCmp = cmp_(pCurr->value, x);
				}
			}

			return pCurr; // note that it may return the tail
		}

		Node* find_node_before_tail()
		{
			Node* pPred = nullptr;
			Node* pCurr = nullptr;

			pPred = pHead_;

			for (int level = (int)MAX_LEVEL; level >= 0; level--)
			{				
				while (true)
				{
					pCurr = pPred->next[level].load(std::memory_order_acquire);

					if (pCurr == pTail_) break;

					pPred = pCurr;
				}
			}

			return pPred;
		}
	


		Node* find_maximal_smaller(TItem x)
		{
			Node* preds[MAX_LEVEL + 1];
			Node* succs[MAX_LEVEL + 1];

			find(x, preds, succs, false);

			return preds[0]; // note that it may return the head
		}

	public:		

		LockFreeSkipList(UserComparator& userComparator, Allocator* allocator)
			: allocator_(allocator), cmp_(userComparator)
			 
		{
			char* mem = allocator_->AllocateAligned(sizeof(Node)+MAX_LEVEL * sizeof(std::atomic<Node*>));
			pTail_ = new (mem)Node((TItem)nullptr, MAX_LEVEL);

			mem = allocator_->AllocateAligned(sizeof(Node)+MAX_LEVEL * sizeof(std::atomic<Node*>));
			pHead_ = new (mem)Node((TItem)nullptr, MAX_LEVEL);

			for (int i = 0; i < MAX_LEVEL + 1; i++) {
				pHead_->next[i].store(pTail_);
			}
		}

		bool contains(const TItem x) const
		{
			Node* n = find_minimal_larger_equal(x);
			if (n == pTail_) return false;
			bool r = (cmp_(n->value, x) == 0);
			return r;
		}

		bool insert(TItem x) {
			
			int topLevel = rand.nextRandomLevel() + 1;
			Node* preds[MAX_LEVEL + 1];
			Node* succs[MAX_LEVEL + 1];

			while (true) {
				bool found = find(x, preds, succs, true);
				if (found)
				{
					return false;
				}
				else
				{
					char* mem = allocator_->AllocateAligned(sizeof(Node)+ topLevel * sizeof(std::atomic<Node*>));
					Node* newNode = new (mem)Node(x, topLevel);

					
					for (int level = 0; level <= topLevel; level++)
					{
						Node* succ = succs[level];
						newNode->next[level].store(succ, std::memory_order_relaxed);
					}
					Node* pred = preds[0];
					Node* succ = succs[0];
					newNode->next[0].store(succ, std::memory_order_relaxed);
					if (!pred->next[0].compare_exchange_strong(succ, newNode, std::memory_order_release, std::memory_order_relaxed))
					{
						continue;  // restart
					}
					for (int level = 1; level <= topLevel; level++)
					{
						while (true) {
							pred = preds[level];
							succ = succs[level];
							if (pred->next[level].compare_exchange_strong(succ, newNode, std::memory_order_release, std::memory_order_relaxed)) break;
							find(x, preds, succs, false);
						}
					}
					return true;
				}
			}
		}
		 
		class Iterator
		{
		private:
			LockFreeSkipList* pList_;
			Node* current_;
		public:
			Iterator(LockFreeSkipList* l) : pList_(l), current_(l->pHead_->next[0])
			{
			}

			bool Valid() const
			{
				return (current_ != pList_->pTail_) && (current_ != pList_->pHead_);
			}

			TItem& item() const
			{
				assert(Valid());
				TItem& i = current_->value;
				return i;
			}

			void Next()
			{
				assert(Valid());
				current_ = current_->next[0];
			}

			void Prev()
			{
				assert(Valid());
				TItem& theItem = item();
				Node* n = pList_->find_maximal_smaller(theItem);
				current_ = n;
			}

			void Seek(const TItem& target)
			{
				Node* n = pList_->find_minimal_larger_equal(target);
				current_ = n;
			}

			void SeekToFirst()
			{
				current_ = pList_->pHead_->next[0];
			}


			void SeekToLast()
			{
				current_ = pList_->find_node_before_tail();

				/*
				// a naive alternative implementation
				Node* prev = nullptr;
				Node* curr = pList_->pHead_->next[0];
				while (curr != pList_->pTail_)
				{
					prev = curr;
					curr = curr->next[0];
				}
				assert(prev != nullptr);
				current_ = prev;
				*/
			}
		};

	};

}






