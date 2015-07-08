// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef PRIORITY_QUEUE_DMCLOCK_H
#define PRIORITY_QUEUE_DMCLOCK_H

//#include "common/Mutex.h"
//#include "common/Formatter.h"

#include <map>
#include <utility>
#include <list>
#include <algorithm>
#include <time.h>
#include <float.h>
#include "utime.h"
#include <queue>
#include <set>

#include "/usr/include/assert.h"
//#include <unordered_map>

/**
 * Manages queue for normal and strict priority items
 *
 * On dequeue, the queue will select the lowest priority queue
 * such that the q has bucket > cost of front queue item.
 *
 * If there is no such queue, we choose the next queue item for
 * the highest priority queue.
 *
 * Before returning a dequeued item, we place into each bucket
 * cost * (priority/total_priority) tokens.
 *
 * enqueue_strict and enqueue_strict_front queue items into queues
 * which are serviced in strict priority order before items queued
 * with enqueue and enqueue_front
 *
 * Within a priority class, we schedule round robin based on the class
 * of type K used to enqueue items.  e.g. you could use entity_inst_t
 * to provide fairness for different clients.
 */

utime_t ceph_clock_now(CephContext* cct) {
	struct timespec tp;
	clock_gettime(CLOCK_REALTIME, &tp);
	utime_t n(tp);
	return n;
}

struct SLO {
	int64_t reserve;
	double_t prop;
	int64_t limit;
};

#define ldlog_p1(cct, sub, lvl)                 \
  (cct->_conf->subsys.should_gather((sub), (lvl)))

template<typename T, typename K>
class PrioritizedQueueDMClock {
	int64_t total_priority;
	int64_t max_tokens_per_subqueue;
	int64_t min_cost;

	typedef std::list<std::pair<double, T> > ListPairs; //will hold deadline time-stamp
	template<class F>
	static unsigned filter_list_pairs(ListPairs *l, F f, std::list<T> *out) {
		unsigned ret = 0;
		if (out) {
			for (typename ListPairs::reverse_iterator i = l->rbegin();
					i != l->rend(); ++i) {
				if (f(i->second)) {
					out->push_front(i->second);
				}
			}
		}
		for (typename ListPairs::iterator i = l->begin(); i != l->end();) {
			if (f(i->second)) {
				l->erase(i++);
				++ret;
			} else {
				++i;
			}
		}
		return ret;
	}

	struct SubQueue {
	private:
		typedef std::map<K, ListPairs> Classes;
		Classes q;
		unsigned tokens, max_tokens;
		int64_t size;
		typename Classes::iterator cur;
	public:
		SubQueue(const SubQueue &other) :
				q(other.q), tokens(other.tokens), max_tokens(other.max_tokens), size(
						other.size), cur(q.begin()) {
		}
		SubQueue() :
				tokens(0), max_tokens(0), size(0), cur(q.begin()) {
		}
		void set_max_tokens(unsigned mt) {
			max_tokens = mt;
		}
		unsigned get_max_tokens() const {
			return max_tokens;
		}
		unsigned num_tokens() const {
			return tokens;
		}
		void put_tokens(unsigned t) {
			tokens += t;
			if (tokens > max_tokens)
				tokens = max_tokens;
		}
		void take_tokens(unsigned t) {
			if (tokens > t)
				tokens -= t;
			else
				tokens = 0;
		}
		void enqueue(K cl, unsigned cost, T item) {
			q[cl].push_back(std::make_pair(cost, item));
			if (cur == q.end())
				cur = q.begin();
			size++;
		}
		void enqueue_front(K cl, unsigned cost, T item) {
			q[cl].push_front(std::make_pair(cost, item));
			if (cur == q.end())
				cur = q.begin();
			size++;
		}
		std::pair<unsigned, T> front() const {
			assert(!(q.empty()));
			assert(cur != q.end());
			return cur->second.front();
		}
		void pop_front() {
			assert(!(q.empty()));
			assert(cur != q.end());
			cur->second.pop_front();
			if (cur->second.empty())
				q.erase(cur++);
			else
				++cur;
			if (cur == q.end())
				cur = q.begin();
			size--;
		}
		unsigned length() const {
			assert(size >= 0);
			return (unsigned) size;
		}
		bool empty() const {
			return q.empty();
		}
		template<class F>
		void remove_by_filter(F f, std::list<T> *out) {
			for (typename Classes::iterator i = q.begin(); i != q.end();) {
				size -= filter_list_pairs(&(i->second), f, out);
				if (i->second.empty()) {
					if (cur == i)
						++cur;
					q.erase(i++);
				} else {
					++i;
				}
			}
			if (cur == q.end())
				cur = q.begin();
		}
		void remove_by_class(K k, std::list<T> *out) {
			typename Classes::iterator i = q.find(k);
			if (i == q.end())
				return;
			size -= i->second.size();
			if (i == cur)
				++cur;
			if (out) {
				for (typename ListPairs::reverse_iterator j =
						i->second.rbegin(); j != i->second.rend(); ++j) {
					out->push_front(j->second);
				}
			}
			q.erase(i);
			if (cur == q.end())
				cur = q.begin();
		}

		/*
		 void dump(Formatter *f) const {
		 f->dump_int("tokens", tokens);
		 f->dump_int("max_tokens", max_tokens);
		 f->dump_int("size", size);
		 f->dump_int("num_keys", q.size());
		 if (!empty())
		 f->dump_int("first_item_cost", front().first);
		 }*/
	};

	struct SubQueueDMClock {
	private:
		typedef std::map<K, std::list<T> > Requests;
		Requests requests;
		unsigned tokens, max_tokens;
		int64_t size;
		int64_t virtual_clock;
		enum tag_types_t {
			Q_NONE = -1, Q_RESERVE = 0, Q_LIMIT, Q_PROP, Q_COUNT
		};

		struct Deadline {
			K cl;
			double_t deadline;
			bool valid;
			Deadline() :
					deadline(0), valid(false) {
			}
			void set_values(K c, double_t d){
				cl = c;
				deadline = d;
				valid = true;
			}
			;
		};
		Deadline min_deadline[Q_COUNT];

		// data structure for dmClock
		struct Tag {
			double_t r_deadline, r_spacing;
			double_t p_deadline, p_spacing;
			double_t l_deadline, l_spacing;
			bool limit_capped, has_more;
			tag_types_t selected_tag;

			Tag() :
					r_deadline(0), r_spacing(0), p_deadline(0), p_spacing(0), l_deadline(
							0), l_spacing(0), limit_capped(false), has_more(
							true), selected_tag(Q_NONE) {
			}
			Tag(utime_t t) :
					r_deadline(t), r_spacing(0), p_deadline(t), p_spacing(0), l_deadline(
							t), l_spacing(0), limit_capped(false), has_more(
							true), selected_tag(Q_NONE) {
			}

			Tag(int64_t t) :
					r_deadline(t), r_spacing(0), p_deadline(t), p_spacing(0), l_deadline(
							t), l_spacing(0), limit_capped(false), has_more(
							true), selected_tag(Q_NONE) {
			}
		};

		typedef std::map<K, Tag> Schedule;
		Schedule schedule;

		void create_new_tag(K cl, SLO slo) {
			Tag tag;
			if (slo.reserve) {
				tag.r_deadline = virtual_clock;
				tag.r_spacing = (double_t) max_tokens / slo.reserve;
				take_tokens(slo.reserve);

				if (!min_deadline[Q_RESERVE].deadline) {
					min_deadline[Q_RESERVE].set_values(cl, tag.r_deadline);
				} else {
					if (min_deadline[Q_RESERVE].deadline > tag.r_deadline)
						min_deadline[Q_RESERVE].set_values(cl, tag.r_deadline);
				}
			}

			if (slo.limit) {
				assert(slo.limit > slo.reserve);
				tag.l_deadline = virtual_clock;
				tag.l_spacing = (double_t) max_tokens / slo.limit;

				if (!min_deadline[Q_LIMIT].deadline) {
					min_deadline[Q_LIMIT].set_values(cl, tag.l_deadline);
				} else {
					if (min_deadline[Q_LIMIT].deadline > tag.l_deadline)
						min_deadline[Q_LIMIT].set_values(cl, tag.l_deadline);
				}
			}

			double_t prop = (double_t) tokens * slo.prop;
			if (slo.prop && prop) {
				tag.p_spacing = (double_t) max_tokens / prop;

				if (min_deadline[Q_PROP].deadline)
					tag.p_deadline = min_deadline[Q_PROP].deadline;
				else
					tag.p_deadline = virtual_clock;

				min_deadline[Q_PROP].set_values(cl, tag.p_deadline);

				if (slo.limit && (prop >= (double_t) slo.limit))
					tag.limit_capped = true;
			}
			schedule[cl] = tag;
		}

		void update_min_deadlines() {
			Tag tag;
			K cl;
			for (typename Schedule::iterator it = schedule.begin();
					it != schedule.end(); ++it) {
				tag = it->second;
				cl = it->first;
				if (!tag.has_more)
					continue;

				if (tag.r_deadline && (tag.r_deadline >= tag.l_deadline)) {
					if (min_deadline[Q_RESERVE].deadline > tag.r_deadline)
						min_deadline[Q_RESERVE].set_values(cl, tag.r_deadline);
				}
				if (tag.p_deadline && (tag.p_deadline >= tag.l_deadline)) {
					if (min_deadline[Q_PROP].deadline > tag.p_deadline)
						min_deadline[Q_PROP].set_values(cl, tag.p_deadline);
				}
			}
		}

		void issue_idle_cycle() {
			cout << "idle cycle issued\n";
			++virtual_clock;
		}

		void find_an_active_client(tag_types_t t) {
			bool flags[Q_COUNT] = { false, false, false };
			bool found = false;
			for (typename Schedule::iterator it = schedule.begin();
					it != schedule.end(); ++it) {
				if (it->second.has_more) {
					K cl = it->first;
					Tag tag = it->second;
					found = true;

					if (tag.r_deadline && !flags[Q_RESERVE]) {
						min_deadline[Q_RESERVE].cl = cl;
						min_deadline[Q_RESERVE].deadline = tag.r_deadline;
						flags[Q_RESERVE] = true;
					}
					if (tag.p_deadline && !flags[Q_PROP]) {
						min_deadline[Q_PROP].cl = cl;
						min_deadline[Q_PROP].deadline = tag.p_deadline;
						flags[Q_PROP] = true;
					}
					if (tag.l_deadline && !flags[Q_LIMIT]) {
						min_deadline[Q_LIMIT].deadline = tag.l_deadline;
						min_deadline[Q_LIMIT].cl = cl;
						flags[Q_LIMIT] = true;
					}
					if (flags[Q_RESERVE] && flags[Q_PROP] && flags[Q_LIMIT])
						break;
				}
			}

			if (found)
				update_min_deadlines();
		}

		void update_tags(K cl) {
			int64_t now = virtual_clock;
			Tag *tag = &schedule[cl];

			if (tag->selected_tag == Q_RESERVE
					|| tag->selected_tag == Q_LIMIT) {
				if (tag->r_deadline) {
					tag->r_deadline = std::max(
							(tag->r_deadline + tag->r_spacing), (double_t) now);
					min_deadline[Q_RESERVE].cl = cl;
					min_deadline[Q_RESERVE].deadline = tag->r_deadline;
				}
			}
			if (tag->selected_tag == Q_RESERVE || tag->selected_tag == Q_PROP) {
				if (tag->p_deadline) {
					tag->p_deadline = std::max(
							(tag->p_deadline + tag->p_spacing), (double_t) now);
					min_deadline[Q_PROP].cl = cl;
					min_deadline[Q_PROP].deadline = tag->p_deadline;

				}
			}
			// always update l-tag
			if (tag->l_deadline) {
				tag->l_deadline = std::max((tag->l_deadline + tag->l_spacing),
						(double_t) now);
//				min_deadline[Q_LIMIT].deadline = tag->l_deadline;
//				min_deadline[Q_LIMIT].cl = cl;
			}
			update_min_deadlines();
		}

	public:
		SubQueueDMClock(const SubQueueDMClock &other) :
				requests(other.requests), tokens(other.tokens), max_tokens(
						other.max_tokens), size(other.size), schedule(
						other.schedule), virtual_clock(other.virtual_clock) {

		}

		SubQueueDMClock() :
				tokens(0), max_tokens(0), size(0), virtual_clock(1) {
		}

		int64_t get_current_clock() {
			return virtual_clock;
		}

		int64_t increment_clock() {
			return ++virtual_clock;
		}

		void set_max_tokens(unsigned mt) {
			max_tokens = mt;
		}

		unsigned get_max_tokens() const {
			return max_tokens;
		}

		unsigned num_tokens() const {
			return tokens;
		}

		void put_tokens(unsigned t) {
			tokens += t;
			if (tokens > max_tokens)
				tokens = max_tokens;
		}

		void take_tokens(unsigned t) {
			if (tokens > t)
				tokens -= t;
			else
				tokens = 0;
		}

		void enqueue(K cl, SLO slo, double cost, T item) {
			bool idle_or_new = (requests.find(cl) == requests.end());
			if (idle_or_new)
				create_new_tag(cl, slo);
			requests[cl].push_back(item);
			size++;
		}

		std::pair<K, Tag> front() {
			assert(!(requests.empty()));
			int64_t t = virtual_clock;
			typename Schedule::iterator it;

			it = schedule.find(min_deadline[Q_RESERVE].cl);
			if (it != schedule.end()) {
				if (it->second.has_more && it->second.r_deadline <= t) {
					it->second.selected_tag = Q_RESERVE;
					return *it;
				}
			}
//
//			it = schedule.find(min_deadline[Q_LIMIT].cl);
//			if (it != schedule.end()) {
//				if (it->second.has_more && it->second.limit_capped
//						&& it->second.l_deadline <= t) {
//					it->second.selected_tag = Q_LIMIT;
//					return *it;
//				}
//			}

			it = schedule.find(min_deadline[Q_PROP].cl);
			if (it != schedule.end()) {
				if (it->second.has_more) {
					it->second.selected_tag = Q_PROP;
					return *it;
				}
			}
			it = schedule.end();
			return std::make_pair(min_deadline[Q_RESERVE].cl, Tag());
		}

		T pop_front() {
			assert(!(requests.empty()));
			std::pair<K, Tag> f = front();
			assert((f.second.selected_tag != Q_NONE));

			//debug
			{
				if (virtual_clock == 20)
					cout << "take a look\n";
				cout << virtual_clock << "\t";
				for (typename Schedule::iterator it = schedule.begin();
						it != schedule.end(); ++it) {
					K cl = it->first;
					Tag tag = it->second;

					if (f.first == cl) {
						if (f.second.selected_tag == Q_RESERVE)
							std::cout << "*";
						if (f.second.selected_tag == Q_PROP)
							std::cout << "~";
						if (f.second.selected_tag == Q_LIMIT)
							std::cout << "_";
					}
					print_tag(tag);
					std::cout << " ||\t ";
				}
				std::cout << std::endl;
			}

			T ret = requests[f.first].front();
			requests[f.first].pop_front();
			virtual_clock++;
			if (requests[f.first].empty()) {
				schedule[f.first].has_more = false;
				find_an_active_client(f.second.selected_tag);
			} else {
				update_tags(f.first);
			}
			size--;

			return ret;
		}

		void print_tag(Tag tag) {
			std::cout << tag.r_deadline << "\t" << tag.p_deadline << "\t"
					<< tag.l_deadline << "\t";
		}
		unsigned length() const {
			assert(size >= 0);
			return (unsigned) size;
		}

		bool empty() const {
			//return requests.empty();
			return (size == 0);
		}

		//		void dump(Formatter *f) const {
		//			f->dump_int("tokens", tokens);
		//			f->dump_int("max_tokens", max_tokens);
		//			f->dump_int("size", size);
		//			f->dump_int("num_keys", q.size());
		//			if (!empty())
		//				f->dump_int("first_item_cost", front().first);
		//		}
	};

	typedef std::map<unsigned, SubQueue> SubQueues;
	SubQueues high_queue;
	SubQueues queue;

	SubQueueDMClock dm_queue;

	SubQueue *create_queue(unsigned priority) {
		typename SubQueues::iterator p = queue.find(priority);
		if (p != queue.end())
			return &p->second;
		total_priority += priority;
		SubQueue *sq = &queue[priority];
		sq->set_max_tokens(max_tokens_per_subqueue);
		return sq;
	}

	void remove_queue(unsigned priority) {
		assert(queue.count(priority));
		queue.erase(priority);
		total_priority -= priority;
		assert(total_priority >= 0);
	}

	void distribute_tokens(unsigned cost) {
		if (total_priority == 0)
			return;
		for (typename SubQueues::iterator i = queue.begin(); i != queue.end();
				++i) {
			i->second.put_tokens(((i->first * cost) / total_priority) + 1);
		}
	}

public:
	PrioritizedQueueDMClock(unsigned max_per, unsigned min_c) :
			total_priority(0), max_tokens_per_subqueue(max_per), min_cost(min_c) {
		dm_queue.set_max_tokens(max_tokens_per_subqueue);
		dm_queue.put_tokens(max_tokens_per_subqueue);
	}

	unsigned length() const {
		unsigned total = 0;
		for (typename SubQueues::const_iterator i = queue.begin();
				i != queue.end(); ++i) {
			assert(i->second.length());
			total += i->second.length();
		}
		for (typename SubQueues::const_iterator i = high_queue.begin();
				i != high_queue.end(); ++i) {
			assert(i->second.length());
			total += i->second.length();
		}
		return total;
	}

	template<class F>
	void remove_by_filter(F f, std::list<T> *removed = 0) {
		for (typename SubQueues::iterator i = queue.begin(); i != queue.end();
				) {
			unsigned priority = i->first;

			i->second.remove_by_filter(f, removed);
			if (i->second.empty()) {
				++i;
				remove_queue(priority);
			} else {
				++i;
			}
		}
		for (typename SubQueues::iterator i = high_queue.begin();
				i != high_queue.end();) {
			i->second.remove_by_filter(f, removed);
			if (i->second.empty()) {
				high_queue.erase(i++);
			} else {
				++i;
			}
		}
	}

	void remove_by_class(K k, std::list<T> *out = 0) {
		for (typename SubQueues::iterator i = queue.begin(); i != queue.end();
				) {
			i->second.remove_by_class(k, out);
			if (i->second.empty()) {
				unsigned priority = i->first;
				++i;
				remove_queue(priority);
			} else {
				++i;
			}
		}
		for (typename SubQueues::iterator i = high_queue.begin();
				i != high_queue.end();) {
			i->second.remove_by_class(k, out);
			if (i->second.empty()) {
				high_queue.erase(i++);
			} else {
				++i;
			}
		}
	}

	void enqueue_strict(K cl, unsigned priority, T item) {
		high_queue[priority].enqueue(cl, 0, item);
	}

	void enqueue_strict_front(K cl, unsigned priority, T item) {
		high_queue[priority].enqueue_front(cl, 0, item);
	}

	void enqueue(K cl, struct SLO slo, unsigned cost, T item) {
		dm_queue.enqueue(cl, slo, cost, item);
	}

	void enqueue2(K cl, unsigned priority, unsigned cost, T item) {
		if (cost < min_cost)
			cost = min_cost;
		if (cost > max_tokens_per_subqueue)
			cost = max_tokens_per_subqueue;
		create_queue(priority)->enqueue(cl, cost, item);
	}

	void enqueue_front(K cl, unsigned priority, unsigned share, T item) { // 1/share internally
		if (share < min_cost)
			share = min_cost;
		if (share > max_tokens_per_subqueue)
			share = max_tokens_per_subqueue;

		create_queue(priority)->enqueue_front(cl, share, item);
	}

	bool empty() const {
		assert(total_priority >= 0);
		assert((total_priority == 0) || !(queue.empty()));
		return queue.empty() && high_queue.empty() && dm_queue.empty();
	}

	T dequeue() {
		assert(!(dm_queue.empty()));
		//dm_queue.increment_clock(); // ceph_clock_now(NULL);
		return dm_queue.pop_front();
	}

	T dequeue2() {
		assert(!empty());

		if (!(high_queue.empty())) {
			T ret = high_queue.rbegin()->second.front().second;
			high_queue.rbegin()->second.pop_front();
			if (high_queue.rbegin()->second.empty())
				high_queue.erase(high_queue.rbegin()->first);
			return ret;
		}

		// if there are multiple buckets/subqueues with sufficient tokens,
		// we behave like a strict priority queue among all subqueues that
		// are eligible to run.
		for (typename SubQueues::iterator i = queue.begin(); i != queue.end();
				++i) {
			assert(!(i->second.empty()));
			if (i->second.front().first < i->second.num_tokens()) {
				T ret = i->second.front().second;
				unsigned cost = i->second.front().first;
				i->second.take_tokens(cost);
				i->second.pop_front();
				if (i->second.empty())
					remove_queue(i->first);
				distribute_tokens(cost);
				return ret;
			}
		}

		// if no subqueues have sufficient tokens, we behave like a strict
		// priority queue.
		T ret = queue.rbegin()->second.front().second;
		unsigned cost = queue.rbegin()->second.front().first;
		queue.rbegin()->second.pop_front();
		if (queue.rbegin()->second.empty())
			remove_queue(queue.rbegin()->first);
		distribute_tokens(cost);
		return ret;
	}

//	void dump(Formatter *f) const {
//		f->dump_int("total_priority", total_priority);
//		f->dump_int("max_tokens_per_subqueue", max_tokens_per_subqueue);
//		f->dump_int("min_cost", min_cost);
//		f->open_array_section("high_queues");
//		for (typename SubQueues::const_iterator p = high_queue.begin();
//				p != high_queue.end(); ++p) {
//			f->open_object_section("subqueue");
//			f->dump_int("priority", p->first);
//			p->second.dump(f);
//			f->close_section();
//		}
//		f->close_section();
//		f->open_array_section("queues");
//		for (typename SubQueues::const_iterator p = queue.begin();
//				p != queue.end(); ++p) {
//			f->open_object_section("subqueue");
//			f->dump_int("priority", p->first);
//			p->second.dump(f);
//			f->close_section();
//		}
//		f->close_section();
//	}

};

#endif
