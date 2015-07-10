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

#include "/usr/include/assert.h"

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
		unsigned throughput_available, throughput_prop, throughput_system;
		int64_t size;
		int64_t virtual_clock;

		// data structure for dmClock
		enum tag_types_t {
			Q_NONE = -1, Q_RESERVE = 0, Q_PROP, Q_LIMIT, Q_COUNT
		};

		struct Tag {
			double_t r_deadline, r_spacing;
			double_t p_deadline, p_spacing;
			double_t l_deadline, l_spacing;
			bool active;
			tag_types_t selected_tag;
			K cl;
			SLO slo;
			double_t stat;

			Tag(K _cl, SLO _slo) :
					r_deadline(0), r_spacing(0), p_deadline(0), p_spacing(0), l_deadline(
							0), l_spacing(0), active(true), selected_tag(
							Q_NONE), cl(_cl), slo(_slo), stat(0) {
			}
			Tag(utime_t t) :
					r_deadline(t), r_spacing(0), p_deadline(t), p_spacing(0), l_deadline(
							t), l_spacing(0), active(true), selected_tag(
							Q_NONE), stat(0) {
			}

			Tag(int64_t t) :
					r_deadline(t), r_spacing(0), p_deadline(t), p_spacing(0), l_deadline(
							t), l_spacing(0), active(true), selected_tag(
							Q_NONE), stat(0) {
			}

		};
		typedef std::vector<Tag> Schedule;
		Schedule schedule;

		struct Deadline {
			unsigned cl_index;
			double_t deadline;
			bool valid;
			Deadline() :
					cl_index(0), deadline(0), valid(false) {
			}
			void set_values(unsigned ci, double_t d, bool v = true) {
				cl_index = ci;
				deadline = d;
				valid = v;
			}
		};
		Deadline min_tag_r, min_tag_p;

		void create_new_tag(K cl, SLO slo) {
			Tag tag(cl, slo);
			if (slo.reserve) {
				tag.r_deadline = get_current_clock();
				tag.r_spacing = (double_t) get_system_throughput()
						/ slo.reserve;
				reserve_throughput(slo.reserve);
			}
			if (slo.limit) {
				assert(slo.limit > slo.reserve);
				tag.l_deadline = get_current_clock();
				tag.l_spacing = (double_t) get_system_throughput() / slo.limit;
			}

			if (slo.prop) {
				reserve_prop_throughput(slo.prop);
				double_t prop = calculate_prop_throughput(slo.prop);
				assert(prop > 0);
				tag.p_spacing = (double_t) get_system_throughput() / prop;
				tag.p_deadline =
						min_tag_p.deadline ?
								min_tag_p.deadline : get_current_clock();

				recalculate_prop_throughput();
			}
			schedule.push_back(tag);
			update_min_deadlines();
		}

		void update_tags(unsigned cl_index, bool was_idle = false) {
			int64_t now = get_current_clock();
			Tag *tag = &schedule[cl_index];

			if (tag->selected_tag == Q_RESERVE || tag->selected_tag == Q_NONE) {
				if (tag->r_deadline) {
					if (was_idle) {
						tag->r_deadline = std::max(
								(tag->r_deadline + tag->r_spacing),
								(double_t) now);
					} else {
						tag->r_deadline = tag->r_deadline + tag->r_spacing;
					}
				}
			}
			if (tag->p_deadline) {
				if (was_idle) {
					tag->p_deadline =
							min_tag_p.deadline ? min_tag_p.deadline : now;
				} else {
					tag->p_deadline = tag->p_deadline + tag->p_spacing;
				}
			}
			if (tag->l_deadline) {
				if (was_idle) {
					tag->l_deadline = std::max(
							(tag->l_deadline + tag->l_spacing), (double_t) now);
				} else {
					tag->l_deadline = tag->l_deadline + tag->l_spacing;
				}

			}
			update_min_deadlines();
		}

		void update_min_deadlines() {
			min_tag_r.valid = min_tag_p.valid = false;
			unsigned index = 0;
			for (typename Schedule::iterator it = schedule.begin();
					it != schedule.end(); ++it, index++) {
				Tag tag = *it;
				if (!tag.active)
					continue;

				if (tag.r_deadline
						&& ((tag.r_deadline >= tag.l_deadline)
								|| (tag.l_deadline <= get_current_clock()))) {
					if (min_tag_r.valid) {
						if (min_tag_r.deadline >= tag.r_deadline)
							min_tag_r.set_values(index, tag.r_deadline);
					} else {
						min_tag_r.set_values(index, tag.r_deadline);
					}
				}

				if (tag.p_deadline && (tag.l_deadline <= get_current_clock())) {
					if (min_tag_p.valid) {
						if (min_tag_p.deadline >= tag.p_deadline)
							min_tag_p.set_values(index, tag.p_deadline);
					} else {
						min_tag_p.set_values(index, tag.p_deadline);
					}
				}
			}
		}

		void issue_idle_cycle() {
			//#ifdef DEBUG
			cout << get_current_clock() << "____idle_____" << "\t" << "\n";
			print_current_tag(Q_NONE);
			//#endif
			increment_clock();
			update_min_deadlines();
		}

		double_t calculate_prop_throughput(double_t prop) const {
			if (throughput_prop && prop) {
				if (prop <= throughput_prop)
					return throughput_available * (prop / throughput_prop);
				else
					return throughput_available;
			}
			return 0;
		}

		void recalculate_prop_throughput() {
			double_t prop;
			for (typename Schedule::iterator it = schedule.begin();
					it != schedule.end(); ++it) {
				if (it->slo.prop) {
					prop = calculate_prop_throughput(it->slo.prop);
					assert(prop > 0);
					it->p_spacing = (double_t) get_system_throughput() / prop;
				}
			}
		}

		//helper function
		void print_iops() {
			std::cout << "throughput at: " <<virtual_clock << ":\n";
			for (unsigned i = 0; i < schedule.size(); i++)
				std::cout << "client " << i << " IOPS :" << schedule[i].stat
						<< std::endl;
		}

		void print_current_tag(tag_types_t tt, int index = -1) {
			cout << get_current_clock() << "\t";
			for (typename Schedule::iterator it = schedule.begin();
					it != schedule.end(); ++it) {
				Tag _tag = *it;
				if (index == (it - schedule.begin())) {
					if (tt == Q_RESERVE)
						std::cout << "*";
					if (tt == Q_PROP)
						std::cout << "~";
					if (tt == Q_LIMIT)
						std::cout << "_";
				}
				std::cout << _tag.r_deadline << "\t " << _tag.p_deadline
						<< " \t " << _tag.l_deadline << " \t || ";
			}
			std::cout << std::endl;
		}

	public:
		SubQueueDMClock(const SubQueueDMClock &other) :
				requests(other.requests), throughput_available(
						other.throughput_available), throughput_prop(
						other.throughput_prop), throughput_system(
						other.throughput_system), size(other.size), schedule(
						other.schedule), virtual_clock(other.virtual_clock) {
		}

		SubQueueDMClock() :
				throughput_available(0), throughput_prop(0), throughput_system(
						0), size(0), virtual_clock(1) {
		}

		int64_t get_current_clock() {
			return virtual_clock;
		}

		int64_t increment_clock() {
			if (virtual_clock == throughput_system) {
				for (unsigned i = 0; i < schedule.size(); i++)
					std::cout << "client " << i << " IOPS :" << schedule[i].stat
							<< std::endl;
			}
			return ++virtual_clock;
		}

		void set_system_throughput(unsigned mt) {
			throughput_system = mt;
		}

		unsigned get_system_throughput() const {
			return throughput_system;
		}

		unsigned get_available_throughput() const {
			return throughput_available;
		}

		void release_throughput(unsigned t) {
			throughput_available += t;
			if (throughput_available > throughput_system)
				throughput_available = throughput_system;
		}

		void reserve_throughput(unsigned t) {
			if (throughput_available > t)
				throughput_available -= t;
			else
				throughput_available = 0;
		}

		void release_prop_throughput(unsigned t) {
			throughput_prop -= t;
			if (throughput_prop < 0)
				throughput_prop = 0;
		}

		void reserve_prop_throughput(unsigned t) {
			throughput_prop += t;
		}

		void purge_idle_clients() {
			bool update_required = false;
			typename Schedule::iterator it = schedule.begin();
			for (; it != schedule.end();) {
				if (!it->active) {
					update_required = true;
					if (it->slo.reserve)
						release_throughput(it->slo.reserve);
					if (it->slo.prop)
						release_prop_throughput(it->slo.prop);

					print_iops(); //testing

					requests.erase(it->cl);
					it = schedule.erase(it);
				} else {
					++it;
				}
			}
			if (update_required)
				recalculate_prop_throughput();
		}

		Tag* front(unsigned &out) {
			assert((size != 0));
			int64_t t = get_current_clock();

			if (min_tag_r.valid) {
				Tag *tag = &schedule[min_tag_r.cl_index];
				if (tag->r_deadline <= t) {
					tag->selected_tag = Q_RESERVE;
					out = min_tag_r.cl_index;
					return tag;
				}
			}
			if (min_tag_p.valid) {
				Tag *tag = &schedule[min_tag_p.cl_index];
				if (tag->p_deadline) {
					tag->selected_tag = Q_PROP;
					out = min_tag_p.cl_index;
					return tag;
				}
			}
			out = -1;
			return NULL;
		}

		T pop_front() {
			assert((size != 0));
			unsigned cl_index;
			Tag *tag = front(cl_index);

			// issue idle cycle
			while (size && tag == NULL) {
				issue_idle_cycle();
				tag = front(cl_index);
			}

			//#ifdef DEBUG
			print_current_tag(tag->selected_tag, cl_index);
			tag->stat++;
			//#endif

			T ret = requests[tag->cl].front();
			requests[tag->cl].pop_front();
			if (requests[tag->cl].empty())
				schedule[tag->cl].active = false;

			increment_clock();
			update_tags(cl_index);
			size--;
			return ret;
		}

		void enqueue(K cl, SLO slo, double cost, T item) {
			bool new_cl = (requests.find(cl) == requests.end());
			if (new_cl) {
				create_new_tag(cl, slo);
			} else {
				if (requests[cl].empty()) {
					unsigned index = -1;
					for (typename Schedule::iterator it = schedule.begin();
							it != schedule.end(); ++it) {
						if (it->cl == cl) {
							index = it - schedule.begin();
							break;
						}
					}
					assert((index >= 0 && index < schedule.size()));
					update_tags(index, true);
				}
			}
			requests[cl].push_back(item);
			size++;
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
		dm_queue.set_system_throughput(max_tokens_per_subqueue);
		dm_queue.release_throughput(max_tokens_per_subqueue);
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
		total += dm_queue.length();
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

	void enqueue(K cl, unsigned priority, unsigned cost, T item) {
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

	T dequeue_mClock() {
		assert(!(dm_queue.empty()));
		// ceph_clock_now(NULL);
		return dm_queue.pop_front();
	}

	void enqueue_mClock(K cl, struct SLO slo, unsigned cost, T item) {
		dm_queue.enqueue(cl, slo, cost, item);
	}

	void purge_mClock(){
		dm_queue.purge_idle_clients();
	}

	T dequeue() {
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
