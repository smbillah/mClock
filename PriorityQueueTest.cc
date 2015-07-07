/*
 * PriorityQueueTest.cc
 *
 *  Created on: Jun 25, 2015
 *      Author: sbillah
 */
#include <iostream>
#include <assert.h>
#include "PrioritizedQueueDMClock.h"
#include <string>
#include "utime.h"
//#include "Clock.h"
#include <iomanip>
#include <queue>


using namespace std;

//utime_t ceph_clock_now(CephContext* cct){
//	struct timespec tp;
//	clock_gettime(CLOCK_REALTIME, &tp);
//	utime_t n(tp);
//	return n;
//}
struct asd {
	int a;
	int b;
	asd() :
			a(0), b(0) {
	}
	asd(int _a, int _b) :
			a(_a), b(_b) {
	}
};

struct compare {
	bool operator()(const asd &left, const asd &right) {
		return left.a < right.a;
	}
};
//typedef pair<int, string> Deadline;
//struct compare_deadline {
//	bool operator()(const Deadline &left, const Deadline &right) {
//		return left.first < right.first;
//	}
//};

//utime_t ceph_clock_now(CephContext* cct) {
//	struct timespec tp;
//	clock_gettime(CLOCK_REALTIME, &tp);
//	utime_t n(tp);
//	return n;
//}

int main(int argc, char* argv[]) {

//	CephContext* cct = NULL;
//	utime_t now = ceph_clock_now(cct);
//	double_t space = 10.5f;
//	now.set_from_double(space + now);

//	typedef multiset<asd, compare> MySet;
//	MySet myset, myset2;
//	asd x1(130, 10), x2(20, 20), x3(30, 30), x4(50,50);
//	myset.insert(x1);
//	myset.insert(x2);
//	myset.insert(x3);
//
//	x1.b += 1;
//	x2.b += 1;
//	x3.b += 1;
//
//	myset.insert(x1);
//	myset.insert(x2);
//	myset.insert(x3);
//
//	myset2.insert(x1);
//	myset2.insert(x2);
//	myset2.insert(x3);
//
//	MySet::iterator it;
//	std::pair<MySet::iterator, MySet::iterator> rr = myset.equal_range(x4);
//	for(it =  rr.first; it !=rr.second ; ++it){
//		cout<< it->a << endl;
//	}
//	return 1;

//	it = myset.begin();
//	asd r = *it;
//	it = myset2.find(r);
//	cout << (it == myset2.end()) << " " << it->a << " " << myset.begin()->a
//			<< endl;


	unsigned int throughput = 1200;
	unsigned int min_c = 10;
	PrioritizedQueueDMClock<string, unsigned> dmClock(throughput, min_c);

	SLO slo1, slo2, slo3;
	slo1.reserve = 250;
	slo2.reserve = 250;
	slo3.reserve = 0;

	slo1.prop = 1.0 / 6;
	slo2.prop = 2.0 / 6;
	slo3.prop = 3.0 / 6;

	slo1.limit = 350;
	slo2.limit = 350;
	slo3.limit = 1000;
//// for dmclocl
	dmClock.enqueue(0, slo1, 0, "client0");
	dmClock.enqueue(1, slo2, 0, "client1");
	dmClock.enqueue(2, slo3, 0, "client2");
	for (int i = 0; i < 3; i++) {
		for (int j = 0; j < 20000; j++) {
			if (i == 0)
				dmClock.enqueue(0, slo1, 0, "client0");
			if (i == 1)
				dmClock.enqueue(1, slo2, 0, "client1");
			if (i == 2)
				dmClock.enqueue(2, slo3, 0, "client2");
		}
	}
//
////for default implementation
//	dmClock.enqueue2(0, 1, min_c, "client0");
//	dmClock.enqueue2(1, 2, min_c, "client1");
//	dmClock.enqueue2(2, 3, min_c, "client2");
//	for (int i = 0; i < 3; i++) {
//		for (int j = 0; j < 20000; j++) {
//			if (i == 0)
//				dmClock.enqueue2(0, 1, min_c, "client0");
//			if (i == 1)
//				dmClock.enqueue2(1, 2, min_c, "client1");
//			if (i == 2)
//				dmClock.enqueue2(2, 3, min_c, "client2");
//		}
//	}


	unsigned time = 0 ;
	int count[3] = {0,0,0};
	while (!dmClock.empty()) {
		if(time == throughput) break;
		string msg = dmClock.dequeue();
//		string msg = dmClock.dequeue2();
		if (msg == "client0")
			count[0] += 1;
		if (msg == "client1")
			count[1] += 1;
		if (msg == "client2")
			count[2] += 1;
		cout <<"clock "<<time<< ":: #0: "<< count[0] << ", #1: "<< count[1]<< ", #2: "<< count[2] << ", msg: "<< msg << endl;
		time++;
		//break;
	}
	cout << "successfully terminated\n";


	return 0;
}

