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


	unsigned int throughput = 350;
	unsigned int min_c = 10;
	PrioritizedQueueDMClock<string, unsigned> dmClock(throughput, min_c);

	SLO slo1, slo2, slo3;
	slo1.reserve = 250;
	slo2.reserve = 250;
	slo3.reserve = 0;

	slo1.prop = 1.0 / 6;
	slo2.prop = 2.0 / 6;
	slo3.prop = 3.0 / 6;

	slo1.limit = 0;
	slo2.limit = 0;
	slo3.limit = 1000;

	//// for dmclock
	dmClock.enqueue_mClock(0, slo1, 0, "client0");
	dmClock.enqueue_mClock(1, slo2, 0, "client1");
	dmClock.enqueue_mClock(2, slo3, 0, "client2");
	for (int i = 0; i < 3; i++) {
		for (int j = 0; j < 20000; j++) {
			if (i == 0)
				dmClock.enqueue_mClock(0, slo1, 0, "client0");
			if (i == 1)
				dmClock.enqueue_mClock(1, slo2, 0, "client1");
			if (i == 2)
				dmClock.enqueue_mClock(2, slo3, 0, "client2");
		}
	}

	unsigned time = 0 ;
	int count[3] = {0,0,0};
	while (!dmClock.empty()) {
		if(time == throughput) break;
		string msg = dmClock.dequeue_mClock();
		if (msg == "client0")
			count[0] += 1;
		if (msg == "client1")
			count[1] += 1;
		if (msg == "client2")
			count[2] += 1;
		//cout <<"clock "<<time<< ":: #0: "<< count[0] << ", #1: "<< count[1]<< ", #2: "<< count[2] << ", msg: "<< msg << endl;
		time++;
		//break;
	}
	cout << "total usage:: #0: "<< count[0] << ", #1: "<< count[1]<< ", #2: "<< count[2] << endl;
	cout << "successfully terminated\n";


	return 0;
}

