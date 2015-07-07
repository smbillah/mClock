/*
 * Iterator_demo.cpp
 *
 *  Created on: Jun 22, 2015
 *      Author: sbillah
 */

// list::rbegin/rend
#include <iostream>
#include <list>
#include <map>
#include <cstdlib>

int demoIterator() {

	int j=0;
	std::map<int, float> mymap;


	std::list<int> mylist;
	if (mylist.begin() == mylist.end())
		std::cout << "empty\n";

	mylist.push_back(10);
	if (mylist.begin() != mylist.end())
		std::cout << "not equal any more\n";

	std::list<int>::iterator it = mylist.begin();
	if (mylist.begin() != mylist.end())
		std::cout << *it;

	for (int i= 1; i <= 5; ++i){
		mylist.push_back(i);
		j = rand()%10;
		mymap[j] = i;
		std::cout << j << " ";
	}

	std::cout << "mylist backwards:";
	for (std::list<int>::reverse_iterator rit = mylist.rbegin();
			rit != mylist.rend(); ++rit)
		std::cout << ' ' << *rit;

	std::cout << "\n map \n";
	for (std::map<int, float>::reverse_iterator rit = mymap.rbegin();	rit != mymap.rend(); ++rit)
			std::cout << ' ' << rit->first<< ":" << rit->second;
	std::cout << '\n';

	return 0;
}

