//#define DEBUG 1
#define TIMING 1
#include<time.h>
#include<sys/time.h>
#include "skeleton.hpp"



long R;
long M;


#define WORK 100
#define SIZE 1000000

class MyMap : public Map<long,long,long> {
		void map( long x) {
			long i;
			for(i=0; i<WORK; i++) 
				emit(&i,&x);
		}
};


class MyReduce : public Reduce<long,long> {


		long reduce(long a, long b) {
			return a+b;
		}
		
		
};





int main(int argc, char* argv[]) {

	if (argc < 3) {
		std::cerr << "usage: " << argv[0] << " #mapper #input\n";
		return -1;
	}
	
	
	M=atoi(argv[1]);
	R=atoi(argv[2]);

	long a[SIZE];
	int i;
	for (i=0; i<SIZE; i++)
		a[i]=i;
		
	/*MapReduce*/
	std::cout << "MAPREDUCE\n";
	
	

	
	std::vector<Map<long,long,long>*> m;
	std::vector<Reduce<long,long>*> r;
	for(i=0; i<M; i++) {
		m.push_back(new MyMap);
	}
	
	for(i=0; i<R; i++) {
		r.push_back(new MyReduce);
	}
	MapReduce<long,long,long> mr(&m,&r);
	mr.setInput( a , SIZE );
	long outsize;
	std::pair<long,long>*out=mr.run_and_wait(&outsize);
	
	
	
	std::cout << "END MAPREDUCE\n";
	for(i=0; i<R; i++) {
		delete r.at(i);
	}
	
	for(i=0; i<M; i++) {
		delete m.at(i);
	}
	
	/*std::cout <<"\n\n\n";
	for (i=0; i<outsize; i++) {
		
		std::cout << out[i].first << " " <<out[i].second << "\n\n";
	
	}
	*/
	
	
	
	delete[] out;
	
	
	return 0;
	
  	

}

