/*
 *  MapReduce Skeleton
 *  Author: Dario Balinzo
 *  dariobalinzo@gmail.com
 *
 *  MapReduce Skeleton
 *
 *
 */
 
 
#include <vector> //std::vector
#include <pthread.h> 
#include <iostream> //debug
#include <utility> //std::pair
#include <unordered_map> //hash table std::unordered_map
#include <queue> //std::priority_queue
#include <algorithm>    // std::sort


pthread_barrier_t barrier;

#include "reduce.hpp"
#include "map.hpp" 
#include "map_thread.cpp"



long max(long i, long j) {
	if (i>j)
		return i;
	return j;
}



template<class In, class Key, class Value>
class MapReduce {
private:
	std::vector<Map<In,Key,Value>*> *all_maps;  //maps object, user instantiated
	long mapper; //#workers in map phase
	std::vector<Reduce<Key,Value>*> *all_reduces; //reduce class, user customised
	long reducer; //#workers in reduce phase
	In* input_data; //input array
	long input_size; //size of the input array
	std::pair<Key,Value>* output;
	long output_size;
public:
	/*Constructor, takes map object and reduces object, one for each map_worker, reduce_worker*/
	MapReduce(std::vector<Map<In,Key,Value>*> *_m, std::vector<Reduce<Key,Value>*> *_r) {
		mapper=_m->size();
		reducer=_r->size();
		all_maps=_m;
		all_reduces=_r;
	}
	
	/*Destructor*/
	~MapReduce(){
	
	};
	
	/*set the input array*/
	inline void setInput(In* input, long size) {
		input_data=input;
		input_size=size;
	}
	
	/*orchestrating the job, MASTER thread*/
	inline std::pair<Key,Value>* run_and_wait(long* _outsize) {
	
		#ifdef TIMING
		double t1,t2,t3; 
		struct timeval tim;
		//timing 
		gettimeofday(&tim, nullptr);  
		t1=tim.tv_sec+(tim.tv_usec/1000000.0); 
		#endif
	
		if (mapper <1 || reducer <1)
			return nullptr;
		long i;
		int rc;
		long nw=max(mapper, reducer);
		//thread data structure TODO initialize!
		pthread_t threads[nw];
		long output_start[nw];
		
		rc=pthread_barrier_init(&barrier, NULL, nw + 1);
		if(rc != 0) {
			std::cerr << "error creating the barrier\n";
			exit(-1);
		}
			
		
		//input data for each thread
		std::vector<worker_args<In,Key,Value>> args(nw);
		//saving the reference for map clone, they will be destroyed at the end
		  
		
		
		for(i=0; i<nw; i++) {
		
			
			args[i].worker_id=i;
			args[i].mapper=mapper;
			args[i].reducer=reducer;
			args[i].input=input_data;
			args[i].input_size=input_size;
			args[i].output=&output;
			args[i].output_start=&(output_start[i]);
			
			//assign map object, one for each mapper
			if (i<mapper) {
				args[i].m=all_maps->at(i);
				//setup private hash table for map worker and assign a generic
				//reduce object, just for computing the partial reduce result
				//during the map phase
				args[i].m->setup( input_size/mapper , all_reduces->at(0), reducer);
				//argsmap[i].m->dummy= new int(i);
				}
			else
				args[i].m=nullptr;
				
			
			if(i<reducer) {
				args[i].r=all_reduces->at(i);
				args[i].r->setup(mapper);
				
				}
			else
				args[i].r=nullptr;
							
		}
		
		for (i=0; i<nw; i++) {
			if (i<reducer) {
				long k;
				for (k=0; k<mapper; k++ ) 
					args[i].r->linkMapperOut(k,args[k].m->getPartial(i));
			}	
		}
		
		//std::cout <<" started!!!\n";
		
		for(i=0; i<nw; i++){
			
      		rc = pthread_create(&threads[i], NULL, worker<In,Key,Value>, (void *) &args[i]);
      		if (rc){
       		  	std::cerr << "ERROR from pthread_create(), mapper create\n";
         		exit(-1);
      		}
      	}
		
		#ifdef DEBUG
		//debugging 
		pthread_barrier_wait(&barrier);
		
		for(i=0; i<mapper; i++) {
			std::cout << "debug mapper" << i << "\n";
			all_maps->at(i)->debug();
		}
		#endif
		
		//wait end of map
		pthread_barrier_wait(&barrier);
		#ifdef TIMING
		gettimeofday(&tim, NULL);  
  		t2=tim.tv_sec+(tim.tv_usec/1000000.0);  
  		//printf("%.6lf seconds elapsed\n", t2-t1);
  		#endif
		//std::cout<< "end map!\n";
		//wait end of reduce
		pthread_barrier_wait(&barrier);
		
		//allocate out
		long size=0;
		for(i=0; i<reducer; i++) {
			output_start[i]=size;
			size+= all_reduces->at(i)->output_size();
		}
		output_size=size;
		output=new std::pair<Key,Value>[size];
		
		//signaling allocation
		pthread_barrier_wait(&barrier);
		



   		for(i=0; i<nw; i++) {
      		rc = pthread_join(threads[i], nullptr);
      		if (rc) {
         		std::cerr << "ERROR; return code from pthread_join() mapper\n";
         		exit(-1);
         	}
		}
		
		rc=pthread_barrier_destroy(&barrier);
		if(rc != 0) {
			std::cerr << "error destroyng the barrier\n";
			exit(-1);
		}
		
		#ifdef DEBUG
		for(i=0; i<reducer; i++) {
			all_reduces->at(i)->debug();
		}
		#endif
		
		for(i=0; i<mapper; i++) {
			all_maps->at(i)->delete_table();
		}
		
		for(i=0; i<reducer; i++) {
			all_reduces->at(i)->delete_structure();
		}
		
		
		#ifdef TIMING
		gettimeofday(&tim, NULL);  
  		t3=tim.tv_sec+(tim.tv_usec/1000000.0);  
  		printf("%.6lf map\n", t2-t1);
  		printf("%.6lf reduce\n", t3-t2);
  		printf("%.6lf total\n", t3-t1);
  		#endif
		
		
		*_outsize=output_size;
		return output;
	}
	



};


