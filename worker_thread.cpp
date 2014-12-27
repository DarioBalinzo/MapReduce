template <class In, class Key, class Value>
class worker_args {
public:
	//std::vector<std::vector<std::pair<Key,Value>* > >* intermediate;
	long worker_id;
	
	In*input; //?
	long input_size;//?
	
	std::pair<Key,Value>** output;
	long *output_start;
	
	Map<In,Key,Value> *m; 
	Reduce<Key,Value> *r; 
	long mapper;
	long reducer;
	
	/*~worker_args<In,Key,Value>(){
		//std::cout << "ask to deleting a map\n";
		delete m; //destroy map
		//delete r;
	}*/
};
 




template<class In, class Key, class Value>
void* worker (void* _arg) {

	//input value parsing
	worker_args<In,Key,Value>* arg= (worker_args<In,Key,Value> *) _arg;
	//std::vector <std::vector<std::pair<Key,Value>*>>* intermediate=arg->intermediate;
	//std::unordered_map<In,Key> table;
	long worker_id=arg->worker_id;
	In*input=arg->input;
	long input_size=arg->input_size;
	std::pair<Key,Value>* output;
	long output_start;
	Map<In,Key,Value> *m=arg->m;
	Reduce<Key,Value> *r=arg->r;
	long mapper=arg->mapper;
	long reducer=arg->reducer;
	
	//std::cout << "alive!!!\n";
	if (worker_id < mapper) {
		long next;
		long i=0;
		for ( next=worker_id; next<input_size; next=(i*mapper) + worker_id) {
			i++;
			m->map(input[next]);
			
		}
		m->shuffle_and_sort();
	}
	
	//wait end of map and shuffle and sort
	pthread_barrier_wait(&barrier);
	#ifdef DEBUG
	//TODO next barrier needed for debugging
	pthread_barrier_wait(&barrier);
	#endif
	
	//doing the reduce
	if(worker_id < reducer) {
		r->do_reduce();
	}
	
	//wait the end of reduce
	pthread_barrier_wait(&barrier);
	//wait end of output allocation
	pthread_barrier_wait(&barrier);
	//filling the output array;
	if(worker_id < reducer) {
		output=*(arg->output);
		output_start=*(arg->output_start);
		r->filling_output(output, output_start );
	}

		
	
	
	return nullptr;
}


